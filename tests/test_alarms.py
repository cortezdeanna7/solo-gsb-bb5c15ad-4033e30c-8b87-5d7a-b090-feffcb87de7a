"""设备级高值告警测试：迟滞阈值状态机、非法阈值不覆盖旧规则、批次重试幂等与持久化。"""

from fastapi.testclient import TestClient

from app import db


def make_device(client, device_id="dev-1"):
    r = client.put(f"/devices/{device_id}/config", json={})
    assert r.status_code == 200


def put_rule(client, device_id="dev-1", trigger=10.0, recovery=5.0):
    return client.put(
        f"/devices/{device_id}/alarm",
        json={"trigger_threshold": trigger, "recovery_threshold": recovery},
    )


def get_state(client, device_id="dev-1"):
    return client.get(f"/devices/{device_id}/alarm").json()


def get_events(client, device_id="dev-1"):
    return client.get(f"/devices/{device_id}/alarm/events").json()


def ingest(client, events, device_id="dev-1"):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def ev(event_id, value, t="2026-09-23T00:00:{:02d}Z"):
    return {"event_id": event_id, "event_time": t.format(10 + len(event_id)), "value": value}


# ---------- 规则读取（未配置 / 设备不存在） ----------

def test_get_alarm_without_rule_is_unconfigured_normal(client):
    make_device(client)
    body = get_state(client)
    assert body["configured"] is False
    assert body["state"] == "normal"
    assert body["trigger_threshold"] is None
    assert body["recovery_threshold"] is None
    assert body["rule_version"] == 0
    assert body["transition_count"] == 0
    assert body["active_alarm"] is None
    assert body["last_transition"] is None
    assert get_events(client) == []


def test_alarm_endpoints_require_existing_device(client):
    assert client.get("/devices/ghost/alarm").status_code == 404
    assert client.get("/devices/ghost/alarm/events").status_code == 404
    assert put_rule(client, "ghost").status_code == 404


def test_ingest_without_rule_creates_no_alarm_state(client):
    make_device(client)
    ingest(client, [ev("e1", 999.0)])
    body = get_state(client)
    assert body["configured"] is False
    assert body["state"] == "normal"
    assert get_events(client) == []


# ---------- 规则写入与校验 ----------

def test_put_rule_creates_at_version_one(client):
    make_device(client)
    r = put_rule(client, trigger=10.0, recovery=5.0)
    assert r.status_code == 200
    body = r.json()
    assert body["configured"] is True
    assert body["trigger_threshold"] == 10.0
    assert body["recovery_threshold"] == 5.0
    assert body["state"] == "normal"
    assert body["rule_version"] == 1
    assert body["updated_at"] is not None


def test_recovery_must_be_strictly_lower_than_trigger(client):
    make_device(client)
    assert put_rule(client, trigger=10.0, recovery=10.0).status_code == 400
    assert put_rule(client, trigger=10.0, recovery=12.0).status_code == 400
    # 从未有过规则：非法写入不得落任何规则
    body = get_state(client)
    assert body["configured"] is False


def test_non_finite_thresholds_rejected(client):
    make_device(client)
    for trig, rec in [("NaN", 1.0), (10.0, "Infinity"), ("-Infinity", 1.0)]:
        r = client.put(
            "/devices/dev-1/alarm",
            json={"trigger_threshold": trig, "recovery_threshold": rec},
        )
        assert r.status_code == 400
    assert get_state(client)["configured"] is False


def test_invalid_threshold_does_not_overwrite_existing_rule(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("fire-1", 11.0)])
    assert get_state(client)["state"] == "alarm"

    # 非法新规则：400，旧阈值、版本与 alarm 运行状态全部原样保留
    r = put_rule(client, trigger=3.0, recovery=9.0)
    assert r.status_code == 400
    body = get_state(client)
    assert body["trigger_threshold"] == 10.0
    assert body["recovery_threshold"] == 5.0
    assert body["rule_version"] == 1
    assert body["state"] == "alarm"
    assert body["active_alarm"]["event_id"] == "fire-1"
    assert len(get_events(client)) == 1


def test_valid_rule_replacement_bumps_version_and_resets_state(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("fire-1", 11.0)])
    assert get_state(client)["state"] == "alarm"

    r = put_rule(client, trigger=20.0, recovery=0.0)
    assert r.status_code == 200
    body = r.json()
    assert body["rule_version"] == 2
    assert body["state"] == "normal"
    assert body["active_alarm"] is None
    assert body["last_transition"] is None
    # 历史流水与累计转换次数不回退
    assert body["transition_count"] == 1
    assert [e["transition"] for e in get_events(client)] == ["fired"]

    # 新阈值生效：11 在新规则下不再触发
    ingest(client, [ev("e-mid", 11.0)])
    assert get_state(client)["state"] == "normal"


# ---------- 迟滞状态机 ----------

def test_hysteresis_state_machine(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)

    # 中间区间（5, 10）在 normal 下不触发
    ingest(client, [ev("a", 7.0)])
    assert get_state(client)["state"] == "normal"
    assert get_events(client) == []

    # 到达触发线才进入 alarm（边界 >= 触发）
    ingest(client, [ev("b", 10.0)])
    s = get_state(client)
    assert s["state"] == "alarm"
    assert s["active_alarm"]["event_id"] == "b"
    assert s["active_alarm"]["value"] == 10.0
    assert s["last_transition"] == "fired"

    # 之后继续走高、以及在中间区间震荡，都保持 alarm，不重复进入
    ingest(client, [ev("c", 20.0), ev("d", 7.0), ev("e", 9.9)])
    s = get_state(client)
    assert s["state"] == "alarm"
    # 首发事件不被后续高值覆盖
    assert s["active_alarm"]["event_id"] == "b"
    assert len(get_events(client)) == 1

    # 到达恢复线才解除（边界 <= 恢复）
    ingest(client, [ev("f", 5.0)])
    s = get_state(client)
    assert s["state"] == "normal"
    assert s["active_alarm"] is None
    assert s["last_transition"] == "cleared"

    # 解除后中间区间保持 normal
    ingest(client, [ev("g", 6.0)])
    assert get_state(client)["state"] == "normal"
    assert [e["transition"] for e in get_events(client)] == ["fired", "cleared"]


def test_fire_and_clear_within_single_batch(client):
    """批次内按提交顺序驱动：同一批次先越过触发线再跌回恢复线，最终 normal。"""
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("x1", 11.0), ev("x2", 4.0)])
    s = get_state(client)
    assert s["state"] == "normal"
    assert s["last_event_id"] == "x2"
    assert [e["transition"] for e in get_events(client)] == ["fired", "cleared"]
    fired, cleared = get_events(client)
    assert fired["event_id"] == "x1"
    assert cleared["event_id"] == "x2"
    assert fired["trigger_threshold"] == 10.0
    assert fired["recovery_threshold"] == 5.0
    assert fired["rule_version"] == 1


def test_can_fire_again_after_recovery(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("p1", 11.0), ev("p2", 4.0), ev("p3", 12.0)])
    s = get_state(client)
    assert s["state"] == "alarm"
    assert s["active_alarm"]["event_id"] == "p3"
    assert [e["transition"] for e in get_events(client)] == ["fired", "cleared", "fired"]
    assert s["transition_count"] == 3


def test_late_events_also_drive_alarm(client):
    """求值只看事件值，accepted 与 late 事件都驱动状态转换。"""
    make_device(client)
    put_rule(client, trigger=10.0, recovery=0.0)
    # 先把水位线推远
    ingest(client, [{
        "event_id": "future", "event_time": "2026-09-23T01:00:00Z", "value": 1.0,
    }])
    # 迟到高值事件仍触发告警
    r = ingest(client, [{
        "event_id": "late-spike", "event_time": "2026-09-23T00:00:05Z", "value": 99.0,
    }])
    assert r.json()["results"][0]["status"] == "late"
    assert get_state(client)["state"] == "alarm"
    assert get_state(client)["active_alarm"]["event_id"] == "late-spike"


# ---------- 批次重试幂等 ----------

def test_batch_retry_does_not_duplicate_transitions(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    batch = [ev("r1", 11.0), ev("r2", 4.0)]
    ingest(client, batch)
    first_events = get_events(client)
    assert [e["transition"] for e in first_events] == ["fired", "cleared"]
    first_count = get_state(client)["transition_count"]

    # 完全相同的批次重试：事件全部去重，状态机不再次求值
    r = ingest(client, batch)
    assert all(x["status"] == "duplicate" for x in r.json()["results"])
    s = get_state(client)
    assert s["state"] == "normal"
    assert s["transition_count"] == first_count
    assert get_events(client) == first_events


def test_retry_of_firing_batch_while_state_advances(client):
    """首批准触发后状态已推进；重试时即使重复事件未被前置去重，条件 UPDATE 也会落空。"""
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("r1", 11.0)])
    # 状态继续前进：先解除再重新触发到另一事件
    ingest(client, [ev("r2", 1.0), ev("r3", 12.0)])
    assert get_state(client)["state"] == "alarm"
    # 重试最老的触发批次：去重跳过，不产生新 fired，当前首发仍为 r3
    ingest(client, [ev("r1", 11.0)])
    s = get_state(client)
    assert s["state"] == "alarm"
    assert s["active_alarm"]["event_id"] == "r3"
    assert [e["event_id"] for e in get_events(client)] == ["r1", "r2", "r3"]


# ---------- 持久化 ----------

def test_alarm_rule_and_state_persisted_across_restarts(client, tmp_path, monkeypatch):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("s1", 11.0), ev("s2", 4.0), ev("s3", 40.0)])
    assert get_state(client)["state"] == "alarm"

    # 重启：同一数据库文件重建 app，规则、状态、流水全部保留
    from app.main import app

    with TestClient(app) as c2:
        body = c2.get("/devices/dev-1/alarm").json()
        assert body["configured"] is True
        assert body["trigger_threshold"] == 10.0
        assert body["recovery_threshold"] == 5.0
        assert body["state"] == "alarm"
        assert body["rule_version"] == 1
        assert body["transition_count"] == 3
        assert body["active_alarm"]["event_id"] == "s3"
        events = c2.get("/devices/dev-1/alarm/events").json()
        assert [e["transition"] for e in events] == ["fired", "cleared", "fired"]

        # 重启后新事件仍按持久化状态继续驱动
        ingest(c2, [ev("s4", 0.0)])
        assert c2.get("/devices/dev-1/alarm").json()["state"] == "normal"


def test_alarm_event_log_is_append_only(client):
    make_device(client)
    put_rule(client, trigger=10.0, recovery=5.0)
    ingest(client, [ev("a1", 11.0)])
    with db.session() as conn:
        import pytest as _pytest

        with _pytest.raises(Exception):
            conn.execute("UPDATE device_alarm_events SET transition = 'cleared' WHERE id = 1")
        with _pytest.raises(Exception):
            conn.execute("DELETE FROM device_alarm_events WHERE id = 1")
