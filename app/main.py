"""传感器遥测汇聚 API。

核心语义：
- 每个设备维护一条持久化的事件时间水位线 watermark = 已见最大 event_time。
- 时间窗为按 window_size_seconds 对齐 epoch 的滚动窗口。
- 当 watermark >= window_end + allowed_lateness_seconds 时，窗口封存（sealed）。
- 重复 (device_id, event_id) 幂等去重，不影响聚合结果。
- 窗口封存后到达的事件标记为 late，仍计入聚合，同时生成一条修正记录，
  窗口的 correction_count 随之递增。

设备可版本化标签：
- PUT /devices/{device_id}/labels 以**全量替换**语义写入不超过 20 个 key/value；
  键名在同一 SQLite 写事务内规范化（去首尾空白、小写、^[a-z0-9][a-z0-9_.-]*$），
  值只去首尾空白；规范化后重复键或非法键名返回 400 且不改动数据。
- 每次成功写入都把 devices.label_version 加一（单调递增，从 1 起），旧标签同事务删除。
- GET /devices/{device_id}/labels 返回当前标签与版本（从未写入时版本 0、标签为空）。
- GET /devices?label_key=&label_value= 按**当前**标签筛选设备（只给 label_value
  不给 label_key 返回 422）。
- PUT 请求带 if_version 时做乐观并发校验：必须等于服务端当前版本，
  不匹配返回 409 且不写入任何数据（写锁事务保证读版本与写入的一致性）。

设备组同步窗口汇总：
- 设备组为固定成员组，创建时所有成员设备必须存在且窗口配置一致
  （window_size_seconds / allowed_lateness_seconds），聚合方式为 sum 或 avg。
- 同一 window_start 下仅当全部成员的设备窗口均已封存（sealed）时，
  才生成组汇总（ready）；任一成员窗口未封存（或尚无窗口）时保持 pending，
  value 为 NULL，绝不用部分成员提前计算。
- 成员窗口因迟到事件发生修正后，已生成的组汇总原位重算且 revision 加一；
- 修正以成员窗口 correction_count 之和为签名：同一 event_id 重试是幂等去重，
  不会产生新修正，签名不变，revision 不重复升版。

组窗口修订审计（append-only 快照）：
- 组窗口首次 ready、以及迟到修正令 revision 升版时，在**同一事务**追加一份不可变快照，
  记录当时的 group value 与每个固定成员窗口的 count、sum、avg、correction_count。
- (group_id, window_start, revision) 唯一约束保证重复上报不会重复留档；
  审计表带禁止 UPDATE/DELETE 的触发器，后续修正只能追加新版本、旧快照永不被覆盖。
- 服务初始化时只对旧库中**已 ready** 的组窗口补一份**当前 revision** 的 baseline，
  绝不伪造更早的历史版本。
- 提供修订列表与两版本 diff 接口；diff 按成员顺序给出前后贡献与数值差，仅列发生变化的成员，
  非法版本返回 422、不存在的组/窗口/修订返回 404。

封存窗口稳健异常检测（只读）：
- GET /devices/{device_id}/anomalies?window_start=&threshold= 仅对**已封存**窗口内事件
  （含落入该窗口的迟到事件）做基于中位数与 MAD（median absolute deviation）的稳健打分：
  score = 0.6745 * |x - median| / MAD，只返回 score > threshold 的事件，threshold 默认 3.5 且必须 > 0。
- MAD 为 0（超过半数事件等于中位数）时，除以 0 无定义：按需求把所有**不等于中位数**的事件
  一律判为异常返回，其 score 记为 null；全部等于中位数则无异常。
- 设备或窗口不存在返回 404，窗口尚未封存（open）返回 409；接口纯只读，不改变任何现有聚合。

跨设备窗口检索（稳定游标分页，只读）：
- GET /windows/search 跨设备检索窗口，可按窗口时间范围（对 window_start 取半开区间 [start, end)）、
  open/sealed 状态及设备**当前**标签（label_key/label_value，语义与 GET /devices 一致）精确筛选，
  按 window_start、device_id 升序返回窗口聚合与设备标识。
- 分页采用与筛选条件绑定的不透明游标：游标内嵌 (window_start, device_id) 键集位置、
  首次查询时的行快照上界（windows 表 MAX(rowid)）与规范化后的全部筛选条件，
  并以 HMAC-SHA256 签名防篡改、默认 1 小时过期。翻页期间**新增**的窗口行 rowid 超出快照上界，
  不会进入本次遍历，因此新增数据既不会在翻页中重复出现也不会挤掉/插入造成遗漏
  （不带游标重新发起检索即可看到新增数据）。
- 游标签名非法、载荷损坏、已过期，或与本次请求的筛选条件不一致，一律返回 400。

设备级高值告警（迟滞阈值状态机）：
- PUT /devices/{device_id}/alarm 设置规则：trigger_threshold 为触发阈值，
  recovery_threshold 为恢复阈值，二者必须为有限数且 recovery < trigger，否则返回 400
  且**绝不覆盖旧规则**（旧规则与旧状态原样保留）；每次成功写入规则 rule_version 加一，
  状态重置为 normal（规则语义改变后旧的告警状态不再适用；append-only 转换历史保留）。
- 状态转换由**事件值**在 ingest 写事务内驱动：normal 下 value >= trigger 首次进入 alarm；
  alarm 下 value <= recovery 才解除恢复 normal；recovery < value < trigger 的中间区间
  保持原状态，避免阈值附近反复抖动。转换只在状态真正翻转时发生一次，同事务追加一条不可变流水。
- 重复 (device_id, event_id) 在求值前去重跳过，因此批次重试/重复上报**绝不重复产生状态变化**。
- GET /devices/{device_id}/alarm 查询当前规则、状态、当前告警首发事件与最近一次转换；
  GET /devices/{device_id}/alarm/events 查询追加式转换流水；均持久化，重启不丢失。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import db

DEFAULT_WINDOW_SIZE_SECONDS = 60
DEFAULT_ALLOWED_LATENESS_SECONDS = 30
MAX_GROUP_MEMBERS = 500
MAX_DEVICE_LABELS = 20
MAX_LABEL_KEY_LENGTH = 63
MAX_LABEL_VALUE_LENGTH = 256
# 跨设备窗口检索：分页大小与游标有效期（秒）；游标在有效期内可稳定翻完首次检索的快照
SEARCH_DEFAULT_LIMIT = 100
SEARCH_MAX_LIMIT = 1000
SEARCH_CURSOR_TTL_SECONDS = 3600
# 规范化后的合法键名：小写字母/数字开头，其余允许下划线、点、连字符
LABEL_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    backfill_baseline_snapshots()
    yield


app = FastAPI(title="Sensor Telemetry Aggregation API", version="1.6.0", lifespan=lifespan)

# 稳健异常检测：基于中位数/MAD 的修正 z 分数，0.6745 为正态分布下 MAD ≈ 0.6745σ 的换算系数
DEFAULT_ANOMALY_THRESHOLD = 3.5
MAD_NORMALIZER = 0.6745


# ---------- Pydantic 模型 ----------

class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    event_time: datetime  # ISO 8601；不带时区时按 UTC 处理
    value: float


class IngestRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    events: list[EventIn] = Field(min_length=1, max_length=10000)


class EventResult(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate", "late"]
    window_start: Optional[datetime] = None


class IngestResponse(BaseModel):
    device_id: str
    watermark: Optional[datetime]
    accepted: int
    duplicates: int
    late: int
    results: list[EventResult]


class DeviceConfig(BaseModel):
    window_size_seconds: int = Field(default=DEFAULT_WINDOW_SIZE_SECONDS, gt=0, le=86400)
    allowed_lateness_seconds: int = Field(default=DEFAULT_ALLOWED_LATENESS_SECONDS, ge=0, le=86400)


class DeviceOut(BaseModel):
    device_id: str
    window_size_seconds: int
    allowed_lateness_seconds: int
    watermark: Optional[datetime]
    event_count: int
    open_windows: int
    sealed_windows: int
    created_at: datetime


class LabelItem(BaseModel):
    key: str = Field(min_length=1, max_length=256)
    value: str = Field(min_length=0, max_length=256)


class LabelUpdateRequest(BaseModel):
    labels: list[LabelItem] = Field(min_length=0, max_length=MAX_DEVICE_LABELS)
    # 乐观并发：客户端基于上次读到的版本提交；缺省表示不校验
    if_version: Optional[int] = Field(default=None, ge=0)


class LabelsOut(BaseModel):
    device_id: str
    version: int
    labels: dict[str, str]
    updated_at: Optional[datetime] = None


class WindowOut(BaseModel):
    device_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["open", "sealed"]
    count: int
    sum: float
    avg: Optional[float]
    min: Optional[float]
    max: Optional[float]
    correction_count: int
    late_event_count: int
    sealed_at: Optional[datetime]


class WindowSearchResponse(BaseModel):
    # 窗口聚合与设备标识，按 window_start、device_id 升序
    windows: list[WindowOut]
    # 下一页游标（不透明，仅与本次筛选条件绑定）；没有更多数据时为 null
    next_cursor: Optional[str] = None


class CorrectionOut(BaseModel):
    id: int
    device_id: str
    event_id: str
    window_start: datetime
    event_time: datetime
    value: float
    created_at: datetime


class AnomalyOut(BaseModel):
    event_id: str
    event_time: datetime
    value: float
    # 修正 z 分数；MAD 为 0 时分数无定义（除以 0），非中位值一律判为异常，score 返回 null
    score: Optional[float] = None


class GroupCreate(BaseModel):
    group_id: str = Field(min_length=1, max_length=128)
    member_ids: list[str] = Field(min_length=1, max_length=MAX_GROUP_MEMBERS)
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int = Field(gt=0, le=86400)
    allowed_lateness_seconds: int = Field(ge=0, le=86400)


class GroupOut(BaseModel):
    group_id: str
    member_ids: list[str]
    aggregation: Literal["sum", "avg"]
    window_size_seconds: int
    allowed_lateness_seconds: int
    created_at: datetime


class GroupWindowOut(BaseModel):
    group_id: str
    window_start: datetime
    window_end: datetime
    status: Literal["pending", "ready"]
    value: Optional[float] = None
    member_count: int
    sealed_member_count: int
    revision: int
    ready_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class PendingMemberOut(BaseModel):
    device_id: str
    window_start: Optional[datetime] = None
    window_end: datetime
    window_status: Optional[Literal["open", "sealed"]] = None  # None = 成员尚无该窗口
    reason: Literal["missing", "open"]


class GroupWindowRevisionOut(BaseModel):
    revision: int
    value: float
    kind: Literal["revision", "baseline"]
    created_at: datetime
    member_count: int


class RevisionMemberStateOut(BaseModel):
    device_id: str
    position: int
    count: int
    sum: float
    avg: Optional[float] = None
    correction_count: int
    contribution: float  # 该成员当时对 group value 的贡献：sum 聚合为 sum，avg 聚合为窗口 avg


class RevisionMemberDeltaOut(BaseModel):
    device_id: str
    position: int
    before: RevisionMemberStateOut
    after: RevisionMemberStateOut
    count_delta: int
    sum_delta: float
    avg_delta: Optional[float] = None
    correction_count_delta: int
    contribution_delta: float


class RevisionDiffOut(BaseModel):
    group_id: str
    window_start: datetime
    aggregation: Literal["sum", "avg"]
    from_revision: int
    to_revision: int
    from_value: float
    to_value: float
    value_delta: float
    members: list[RevisionMemberDeltaOut]  # 仅含发生变化的成员，按成员顺序排列


# ---------- 设备级高值告警模型 ----------

class AlarmRuleIn(BaseModel):
    # 触发阈值：normal 状态下事件值首次 >= 该值进入 alarm
    trigger_threshold: float
    # 恢复阈值：alarm 状态下事件值 <= 该值才解除；必须严格小于 trigger_threshold
    recovery_threshold: float


class ActiveAlarmOut(BaseModel):
    # 进入当前告警状态的首发事件（仅首次越过触发线的事件；之后的高值不更新）
    event_id: str
    event_time: datetime
    value: float
    started_at: datetime


class AlarmStateOut(BaseModel):
    device_id: str
    configured: bool  # 是否设置过规则；False 时阈值为 null、state 固定 normal
    trigger_threshold: Optional[float] = None
    recovery_threshold: Optional[float] = None
    state: Literal["normal", "alarm"]
    rule_version: int
    transition_count: int
    active_alarm: Optional[ActiveAlarmOut] = None  # state=alarm 时为首发事件，normal 时为 null
    last_transition: Optional[Literal["fired", "cleared"]] = None
    last_event_id: Optional[str] = None
    last_event_time: Optional[datetime] = None
    last_value: Optional[float] = None
    last_transition_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AlarmTransitionOut(BaseModel):
    id: int
    device_id: str
    event_id: str
    event_time: datetime
    value: float
    transition: Literal["fired", "cleared"]
    trigger_threshold: float
    recovery_threshold: float
    rule_version: int
    created_at: datetime


# ---------- 工具函数 ----------

def to_epoch(dt: datetime) -> float:
    """datetime -> epoch 秒；naive 时间按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def window_floor(t: float, size: int) -> float:
    return math.floor(t / size) * size


def get_device(conn: sqlite3.Connection, device_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)).fetchone()


def get_or_create_device(conn: sqlite3.Connection, device_id: str, now: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        conn.execute(
            "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
            " VALUES (?, ?, ?, NULL, ?)",
            (device_id, DEFAULT_WINDOW_SIZE_SECONDS, DEFAULT_ALLOWED_LATENESS_SECONDS, now),
        )
        row = get_device(conn, device_id)
    return row


def require_device(conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
    row = get_device(conn, device_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return row


_DEVICE_STATS_SELECT = (
    "SELECT d.*,"
    " (SELECT COUNT(*) FROM events WHERE device_id = d.device_id) AS event_count,"
    " (SELECT COUNT(*) FROM windows WHERE device_id = d.device_id AND status = 'open') AS open_windows,"
    " (SELECT COUNT(*) FROM windows WHERE device_id = d.device_id AND status = 'sealed') AS sealed_windows"
    " FROM devices d"
)


def device_row_to_out(r: sqlite3.Row) -> DeviceOut:
    return DeviceOut(
        device_id=r["device_id"],
        window_size_seconds=r["window_size_seconds"],
        allowed_lateness_seconds=r["allowed_lateness_seconds"],
        watermark=to_iso(r["watermark"]),
        event_count=r["event_count"],
        open_windows=r["open_windows"],
        sealed_windows=r["sealed_windows"],
        created_at=r["created_at"],
    )


def device_out(conn: sqlite3.Connection, device_id: str) -> DeviceOut:
    row = conn.execute(
        _DEVICE_STATS_SELECT + " WHERE d.device_id = ?", (device_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"device not found: {device_id}")
    return device_row_to_out(row)


def normalize_label_key(raw: str) -> str:
    """键名规范化：去首尾空白并转小写；非法键名抛 400。"""
    key = raw.strip().lower()
    if len(key) > MAX_LABEL_KEY_LENGTH or not LABEL_KEY_RE.fullmatch(key):
        raise HTTPException(
            status_code=400,
            detail=(
                f"invalid label key: {raw!r}; after normalization it must match "
                f"{LABEL_KEY_RE.pattern} and be at most {MAX_LABEL_KEY_LENGTH} characters"
            ),
        )
    return key


def normalize_label_value(raw: str) -> str:
    """值规范化：仅去首尾空白；空值或超长值抛 400。"""
    value = raw.strip()
    if not value:
        raise HTTPException(status_code=400, detail="label value must not be empty")
    if len(value) > MAX_LABEL_VALUE_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"label value must be at most {MAX_LABEL_VALUE_LENGTH} characters",
        )
    return value


def normalize_labels(items: list[LabelItem]) -> dict[str, str]:
    """规范化整批标签并拒绝规范化后的重复键；结果按键名排序。"""
    labels: dict[str, str] = {}
    for item in items:
        key = normalize_label_key(item.key)
        value = normalize_label_value(item.value)
        if key in labels:
            raise HTTPException(
                status_code=400,
                detail=f"duplicate label key after normalization: {key}",
            )
        labels[key] = value
    return dict(sorted(labels.items()))


def get_device_labels(conn: sqlite3.Connection, device_id: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT label_key, label_value FROM device_labels"
        " WHERE device_id = ? ORDER BY label_key",
        (device_id,),
    ).fetchall()
    return {r["label_key"]: r["label_value"] for r in rows}


# ---------- 设备级高值告警 ----------

def load_alarm_state(conn: sqlite3.Connection, device_id: str) -> Optional[dict[str, Any]]:
    """读取设备告警规则与当前状态；未设置规则返回 None。

    ingest 循环开始前读取一次：SQLite 写事务在整个批次期间独占写锁，
    规则不会在批次处理中途被其他写事务改掉，故批次内复用同一份可变状态即可。
    """
    row = conn.execute(
        "SELECT * FROM device_alarms WHERE device_id = ?", (device_id,)
    ).fetchone()
    return dict(row) if row is not None else None


def apply_alarm_transition(
    conn: sqlite3.Connection,
    alarm: dict[str, Any],
    *,
    device_id: str,
    event_id: str,
    t: float,
    value: float,
    now: str,
) -> None:
    """按迟滞阈值状态机对单个**已去重**事件求值，必要时在当前事务内翻转状态。

    - normal 且 value >= trigger_threshold → fired，进入 alarm；
    - alarm 且 value <= recovery_threshold → cleared，恢复 normal；
    - 中间区间（recovery < value < trigger）与同态区间保持原状态，不产生任何写入。

    转换为带 state 守卫的条件 UPDATE：仅当数据库中当前状态仍是预期状态时才生效，
    并以 cursor.rowcount == 1 作为“本次调用真正完成翻转”的唯一判据。重复批次的事件
    在调用前已被 (device_id, event_id) 幂等去重跳过，即使进入这里条件 UPDATE 也会因
    状态已推进而落空，因此**批次重试绝不会重复产生状态变化或重复流水**。
    """
    trigger = alarm["trigger_threshold"]
    recovery = alarm["recovery_threshold"]
    state = alarm["state"]

    if state == "normal" and value >= trigger:
        cur = conn.execute(
            "UPDATE device_alarms SET state = 'alarm', transition_count = transition_count + 1,"
            " active_alarm_event_id = ?, active_alarm_event_time = ?, active_alarm_value = ?,"
            " active_alarm_started_at = ?, last_transition = 'fired', last_event_id = ?,"
            " last_event_time = ?, last_value = ?, last_transition_at = ?, updated_at = ?"
            " WHERE device_id = ? AND state = 'normal'",
            (event_id, t, value, now, event_id, t, value, now, now, device_id),
        )
        if cur.rowcount == 1:
            conn.execute(
                "INSERT INTO device_alarm_events (device_id, event_id, event_time, value, transition,"
                " trigger_threshold, recovery_threshold, rule_version, created_at)"
                " VALUES (?, ?, ?, ?, 'fired', ?, ?, ?, ?)",
                (device_id, event_id, t, value, trigger, recovery, alarm["rule_version"], now),
            )
            alarm["state"] = "alarm"
            alarm["transition_count"] += 1
    elif state == "alarm" and value <= recovery:
        cur = conn.execute(
            "UPDATE device_alarms SET state = 'normal', transition_count = transition_count + 1,"
            " active_alarm_event_id = NULL, active_alarm_event_time = NULL, active_alarm_value = NULL,"
            " active_alarm_started_at = NULL, last_transition = 'cleared', last_event_id = ?,"
            " last_event_time = ?, last_value = ?, last_transition_at = ?, updated_at = ?"
            " WHERE device_id = ? AND state = 'alarm'",
            (event_id, t, value, now, now, device_id),
        )
        if cur.rowcount == 1:
            conn.execute(
                "INSERT INTO device_alarm_events (device_id, event_id, event_time, value, transition,"
                " trigger_threshold, recovery_threshold, rule_version, created_at)"
                " VALUES (?, ?, ?, ?, 'cleared', ?, ?, ?, ?)",
                (device_id, event_id, t, value, trigger, recovery, alarm["rule_version"], now),
            )
            alarm["state"] = "normal"
            alarm["transition_count"] += 1


def alarm_row_to_out(device_id: str, r: Optional[sqlite3.Row]) -> AlarmStateOut:
    if r is None:
        return AlarmStateOut(
            device_id=device_id,
            configured=False,
            state="normal",
            rule_version=0,
            transition_count=0,
        )
    active = None
    if r["state"] == "alarm" and r["active_alarm_event_id"] is not None:
        active = ActiveAlarmOut(
            event_id=r["active_alarm_event_id"],
            event_time=to_iso(r["active_alarm_event_time"]),
            value=r["active_alarm_value"],
            started_at=r["active_alarm_started_at"],
        )
    return AlarmStateOut(
        device_id=device_id,
        configured=True,
        trigger_threshold=r["trigger_threshold"],
        recovery_threshold=r["recovery_threshold"],
        state=r["state"],
        rule_version=r["rule_version"],
        transition_count=r["transition_count"],
        active_alarm=active,
        last_transition=r["last_transition"],
        last_event_id=r["last_event_id"],
        last_event_time=to_iso(r["last_event_time"]),
        last_value=r["last_value"],
        last_transition_at=r["last_transition_at"],
        updated_at=r["updated_at"],
    )


def window_row_to_out(r: sqlite3.Row) -> WindowOut:
    return WindowOut(
        device_id=r["device_id"],
        window_start=to_iso(r["window_start"]),
        window_end=to_iso(r["window_end"]),
        status=r["status"],
        count=r["count"],
        sum=r["sum"],
        avg=r["sum"] / r["count"] if r["count"] else None,
        min=r["min"],
        max=r["max"],
        correction_count=r["correction_count"],
        late_event_count=r["late_event_count"],
        sealed_at=r["sealed_at"],
    )


# ---------- 跨设备窗口检索：游标编解码 ----------

# 进程级签名密钥：首次需要时惰性生成；服务重启后旧游标签名自然失效（按非法游标返回 400）。
_cursor_secret: Optional[bytes] = None


def _get_cursor_secret() -> bytes:
    global _cursor_secret
    if _cursor_secret is None:
        _cursor_secret = os.urandom(32)
    return _cursor_secret


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _sign(payload_b64: str) -> str:
    return hmac.new(
        _get_cursor_secret(), payload_b64.encode("ascii"), hashlib.sha256
    ).hexdigest()


def encode_search_cursor(
    window_start: float,
    device_id: str,
    snapshot_max_rowid: int,
    filters: dict[str, Any],
    issued_at: Optional[float] = None,
) -> str:
    """生成不透明游标：载荷为版本化 JSON（base64url），附 HMAC-SHA256 签名。"""
    payload = {
        "v": 1,
        "ws": window_start,
        "did": device_id,
        "snap": snapshot_max_rowid,
        "iss": issued_at if issued_at is not None else time.time(),
        "f": filters,
    }
    payload_b64 = _b64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    return payload_b64 + "." + _sign(payload_b64)


def decode_search_cursor(cursor: str, filters: dict[str, Any]) -> dict[str, Any]:
    """校验并解码游标，返回载荷；任何非法情形（含过期、筛选条件不一致）抛 400。

    - 结构损坏/载荷不是合法 JSON/缺字段/签名不匹配 → 400（防篡改、防伪造）；
    - 超过 SEARCH_CURSOR_TTL_SECONDS → 400（过期游标必须重新发起检索）；
    - 游标绑定的筛选条件与当前请求不一致 → 400（游标只在同一组筛选下有效）。
    """
    invalid = HTTPException(status_code=400, detail="invalid cursor")
    try:
        payload_b64, _, sig = cursor.partition(".")
        if not payload_b64 or not sig or not hmac.compare_digest(sig, _sign(payload_b64)):
            raise invalid
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if not isinstance(payload, dict):
            raise invalid
        if payload.get("v") != 1:
            raise invalid
        window_start = float(payload["ws"])
        device_id = payload["did"]
        snapshot_max_rowid = int(payload["snap"])
        issued_at = float(payload["iss"])
    except (ValueError, TypeError, KeyError, UnicodeError):
        # ValueError/UnicodeError: base64 或数值转换失败；KeyError: 缺字段
        raise invalid

    if not isinstance(device_id, str) or not device_id:
        raise invalid
    if time.time() - issued_at > SEARCH_CURSOR_TTL_SECONDS:
        raise HTTPException(status_code=400, detail="cursor expired; please restart the search")
    if payload.get("f") != filters:
        raise HTTPException(
            status_code=400,
            detail="cursor does not match the current filter parameters",
        )
    return {
        "window_start": window_start,
        "device_id": device_id,
        "snapshot_max_rowid": snapshot_max_rowid,
    }


# ---------- 设备组工具 ----------

def placeholders(n: int) -> str:
    return ",".join("?" * n)


def require_group(conn: sqlite3.Connection, group_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"group not found: {group_id}")
    return row


def group_member_ids(conn: sqlite3.Connection, group_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT device_id FROM device_group_members WHERE group_id = ? ORDER BY position",
        (group_id,),
    ).fetchall()
    return [r["device_id"] for r in rows]


def reconcile_group(conn: sqlite3.Connection, group_id: str, now: str) -> None:
    """对一个组的各 window_start 做对账。

    - 全部成员窗口 sealed → ready，计算 sum/avg；已 ready 且成员发生新修正时原位重算、revision+1。
    - 否则维持/落为 pending，value 始终为 NULL，绝不使用部分成员数据。
    触发时机：成员设备 ingest 完成封存后，以及组创建后。
    """
    group = conn.execute("SELECT * FROM device_groups WHERE group_id = ?", (group_id,)).fetchone()
    if group is None:
        return
    size = group["window_size_seconds"]
    aggregation = group["aggregation"]

    members = group_member_ids(conn, group_id)
    member_n = len(members)

    # 组窗口口径：任一成员出现过的、属于本组对齐网格的 window_start 都是候选。
    # 组创建时已保证成员窗口配置一致，这里再按窗口大小过滤以排除理论上的异网格数据。
    starts = [
        r["window_start"]
        for r in conn.execute(
            f"SELECT DISTINCT w.window_start AS window_start FROM windows w"
            f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
            f" WHERE (w.window_start - CAST(w.window_start AS INTEGER)) = 0"
            f"   AND CAST(w.window_start AS INTEGER) % ? = 0"
            f" ORDER BY w.window_start",
            (group_id, size),
        ).fetchall()
    ]

    for w_start in starts:
        w_end = w_start + size
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT w.* FROM windows w"
                f" JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                f" WHERE w.window_start = ? ORDER BY m.position",
                (group_id, w_start),
            ).fetchall()
        }
        sealed = [win_rows[d] for d in members if d in win_rows and win_rows[d]["status"] == "sealed"]
        sealed_n = len(sealed)
        all_sealed = sealed_n == member_n
        correction_sig = sum(r["correction_count"] for r in sealed)

        existing = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()

        if all_sealed:
            if aggregation == "sum":
                value = sum(r["sum"] for r in sealed)
            else:
                # avg：各成员窗口平均值的算术平均（每个固定成员权重相同）
                value = sum(r["sum"] / r["count"] for r in sealed if r["count"] > 0) / member_n

            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'ready', ?, ?, ?, 1, ?, ?, ?, ?)",
                    (group_id, w_start, w_end, value, member_n, member_n, correction_sig, now, now, now),
                )
                # 首次 ready：同一事务追加 revision=1 的不可变快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
            elif existing["status"] == "ready":
                # 已生成的组汇总原位重算：仅当成员窗口出现新的迟到修正（签名推进）时升版。
                # 同一 event_id 重试在 ingest 侧幂等去重，correction_count 不变，故不重复升版。
                if correction_sig > existing["correction_sig"]:
                    new_revision = existing["revision"] + 1
                    conn.execute(
                        "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                        " revision = revision + 1, correction_sig = ?, updated_at = ?"
                        " WHERE group_id = ? AND window_start = ?",
                        (value, member_n, correction_sig, now, group_id, w_start),
                    )
                    # revision 升版：同一事务追加新快照；旧快照不动、绝不覆盖
                    append_revision_snapshot(
                        conn, group_id, w_start, new_revision, value, members, win_rows, now
                    )
            else:
                # pending → ready（迟到修正发生在 ready 之前，已包含在首次结果里，revision 从 1 起）
                conn.execute(
                    "UPDATE group_windows SET status = 'ready', value = ?, sealed_member_count = ?,"
                    " revision = 1, correction_sig = ?, ready_at = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (value, member_n, correction_sig, now, now, group_id, w_start),
                )
                # pending → ready 的首次 ready：同一事务追加 revision=1 快照
                append_revision_snapshot(conn, group_id, w_start, 1, value, members, win_rows, now)
        else:
            if existing is None:
                conn.execute(
                    "INSERT INTO group_windows (group_id, window_start, window_end, status, value,"
                    " member_count, sealed_member_count, revision, correction_sig, ready_at,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'pending', NULL, ?, ?, 0, 0, NULL, ?, ?)",
                    (group_id, w_start, w_end, member_n, sealed_n, now, now),
                )
            elif existing["status"] == "pending" and existing["sealed_member_count"] != sealed_n:
                conn.execute(
                    "UPDATE group_windows SET sealed_member_count = ?, updated_at = ?"
                    " WHERE group_id = ? AND window_start = ?",
                    (sealed_n, now, group_id, w_start),
                )


def reconcile_groups_for_device(conn: sqlite3.Connection, device_id: str, now: str) -> None:
    """成员设备 ingest 后，对其所属的全部组做对账。"""
    group_ids = [
        r["group_id"]
        for r in conn.execute(
            "SELECT group_id FROM device_group_members WHERE device_id = ?", (device_id,)
        ).fetchall()
    ]
    for group_id in group_ids:
        reconcile_group(conn, group_id, now)


def append_revision_snapshot(
    conn: sqlite3.Connection,
    group_id: str,
    window_start: float,
    revision: int,
    value: float,
    members: list[str],
    win_rows: dict[str, sqlite3.Row],
    now: str,
    kind: Literal["revision", "baseline"] = "revision",
) -> None:
    """在当前事务内为组窗口的一个 revision 追加不可变快照。

    快照头记录当时的 group value；成员明细按固定成员顺序记录每个成员窗口当时的
    count/sum/avg/correction_count。(group_id, window_start, revision) 唯一约束兜底，
    重复上报（签名未推进）不会再次进入这里，即使进入也会被约束挡下而不重复留档。
    """
    cur = conn.execute(
        "INSERT INTO group_window_snapshots"
        " (group_id, window_start, revision, value, kind, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (group_id, window_start, revision, value, kind, now),
    )
    snapshot_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO group_window_snapshot_members"
        " (snapshot_id, group_id, device_id, position, count, sum, avg, correction_count)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                snapshot_id,
                group_id,
                device_id,
                position,
                win_rows[device_id]["count"],
                win_rows[device_id]["sum"],
                (
                    win_rows[device_id]["sum"] / win_rows[device_id]["count"]
                    if win_rows[device_id]["count"]
                    else None
                ),
                win_rows[device_id]["correction_count"],
            )
            for position, device_id in enumerate(members)
        ],
    )


def backfill_baseline_snapshots() -> None:
    """旧库升级时为已 ready 但没有任何快照的组窗口补一份**当前 revision** 的 baseline。

    只补当前版本这一份，绝不伪造 ready 之后可能存在过的早期版本；幂等：
    已有任意快照的 ready 窗口跳过，新库（启动即为空）无任何可补数据。
    """
    now = utcnow_iso()
    with db.session() as conn:
        ready_rows = conn.execute(
            "SELECT * FROM group_windows WHERE status = 'ready'"
        ).fetchall()
        for gw in ready_rows:
            exists = conn.execute(
                "SELECT 1 FROM group_window_snapshots"
                " WHERE group_id = ? AND window_start = ? LIMIT 1",
                (gw["group_id"], gw["window_start"]),
            ).fetchone()
            if exists:
                continue

            members = group_member_ids(conn, gw["group_id"])
            win_rows = {
                r["device_id"]: r
                for r in conn.execute(
                    "SELECT w.* FROM windows w"
                    " JOIN device_group_members m ON m.device_id = w.device_id AND m.group_id = ?"
                    " WHERE w.window_start = ?",
                    (gw["group_id"], gw["window_start"]),
                ).fetchall()
            }
            # 数据不自洽（有成员缺窗口）时不补，避免留下不完整的基线
            if len(win_rows) != len(members):
                continue
            append_revision_snapshot(
                conn,
                gw["group_id"],
                gw["window_start"],
                gw["revision"],
                gw["value"],
                members,
                win_rows,
                now,
                kind="baseline",
            )


def group_window_out(r: sqlite3.Row) -> GroupWindowOut:
    return GroupWindowOut(
        group_id=r["group_id"],
        window_start=to_iso(r["window_start"]),
        window_end=to_iso(r["window_end"]),
        status=r["status"],
        value=r["value"],
        member_count=r["member_count"],
        sealed_member_count=r["sealed_member_count"],
        revision=r["revision"],
        ready_at=r["ready_at"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


# ---------- 接口 ----------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest(req: IngestRequest) -> IngestResponse:
    """按设备提交事件批次。批次内事件按提交顺序处理（与流式语义一致）。"""
    now = utcnow_iso()
    results: list[EventResult] = []

    with db.session() as conn:
        dev = get_or_create_device(conn, req.device_id, now)
        watermark = dev["watermark"]
        size = dev["window_size_seconds"]
        lateness = dev["allowed_lateness_seconds"]
        # 告警规则与状态在批次开始前读取一次；未配置告警的设备 None，事件不参与告警求值
        alarm = load_alarm_state(conn, req.device_id)

        for ev in req.events:
            t = to_epoch(ev.event_time)

            # 幂等去重：同一 (device_id, event_id) 只生效一次
            dup = conn.execute(
                "SELECT 1 FROM events WHERE device_id = ? AND event_id = ?",
                (req.device_id, ev.event_id),
            ).fetchone()
            if dup:
                results.append(EventResult(event_id=ev.event_id, status="duplicate"))
                continue

            w_start = window_floor(t, size)
            w_end = w_start + size
            # 所属窗口已封存（水位线已越过 window_end + 允许迟到窗口）=> 迟到数据
            is_late = watermark is not None and watermark >= w_end + lateness
            status = "late" if is_late else "accepted"

            conn.execute(
                "INSERT INTO events (device_id, event_id, event_time, value, status, ingested_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (req.device_id, ev.event_id, t, ev.value, status, now),
            )
            conn.execute(
                """
                INSERT INTO windows (device_id, window_start, window_end, count, sum, min, max,
                                     status, correction_count, late_event_count)
                VALUES (?, ?, ?, 1, ?, ?, ?, 'open', ?, ?)
                ON CONFLICT (device_id, window_start) DO UPDATE SET
                    count             = count + 1,
                    sum               = sum + excluded.sum,
                    min               = MIN(windows.min, excluded.min),
                    max               = MAX(windows.max, excluded.max),
                    correction_count  = correction_count + excluded.correction_count,
                    late_event_count  = late_event_count + excluded.late_event_count
                """,
                (
                    req.device_id, w_start, w_end, ev.value, ev.value, ev.value,
                    1 if is_late else 0,
                    1 if is_late else 0,
                ),
            )
            if is_late:
                # 迟到数据落入已封存窗口：生成修正记录
                conn.execute(
                    "INSERT INTO corrections (device_id, event_id, window_start, event_time, value, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (req.device_id, ev.event_id, w_start, t, ev.value, now),
                )

            if watermark is None or t > watermark:
                watermark = t

            # 高值告警：按事件值驱动迟滞状态机；重复事件已在上方 continue 跳过，
            # 批次重试不会再次求值、更不会重复翻转状态
            if alarm is not None:
                apply_alarm_transition(
                    conn, alarm, device_id=req.device_id,
                    event_id=ev.event_id, t=t, value=ev.value, now=now,
                )

            results.append(EventResult(event_id=ev.event_id, status=status, window_start=to_iso(w_start)))

        # 持久化水位线，并封存所有越过允许迟到窗口的窗口
        conn.execute(
            "UPDATE devices SET watermark = ? WHERE device_id = ?",
            (watermark, req.device_id),
        )
        conn.execute(
            "UPDATE windows SET status = 'sealed', sealed_at = ?"
            " WHERE device_id = ? AND status = 'open' AND window_end + ? <= ?",
            (now, req.device_id, lateness, watermark),
        )

        # 窗口封存 / 迟到修正后，同步重算该设备所在组的同步窗口汇总
        reconcile_groups_for_device(conn, req.device_id, now)

    return IngestResponse(
        device_id=req.device_id,
        watermark=to_iso(watermark),
        accepted=sum(1 for r in results if r.status == "accepted"),
        duplicates=sum(1 for r in results if r.status == "duplicate"),
        late=sum(1 for r in results if r.status == "late"),
        results=results,
    )


@app.put("/devices/{device_id}/config", response_model=DeviceOut)
def put_device_config(device_id: str, cfg: DeviceConfig) -> DeviceOut:
    """创建或更新设备配置。已有数据后不允许修改窗口大小（避免历史窗口口径不一致）。"""
    with db.session() as conn:
        dev = get_device(conn, device_id)
        if dev is None:
            conn.execute(
                "INSERT INTO devices (device_id, window_size_seconds, allowed_lateness_seconds, watermark, created_at)"
                " VALUES (?, ?, ?, NULL, ?)",
                (device_id, cfg.window_size_seconds, cfg.allowed_lateness_seconds, utcnow_iso()),
            )
        else:
            if cfg.window_size_seconds != dev["window_size_seconds"]:
                n = conn.execute(
                    "SELECT COUNT(*) AS c FROM windows WHERE device_id = ?", (device_id,)
                ).fetchone()["c"]
                if n > 0:
                    raise HTTPException(
                        status_code=409,
                        detail="window_size_seconds cannot be changed after data has been ingested",
                    )
            conn.execute(
                "UPDATE devices SET window_size_seconds = ?, allowed_lateness_seconds = ? WHERE device_id = ?",
                (cfg.window_size_seconds, cfg.allowed_lateness_seconds, device_id),
            )
        return device_out(conn, device_id)


@app.get("/devices", response_model=list[DeviceOut])
def list_devices(
    label_key: Optional[str] = Query(default=None),
    label_value: Optional[str] = Query(default=None),
) -> list[DeviceOut]:
    """列出全部设备；可按当前标签筛选。

    - `label_key` + `label_value`：当前标签精确匹配该键值对的设备；
    - 仅 `label_key`：当前标签中存在该键的设备；
    - 仅 `label_value`：422（值必须依附键筛选）；
    - 都不给：全部设备。
    筛选键名与写入时同样规范化（去空白、小写）后再匹配。
    """
    if label_key is not None and label_key.strip() == "":
        label_key = None
    if label_value is not None and label_key is None:
        raise HTTPException(
            status_code=422, detail="label_value requires label_key to be set"
        )

    sql = _DEVICE_STATS_SELECT
    params: list = []
    if label_key is not None:
        key = label_key.strip().lower()
        if label_value is not None:
            sql += (
                " WHERE EXISTS (SELECT 1 FROM device_labels l"
                " WHERE l.device_id = d.device_id AND l.label_key = ? AND l.label_value = ?)"
            )
            params.extend([key, label_value.strip()])
        else:
            sql += (
                " WHERE EXISTS (SELECT 1 FROM device_labels l"
                " WHERE l.device_id = d.device_id AND l.label_key = ?)"
            )
            params.append(key)
    sql += " ORDER BY d.device_id"

    with db.session() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [device_row_to_out(r) for r in rows]


@app.get("/devices/{device_id}", response_model=DeviceOut)
def get_device_info(device_id: str) -> DeviceOut:
    with db.session() as conn:
        return device_out(conn, device_id)


# ---------- 设备标签接口 ----------

@app.put("/devices/{device_id}/labels", response_model=LabelsOut)
def put_device_labels(device_id: str, req: LabelUpdateRequest) -> LabelsOut:
    """全量替换设备标签（不超过 20 个 key/value），版本单调递增。

    在单个 SQLite 写事务内完成：规范化键名（去空白、小写）、拒绝重复键、
    乐观并发校验（if_version 与当前版本不符返回 409 且不写入任何数据）、
    删除旧标签、写入新标签并把版本加一。
    """
    labels = normalize_labels(req.labels)  # 规范化/校验失败时不进入事务、不改数据
    now = utcnow_iso()

    with db.immediate_session() as conn:
        dev = require_device(conn, device_id)
        current_version = dev["label_version"]

        if req.if_version is not None and req.if_version != current_version:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"label version conflict: if_version={req.if_version} "
                    f"does not match current version {current_version}"
                ),
            )

        new_version = current_version + 1
        conn.execute("DELETE FROM device_labels WHERE device_id = ?", (device_id,))
        conn.executemany(
            "INSERT INTO device_labels (device_id, label_key, label_value, updated_at)"
            " VALUES (?, ?, ?, ?)",
            [(device_id, key, value, now) for key, value in labels.items()],
        )
        conn.execute(
            "UPDATE devices SET label_version = ?, labels_updated_at = ? WHERE device_id = ?",
            (new_version, now, device_id),
        )

    return LabelsOut(
        device_id=device_id, version=new_version, labels=labels, updated_at=now
    )


@app.get("/devices/{device_id}/labels", response_model=LabelsOut)
def get_device_labels_endpoint(device_id: str) -> LabelsOut:
    """返回设备当前标签与版本；从未写入标签时版本为 0、标签为空。"""
    with db.session() as conn:
        dev = require_device(conn, device_id)
        return LabelsOut(
            device_id=device_id,
            version=dev["label_version"],
            labels=get_device_labels(conn, device_id),
            updated_at=dev["labels_updated_at"],
        )


@app.get("/devices/{device_id}/windows", response_model=list[WindowOut])
def list_windows(
    device_id: str,
    status: Optional[Literal["open", "sealed"]] = None,
) -> list[WindowOut]:
    """返回设备各时间窗的聚合值、状态与修正次数。"""
    with db.session() as conn:
        require_device(conn, device_id)
        sql = "SELECT * FROM windows WHERE device_id = ?"
        params: list = [device_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [window_row_to_out(r) for r in rows]


@app.get("/devices/{device_id}/corrections", response_model=list[CorrectionOut])
def list_corrections(device_id: str) -> list[CorrectionOut]:
    """返回设备全部修正记录（窗口封存后到达的迟到数据）。"""
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM corrections WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [
            CorrectionOut(
                id=r["id"],
                device_id=r["device_id"],
                event_id=r["event_id"],
                window_start=to_iso(r["window_start"]),
                event_time=to_iso(r["event_time"]),
                value=r["value"],
                created_at=r["created_at"],
            )
            for r in rows
        ]


# ---------- 设备级高值告警接口 ----------

@app.put("/devices/{device_id}/alarm", response_model=AlarmStateOut)
def put_device_alarm(device_id: str, req: AlarmRuleIn) -> AlarmStateOut:
    """设置设备高值告警规则（触发阈值 + 较低的恢复阈值）。

    recovery_threshold 必须严格小于 trigger_threshold，且二者都必须是有限数（拒绝 NaN/Infinity）。
    校验失败返回 400 且**绝不覆盖旧规则**：旧阈值与旧运行状态（state、当前告警、最近转换）
    原样保留。每次成功写入 rule_version 加一；新规则语义改变后旧的告警状态不再适用，
    状态重置为 normal、清除当前告警首发事件（历史转换流水 append-only 保留，
    transition_count 累计不回退）。设备不存在返回 404。
    """
    trigger = req.trigger_threshold
    recovery = req.recovery_threshold
    if not math.isfinite(trigger) or not math.isfinite(recovery):
        raise HTTPException(
            status_code=400,
            detail="trigger_threshold and recovery_threshold must be finite numbers",
        )
    if recovery >= trigger:
        raise HTTPException(
            status_code=400,
            detail=(
                f"recovery_threshold ({recovery}) must be strictly lower than "
                f"trigger_threshold ({trigger}); the gap between them is the hysteresis band"
            ),
        )

    now = utcnow_iso()
    with db.session() as conn:
        require_device(conn, device_id)
        existing = conn.execute(
            "SELECT * FROM device_alarms WHERE device_id = ?", (device_id,)
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO device_alarms (device_id, trigger_threshold, recovery_threshold,"
                " state, rule_version, transition_count, created_at, updated_at)"
                " VALUES (?, ?, ?, 'normal', 1, 0, ?, ?)",
                (device_id, trigger, recovery, now, now),
            )
        else:
            # 新规则即新状态机：重置运行状态，但累计转换次数（历史长度）保留不回退
            conn.execute(
                "UPDATE device_alarms SET trigger_threshold = ?, recovery_threshold = ?,"
                " state = 'normal', rule_version = rule_version + 1,"
                " active_alarm_event_id = NULL, active_alarm_event_time = NULL,"
                " active_alarm_value = NULL, active_alarm_started_at = NULL,"
                " last_transition = NULL, last_event_id = NULL, last_event_time = NULL,"
                " last_value = NULL, last_transition_at = NULL, updated_at = ?"
                " WHERE device_id = ?",
                (trigger, recovery, now, device_id),
            )
        row = conn.execute(
            "SELECT * FROM device_alarms WHERE device_id = ?", (device_id,)
        ).fetchone()
        return alarm_row_to_out(device_id, row)


@app.get("/devices/{device_id}/alarm", response_model=AlarmStateOut)
def get_device_alarm(device_id: str) -> AlarmStateOut:
    """查询设备当前告警规则与状态。

    设备存在但从未设置规则时返回 configured=false、阈值为 null、state=normal、rule_version=0；
    设备不存在返回 404。state=alarm 时 active_alarm 给出**首次**越过触发线的事件
    （之后的高值不覆盖），解除后为 null。
    """
    with db.session() as conn:
        require_device(conn, device_id)
        row = conn.execute(
            "SELECT * FROM device_alarms WHERE device_id = ?", (device_id,)
        ).fetchone()
        return alarm_row_to_out(device_id, row)


@app.get("/devices/{device_id}/alarm/events", response_model=list[AlarmTransitionOut])
def list_device_alarm_events(device_id: str) -> list[AlarmTransitionOut]:
    """返回设备告警状态转换流水（append-only），按发生顺序升序。

    仅包含真正的状态翻转（fired/cleared），每条记录转换时生效的阈值与规则版本；
    重复上报与批次重试不会产生重复流水。设备不存在返回 404，从未配置告警返回 []。
    """
    with db.session() as conn:
        require_device(conn, device_id)
        rows = conn.execute(
            "SELECT * FROM device_alarm_events WHERE device_id = ? ORDER BY id",
            (device_id,),
        ).fetchall()
        return [
            AlarmTransitionOut(
                id=r["id"],
                device_id=r["device_id"],
                event_id=r["event_id"],
                event_time=to_iso(r["event_time"]),
                value=r["value"],
                transition=r["transition"],
                trigger_threshold=r["trigger_threshold"],
                recovery_threshold=r["recovery_threshold"],
                rule_version=r["rule_version"],
                created_at=r["created_at"],
            )
            for r in rows
        ]


# ---------- 跨设备窗口检索接口 ----------
@app.get("/windows/search", response_model=WindowSearchResponse)
def search_windows(
    start: Optional[datetime] = Query(
        default=None, description="窗口时间范围下界（对 window_start，含），ISO 8601"
    ),
    end: Optional[datetime] = Query(
        default=None, description="窗口时间范围上界（对 window_start，不含），ISO 8601"
    ),
    status: Optional[Literal["open", "sealed"]] = Query(
        default=None, description="窗口状态精确筛选"
    ),
    label_key: Optional[str] = Query(default=None, description="设备当前标签键（规范化后匹配）"),
    label_value: Optional[str] = Query(default=None, description="设备当前标签值（与 label_key 同时给出时按键值精确匹配）"),
    limit: int = Query(default=SEARCH_DEFAULT_LIMIT, ge=1, le=SEARCH_MAX_LIMIT),
    cursor: Optional[str] = Query(default=None, description="上一页返回的 next_cursor；只可用于相同筛选条件"),
) -> WindowSearchResponse:
    """跨设备检索窗口（稳定游标分页，只读）。

    按窗口时间范围（对 window_start 的半开区间 [start, end)）、open/sealed 状态及设备
    **当前**标签精确筛选，按 window_start、device_id 升序返回窗口聚合与设备标识。
    设备标签筛选语义与 GET /devices 一致：label_key+label_value 精确匹配键值对、
    仅 label_key 匹配含该键的设备、只给 label_value 返回 422。

    分页游标与本次筛选条件绑定并签名：翻页期间新增的窗口不会进入本次遍历的行快照，
    既不会在后续页重复出现，也不会把原有行挤出本页或插入到已遍历区间造成遗漏；
    需要看新增数据时不带 cursor 重新发起检索即可。游标非法、篡改、过期或与当前筛选
    条件不一致均返回 400。
    """
    if label_key is not None and label_key.strip() == "":
        label_key = None
    if label_value is not None and label_key is None:
        raise HTTPException(status_code=422, detail="label_value requires label_key to be set")

    start_epoch = to_epoch(start) if start is not None else None
    end_epoch = to_epoch(end) if end is not None else None
    if start_epoch is not None and end_epoch is not None and end_epoch <= start_epoch:
        raise HTTPException(
            status_code=422, detail="end must be strictly greater than start"
        )

    key = label_key.strip().lower() if label_key is not None else None
    value = label_value.strip() if label_value is not None else None
    # 游标绑定的规范化筛选条件（JSON 可序列化、与请求解码后的形态比较）
    filters: dict[str, Any] = {
        "start": start_epoch,
        "end": end_epoch,
        "status": status,
        "label_key": key,
        "label_value": value,
    }

    with db.session() as conn:
        if cursor is not None:
            cur = decode_search_cursor(cursor, filters)
            anchor_ws = cur["window_start"]
            anchor_device = cur["device_id"]
            snapshot_max_rowid = cur["snapshot_max_rowid"]
        else:
            anchor_ws = None
            anchor_device = None
            # 首次检索固定行快照上界：本事务内 rowid 大于该值的行（翻页期间新增）一律忽略，
            # 保证后续页与首页遍历的是同一份稳定集合
            row = conn.execute("SELECT COALESCE(MAX(rowid), 0) AS m FROM windows").fetchone()
            snapshot_max_rowid = row["m"]

        sql = "SELECT w.* FROM windows w"
        # JOIN 占位符在语句文本上先于 WHERE，参数分两组按出现顺序组装
        join_params: list[Any] = []
        clauses = ["w.rowid <= ?"]
        where_params: list[Any] = [snapshot_max_rowid]
        if start_epoch is not None:
            clauses.append("w.window_start >= ?")
            where_params.append(start_epoch)
        if end_epoch is not None:
            clauses.append("w.window_start < ?")
            where_params.append(end_epoch)
        if status is not None:
            clauses.append("w.status = ?")
            where_params.append(status)
        if key is not None:
            # device_labels 主键 (device_id, label_key) 保证每个设备至多匹配一行，JOIN 不会产生重复窗口
            if value is not None:
                sql += (
                    " JOIN device_labels dl ON dl.device_id = w.device_id"
                    " AND dl.label_key = ? AND dl.label_value = ?"
                )
                join_params.extend([key, value])
            else:
                sql += (
                    " JOIN device_labels dl ON dl.device_id = w.device_id"
                    " AND dl.label_key = ?"
                )
                join_params.append(key)
        if anchor_ws is not None:
            # 键集翻页：(window_start, device_id) 严格大于游标锚点
            clauses.append(
                "(w.window_start > ? OR (w.window_start = ? AND w.device_id > ?))"
            )
            where_params.extend([anchor_ws, anchor_ws, anchor_device])

        sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY w.window_start, w.device_id LIMIT ?"

        rows = conn.execute(sql, [*join_params, *where_params, limit + 1]).fetchall()
        has_more = len(rows) > limit
        page = rows[:limit]
        windows = [window_row_to_out(r) for r in page]

        next_cursor: Optional[str] = None
        if has_more:
            last = page[-1]
            next_cursor = encode_search_cursor(
                last["window_start"],
                last["device_id"],
                snapshot_max_rowid,
                filters,
            )

        return WindowSearchResponse(windows=windows, next_cursor=next_cursor)


# ---------- 封存窗口稳健异常检测接口 ----------

def _median(values: list[float]) -> float:
    """有限样本中位数：偶数个时取中间两个的平均值。"""
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


@app.get("/devices/{device_id}/anomalies", response_model=list[AnomalyOut])
def list_device_anomalies(
    device_id: str,
    window_start: datetime = Query(..., description="窗口起始时间（ISO 8601），按设备窗口大小对齐"),
    threshold: float = Query(default=DEFAULT_ANOMALY_THRESHOLD, gt=0),
) -> list[AnomalyOut]:
    """对指定**已封存**窗口内的事件做中位数/MAD 稳健异常检测（只读）。

    score = 0.6745 * |value - median| / MAD（修正 z 分数），仅返回 score > threshold 的事件，
    threshold 默认 3.5 且必须 > 0，否则返回 422。MAD 为 0 时除以 0 无定义，
    按需求把所有不等于中位数的事件一律判为异常（score 返回 null）。
    检测包含封存后到达的迟到事件；纯只读，不改变窗口聚合与修正计数。
    设备或窗口不存在返回 404，窗口尚未封存（open）返回 409。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        require_device(conn, device_id)
        win = conn.execute(
            "SELECT * FROM windows WHERE device_id = ? AND window_start = ?",
            (device_id, w_start),
        ).fetchone()
        if win is None:
            raise HTTPException(
                status_code=404,
                detail=f"window not found for device {device_id} at window_start {window_start.isoformat()}",
            )
        if win["status"] != "sealed":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"window at {window_start.isoformat()} is not sealed; "
                    "anomaly detection is only available on sealed windows"
                ),
            )

        rows = conn.execute(
            "SELECT event_id, event_time, value FROM events"
            " WHERE device_id = ? AND event_time >= ? AND event_time < ?"
            " ORDER BY event_time, event_id",
            (device_id, win["window_start"], win["window_end"]),
        ).fetchall()

        values = [r["value"] for r in rows]
        if not values:
            return []

        median = _median(values)
        mad = _median([abs(v - median) for v in values])

        anomalies: list[AnomalyOut] = []
        if mad == 0.0:
            # MAD 为 0：除以 0 无定义，所有非中位值按异常处理；全等于中位数则无异常
            for r in rows:
                if r["value"] != median:
                    anomalies.append(AnomalyOut(
                        event_id=r["event_id"],
                        event_time=to_iso(r["event_time"]),
                        value=r["value"],
                        score=None,
                    ))
            return anomalies

        for r in rows:
            score = MAD_NORMALIZER * abs(r["value"] - median) / mad
            if score > threshold:
                anomalies.append(AnomalyOut(
                    event_id=r["event_id"],
                    event_time=to_iso(r["event_time"]),
                    value=r["value"],
                    score=score,
                ))
        return anomalies


# ---------- 设备组接口 ----------

@app.post("/groups", response_model=GroupOut, status_code=201)
def create_group(req: GroupCreate) -> GroupOut:
    """创建固定成员设备组。

    成员设备必须均已存在，且窗口配置（window_size_seconds、allowed_lateness_seconds）
    与请求一致，保证同一 window_start 的成员窗口对齐、封存口径一致。
    """
    now = utcnow_iso()
    member_ids = req.member_ids
    if len(set(member_ids)) != len(member_ids):
        raise HTTPException(status_code=400, detail="member_ids must not contain duplicates")

    with db.session() as conn:
        if conn.execute(
            "SELECT 1 FROM device_groups WHERE group_id = ?", (req.group_id,)
        ).fetchone():
            raise HTTPException(status_code=409, detail=f"group already exists: {req.group_id}")

        missing: list[str] = []
        mismatched: list[str] = []
        for device_id in member_ids:
            dev = get_device(conn, device_id)
            if dev is None:
                missing.append(device_id)
            elif (
                dev["window_size_seconds"] != req.window_size_seconds
                or dev["allowed_lateness_seconds"] != req.allowed_lateness_seconds
            ):
                mismatched.append(device_id)
        if missing:
            raise HTTPException(
                status_code=404, detail=f"devices not found: {', '.join(missing)}"
            )
        if mismatched:
            raise HTTPException(
                status_code=409,
                detail=(
                    "devices with window config different from group config: "
                    + ", ".join(mismatched)
                ),
            )

        conn.execute(
            "INSERT INTO device_groups (group_id, window_size_seconds, allowed_lateness_seconds,"
            " aggregation, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                req.group_id,
                req.window_size_seconds,
                req.allowed_lateness_seconds,
                req.aggregation,
                now,
            ),
        )
        conn.executemany(
            "INSERT INTO device_group_members (group_id, device_id, position) VALUES (?, ?, ?)",
            [(req.group_id, device_id, i) for i, device_id in enumerate(member_ids)],
        )

        # 若成员已有封存窗口，组创建后立即对账补算
        reconcile_group(conn, req.group_id, now)

        return GroupOut(
            group_id=req.group_id,
            member_ids=member_ids,
            aggregation=req.aggregation,
            window_size_seconds=req.window_size_seconds,
            allowed_lateness_seconds=req.allowed_lateness_seconds,
            created_at=now,
        )


@app.get("/groups/{group_id}", response_model=GroupOut)
def get_group(group_id: str) -> GroupOut:
    with db.session() as conn:
        group = require_group(conn, group_id)
        return GroupOut(
            group_id=group["group_id"],
            member_ids=group_member_ids(conn, group_id),
            aggregation=group["aggregation"],
            window_size_seconds=group["window_size_seconds"],
            allowed_lateness_seconds=group["allowed_lateness_seconds"],
            created_at=group["created_at"],
        )


@app.get("/groups/{group_id}/windows", response_model=list[GroupWindowOut])
def list_group_windows(
    group_id: str,
    status: Optional[Literal["pending", "ready"]] = None,
    window_start: Optional[datetime] = None,
) -> list[GroupWindowOut]:
    """按窗口查询组汇总，可按 status / window_start 过滤。pending 窗口 value 为 null。"""
    with db.session() as conn:
        require_group(conn, group_id)
        sql = "SELECT * FROM group_windows WHERE group_id = ?"
        params: list = [group_id]
        if status:
            sql += " AND status = ?"
            params.append(status)
        if window_start is not None:
            sql += " AND window_start = ?"
            params.append(to_epoch(window_start))
        sql += " ORDER BY window_start"
        rows = conn.execute(sql, params).fetchall()
        return [group_window_out(r) for r in rows]


@app.get("/groups/{group_id}/windows/{window_start}", response_model=GroupWindowOut)
def get_group_window(group_id: str, window_start: datetime) -> GroupWindowOut:
    """查询组在单个 window_start 上的汇总（pending 或 ready）。"""
    with db.session() as conn:
        require_group(conn, group_id)
        row = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, to_epoch(window_start)),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        return group_window_out(row)


@app.get(
    "/groups/{group_id}/windows/{window_start}/pending",
    response_model=list[PendingMemberOut],
)
def list_pending_members(group_id: str, window_start: datetime) -> list[PendingMemberOut]:
    """返回该组窗口尚未封存的成员明细。

    - 组窗口已 ready：返回空列表（无缺失成员）。
    - 组窗口尚不存在（成员均无数据）：404。
    - 成员在该 window_start 没有窗口：reason=missing；窗口仍 open：reason=open。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        group = require_group(conn, group_id)
        gw = conn.execute(
            "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
            (group_id, w_start),
        ).fetchone()
        if gw is None:
            raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
        if gw["status"] == "ready":
            return []

        w_end = w_start + group["window_size_seconds"]
        members = group_member_ids(conn, group_id)
        placeholders_sql = placeholders(len(members))
        win_rows = {
            r["device_id"]: r
            for r in conn.execute(
                f"SELECT * FROM windows WHERE window_start = ? AND device_id IN ({placeholders_sql})",
                [w_start, *members],
            ).fetchall()
        }

        result: list[PendingMemberOut] = []
        for device_id in members:
            w = win_rows.get(device_id)
            if w is None:
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=None,
                    window_end=to_iso(w_end),
                    window_status=None,
                    reason="missing",
                ))
            elif w["status"] != "sealed":
                result.append(PendingMemberOut(
                    device_id=device_id,
                    window_start=to_iso(w["window_start"]),
                    window_end=to_iso(w["window_end"]),
                    window_status=w["status"],
                    reason="open",
                ))
        return result


# ---------- 组窗口修订审计接口 ----------

def _require_group_window(conn: sqlite3.Connection, group_id: str, w_start: float) -> sqlite3.Row:
    require_group(conn, group_id)
    gw = conn.execute(
        "SELECT * FROM group_windows WHERE group_id = ? AND window_start = ?",
        (group_id, w_start),
    ).fetchone()
    if gw is None:
        raise HTTPException(status_code=404, detail="group window not found (no member has data yet)")
    return gw


def _require_snapshot(
    conn: sqlite3.Connection, group_id: str, w_start: float, revision: int
) -> sqlite3.Row:
    snap = conn.execute(
        "SELECT * FROM group_window_snapshots WHERE group_id = ? AND window_start = ? AND revision = ?",
        (group_id, w_start, revision),
    ).fetchone()
    if snap is None:
        raise HTTPException(
            status_code=404, detail=f"revision not found: {revision}"
        )
    return snap


def _snapshot_member_states(
    conn: sqlite3.Connection, aggregation: str, snapshot_id: int
) -> dict[str, dict]:
    """读取某份快照的成员明细，并算出各成员当时对 group value 的贡献。"""
    rows = conn.execute(
        "SELECT * FROM group_window_snapshot_members WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    states: dict[str, dict] = {}
    for r in rows:
        contribution = r["sum"] if aggregation == "sum" else (r["avg"] if r["avg"] is not None else 0.0)
        states[r["device_id"]] = {
            "device_id": r["device_id"],
            "position": r["position"],
            "count": r["count"],
            "sum": r["sum"],
            "avg": r["avg"],
            "correction_count": r["correction_count"],
            "contribution": contribution,
        }
    return states


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions",
    response_model=list[GroupWindowRevisionOut],
)
def list_group_window_revisions(group_id: str, window_start: datetime) -> list[GroupWindowRevisionOut]:
    """返回组窗口的修订审计列表（不可变快照），按 revision 升序。

    组窗口存在但尚未 ready（无任何修订）时返回空列表；组或窗口不存在返回 404。
    """
    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        rows = conn.execute(
            "SELECT s.*, ("
            "  SELECT COUNT(*) FROM group_window_snapshot_members m WHERE m.snapshot_id = s.id"
            " ) AS member_count"
            " FROM group_window_snapshots s"
            " WHERE s.group_id = ? AND s.window_start = ?"
            " ORDER BY s.revision",
            (group_id, w_start),
        ).fetchall()
        return [
            GroupWindowRevisionOut(
                revision=r["revision"],
                value=r["value"],
                kind=r["kind"],
                created_at=r["created_at"],
                member_count=r["member_count"],
            )
            for r in rows
        ]


@app.get(
    "/groups/{group_id}/windows/{window_start}/revisions/diff",
    response_model=RevisionDiffOut,
)
def diff_group_window_revisions(
    group_id: str,
    window_start: datetime,
    from_revision: int = Query(..., ge=1),
    to_revision: int = Query(..., ge=1),
) -> RevisionDiffOut:
    """对比组窗口两个 revision 的不可变快照。

    按成员顺序返回每个发生变化成员的前后贡献与各数值差（count/sum/avg/correction_count），
    仅列变化成员。from/to 必须为 >=1 且 from < to，否则 422；
    组、窗口或任一 revision 不存在返回 404。
    """
    if from_revision >= to_revision:
        raise HTTPException(
            status_code=422, detail="from_revision must be a positive revision smaller than to_revision"
        )

    w_start = to_epoch(window_start)
    with db.session() as conn:
        _require_group_window(conn, group_id, w_start)
        group = conn.execute(
            "SELECT aggregation FROM device_groups WHERE group_id = ?", (group_id,)
        ).fetchone()
        snap_from = _require_snapshot(conn, group_id, w_start, from_revision)
        snap_to = _require_snapshot(conn, group_id, w_start, to_revision)

        before_states = _snapshot_member_states(conn, group["aggregation"], snap_from["id"])
        after_states = _snapshot_member_states(conn, group["aggregation"], snap_to["id"])

        member_deltas: list[RevisionMemberDeltaOut] = []
        for device_id in sorted(set(before_states) | set(after_states),
                                key=lambda d: after_states.get(d, before_states[d])["position"]):
            b = before_states.get(device_id)
            a = after_states.get(device_id)
            if b is None or a is None or (
                b["count"], b["sum"], b["avg"], b["correction_count"]
            ) != (
                a["count"], a["sum"], a["avg"], a["correction_count"]
            ):
                # 固定成员组两侧成员集合必然相同；任一缺失视为变化（防御性处理）
                b = b or a
                a = a or b
                member_deltas.append(RevisionMemberDeltaOut(
                    device_id=device_id,
                    position=a["position"],
                    before=RevisionMemberStateOut(**b),
                    after=RevisionMemberStateOut(**a),
                    count_delta=a["count"] - b["count"],
                    sum_delta=a["sum"] - b["sum"],
                    avg_delta=(
                        a["avg"] - b["avg"]
                        if a["avg"] is not None and b["avg"] is not None
                        else None
                    ),
                    correction_count_delta=a["correction_count"] - b["correction_count"],
                    contribution_delta=a["contribution"] - b["contribution"],
                ))

        return RevisionDiffOut(
            group_id=group_id,
            window_start=to_iso(w_start),
            aggregation=group["aggregation"],
            from_revision=from_revision,
            to_revision=to_revision,
            from_value=snap_from["value"],
            to_value=snap_to["value"],
            value_delta=snap_to["value"] - snap_from["value"],
            members=member_deltas,
        )
