"""SQLite 数据访问层。

数据库路径通过环境变量 TELEMETRY_DB_PATH 配置，默认 ./telemetry.db。
每次连接时读取环境变量，便于测试隔离与容器挂载持久化。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id                TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL DEFAULT 60,
    allowed_lateness_seconds INTEGER NOT NULL DEFAULT 30,
    watermark                REAL,          -- 已见最大 event_time（epoch 秒），持久化水位线
    label_version            INTEGER NOT NULL DEFAULT 0,  -- 标签集版本：每次成功 PUT +1
    labels_updated_at        TEXT,          -- 最近一次标签写入时间（从未写入为 NULL）
    created_at               TEXT NOT NULL
);

-- 设备当前标签（仅保存最新版本的键值；旧版本随版本号被覆盖替换，不保留历史）
CREATE TABLE IF NOT EXISTS device_labels (
    device_id   TEXT NOT NULL,
    label_key   TEXT NOT NULL,               -- 规范化后的键名（去首尾空白、小写）
    label_value TEXT NOT NULL,               -- 去首尾空白后的值
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (device_id, label_key),      -- 规范化后重复键在同事务替换，不会落两行
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

CREATE INDEX IF NOT EXISTS idx_device_labels_key_value
    ON device_labels (label_key, label_value);

CREATE TABLE IF NOT EXISTS events (
    device_id   TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    event_time  REAL NOT NULL,              -- epoch 秒
    value       REAL NOT NULL,
    status      TEXT NOT NULL,              -- accepted | late
    ingested_at TEXT NOT NULL,
    PRIMARY KEY (device_id, event_id)       -- 幂等去重约束
);

CREATE TABLE IF NOT EXISTS windows (
    device_id         TEXT NOT NULL,
    window_start      REAL NOT NULL,
    window_end        REAL NOT NULL,
    count             INTEGER NOT NULL DEFAULT 0,
    sum               REAL NOT NULL DEFAULT 0,
    min               REAL,
    max               REAL,
    status            TEXT NOT NULL DEFAULT 'open',   -- open | sealed
    correction_count  INTEGER NOT NULL DEFAULT 0,
    late_event_count  INTEGER NOT NULL DEFAULT 0,
    sealed_at         TEXT,
    PRIMARY KEY (device_id, window_start)
);

CREATE TABLE IF NOT EXISTS corrections (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    window_start REAL NOT NULL,
    event_time   REAL NOT NULL,
    value        REAL NOT NULL,
    created_at   TEXT NOT NULL
);

-- 设备组：固定成员，成员设备的窗口配置（大小/允许迟到）必须一致
CREATE TABLE IF NOT EXISTS device_groups (
    group_id                 TEXT PRIMARY KEY,
    window_size_seconds      INTEGER NOT NULL,
    allowed_lateness_seconds INTEGER NOT NULL,
    aggregation              TEXT NOT NULL CHECK (aggregation IN ('sum', 'avg')),
    created_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_group_members (
    group_id  TEXT NOT NULL,
    device_id TEXT NOT NULL,
    position  INTEGER NOT NULL,              -- 保留创建时的成员顺序
    PRIMARY KEY (group_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_device
    ON device_group_members (device_id);

-- 组同步窗口汇总：pending（有成员窗口未封存）→ ready（全部封存）
CREATE TABLE IF NOT EXISTS group_windows (
    group_id            TEXT NOT NULL,
    window_start        REAL NOT NULL,
    window_end          REAL NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',  -- pending | ready
    value               REAL,                              -- pending 时为 NULL，禁止部分成员提前计算
    member_count        INTEGER NOT NULL,                 -- 固定成员总数
    sealed_member_count INTEGER NOT NULL DEFAULT 0,
    revision            INTEGER NOT NULL DEFAULT 0,       -- ready 后从 1 起，每次成员修正原位 +1
    correction_sig      INTEGER NOT NULL DEFAULT 0,       -- Σ 成员窗口 correction_count，用于判定是否升版
    ready_at            TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (group_id, window_start)
);

-- 组窗口修订审计：组窗口首次 ready、以及迟到修正令 revision 升版时，
-- 在同一事务追加一份不可变快照（append-only），后续修正只能追加新版本、不得覆盖旧快照。
CREATE TABLE IF NOT EXISTS group_window_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id     TEXT NOT NULL,
    window_start REAL NOT NULL,
    revision     INTEGER NOT NULL,                -- 对应 group_windows.revision
    value        REAL NOT NULL,                   -- 该修订当时的组汇总值
    kind         TEXT NOT NULL DEFAULT 'revision'
                 CHECK (kind IN ('revision', 'baseline')),  -- baseline=旧库启动时按当前版本补的基线
    created_at   TEXT NOT NULL,
    UNIQUE (group_id, window_start, revision)     -- 重复上报不得重复留档
);

CREATE TABLE IF NOT EXISTS group_window_snapshot_members (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id      INTEGER NOT NULL,
    group_id         TEXT NOT NULL,
    device_id        TEXT NOT NULL,
    position         INTEGER NOT NULL,            -- 固定成员的创建顺序
    count            INTEGER NOT NULL,            -- 成员窗口当时的 count
    sum              REAL NOT NULL,               -- 成员窗口当时的 sum
    avg              REAL,                        -- 成员窗口当时的 avg（sum/count）
    correction_count INTEGER NOT NULL,            -- 成员窗口当时的 correction_count
    UNIQUE (snapshot_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_gws_members_snapshot
    ON group_window_snapshot_members (snapshot_id, position);

-- 审计表只允许 INSERT：任何 UPDATE/DELETE 直接中止，保证历史快照不可变。
-- 设备级高值告警：规则（触发/恢复阈值）与运行状态存于同一行，随规则写入与状态转换持久化。
-- recovery_threshold 必须严格小于 trigger_threshold，二者之间为迟滞区，落入该区保持原状态。
CREATE TABLE IF NOT EXISTS device_alarms (
    device_id               TEXT PRIMARY KEY,
    trigger_threshold       REAL NOT NULL,
    recovery_threshold      REAL NOT NULL,
    state                   TEXT NOT NULL DEFAULT 'normal'
                            CHECK (state IN ('normal', 'alarm')),
    rule_version            INTEGER NOT NULL DEFAULT 1,   -- 每次成功写入规则 +1
    transition_count        INTEGER NOT NULL DEFAULT 0,   -- 历史状态转换总次数（仅追加、不回退）
    active_alarm_event_id   TEXT,                         -- 当前告警首发事件（state=alarm 时非空）
    active_alarm_event_time REAL,
    active_alarm_value      REAL,
    active_alarm_started_at TEXT,
    last_transition         TEXT,                         -- fired | cleared；规则重置后为 NULL
    last_event_id           TEXT,
    last_event_time         REAL,
    last_value              REAL,
    last_transition_at      TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    CHECK (recovery_threshold < trigger_threshold),
    FOREIGN KEY (device_id) REFERENCES devices (device_id)
);

-- 告警状态转换流水：仅在状态真正翻转（normal→alarm / alarm→normal）的同一事务追加一条，
-- 记录转换时生效的阈值与规则版本，append-only、永不更新或删除。
CREATE TABLE IF NOT EXISTS device_alarm_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id          TEXT NOT NULL,
    event_id           TEXT NOT NULL,
    event_time         REAL NOT NULL,
    value              REAL NOT NULL,
    transition         TEXT NOT NULL CHECK (transition IN ('fired', 'cleared')),
    trigger_threshold  REAL NOT NULL,
    recovery_threshold REAL NOT NULL,
    rule_version       INTEGER NOT NULL,
    created_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_device_alarm_events_device
    ON device_alarm_events (device_id, id);

-- 告警流水只允许 INSERT：任何 UPDATE/DELETE 直接中止，保证转换历史不可变。
CREATE TRIGGER IF NOT EXISTS trg_dae_no_update
BEFORE UPDATE ON device_alarm_events
BEGIN
    SELECT RAISE(ABORT, 'device_alarm_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_dae_no_delete
BEFORE DELETE ON device_alarm_events
BEGIN
    SELECT RAISE(ABORT, 'device_alarm_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gws_no_update
BEFORE UPDATE ON group_window_snapshots
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshots is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gws_no_delete
BEFORE DELETE ON group_window_snapshots
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshots is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gwsm_no_update
BEFORE UPDATE ON group_window_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshot_members is append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_gwsm_no_delete
BEFORE DELETE ON group_window_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'group_window_snapshot_members is append-only');
END;
"""


def db_path() -> str:
    return os.environ.get("TELEMETRY_DB_PATH", "telemetry.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """旧库就地迁移（幂等）：为 devices 表补标签版本列。

    CREATE TABLE IF NOT EXISTS 不会给已存在的旧表加列，这里按 PRAGMA 检测结果补。
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(devices)").fetchall()}
    if "label_version" not in cols:
        conn.execute(
            "ALTER TABLE devices ADD COLUMN label_version INTEGER NOT NULL DEFAULT 0"
        )
    if "labels_updated_at" not in cols:
        conn.execute("ALTER TABLE devices ADD COLUMN labels_updated_at TEXT")


@contextmanager
def session() -> Iterator[sqlite3.Connection]:
    """事务会话：正常退出提交，异常回滚，始终关闭连接。"""
    conn = connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


@contextmanager
def immediate_session() -> Iterator[sqlite3.Connection]:
    """立即加写锁的事务会话：进入即 BEGIN IMMEDIATE，正常退出提交，异常回滚。

    用于"读当前版本 → 校验 → 写入"的乐观并发场景：写锁保证同一时刻只有一个
    写事务，读到的版本在事务内不会被其他写者改变。与 Python 3.12 之前的
    sqlite3 隐式 BEGIN 行为兼容（这里显式 BEGIN，不依赖 autocommit 语义）。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
    finally:
        conn.close()
