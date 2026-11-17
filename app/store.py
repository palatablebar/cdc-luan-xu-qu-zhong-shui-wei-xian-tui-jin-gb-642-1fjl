"""SQLite 存储：批次、输入文件、事件、快照、水位序列、阶段、合计、运行清单。

约定：

- 金额列一律 TEXT（Decimal 字符串），不落 float；主键 pk 一律 TEXT，前导零与
  超过 2^53 的整数都按原样保存；
- 一次运行的所有行在同一个事务里「先删后插」，失败整体回滚，不留半批数据；
- 页面与导出只认 baseline 表里的发布基线（active 批次），重算期间基线不动，
  因此页面与导出永远是同一份完整版本；重算失败时基线继续指向旧版，不丢数据；
- 重算过程只在 run_attempts 留运行痕迹（running/failed），不写业务结果行，
  同一批次键的重试复用同一 attempt 行，不新增、不重复累计。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    watermark_utc TEXT,
    max_event_time TEXT,
    params_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT '',
    replay_of TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_key ON batches(batch_key, status);
CREATE TABLE IF NOT EXISTS inputs (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, name TEXT NOT NULL,
    bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, declared_rows INTEGER,
    encoding TEXT, declared TEXT, kind TEXT, status TEXT NOT NULL, code TEXT,
    note TEXT, lines INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    batch_id TEXT NOT NULL, row_ref TEXT NOT NULL, seq INTEGER NOT NULL,
    table_name TEXT NOT NULL, pk TEXT NOT NULL, version INTEGER,
    op TEXT, event_time_utc TEXT, event_time_raw TEXT, schema_version TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}', amount TEXT, qty INTEGER,
    row_class TEXT NOT NULL, reason_code TEXT, field TEXT, raw_value TEXT,
    detail TEXT, late INTEGER NOT NULL DEFAULT 0, flags_json TEXT NOT NULL DEFAULT '[]',
    raw_text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_class ON events(batch_id, row_class);
CREATE TABLE IF NOT EXISTS snapshots (
    batch_id TEXT NOT NULL, table_name TEXT NOT NULL, pk TEXT NOT NULL,
    version INTEGER NOT NULL, op TEXT NOT NULL, amount TEXT, qty INTEGER,
    event_time_utc TEXT NOT NULL, row_ref TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS watermarks (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, row_ref TEXT NOT NULL,
    event_time_utc TEXT NOT NULL, watermark_utc TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, behind_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stages (
    batch_id TEXT NOT NULL, stage TEXT NOT NULL, in_count INTEGER NOT NULL,
    out_count INTEGER NOT NULL, quarantine_count INTEGER NOT NULL,
    status TEXT NOT NULL, elapsed_ms INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS totals (
    batch_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    batch_id TEXT PRIMARY KEY, json TEXT NOT NULL
);
-- 发布基线：单行（id 恒为 1）。只有 status='done' 的完整批次才能成为基线，
-- 切换与结果落库在同一事务内完成，读路径（页面/导出）只认这一行。
CREATE TABLE IF NOT EXISTS baseline (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    batch_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);
-- 重算尝试：运行痕迹与业务结果分离。同一 batch_key 的多次重试复用同一行，
-- running 时唯一，用于并发拦截；failed 只留审计，绝不触碰基线与结果表。
CREATE TABLE IF NOT EXISTS run_attempts (
    batch_key TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 1,
    last_error TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT ''
);
"""


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_columns(conn)

    @staticmethod
    def _ensure_columns(conn) -> None:
        """轻量迁移：老库缺列时补上，避免升级后 500。"""
        wanted = {"inputs": {"declared": "TEXT"}}
        for table, columns in wanted.items():
            existing = {row["name"] for row in conn.execute(
                f"PRAGMA table_info({table})").fetchall()}
            for column, ddl in columns.items():
                if column not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 批次 ----------
    def get_batch(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        return rows[0] if rows else None

    def find_done_by_key(self, batch_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM batches WHERE batch_key=? AND status='done' "
            "ORDER BY created_at LIMIT 1", (batch_key,))
        return rows[0] if rows else None

    def latest_batch(self, status: str = "done") -> dict | None:
        """最新批次。注意：这是历史视角，读路径请用 active_batch()。

        只返回指定状态的批次（默认 done）；没有完成批次时返回 None，
        绝不把 running/failed 的半成品批次暴露给页面与导出。
        """
        rows = self._all(
            "SELECT * FROM batches WHERE status=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (status,))
        return rows[0] if rows else None

    # ---------- 发布基线（页面与导出共用的唯一基准版本） ----------
    def active_batch(self) -> dict | None:
        """当前对外基线：baseline 指向的已完成批次。

        老库（升级前已落库、还没有 baseline 行）回退到最新的 done 批次，
        保证升级瞬间页面不空；首次有新批次落库时会在同一事务里写入基线。
        """
        rows = self._all(
            """SELECT b.* FROM baseline bl
               JOIN batches b ON b.batch_id = bl.batch_id
               WHERE bl.id = 1 AND b.status = 'done'""", ())
        if rows:
            return rows[0]
        return self.latest_batch("done")

    def active_batch_id(self) -> str | None:
        batch = self.active_batch()
        return batch["batch_id"] if batch else None

    def baseline_row(self) -> dict | None:
        rows = self._all("SELECT * FROM baseline WHERE id = 1", ())
        return rows[0] if rows else None

    def publish_baseline(self, conn, batch_id: str, published_at: str) -> None:
        """在调用方事务内把基线指向新完成批次（单点原子切换）。"""
        conn.execute(
            """INSERT INTO baseline (id, batch_id, published_at) VALUES (1, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 batch_id = excluded.batch_id,
                 published_at = excluded.published_at""",
            (batch_id, published_at))

    # ---------- 重算尝试（运行痕迹，不碰业务结果） ----------
    def start_attempt(self, *, batch_key: str, batch_id: str, run_id: str,
                      started_at: str, note: str = "") -> dict:
        """登记一次重算尝试。

        同一 batch_key 已有 running 尝试 → 409，拒绝并发重算；
        已 failed 的同键重试：attempts + 1、状态回 running，不新增行。
        """
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM run_attempts WHERE batch_key=?", (batch_key,)
            ).fetchone()
            if row is not None and row["status"] == "running":
                # 自愈：结果已落库但尝试标记没来得及收尾（进程崩溃），
                # 视为上一轮已完成，允许新一轮登记，而不是死锁在 running。
                done = conn.execute(
                    "SELECT 1 FROM batches WHERE batch_id=? AND status='done'",
                    (row["batch_id"],)).fetchone()
                if done is not None:
                    conn.execute(
                        "UPDATE run_attempts SET status='done', finished_at=? "
                        "WHERE batch_key=?", (started_at, batch_key))
                    row = None
            if row is not None and row["status"] == "running":
                from .errors import Conflict
                raise Conflict(
                    f"批次键 {batch_key} 已有运行中的重算（{row['run_id']}），"
                    "请等其结束后再试",
                    code="B_RERUN_IN_PROGRESS",
                    detail={"batch_key": batch_key, "run_id": row["run_id"]})
            if row is None:
                conn.execute(
                    """INSERT INTO run_attempts (batch_key,batch_id,run_id,status,
                          started_at,attempts,note)
                       VALUES (?,?,?,'running',?,1,?)""",
                    (batch_key, batch_id, run_id, started_at, note))
                attempts = 1
            else:
                attempts = row["attempts"] + 1
                conn.execute(
                    """UPDATE run_attempts SET batch_id=?, run_id=?, status='running',
                          started_at=?, finished_at=NULL, attempts=?,
                          last_error='', note=? WHERE batch_key=?""",
                    (batch_id, run_id, started_at, attempts, note, batch_key))
        return {"batch_key": batch_key, "batch_id": batch_id, "run_id": run_id,
                "attempts": attempts}

    def finish_attempt(self, *, batch_key: str, status: str, finished_at: str,
                       error: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                """UPDATE run_attempts SET status=?, finished_at=?, last_error=?
                   WHERE batch_key=?""",
                (status, finished_at, error, batch_key))

    def get_attempt(self, batch_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM run_attempts WHERE batch_key=?", (batch_key,))
        return rows[0] if rows else None

    def running_attempts(self) -> list[dict]:
        return self._all(
            "SELECT * FROM run_attempts WHERE status='running' ORDER BY started_at", ())

    def latest_attempts(self, limit: int = 10) -> list[dict]:
        return self._all(
            "SELECT * FROM run_attempts ORDER BY started_at DESC LIMIT ?", (limit,))

    def list_batches(self, limit: int = 30) -> list[dict]:
        return self._all(
            "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,))

    def watermark_high(self) -> str | None:
        rows = self._all(
            "SELECT watermark_utc FROM batches WHERE status='done' "
            "AND watermark_utc IS NOT NULL ORDER BY watermark_utc DESC LIMIT 1", ())
        return rows[0]["watermark_utc"] if rows else None

    # ---------- 一次运行落库（单事务替换 + 同事务发布基线） ----------
    def save_run(self, *, batch: dict, inputs: list[dict], events: list[dict],
                 snapshots: list[dict], watermarks: list[dict], stages: list[dict],
                 totals: dict, manifest: dict, published_at: str) -> None:
        with self.connect() as conn:
            for table in ("inputs", "events", "snapshots", "watermarks", "stages", "totals"):
                conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))
            for row in events:
                conn.execute(
                    """INSERT INTO events (batch_id,row_ref,seq,table_name,pk,version,op,
                        event_time_utc,event_time_raw,schema_version,payload_json,amount,qty,
                        row_class,reason_code,field,raw_value,detail,late,flags_json,raw_text)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], row["row_ref"], row["seq"], row["table"], row["pk"],
                     row.get("version"), row.get("op"), row.get("event_time"),
                     row.get("event_time_raw"), row.get("schema"),
                     json.dumps(row.get("payload", {}), ensure_ascii=False,
                                default=str),
                     None if row.get("amount") is None else str(row["amount"]),
                     row.get("qty"), row["row_class"], row.get("reason_code"),
                     row.get("field"), row.get("raw_value"), row.get("detail"),
                     1 if row.get("late") else 0,
                     json.dumps(row.get("flags", []), ensure_ascii=False),
                     row.get("raw_text", "")))
            for s in snapshots:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for w in watermarks:
                conn.execute(
                    """INSERT INTO watermarks (batch_id,seq,row_ref,event_time_utc,
                        watermark_utc,late,behind_seconds) VALUES (?,?,?,?,?,?,?)""",
                    (batch["batch_id"], w["seq"], w["row_ref"], w["event_time"],
                     w["watermark"], 1 if w.get("late") else 0, w.get("behind_seconds", 0)))
            for s in stages:
                conn.execute(
                    """INSERT INTO stages (batch_id,stage,in_count,out_count,
                        quarantine_count,status,elapsed_ms,note) VALUES (?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["stage"], s["in_count"], s["out_count"],
                     s["quarantine_count"], s["status"], s["elapsed_ms"], s.get("note", "")))
            for key, value in totals.items():
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch["batch_id"], key, str(value)))
            conn.execute(
                "INSERT INTO manifests (batch_id,json) VALUES (?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET json=excluded.json",
                (batch["batch_id"], json.dumps(manifest, ensure_ascii=False, default=str)))
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note, input_digest=excluded.input_digest,
                     watermark_utc=excluded.watermark_utc,
                     max_event_time=excluded.max_event_time""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 batch.get("note", ""), batch.get("replay_of")))
            # 结果全部落库成功后，才在同一事务切换基线；事务失败则整体回滚，
            # 旧基线与旧结果原封不动，页面/导出不会读到半成品。
            self.publish_baseline(conn, batch["batch_id"], published_at)


    # ---------- 快照（可重复物化） ----------
    def replace_snapshot(self, batch_id: str, rows: list[dict], totals: dict) -> None:
        """按同一 as_of 重算快照：先删后插，不新增事件行。"""
        with self.connect() as conn:
            conn.execute("DELETE FROM snapshots WHERE batch_id=?", (batch_id,))
            for s in rows:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for key, value in totals.items():
                conn.execute("DELETE FROM totals WHERE batch_id=? AND key=?",
                             (batch_id, key))
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch_id, key, str(value)))

    def save_attempt(self, *, batch: dict, inputs: list[dict], note: str = "") -> None:
        """失败尝试：只写批次与输入，不动任何已存在批次的结果行。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 note or batch.get("note", ""), batch.get("replay_of")))
            conn.execute("DELETE FROM inputs WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))

    # ---------- 读取 ----------
    def load_inputs(self, batch_id: str) -> list[dict]:
        return self._all("SELECT * FROM inputs WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_events(self, batch_id: str, row_class: str | None = None) -> list[dict]:
        if row_class:
            rows = self._all(
                "SELECT * FROM events WHERE batch_id=? AND row_class=? ORDER BY seq",
                (batch_id, row_class))
        else:
            rows = self._all("SELECT * FROM events WHERE batch_id=? ORDER BY seq", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
        return rows

    def load_snapshot(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM snapshots WHERE batch_id=? ORDER BY table_name, pk", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
            r["pending"] = bool(r["pending"])
        return rows

    def load_watermarks(self, batch_id: str) -> list[dict]:
        return self._all(
            "SELECT * FROM watermarks WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_stages(self, batch_id: str) -> list[dict]:
        from . import config
        rows = self._all("SELECT * FROM stages WHERE batch_id=? ORDER BY rowid", (batch_id,))
        for row in rows:
            row["label"] = config.STAGE_LABELS.get(row["stage"], row["stage"])
        return rows

    def load_totals(self, batch_id: str) -> dict:
        rows = self._all("SELECT key, value FROM totals WHERE batch_id=?", (batch_id,))
        return {r["key"]: r["value"] for r in rows}

    def load_manifest(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT json FROM manifests WHERE batch_id=?", (batch_id,))
        return json.loads(rows[0]["json"]) if rows else None

    def count_rows(self, batch_id: str) -> dict:
        out = {}
        for table in ("inputs", "events", "snapshots", "watermarks", "stages"):
            out[table] = self._all(
                f"SELECT COUNT(*) AS n FROM {table} WHERE batch_id=?", (batch_id,))[0]["n"]
        return out

    def _all(self, sql: str, args: tuple) -> list[dict]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
