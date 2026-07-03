from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SQLITE_PATH = DATA_DIR / "ebo_runtime.sqlite3"
_DB_LOCK = threading.RLock()
_INITIALIZED = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(SQLITE_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_runtime_db() -> dict[str, Any]:
    """Initialisiert die erste lokale Runtime-Datenbank.

    Schritt 1 ist bewusst deploy-sicher: Der bestehende Bot bleibt bei JSON,
    neue Systeme wie Audit/Voice-Attendance bekommen aber schon eine saubere
    tabellarische Basis. Später kann diese Schicht auf PostgreSQL umgestellt
    werden, ohne die Bot-Logik überall neu anzufassen.
    """
    global _INITIALIZED
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id INTEGER NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, key)
                );

                CREATE TABLE IF NOT EXISTS module_settings (
                    guild_id INTEGER NOT NULL,
                    module TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, module, key)
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    actor_id INTEGER,
                    action TEXT NOT NULL,
                    target_type TEXT,
                    target_id TEXT,
                    summary TEXT,
                    old_value_json TEXT,
                    new_value_json TEXT,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audit_logs_guild_created
                    ON audit_logs (guild_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_created
                    ON audit_logs (actor_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_logs_action_created
                    ON audit_logs (action, created_at DESC);

                CREATE TABLE IF NOT EXISTS voice_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    joined_at TEXT NOT NULL,
                    left_at TEXT,
                    duration_seconds INTEGER,
                    event_id TEXT,
                    source TEXT NOT NULL DEFAULT 'voice_state',
                    metadata_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_user_joined
                    ON voice_sessions (guild_id, user_id, joined_at DESC);
                CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_channel_joined
                    ON voice_sessions (guild_id, channel_id, joined_at DESC);
                CREATE INDEX IF NOT EXISTS idx_voice_sessions_event
                    ON voice_sessions (guild_id, event_id);
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                (1, "runtime_db_audit_voice_base", _now_iso()),
            )
            conn.commit()
            _INITIALIZED = True
            return {"ok": True, "backend": "sqlite", "path": str(SQLITE_PATH)}
        finally:
            conn.close()


def db_status() -> dict[str, Any]:
    exists = SQLITE_PATH.exists()
    size = SQLITE_PATH.stat().st_size if exists else 0
    return {
        "backend": "sqlite",
        "path": str(SQLITE_PATH),
        "exists": exists,
        "size_bytes": size,
        "initialized": _INITIALIZED,
        "database_url_configured": bool(os.getenv("DATABASE_URL")),
    }


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def write_audit_log(
    *,
    guild_id: Optional[int],
    actor_id: Optional[int],
    action: str,
    target_type: str = "",
    target_id: str = "",
    summary: str = "",
    old_value: Any = None,
    new_value: Any = None,
    metadata: Any = None,
) -> int:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO audit_logs(
                    guild_id, actor_id, action, target_type, target_id, summary,
                    old_value_json, new_value_json, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(guild_id) if guild_id is not None else None,
                    int(actor_id) if actor_id is not None else None,
                    str(action or "unknown")[:120],
                    str(target_type or "")[:80],
                    str(target_id or "")[:160],
                    str(summary or "")[:1200],
                    _json_dumps(old_value) if old_value is not None else None,
                    _json_dumps(new_value) if new_value is not None else None,
                    _json_dumps(metadata) if metadata is not None else None,
                    _now_iso(),
                ),
            )
            conn.commit()
            return int(cur.lastrowid or 0)
        finally:
            conn.close()


def fetch_audit_logs(guild_id: Optional[int], limit: int = 10) -> list[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()
    limit = max(1, min(int(limit or 10), 50))
    with _DB_LOCK:
        conn = _connect()
        try:
            if guild_id is None:
                rows = conn.execute(
                    "SELECT * FROM audit_logs ORDER BY created_at DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM audit_logs WHERE guild_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                    (int(guild_id), limit),
                ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()


def count_audit_logs(guild_id: Optional[int] = None) -> int:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        conn = _connect()
        try:
            if guild_id is None:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_logs").fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_logs WHERE guild_id = ?", (int(guild_id),)).fetchone()
            return int(row["c"] if row else 0)
        finally:
            conn.close()
