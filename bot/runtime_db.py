from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SQLITE_PATH = DATA_DIR / "ebo_runtime.sqlite3"
_DB_LOCK = threading.RLock()
_INITIALIZED = False
_BACKEND = "sqlite"
_POSTGRES_ERROR = ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _database_url() -> str:
    return str(os.getenv("DATABASE_URL") or "").strip()


def _normalized_database_url() -> str:
    url = _database_url()
    # Railway/Heroku style is often postgres://. psycopg accepts postgresql:// reliably.
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _want_postgres() -> bool:
    url = _database_url().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def _pg_connect():
    # Lazy import: the bot can still run locally without psycopg installed.
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    return psycopg.connect(_normalized_database_url(), row_factory=dict_row, connect_timeout=10)


def _sqlite_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(SQLITE_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_sqlite() -> dict[str, Any]:
    global _INITIALIZED, _BACKEND
    conn = _sqlite_connect()
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
            
            CREATE TABLE IF NOT EXISTS dashboard_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                guild_name TEXT,
                schema_version INTEGER NOT NULL,
                generated_at TEXT NOT NULL,
                published_at TEXT NOT NULL,
                snapshot_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_guild_published
                ON dashboard_snapshots (guild_id, published_at DESC);
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
            (1, "runtime_db_audit_voice_base", _now_iso()),
        )
        conn.commit()
        _INITIALIZED = True
        _BACKEND = "sqlite"
        return {"ok": True, "backend": _BACKEND, "path": str(SQLITE_PATH)}
    finally:
        conn.close()


def _init_postgres() -> dict[str, Any]:
    global _INITIALIZED, _BACKEND, _POSTGRES_ERROR
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id BIGINT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, key)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS module_settings (
                    guild_id BIGINT NOT NULL,
                    module TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, module, key)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT,
                    actor_id BIGINT,
                    action TEXT NOT NULL,
                    target_type TEXT,
                    target_id TEXT,
                    summary TEXT,
                    old_value_json TEXT,
                    new_value_json TEXT,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_logs_guild_created
                    ON audit_logs (guild_id, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_created
                    ON audit_logs (actor_id, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_logs_action_created
                    ON audit_logs (action, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS voice_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
                    joined_at TEXT NOT NULL,
                    left_at TEXT,
                    duration_seconds INTEGER,
                    event_id TEXT,
                    source TEXT NOT NULL DEFAULT 'voice_state',
                    metadata_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_user_joined
                    ON voice_sessions (guild_id, user_id, joined_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_channel_joined
                    ON voice_sessions (guild_id, channel_id, joined_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_voice_sessions_event
                    ON voice_sessions (guild_id, event_id)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    guild_name TEXT,
                    schema_version INTEGER NOT NULL,
                    generated_at TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_snapshots_guild_published
                    ON dashboard_snapshots (guild_id, published_at DESC)
                """
            )
            cur.execute(
                """
                INSERT INTO schema_migrations(version, name, applied_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (version) DO NOTHING
                """,
                (1, "runtime_db_audit_voice_base", _now_iso()),
            )
        conn.commit()
        _INITIALIZED = True
        _BACKEND = "postgres"
        _POSTGRES_ERROR = ""
        return {"ok": True, "backend": _BACKEND, "path": "PostgreSQL über DATABASE_URL"}
    finally:
        conn.close()


def init_runtime_db() -> dict[str, Any]:
    """Initialisiert die Runtime-Datenbank.

    Production/Railway: Wenn DATABASE_URL auf postgres/postgresql zeigt, wird Postgres genutzt.
    Fallback: Ohne DATABASE_URL oder bei lokalem Test nutzt der Bot SQLite, damit bestehende Deploys nicht brechen.
    """
    global _INITIALIZED, _BACKEND, _POSTGRES_ERROR
    with _DB_LOCK:
        if _want_postgres():
            try:
                return _init_postgres()
            except Exception as e:
                _POSTGRES_ERROR = repr(e)
                print(f"[runtime_db] PostgreSQL konnte nicht initialisiert werden, SQLite-Fallback aktiv: {e!r}")
                info = _init_sqlite()
                _BACKEND = "sqlite_fallback"
                return {**info, "backend": _BACKEND, "postgres_error": _POSTGRES_ERROR}
        return _init_sqlite()


def db_status() -> dict[str, Any]:
    parsed = urlparse(_database_url()) if _database_url() else None
    exists = SQLITE_PATH.exists()
    size = SQLITE_PATH.stat().st_size if exists else 0
    return {
        "backend": _BACKEND,
        "path": str(SQLITE_PATH) if _BACKEND != "postgres" else "PostgreSQL über DATABASE_URL",
        "exists": exists,
        "size_bytes": size,
        "initialized": _INITIALIZED,
        "database_url_configured": bool(_database_url()),
        "database_url_kind": (parsed.scheme if parsed else ""),
        "postgres_error": _POSTGRES_ERROR,
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

    values = (
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
    )

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO audit_logs(
                            guild_id, actor_id, action, target_type, target_id, summary,
                            old_value_json, new_value_json, metadata_json, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        values,
                    )
                    row = cur.fetchone()
                conn.commit()
                return int((row or {}).get("id") or 0)
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO audit_logs(
                    guild_id, actor_id, action, target_type, target_id, summary,
                    old_value_json, new_value_json, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
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
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    if guild_id is None:
                        cur.execute(
                            "SELECT * FROM audit_logs ORDER BY created_at DESC, id DESC LIMIT %s",
                            (limit,),
                        )
                    else:
                        cur.execute(
                            "SELECT * FROM audit_logs WHERE guild_id = %s ORDER BY created_at DESC, id DESC LIMIT %s",
                            (int(guild_id), limit),
                        )
                    return [dict(row) for row in cur.fetchall()]
            finally:
                conn.close()

        conn = _sqlite_connect()
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
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    if guild_id is None:
                        cur.execute("SELECT COUNT(*) AS c FROM audit_logs")
                    else:
                        cur.execute("SELECT COUNT(*) AS c FROM audit_logs WHERE guild_id = %s", (int(guild_id),))
                    row = cur.fetchone()
                    return int((row or {}).get("c") or 0)
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            if guild_id is None:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_logs").fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_logs WHERE guild_id = ?", (int(guild_id),)).fetchone()
            return int(row["c"] if row else 0)
        finally:
            conn.close()

def _parse_iso_utc(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(value or ""))
    except Exception:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _duration_seconds(joined_at: str, left_at: str) -> int:
    start = _parse_iso_utc(joined_at)
    end = _parse_iso_utc(left_at)
    return max(0, int((end - start).total_seconds()))


def start_voice_session(*, guild_id: int, user_id: int, channel_id: int, member_name: str = "", channel_name: str = "", metadata: Any = None) -> int:
    """Startet eine Voice-Session und schließt vorher offene Alt-Sessions dieses Users.

    Dadurch entstehen nach Gateway-Reconnects oder verpassten Voice-Events keine
    dauerhaft offenen Doppel-Sessions.
    """
    if not _INITIALIZED:
        init_runtime_db()

    joined_at = _now_iso()
    meta = {"member_name": member_name or "", "channel_name": channel_name or ""}
    if isinstance(metadata, dict):
        meta.update(metadata)

    with _DB_LOCK:
        # Erst alle offenen Sessions des Users im Server sauber beenden.
        close_open_voice_sessions_for_user(int(guild_id), int(user_id), left_at=joined_at)

        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO voice_sessions(
                            guild_id, user_id, channel_id, joined_at, left_at, duration_seconds,
                            event_id, source, metadata_json
                        ) VALUES (%s, %s, %s, %s, NULL, NULL, NULL, %s, %s)
                        RETURNING id
                        """,
                        (int(guild_id), int(user_id), int(channel_id), joined_at, "voice_state", _json_dumps(meta)),
                    )
                    row = cur.fetchone()
                conn.commit()
                return int((row or {}).get("id") or 0)
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO voice_sessions(
                    guild_id, user_id, channel_id, joined_at, left_at, duration_seconds,
                    event_id, source, metadata_json
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (int(guild_id), int(user_id), int(channel_id), joined_at, "voice_state", _json_dumps(meta)),
            )
            conn.commit()
            return int(cur.lastrowid or 0)
        finally:
            conn.close()


def close_open_voice_sessions_for_user(guild_id: int, user_id: int, *, left_at: Optional[str] = None, channel_id: Optional[int] = None) -> int:
    """Schließt offene Voice-Sessions eines Users und trägt duration_seconds nach."""
    if not _INITIALIZED:
        init_runtime_db()
    closed_at = str(left_at or _now_iso())

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    if channel_id is None:
                        cur.execute(
                            "SELECT id, joined_at FROM voice_sessions WHERE guild_id = %s AND user_id = %s AND left_at IS NULL",
                            (int(guild_id), int(user_id)),
                        )
                    else:
                        cur.execute(
                            "SELECT id, joined_at FROM voice_sessions WHERE guild_id = %s AND user_id = %s AND channel_id = %s AND left_at IS NULL",
                            (int(guild_id), int(user_id), int(channel_id)),
                        )
                    rows = [dict(row) for row in cur.fetchall()]
                    for row in rows:
                        cur.execute(
                            "UPDATE voice_sessions SET left_at = %s, duration_seconds = %s WHERE id = %s",
                            (closed_at, _duration_seconds(str(row.get("joined_at", "")), closed_at), int(row.get("id") or 0)),
                        )
                conn.commit()
                return len(rows)
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            if channel_id is None:
                rows = conn.execute(
                    "SELECT id, joined_at FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND left_at IS NULL",
                    (int(guild_id), int(user_id)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, joined_at FROM voice_sessions WHERE guild_id = ? AND user_id = ? AND channel_id = ? AND left_at IS NULL",
                    (int(guild_id), int(user_id), int(channel_id)),
                ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE voice_sessions SET left_at = ?, duration_seconds = ? WHERE id = ?",
                    (closed_at, _duration_seconds(str(row["joined_at"]), closed_at), int(row["id"])),
                )
            conn.commit()
            return len(rows)
        finally:
            conn.close()


def fetch_voice_sessions(
    guild_id: int,
    *,
    since_iso: Optional[str] = None,
    until_iso: Optional[str] = None,
    channel_ids: Optional[list[int]] = None,
    user_ids: Optional[list[int]] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Liest Voice-Sessions, die ein Zeitfenster überlappen.

    Overlap-Logik: joined_at < until AND (left_at IS NULL OR left_at > since).
    """
    if not _INITIALIZED:
        init_runtime_db()
    limit = max(1, min(int(limit or 500), 5000))
    since = str(since_iso or "1970-01-01T00:00:00+00:00")
    until = str(until_iso or _now_iso())
    channel_ids = [int(x) for x in (channel_ids or []) if int(x or 0)]
    user_ids = [int(x) for x in (user_ids or []) if int(x or 0)]

    def _filter_py(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        ch_set = set(channel_ids) if channel_ids else None
        u_set = set(user_ids) if user_ids else None
        for r in rows:
            try:
                if ch_set is not None and int(r.get("channel_id") or 0) not in ch_set:
                    continue
                if u_set is not None and int(r.get("user_id") or 0) not in u_set:
                    continue
                out.append(r)
            except Exception:
                continue
        return out

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT * FROM voice_sessions
                        WHERE guild_id = %s
                          AND joined_at < %s
                          AND (left_at IS NULL OR left_at > %s)
                        ORDER BY joined_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(guild_id), until, since, limit),
                    )
                    return _filter_py([dict(row) for row in cur.fetchall()])
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            rows = conn.execute(
                """
                SELECT * FROM voice_sessions
                WHERE guild_id = ?
                  AND joined_at < ?
                  AND (left_at IS NULL OR left_at > ?)
                ORDER BY joined_at DESC, id DESC
                LIMIT ?
                """,
                (int(guild_id), until, since, limit),
            ).fetchall()
            return _filter_py([dict(row) for row in rows])
        finally:
            conn.close()


def count_voice_sessions(guild_id: Optional[int] = None, *, open_only: bool = False) -> int:
    if not _INITIALIZED:
        init_runtime_db()
    clauses = []
    params_pg: list[Any] = []
    params_sqlite: list[Any] = []
    if guild_id is not None:
        clauses.append("guild_id = {}")
        params_pg.append(int(guild_id))
        params_sqlite.append(int(guild_id))
    if open_only:
        clauses.append("left_at IS NULL")
    where = ""
    if clauses:
        pg_parts = []
        sqlite_parts = []
        pgi = 0
        for c in clauses:
            if "{}" in c:
                pg_parts.append(c.format("%s"))
                sqlite_parts.append(c.format("?"))
                pgi += 1
            else:
                pg_parts.append(c)
                sqlite_parts.append(c)
        where_pg = " WHERE " + " AND ".join(pg_parts)
        where_sqlite = " WHERE " + " AND ".join(sqlite_parts)
    else:
        where_pg = where_sqlite = ""

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS c FROM voice_sessions" + where_pg, tuple(params_pg))
                    row = cur.fetchone()
                    return int((row or {}).get("c") or 0)
            finally:
                conn.close()
        conn = _sqlite_connect()
        try:
            row = conn.execute("SELECT COUNT(*) AS c FROM voice_sessions" + where_sqlite, tuple(params_sqlite)).fetchone()
            return int(row["c"] if row else 0)
        finally:
            conn.close()


def aggregate_voice_seconds(
    guild_id: int,
    *,
    since_iso: str,
    until_iso: str,
    channel_ids: Optional[list[int]] = None,
    user_ids: Optional[list[int]] = None,
) -> dict[int, int]:
    """Summiert Voice-Zeit pro User im Zeitfenster, auf das Fenster gekürzt."""
    rows = fetch_voice_sessions(
        int(guild_id),
        since_iso=since_iso,
        until_iso=until_iso,
        channel_ids=channel_ids,
        user_ids=user_ids,
        limit=5000,
    )
    start = _parse_iso_utc(since_iso)
    end = _parse_iso_utc(until_iso)
    now = datetime.now(timezone.utc)
    totals: dict[int, int] = {}
    for r in rows:
        try:
            uid = int(r.get("user_id") or 0)
            if not uid:
                continue
            a = _parse_iso_utc(str(r.get("joined_at") or ""))
            b_raw = r.get("left_at")
            b = _parse_iso_utc(str(b_raw)) if b_raw else min(now, end)
            overlap_start = max(start, a)
            overlap_end = min(end, b)
            sec = max(0, int((overlap_end - overlap_start).total_seconds()))
            if sec > 0:
                totals[uid] = totals.get(uid, 0) + sec
        except Exception:
            continue
    return totals



def save_dashboard_snapshot(*, guild_id: int, guild_name: str, snapshot: Any) -> int:
    """Speichert den read-only Dashboard-Snapshot in der Runtime-DB.

    Das ist bewusst KEINE Migration produktiver Bot-Daten. Alte JSON-Systeme bleiben
    die Quelle. Diese Tabelle ist nur die Brücke für den separaten Web-Service.
    """
    if not _INITIALIZED:
        init_runtime_db()

    schema_version = 0
    generated_at = _now_iso()
    if isinstance(snapshot, dict):
        try:
            schema_version = int(snapshot.get("schema_version") or 0)
        except Exception:
            schema_version = 0
        generated_at = str(snapshot.get("generated_at") or generated_at)

    values = (
        int(guild_id),
        str(guild_name or "")[:200],
        int(schema_version),
        generated_at,
        _now_iso(),
        _json_dumps(snapshot),
    )

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dashboard_snapshots(
                            guild_id, guild_name, schema_version, generated_at, published_at, snapshot_json
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        values,
                    )
                    row = cur.fetchone()
                    # Kleine Aufräumung: pro Gilde die letzten 50 Snapshots behalten.
                    cur.execute(
                        """
                        DELETE FROM dashboard_snapshots
                        WHERE guild_id = %s
                          AND id NOT IN (
                            SELECT id FROM dashboard_snapshots
                            WHERE guild_id = %s
                            ORDER BY published_at DESC, id DESC
                            LIMIT 50
                          )
                        """,
                        (int(guild_id), int(guild_id)),
                    )
                conn.commit()
                return int((row or {}).get("id") or 0)
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO dashboard_snapshots(
                    guild_id, guild_name, schema_version, generated_at, published_at, snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            conn.execute(
                """
                DELETE FROM dashboard_snapshots
                WHERE guild_id = ?
                  AND id NOT IN (
                    SELECT id FROM dashboard_snapshots
                    WHERE guild_id = ?
                    ORDER BY published_at DESC, id DESC
                    LIMIT 50
                  )
                """,
                (int(guild_id), int(guild_id)),
            )
            conn.commit()
            return int(cur.lastrowid or 0)
        finally:
            conn.close()


def fetch_latest_dashboard_snapshot(guild_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    if guild_id is None:
                        cur.execute(
                            "SELECT * FROM dashboard_snapshots ORDER BY published_at DESC, id DESC LIMIT 1"
                        )
                    else:
                        cur.execute(
                            "SELECT * FROM dashboard_snapshots WHERE guild_id = %s ORDER BY published_at DESC, id DESC LIMIT 1",
                            (int(guild_id),),
                        )
                    row = cur.fetchone()
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                if guild_id is None:
                    row = conn.execute(
                        "SELECT * FROM dashboard_snapshots ORDER BY published_at DESC, id DESC LIMIT 1"
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM dashboard_snapshots WHERE guild_id = ? ORDER BY published_at DESC, id DESC LIMIT 1",
                        (int(guild_id),),
                    ).fetchone()
            finally:
                conn.close()

    if not row:
        return None
    d = dict(row)
    try:
        d["snapshot"] = json.loads(d.get("snapshot_json") or "{}")
    except Exception:
        d["snapshot"] = {}
    d.pop("snapshot_json", None)
    return d


def count_dashboard_snapshots(guild_id: Optional[int] = None) -> int:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    if guild_id is None:
                        cur.execute("SELECT COUNT(*) AS c FROM dashboard_snapshots")
                    else:
                        cur.execute("SELECT COUNT(*) AS c FROM dashboard_snapshots WHERE guild_id = %s", (int(guild_id),))
                    row = cur.fetchone()
                    return int((row or {}).get("c") or 0)
            finally:
                conn.close()
        conn = _sqlite_connect()
        try:
            if guild_id is None:
                row = conn.execute("SELECT COUNT(*) AS c FROM dashboard_snapshots").fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM dashboard_snapshots WHERE guild_id = ?", (int(guild_id),)).fetchone()
            return int(row["c"] if row else 0)
        finally:
            conn.close()
