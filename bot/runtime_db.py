from __future__ import annotations

import json
import os
import sqlite3
import threading
import re
import difflib
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

            CREATE TABLE IF NOT EXISTS guild_profiles (
                guild_id INTEGER PRIMARY KEY,
                discord_name TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                short_name TEXT NOT NULL DEFAULT '',
                bot_display_name TEXT NOT NULL DEFAULT '',
                timezone_name TEXT NOT NULL DEFAULT 'Europe/Berlin',
                logo_url TEXT NOT NULL DEFAULT '',
                banner_url TEXT NOT NULL DEFAULT '',
                accent_color TEXT NOT NULL DEFAULT '#d6a84f',
                invite_url TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                previous_guild_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_guild_profiles_status
                ON guild_profiles (status, updated_at DESC);

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


            CREATE TABLE IF NOT EXISTS guild_members (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                server_name TEXT NOT NULL DEFAULT '',
                discord_username TEXT NOT NULL DEFAULT '',
                avatar_url TEXT NOT NULL DEFAULT '',
                ingame_name TEXT NOT NULL DEFAULT '',
                main_role TEXT NOT NULL DEFAULT '',
                gearscore TEXT NOT NULL DEFAULT '',
                member_role_id INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                joined_at TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                left_at TEXT,
                roles_json TEXT NOT NULL DEFAULT '[]',
                profile_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_guild_members_active_name
                ON guild_members (guild_id, is_active, server_name);

            CREATE TABLE IF NOT EXISTS guild_item_links (
                guild_id INTEGER NOT NULL,
                reference_type TEXT NOT NULL,
                reference_key TEXT NOT NULL,
                catalog_item_id INTEGER,
                source_item_id TEXT,
                canonical_name TEXT,
                source_url TEXT,
                image_url TEXT,
                match_method TEXT,
                confidence REAL NOT NULL DEFAULT 0,
                aliases_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, reference_type, reference_key)
            );

            CREATE INDEX IF NOT EXISTS idx_guild_item_links_catalog
                ON guild_item_links (guild_id, catalog_item_id);
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
                CREATE TABLE IF NOT EXISTS guild_profiles (
                    guild_id BIGINT PRIMARY KEY,
                    discord_name TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    short_name TEXT NOT NULL DEFAULT '',
                    bot_display_name TEXT NOT NULL DEFAULT '',
                    timezone_name TEXT NOT NULL DEFAULT 'Europe/Berlin',
                    logo_url TEXT NOT NULL DEFAULT '',
                    banner_url TEXT NOT NULL DEFAULT '',
                    accent_color TEXT NOT NULL DEFAULT '#d6a84f',
                    invite_url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    previous_guild_id BIGINT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_guild_profiles_status
                    ON guild_profiles (status, updated_at DESC)
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
                CREATE TABLE IF NOT EXISTS guild_members (
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    server_name TEXT NOT NULL DEFAULT '',
                    discord_username TEXT NOT NULL DEFAULT '',
                    avatar_url TEXT NOT NULL DEFAULT '',
                    ingame_name TEXT NOT NULL DEFAULT '',
                    main_role TEXT NOT NULL DEFAULT '',
                    gearscore TEXT NOT NULL DEFAULT '',
                    member_role_id BIGINT NOT NULL DEFAULT 0,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    joined_at TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    left_at TEXT,
                    roles_json TEXT NOT NULL DEFAULT '[]',
                    profile_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, user_id)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_guild_members_active_name
                    ON guild_members (guild_id, is_active, server_name)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_item_links (
                    guild_id BIGINT NOT NULL,
                    reference_type TEXT NOT NULL,
                    reference_key TEXT NOT NULL,
                    catalog_item_id BIGINT,
                    source_item_id TEXT,
                    canonical_name TEXT,
                    source_url TEXT,
                    image_url TEXT,
                    match_method TEXT,
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    aliases_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, reference_type, reference_key)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_guild_item_links_catalog
                    ON guild_item_links (guild_id, catalog_item_id)
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



def upsert_guild_profile(
    guild_id: int,
    *,
    discord_name: Optional[str] = None,
    display_name: Optional[str] = None,
    short_name: Optional[str] = None,
    bot_display_name: Optional[str] = None,
    timezone_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    banner_url: Optional[str] = None,
    accent_color: Optional[str] = None,
    invite_url: Optional[str] = None,
    status: Optional[str] = None,
    previous_guild_id: Optional[int] = None,
) -> bool:
    """Legt das zentrale Gildenprofil an oder aktualisiert nur übergebene Felder."""
    if not _INITIALIZED:
        init_runtime_db()
    gid = int(guild_id)
    current = get_guild_profile(gid) or {}
    now = _now_iso()
    values = {
        "guild_id": gid,
        "discord_name": str(discord_name if discord_name is not None else current.get("discord_name") or "")[:120],
        "display_name": str(display_name if display_name is not None else current.get("display_name") or discord_name or "Gilde")[:120],
        "short_name": str(short_name if short_name is not None else current.get("short_name") or display_name or discord_name or "Gilde")[:60],
        "bot_display_name": str(bot_display_name if bot_display_name is not None else current.get("bot_display_name") or "Gildenknecht")[:120],
        "timezone_name": str(timezone_name if timezone_name is not None else current.get("timezone_name") or current.get("timezone") or "Europe/Berlin")[:80],
        "logo_url": str(logo_url if logo_url is not None else current.get("logo_url") or "")[:1200],
        "banner_url": str(banner_url if banner_url is not None else current.get("banner_url") or "")[:1200],
        "accent_color": str(accent_color if accent_color is not None else current.get("accent_color") or "#d6a84f")[:20],
        "invite_url": str(invite_url if invite_url is not None else current.get("invite_url") or "")[:1200],
        "status": str(status if status is not None else current.get("status") or "active")[:30],
        "previous_guild_id": int(previous_guild_id) if previous_guild_id is not None else current.get("previous_guild_id"),
        "created_at": str(current.get("created_at") or now),
        "updated_at": now,
    }
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO guild_profiles(
                            guild_id, discord_name, display_name, short_name, bot_display_name,
                            timezone_name, logo_url, banner_url, accent_color, invite_url,
                            status, previous_guild_id, created_at, updated_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (guild_id) DO UPDATE SET
                            discord_name=EXCLUDED.discord_name,
                            display_name=EXCLUDED.display_name,
                            short_name=EXCLUDED.short_name,
                            bot_display_name=EXCLUDED.bot_display_name,
                            timezone_name=EXCLUDED.timezone_name,
                            logo_url=EXCLUDED.logo_url,
                            banner_url=EXCLUDED.banner_url,
                            accent_color=EXCLUDED.accent_color,
                            invite_url=EXCLUDED.invite_url,
                            status=EXCLUDED.status,
                            previous_guild_id=EXCLUDED.previous_guild_id,
                            updated_at=EXCLUDED.updated_at
                        """,
                        tuple(values[k] for k in (
                            "guild_id", "discord_name", "display_name", "short_name", "bot_display_name",
                            "timezone_name", "logo_url", "banner_url", "accent_color", "invite_url",
                            "status", "previous_guild_id", "created_at", "updated_at",
                        )),
                    )
                conn.commit()
                return True
            finally:
                conn.close()
        conn = _sqlite_connect()
        try:
            conn.execute(
                """
                INSERT INTO guild_profiles(
                    guild_id, discord_name, display_name, short_name, bot_display_name,
                    timezone_name, logo_url, banner_url, accent_color, invite_url,
                    status, previous_guild_id, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    discord_name=excluded.discord_name,
                    display_name=excluded.display_name,
                    short_name=excluded.short_name,
                    bot_display_name=excluded.bot_display_name,
                    timezone_name=excluded.timezone_name,
                    logo_url=excluded.logo_url,
                    banner_url=excluded.banner_url,
                    accent_color=excluded.accent_color,
                    invite_url=excluded.invite_url,
                    status=excluded.status,
                    previous_guild_id=excluded.previous_guild_id,
                    updated_at=excluded.updated_at
                """,
                tuple(values[k] for k in (
                    "guild_id", "discord_name", "display_name", "short_name", "bot_display_name",
                    "timezone_name", "logo_url", "banner_url", "accent_color", "invite_url",
                    "status", "previous_guild_id", "created_at", "updated_at",
                )),
            )
            conn.commit()
            return True
        finally:
            conn.close()


def get_guild_profile(guild_id: int) -> Optional[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM guild_profiles WHERE guild_id=%s", (int(guild_id),))
                    row = cur.fetchone()
                    out = dict(row) if row else None
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                row = conn.execute("SELECT * FROM guild_profiles WHERE guild_id=?", (int(guild_id),)).fetchone()
                out = dict(row) if row else None
            finally:
                conn.close()
    if out is not None:
        out["timezone"] = out.get("timezone_name") or "Europe/Berlin"
    return out


def list_guild_profiles(*, include_archived: bool = True) -> list[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()
    where = "" if include_archived else " WHERE status='active'"
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM guild_profiles" + where + " ORDER BY updated_at DESC, guild_id")
                    rows = [dict(x) for x in cur.fetchall()]
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                rows = [dict(x) for x in conn.execute("SELECT * FROM guild_profiles" + where + " ORDER BY updated_at DESC, guild_id").fetchall()]
            finally:
                conn.close()
    for row in rows:
        row["timezone"] = row.get("timezone_name") or "Europe/Berlin"
    return rows


def get_all_guild_settings(guild_id: int) -> dict[str, Any]:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT key, value_json FROM guild_settings WHERE guild_id=%s", (int(guild_id),))
                    rows = [dict(x) for x in cur.fetchall()]
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                rows = [dict(x) for x in conn.execute("SELECT key, value_json FROM guild_settings WHERE guild_id=?", (int(guild_id),)).fetchall()]
            finally:
                conn.close()
    out: dict[str, Any] = {}
    for row in rows:
        try:
            out[str(row.get("key") or "")] = json.loads(row.get("value_json") or "null")
        except Exception:
            continue
    return out


def _pg_table_columns(cur: Any, table: str) -> list[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position",
        (str(table),),
    )
    return [str(row.get("column_name") or "") for row in cur.fetchall() if str(row.get("column_name") or "")]


def rehome_guild_data(source_guild_id: int, target_guild_id: int, active_user_ids: list[int]) -> dict[str, Any]:
    """Übernimmt sichere Mitglieder-/Historien-Daten auf einen neuen Discord-Server.

    Servergebundene Rollen, Kanäle, Events, RSVPs, aktive Auktionen und Gebote
    werden bewusst nicht kopiert. Die Funktion ist wiederholbar (ON CONFLICT).
    """
    if not _INITIALIZED:
        init_runtime_db()
    source, target = int(source_guild_id), int(target_guild_id)
    if not source or not target or source == target:
        raise ValueError("Ungültige Quell-/Ziel-Guild-ID")
    users = sorted({int(x) for x in active_user_ids if int(x or 0)})
    counts: dict[str, int] = {}

    source_profile = get_guild_profile(source)
    target_profile = get_guild_profile(target)
    if source_profile:
        upsert_guild_profile(source, status="archived")
    if target_profile:
        upsert_guild_profile(target, previous_guild_id=source, status="active")
    else:
        upsert_guild_profile(
            target,
            display_name=str((source_profile or {}).get("display_name") or "Gilde"),
            short_name=str((source_profile or {}).get("short_name") or "Gilde"),
            bot_display_name=str((source_profile or {}).get("bot_display_name") or "Gildenknecht"),
            timezone_name=str((source_profile or {}).get("timezone_name") or "Europe/Berlin"),
            logo_url=str((source_profile or {}).get("logo_url") or ""),
            banner_url=str((source_profile or {}).get("banner_url") or ""),
            accent_color=str((source_profile or {}).get("accent_color") or "#d6a84f"),
            invite_url=str((source_profile or {}).get("invite_url") or ""),
            previous_guild_id=source,
            status="active",
        )

    # Nur nicht-Discordgebundene zentrale Einstellungen übernehmen.
    blocked = {
        "dashboard_member_role_id", "dashboard_admin_role_ids", "dashboard_allowed_role_ids",
        "dashboard_news_channel_name", "dashboard_guides_channel_name", "dashboard_announcements_channel_name",
    }
    for key, value in get_all_guild_settings(source).items():
        # Sämtliche Rollen-/Kanalzuordnungen sind an den alten Discord-Server gebunden.
        if key not in blocked and not key.startswith("guild_role_") and not key.startswith("guild_channel_"):
            set_guild_setting(target, key, value)
            counts["guild_settings"] = counts.get("guild_settings", 0) + 1

    if _BACKEND != "postgres":
        if users:
            conn = _sqlite_connect()
            try:
                placeholders = ",".join("?" for _ in users)
                rows = conn.execute(f"SELECT * FROM guild_members WHERE guild_id=? AND user_id IN ({placeholders})", (source, *users)).fetchall()
                for raw in rows:
                    row = dict(raw); row["guild_id"] = target; row["is_active"] = 1; row["left_at"] = None; row["updated_at"] = _now_iso()
                    cols = list(row); qs = ",".join("?" for _ in cols)
                    conn.execute(f"INSERT OR REPLACE INTO guild_members ({','.join(cols)}) VALUES ({qs})", tuple(row[c] for c in cols))
                conn.commit(); counts["guild_members"] = len(rows)
            finally:
                conn.close()
        return {"ok": True, "backend": _BACKEND, "source_guild_id": source, "target_guild_id": target, "counts": counts}

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            user_texts = [str(x) for x in users]
            if users:
                # Mitgliederstammdaten: neue Discord-Namen/Rollen behalten, alte
                # Ingame-Profile nur dort ergänzen, wo im Ziel noch nichts steht.
                if _pg_table_columns(cur, "guild_members"):
                    cur.execute(
                        """
                        INSERT INTO guild_members
                            (guild_id,user_id,server_name,discord_username,avatar_url,ingame_name,main_role,gearscore,
                             member_role_id,is_active,joined_at,first_seen_at,last_seen_at,left_at,roles_json,profile_json,updated_at)
                        SELECT %s,user_id,server_name,discord_username,avatar_url,ingame_name,main_role,gearscore,
                               0,TRUE,joined_at,first_seen_at,last_seen_at,NULL,roles_json,profile_json,NOW()
                        FROM guild_members
                        WHERE guild_id=%s AND user_id::text = ANY(%s)
                        ON CONFLICT (guild_id,user_id) DO UPDATE SET
                            ingame_name=COALESCE(NULLIF(guild_members.ingame_name,''),EXCLUDED.ingame_name),
                            main_role=COALESCE(NULLIF(guild_members.main_role,''),EXCLUDED.main_role),
                            gearscore=COALESCE(NULLIF(guild_members.gearscore,''),EXCLUDED.gearscore),
                            profile_json=CASE WHEN guild_members.profile_json IS NULL OR guild_members.profile_json::text IN ('{}','null','') THEN EXCLUDED.profile_json ELSE guild_members.profile_json END,
                            is_active=TRUE,left_at=NULL,updated_at=NOW()
                        """,
                        (target, source, user_texts),
                    )
                    counts["guild_members"] = max(0, int(cur.rowcount or 0))

                if _pg_table_columns(cur, "phase3_members"):
                    cur.execute(
                        """
                        INSERT INTO phase3_members (guild_id,user_id,discord_name,ingame_name,roles_json,raw_json,source,updated_at)
                        SELECT %s,user_id,discord_name,ingame_name,roles_json,raw_json,'guild_rehome',NOW()
                        FROM phase3_members
                        WHERE guild_id::text=%s AND user_id::text = ANY(%s)
                        ON CONFLICT (guild_id,user_id) DO UPDATE SET
                            ingame_name=COALESCE(NULLIF(phase3_members.ingame_name,''),EXCLUDED.ingame_name),
                            raw_json=CASE WHEN phase3_members.raw_json IS NULL OR phase3_members.raw_json='{}'::jsonb THEN EXCLUDED.raw_json ELSE phase3_members.raw_json END,
                            updated_at=NOW()
                        """,
                        (str(target), str(source), user_texts),
                    )
                    counts["phase3_members"] = max(0, int(cur.rowcount or 0))

                if _pg_table_columns(cur, "phase3_ec_balances"):
                    cur.execute(
                        """
                        INSERT INTO phase3_ec_balances (guild_id,user_id,balance,raw_json,source,updated_at)
                        SELECT %s,user_id,balance,raw_json,'guild_rehome',NOW()
                        FROM phase3_ec_balances
                        WHERE guild_id::text=%s AND user_id::text = ANY(%s)
                        ON CONFLICT (guild_id,user_id) DO UPDATE SET
                            balance=EXCLUDED.balance,raw_json=EXCLUDED.raw_json,source='guild_rehome',updated_at=NOW()
                        """,
                        (str(target), str(source), user_texts),
                    )
                    counts["phase3_ec_balances"] = max(0, int(cur.rowcount or 0))

                # Need-Builder ist rein spielerbezogen. Da build_id global eindeutig
                # ist, werden diese Datensätze auf die neue Guild-ID verschoben.
                if _pg_table_columns(cur, "dashboard_need_builds"):
                    cur.execute(
                        "UPDATE dashboard_need_builds SET guild_id=%s, updated_at=NOW() WHERE guild_id=%s AND user_id::text = ANY(%s)",
                        (target, source, user_texts),
                    )
                    counts["dashboard_need_builds"] = max(0, int(cur.rowcount or 0))

            tables = [
                ("phase3_ec_transactions", "user_id", True),
                ("phase3_loot_needs", "user_id", False),
                ("phase3_loot_history", "user_id", False),
                ("phase3_need_change_log", "target_user_id", False),
                ("phase3_absences", "user_id", False),
                ("guild_item_links", None, False),
            ]
            for table, user_col, include_null in tables:
                cols = _pg_table_columns(cur, table)
                if not cols or "guild_id" not in cols:
                    continue
                select_parts = ["%s" if c == "guild_id" else f'"{c}"' for c in cols]
                where = "guild_id::text=%s"
                params: list[Any] = [str(target) if table.startswith("phase3_") else target, str(source)]
                if user_col:
                    if not users:
                        continue
                    where += f' AND ("{user_col}"::text = ANY(%s)'
                    params.append(user_texts)
                    if include_null:
                        where += f' OR "{user_col}" IS NULL'
                    where += ")"
                quoted_cols = ",".join(chr(34) + c + chr(34) for c in cols)
                sql = f'INSERT INTO "{table}" ({quoted_cols}) SELECT {",".join(select_parts)} FROM "{table}" WHERE {where} ON CONFLICT DO NOTHING'
                cur.execute(sql, tuple(params))
                counts[table] = max(0, int(cur.rowcount or 0))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "backend": _BACKEND, "source_guild_id": source, "target_guild_id": target, "active_users": len(users), "counts": counts}


def get_guild_setting(guild_id: int, key: str, default: Any = None) -> Any:
    """Liest eine serverbezogene Einstellung aus der Runtime-DB.

    Wird u.a. fürs spätere Multi-Guild-/Dashboard-Setup genutzt.
    Werte werden als JSON gespeichert, damit Zahlen, Strings und Dicts sauber
    erhalten bleiben. Gibt `default` zurück, wenn kein Wert existiert oder JSON
    defekt ist.
    """
    if not _INITIALIZED:
        init_runtime_db()
    key = str(key or "").strip()
    if not key:
        return default
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT value_json FROM guild_settings WHERE guild_id = %s AND key = %s",
                        (int(guild_id), key),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                row = conn.execute(
                    "SELECT value_json FROM guild_settings WHERE guild_id = ? AND key = ?",
                    (int(guild_id), key),
                ).fetchone()
            finally:
                conn.close()
    if not row:
        return default
    try:
        return json.loads(dict(row).get("value_json") or "null")
    except Exception:
        return default


def set_guild_setting(guild_id: int, key: str, value: Any) -> bool:
    """Speichert eine serverbezogene Einstellung in der Runtime-DB."""
    if not _INITIALIZED:
        init_runtime_db()
    key = str(key or "").strip()
    if not key:
        return False
    value_json = _json_dumps(value)
    updated_at = _now_iso()
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO guild_settings(guild_id, key, value_json, updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (guild_id, key)
                        DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at
                        """,
                        (int(guild_id), key, value_json, updated_at),
                    )
                conn.commit()
                return True
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            conn.execute(
                """
                INSERT INTO guild_settings(guild_id, key, value_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id, key)
                DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
                """,
                (int(guild_id), key, value_json, updated_at),
            )
            conn.commit()
            return True
        finally:
            conn.close()


def delete_guild_setting(guild_id: int, key: str) -> bool:
    """Löscht eine serverbezogene Einstellung."""
    if not _INITIALIZED:
        init_runtime_db()
    key = str(key or "").strip()
    if not key:
        return False
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM guild_settings WHERE guild_id = %s AND key = %s",
                        (int(guild_id), key),
                    )
                conn.commit()
                return True
            finally:
                conn.close()
        conn = _sqlite_connect()
        try:
            conn.execute(
                "DELETE FROM guild_settings WHERE guild_id = ? AND key = ?",
                (int(guild_id), key),
            )
            conn.commit()
            return True
        finally:
            conn.close()


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


# ---------------------------------------------------------------------------
# Phase 4 foundation: zentrale Mitglieder-Synchronisierung + feste Item-Links
# ---------------------------------------------------------------------------


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return default


def _normalize_item_reference(value: Any) -> str:
    raw = str(value or "").strip()
    # Eigene Kurzformen wie "DaVinci" vor dem casefold in "Da Vinci" teilen.
    raw = re.sub(r"(?<=[a-zäöüß])(?=[A-ZÄÖÜ])", " ", raw)
    text = raw.casefold()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    stop = {"der", "die", "das", "des", "den", "dem", "von", "vom", "zur", "zum", "und", "of", "the"}
    parts = [p for p in text.split() if p and p not in stop]
    return " ".join(parts)


_ITEM_REFERENCE_TYPE_SUFFIXES = (
    "siegelring", "fingerring", "ring", "ohrring", "ohrringe", "brosche", "halskette", "kette",
    "armband", "guertel", "gurtel", "umhang", "mantel", "handschutz", "handschuh", "handschuhe",
    "stiefel", "schuhe", "helm", "hut", "maske", "schleier", "robe", "brust", "ruestung", "rustung", "hose",
)


def _item_reference_stem(token: str) -> str:
    value = str(token or "")
    for suffix in ("ungen", "ern", "ens", "en", "es", "er", "em", "e", "s"):
        if len(value) >= max(6, len(suffix) + 3) and value.endswith(suffix):
            return value[:-len(suffix)]
    return value


def _item_reference_is_type_token(token: str) -> bool:
    value = str(token or "")
    return any(value.endswith(suffix) for suffix in _ITEM_REFERENCE_TYPE_SUFFIXES)


def _item_reference_slot_hint(value: Any) -> str:
    tokens = _normalize_item_reference(value).split()
    for token in tokens:
        if token.endswith("ohrring") or token.endswith("ohrringe"):
            return "ohrringe"
        if token.endswith("siegelring") or token.endswith("fingerring") or token == "ring":
            return "ring"
        if token.endswith("brosche"):
            return "brosche"
        if token.endswith("handschutz") or token.endswith("handschuh") or token.endswith("handschuhe"):
            return "handschuhe"
        if token.endswith("robe") or token in {"brust", "ruestung", "rustung"}:
            return "brust"
        if token.endswith("halskette") or token == "kette":
            return "kette"
        if token.endswith("armband"):
            return "armband"
        if token.endswith("guertel") or token.endswith("gurtel"):
            return "gurtel"
        if token.endswith("umhang") or token.endswith("mantel"):
            return "umhang"
        if token.endswith("helm") or token.endswith("hut") or token.endswith("maske") or token.endswith("schleier"):
            return "helm"
        if token.endswith("hose"):
            return "hose"
        if token.endswith("schuhe") or token.endswith("stiefel"):
            return "schuhe"
    return ""


def _item_reference_core_tokens(value: Any) -> set[str]:
    out: set[str] = set()
    for token in _normalize_item_reference(value).split():
        stem = _item_reference_stem(token)
        if len(stem) >= 2 and not _item_reference_is_type_token(stem):
            out.add(stem)
    return out


def _catalog_reference_consistent(item_name: Any, item: dict[str, Any]) -> bool:
    clean_name = str(item_name or "").strip()
    if not clean_name:
        return True
    wanted_slot = _item_reference_slot_hint(clean_name)
    candidate_slot = _item_reference_slot_hint(item.get("sub_category") or item.get("name") or item.get("main_category"))
    wanted_family = "accessory" if wanted_slot in {"ring", "ohrringe", "brosche", "kette", "armband", "gurtel"} else ("armor" if wanted_slot in {"handschuhe", "brust", "umhang", "helm", "hose", "schuhe"} else "")
    candidate_family = _normalize_item_reference(item.get("main_category"))
    if wanted_family and candidate_family and wanted_family not in candidate_family:
        return False
    if wanted_slot and candidate_slot and wanted_slot != candidate_slot:
        return False
    if wanted_slot and not candidate_slot and item.get("sub_category"):
        return False

    wanted_core = _item_reference_core_tokens(clean_name)
    candidate_core = _item_reference_core_tokens(item.get("name"))
    if not wanted_core:
        return True
    if wanted_core & candidate_core:
        return True
    a = "".join(sorted(wanted_core))
    b = "".join(sorted(candidate_core))
    if a and b and (a in b or b in a):
        return True
    return difflib.SequenceMatcher(None, a, b).ratio() >= 0.72


def sync_guild_members(*, guild_id: int, members: list[dict[str, Any]], member_role_id: int = 0) -> dict[str, Any]:
    """Spiegelt die aktuell berechtigten Gildenmitglieder in eine zentrale Tabelle.

    Historische Profile bleiben erhalten. Wer die Gildenrolle verliert, wird nur
    auf ``is_active = false`` gesetzt und verschwindet damit aus aktiven Listen.
    """
    if not _INITIALIZED:
        init_runtime_db()
    now = _now_iso()
    rows: dict[int, dict[str, Any]] = {}
    for raw in members or []:
        if not isinstance(raw, dict):
            continue
        try:
            uid = int(raw.get("user_id") or raw.get("id") or 0)
        except Exception:
            uid = 0
        if not uid:
            continue
        rows[uid] = dict(raw)

    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id, is_active FROM guild_members WHERE guild_id = %s", (int(guild_id),))
                    existing = {int(r.get("user_id") or 0): bool(r.get("is_active")) for r in (cur.fetchall() or [])}
                    for uid, raw in rows.items():
                        roles = raw.get("roles") if isinstance(raw.get("roles"), list) else []
                        profile = raw.get("profile") if isinstance(raw.get("profile"), dict) else {}
                        cur.execute(
                            """
                            INSERT INTO guild_members (
                                guild_id, user_id, server_name, discord_username, avatar_url,
                                ingame_name, main_role, gearscore, member_role_id, is_active,
                                joined_at, first_seen_at, last_seen_at, left_at,
                                roles_json, profile_json, updated_at
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,%s,%s,NULL,%s,%s,%s)
                            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                                server_name = EXCLUDED.server_name,
                                discord_username = EXCLUDED.discord_username,
                                avatar_url = EXCLUDED.avatar_url,
                                ingame_name = EXCLUDED.ingame_name,
                                main_role = EXCLUDED.main_role,
                                gearscore = EXCLUDED.gearscore,
                                member_role_id = EXCLUDED.member_role_id,
                                is_active = TRUE,
                                joined_at = COALESCE(NULLIF(EXCLUDED.joined_at,''), guild_members.joined_at),
                                last_seen_at = EXCLUDED.last_seen_at,
                                left_at = NULL,
                                roles_json = EXCLUDED.roles_json,
                                profile_json = EXCLUDED.profile_json,
                                updated_at = EXCLUDED.updated_at
                            """,
                            (
                                int(guild_id), uid,
                                str(raw.get("server_name") or raw.get("display_name") or ""),
                                str(raw.get("discord_username") or raw.get("discord_name") or ""),
                                str(raw.get("avatar_url") or ""),
                                str(raw.get("ingame_name") or ""),
                                str(raw.get("main_role") or ""),
                                str(raw.get("gearscore") or ""),
                                int(member_role_id or raw.get("member_role_id") or 0),
                                str(raw.get("joined_at") or ""), now, now,
                                json.dumps(roles, ensure_ascii=False),
                                json.dumps(profile, ensure_ascii=False), now,
                            ),
                        )
                    inactive_ids = [uid for uid, was_active in existing.items() if was_active and uid not in rows]
                    for uid in inactive_ids:
                        cur.execute(
                            """
                            UPDATE guild_members
                            SET is_active = FALSE, left_at = COALESCE(left_at, %s), updated_at = %s
                            WHERE guild_id = %s AND user_id = %s
                            """,
                            (now, now, int(guild_id), int(uid)),
                        )
                conn.commit()
                return {"ok": True, "backend": _BACKEND, "active": len(rows), "deactivated": len(inactive_ids)}
            finally:
                conn.close()

        conn = _sqlite_connect()
        try:
            existing_rows = conn.execute("SELECT user_id, is_active FROM guild_members WHERE guild_id = ?", (int(guild_id),)).fetchall()
            existing = {int(r["user_id"]): bool(r["is_active"]) for r in existing_rows}
            for uid, raw in rows.items():
                roles = raw.get("roles") if isinstance(raw.get("roles"), list) else []
                profile = raw.get("profile") if isinstance(raw.get("profile"), dict) else {}
                conn.execute(
                    """
                    INSERT INTO guild_members (
                        guild_id, user_id, server_name, discord_username, avatar_url,
                        ingame_name, main_role, gearscore, member_role_id, is_active,
                        joined_at, first_seen_at, last_seen_at, left_at,
                        roles_json, profile_json, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?)
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET
                        server_name=excluded.server_name,
                        discord_username=excluded.discord_username,
                        avatar_url=excluded.avatar_url,
                        ingame_name=excluded.ingame_name,
                        main_role=excluded.main_role,
                        gearscore=excluded.gearscore,
                        member_role_id=excluded.member_role_id,
                        is_active=1,
                        joined_at=CASE WHEN excluded.joined_at <> '' THEN excluded.joined_at ELSE guild_members.joined_at END,
                        last_seen_at=excluded.last_seen_at,
                        left_at=NULL,
                        roles_json=excluded.roles_json,
                        profile_json=excluded.profile_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        int(guild_id), uid,
                        str(raw.get("server_name") or raw.get("display_name") or ""),
                        str(raw.get("discord_username") or raw.get("discord_name") or ""),
                        str(raw.get("avatar_url") or ""),
                        str(raw.get("ingame_name") or ""),
                        str(raw.get("main_role") or ""),
                        str(raw.get("gearscore") or ""),
                        int(member_role_id or raw.get("member_role_id") or 0),
                        str(raw.get("joined_at") or ""), now, now, None,
                        json.dumps(roles, ensure_ascii=False),
                        json.dumps(profile, ensure_ascii=False), now,
                    ),
                )
            inactive_ids = [uid for uid, was_active in existing.items() if was_active and uid not in rows]
            for uid in inactive_ids:
                conn.execute(
                    "UPDATE guild_members SET is_active = 0, left_at = COALESCE(left_at, ?), updated_at = ? WHERE guild_id = ? AND user_id = ?",
                    (now, now, int(guild_id), int(uid)),
                )
            conn.commit()
            return {"ok": True, "backend": _BACKEND, "active": len(rows), "deactivated": len(inactive_ids)}
        finally:
            conn.close()


def fetch_guild_members(guild_id: int, *, active_only: bool = True) -> list[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    sql = "SELECT * FROM guild_members WHERE guild_id = %s"
                    params: tuple[Any, ...] = (int(guild_id),)
                    if active_only:
                        sql += " AND is_active = TRUE"
                    sql += " ORDER BY COALESCE(NULLIF(ingame_name,''), NULLIF(server_name,''), user_id::text)"
                    cur.execute(sql, params)
                    rows = [dict(r) for r in (cur.fetchall() or [])]
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                sql = "SELECT * FROM guild_members WHERE guild_id = ?"
                if active_only:
                    sql += " AND is_active = 1"
                sql += " ORDER BY CASE WHEN ingame_name <> '' THEN ingame_name ELSE server_name END"
                rows = [dict(r) for r in conn.execute(sql, (int(guild_id),)).fetchall()]
            finally:
                conn.close()
    for row in rows:
        row["is_active"] = bool(row.get("is_active"))
        row["roles"] = _json_loads(row.pop("roles_json", "[]"), [])
        row["profile"] = _json_loads(row.pop("profile_json", "{}"), {})
    return rows


def get_guild_item_link(guild_id: int, reference_type: str, reference_key: str) -> Optional[dict[str, Any]]:
    if not _INITIALIZED:
        init_runtime_db()
    rtype = str(reference_type or "local_item").strip() or "local_item"
    rkey = str(reference_key or "").strip()
    if not rkey:
        return None
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM guild_item_links WHERE guild_id=%s AND reference_type=%s AND reference_key=%s", (int(guild_id), rtype, rkey))
                    row = cur.fetchone()
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                row = conn.execute("SELECT * FROM guild_item_links WHERE guild_id=? AND reference_type=? AND reference_key=?", (int(guild_id), rtype, rkey)).fetchone()
            finally:
                conn.close()
    if not row:
        return None
    out = dict(row)
    out["aliases"] = _json_loads(out.pop("aliases_json", "[]"), [])
    return out


def upsert_guild_item_link(*, guild_id: int, reference_type: str, reference_key: str, item: dict[str, Any], match_method: str, confidence: float = 1.0, aliases: Optional[list[str]] = None) -> bool:
    if not _INITIALIZED:
        init_runtime_db()
    rtype = str(reference_type or "local_item").strip() or "local_item"
    rkey = str(reference_key or "").strip()
    try:
        catalog_item_id = int(item.get("id") or item.get("catalog_item_id") or 0)
    except Exception:
        catalog_item_id = 0
    if not rkey or not catalog_item_id:
        return False
    now = _now_iso()
    values = (
        int(guild_id), rtype, rkey, catalog_item_id,
        str(item.get("source_item_id") or ""), str(item.get("name") or item.get("canonical_name") or ""),
        str(item.get("source_url") or ""), str(item.get("manual_image_url") or item.get("image_url") or item.get("icon_url") or ""),
        str(match_method or "manual"), float(confidence or 0), json.dumps(aliases or [], ensure_ascii=False), now,
    )
    with _DB_LOCK:
        if _BACKEND == "postgres":
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO guild_item_links (guild_id, reference_type, reference_key, catalog_item_id, source_item_id, canonical_name, source_url, image_url, match_method, confidence, aliases_json, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (guild_id, reference_type, reference_key) DO UPDATE SET
                            catalog_item_id=EXCLUDED.catalog_item_id, source_item_id=EXCLUDED.source_item_id,
                            canonical_name=EXCLUDED.canonical_name, source_url=EXCLUDED.source_url,
                            image_url=EXCLUDED.image_url, match_method=EXCLUDED.match_method,
                            confidence=EXCLUDED.confidence, aliases_json=EXCLUDED.aliases_json,
                            updated_at=EXCLUDED.updated_at
                        """,
                        values,
                    )
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _sqlite_connect()
            try:
                conn.execute(
                    """
                    INSERT INTO guild_item_links (guild_id, reference_type, reference_key, catalog_item_id, source_item_id, canonical_name, source_url, image_url, match_method, confidence, aliases_json, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (guild_id, reference_type, reference_key) DO UPDATE SET
                        catalog_item_id=excluded.catalog_item_id, source_item_id=excluded.source_item_id,
                        canonical_name=excluded.canonical_name, source_url=excluded.source_url,
                        image_url=excluded.image_url, match_method=excluded.match_method,
                        confidence=excluded.confidence, aliases_json=excluded.aliases_json,
                        updated_at=excluded.updated_at
                    """,
                    values,
                )
                conn.commit()
            finally:
                conn.close()
    return True


def search_catalog_items(query: str = "", *, limit: int = 25) -> list[dict[str, Any]]:
    """Sucht aktive Katalogitems für Discord-Autocomplete/Picker.

    Die Funktion liefert immer die feste ``item_catalog.id`` mit aus. Bei SQLite
    ist der externe Questlog-Katalog nicht vorhanden, deshalb wird dort leer
    zurückgegeben statt auf unsichere Namen auszuweichen.
    """
    if not _INITIALIZED:
        init_runtime_db()
    if _BACKEND != "postgres":
        return []
    text = str(query or "").strip()
    safe_limit = max(1, min(int(limit or 25), 50))
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if text:
                cur.execute(
                    """
                    SELECT id, source, source_url, source_item_id, name,
                           main_category, sub_category, rarity, image_url, icon_url
                    FROM item_catalog
                    WHERE is_active = TRUE AND name ILIKE %s
                    ORDER BY CASE WHEN lower(name) = lower(%s) THEN 0
                                  WHEN lower(name) LIKE lower(%s) THEN 1
                                  ELSE 2 END,
                             name ASC
                    LIMIT %s
                    """,
                    (f"%{text}%", text, f"{text}%", safe_limit),
                )
            else:
                cur.execute(
                    """
                    SELECT id, source, source_url, source_item_id, name,
                           main_category, sub_category, rarity, image_url, icon_url
                    FROM item_catalog
                    WHERE is_active = TRUE
                    ORDER BY updated_at DESC NULLS LAST, name ASC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
            return [dict(row) for row in (cur.fetchall() or [])]
    finally:
        conn.close()


def resolve_catalog_item_reference(*, guild_id: int, local_item_id: str = "", item_name: str = "", catalog_item_id: int = 0, source_item_id: str = "", allow_fuzzy: bool = True) -> Optional[dict[str, Any]]:
    """Löst eine Bot-/Auktionsreferenz auf die feste ``item_catalog.id`` auf.

    Priorität: explizite ID → gespeicherter Link → Questlog/source_item_id →
    exakter Name → sehr sicherer Fuzzy-Treffer. Unsichere Treffer werden nicht
    gespeichert, damit ähnlich benannte Ringe nicht falsch verknüpft werden.
    """
    if not _INITIALIZED:
        init_runtime_db()
    if _BACKEND != "postgres":
        return None

    local_key = str(local_item_id or "").strip()
    alias_key = _normalize_item_reference(item_name)
    try:
        explicit_id = int(catalog_item_id or 0)
    except Exception:
        explicit_id = 0

    def fetch_one(where: str, params: tuple[Any, ...]) -> Optional[dict[str, Any]]:
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT ic.id, ic.source, ic.source_url, ic.source_item_id, ic.name,
                           ic.main_category, ic.sub_category, ic.rarity, ic.stats,
                           ic.abilities, ic.traits, ic.image_url, ic.icon_url,
                           ov.image_url AS manual_image_url
                    FROM item_catalog ic
                    LEFT JOIN item_catalog_image_overrides ov ON ov.source_url = ic.source_url
                    WHERE ic.is_active = TRUE AND ({where})
                    LIMIT 1
                    """,
                    params,
                )
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            conn.close()

    item: Optional[dict[str, Any]] = None
    method = ""
    confidence = 0.0

    if explicit_id:
        candidate = fetch_one("ic.id = %s", (explicit_id,))
        if candidate and _catalog_reference_consistent(item_name, candidate):
            item = candidate
            method, confidence = "catalog_id", 1.0

    if item is None and local_key:
        link = get_guild_item_link(int(guild_id), "local_item", local_key)
        if link and int(link.get("catalog_item_id") or 0):
            candidate = fetch_one("ic.id = %s", (int(link.get("catalog_item_id") or 0),))
            if candidate and _catalog_reference_consistent(item_name, candidate):
                item = candidate
                method, confidence = "stored_local_link", float(link.get("confidence") or 1.0)

    if item is None and alias_key:
        link = get_guild_item_link(int(guild_id), "alias", alias_key)
        if link and int(link.get("catalog_item_id") or 0):
            candidate = fetch_one("ic.id = %s", (int(link.get("catalog_item_id") or 0),))
            if candidate and _catalog_reference_consistent(item_name, candidate):
                item = candidate
                method, confidence = "stored_alias", float(link.get("confidence") or 1.0)

    source_candidates = [str(source_item_id or "").strip(), local_key]
    if item is None:
        for source in [x for x in source_candidates if x and not x.startswith("junk:")]:
            candidate = fetch_one("ic.source_item_id = %s", (source,))
            if candidate and _catalog_reference_consistent(item_name, candidate):
                item = candidate
                method, confidence = "source_item_id", 1.0
                break

    clean_name = str(item_name or "").strip()
    if item is None and clean_name:
        item = fetch_one("lower(ic.name) = lower(%s)", (clean_name,))
        if item:
            method, confidence = "exact_name", 0.99

    if item is None and clean_name and allow_fuzzy:
        core_tokens = sorted(_item_reference_core_tokens(clean_name), key=len, reverse=True)
        all_tokens = [t for t in alias_key.split() if len(t) >= 3]
        query = (core_tokens or all_tokens or [clean_name])[0]
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ic.id, ic.source, ic.source_url, ic.source_item_id, ic.name,
                           ic.main_category, ic.sub_category, ic.rarity, ic.stats,
                           ic.abilities, ic.traits, ic.image_url, ic.icon_url,
                           ov.image_url AS manual_image_url
                    FROM item_catalog ic
                    LEFT JOIN item_catalog_image_overrides ov ON ov.source_url = ic.source_url
                    WHERE ic.is_active = TRUE AND ic.name ILIKE %s
                    LIMIT 160
                    """,
                    (f"%{query}%",),
                )
                candidates = [dict(r) for r in (cur.fetchall() or [])]
        finally:
            conn.close()

        wanted_slot = _item_reference_slot_hint(clean_name)
        wanted_core = _item_reference_core_tokens(clean_name)
        best = None
        best_score = 0.0
        for cand in candidates:
            if not _catalog_reference_consistent(clean_name, cand):
                continue
            candidate_slot = _item_reference_slot_hint(cand.get("sub_category") or cand.get("name") or cand.get("main_category"))
            candidate_core = _item_reference_core_tokens(cand.get("name"))
            seq = difflib.SequenceMatcher(None, alias_key, _normalize_item_reference(cand.get("name"))).ratio()
            overlap = len(wanted_core & candidate_core) / max(1, len(wanted_core | candidate_core))
            score = seq * 0.48 + overlap * 0.42
            if wanted_slot and candidate_slot == wanted_slot:
                score += 0.18
            if score > best_score:
                best, best_score = cand, score
        # Slotgleichheit + echter Namenskern reichen für Gilden-Kurznamen wie
        # "Ring DaVinci" -> "Da Vincis Siegelring". Slotfremde Treffer sind oben blockiert.
        if best is not None and best_score >= 0.72:
            item = best
            method, confidence = "slot_safe_fuzzy_name", float(min(best_score, 1.0))

    if item is None:
        return None
    item = dict(item)
    item["catalog_item_id"] = int(item.get("id") or 0)
    item["match_method"] = method
    item["match_confidence"] = confidence
    if local_key:
        upsert_guild_item_link(guild_id=int(guild_id), reference_type="local_item", reference_key=local_key, item=item, match_method=method, confidence=confidence, aliases=[clean_name] if clean_name else [])
    if alias_key:
        upsert_guild_item_link(guild_id=int(guild_id), reference_type="alias", reference_key=alias_key, item=item, match_method=method, confidence=confidence, aliases=[clean_name] if clean_name else [])
    return item
