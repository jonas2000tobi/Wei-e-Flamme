from __future__ import annotations

import html
import json
import os
import secrets
import csv
import re
import io
import base64
import hashlib
import hmac
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="Ebo Dashboard", version="0.9.0")
security = HTTPBasic(auto_error=False)

STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

ASSET_VER = "ebo-theme-root-1"


def _database_url() -> str:
    return str(os.getenv("DATABASE_URL") or "").strip()


def _normalized_database_url() -> str:
    url = _database_url()
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _pg_connect():
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    return psycopg.connect(_normalized_database_url(), row_factory=dict_row, connect_timeout=10)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
# Das Dashboard unterstützt jetzt zwei Modi:
# 1) Basic Auth über DASHBOARD_USERNAME / DASHBOARD_PASSWORD (Fallback/Test)
# 2) Discord OAuth über DASHBOARD_DISCORD_CLIENT_ID / DASHBOARD_DISCORD_CLIENT_SECRET
#
# Discord OAuth ist bewusst optional. Wenn es nicht konfiguriert ist, bleibt dein
# bisheriger Passwort-Login unverändert.

SESSION_COOKIE = "ebo_dashboard_session"
STATE_COOKIE = "ebo_dashboard_state"
DISCORD_API_BASE = "https://discord.com/api/v10"
DISCORD_OAUTH_TOKEN_URL = "https://discord.com/api/oauth2/token"


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name) or default).strip()


def _discord_oauth_enabled() -> bool:
    return bool(_env("DASHBOARD_DISCORD_CLIENT_ID") and _env("DASHBOARD_DISCORD_CLIENT_SECRET"))


def _auth_mode() -> str:
    mode = _env("DASHBOARD_AUTH_MODE", "hybrid").lower()
    if mode not in {"basic", "discord", "hybrid"}:
        return "hybrid"
    return mode


def _session_secret() -> str:
    # Eigene Variable ist besser. Fallback auf Dashboard-Passwort, damit bestehende Setups nicht brechen.
    return _env("DASHBOARD_SESSION_SECRET") or _env("DASHBOARD_PASSWORD") or "dev-dashboard-secret-change-me"


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(value: str) -> bytes:
    value = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value.encode("ascii"))


def _sign(value: str) -> str:
    return hmac.new(_session_secret().encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def _make_token(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body = _b64e(raw)
    return f"{body}.{_sign(body)}"


def _read_token(token: str) -> Optional[dict[str, Any]]:
    try:
        body, sig = str(token or "").split(".", 1)
        if not hmac.compare_digest(sig, _sign(body)):
            return None
        payload = json.loads(_b64d(body).decode("utf-8"))
        exp = int(payload.get("exp") or 0)
        if exp and exp < int(time.time()):
            return None
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _current_user(request: Request) -> Optional[dict[str, Any]]:
    return _read_token(request.cookies.get(SESSION_COOKIE, ""))


def _csv_ids(value: str) -> set[str]:
    return {x.strip() for x in str(value or "").replace(";", ",").split(",") if x.strip()}


def _configured_member_role_id_from_snapshot() -> str:
    try:
        payload = _snapshot_payload()
        snap = payload.get("snapshot") or {}
        member_filter = ((snap.get("settings") or {}).get("member_filter") or (snap.get("guild") or {}).get("member_filter") or {})
        if isinstance(member_filter, dict):
            return str(member_filter.get("role_id") or "").strip()
    except Exception:
        return ""
    return ""


def _configured_member_role_name_from_snapshot() -> str:
    try:
        payload = _snapshot_payload()
        snap = payload.get("snapshot") or {}
        member_filter = ((snap.get("settings") or {}).get("member_filter") or (snap.get("guild") or {}).get("member_filter") or {})
        if isinstance(member_filter, dict):
            return str(member_filter.get("role_name") or "").strip()
    except Exception:
        return ""
    return ""


def _allowed_role_ids() -> set[str]:
    explicit = _csv_ids(_env("DASHBOARD_ALLOWED_ROLE_IDS"))
    admin = _csv_ids(_env("DASHBOARD_ADMIN_ROLE_IDS"))
    member = _csv_ids(_env("DASHBOARD_MEMBER_ROLE_IDS"))
    configured = _env("DASHBOARD_MEMBER_ROLE_ID") or _configured_member_role_id_from_snapshot()
    out = set()
    out.update(explicit)
    out.update(admin)
    out.update(member)
    if configured:
        out.add(str(configured))
    return out


def _admin_role_ids() -> set[str]:
    return _csv_ids(_env("DASHBOARD_ADMIN_ROLE_IDS"))


def _snapshot_auth_lists() -> dict[str, Any]:
    """Auth-Listen aus dem aktuellen Bot-Snapshot.

    Der Bot schreibt erlaubte Member-IDs und Admin-Member-IDs in Postgres.
    Dadurch muss das Web-Dashboard beim Discord-Login keine Rollen direkt
    über identify abfragen. Das vermeidet Discord-403-Probleme
    und ist für Railway/Custom-Domains robuster.
    """
    payload = _snapshot_payload()
    snap = payload.get("snapshot") or {}
    auth = snap.get("auth") or {}
    allowed = {str(x) for x in (auth.get("allowed_member_ids") or []) if str(x).strip()}
    admins = {str(x) for x in (auth.get("admin_member_ids") or []) if str(x).strip()}
    return {
        "ok": bool(payload.get("ok")),
        "auth": auth,
        "allowed_member_ids": allowed,
        "admin_member_ids": admins,
        "guild_id": str(payload.get("guild_id") or ((snap.get("guild") or {}).get("id") or "")),
    }


def _cookie_secure() -> bool:
    return _env("DASHBOARD_COOKIE_SECURE", "1") not in {"0", "false", "False", "nein", "no"}


def _basic_auth(credentials: Optional[HTTPBasicCredentials]) -> bool:
    password = _env("DASHBOARD_PASSWORD")
    if not password:
        # Für den allerersten Test erlaubt. Auf Railway danach unbedingt setzen oder Discord OAuth nutzen.
        return True
    username = _env("DASHBOARD_USERNAME", "admin") or "admin"
    if not credentials:
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate": "Basic"})
    ok_user = secrets.compare_digest(credentials.username, username)
    ok_pw = secrets.compare_digest(credentials.password, password)
    if not (ok_user and ok_pw):
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate": "Basic"})
    return True


def _auth(request: Request, credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> bool:
    mode = _auth_mode()

    if mode in {"discord", "hybrid"} and _discord_oauth_enabled():
        user = _current_user(request)
        if user:
            return True
        if mode == "discord":
            raise HTTPException(status_code=303, detail="Login required", headers={"Location": f"/login?next={urllib.parse.quote(str(request.url.path))}"})

    # Hybrid/Basic-Fallback bleibt absichtlich erhalten, damit du dich nicht aussperrst.
    if mode in {"basic", "hybrid"}:
        return _basic_auth(credentials)

    raise HTTPException(status_code=303, detail="Login required", headers={"Location": "/login"})


def _request_json(url: str, *, method: str = "GET", data: Optional[dict[str, Any]] = None, token: str = "") -> dict[str, Any]:
    body = None
    headers = {
        "Accept": "application/json",
        "User-Agent": "Ebo-Dashboard/1.0 (+https://dashboardweb-production-2933.up.railway.app)",
    }
    if data is not None:
        body = urllib.parse.urlencode(data).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:  # nosec - Discord API URL only from constants
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8")[:800]
        except Exception:
            detail = ""
        safe_url = url.replace(_env("DASHBOARD_DISCORD_CLIENT_SECRET"), "<secret>")
        raise RuntimeError(f"Discord HTTP {exc.code} bei {safe_url}: {detail or exc.reason}") from exc



def _clean_external_url(value: str) -> str:
    """Railway/Discord-OAuth URL robust bereinigen.

    Schutz gegen typische Copy-Paste-Fehler:
    - https://https://...
    - Slash am Ende bei Base URL
    - doppelte Slashes im Pfad
    """
    value = str(value or "").strip()
    if value.startswith("https://https://"):
        value = "https://" + value[len("https://https://"):]
    if value.startswith("http://https://"):
        value = "https://" + value[len("http://https://"):]
    if value.startswith("https://http://"):
        value = "http://" + value[len("https://http://"):]
    # Doppelte Slashes im Pfad reduzieren, Scheme behalten.
    if "://" in value:
        scheme, rest = value.split("://", 1)
        while "//" in rest:
            rest = rest.replace("//", "/")
        value = f"{scheme}://{rest}"
    return value.rstrip("/")

def _base_url(request: Request) -> str:
    # Railway setzt üblicherweise Host/Proto korrekt. Bei Custom Domain sonst per Env überschreiben.
    forced = _clean_external_url(_env("DASHBOARD_PUBLIC_BASE_URL"))
    if forced:
        return forced
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return _clean_external_url(f"{proto}://{host}")


def _redirect_uri(request: Request) -> str:
    forced = _clean_external_url(_env("DASHBOARD_DISCORD_REDIRECT_URI"))
    if forced:
        return forced
    return f"{_base_url(request)}/auth/discord/callback"


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _asset(name: str) -> str:
    return f"/static/{name}?v={ASSET_VER}"


def _dt(value: Any) -> str:
    s = str(value or "")
    if not s:
        return "—"
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    except Exception:
        return s[:19]


def _short(value: Any, n: int = 80) -> str:
    s = str(value or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(".", "").replace(",", ".") if isinstance(value, str) and "," in value else value)
    except Exception:
        return default


def _latest_snapshot_row() -> Optional[dict[str, Any]]:
    if not _database_url():
        return None
    guild_id = str(os.getenv("DASHBOARD_GUILD_ID") or "").strip()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if guild_id:
                cur.execute(
                    """
                    SELECT * FROM dashboard_snapshots
                    WHERE guild_id = %s
                    ORDER BY published_at DESC, id DESC
                    LIMIT 1
                    """,
                    (int(guild_id),),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM dashboard_snapshots
                    ORDER BY published_at DESC, id DESC
                    LIMIT 1
                    """
                )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _snapshot_payload() -> dict[str, Any]:
    row = _latest_snapshot_row()
    if not row:
        return {"ok": False, "error": "Noch kein Dashboard-Snapshot in Postgres gefunden."}
    try:
        snap = json.loads(row.get("snapshot_json") or "{}")
    except Exception as exc:
        return {"ok": False, "error": f"Snapshot JSON kaputt: {type(exc).__name__}: {exc}"}
    return {
        "ok": True,
        "id": row.get("id"),
        "guild_id": row.get("guild_id"),
        "guild_name": row.get("guild_name"),
        "generated_at": row.get("generated_at"),
        "published_at": row.get("published_at"),
        "snapshot": snap,
    }


# ---------------------------------------------------------------------------
# Safe Admin Actions: Notizen / Prüfmarkierungen
# ---------------------------------------------------------------------------

def _is_dashboard_admin(request: Request) -> bool:
    user = _current_user(request) or {}
    return str(user.get("role") or "") == "admin"


def _admin_auth(request: Request, credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> bool:
    _auth(request, credentials)
    if _is_dashboard_admin(request):
        return True
    # Nur für lokalen Notfall, wenn Discord OAuth komplett deaktiviert ist.
    if _auth_mode() in {"basic", "hybrid"} and not _discord_oauth_enabled():
        return True
    raise HTTPException(status_code=403, detail="Dashboard-Adminrolle erforderlich")


def _safe_guild_id(data: Optional[dict[str, Any]] = None) -> int:
    raw = _env("DASHBOARD_GUILD_ID")
    if not raw and data:
        raw = str(data.get("guild_id") or "")
    try:
        return int(str(raw).strip())
    except Exception:
        return 0


def _ensure_admin_tables() -> None:
    if not _database_url():
        return
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_member_admin_state (
                    guild_id BIGINT NOT NULL,
                    member_user_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ok',
                    note TEXT NOT NULL DEFAULT '',
                    updated_by_id TEXT,
                    updated_by_name TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, member_user_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_admin_action_log (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    actor_id TEXT,
                    actor_name TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_ec_award_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    full_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    partial_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    actor_name TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    processed_at TIMESTAMPTZ,
                    result_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_ec_award_requests_lookup
                ON dashboard_ec_award_requests (guild_id, event_id, status, requested_at DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_loot_action_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    auction_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    amount INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    actor_name TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    processed_at TIMESTAMPTZ,
                    result_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_loot_action_requests_lookup
                ON dashboard_loot_action_requests (guild_id, auction_id, status, requested_at DESC)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _member_admin_state(guild_id: int, user_id: int) -> dict[str, Any]:
    if not _database_url() or not guild_id or not user_id:
        return {}
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT guild_id, member_user_id, status, note, updated_by_id, updated_by_name, updated_at
                FROM dashboard_member_admin_state
                WHERE guild_id = %s AND member_user_id = %s
                """,
                (guild_id, int(user_id)),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def _all_member_admin_states(guild_id: int) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id:
        return []
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT guild_id, member_user_id, status, note, updated_by_id, updated_by_name, updated_at
                FROM dashboard_member_admin_state
                WHERE guild_id = %s
                ORDER BY updated_at DESC
                """,
                (guild_id,),
            )
            return [dict(r) for r in (cur.fetchall() or [])]
    finally:
        conn.close()


def _admin_action_log(guild_id: int, limit: int = 100) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id:
        return []
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json, created_at
                FROM dashboard_admin_action_log
                WHERE guild_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (guild_id, int(limit)),
            )
            return [dict(r) for r in (cur.fetchall() or [])]
    finally:
        conn.close()


def _save_member_admin_state(guild_id: int, user_id: int, status: str, note: str, actor: dict[str, Any]) -> None:
    if not _database_url() or not guild_id or not user_id:
        raise RuntimeError("DATABASE_URL/Guild/User fehlt")
    status = str(status or "ok").strip().lower()
    if status not in {"ok", "check", "watch", "critical"}:
        status = "ok"
    note = str(note or "").strip()[:4000]
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("user_id") or "")
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_member_admin_state
                    (guild_id, member_user_id, status, note, updated_by_id, updated_by_name, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (guild_id, member_user_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    note = EXCLUDED.note,
                    updated_by_id = EXCLUDED.updated_by_id,
                    updated_by_name = EXCLUDED.updated_by_name,
                    updated_at = NOW()
                """,
                (guild_id, int(user_id), status, note, actor_id, actor_name),
            )
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (guild_id, "member_state_save", "member", str(user_id), actor_id, actor_name, json.dumps({"status": status, "note_length": len(note)}, ensure_ascii=False)),
            )
        conn.commit()
    finally:
        conn.close()


def _delete_member_admin_state(guild_id: int, user_id: int, actor: dict[str, Any]) -> None:
    if not _database_url() or not guild_id or not user_id:
        raise RuntimeError("DATABASE_URL/Guild/User fehlt")
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("user_id") or "")
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM dashboard_member_admin_state
                WHERE guild_id = %s AND member_user_id = %s
                """,
                (guild_id, int(user_id)),
            )
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (guild_id, "member_state_delete", "member", str(user_id), actor_id, actor_name, "{}"),
            )
        conn.commit()
    finally:
        conn.close()


def _status_label(value: Any) -> str:
    v = str(value or "ok").lower()
    return {
        "ok": "✅ OK",
        "check": "🔎 Prüfen",
        "watch": "👀 Beobachten",
        "critical": "⛔ Kritisch",
    }.get(v, "✅ OK")


def _admin_member_panel(data: dict[str, Any], user_id: int, current_user: Optional[dict[str, Any]]) -> str:
    if not current_user or str(current_user.get("role") or "") != "admin":
        return ""
    guild_id = _safe_guild_id(data)
    state = _member_admin_state(guild_id, int(user_id)) if guild_id else {}
    status = str(state.get("status") or "ok").lower()
    note = str(state.get("note") or "")
    def selected(v: str) -> str:
        return " selected" if status == v else ""
    last = "Noch keine interne Notiz."
    if state:
        last = f"Zuletzt geändert: {_dt(state.get('updated_at'))} · von {_e(state.get('updated_by_name') or state.get('updated_by_id') or 'unbekannt')}"
    return f"""
    <section class="panel" id="leitung">
      <h2>🛡️ Leitungsnotiz</h2>
      <p class="muted">Sichere Admin-Funktion: speichert nur interne Dashboard-Notizen/Prüfstatus. EC, Loot, Needs und Events werden nicht verändert.</p>
      <form method="post" action="/admin/member/{int(user_id)}/save" style="display:grid; gap:10px; max-width:760px;">
        <label>Status<br>
          <select name="status" style="width:260px; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);">
            <option value="ok"{selected('ok')}>✅ OK</option>
            <option value="check"{selected('check')}>🔎 Prüfen</option>
            <option value="watch"{selected('watch')}>👀 Beobachten</option>
            <option value="critical"{selected('critical')}>⛔ Kritisch</option>
          </select>
        </label>
        <label>Interne Notiz<br>
          <textarea name="note" rows="5" maxlength="4000" style="width:100%; padding:12px; border-radius:12px; background:#08090d; color:var(--text); border:1px solid var(--line); resize:vertical;">{_e(note)}</textarea>
        </label>
        <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
          <button class="btn" type="submit" style="border:0; cursor:pointer;">Speichern</button>
          <button class="btn" formaction="/admin/member/{int(user_id)}/clear" formmethod="post" type="submit" style="border:0; cursor:pointer; background:#303442; color:var(--text);">Notiz löschen</button>
          <span class="muted">{last}</span>
        </div>
      </form>
    </section>
    """


def _render_admin_actions_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Leitung · Ebo Dashboard", f"<section class='panel'><h1>🛡️ Leitung</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    states = _all_member_admin_states(guild_id)
    names = _profile_name_map(snap)
    rows = []
    for st in states:
        uid = _user_id(st.get("member_user_id"))
        rows.append([
            _member_link(uid, names.get(uid, f"User {uid}")),
            _status_label(st.get("status")),
            _short(st.get("note"), 220) or "—",
            st.get("updated_by_name") or st.get("updated_by_id") or "—",
            _dt(st.get("updated_at")),
        ])
    logs = _admin_action_log(guild_id, limit=120)
    log_rows = []
    for lg in logs:
        uid = _user_id(lg.get("target_id"))
        log_rows.append([
            _dt(lg.get("created_at")),
            lg.get("action_type"),
            _member_link(uid, names.get(uid, f"User {uid}")) if uid else lg.get("target_id"),
            lg.get("actor_name") or lg.get("actor_id") or "—",
        ])
    body = f"""
    <nav class="topnav"><a href="/">Kommando</a><a href="/members">Mitglieder</a><a href="/audit">Audit</a><a href="/settings">Einstellungen</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Ebene 3 · Schritt 1</div>
        <h1>🛡️ Leitungsbereich</h1>
        <p>Sichere Admin-Aktionen: interne Notizen und Prüfmarkierungen. Keine EC-, Loot-, Event- oder Need-Änderungen.</p>
      </div>
      <a class="btn" href="/members">Mitglied suchen</a>
    </section>
    <section class="grid">
      {_card('Interne Markierungen', len(rows), 'Mitglieder mit Notiz/Status')}
      {_card('Admin-Aktionen', len(logs), 'letzte sichere Web-Aktionen')}
      {_card('Guild-ID', guild_id or '—', 'Dashboard-Kontext')}
      {_card('Schreibrechte', 'Notizen', 'noch kein EC/Loot/Need-Write')}
    </section>
    <section class="panel"><h2>👥 Markierte Mitglieder</h2>{_table(['Mitglied','Status','Notiz','Geändert von','Geändert am'], rows, placeholder='Markierungen durchsuchen…')}</section>
    <section class="panel"><h2>🧾 Web-Admin-Aktionslog</h2>{_table(['Zeit','Aktion','Ziel','Akteur'], log_rows, placeholder='Adminlog durchsuchen…')}</section>
    """
    return _html_shell("Leitung · Ebo Dashboard", body)


def _card(title: str, value: Any, sub: str = "") -> str:
    return f"""
    <div class="card">
      <div class="card-title">{_e(title)}</div>
      <div class="card-value">{_e(value)}</div>
      <div class="card-sub">{_e(sub)}</div>
    </div>
    """


def _raw(html_value: str) -> dict[str, str]:
    return {"__html__": str(html_value or "")}


def _cell(value: Any) -> str:
    if isinstance(value, dict) and "__html__" in value:
        return str(value.get("__html__") or "")
    return _e(value)


def _member_link(user_id: Any, label: Any) -> dict[str, str]:
    uid = _user_id(user_id)
    text = _e(label or f"User {uid}")
    if not uid:
        return _raw(text)
    return _raw(f'<a class="link" href="/member/{uid}">{text}</a>')


def _event_link(event_id: Any, label: Any) -> dict[str, str]:
    eid = str(event_id or "").strip()
    text = _e(label or eid or "Event")
    if not eid:
        return _raw(text)
    return _raw(f'<a class="link" href="/event/{_e(eid)}">{text}</a>')


def _auction_link(auction_id: Any, label: Any) -> dict[str, str]:
    aid = str(auction_id or "").strip()
    text = _e(label or aid or "Auktion")
    if not aid:
        return _raw(text)
    return _raw(f'<a class="link" href="/auction/{_e(aid)}">{text}</a>')


def _table(headers: list[str], rows: list[list[Any]], *, searchable: bool = True, placeholder: str = "Tabelle durchsuchen…") -> str:
    if not rows:
        return '<div class="empty">Keine Daten vorhanden.</div>'
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{_cell(c)}</td>" for c in row) + "</tr>" for row in rows)
    search = f'<input class="table-search" type="search" placeholder="{_e(placeholder)}" oninput="filterNextTable(this)">' if searchable else ""
    return f"{search}<div class='table-wrap'><table class='searchable-table'><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def _profile_name_map(snap: dict[str, Any]) -> dict[int, str]:
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    names: dict[int, str] = {}
    for p in profiles:
        if not isinstance(p, dict):
            continue
        try:
            uid = int(p.get("user_id") or 0)
        except Exception:
            uid = 0
        if uid:
            names[uid] = str(p.get("display_name") or p.get("ingame_name") or f"User {uid}")
    return names


def _fmt_ec(value: Any) -> str:
    n = _num(value)
    if abs(n - round(n)) < 0.0001:
        return str(int(round(n)))
    return f"{n:.1f}"


def _user_id(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _balance_map(snap: dict[str, Any]) -> dict[int, float]:
    balances = (((snap.get("ec") or {}).get("balances") or {}).get("top") or [])
    out: dict[int, float] = {}
    for b in balances:
        if not isinstance(b, dict):
            continue
        uid = _user_id(b.get("user_id") or b.get("member_id") or b.get("discord_id"))
        if uid:
            out[uid] = _num(b.get("balance"), 0)
    return out


def _need_user_ids(snap: dict[str, Any]) -> set[int]:
    needs = ((snap.get("loot") or {}).get("needs") or {})
    ids: set[int] = set()
    for n in needs.get("items") or []:
        if not isinstance(n, dict):
            continue
        uid = _user_id(n.get("user_id") or n.get("member_id") or n.get("discord_id"))
        if uid:
            ids.add(uid)
    return ids


def _needs_by_user(snap: dict[str, Any]) -> dict[int, dict[str, Any]]:
    needs = ((snap.get("loot") or {}).get("needs") or {})
    rows = needs.get("items") or needs.get("sample") or []
    out: dict[int, dict[str, Any]] = {}
    for n in rows:
        if not isinstance(n, dict):
            continue
        uid = _user_id(n.get("user_id") or n.get("member_id") or n.get("discord_id"))
        if uid:
            out[uid] = n
    return out


def _tx_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    txs = (((snap.get("ec") or {}).get("transactions") or {}).get("recent") or [])
    out = []
    for tx in txs:
        if not isinstance(tx, dict):
            continue
        uid = _user_id(tx.get("user_id") or tx.get("target_user_id") or tx.get("member_id"))
        if uid == int(user_id):
            out.append(tx)
    return out[:limit]


def _voice_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    sessions = ((snap.get("voice") or {}).get("recent_sessions") or [])
    out = []
    for v in sessions:
        if not isinstance(v, dict):
            continue
        uid = _user_id(v.get("user_id") or v.get("member_id"))
        if uid == int(user_id):
            out.append(v)
    return out[:limit]


def _auctions_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    out = []
    for a in auctions:
        if not isinstance(a, dict):
            continue
        if _user_id(a.get("top_bid_user_id")) == int(user_id) or _user_id(a.get("winner_user_id")) == int(user_id):
            out.append(a)
    return out[:limit]


def _need_list_html(title: str, items: Any) -> str:
    arr = items if isinstance(items, list) else []
    if not arr:
        return f"<h3>{_e(title)}</h3><div class='empty'>Keine Einträge.</div>"
    lis = "".join(f"<li>{_e(x)}</li>" for x in arr[:80])
    more = f"<p class='muted'>+ {len(arr) - 80} weitere</p>" if len(arr) > 80 else ""
    return f"<h3>{_e(title)} <span class='pill'>{len(arr)}</span></h3><ul class='need-list'>{lis}</ul>{more}"


def _safe_percent(part: float, total: float) -> str:
    if not total:
        return "0 %"
    return f"{round((part / total) * 100)} %"


def _analytics_from_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    events = ((snap.get("events") or {}).get("items") or [])
    balances = (((snap.get("ec") or {}).get("balances") or {}).get("top") or [])
    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    auction_status = (((snap.get("loot") or {}).get("auctions") or {}).get("by_status") or {})
    needs = (((snap.get("loot") or {}).get("needs") or {}))
    voice = snap.get("voice") or {}
    audit = snap.get("audit") or {}

    roles = Counter(str(p.get("main_role") or "Unbekannt") for p in profiles if isinstance(p, dict))
    gs_values = [_num(p.get("gearscore"), 0) for p in profiles if isinstance(p, dict) and _num(p.get("gearscore"), 0) > 0]
    ec_values = [_num(b.get("balance"), 0) for b in balances if isinstance(b, dict)]
    total_ec = sum(ec_values)
    avg_ec = total_ec / len(ec_values) if ec_values else 0
    avg_gs = sum(gs_values) / len(gs_values) if gs_values else 0

    total_event_participants = sum(int(_num(ev.get("participant_count"), 0)) for ev in events if isinstance(ev, dict))
    total_maybe = sum(int(_num(ev.get("maybe_count"), 0)) for ev in events if isinstance(ev, dict))
    total_no = sum(int(_num(ev.get("no_count"), 0)) for ev in events if isinstance(ev, dict))
    voice_enabled = sum(1 for ev in events if isinstance(ev, dict) and ev.get("voice_enabled"))

    active_statuses = {"open", "active", "running", "bidding", "roll", "sale", "free", "main", "secondary"}
    active_auctions = [a for a in auctions if isinstance(a, dict) and str(a.get("status") or "").lower() in active_statuses]
    total_listed_bids = sum(int(_num(a.get("bid_count"), 0)) for a in auctions if isinstance(a, dict))
    recent_voice_seconds = sum(int(_num(v.get("duration_seconds"), 0)) for v in (voice.get("recent_sessions") or []) if isinstance(v, dict))

    role_member_count = int(_num(((snap.get("guild") or {}).get("member_filter") or {}).get("eligible_count"), 0))
    missing_profiles = max(0, role_member_count - len(profiles))
    missing_ec = max(0, role_member_count - len(ec_values))
    missing_needs = max(0, role_member_count - int(_num(needs.get("user_count"), 0)))

    return {
        "role_distribution": roles.most_common(),
        "role_member_count": role_member_count,
        "missing_profiles": missing_profiles,
        "missing_ec": missing_ec,
        "missing_needs": missing_needs,
        "profile_coverage": _safe_percent(len(profiles), role_member_count),
        "ec_coverage": _safe_percent(len(ec_values), role_member_count),
        "need_coverage": _safe_percent(int(_num(needs.get("user_count"), 0)), role_member_count),
        "avg_gearscore": avg_gs,
        "gearscore_count": len(gs_values),
        "total_ec": total_ec,
        "avg_ec": avg_ec,
        "ec_count": len(ec_values),
        "total_event_participants": total_event_participants,
        "total_maybe": total_maybe,
        "total_no": total_no,
        "voice_enabled_events": voice_enabled,
        "active_auctions": len(active_auctions),
        "auction_status": auction_status,
        "total_listed_bids": total_listed_bids,
        "need_entries_estimated": needs.get("need_entries_estimated", 0),
        "recent_voice_hours": recent_voice_seconds / 3600,
        "audit_total": audit.get("logs_total", 0),
    }


def _bars(items: list[tuple[Any, Any]], *, max_items: int = 8) -> str:
    if not items:
        return '<div class="empty">Keine Daten vorhanden.</div>'
    parsed = [(str(k or "—"), float(_num(v))) for k, v in items[:max_items]]
    max_v = max([v for _, v in parsed] or [1]) or 1
    rows = []
    for label, value in parsed:
        width = max(4, int((value / max_v) * 100)) if max_v else 4
        rows.append(f"""
        <div class="bar-row">
          <div class="bar-label">{_e(label)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div>
          <div class="bar-value">{_e(int(value) if value.is_integer() else round(value, 1))}</div>
        </div>
        """)
    return "".join(rows)


def _render_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell(
            "Ebo Dashboard",
            f"""
            <section class="panel">
              <h1>📊 Ebo Dashboard</h1>
              <p class="muted">{_e(data.get('error'))}</p>
              <p>Starte den Bot mit der aktuellen Version und warte bis zu 5 Minuten. Oder nutze im Discord <code>/dashboard_status</code>, damit direkt ein Snapshot veröffentlicht wird.</p>
            </section>
            """,
        )

    snap: dict[str, Any] = data.get("snapshot") or {}
    guild = snap.get("guild") or {}
    profiles = snap.get("profiles") or {}
    events = snap.get("events") or {}
    ec = snap.get("ec") or {}
    loot = snap.get("loot") or {}
    voice = snap.get("voice") or {}
    audit = snap.get("audit") or {}
    event_checks = snap.get("event_checks") or {}
    member_filter = guild.get("member_filter") or {}
    analytics = _analytics_from_snapshot(snap)
    names = _profile_name_map(snap)
    balances_by_user = _balance_map(snap)
    need_user_ids = _need_user_ids(snap)
    needs_by_user = _needs_by_user(snap)

    role_line = "Gildenrolle nicht gesetzt"
    if isinstance(member_filter, dict) and member_filter.get("mode") == "discord_role":
        role_line = f"Rolle: {member_filter.get('role_name')} · {member_filter.get('eligible_count', 0)} Mitglieder"

    cards = "".join([
        _card("Rollenmitglieder", (member_filter or {}).get("eligible_count", 0), role_line),
        _card("Profile", profiles.get("count", 0), f"ausgefiltert: {profiles.get('stale_count', 0)}"),
        _card("Events", events.get("count", 0), f"Voice-Events: {analytics['voice_enabled_events']}"),
        _card("EC-Konten", (ec.get("balances") or {}).get("count", 0), f"Ø {_fmt_ec(analytics['avg_ec'])} EC"),
        _card("Auktionen", (loot.get("auctions") or {}).get("count", 0), f"aktiv: {analytics['active_auctions']}"),
        _card("Need-User", (loot.get("needs") or {}).get("user_count", 0), f"Needs ca.: {analytics['need_entries_estimated']}"),
        _card("Voice", voice.get("sessions_total", 0), f"offen: {voice.get('sessions_open', 0)}"),
        _card("Audit", audit.get("logs_total", 0), "Einträge"),
    ])

    profile_rows = []
    low_profile_rows = []
    for p in profiles.get("items") or []:
        uid = _user_id(p.get("user_id"))
        ec_value = balances_by_user.get(uid)
        need_state = "ja" if uid in need_user_ids else "nein"
        gs = _num(p.get("gearscore"), 0)
        profile_rows.append([_member_link(uid, p.get("display_name")), p.get("ingame_name"), p.get("main_role"), p.get("gearscore"), _fmt_ec(ec_value) if ec_value is not None else "—", need_state])
        if gs <= 0 or not p.get("main_role") or ec_value is None:
            low_profile_rows.append([_member_link(uid, p.get("display_name")), p.get("ingame_name"), p.get("main_role") or "—", p.get("gearscore") or "—", _fmt_ec(ec_value) if ec_value is not None else "kein EC-Konto"])

    event_rows = []
    for ev in events.get("items") or []:
        event_rows.append([_event_link(ev.get("event_id"), ev.get("title")), _dt(ev.get("when_iso")), ev.get("participant_count"), ev.get("maybe_count"), ev.get("no_count"), "ja" if ev.get("voice_enabled") else "nein"])

    all_balances = [b for b in ((ec.get("balances") or {}).get("top") or []) if isinstance(b, dict)]
    sorted_balances = sorted(all_balances, key=lambda b: _num(b.get("balance"), 0), reverse=True)
    balance_rows = []
    for b in sorted_balances:
        balance_rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("balance"))])
    bottom_balance_rows = []
    for b in list(reversed(sorted_balances))[:12]:
        bottom_balance_rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("balance"))])

    auction_rows = []
    for a in (loot.get("auctions") or {}).get("items") or []:
        leader = "—"
        uid = int(_num(a.get("top_bid_user_id"), 0))
        if a.get("top_bid_amount") is not None:
            leader = f"{names.get(uid, f'User {uid}')} / {_fmt_ec(a.get('top_bid_amount'))} EC"
        auction_rows.append([_auction_link(a.get("auction_id"), a.get("item_name")), a.get("status"), a.get("phase"), a.get("bid_count"), leader, _dt(a.get("ends_at"))])

    voice_rows = []
    for v in voice.get("recent_sessions") or []:
        seconds = int(_num(v.get("duration_seconds"), 0))
        minutes = round(seconds / 60, 1) if seconds else "—"
        voice_rows.append([v.get("member_name") or v.get("user_id"), v.get("channel_name") or v.get("channel_id"), _dt(v.get("joined_at")), _dt(v.get("left_at")), minutes])

    audit_rows = []
    for a in audit.get("recent_logs") or []:
        audit_rows.append([_dt(a.get("created_at")), a.get("action"), _short(a.get("summary"), 120), a.get("actor_id")])

    status_items = []
    for k, v in (analytics.get("auction_status") or {}).items():
        status_items.append((str(k), int(_num(v))))
    status_items.sort(key=lambda x: x[1], reverse=True)

    quality_cards = "".join([
        _card("Profil-Abdeckung", analytics.get("profile_coverage"), f"fehlt: {analytics.get('missing_profiles', 0)}"),
        _card("EC-Abdeckung", analytics.get("ec_coverage"), f"fehlt: {analytics.get('missing_ec', 0)}"),
        _card("Need-Abdeckung", analytics.get("need_coverage"), f"ohne Need: {analytics.get('missing_needs', 0)}"),
        _card("Snapshot", _dt(data.get("published_at")), "letzte Veröffentlichung"),
    ])

    body = f"""
    <nav class="topnav">
      <a href="#overview">Übersicht</a>
      <a href="/members">Mitglieder</a>
      <a href="/needs">Needs</a>
      <a href="/loot">Loot</a>
      <a href="/planning">Planung</a>
      <a href="/fairness">Fairness</a>
      <a href="/analytics">Analytics</a><a href="/voice">Voice</a>
      <a href="/ec">EC-Verlauf</a>
      <a href="/attendance">Anwesenheit</a>
      <a href="/settings">Einstellungen</a>
      <a href="/audit">Audit</a>
      <a href="/system">System</a>
      <a href="/exports">Exports</a>
      <a href="#quality">Datenqualität</a>
      <a href="#members">Mitglieder</a>
      <a href="#events">Events</a>
      <a href="#loot">Loot</a>
      <a href="#logs">Logs</a>
      <a href="/api/snapshot">JSON</a>
    </nav>

    <section class="hero" id="overview">
      <div>
        <div class="eyebrow">Read-only Dashboard</div>
        <h1>🏰 {_e(guild.get('name') or data.get('guild_name') or 'Ebolus')}</h1>
        <p>Snapshot veröffentlicht: <strong>{_e(_dt(data.get('published_at')))}</strong> · generiert: {_e(_dt(data.get('generated_at')))}</p>
        <p class="muted">{_e(role_line)} · alte JSON-Einträge werden nur ausgeblendet, nicht gelöscht.</p>
      </div>
      <a class="btn" href="/api/snapshot">JSON ansehen</a>
    </section>

    <section class="grid">{cards}</section>

    <section class="panel" id="analytics">
      <h2>📈 Analytics Schnellblick</h2>
      <div class="analytics-grid">
        <div class="metric"><span>Ø Gearscore</span><strong>{_e(round(analytics['avg_gearscore']))}</strong><small>{_e(analytics['gearscore_count'])} Profile mit GS</small></div>
        <div class="metric"><span>EC gesamt</span><strong>{_e(_fmt_ec(analytics['total_ec']))}</strong><small>über {analytics['ec_count']} Konten</small></div>
        <div class="metric"><span>Event-Zusagen</span><strong>{_e(analytics['total_event_participants'])}</strong><small>im Snapshot</small></div>
        <div class="metric"><span>Gebote gelistet</span><strong>{_e(analytics['total_listed_bids'])}</strong><small>in geladenen Auktionen</small></div>
      </div>
      <div class="split">
        <div><h3>Rollenverteilung</h3>{_bars(analytics['role_distribution'])}</div>
        <div><h3>Auktionsstatus</h3>{_bars(status_items)}</div>
      </div>
    </section>

    <section class="panel" id="quality">
      <h2>🧹 Datenqualität</h2>
      <p class="muted">Diese Werte helfen beim Aufräumen vor Vermietung/Massennutzung. Es wird nichts gelöscht, nur angezeigt.</p>
      <div class="grid mini-grid">{quality_cards}</div>
      <h3>Auffällige Profile</h3>
      {_table(['Name','Ingame','Rolle','GS','EC'], low_profile_rows[:40], placeholder='Auffälligkeiten durchsuchen…')}
    </section>

    <section class="panel" id="members"><h2>👥 Mitgliederprofile</h2>{_table(['Name','Ingame','Rolle','GS','EC','Needliste'], profile_rows, placeholder='Mitglieder durchsuchen…')}</section>
    <section class="panel" id="events"><h2>📅 Events</h2>{_table(['Event','Zeit','Teilnehmer','Vielleicht','Abgemeldet','Voice'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel"><h2>🪙 EC-Konten</h2>{_table(['Spieler','EC'], balance_rows, placeholder='EC-Konten durchsuchen…')}<h3>Unterste EC-Konten</h3>{_table(['Spieler','EC'], bottom_balance_rows, placeholder='Unterste EC-Konten durchsuchen…')}</section>
    <section class="panel" id="loot"><h2>🎁 Auktionen</h2>{_table(['Item','Status','Phase','Gebote','Führend','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel"><h2>🎙️ Voice-Sessions</h2>{_table(['Spieler','Kanal','Rein','Raus','Minuten'], voice_rows, placeholder='Voice-Sessions durchsuchen…')}</section>
    <section class="panel" id="logs"><h2>🧾 Audit-Log</h2>{_table(['Zeit','Aktion','Zusammenfassung','Actor'], audit_rows, placeholder='Audit-Logs durchsuchen…')}</section>
    """
    return _html_shell("Ebo Dashboard", body)


def _render_member_detail(data: dict[str, Any], user_id: int, current_user: Optional[dict[str, Any]] = None) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    balances = _balance_map(snap)
    needs_by_user = _needs_by_user(snap)
    profile = None
    for p in profiles:
        if isinstance(p, dict) and _user_id(p.get("user_id")) == int(user_id):
            profile = p
            break
    if not profile:
        return _html_shell(
            "Mitglied nicht gefunden",
            "<section class='panel'><h1>❌ Mitglied nicht gefunden</h1><p class='muted'>Dieses Mitglied ist nicht im aktuellen Dashboard-Snapshot oder hat nicht die gesetzte Gildenrolle.</p><p><a class='btn' href='/'>Zurück</a></p></section>",
        )

    display = profile.get("display_name") or profile.get("ingame_name") or f"User {user_id}"
    ec_value = balances.get(int(user_id))
    need_info = needs_by_user.get(int(user_id), {})
    main_needs = need_info.get("main") if isinstance(need_info, dict) else []
    secondary_needs = need_info.get("secondary") if isinstance(need_info, dict) else []

    tx_rows = []
    for tx in _tx_for_user(snap, user_id):
        tx_rows.append([_dt(tx.get("created_at")), _fmt_ec(tx.get("amount")), tx.get("raw_type"), _short(tx.get("reason"), 140)])

    voice_rows = []
    for v in _voice_for_user(snap, user_id):
        seconds = int(_num(v.get("duration_seconds"), 0))
        minutes = round(seconds / 60, 1) if seconds else "—"
        voice_rows.append([v.get("channel_name") or v.get("channel_id"), _dt(v.get("joined_at")), _dt(v.get("left_at")), minutes])

    auction_rows = []
    for a in _auctions_for_user(snap, user_id):
        auction_rows.append([a.get("item_name"), a.get("status"), a.get("phase"), _fmt_ec(a.get("top_bid_amount")) if a.get("top_bid_amount") is not None else "—", _dt(a.get("ends_at"))])

    cards = "".join([
        _card("Ingame", profile.get("ingame_name") or "—", "Profil"),
        _card("Rolle", profile.get("main_role") or "—", "Main-Rolle"),
        _card("Gearscore", profile.get("gearscore") or "—", "Profilwert"),
        _card("EC", _fmt_ec(ec_value) if ec_value is not None else "—", "aktueller Kontostand"),
    ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/ec">EC-Verlauf</a><a href="#needs">Needs</a><a href="#ec">EC</a><a href="#voice">Voice</a><a href="/api/snapshot">JSON</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Mitglied</div>
        <h1>👤 {_e(display)}</h1>
        <p class="muted">User-ID: {_e(user_id)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    {_admin_member_panel(data, int(user_id), current_user)}
    <section class="panel" id="needs">
      <h2>🎁 Needliste</h2>
      <div class="split">
        <div>{_need_list_html('Main-Needs', main_needs)}</div>
        <div>{_need_list_html('Secondary-Needs', secondary_needs)}</div>
      </div>
    </section>
    <section class="panel" id="ec"><h2>🪙 Letzte EC-Buchungen</h2>{_table(['Zeit','Betrag','Typ','Grund'], tx_rows, placeholder='Buchungen durchsuchen…')}</section>
    <section class="panel"><h2>🎁 Auktionen mit aktueller Führung/Gewinn</h2>{_table(['Item','Status','Phase','Gebot','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel" id="voice"><h2>🎙️ Voice-Sessions</h2>{_table(['Kanal','Rein','Raus','Minuten'], voice_rows, placeholder='Voice durchsuchen…')}</section>
    """
    return _html_shell(f"{display} · Ebo Dashboard", body)


def _event_by_id(snap: dict[str, Any], event_id: str) -> Optional[dict[str, Any]]:
    """Event suchen – inklusive offener DKP-/EC-Anwesenheitschecks.

    Wichtig für Ebolus: Ein Event darf im Dashboard nicht verschwinden, solange
    im DKP-Log noch ein EC-Anwesenheitscheck offen ist. Deshalb durchsuchen wir
    nicht nur snapshot["events"], sondern auch event_checks und erzeugen bei
    Bedarf einen kleinen Platzhalter-Eintrag.
    """
    target = str(event_id or "")
    for ev in _events_with_pending_ec_checks(snap):
        if isinstance(ev, dict) and str(ev.get("event_id") or "") == target:
            return ev
    return None


def _participant_rows(people: Any) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for p in people or []:
        if not isinstance(p, dict):
            continue
        rows.append([_member_link(p.get("user_id"), p.get("display_name")), "ja" if p.get("is_dashboard_member") else "nein"])
    return rows


def _role_signup_html(event: dict[str, Any]) -> str:
    parts = ((event.get("participants") or {}).get("yes") or [])
    if not parts:
        return "<div class='empty'>Keine Zusagen vorhanden.</div>"
    blocks: list[str] = []
    for group in parts:
        if not isinstance(group, dict):
            continue
        role = group.get("role") or "Unbekannt"
        people = group.get("participants") or []
        blocks.append(f"""
        <div class="subpanel">
          <h3>{_e(role)} <span class='pill'>{_e(len(people))}</span></h3>
          {_table(['Spieler','Gildenrolle'], _participant_rows(people), placeholder=f'{role} durchsuchen…')}
        </div>
        """)
    return "".join(blocks)


def _render_event_detail(data: dict[str, Any], event_id: str) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    event = _event_by_id(snap, event_id) or _event_stub_from_attendance_review(guild_id, event_id)
    if not event:
        return _html_shell(
            "Event nicht gefunden",
            "<section class='panel'><h1>❌ Event nicht gefunden</h1><p class='muted'>Dieses Event ist nicht im aktuellen Dashboard-Snapshot und hat keinen Review-Fallback.</p><p><a class='btn' href='/attendance'>Zur Anwesenheit</a></p></section>",
        )

    participants = event.get("participants") or {}
    maybe_rows = _participant_rows(participants.get("maybe") or [])
    no_rows = _participant_rows(participants.get("no") or [])
    yes_counts = event.get("yes_counts") or {}
    role_items = sorted([(str(k), int(_num(v))) for k, v in yes_counts.items()], key=lambda x: x[0].lower())

    cards = "".join([
        _card("Teilnehmer", event.get("participant_count", 0), "alle Rückmeldungen"),
        _card("Vielleicht", event.get("maybe_count", 0), "unsicher"),
        _card("Abgemeldet", event.get("no_count", 0), "Nein/abgemeldet"),
        _card("Voice", "ja" if event.get("voice_enabled") else "nein", event.get("voice_channel_id") or event.get("voice_last_channel_id") or "kein Voice"),
    ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="#signups">Zusagen</a><a href="#maybe">Vielleicht</a><a href="#no">Abgemeldet</a><a href="/api/snapshot">JSON</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Event</div>
        <h1>📅 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Event-ID: {_e(event_id)} · Zeit: {_e(_dt(event.get('when_iso')))} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
        {f"<p>{_e(event.get('description'))}</p>" if event.get('description') else ""}
      </div>
      <a class="btn" href="/#events">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>📊 Rollenverteilung</h2>{_bars(role_items, max_items=12)}</section>
    <section class="panel" id="signups"><h2>✅ Zusagen nach Rolle</h2>{_role_signup_html(event)}</section>
    <section class="panel" id="maybe"><h2>🟡 Vielleicht</h2>{_table(['Spieler','Gildenrolle'], maybe_rows, placeholder='Vielleicht durchsuchen…')}</section>
    <section class="panel" id="no"><h2>❌ Abgemeldet</h2>{_table(['Spieler','Gildenrolle'], no_rows, placeholder='Abmeldungen durchsuchen…')}</section>
    {_raw(_event_ec_queue_panel(_safe_guild_id(data), str(event_id)))}
    """
    return _html_shell(f"{event.get('title') or 'Event'} · Ebo Dashboard", body)


def _auction_by_id(snap: dict[str, Any], auction_id: str) -> Optional[dict[str, Any]]:
    for auc in (((snap.get("loot") or {}).get("auctions") or {}).get("items") or []):
        if isinstance(auc, dict) and str(auc.get("auction_id") or "") == str(auction_id):
            return auc
    return None


def _phase_label(auction: dict[str, Any]) -> str:
    phase = str(auction.get("phase") or "").strip()
    mode = str(auction.get("eligibility_mode") or "").strip()
    if phase == "need" and mode == "main_need":
        return "Main-Need-Auktion"
    if phase == "need" and mode == "secondary_need":
        return "Second-Need-Auktion"
    if phase == "free":
        return "Freie Auktion"
    if phase == "sale":
        return "Sale / Müll" if auction.get("junk_drop") else "Sale"
    return phase or mode or "Auktion"


def _bid_rows(auction: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for b in auction.get("bids") or []:
        if not isinstance(b, dict):
            continue
        rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("amount")), _dt(b.get("created_at"))])
    return rows


def _eligible_rows(auction: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for u in auction.get("eligible_users") or []:
        if not isinstance(u, dict):
            continue
        rows.append([_member_link(u.get("user_id"), u.get("display_name"))])
    return rows


def _junk_roll_rows(auction: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for r in auction.get("junk_rolls") or []:
        if not isinstance(r, dict):
            continue
        rows.append([_member_link(r.get("user_id"), r.get("display_name")), r.get("roll")])
    return rows



# ---------------------------------------------------------------------------
# Dashboard Loot-Aktionsqueue: Bieten / Sale / Müll
# ---------------------------------------------------------------------------

def _loot_action_status_label(value: Any) -> str:
    v = str(value or "").strip().lower()
    return {
        "pending": "🟡 offen",
        "processing": "🔵 wird verarbeitet",
        "done": "✅ erledigt",
        "failed": "❌ Fehler",
        "rejected": "⛔ blockiert",
        "cancelled": "⚫ abgebrochen",
    }.get(v, v or "—")


def _loot_action_type_label(value: Any) -> str:
    v = str(value or "").strip().lower()
    return {
        "bid": "Gebot",
        "sale_buy": "Sofortkauf",
        "junk_roll": "Müll-Wurf",
    }.get(v, v or "—")


def _loot_current_price(auction: dict[str, Any]) -> int:
    values: list[int] = []
    for b in auction.get("bids") or []:
        if isinstance(b, dict):
            try:
                values.append(int(_num(b.get("amount"), 0)))
            except Exception:
                pass
    if auction.get("top_bid_amount") is not None:
        try:
            values.append(int(_num(auction.get("top_bid_amount"), 0)))
        except Exception:
            pass
    if values:
        return max(values)
    try:
        return max(0, int(_num(auction.get("start_bid"), 0)) - int(_num(auction.get("min_increment"), 5)))
    except Exception:
        return 0


def _loot_min_next_bid(auction: dict[str, Any]) -> int:
    step = int(_num(auction.get("min_increment"), 5) or 5)
    current = _loot_current_price(auction)
    start = int(_num(auction.get("start_bid"), 1) or 1)
    if current > 0:
        return current + max(1, step)
    return max(0, start)


def _loot_is_sale_like(auction: dict[str, Any]) -> bool:
    phase = _loot_status(auction.get("phase"))
    kind = _loot_status(auction.get("kind"))
    return phase == "sale" or kind == "sale" or auction.get("fixed_price") is not None


def _loot_auction_eligible_user_ids(auction: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    for raw in auction.get("eligible_user_ids") or []:
        uid = _user_id(raw)
        if uid:
            ids.add(uid)
    for raw in auction.get("eligible_users") or []:
        if isinstance(raw, dict):
            uid = _user_id(raw.get("user_id") or raw.get("id") or raw.get("member_id"))
            if uid:
                ids.add(uid)
    return ids


def _loot_action_requests_for_auction(guild_id: int, auction_id: str, limit: int = 20) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id or not auction_id:
        return []
    try:
        _ensure_admin_tables()
    except Exception:
        return []
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, auction_id, action_type, amount, status,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, payload_json, result_json
                FROM dashboard_loot_action_requests
                WHERE guild_id = %s AND auction_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), str(auction_id), int(limit)),
            )
            rows: list[dict[str, Any]] = []
            for row in cur.fetchall() or []:
                out = dict(row)
                try:
                    out["payload"] = json.loads(out.get("payload_json") or "{}")
                except Exception:
                    out["payload"] = {}
                try:
                    out["result"] = json.loads(out.get("result_json") or "{}")
                except Exception:
                    out["result"] = {}
                rows.append(out)
            return rows
    finally:
        conn.close()


def _loot_action_active_for_actor(guild_id: int, auction_id: str, actor_id: str) -> dict[str, Any]:
    if not _database_url() or not guild_id or not auction_id or not actor_id:
        return {}
    try:
        _ensure_admin_tables()
    except Exception:
        return {}
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id, status, action_type, amount, requested_at
                FROM dashboard_loot_action_requests
                WHERE guild_id = %s AND auction_id = %s AND actor_id = %s AND status IN ('pending','processing')
                ORDER BY requested_at DESC, id DESC
                LIMIT 1
                """,
                (int(guild_id), str(auction_id), str(actor_id)),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def _loot_action_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    out: list[list[Any]] = []
    for r in rows:
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        info = result.get("message") or result.get("error") or r.get("request_id")
        out.append([
            _dt(r.get("requested_at")),
            _loot_action_status_label(r.get("status")),
            _loot_action_type_label(r.get("action_type")),
            _fmt_ec(r.get("amount")) if r.get("amount") is not None else "—",
            r.get("actor_name") or r.get("actor_id") or "—",
            _short(info, 140),
        ])
    return out


def _loot_action_queue_panel(guild_id: int, auction_id: str) -> str:
    rows = _loot_action_requests_for_auction(int(guild_id), str(auction_id), limit=20)
    return f"""
    <section class="panel" id="loot-actions">
      <h2>🧾 Dashboard-Aktionen</h2>
      <p class="muted">Gebote/Käufe aus dem Dashboard gehen zuerst in diese Queue. Der Bot verarbeitet sie und schreibt dann in die echten Loot-/EC-Daten.</p>
      {_table(['Zeit','Status','Aktion','EC','Spieler','Info'], _loot_action_rows(rows), placeholder='Aktionen durchsuchen…')}
    </section>
    """


def _loot_dashboard_action_panel(guild_id: int, auction: dict[str, Any], current_user: Optional[dict[str, Any]], snap: dict[str, Any]) -> str:
    auction_id = str(auction.get("auction_id") or auction.get("id") or "").strip()
    if not auction_id:
        return ""
    status = _loot_status(auction.get("status"))
    if not _loot_is_active(auction) or status in {"closed", "done", "delivered", "cancelled", "deleted"}:
        return """
        <section class="panel"><h2>🛒 Bieten / Kaufen</h2><div class="empty">Diese Auktion ist nicht mehr aktiv.</div></section>
        """
    if not current_user or not _user_id(current_user.get("user_id")):
        return f"""
        <section class="panel"><h2>🛒 Bieten / Kaufen</h2>
          <p class="muted">Zum Bieten muss das Dashboard über Discord-Login wissen, welcher Spieler du bist.</p>
          <a class="btn" href="/auth/discord/start?next={urllib.parse.quote('/auction/' + auction_id)}">Mit Discord einloggen</a>
        </section>
        """

    user_id = _user_id(current_user.get("user_id"))
    names = _profile_name_map(snap)
    balances = _balance_map(snap)
    balance = balances.get(user_id)
    actor_name = current_user.get("username") or names.get(user_id) or f"User {user_id}"
    mode = _loot_status(auction.get("eligibility_mode"))
    eligible = _loot_auction_eligible_user_ids(auction)
    warnings: list[str] = []
    if mode in {"main_need", "secondary_need"} and eligible and user_id not in eligible:
        warnings.append("Du bist laut Snapshot für diese Need-Phase nicht berechtigt. Der Bot prüft das endgültig.")
    active_req = _loot_action_active_for_actor(int(guild_id), auction_id, str(user_id))
    if active_req:
        warnings.append(f"Du hast bereits eine offene Dashboard-Aktion: {_loot_action_type_label(active_req.get('action_type'))} · {_loot_action_status_label(active_req.get('status'))}.")
    warn_html = "".join(f"<p class='muted'>⚠️ {_e(w)}</p>" for w in warnings)
    bal_text = _fmt_ec(balance) + " EC" if balance is not None else "im Snapshot nicht geladen"

    if _loot_is_sale_like(auction):
        price = int(_num(auction.get("fixed_price") if auction.get("fixed_price") is not None else auction.get("start_bid"), 0))
        is_junk = bool(auction.get("junk_drop")) and price <= 0
        action = "junk_roll" if is_junk else "sale_buy"
        label = "🎲 Müll würfeln" if is_junk else ("Gratis nehmen" if price <= 0 else f"Sofort kaufen für {price} EC")
        disabled = "disabled" if active_req else ""
        confirm = "Müll-Wurf im Dashboard anfragen?" if is_junk else "Sale-Kauf im Dashboard anfragen?"
        return f"""
        <section class="panel" id="bid">
          <h2>🛒 Kaufen / Müll</h2>
          <p class="muted">Angemeldet als <strong>{_e(actor_name)}</strong> · EC: <strong>{_e(bal_text)}</strong></p>
          {warn_html}
          <form method="post" action="/admin/auction/{_e(auction_id)}/sale" onsubmit="return confirm('{_e(confirm)}');">
            <input type="hidden" name="action_type" value="{_e(action)}">
            <button class="btn" type="submit" {disabled}>{_e(label)}</button>
          </form>
          <p class="muted">Der Bot verarbeitet die Anfrage, prüft Mitgliedschaft/EC und aktualisiert Discord-Nachrichten.</p>
        </section>
        """

    current = _loot_current_price(auction)
    min_next = _loot_min_next_bid(auction)
    quick = [min_next, max(min_next, current + 10), max(min_next, current + 25)]
    quick = list(dict.fromkeys(int(x) for x in quick if int(x) >= min_next))
    quick_buttons = "".join(
        f"<button class='btn mini-btn' type='submit' name='quick_amount' value='{int(q)}'>{int(q)} EC</button>" for q in quick[:3]
    )
    disabled = "disabled" if active_req else ""
    return f"""
    <section class="panel" id="bid">
      <h2>💰 Dashboard-Gebot</h2>
      <p class="muted">Angemeldet als <strong>{_e(actor_name)}</strong> · EC: <strong>{_e(bal_text)}</strong> · Mindestgebot: <strong>{_e(min_next)} EC</strong></p>
      {warn_html}
      <form method="post" action="/admin/auction/{_e(auction_id)}/bid" style="display:grid; gap:12px; max-width:720px;" onsubmit="return confirm('Gebot über Dashboard an den Bot senden?');">
        <div style="display:flex; flex-wrap:wrap; gap:8px;">{quick_buttons}</div>
        <label>Eigenes Gebot in EC<br><input name="amount" type="number" min="{int(min_next)}" step="1" placeholder="z. B. {int(min_next)}" style="width:220px; padding:10px; border-radius:10px;"></label>
        <button class="btn" type="submit" {disabled}>Gebot senden</button>
      </form>
      <p class="muted">Abgebucht wird nicht beim Bieten, sondern wie bisher erst bei Übergabe/Gewinner-Bestätigung.</p>
    </section>
    """


def _enqueue_loot_action_request(guild_id: int, auction: dict[str, Any], action_type: str, amount: int, actor: dict[str, Any]) -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt. Ohne Postgres kann das Dashboard keine Bot-Aktion anstoßen."}
    auction_id = str(auction.get("auction_id") or auction.get("id") or "").strip()
    if not guild_id or not auction_id:
        return {"ok": False, "error": "Guild/Auktion fehlt."}
    actor_id = str(actor.get("user_id") or "").strip()
    actor_name = str(actor.get("username") or actor_id or "Dashboard")
    if not actor_id:
        return {"ok": False, "error": "Discord-Login erforderlich. Mit Basic-Login kann das Dashboard nicht wissen, wer bietet."}
    active = _loot_action_active_for_actor(int(guild_id), auction_id, actor_id)
    if active:
        return {"ok": False, "error": f"Du hast bereits eine offene Aktion für diese Auktion ({active.get('status')}). Bitte kurz warten."}
    action = str(action_type or "").strip().lower()
    if action not in {"bid", "sale_buy", "junk_roll"}:
        return {"ok": False, "error": "Unbekannte Loot-Aktion."}
    request_id = f"dash-loot-{int(time.time())}-{secrets.token_hex(6)}"
    payload = {
        "auction_id": auction_id,
        "item_name": str(auction.get("item_name") or auction.get("item") or auction_id),
        "action_type": action,
        "amount": int(amount or 0),
        "phase": str(auction.get("phase") or ""),
        "eligibility_mode": str(auction.get("eligibility_mode") or ""),
        "requested_by": {"id": actor_id, "name": actor_name},
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source": "dashboard_loot_action",
    }
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_loot_action_requests
                    (request_id, guild_id, auction_id, action_type, amount, status, payload_json, actor_id, actor_name, requested_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s, NOW())
                RETURNING id, request_id, status
                """,
                (request_id, int(guild_id), auction_id, action, int(amount or 0), json.dumps(payload, ensure_ascii=False, separators=(",", ":")), actor_id, actor_name),
            )
            row = dict(cur.fetchone() or {})
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (int(guild_id), f"loot_action_{action}_create", "auction", auction_id, actor_id, actor_name, json.dumps(payload, ensure_ascii=False)),
            )
        conn.commit()
        return {"ok": True, "request": row, "request_id": request_id}
    finally:
        conn.close()

def _render_auction_detail(data: dict[str, Any], auction_id: str, current_user: Optional[dict[str, Any]] = None, msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    auction = _auction_by_id(snap, auction_id)
    if not auction:
        return _html_shell(
            "Auktion nicht gefunden",
            "<section class='panel'><h1>❌ Auktion nicht gefunden</h1><p class='muted'>Diese Auktion ist nicht im aktuellen Dashboard-Snapshot.</p><p><a class='btn' href='/#loot'>Zurück</a></p></section>",
        )

    leader = "—"
    if auction.get("top_bid_amount") is not None:
        uid = _user_id(auction.get("top_bid_user_id"))
        leader = f"{auction.get('top_bid_user_name') or f'User {uid}'} · {_fmt_ec(auction.get('top_bid_amount'))} EC"
    winner = "—"
    if auction.get("winner_user_id"):
        winner = f"{auction.get('winner_name') or ('User ' + str(auction.get('winner_user_id')))}"

    cards = "".join([
        _card("Status", auction.get("status") or "—", _phase_label(auction)),
        _card("Gebote", auction.get("bid_count", 0), f"Führend: {leader}"),
        _card("Gewinner", winner, _dt(auction.get("delivered_at")) if auction.get("delivered_at") else "noch offen"),
        _card("Ende", _dt(auction.get("ends_at")), f"Start: {_dt(auction.get('created_at'))}"),
        _card("Startgebot", _fmt_ec(auction.get("start_bid")), "EC"),
        _card("Mindestschritt", _fmt_ec(auction.get("min_increment")), "EC"),
        _card("Festpreis", _fmt_ec(auction.get("fixed_price")), "Sale"),
        _card("Berechtigt", auction.get("eligible_count", 0), auction.get("eligibility_mode") or "alle"),
    ])

    bid_rows = _bid_rows(auction)
    eligible_rows = _eligible_rows(auction)
    roll_rows = _junk_roll_rows(auction)
    channel_info = [
        ["Auktions-/Log-Nachricht", auction.get("channel_id") or "—", auction.get("message_id") or "—"],
        ["Auktionshaus-Nachricht", "—", auction.get("market_message_id") or auction.get("active_message_id") or "—"],
    ]

    extra_roll_section = ""
    if auction.get("junk_drop") or roll_rows:
        extra_roll_section = f"""
        <section class="panel" id="rolls">
          <h2>🎲 Müll-Würfe</h2>
          <p class="muted">Würfelphase bis: {_e(_dt(auction.get('junk_roll_until')))} · Gewinnerwurf: {_e(auction.get('junk_roll_winner_roll') or '—')}</p>
          {_table(['Spieler','Wurf'], roll_rows, placeholder='Würfe durchsuchen…')}
        </section>
        """

    guild_id = _safe_guild_id(data)
    action_panel = _loot_dashboard_action_panel(int(guild_id), auction, current_user, snap) if guild_id else ""
    queue_panel = _loot_action_queue_panel(int(guild_id), auction_id) if guild_id else ""
    msg_panel = f"<section class='panel'><p>{_e(msg)}</p></section>" if msg else ""

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/loot">Loot</a><a href="#bid">Bieten/Kaufen</a><a href="#loot-actions">Queue</a><a href="#bids">Gebote</a><a href="#eligible">Berechtigte</a><a href="#tech">Technik</a><a href="/api/snapshot">JSON</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Auktion</div>
        <h1>🎁 {_e(auction.get('item_name') or auction_id)}</h1>
        <p class="muted">Auktions-ID: {_e(auction_id)} · {_e(_phase_label(auction))} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/#loot">Zurück</a>
    </section>
    {msg_panel}
    <section class="grid">{cards}</section>
    {action_panel}
    {queue_panel}
    <section class="panel" id="bids"><h2>💰 Gebotshistorie</h2>{_table(['Spieler','Gebot','Zeit'], bid_rows, placeholder='Gebote durchsuchen…')}</section>
    {extra_roll_section}
    <section class="panel" id="eligible"><h2>✅ Berechtigte Spieler</h2><p class="muted">Bei freien Auktionen/Sale kann die Liste leer sein, weil dann alle berechtigt sind.</p>{_table(['Spieler'], eligible_rows, placeholder='Berechtigte durchsuchen…')}</section>
    <section class="panel" id="tech"><h2>🧾 Technische Infos</h2>{_table(['Bereich','Kanal-ID','Nachricht-ID'], channel_info, searchable=False)}</section>
    """
    return _html_shell(f"{auction.get('item_name') or 'Auktion'} · Ebo Dashboard", body)


def _html_shell(title: str, body: str) -> str:
    auth_note = ""
    if _discord_oauth_enabled():
        auth_note = '<div class="authbar">🔐 Discord-Login aktiv · <a href="/me">Mein Login</a> · <a href="/logout">Logout</a></div>'
    elif not _env("DASHBOARD_PASSWORD"):
        auth_note = '<div class="warn">⚠️ DASHBOARD_PASSWORD ist nicht gesetzt. Dashboard ist aktuell ohne Login erreichbar.</div>'
    else:
        auth_note = '<div class="authbar">🔐 Passwort-Login aktiv</div>'
    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_e(title)}</title>
  <link rel="icon" type="image/png" href="{_asset('favicon.png')}">
  <meta property="og:title" content="{_e(title)}">
  <meta property="og:image" content="{_asset('opengraph.webp')}">
  <meta name="theme-color" content="#0f1014">
  <style>
    :root {{ --bg:#0f1014; --panel:#181a22; --panel2:#20232d; --text:#f1eadb; --muted:#a8a193; --gold:#d6a84f; --line:#333746; --red:#d96868; --green:#81c784; }}
    * {{ box-sizing:border-box; }} html {{ scroll-behavior:smooth; }}
    body {{ margin:0; font-family:Inter, system-ui, Segoe UI, sans-serif; background:linear-gradient(180deg,rgba(15,16,20,.80),rgba(15,16,20,.96)), url("{_asset('dashboard_bg.webp')}") center top / cover fixed no-repeat; color:var(--text); }}
    main {{ max-width:1240px; margin:0 auto; padding:22px 18px 60px; }}
    .topnav {{ position:sticky; top:0; z-index:5; display:flex; gap:8px; flex-wrap:wrap; padding:10px; margin:-22px -18px 18px; background:rgba(10,11,15,.86); backdrop-filter:blur(12px); border-bottom:1px solid rgba(214,168,79,.24); box-shadow:0 10px 30px rgba(0,0,0,.35); }}
    .topnav a {{ color:var(--text); text-decoration:none; padding:9px 12px; border:1px solid var(--line); border-radius:999px; background:linear-gradient(180deg,rgba(32,35,45,.95),rgba(13,14,20,.92)); font-size:13px; display:inline-flex; align-items:center; gap:8px; box-shadow:inset 0 1px 0 rgba(255,255,255,.04); }}
    .topnav a::before {{ content:""; width:22px; height:22px; flex:0 0 22px; background:center / contain no-repeat; filter:drop-shadow(0 1px 3px rgba(0,0,0,.7)); display:none; }}
    .topnav a[href="/"]::before {{ display:block; background-image:url("{_asset('nav_kommando.png')}"); }}
    .topnav a[href="/overview"]::before {{ display:block; background-image:url("{_asset('nav_kommando.png')}"); }}
    .topnav a[href="/planning"]::before {{ display:block; background-image:url("{_asset('nav_planung.png')}"); }}
    .topnav a[href="/members"]::before {{ display:block; background-image:url("{_asset('nav_mitglieder.png')}"); }}
    .topnav a[href="/needs"]::before {{ display:block; background-image:url("{_asset('nav_needs.png')}"); }}
    .topnav a[href="/loot"]::before {{ display:block; background-image:url("{_asset('nav_loot.png')}"); }}
    .topnav a[href="/fairness"]::before {{ display:block; background-image:url("{_asset('nav_fairness.png')}"); }}
    .topnav a[href="/analytics"]::before {{ display:block; background-image:url("{_asset('nav_analytics.png')}"); }}
    .topnav a[href="/voice"]::before {{ display:block; background-image:url("{_asset('nav_voice.png')}"); }}
    .topnav a[href="/ec"]::before {{ display:block; background-image:url("{_asset('nav_ec.png')}"); }}
    .topnav a[href="/ec-queue"]::before {{ display:block; background-image:url("{_asset('nav_ec.png')}"); }}
    .topnav a[href="/attendance"]::before {{ display:block; background-image:url("{_asset('nav_anwesenheit.png')}"); }}
    .topnav a[href="/attendance-stats"]::before {{ display:block; background-image:url("{_asset('nav_anwesenheit.png')}"); }}
    .topnav a[href="/audit"]::before {{ display:block; background-image:url("{_asset('nav_audit.png')}"); }}
    .topnav a[href="/admin"]::before {{ display:block; background-image:url("{_asset('nav_leitung.png')}"); }}
    .topnav a[href="/settings"]::before {{ display:block; background-image:url("{_asset('nav_einstellungen.png')}"); }}
    .topnav a[href="/system"]::before {{ display:block; background-image:url("{_asset('nav_system.png')}"); }}
    .topnav a[href="/exports"]::before {{ display:block; background-image:url("{_asset('nav_exports.png')}"); }}
    .topnav a:hover {{ border-color:var(--gold); color:var(--gold); transform:translateY(-1px); }}
    .hero {{ position:relative; overflow:hidden; display:flex; justify-content:space-between; gap:18px; align-items:center; padding:30px; border:1px solid rgba(214,168,79,.32); background:linear-gradient(90deg,rgba(10,11,15,.90),rgba(24,26,34,.78)), url("{_asset('hero_banner.webp')}") center / cover no-repeat; border-radius:20px; margin-bottom:18px; box-shadow:0 18px 44px rgba(0,0,0,.42); }}
    .hero::after {{ content:""; position:absolute; inset:0; pointer-events:none; background:radial-gradient(circle at 76% 50%,rgba(214,168,79,.16),transparent 34%), linear-gradient(180deg,transparent,rgba(0,0,0,.24)); }}
    .hero > * {{ position:relative; z-index:1; }}
    .hero h1::before {{ content:""; display:inline-block; width:38px; height:38px; margin-right:10px; vertical-align:-8px; background:url("{_asset('logo_128.png')}") center / contain no-repeat; filter:drop-shadow(0 2px 7px rgba(0,0,0,.8)); }}
    .eyebrow {{ color:var(--gold); text-transform:uppercase; letter-spacing:.12em; font-size:12px; font-weight:700; }}
    h1,h2,h3 {{ margin:0 0 8px; }} p {{ color:var(--muted); }}
    .muted {{ color:var(--muted); }}
    .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:18px 0; }}
    .mini-grid {{ margin:12px 0 18px; }}
    .card,.panel {{ background:linear-gradient(180deg,rgba(24,26,34,.94),rgba(16,18,25,.94)), url("{_asset('panel_texture.webp')}") center / cover; border:1px solid rgba(214,168,79,.16); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.32), inset 0 1px 0 rgba(255,255,255,.03); }}
    .card {{ padding:16px; }} .card-title {{ color:var(--muted); font-size:13px; }} .card-value {{ font-size:28px; font-weight:800; color:var(--gold); }} .card-sub {{ color:var(--muted); font-size:12px; }}
    .panel {{ padding:18px; margin:14px 0; scroll-margin-top:70px; }}
    .subpanel {{ background:rgba(32,35,45,.72); border:1px solid var(--line); border-radius:14px; padding:14px; margin:12px 0; }}
    .analytics-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:12px 0 18px; }}
    .metric {{ background:var(--panel2); border:1px solid var(--line); border-radius:14px; padding:14px; }}
    .metric span {{ display:block; color:var(--muted); font-size:13px; }} .metric strong {{ display:block; color:var(--gold); font-size:28px; }} .metric small {{ color:var(--muted); }}
    .split {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .bar-row {{ display:grid; grid-template-columns:110px 1fr 44px; gap:10px; align-items:center; margin:8px 0; }} .bar-label,.bar-value {{ color:var(--muted); font-size:13px; }}
    .bar-track {{ height:10px; background:#0b0c10; border:1px solid var(--line); border-radius:999px; overflow:hidden; }} .bar-fill {{ height:100%; background:linear-gradient(90deg,var(--gold),#f1d28a); }}
    .table-search {{ width:100%; max-width:420px; margin:8px 0 12px; padding:10px 12px; border-radius:10px; border:1px solid var(--line); background:#08090d; color:var(--text); outline:none; }}
    .table-search:focus {{ border-color:var(--gold); }}
    .table-wrap {{ overflow-x:auto; }} table {{ width:100%; border-collapse:collapse; font-size:14px; }} th,td {{ padding:10px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ color:var(--gold); font-size:12px; text-transform:uppercase; letter-spacing:.05em; }} tr:hover td {{ background:rgba(255,255,255,.025); }}
    .btn {{ display:inline-block; padding:10px 14px; border-radius:10px; background:var(--gold); color:#111; font-weight:800; text-decoration:none; white-space:nowrap; }}
    .link {{ color:var(--gold); text-decoration:none; font-weight:700; }} .link:hover {{ text-decoration:underline; }}
    .pill {{ display:inline-block; padding:2px 8px; border:1px solid var(--line); border-radius:999px; color:var(--gold); font-size:12px; vertical-align:middle; }}
    .queue-badge {{ display:inline-flex; align-items:center; gap:7px; padding:4px 9px; border:1px solid rgba(214,168,79,.30); border-radius:999px; color:var(--gold); background:rgba(214,168,79,.08); text-decoration:none; font-size:12px; font-weight:800; white-space:nowrap; }}
    .queue-badge small {{ color:var(--muted); font-weight:700; }}
    .queue-badge.ok {{ color:#b7f2bd; border-color:rgba(129,199,132,.42); background:rgba(129,199,132,.10); }}
    .queue-badge.wait {{ color:#ffe0a3; border-color:rgba(214,168,79,.46); background:rgba(214,168,79,.12); }}
    .queue-badge.bad {{ color:#ffb3b3; border-color:rgba(217,104,104,.42); background:rgba(217,104,104,.10); }}
    .need-list {{ margin:8px 0 16px; padding-left:22px; color:var(--text); }} .need-list li {{ margin:5px 0; }}
    code {{ background:#05060a; border:1px solid var(--line); padding:2px 5px; border-radius:6px; }}
    .empty {{ color:var(--muted); padding:18px 16px 18px 58px; min-height:58px; display:flex; align-items:center; border:1px dashed rgba(214,168,79,.18); border-radius:14px; background:linear-gradient(90deg,rgba(10,11,15,.70),rgba(24,26,34,.54)); position:relative; }}
    .empty::before {{ content:""; position:absolute; left:16px; top:50%; width:30px; height:30px; transform:translateY(-50%); background:url("{_asset('status_ec_offen.png')}") center / contain no-repeat; opacity:.78; }}
    .warn {{ background:#3a250d; border:1px solid #8a5b18; padding:12px 14px; border-radius:12px; margin-bottom:14px; color:#ffe0a3; }}
    .authbar {{ display:flex; gap:10px; align-items:center; justify-content:flex-end; background:rgba(24,26,34,.9); border:1px solid var(--line); border-radius:12px; padding:10px 12px; margin-bottom:14px; color:var(--muted); font-size:13px; }} .authbar a {{ color:var(--gold); text-decoration:none; font-weight:700; }}
    @media(max-width:1000px) {{ .grid,.analytics-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .split {{ grid-template-columns:1fr; }} .hero {{ flex-direction:column; align-items:flex-start; }} }}
    @media(max-width:560px) {{ .grid,.analytics-grid {{ grid-template-columns:1fr; }} .bar-row {{ grid-template-columns:90px 1fr 38px; }} }}
  </style>
</head>
<body><main>{auth_note}{body}</main><script>
function filterNextTable(input) {{
  const wrap = input.nextElementSibling;
  if (!wrap) return;
  const table = wrap.querySelector('table');
  if (!table) return;
  const q = (input.value || '').toLowerCase().trim();
  for (const row of table.querySelectorAll('tbody tr')) {{
    const text = row.innerText.toLowerCase();
    row.style.display = (!q || text.includes(q)) ? '' : 'none';
  }}
}}
</script></body>
</html>"""



def _ec_transactions(snap: dict[str, Any]) -> dict[str, Any]:
    return ((snap.get("ec") or {}).get("transactions") or {}) if isinstance((snap.get("ec") or {}).get("transactions"), dict) else {}




def _ec_award_requests_for_dashboard(guild_id: int, limit: int = 80) -> list[dict[str, Any]]:
    """Letzte EC-Buchungsanfragen aus dem Dashboard.

    Sichtbar auf /ec, damit man nach Klick auf "EC wirklich buchen" sofort sieht,
    ob der Bot die Anfrage noch offen hat, verarbeitet oder abgelehnt hat.
    """
    if not _database_url() or not guild_id:
        return []
    try:
        _ensure_admin_tables()
    except Exception:
        # Fallback: EC-Verlauf darf nicht komplett sterben, nur weil die Queue-Tabelle fehlt.
        return []
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, event_id, event_type, status, full_ec, partial_ec,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, result_json, payload_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), int(limit)),
            )
            rows = []
            for row in cur.fetchall() or []:
                out = dict(row)
                try:
                    out["result"] = json.loads(out.get("result_json") or "{}")
                except Exception:
                    out["result"] = {}
                try:
                    out["payload"] = json.loads(out.get("payload_json") or "{}")
                except Exception:
                    out["payload"] = {}
                rows.append(out)
            return rows
    finally:
        conn.close()


def _ec_award_status_label(value: Any) -> str:
    v = str(value or "").strip().lower()
    return {
        "pending": "🟡 offen",
        "processing": "🔵 wird verarbeitet",
        "done": "✅ erledigt",
        "failed": "❌ fehlgeschlagen",
        "rejected": "⛔ blockiert",
        "cancelled": "⚫ abgebrochen",
    }.get(v, v or "—")


def _ec_award_request_table_rows(rows: list[dict[str, Any]], snap: dict[str, Any]) -> list[list[Any]]:
    names = _profile_name_map(snap)
    event_name_by_id = {str(ev.get("event_id") or ""): str(ev.get("title") or ev.get("event_id") or "Event") for ev in ((snap.get("events") or {}).get("items") or []) if isinstance(ev, dict)}
    out: list[list[Any]] = []
    for r in rows:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        event_id = str(r.get("event_id") or "")
        event_title = payload.get("event_title") or event_name_by_id.get(event_id) or event_id
        status = str(r.get("status") or "")
        total = result.get("total_ec") if result.get("total_ec") is not None else payload.get("total_ec")
        applied = result.get("applied_count") if result.get("applied_count") is not None else payload.get("recipient_count")
        skipped = result.get("skipped_count") if result.get("skipped_count") is not None else "—"
        error = ""
        if result and not result.get("ok", status == "done"):
            error = str(result.get("error") or "")
        out.append([
            _dt(r.get("requested_at")),
            _ec_award_status_label(status),
            _event_link(event_id, event_title),
            r.get("event_type") or "—",
            _fmt_ec(total),
            applied,
            skipped,
            r.get("actor_name") or r.get("actor_id") or "—",
            _short(error or r.get("request_id"), 120),
        ])
    return out




def _ec_award_request_by_request_id(guild_id: int, request_id: str) -> dict[str, Any]:
    if not _database_url() or not guild_id or not request_id:
        return {}
    try:
        _ensure_admin_tables()
    except Exception:
        return {}
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, event_id, event_type, status, full_ec, partial_ec,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, result_json, payload_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s AND request_id = %s
                LIMIT 1
                """,
                (int(guild_id), str(request_id)),
            )
            row = cur.fetchone()
            if not row:
                return {}
            out = dict(row)
            try:
                out["result"] = json.loads(out.get("result_json") or "{}")
            except Exception:
                out["result"] = {}
            try:
                out["payload"] = json.loads(out.get("payload_json") or "{}")
            except Exception:
                out["payload"] = {}
            return out
    finally:
        conn.close()


def _dt_obj(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ec_award_request_is_stale(row: dict[str, Any], minutes: int = 15) -> bool:
    if str(row.get("status") or "").lower() != "processing":
        return False
    claimed = _dt_obj(row.get("claimed_at"))
    if not claimed:
        return False
    return (datetime.now(timezone.utc) - claimed).total_seconds() > int(minutes) * 60


def _ec_queue_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"pending": 0, "processing": 0, "done": 0, "failed": 0, "rejected": 0, "cancelled": 0, "stale": 0}
    for r in rows:
        st = str(r.get("status") or "").lower()
        if st in counts:
            counts[st] += 1
        if _ec_award_request_is_stale(r):
            counts["stale"] += 1
    return counts


def _ec_request_action_forms(row: dict[str, Any], *, allow_admin: bool) -> dict[str, str]:
    if not allow_admin:
        return _raw("<span class='muted'>nur Admin</span>")
    request_id = _e(row.get("request_id") or "")
    status = str(row.get("status") or "").lower()
    forms: list[str] = []
    if status == "pending":
        forms.append(
            f"<form method=\"post\" action=\"/admin/ec-award-requests/{request_id}/cancel\" style=\"display:inline\" onsubmit=\"return confirm('Offene EC-Buchungsanfrage abbrechen?');\"><button class=\"btn mini-btn danger-btn\" type=\"submit\">Abbrechen</button></form>"
        )
    if status in {"failed", "rejected", "cancelled"}:
        forms.append(
            f"<form method=\"post\" action=\"/admin/ec-award-requests/{request_id}/retry\" style=\"display:inline\" onsubmit=\"return confirm('EC-Buchungsanfrage wieder auf offen setzen? Der Bot prüft Doppelbuchungen erneut.');\"><button class=\"btn mini-btn\" type=\"submit\">Neu versuchen</button></form>"
        )
    if status == "processing" and _ec_award_request_is_stale(row):
        forms.append(
            f"<form method=\"post\" action=\"/admin/ec-award-requests/{request_id}/requeue\" style=\"display:inline\" onsubmit=\"return confirm('Diese Verarbeitung wirkt veraltet. Wieder auf offen setzen?');\"><button class=\"btn mini-btn\" type=\"submit\">Wieder öffnen</button></form>"
        )
    if not forms:
        return _raw("<span class='muted'>—</span>")
    return _raw("<div class='queue-actions'>" + " ".join(forms) + "</div>")


def _ec_award_request_control_rows(rows: list[dict[str, Any]], snap: dict[str, Any], *, allow_admin: bool = False) -> list[list[Any]]:
    event_name_by_id = {
        str(ev.get("event_id") or ""): str(ev.get("title") or ev.get("event_id") or "Event")
        for ev in ((snap.get("events") or {}).get("items") or [])
        if isinstance(ev, dict)
    }
    out: list[list[Any]] = []
    for r in rows:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        event_id = str(r.get("event_id") or "")
        event_title = payload.get("event_title") or event_name_by_id.get(event_id) or event_id
        status = str(r.get("status") or "")
        total = result.get("total_ec") if result.get("total_ec") is not None else payload.get("total_ec")
        applied = result.get("applied_count") if result.get("applied_count") is not None else payload.get("recipient_count")
        skipped = result.get("skipped_count") if result.get("skipped_count") is not None else "—"
        error = ""
        if result and not result.get("ok", status == "done"):
            error = str(result.get("error") or "")
        stale = " · alt" if _ec_award_request_is_stale(r) else ""
        out.append([
            _dt(r.get("requested_at")),
            _ec_award_status_label(status) + stale,
            _event_link(event_id, event_title),
            r.get("event_type") or "—",
            _fmt_ec(total),
            applied,
            skipped,
            r.get("actor_name") or r.get("actor_id") or "—",
            _short(error or r.get("request_id"), 140),
            _ec_request_action_forms(r, allow_admin=allow_admin),
        ])
    return out


def _update_ec_award_request_status(guild_id: int, request_id: str, new_status: str, actor: dict[str, Any], *, allowed_current: set[str], result_patch: Optional[dict[str, Any]] = None) -> tuple[bool, str]:
    if not _database_url() or not guild_id or not request_id:
        return False, "DATABASE_URL/Guild/Request fehlt."
    current = _ec_award_request_by_request_id(int(guild_id), str(request_id))
    if not current:
        return False, "EC-Buchungsanfrage nicht gefunden."
    old_status = str(current.get("status") or "").lower()
    if old_status not in allowed_current:
        return False, f"Status {old_status or '—'} kann hier nicht geändert werden."
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("user_id") or "Dashboard")
    payload = current.get("payload") if isinstance(current.get("payload"), dict) else {}
    result = current.get("result") if isinstance(current.get("result"), dict) else {}
    if result_patch:
        result.update(result_patch)
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if new_status == "pending":
                cur.execute(
                    """
                    UPDATE dashboard_ec_award_requests
                    SET status = 'pending', claimed_at = NULL, processed_at = NULL, result_json = %s
                    WHERE guild_id = %s AND request_id = %s
                    """,
                    (json.dumps(result, ensure_ascii=False, separators=(",", ":")), int(guild_id), str(request_id)),
                )
            else:
                cur.execute(
                    """
                    UPDATE dashboard_ec_award_requests
                    SET status = %s, processed_at = CASE WHEN %s IN ('cancelled','failed','rejected') THEN NOW() ELSE processed_at END, result_json = %s
                    WHERE guild_id = %s AND request_id = %s
                    """,
                    (str(new_status), str(new_status), json.dumps(result, ensure_ascii=False, separators=(",", ":")), int(guild_id), str(request_id)),
                )
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (int(guild_id), f"ec_award_request_{new_status}", "ec_award_request", str(request_id), actor_id, actor_name, json.dumps({"old_status": old_status, "new_status": new_status, "event_id": current.get("event_id"), "event_title": payload.get("event_title")}, ensure_ascii=False)),
            )
        conn.commit()
        return True, f"EC-Buchungsanfrage wurde auf {new_status} gesetzt."
    finally:
        conn.close()


def _render_ec_queue_dashboard(data: dict[str, Any], current_user: Optional[dict[str, Any]] = None, msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("EC-Queue · Ebo Dashboard", f"<section class='panel'><h1>🌐 EC-Queue</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    rows = _ec_award_requests_for_dashboard(guild_id, limit=200) if guild_id else []
    counts = _ec_queue_counts(rows)
    allow_admin = bool(current_user and str(current_user.get("role") or "") == "admin")
    table_rows = _ec_award_request_control_rows(rows, snap, allow_admin=allow_admin)
    total_ec_waiting = sum(_num((r.get("payload") or {}).get("total_ec"), 0) for r in rows if isinstance(r.get("payload"), dict) and str(r.get("status") or "").lower() in {"pending", "processing"})
    cards = "".join([
        _card("Offen", counts.get("pending", 0), "wartet auf Bot"),
        _card("In Arbeit", counts.get("processing", 0), f"alt: {counts.get('stale', 0)}"),
        _card("Erledigt", counts.get("done", 0), "vom Bot gebucht"),
        _card("Probleme", counts.get("failed", 0) + counts.get("rejected", 0), "fehlgeschlagen/blockiert"),
        _card("Abgebrochen", counts.get("cancelled", 0), "manuell gestoppt"),
        _card("Wartende EC", _fmt_ec(total_ec_waiting), "pending + processing"),
    ])
    notice = f"<div class='warn'>{_e(msg)}</div>" if msg else ""
    admin_note = "Admin-Aktionen aktiv." if allow_admin else "Nur Dashboard-Admins sehen Aktionen."
    body = f"""
    <nav class="topnav"><a href="/">← Kommando</a><a href="/ec">EC-Verlauf</a><a href="/ec-queue">EC-Queue</a><a href="/attendance">Anwesenheit</a><a href="/audit">Audit</a><a href="/api/ec-award-requests">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Dashboard → Bot</div>
        <h1>🌐 EC-Buchungsqueue</h1>
        <p>Kontrolle für EC-Anfragen aus dem Attendance Review. Das Dashboard schreibt weiterhin keine JSON-Daten; der Bot verarbeitet diese Queue.</p>
        <p class="muted">{_e(admin_note)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/ec">EC-Verlauf</a>
    </section>
    {notice}
    <section class="grid mini-grid">{cards}</section>
    <section class="panel">
      <h2>📋 Letzte EC-Anfragen</h2>
      <p class="muted">Abbrechen geht nur bei offenen Anfragen. Fehlgeschlagene/blockierte/abgebrochene Anfragen können neu geöffnet werden. Alte Processing-Anfragen können wieder geöffnet werden, wenn der Bot hängen geblieben ist.</p>
      {_table(['Angefragt','Status','Event','Typ','EC','Gebucht','Übersprungen','Admin','Details','Aktion'], table_rows, placeholder='EC-Queue durchsuchen…')}
    </section>
    """
    return _html_shell("EC-Queue · Ebo Dashboard", body)


def _render_ec_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>🪙 EC-Verlauf</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    txs = _ec_transactions(snap)
    balances = (((snap.get("ec") or {}).get("balances") or {}).get("top") or [])
    recent = txs.get("items") or txs.get("recent") or []
    top_earned = txs.get("top_earned") or []
    top_spent = txs.get("top_spent") or []
    top_activity = txs.get("top_activity") or []

    total_ec = sum(_num(b.get("balance"), 0) for b in balances if isinstance(b, dict))
    avg_ec = total_ec / len(balances) if balances else 0
    total_earned = _num(txs.get("total_earned"), 0)
    total_spent = _num(txs.get("total_spent"), 0)
    net_loaded = _num(txs.get("net_loaded"), total_earned - total_spent)
    guild_id = _safe_guild_id(data)
    award_requests = _ec_award_requests_for_dashboard(guild_id, limit=80) if guild_id else []
    award_request_rows = _ec_award_request_table_rows(award_requests, snap)
    pending_count = sum(1 for r in award_requests if str(r.get("status") or "").lower() in {"pending", "processing"})
    done_count = sum(1 for r in award_requests if str(r.get("status") or "").lower() == "done")

    cards = "".join([
        _card("EC gesamt", _fmt_ec(total_ec), f"über {len(balances)} Konten"),
        _card("Ø EC", _fmt_ec(avg_ec), "Durchschnitt pro Konto"),
        _card("Verdient", _fmt_ec(total_earned), "geladene Buchungen"),
        _card("Ausgegeben", _fmt_ec(total_spent), "geladene Buchungen"),
        _card("Netto", _fmt_ec(net_loaded), "Verdient minus ausgegeben"),
        _card("Buchungen", txs.get("count", len(recent)), f"geladen: {txs.get('loaded_count', len(recent))}"),
        _card("Dashboard-Queue", pending_count, f"erledigt: {done_count}"),
    ])

    recent_rows = []
    for tx in recent[:250]:
        if not isinstance(tx, dict):
            continue
        uid = _user_id(tx.get("user_id"))
        amount = _num(tx.get("amount"), 0)
        recent_rows.append([
            _dt(tx.get("created_at")),
            _member_link(uid, tx.get("display_name") or names.get(uid, f"User {uid}")),
            _fmt_ec(amount),
            tx.get("raw_type") or "—",
            _short(tx.get("reason"), 180),
            tx.get("event_id") or tx.get("auction_id") or "—",
        ])

    earned_rows = [[_member_link(r.get("user_id"), r.get("display_name")), _fmt_ec(r.get("earned")), _fmt_ec(r.get("net")), r.get("count")] for r in top_earned[:25] if isinstance(r, dict)]
    spent_rows = [[_member_link(r.get("user_id"), r.get("display_name")), _fmt_ec(r.get("spent")), _fmt_ec(r.get("net")), r.get("count")] for r in top_spent[:25] if isinstance(r, dict)]
    activity_rows = [[_member_link(r.get("user_id"), r.get("display_name")), r.get("count"), _fmt_ec(r.get("earned")), _fmt_ec(r.get("spent")), _fmt_ec(r.get("net"))] for r in top_activity[:25] if isinstance(r, dict)]

    balance_rows = []
    for b in balances:
        if not isinstance(b, dict):
            continue
        balance_rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("balance"))])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/attendance">Anwesenheit</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="#queue">Dashboard-Buchungen</a><a href="#recent">Buchungen</a><a href="#top">Toplisten</a><a href="#balances">Konten</a><a href="/api/ec">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Analytics</div>
        <h1>🪙 EC-Verlauf</h1>
        <p class="muted">Read-only Auswertung. Es wird nichts verändert. Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel" id="queue">
      <h2>🌐 Dashboard-EC-Buchungen</h2>
      <p class="muted">Hier siehst du nach „EC wirklich buchen“, ob die Postgres-Anfrage noch offen ist, vom Bot verarbeitet wird oder fertig/abgelehnt wurde. Die Seite schreibt nichts direkt in JSON.</p>
      <p><a class="btn" href="/ec-queue">EC-Queue öffnen</a></p>
      {_table(['Angefragt','Status','Event','Typ','EC','Gebucht','Übersprungen','Admin','Details'], award_request_rows, placeholder='Dashboard-Buchungen durchsuchen…')}
    </section>
    <section class="panel" id="top">
      <h2>🏆 EC-Toplisten</h2>
      <div class="split">
        <div><h3>Meiste EC verdient</h3>{_table(['Spieler','Verdient','Netto','Buchungen'], earned_rows, placeholder='Verdienst durchsuchen…')}</div>
        <div><h3>Meiste EC ausgegeben</h3>{_table(['Spieler','Ausgegeben','Netto','Buchungen'], spent_rows, placeholder='Ausgaben durchsuchen…')}</div>
      </div>
      <h3>Meiste Buchungen</h3>{_table(['Spieler','Buchungen','Verdient','Ausgegeben','Netto'], activity_rows, placeholder='Aktivität durchsuchen…')}
    </section>
    <section class="panel" id="recent"><h2>🧾 Letzte EC-Buchungen</h2>{_table(['Zeit','Spieler','Betrag','Typ','Grund','Quelle'], recent_rows, placeholder='Buchungen durchsuchen…')}</section>
    <section class="panel" id="balances"><h2>🪙 Alle EC-Konten</h2>{_table(['Spieler','EC'], balance_rows, placeholder='EC-Konten durchsuchen…')}</section>
    """
    return _html_shell("EC-Verlauf · Ebo Dashboard", body)




def _participant_ids(participants: dict[str, Any]) -> tuple[set[int], set[int], set[int]]:
    yes_ids: set[int] = set()
    for grp in participants.get("yes") or []:
        if not isinstance(grp, dict):
            continue
        for p in grp.get("participants") or []:
            if not isinstance(p, dict):
                continue
            uid = _user_id(p.get("user_id") or p.get("id") or p.get("member_id"))
            if uid:
                yes_ids.add(uid)
    maybe_ids = {_user_id(p.get("user_id") or p.get("id") or p.get("member_id")) for p in (participants.get("maybe") or []) if isinstance(p, dict)}
    no_ids = {_user_id(p.get("user_id") or p.get("id") or p.get("member_id")) for p in (participants.get("no") or []) if isinstance(p, dict)}
    maybe_ids.discard(0)
    no_ids.discard(0)
    return yes_ids, maybe_ids, no_ids


def _activity_analytics(snap: dict[str, Any]) -> dict[str, Any]:
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    events = ((snap.get("events") or {}).get("items") or [])
    names = _profile_name_map(snap)
    member_ids = [_user_id(p.get("user_id")) for p in profiles if isinstance(p, dict) and _user_id(p.get("user_id"))]
    member_set = set(member_ids)
    by_user: dict[int, dict[str, Any]] = {}
    for uid in member_ids:
        by_user[uid] = {
            "user_id": uid,
            "display_name": names.get(uid, f"User {uid}"),
            "yes": 0,
            "maybe": 0,
            "no": 0,
            "missing": 0,
            "events_total": 0,
            "participation_rate": 0.0,
            "response_rate": 0.0,
        }

    event_rows: list[dict[str, Any]] = []
    role_totals: Counter[str] = Counter()
    for ev in events:
        if not isinstance(ev, dict):
            continue
        participants = ev.get("participants") if isinstance(ev.get("participants"), dict) else {}
        yes_ids, maybe_ids, no_ids = _participant_ids(participants)
        responded = (yes_ids | maybe_ids | no_ids) & member_set
        yes_m = yes_ids & member_set
        maybe_m = maybe_ids & member_set
        no_m = no_ids & member_set
        for role, count in (ev.get("yes_counts") or {}).items():
            role_totals[str(role or "Unbekannt")] += int(_num(count, 0))
        for uid, bucket in by_user.items():
            bucket["events_total"] += 1
            if uid in yes_m:
                bucket["yes"] += 1
            elif uid in maybe_m:
                bucket["maybe"] += 1
            elif uid in no_m:
                bucket["no"] += 1
            else:
                bucket["missing"] += 1
        member_count = len(member_set)
        event_rows.append({
            "event_id": ev.get("event_id"),
            "title": ev.get("title") or ev.get("event_id") or "Event",
            "when_iso": ev.get("when_iso"),
            "yes": len(yes_m),
            "maybe": len(maybe_m),
            "no": len(no_m),
            "responded": len(responded),
            "missing": max(0, member_count - len(responded)),
            "response_rate": (len(responded) / member_count * 100) if member_count else 0,
            "participation_rate": (len(yes_m) / member_count * 100) if member_count else 0,
        })

    for bucket in by_user.values():
        total = int(bucket.get("events_total") or 0)
        yes = int(bucket.get("yes") or 0)
        responded = yes + int(bucket.get("maybe") or 0) + int(bucket.get("no") or 0)
        bucket["participation_rate"] = (yes / total * 100) if total else 0
        bucket["response_rate"] = (responded / total * 100) if total else 0

    user_rows = list(by_user.values())
    most_participation = sorted(user_rows, key=lambda x: (float(x.get("participation_rate") or 0), int(x.get("yes") or 0)), reverse=True)
    most_missing = sorted(user_rows, key=lambda x: (int(x.get("missing") or 0), -int(x.get("yes") or 0)), reverse=True)
    event_rows.sort(key=lambda x: str(x.get("when_iso") or ""), reverse=True)

    voice = snap.get("voice") or {}
    voice_by_user: list[dict[str, Any]] = []
    raw_voice = voice.get("by_user") if isinstance(voice.get("by_user"), list) else []
    if raw_voice:
        for row in raw_voice:
            if not isinstance(row, dict):
                continue
            uid = _user_id(row.get("user_id"))
            if not uid or uid not in member_set:
                continue
            voice_by_user.append({
                "user_id": uid,
                "display_name": names.get(uid, f"User {uid}"),
                "sessions": int(_num(row.get("sessions"), 0)),
                "total_seconds": int(_num(row.get("total_seconds"), 0)),
                "last_left_at": row.get("last_left_at") or row.get("last_joined_at") or "",
            })
    else:
        temp: dict[int, dict[str, Any]] = {}
        for sess in voice.get("recent_sessions") or []:
            if not isinstance(sess, dict):
                continue
            uid = _user_id(sess.get("user_id") or sess.get("member_id"))
            if not uid or uid not in member_set:
                continue
            bucket = temp.setdefault(uid, {"user_id": uid, "display_name": names.get(uid, f"User {uid}"), "sessions": 0, "total_seconds": 0, "last_left_at": ""})
            bucket["sessions"] += 1
            bucket["total_seconds"] += int(_num(sess.get("duration_seconds"), 0))
            left = str(sess.get("left_at") or sess.get("joined_at") or "")
            if left and left > str(bucket.get("last_left_at") or ""):
                bucket["last_left_at"] = left
        voice_by_user = list(temp.values())
    voice_by_user.sort(key=lambda x: int(x.get("total_seconds") or 0), reverse=True)

    total_events = len(events)
    total_slots = total_events * len(member_set)
    total_yes = sum(int(x.get("yes") or 0) for x in user_rows)
    total_responded = sum(int(x.get("yes") or 0) + int(x.get("maybe") or 0) + int(x.get("no") or 0) for x in user_rows)
    total_missing = max(0, total_slots - total_responded)
    total_voice_seconds = sum(int(x.get("total_seconds") or 0) for x in voice_by_user)

    return {
        "member_count": len(member_set),
        "event_count": total_events,
        "total_slots": total_slots,
        "total_yes": total_yes,
        "total_responded": total_responded,
        "total_missing": total_missing,
        "participation_rate": (total_yes / total_slots * 100) if total_slots else 0,
        "response_rate": (total_responded / total_slots * 100) if total_slots else 0,
        "most_participation": most_participation,
        "most_missing": most_missing,
        "events": event_rows,
        "role_totals": role_totals.most_common(),
        "voice_by_user": voice_by_user,
        "total_voice_hours": total_voice_seconds / 3600,
    }


def _render_activity_analytics(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Analytics · Ebo Dashboard", f"<section class='panel'><h1>📈 Analytics</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    act = _activity_analytics(snap)
    base = _analytics_from_snapshot(snap)

    cards = "".join([
        _card("Events", act.get("event_count", 0), "im Snapshot"),
        _card("Mitglieder", act.get("member_count", 0), "Gildenrolle"),
        _card("Teilnahmequote", f"{act.get('participation_rate', 0):.0f} %", "Zusagen / mögliche Plätze"),
        _card("Antwortquote", f"{act.get('response_rate', 0):.0f} %", "Zusage/Vielleicht/Nein"),
        _card("Nicht abgestimmt", act.get("total_missing", 0), "über alle Events"),
        _card("Voice-Stunden", f"{act.get('total_voice_hours', 0):.1f}", "geladene Sessions"),
        _card("EC gesamt", _fmt_ec(base.get("total_ec", 0)), f"Ø {_fmt_ec(base.get('avg_ec', 0))}"),
        _card("Aktive Auktionen", base.get("active_auctions", 0), "aktuell offen"),
    ])

    top_rows = []
    for r in act.get("most_participation") or []:
        if not isinstance(r, dict):
            continue
        top_rows.append([
            _member_link(r.get("user_id"), r.get("display_name")),
            f"{float(r.get('participation_rate') or 0):.0f} %",
            r.get("yes"),
            r.get("maybe"),
            r.get("no"),
            r.get("missing"),
        ])

    missing_rows = []
    for r in act.get("most_missing") or []:
        if not isinstance(r, dict):
            continue
        missing_rows.append([
            _member_link(r.get("user_id"), r.get("display_name")),
            r.get("missing"),
            f"{float(r.get('response_rate') or 0):.0f} %",
            r.get("yes"),
            r.get("maybe"),
            r.get("no"),
        ])

    event_rows = []
    for ev in act.get("events") or []:
        if not isinstance(ev, dict):
            continue
        event_rows.append([
            _event_link(ev.get("event_id"), ev.get("title")),
            _dt(ev.get("when_iso")),
            ev.get("yes"),
            ev.get("maybe"),
            ev.get("no"),
            ev.get("missing"),
            f"{float(ev.get('response_rate') or 0):.0f} %",
        ])

    voice_rows = []
    for v in act.get("voice_by_user") or []:
        if not isinstance(v, dict):
            continue
        seconds = int(_num(v.get("total_seconds"), 0))
        voice_rows.append([
            _member_link(v.get("user_id"), v.get("display_name")),
            f"{seconds / 3600:.1f} h",
            v.get("sessions"),
            _dt(v.get("last_left_at")),
        ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/ec">EC-Verlauf</a><a href="#activity">Teilnahme</a><a href="#missing">Nicht abgestimmt</a><a href="#voice">Voice</a><a href="/api/analytics">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Analytics</div>
        <h1>📈 Aktivität & Teilnahme</h1>
        <p class="muted">Read-only Auswertung aus dem aktuellen Dashboard-Snapshot. Es wird nichts verändert. Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🎭 Rollenverteilung in Zusagen</h2>{_bars(act.get('role_totals') or [], max_items=12)}</section>
    <section class="panel" id="activity"><h2>✅ Aktivste Teilnehmer</h2>{_table(['Spieler','Quote','Zusagen','Vielleicht','Nein','Nicht abgestimmt'], top_rows[:80], placeholder='Teilnahme durchsuchen…')}</section>
    <section class="panel" id="missing"><h2>⚠️ Meiste Nicht-Abstimmungen</h2>{_table(['Spieler','Nicht abgestimmt','Antwortquote','Zusagen','Vielleicht','Nein'], missing_rows[:80], placeholder='Nicht-Abstimmer durchsuchen…')}</section>
    <section class="panel"><h2>📅 Events im Vergleich</h2>{_table(['Event','Zeit','Zusagen','Vielleicht','Nein','Nicht abgestimmt','Antwortquote'], event_rows[:200], placeholder='Events durchsuchen…')}</section>
    <section class="panel" id="voice"><h2>🎙️ Voice-Zeit</h2>{_table(['Spieler','Voice-Zeit','Sessions','zuletzt'], voice_rows[:120], placeholder='Voice durchsuchen…')}</section>
    """
    return _html_shell("Analytics · Ebo Dashboard", body)




def _source_health_rows(snap: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for key, info in ((snap.get("source_health") or {}).items() if isinstance(snap.get("source_health"), dict) else []):
        if not isinstance(info, dict):
            continue
        rows.append([
            key,
            info.get("file"),
            "ja" if info.get("exists") else "nein",
            "OK" if info.get("ok") else _short(info.get("error"), 120),
            info.get("size_bytes", 0),
            _dt(info.get("modified_at")),
        ])
    return rows


def _render_settings_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>⚙️ Einstellungen</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild = snap.get("guild") or {}
    settings = snap.get("settings") or {}
    member_filter = settings.get("member_filter") or ((guild.get("member_filter") or {}))
    counts = settings.get("counts") or {}

    role_text = "nicht gesetzt"
    if isinstance(member_filter, dict) and member_filter.get("mode") == "discord_role":
        role_text = f"{member_filter.get('role_name')} ({member_filter.get('role_id')})"

    auth_mode = _auth_mode()
    discord_state = "aktiv" if _discord_oauth_enabled() else "nicht eingerichtet"
    allowed_roles = ", ".join(sorted(_allowed_role_ids())) or "—"
    admin_roles = ", ".join(sorted(_admin_role_ids())) or "—"

    cards = "".join([
        _card("Gildenrolle", role_text, f"Mitglieder: {member_filter.get('eligible_count', 0) if isinstance(member_filter, dict) else 0}"),
        _card("Login", auth_mode, f"Discord: {discord_state}"),
        _card("Module", counts.get("modules", 0), "gefundene Config-Bereiche"),
        _card("Kanäle", counts.get("channels", 0), "aus Configs erkannt"),
        _card("Rollen", counts.get("roles", 0), "aus Configs erkannt"),
        _card("Backend", (snap.get("storage") or {}).get("runtime_backend"), "Runtime-Datenbank"),
    ])

    module_rows = []
    for m in settings.get("modules") or []:
        if isinstance(m, dict):
            module_rows.append([m.get("module"), "ja" if m.get("configured") else "nein", "ja" if m.get("source_exists") else "nein", m.get("top_level_keys")])

    channel_rows = []
    for ch in settings.get("channels") or []:
        if isinstance(ch, dict):
            channel_rows.append([ch.get("source"), ch.get("key"), ch.get("name") or "nicht aufgelöst", ch.get("channel_id")])

    role_rows = []
    for r in settings.get("roles") or []:
        if isinstance(r, dict):
            role_rows.append([r.get("source"), r.get("key"), r.get("name") or "nicht aufgelöst", r.get("role_id")])

    setting_rows = []
    for row in settings.get("settings") or []:
        if isinstance(row, dict):
            setting_rows.append([row.get("source"), row.get("key"), row.get("value")])

    auth_rows = [
        ["DASHBOARD_AUTH_MODE", auth_mode, "basic / hybrid / discord"],
        ["Discord OAuth", discord_state, "Client ID + Secret gesetzt"],
        ["Gildenrolle", role_text, "Fallback für erlaubte Dashboard-Rolle"],
        ["Allowed Role IDs", allowed_roles, "DASHBOARD_ALLOWED_ROLE_IDS / MEMBER_ROLE_ID(S)"],
        ["Admin Role IDs", admin_roles, "DASHBOARD_ADMIN_ROLE_IDS"],
        ["Public Base URL", _env("DASHBOARD_PUBLIC_BASE_URL") or "auto", "für Redirect URI / Custom Domain"],
        ["Redirect URI", _env("DASHBOARD_DISCORD_REDIRECT_URI") or "auto: /auth/discord/callback", "muss im Discord Developer Portal stehen"],
        ["Session Secret", "gesetzt" if _env("DASHBOARD_SESSION_SECRET") else "Fallback", "für signiertes Dashboard-Cookie"],
    ]

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/ec">EC-Verlauf</a><a href="/audit">Audit</a><a href="/system">System</a><a href="/api/settings">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Read-only Setup</div>
        <h1>⚙️ Einstellungen & Setup</h1>
        <p class="muted">Zeigt die aktuelle Bot-/Server-Konfiguration aus dem Snapshot. Es wird nichts verändert.</p>
      </div>
      <a class="btn" href="/">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🔐 Login & Rechte</h2><p class="muted">Read-only Anzeige. Änderungen machst du aktuell über Railway-Variablen oder Discord-Commands.</p>{_table(['Setting','Wert','Hinweis'], auth_rows, placeholder='Login-Settings durchsuchen…')}</section>
    <section class="panel"><h2>🧩 Module</h2>{_table(['Bereich','konfiguriert','Quelle vorhanden','Keys'], module_rows, placeholder='Module durchsuchen…')}</section>
    <section class="panel"><h2>📺 Kanäle</h2>{_table(['Quelle','Setting','Kanal','ID'], channel_rows, placeholder='Kanäle durchsuchen…')}</section>
    <section class="panel"><h2>🎭 Rollen</h2>{_table(['Quelle','Setting','Rolle','ID'], role_rows, placeholder='Rollen durchsuchen…')}</section>
    <section class="panel"><h2>🔧 Erkannte Einstellungen</h2>{_table(['Quelle','Key','Wert'], setting_rows, placeholder='Settings durchsuchen…')}</section>
    """
    return _html_shell("Einstellungen · Ebo Dashboard", body)


def _render_audit_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>🧾 Audit</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    audit = snap.get("audit") or {}
    logs = [x for x in (audit.get("recent_logs") or []) if isinstance(x, dict)]
    by_action = Counter(str(x.get("action") or "Unbekannt") for x in logs)
    by_actor = Counter(str(x.get("actor_id") or "Unbekannt") for x in logs)
    cards = "".join([
        _card("Audit gesamt", audit.get("logs_total", 0), "in Runtime-DB"),
        _card("geladen", len(logs), "im Snapshot"),
        _card("Aktionen", len(by_action), "unterschiedliche Typen"),
        _card("Akteure", len(by_actor), "unterschiedliche IDs"),
    ])
    log_rows = []
    for a in logs:
        log_rows.append([_dt(a.get("created_at")), a.get("action"), a.get("actor_id"), _short(a.get("summary"), 180)])
    action_rows = [[k, v] for k, v in by_action.most_common(80)]
    actor_rows = [[k, v] for k, v in by_actor.most_common(80)]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/settings">Einstellungen</a><a href="/system">System</a><a href="#logs">Logs</a><a href="/api/audit">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Audit Trail</div><h1>🧾 Audit-Log</h1><p class="muted">Read-only Protokoll der wichtigsten Bot-Aktionen. Snapshot: {_e(_dt(data.get('published_at')))}</p></div><a class="btn" href="/">Zurück</a></section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>Aktionen</h2>{_bars(by_action.most_common(12), max_items=12)}</div><div class="panel"><h2>Akteure</h2>{_bars(by_actor.most_common(12), max_items=12)}</div></section>
    <section class="panel"><h2>Aktionen als Tabelle</h2>{_table(['Aktion','Anzahl'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>Akteure als Tabelle</h2>{_table(['Actor ID','Anzahl'], actor_rows, placeholder='Akteure durchsuchen…')}</section>
    <section class="panel" id="logs"><h2>Letzte Audit-Einträge</h2>{_table(['Zeit','Aktion','Actor','Zusammenfassung'], log_rows, placeholder='Audit durchsuchen…')}</section>
    """
    return _html_shell("Audit · Ebo Dashboard", body)


def _render_system_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>🛠️ System</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    storage = snap.get("storage") or {}
    guild = snap.get("guild") or {}
    source_rows = _source_health_rows(snap)
    source_ok = sum(1 for r in source_rows if str(r[3]) == "OK")
    source_bad = len(source_rows) - source_ok
    cards = "".join([
        _card("Snapshot ID", data.get("id"), "Postgres"),
        _card("Schema", snap.get("schema_version"), "Dashboard-Schema"),
        _card("Backend", storage.get("runtime_backend"), storage.get("database_url_kind")),
        _card("Quellen OK", source_ok, f"Fehler/fehlen: {source_bad}"),
        _card("Discord Cache", guild.get("cached_members_loaded"), "geladene Members"),
        _card("Guild ID", guild.get("id"), guild.get("name")),
    ])
    storage_rows = [[k, v] for k, v in storage.items()]
    guild_rows = [[k, v] for k, v in guild.items() if k != "member_filter"]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/settings">Einstellungen</a><a href="/audit">Audit</a><a href="/api/system">API</a></nav>
    <section class="hero"><div><div class="eyebrow">System</div><h1>🛠️ System & Datenquellen</h1><p class="muted">Nur Diagnose. Keine Schreibzugriffe.</p></div><a class="btn" href="/">Zurück</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>Speicher</h2>{_table(['Key','Wert'], storage_rows, placeholder='Speicher durchsuchen…')}</section>
    <section class="panel"><h2>Guild</h2>{_table(['Key','Wert'], guild_rows, placeholder='Guild durchsuchen…')}</section>
    <section class="panel"><h2>JSON-Quellen</h2>{_table(['Key','Datei','vorhanden','Status','Bytes','Geändert'], source_rows, placeholder='Quellen durchsuchen…')}</section>
    """
    return _html_shell("System · Ebo Dashboard", body)



def _insights(snap: dict[str, Any]) -> dict[str, Any]:
    return (snap.get("insights") or {}) if isinstance(snap.get("insights"), dict) else {}


def _insight_members(snap: dict[str, Any]) -> list[dict[str, Any]]:
    ins = _insights(snap)
    members = ins.get("members") if isinstance(ins.get("members"), list) else []
    if members:
        return [m for m in members if isinstance(m, dict)]

    # Fallback für ältere Snapshots: aus Profilen/EC/Needs zusammenbauen.
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    balances = _balance_map(snap)
    need_ids = _need_user_ids(snap)
    rows: list[dict[str, Any]] = []
    for p in profiles:
        if not isinstance(p, dict):
            continue
        uid = _user_id(p.get("user_id"))
        rows.append({
            "user_id": uid,
            "display_name": p.get("display_name") or p.get("ingame_name") or f"User {uid}",
            "ingame_name": p.get("ingame_name"),
            "main_role": p.get("main_role"),
            "gearscore": p.get("gearscore"),
            "ec_balance": balances.get(uid),
            "has_profile": True,
            "has_ec": uid in balances,
            "has_needs": uid in need_ids,
            "risk_score": 0,
            "risk_flags": [],
        })
    return rows


def _yesno(value: Any) -> str:
    return "ja" if bool(value) else "nein"


def _risk_flags_text(m: dict[str, Any]) -> str:
    flags = m.get("risk_flags") if isinstance(m.get("risk_flags"), list) else []
    return ", ".join(str(x) for x in flags) if flags else "—"


def _render_members_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Mitglieder · Ebo Dashboard", f"<section class='panel'><h1>👥 Mitglieder</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    ins = _insights(snap)
    members = _insight_members(snap)
    quality = ins.get("quality") if isinstance(ins.get("quality"), dict) else {}

    rows = []
    for m in members:
        uid = _user_id(m.get("user_id"))
        rows.append([
            _member_link(uid, m.get("display_name")),
            m.get("ingame_name") or "—",
            m.get("main_role") or "—",
            m.get("gearscore") or "—",
            _fmt_ec(m.get("ec_balance")) if m.get("ec_balance") is not None else "—",
            m.get("main_need_count", "—"),
            m.get("secondary_need_count", "—"),
            m.get("event_responses", "—"),
            f"{_num(m.get('voice_hours'), 0):.1f} h",
            m.get("loot_won_count", "—"),
            _risk_flags_text(m),
        ])

    risk_rows = []
    for m in (ins.get("risk_members") or [])[:120]:
        if not isinstance(m, dict):
            continue
        risk_rows.append([_member_link(m.get("user_id"), m.get("display_name")), m.get("risk_score"), _risk_flags_text(m)])

    cards = "".join([
        _card("Mitglieder", len(members), "gesetzte Gildenrolle"),
        _card("ohne Profil", quality.get("missing_profile", 0), "Datenqualität"),
        _card("ohne EC", quality.get("missing_ec", 0), "Datenqualität"),
        _card("ohne Needs", quality.get("missing_needs", 0), "Needliste"),
        _card("keine Eventantwort", quality.get("no_event_response", 0), "im Snapshot"),
        _card("keine Voice-Zeit", quality.get("no_voice_time", 0), "gemessen"),
    ])
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/needs">Needs</a><a href="/loot">Loot</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/exports">Exports</a><a href="/api/members">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Roster & Datenqualität</div><h1>👥 Mitglieder</h1><p class="muted">Alle Mitglieder aus der gesetzten Gildenrolle. Read-only.</p></div><a class="btn" href="/export/members.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>⚠️ Auffällige Mitglieder</h2>{_table(['Spieler','Score','Hinweise'], risk_rows, placeholder='Auffälligkeiten durchsuchen…')}</section>
    <section class="panel"><h2>👥 Mitgliederliste</h2>{_table(['Name','Ingame','Rolle','GS','EC','Main','Secondary','Eventantworten','Voice','Loot','Hinweise'], rows, placeholder='Mitglieder durchsuchen…')}</section>
    """
    return _html_shell("Mitglieder · Ebo Dashboard", body)


def _render_needs_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Needs · Ebo Dashboard", f"<section class='panel'><h1>🎁 Needs</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    ins = _insights(snap)
    need_ins = ins.get("needs") if isinstance(ins.get("needs"), dict) else {}
    needs = (((snap.get("loot") or {}).get("needs") or {}).get("items") or [])

    top_main_rows = [[x.get("label"), x.get("count")] for x in (need_ins.get("top_main") or []) if isinstance(x, dict)]
    top_secondary_rows = [[x.get("label"), x.get("count")] for x in (need_ins.get("top_secondary") or []) if isinstance(x, dict)]
    without_rows = [[_member_link(x.get("user_id"), x.get("display_name")), x.get("main_role") or "—", x.get("gearscore") or "—"] for x in (need_ins.get("users_without_needs") or []) if isinstance(x, dict)]

    all_rows = []
    for n in needs:
        if not isinstance(n, dict):
            continue
        main = ", ".join(str(x) for x in (n.get("main") or [])) or "—"
        sec = ", ".join(str(x) for x in (n.get("secondary") or [])) or "—"
        all_rows.append([_member_link(n.get("user_id"), n.get("display_name")), n.get("main_count"), n.get("secondary_count"), _short(main, 260), _short(sec, 260)])

    cards = "".join([
        _card("Main-Needs", need_ins.get("main_total", sum(_num(x[1]) for x in top_main_rows)), "offene Einträge"),
        _card("Secondary-Needs", need_ins.get("secondary_total", sum(_num(x[1]) for x in top_secondary_rows)), "offene Einträge"),
        _card("Need-User", (snap.get("loot") or {}).get("needs", {}).get("user_count", len(all_rows)), "mit Einträgen"),
        _card("ohne Needliste", len(without_rows), "Gildenrolle"),
    ])
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/members">Mitglieder</a><a href="/loot">Loot</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/exports">Exports</a><a href="/api/needs">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Needlisten</div><h1>🎁 Need-Analytics</h1><p class="muted">Zeigt, welche Items wie oft gebraucht werden. Read-only.</p></div><a class="btn" href="/export/needs.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>Top Main-Needs</h2>{_table(['Item','Anzahl'], top_main_rows, placeholder='Main-Needs durchsuchen…')}</div><div class="panel"><h2>Top Secondary-Needs</h2>{_table(['Item','Anzahl'], top_secondary_rows, placeholder='Secondary-Needs durchsuchen…')}</div></section>
    <section class="panel"><h2>🧹 Mitglieder ohne Needliste</h2>{_table(['Spieler','Rolle','GS'], without_rows, placeholder='Ohne Needliste durchsuchen…')}</section>
    <section class="panel"><h2>Alle Needlisten</h2>{_table(['Spieler','Main','Secondary','Main-Needs','Secondary-Needs'], all_rows, placeholder='Needlisten durchsuchen…')}</section>
    """
    return _html_shell("Needs · Ebo Dashboard", body)


def _render_loot_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Loot · Ebo Dashboard", f"<section class='panel'><h1>🎁 Loot</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    ins = _insights(snap)
    loot_ins = ins.get("loot") if isinstance(ins.get("loot"), dict) else {}
    names = _profile_name_map(snap)
    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])

    active_rows = []
    closed_rows = []
    for a in auctions:
        if not isinstance(a, dict):
            continue
        leader = "—"
        uid = _user_id(a.get("top_bid_user_id"))
        if uid and a.get("top_bid_amount") is not None:
            leader = f"{names.get(uid, a.get('top_bid_user_name') or f'User {uid}')} · {_fmt_ec(a.get('top_bid_amount'))} EC"
        row = [_auction_link(a.get("auction_id"), a.get("item_name")), _phase_label(a), a.get("status"), a.get("bid_count"), leader, _member_link(a.get("winner_user_id"), a.get("winner_name")) if a.get("winner_user_id") else "—", _dt(a.get("ends_at"))]
        if str(a.get("status") or "").lower() in {"open", "active", "running", "bidding", "roll", "sale", "free", "main", "secondary"}:
            active_rows.append(row)
        else:
            closed_rows.append(row)

    winner_rows = []
    for w in loot_ins.get("winner_rows") or []:
        if not isinstance(w, dict):
            continue
        winner_rows.append([_member_link(w.get("user_id"), names.get(_user_id(w.get("user_id"),), f"User {w.get('user_id')}")), w.get("won_count")])

    leader_rows = []
    for l in loot_ins.get("active_leaders") or []:
        if not isinstance(l, dict):
            continue
        leader_rows.append([_member_link(l.get("user_id"), names.get(_user_id(l.get("user_id")), f"User {l.get('user_id')}")), l.get("lead_count")])

    cards = "".join([
        _card("Auktionen", len(auctions), "im Snapshot"),
        _card("Aktiv", len(active_rows), "läuft/offen"),
        _card("Abgeschlossen", len(closed_rows), "nicht aktiv"),
        _card("Gewinner", len(winner_rows), "Spieler mit Loot"),
    ])
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/members">Mitglieder</a><a href="/needs">Needs</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/exports">Exports</a><a href="/api/loot">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot & Auktionen</div><h1>🎁 Loot-Dashboard</h1><p class="muted">Aktive Auktionen, Gewinnerverteilung und Auktionshistorie. Read-only.</p></div><a class="btn" href="/export/auctions.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>🏆 Loot-Gewinner</h2>{_table(['Spieler','Items'], winner_rows, placeholder='Gewinner durchsuchen…')}</div><div class="panel"><h2>📈 Aktuell führend</h2>{_table(['Spieler','Führungen'], leader_rows, placeholder='Führungen durchsuchen…')}</div></section>
    <section class="panel"><h2>🟢 Aktive Auktionen</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende'], active_rows, placeholder='Aktive Auktionen durchsuchen…')}</section>
    <section class="panel"><h2>📜 Auktionshistorie</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende'], closed_rows[:250], placeholder='Auktionshistorie durchsuchen…')}</section>
    """
    return _html_shell("Loot · Ebo Dashboard", body)



_LOOT_ACTIVE_STATUSES = {"open", "active", "running", "bidding", "roll", "rolling", "sale", "free", "main", "secondary", "pending", "need"}
_LOOT_DONE_STATUSES = {"done", "closed", "finished", "ended", "delivered", "completed", "sold", "awarded", "winner"}
_LOOT_CANCELLED_STATUSES = {"cancelled", "canceled", "deleted", "aborted", "ignored"}


def _loot_text(value: Any) -> str:
    """Robuste Text-Extraktion für Item-/Need-/Auktionswerte aus älteren und neueren Snapshots."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        for key in (
            "item_name", "item", "name", "label", "title", "display_name", "short_name",
            "ingame_name", "value", "text", "query", "loot_name",
        ):
            v = value.get(key)
            if isinstance(v, (str, int, float)) and str(v).strip():
                return str(v).strip()
        # Manche Need-Einträge liegen als {slot: item} vor.
        for v in value.values():
            if isinstance(v, (str, int, float)) and str(v).strip():
                return str(v).strip()
    return str(value).strip()


def _loot_key(value: Any) -> str:
    label = _loot_text(value).lower()
    label = re.sub(r"\s+", " ", label).strip()
    label = re.sub(r"[^a-z0-9äöüß ._+\-/]", "", label)
    return label


def _loot_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _loot_status_label(status: Any) -> dict[str, str]:
    raw = str(status or "—").strip() or "—"
    s = raw.lower()
    if s in _LOOT_ACTIVE_STATUSES:
        return _raw(f"<span class='pill'>offen</span>")
    if s in _LOOT_DONE_STATUSES:
        return _raw(f"<span class='pill'>erledigt</span>")
    if s in _LOOT_CANCELLED_STATUSES:
        return _raw(f"<span class='pill'>abgebrochen</span>")
    return _raw(f"<span class='pill'>{_e(raw)}</span>")


def _loot_is_active(auction: dict[str, Any]) -> bool:
    s = _loot_status(auction.get("status"))
    phase = _loot_status(auction.get("phase"))
    return s in _LOOT_ACTIVE_STATUSES or (not s and phase in {"need", "free", "sale", "roll"})


def _loot_is_done(auction: dict[str, Any]) -> bool:
    s = _loot_status(auction.get("status"))
    return s in _LOOT_DONE_STATUSES or bool(auction.get("delivered_at"))


def _loot_bid_count(auction: dict[str, Any]) -> int:
    if auction.get("bid_count") is not None:
        return int(_num(auction.get("bid_count"), 0))
    bids = auction.get("bids")
    if isinstance(bids, list):
        return len(bids)
    return 0


def _loot_leader_text(auction: dict[str, Any], names: dict[int, str]) -> str:
    uid = _user_id(auction.get("top_bid_user_id") or auction.get("leader_user_id"))
    amount = auction.get("top_bid_amount")
    if amount is None:
        amount = auction.get("leader_bid") or auction.get("highest_bid")
    if uid and amount is not None:
        return f"{names.get(uid, auction.get('top_bid_user_name') or auction.get('leader_name') or f'User {uid}')} · {_fmt_ec(amount)} EC"
    if auction.get("top_bid_user_name") or auction.get("leader_name"):
        return str(auction.get("top_bid_user_name") or auction.get("leader_name"))
    return "—"


def _loot_winner_cell(auction: dict[str, Any], names: dict[int, str]) -> Any:
    uid = _user_id(auction.get("winner_user_id") or auction.get("delivered_to_user_id"))
    if uid:
        return _member_link(uid, auction.get("winner_name") or names.get(uid, f"User {uid}"))
    return auction.get("winner_name") or "—"


def _loot_need_index(snap: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index: normalisierter Itemname -> Spieler mit Main-/Second-Need."""
    rows = (((snap.get("loot") or {}).get("needs") or {}).get("items") or [])
    names = _profile_name_map(snap)
    idx: dict[str, dict[str, Any]] = {}

    def add(kind: str, entry: Any, user_id: int, display_name: str) -> None:
        label = _loot_text(entry)
        key = _loot_key(label)
        if not key:
            return
        bucket = idx.setdefault(key, {"item": label, "main": [], "secondary": []})
        if len(label) > len(str(bucket.get("item") or "")):
            bucket["item"] = label
        person = {"user_id": user_id, "display_name": display_name}
        arr = bucket.setdefault(kind, [])
        if not any(_user_id(x.get("user_id")) == user_id for x in arr if isinstance(x, dict)):
            arr.append(person)

    for row in rows:
        if not isinstance(row, dict):
            continue
        uid = _user_id(row.get("user_id") or row.get("discord_id") or row.get("member_id"))
        name = str(row.get("display_name") or row.get("name") or names.get(uid, f"User {uid}" if uid else "Unbekannt"))
        for entry in (row.get("main") or row.get("main_needs") or row.get("main_items") or []):
            add("main", entry, uid, name)
        for entry in (row.get("secondary") or row.get("secondary_needs") or row.get("secondary_items") or row.get("second") or []):
            add("secondary", entry, uid, name)
    return idx


def _loot_people_html(people: list[dict[str, Any]], *, limit: int = 5) -> dict[str, str]:
    if not people:
        return _raw("—")
    parts = []
    for p in people[:limit]:
        uid = _user_id(p.get("user_id"))
        label = _e(p.get("display_name") or f"User {uid}")
        if uid:
            parts.append(f'<a class="link" href="/member/{uid}">{label}</a>')
        else:
            parts.append(label)
    more = len(people) - limit
    if more > 0:
        parts.append(f"+{more}")
    return _raw(", ".join(parts))


def _loot_mode_bucket(auction: dict[str, Any]) -> str:
    phase = _loot_status(auction.get("phase"))
    mode = _loot_status(auction.get("eligibility_mode"))
    text = f"{phase} {mode}"
    if "main" in text:
        return "Main-Need"
    if "secondary" in text or "second" in text:
        return "Second-Need"
    if "sale" in text or auction.get("fixed_price") is not None:
        return "Sale"
    if "free" in text or "all" in text:
        return "Freie Auktion"
    if auction.get("junk_drop"):
        return "Müll/Sale"
    return _phase_label(auction)


def _loot_next_step(auction: dict[str, Any], need_info: Optional[dict[str, Any]] = None) -> str:
    s = _loot_status(auction.get("status"))
    bids = _loot_bid_count(auction)
    winner = bool(auction.get("winner_user_id") or auction.get("winner_name"))
    delivered = bool(auction.get("delivered_at"))
    phase = _loot_status(auction.get("phase"))
    mode = _loot_mode_bucket(auction).lower()
    main_need_count = len((need_info or {}).get("main") or [])
    sec_need_count = len((need_info or {}).get("secondary") or [])

    if delivered:
        return "fertig"
    if winner and not delivered:
        return "Übergabe markieren / prüfen"
    if s in _LOOT_CANCELLED_STATUSES:
        return "abgebrochen"
    if s in _LOOT_DONE_STATUSES:
        return "Ergebnis prüfen"
    if bids > 0:
        return "läuft: Gebote beobachten"
    if "sale" in mode:
        return "Sale ohne Käufer prüfen"
    if "freie" in mode or phase == "free":
        return "freie Auktion offen"
    if main_need_count <= 0 and sec_need_count <= 0:
        return "kein Need sichtbar → freie Auktion/Sale prüfen"
    if main_need_count > 0:
        return "Main-Need-Spieler warten auf Gebot"
    if sec_need_count > 0:
        return "Second-Need-Spieler warten auf Gebot"
    return "prüfen"


def _loot_center_payload_from_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    names = _profile_name_map(snap)
    loot = snap.get("loot") or {}
    auction_items = (((loot.get("auctions") or {}).get("items") or []))
    auctions = [a for a in auction_items if isinstance(a, dict)]
    need_index = _loot_need_index(snap)

    active: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    handover: list[dict[str, Any]] = []
    no_bid: list[dict[str, Any]] = []
    sale_free: list[dict[str, Any]] = []
    next_actions: list[dict[str, Any]] = []

    for a in auctions:
        item = _loot_text(a.get("item_name") or a.get("item") or a.get("name") or a.get("label") or a.get("auction_id"))
        key = _loot_key(item)
        need_info = need_index.get(key, {"main": [], "secondary": []})
        bids = _loot_bid_count(a)
        mode = _loot_mode_bucket(a)
        entry = {
            "auction_id": str(a.get("auction_id") or a.get("id") or ""),
            "item": item or str(a.get("auction_id") or "—"),
            "status": str(a.get("status") or "—"),
            "phase": str(a.get("phase") or "—"),
            "mode": mode,
            "bid_count": bids,
            "leader": _loot_leader_text(a, names),
            "winner_user_id": _user_id(a.get("winner_user_id") or a.get("delivered_to_user_id")),
            "winner_name": str(a.get("winner_name") or ""),
            "ends_at": a.get("ends_at") or a.get("expires_at") or a.get("end_at"),
            "created_at": a.get("created_at") or a.get("started_at"),
            "delivered_at": a.get("delivered_at"),
            "fixed_price": a.get("fixed_price"),
            "start_bid": a.get("start_bid"),
            "min_increment": a.get("min_increment"),
            "main_need_count": len(need_info.get("main") or []),
            "secondary_need_count": len(need_info.get("secondary") or []),
            "next_step": _loot_next_step(a, need_info),
            "raw": a,
        }
        if _loot_is_active(a):
            active.append(entry)
            next_actions.append(entry)
            if bids <= 0:
                no_bid.append(entry)
            if mode.lower() in {"sale", "freie auktion", "müll/sale"}:
                sale_free.append(entry)
        else:
            history.append(entry)
        if (a.get("winner_user_id") or a.get("winner_name")) and not a.get("delivered_at"):
            handover.append(entry)
            if entry not in next_actions:
                next_actions.append(entry)

    need_rows: list[dict[str, Any]] = []
    for key, info in need_index.items():
        main = info.get("main") or []
        sec = info.get("secondary") or []
        linked_active = [a for a in active if _loot_key(a.get("item")) == key]
        need_rows.append({
            "item": info.get("item") or key,
            "main_count": len(main),
            "secondary_count": len(sec),
            "total": len(main) + len(sec),
            "main": main,
            "secondary": sec,
            "active_auction_count": len(linked_active),
        })
    need_rows.sort(key=lambda x: (-int(x.get("main_count") or 0), -int(x.get("secondary_count") or 0), str(x.get("item") or "").lower()))

    next_actions.sort(key=lambda x: (0 if "Übergabe" in str(x.get("next_step")) else 1, _dt(x.get("ends_at")), str(x.get("item") or "").lower()))
    active.sort(key=lambda x: (_dt(x.get("ends_at")), str(x.get("item") or "").lower()))
    history.sort(key=lambda x: (_dt(x.get("ends_at")), str(x.get("item") or "").lower()), reverse=True)

    return {
        "auctions_total": len(auctions),
        "active": active,
        "history": history,
        "handover": handover,
        "no_bid": no_bid,
        "sale_free": sale_free,
        "next_actions": next_actions,
        "needs": need_rows,
        "raw_loot": loot,
    }



def _loot_words(value: Any) -> set[str]:
    key = _loot_key(value)
    return {t for t in re.split(r"[^a-z0-9äöüß]+", key) if len(t) >= 2}


def _loot_match_score(query: str, candidate: Any) -> int:
    """Einfacher robuster Item-Match für Truhen-/Drop-Checks."""
    q = _loot_key(query)
    c = _loot_key(candidate)
    if not q or not c:
        return 0
    if q == c:
        return 100
    if q in c or c in q:
        # "Aridus Stab" findet auch längere Ingame-Namen.
        return 92
    qw = _loot_words(q)
    cw = _loot_words(c)
    if not qw or not cw:
        return 0
    inter = len(qw & cw)
    if inter <= 0:
        return 0
    # Wichtigere Wertung: wie viel vom Suchbegriff wurde getroffen?
    coverage = inter / max(1, len(qw))
    precision = inter / max(1, len(cw))
    score = int(round((coverage * 0.75 + precision * 0.25) * 88))
    # Bonus, wenn mindestens zwei Wörter übereinstimmen.
    if inter >= 2:
        score += 8
    return min(score, 89)


def _loot_check_payload_from_snapshot(snap: dict[str, Any], item_query: str = "") -> dict[str, Any]:
    """Read-only Drop-/Truhencheck: Wer braucht ein Item und läuft dazu schon eine Auktion?"""
    names = _profile_name_map(snap)
    query = str(item_query or "").strip()
    need_index = _loot_need_index(snap)
    auctions_raw = [a for a in ((((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])) if isinstance(a, dict)]

    need_matches: list[dict[str, Any]] = []
    for key, info in need_index.items():
        item = info.get("item") or key
        score = _loot_match_score(query, item) if query else 0
        if query and score < 40:
            continue
        need_matches.append({
            "item": item,
            "score": score,
            "main_count": len(info.get("main") or []),
            "secondary_count": len(info.get("secondary") or []),
            "main": info.get("main") or [],
            "secondary": info.get("secondary") or [],
        })

    if query:
        need_matches.sort(key=lambda r: (-int(r.get("score") or 0), -int(r.get("main_count") or 0), -int(r.get("secondary_count") or 0), str(r.get("item") or "").lower()))
    else:
        need_matches.sort(key=lambda r: (-int(r.get("main_count") or 0), -int(r.get("secondary_count") or 0), str(r.get("item") or "").lower()))
        need_matches = need_matches[:50]

    auction_matches: list[dict[str, Any]] = []
    for a in auctions_raw:
        item = _loot_text(a.get("item_name") or a.get("item") or a.get("name") or a.get("label") or a.get("auction_id"))
        score = _loot_match_score(query, item) if query else 0
        if query and score < 40:
            continue
        auction_matches.append({
            "auction_id": str(a.get("auction_id") or a.get("id") or ""),
            "item": item or str(a.get("auction_id") or "—"),
            "score": score,
            "status": str(a.get("status") or "—"),
            "phase": str(a.get("phase") or "—"),
            "mode": _loot_mode_bucket(a),
            "active": _loot_is_active(a),
            "bid_count": _loot_bid_count(a),
            "leader": _loot_leader_text(a, names),
            "winner": _loot_winner_cell(a, names),
            "ends_at": a.get("ends_at") or a.get("expires_at") or a.get("end_at"),
            "next_step": _loot_next_step(a, need_index.get(_loot_key(item))),
            "raw": a,
        })
    auction_matches.sort(key=lambda r: (not bool(r.get("active")), -int(r.get("score") or 0), _dt(r.get("ends_at")), str(r.get("item") or "").lower()))

    best = need_matches[0] if need_matches else None
    active_auction_count = sum(1 for a in auction_matches if a.get("active"))
    main_total = sum(int(m.get("main_count") or 0) for m in need_matches[:5]) if query else 0
    sec_total = sum(int(m.get("secondary_count") or 0) for m in need_matches[:5]) if query else 0

    if not query:
        verdict = "Item eingeben"
        next_step = "Gib oben ein gedropptes Item ein, z. B. Aridus Stab."
    elif active_auction_count > 0:
        verdict = "Auktion läuft bereits"
        next_step = "Erst vorhandene Auktion prüfen, bevor du eine neue Aktion startest."
    elif best and int(best.get("score") or 0) >= 70 and int(best.get("main_count") or 0) > 0:
        verdict = "Main-Need vorhanden"
        next_step = "Nicht frei verkaufen. Main-Need-Spieler/Auktion prüfen."
    elif best and int(best.get("score") or 0) >= 70 and int(best.get("secondary_count") or 0) > 0:
        verdict = "Nur Second-Need sichtbar"
        next_step = "Second-Need oder freie Auktion nach euren Regeln prüfen."
    elif need_matches:
        verdict = "Ähnliche Treffer prüfen"
        next_step = "Name weicht ab. Bitte Treffer manuell vergleichen, bevor du Sale/freie Auktion machst."
    else:
        verdict = "Kein Need gefunden"
        next_step = "Nach Snapshot aktuell kein Main-/Second-Need sichtbar. Freie Auktion/Sale möglich, wenn eure Regeln passen."

    return {
        "query": query,
        "verdict": verdict,
        "next_step": next_step,
        "main_total_top_matches": main_total,
        "secondary_total_top_matches": sec_total,
        "active_auction_count": active_auction_count,
        "need_matches": need_matches,
        "auction_matches": auction_matches,
    }


def _render_loot_check(data: dict[str, Any], item_query: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Truhencheck · Ebo Dashboard", f"<section class='panel'><h1>🔎 Truhencheck</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    check = _loot_check_payload_from_snapshot(snap, item_query)
    names = _profile_name_map(snap)

    q = check.get("query") or ""
    cards = "".join([
        _card("Ergebnis", check.get("verdict"), check.get("next_step")),
        _card("Main", check.get("main_total_top_matches", 0), "in besten Treffern"),
        _card("Second", check.get("secondary_total_top_matches", 0), "in besten Treffern"),
        _card("Aktive Auktion", check.get("active_auction_count", 0), "passend zum Item"),
    ])

    need_rows = []
    for n in check.get("need_matches") or []:
        need_rows.append([
            n.get("item"),
            n.get("score") if q else "—",
            n.get("main_count"),
            _loot_people_html(n.get("main") or [], limit=12),
            n.get("secondary_count"),
            _loot_people_html(n.get("secondary") or [], limit=12),
        ])

    auction_rows = []
    for a in check.get("auction_matches") or []:
        aid = a.get("auction_id")
        auction_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("score") if q else "—",
            a.get("mode"),
            _loot_status_label(a.get("status")),
            a.get("bid_count"),
            a.get("leader"),
            a.get("winner") if isinstance(a.get("winner"), dict) else a.get("winner"),
            _dt(a.get("ends_at")),
            a.get("next_step"),
        ])

    form_value = _e(q)
    csv_link = f"/export/loot_check.csv?item={urllib.parse.quote(q)}" if q else "/export/loot_check.csv"
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/loot">Loot</a><a href="/needs">Needs</a><a href="/members">Mitglieder</a><a href="/api/loot-check?item={urllib.parse.quote(q)}">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot-Truhencheck</div><h1>🔎 Item prüfen</h1><p class="muted">Prüft ein gedropptes Item gegen Main-/Second-Needs und laufende Auktionen. Read-only, keine Bot-Daten werden verändert.</p></div><a class="btn" href="{_e(csv_link)}">CSV herunterladen</a></section>
    <section class="panel">
      <h2>Item suchen</h2>
      <form method="get" action="/loot-check" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
        <input name="item" value="{form_value}" placeholder="z. B. Aridus Stab" style="min-width:280px;flex:1;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);color:inherit">
        <button class="btn" type="submit">Prüfen</button>
      </form>
      <p class="muted">Tipp: Teilnamen gehen auch. Bei abweichenden Ingame-Namen zeigt der Check ähnliche Treffer zur manuellen Prüfung.</p>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🎯 Need-Treffer</h2>{_table(['Item','Match','Main','Main-Spieler','Second','Second-Spieler'], need_rows, placeholder='Need-Treffer durchsuchen…')}</section>
    <section class="panel"><h2>🏷️ Passende Auktionen</h2>{_table(['Item','Match','Bereich','Status','Gebote','Führend','Gewinner','Ende','Nächster Schritt'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    """
    return _html_shell("Truhencheck · Ebo Dashboard", body)

def _render_loot_center(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Loot · Ebo Dashboard", f"<section class='panel'><h1>🎁 Loot</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    center = _loot_center_payload_from_snapshot(snap)

    cards = "".join([
        _card("Aktive Auktionen", len(center["active"]), "offen/läuft"),
        _card("Übergabe offen", len(center["handover"]), "Gewinner ohne Übergabe"),
        _card("Ohne Gebot", len(center["no_bid"]), "aktive Auktionen"),
        _card("Need-Items", len(center["needs"]), "aus Needlisten"),
    ])

    action_rows = []
    for a in center["next_actions"][:80]:
        aid = a.get("auction_id")
        action_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("mode"),
            _loot_status_label(a.get("status")),
            a.get("bid_count"),
            a.get("leader"),
            _loot_winner_cell(a.get("raw") or {}, names),
            _dt(a.get("ends_at")),
            a.get("next_step"),
        ])

    active_rows = []
    for a in center["active"]:
        aid = a.get("auction_id")
        active_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("mode"),
            _loot_status_label(a.get("status")),
            a.get("bid_count"),
            a.get("leader"),
            f"{a.get('main_need_count', 0)} / {a.get('secondary_need_count', 0)}",
            _dt(a.get("ends_at")),
        ])

    handover_rows = []
    for a in center["handover"]:
        aid = a.get("auction_id")
        handover_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            _loot_winner_cell(a.get("raw") or {}, names),
            a.get("leader"),
            _dt(a.get("ends_at")),
            a.get("next_step"),
        ])

    no_bid_rows = []
    for a in center["no_bid"][:80]:
        aid = a.get("auction_id")
        no_bid_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("mode"),
            f"{a.get('main_need_count', 0)} / {a.get('secondary_need_count', 0)}",
            _dt(a.get("ends_at")),
            a.get("next_step"),
        ])

    need_rows = []
    for n in center["needs"][:150]:
        need_rows.append([
            n.get("item"),
            n.get("main_count"),
            _loot_people_html(n.get("main") or []),
            n.get("secondary_count"),
            _loot_people_html(n.get("secondary") or []),
            n.get("active_auction_count"),
        ])

    history_rows = []
    for a in center["history"][:250]:
        aid = a.get("auction_id")
        history_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("mode"),
            _loot_status_label(a.get("status")),
            a.get("bid_count"),
            a.get("leader"),
            _loot_winner_cell(a.get("raw") or {}, names),
            _dt(a.get("ends_at")),
        ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/loot">Loot</a><a href="/loot-check">Truhencheck</a><a href="/needs">Needs</a><a href="/members">Mitglieder</a><a href="/fairness">Fairness</a><a href="/exports">Exports</a><a href="/api/loot-center">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot-Zentrale</div><h1>🎁 Loot / Auktionen</h1><p class="muted">Kontrollzentrum für aktive Auktionen, offene Übergaben, Gebote und Need-Spieler. Read-only, keine Bot-Daten werden verändert.</p></div><a class="btn" href="/export/loot_center.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🔎 Schneller Truhencheck</h2><form method="get" action="/loot-check" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><input name="item" placeholder="Itemname eingeben, z. B. Aridus Stab" style="min-width:280px;flex:1;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);color:inherit"><button class="btn" type="submit">Need prüfen</button></form><p class="muted">Prüft Main-/Second-Needs und ob dazu schon eine Auktion läuft.</p></section>
    <section class="panel"><h2>🔥 Nächste Loot-Aktionen</h2><p class="muted">Das ist die Arbeitsliste: Übergaben, offene Auktionen, Sale/Freie-Auktion-Kandidaten und aktive Auktionen ohne Gebote.</p>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende','Nächster Schritt'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>🟢 Aktive Auktionen</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Main/Second Need','Ende'], active_rows, placeholder='Aktive Auktionen durchsuchen…')}</section>
    <section class="split"><div class="panel"><h2>🏁 Übergabe offen</h2>{_table(['Item','Gewinner','Führend','Ende','Nächster Schritt'], handover_rows, placeholder='Übergaben durchsuchen…')}</div><div class="panel"><h2>🟡 Ohne Gebot / Sale prüfen</h2>{_table(['Item','Bereich','Main/Second Need','Ende','Hinweis'], no_bid_rows, placeholder='Ohne Gebot durchsuchen…')}</div></section>
    <section class="panel"><h2>🎯 Need-Spieler pro Item</h2><p class="muted">Damit sieht man direkt, ob ein Drop Main-/Second-Need-Spieler hat und ob schon eine Auktion dazu läuft.</p>{_table(['Item','Main','Main-Spieler','Second','Second-Spieler','aktive Auktionen'], need_rows, placeholder='Need-Item suchen…')}</section>
    <section class="panel"><h2>📜 Auktionshistorie</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende'], history_rows, placeholder='Historie durchsuchen…')}</section>
    """
    return _html_shell("Loot · Ebo Dashboard", body)


def _render_exports_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Exports · Ebo Dashboard", f"<section class='panel'><h1>⬇️ Exports</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    cards = "".join([
        _card("Snapshot", data.get("id"), _dt(data.get("published_at"))),
        _card("Mitglieder", len(_insight_members(snap)), "CSV"),
        _card("EC-Konten", len((((snap.get("ec") or {}).get("balances") or {}).get("top") or [])), "CSV"),
        _card("Auktionen", len((((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])), "CSV"),
    ])
    rows = [
        ["Mitglieder", _raw('<a class="link" href="/export/members.csv">members.csv</a>'), "Roster, Qualität, EC, Voice, Loot"],
        ["EC", _raw('<a class="link" href="/export/ec.csv">ec.csv</a>'), "EC-Konten"],
        ["Needs", _raw('<a class="link" href="/export/needs.csv">needs.csv</a>'), "Main/Secondary je Spieler"],
        ["Auktionen", _raw('<a class="link" href="/export/auctions.csv">auctions.csv</a>'), "Auktionsübersicht"],
        ["Fairness", _raw('<a class="link" href="/export/fairness.csv">fairness.csv</a>'), "Loot/EC/Need-Hinweise"],
        ["JSON Snapshot", _raw('<a class="link" href="/api/snapshot">api/snapshot</a>'), "voller Snapshot"],
    ]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/members">Mitglieder</a><a href="/needs">Needs</a><a href="/loot">Loot</a><a href="/system">System</a></nav>
    <section class="hero"><div><div class="eyebrow">Read-only Download</div><h1>⬇️ Exports</h1><p class="muted">CSV/JSON für Kontrolle, Excel oder spätere Migration. Es wird nichts verändert.</p></div><a class="btn" href="/api/snapshot">JSON ansehen</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>Downloads</h2>{_table(['Bereich','Datei','Inhalt'], rows, searchable=False)}</section>
    """
    return _html_shell("Exports · Ebo Dashboard", body)


def _csv_response(filename: str, headers: list[str], rows: list[list[Any]]) -> Response:
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=';')
    writer.writerow(headers)
    for row in rows:
        writer.writerow([str(x.get('__html__') if isinstance(x, dict) and '__html__' in x else x if x is not None else '') for x in row])
    return Response(content=buf.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": f"attachment; filename={filename}"})




# ---------------------------------------------------------------------------
# Step 3.10: Planung, Fairness, Compare
# ---------------------------------------------------------------------------

def _role_bucket(label: Any) -> str:
    txt = str(label or "").strip().lower()
    if any(x in txt for x in ("tank", "wächter", "waechter")):
        return "Tank"
    if any(x in txt for x in ("heal", "heiler", "support")):
        return "Heiler"
    if any(x in txt for x in ("dps", "dd", "damage", "schaden")):
        return "DPS"
    if any(x in txt for x in ("reserve", "bank")):
        return "Reserve"
    return str(label or "Andere") or "Andere"


def _event_role_summary(ev: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {"Tank": 0, "Heiler": 0, "DPS": 0, "Reserve": 0, "Andere": 0}
    for role, count in (ev.get("yes_counts") or {}).items():
        bucket = _role_bucket(role)
        if bucket not in out:
            out[bucket] = 0
        out[bucket] += int(_num(count, 0))
    return out


def _event_readiness_score(role_counts: dict[str, int], participant_count: int) -> tuple[int, list[str]]:
    """Kleine Faustregel, bewusst nicht spielmechanisch hart.

    Ziel ist kein automatisches Urteil, sondern ein schneller Leitungs-Hinweis.
    """
    issues: list[str] = []
    score = 100
    if role_counts.get("Tank", 0) <= 0:
        score -= 30
        issues.append("kein Tank")
    if role_counts.get("Heiler", 0) <= 0:
        score -= 30
        issues.append("kein Heiler")
    if role_counts.get("DPS", 0) <= 0:
        score -= 20
        issues.append("kein DPS")
    if participant_count < 6:
        score -= 15
        issues.append("wenig Teilnehmer")
    if participant_count < 3:
        score -= 20
        issues.append("kritisch wenige Teilnehmer")
    return max(0, min(100, score)), issues


def _planning_analytics(snap: dict[str, Any]) -> dict[str, Any]:
    events = ((snap.get("events") or {}).get("items") or [])
    insights = _insights(snap)
    needs = (insights.get("needs") or {}) if isinstance(insights, dict) else {}
    top_main = needs.get("top_main") if isinstance(needs.get("top_main"), list) else []
    top_secondary = needs.get("top_secondary") if isinstance(needs.get("top_secondary"), list) else []

    event_rows: list[dict[str, Any]] = []
    role_totals: Counter[str] = Counter()
    risk_count = 0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        counts = _event_role_summary(ev)
        participant_count = int(_num(ev.get("participant_count"), 0))
        score, issues = _event_readiness_score(counts, participant_count)
        if score < 70:
            risk_count += 1
        for k, v in counts.items():
            role_totals[k] += int(v)
        event_rows.append({
            "event_id": ev.get("event_id"),
            "title": ev.get("title") or ev.get("event_id") or "Event",
            "when_iso": ev.get("when_iso"),
            "participants": participant_count,
            "maybe": int(_num(ev.get("maybe_count"), 0)),
            "no": int(_num(ev.get("no_count"), 0)),
            "tank": counts.get("Tank", 0),
            "healer": counts.get("Heiler", 0),
            "dps": counts.get("DPS", 0),
            "reserve": counts.get("Reserve", 0),
            "readiness": score,
            "issues": issues,
            "voice": bool(ev.get("voice_enabled") or ev.get("voice_channel_id") or ev.get("voice_last_channel_id")),
        })
    event_rows.sort(key=lambda x: str(x.get("when_iso") or ""), reverse=True)

    return {
        "events_total": len(event_rows),
        "events_at_risk": risk_count,
        "avg_readiness": (sum(int(x.get("readiness") or 0) for x in event_rows) / len(event_rows)) if event_rows else 0,
        "role_totals": role_totals.most_common(),
        "events": event_rows,
        "top_main_needs": top_main[:40],
        "top_secondary_needs": top_secondary[:40],
    }


def _fairness_analytics(snap: dict[str, Any]) -> dict[str, Any]:
    members = _insight_members(snap)
    rows: list[dict[str, Any]] = []
    for m in members:
        if not isinstance(m, dict):
            continue
        main_needs = int(_num(m.get("main_need_count"), 0))
        sec_needs = int(_num(m.get("secondary_need_count"), 0))
        loot_won = int(_num(m.get("loot_won_count"), 0))
        ec_balance = _num(m.get("ec_balance"), 0)
        earned = _num(m.get("ec_earned_loaded"), 0)
        spent = _num(m.get("ec_spent_loaded"), 0)
        voice_hours = _num(m.get("voice_hours"), 0)
        yes = int(_num(m.get("event_yes"), 0))
        need_total = main_needs + sec_needs
        flags: list[str] = []
        if need_total >= 3 and loot_won <= 0:
            flags.append("viel Bedarf, kein Loot")
        if ec_balance >= 100 and loot_won <= 0:
            flags.append("viel EC, kein Loot")
        if loot_won >= 3 and spent <= 0:
            flags.append("viel Loot, kaum Ausgaben")
        if yes <= 0 and loot_won > 0:
            flags.append("Loot ohne Eventzusagen im Snapshot")
        if voice_hours <= 0 and yes > 0:
            flags.append("Zusagen ohne Voice-Zeit")
        pressure = need_total * 2 + max(0, 2 - loot_won) + (1 if ec_balance > 0 else 0)
        rows.append({
            "user_id": m.get("user_id"),
            "display_name": m.get("display_name"),
            "role": m.get("main_role"),
            "ec_balance": ec_balance,
            "ec_earned": earned,
            "ec_spent": spent,
            "loot_won": loot_won,
            "main_needs": main_needs,
            "secondary_needs": sec_needs,
            "need_total": need_total,
            "event_yes": yes,
            "voice_hours": voice_hours,
            "pressure": pressure,
            "flags": flags,
            "flag_count": len(flags),
        })
    rows.sort(key=lambda x: (-int(x.get("flag_count") or 0), -float(x.get("pressure") or 0), str(x.get("display_name") or "").lower()))
    top_pressure = sorted(rows, key=lambda x: (-float(x.get("pressure") or 0), str(x.get("display_name") or "").lower()))[:60]
    loot_winners = sorted(rows, key=lambda x: (-int(x.get("loot_won") or 0), str(x.get("display_name") or "").lower()))[:60]
    high_ec = sorted(rows, key=lambda x: (-float(x.get("ec_balance") or 0), str(x.get("display_name") or "").lower()))[:60]
    return {
        "member_count": len(rows),
        "flagged_count": sum(1 for r in rows if int(r.get("flag_count") or 0) > 0),
        "total_loot_won": sum(int(r.get("loot_won") or 0) for r in rows),
        "total_need_count": sum(int(r.get("need_total") or 0) for r in rows),
        "rows": rows[:800],
        "flagged": [r for r in rows if int(r.get("flag_count") or 0) > 0][:200],
        "top_pressure": top_pressure,
        "loot_winners": loot_winners,
        "high_ec": high_ec,
    }


def _render_planning_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Planung · Ebo Dashboard", f"<section class='panel'><h1>📅 Planung</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    plan = _planning_analytics(snap)
    cards = "".join([
        _card("Events", plan.get("events_total", 0), "im Snapshot"),
        _card("Event-Hinweise", plan.get("events_at_risk", 0), "wenig/fehlende Rollen"),
        _card("Ø Bereitschaft", f"{round(_num(plan.get('avg_readiness'), 0))}%", "Faustregel"),
        _card("Snapshot", _dt(data.get("published_at")), "read-only"),
    ])
    event_rows = []
    for ev in plan.get("events") or []:
        if not isinstance(ev, dict):
            continue
        issues = ", ".join(str(x) for x in (ev.get("issues") or [])) or "—"
        event_rows.append([
            _event_link(ev.get("event_id"), ev.get("title")),
            _dt(ev.get("when_iso")),
            ev.get("participants"),
            ev.get("tank"),
            ev.get("healer"),
            ev.get("dps"),
            ev.get("reserve"),
            f"{int(_num(ev.get('readiness'), 0))}%",
            issues,
            "ja" if ev.get("voice") else "nein",
        ])
    main_rows = [[x.get("label"), x.get("count")] for x in plan.get("top_main_needs") or [] if isinstance(x, dict)]
    sec_rows = [[x.get("label"), x.get("count")] for x in plan.get("top_secondary_needs") or [] if isinstance(x, dict)]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/fairness">Fairness</a><a href="/needs">Needs</a><a href="/api/planning">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Planung</div><h1>📅 Event-/Raid-Planung</h1><p class="muted">Schnellprüfung für Rolle, Teilnehmer, Voice und häufige Needs. Read-only.</p></div><a class="btn" href="/api/planning">API</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>Rollen-Summen über Events</h2>{_bars(plan.get('role_totals') or [])}</section>
    <section class="panel"><h2>Event-Bereitschaft</h2>{_table(['Event','Zeit','Teilnehmer','Tank','Heiler','DPS','Reserve','Score','Hinweise','Voice'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel"><h2>Bedarfs-Hotspots</h2><div class="split"><div><h3>Main-Needs</h3>{_table(['Item','Anzahl'], main_rows, placeholder='Main-Needs durchsuchen…')}</div><div><h3>Secondary-Needs</h3>{_table(['Item','Anzahl'], sec_rows, placeholder='Secondary-Needs durchsuchen…')}</div></div></section>
    """
    return _html_shell("Planung · Ebo Dashboard", body)


def _render_fairness_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Fairness · Ebo Dashboard", f"<section class='panel'><h1>⚖️ Fairness</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    fair = _fairness_analytics(snap)
    cards = "".join([
        _card("Mitglieder", fair.get("member_count", 0), "aus Gildenrolle"),
        _card("Hinweise", fair.get("flagged_count", 0), "prüfen, nicht automatisch urteilen"),
        _card("Loot erhalten", fair.get("total_loot_won", 0), "geladene Historie"),
        _card("Offene Needs", fair.get("total_need_count", 0), "Main + Secondary"),
    ])
    flagged_rows = []
    for r in fair.get("flagged") or []:
        if not isinstance(r, dict):
            continue
        flagged_rows.append([
            _member_link(r.get("user_id"), r.get("display_name")),
            r.get("role") or "—",
            _fmt_ec(r.get("ec_balance")),
            r.get("loot_won"),
            r.get("main_needs"),
            r.get("secondary_needs"),
            round(_num(r.get("voice_hours"), 0), 1),
            ", ".join(str(x) for x in (r.get("flags") or [])),
        ])
    pressure_rows = [[_member_link(r.get("user_id"), r.get("display_name")), r.get("pressure"), r.get("main_needs"), r.get("secondary_needs"), r.get("loot_won"), _fmt_ec(r.get("ec_balance"))] for r in fair.get("top_pressure") or [] if isinstance(r, dict)]
    loot_rows = [[_member_link(r.get("user_id"), r.get("display_name")), r.get("loot_won"), _fmt_ec(r.get("ec_spent")), _fmt_ec(r.get("ec_balance")), r.get("event_yes"), round(_num(r.get("voice_hours"), 0), 1)] for r in fair.get("loot_winners") or [] if isinstance(r, dict)]
    ec_rows = [[_member_link(r.get("user_id"), r.get("display_name")), _fmt_ec(r.get("ec_balance")), _fmt_ec(r.get("ec_earned")), _fmt_ec(r.get("ec_spent")), r.get("loot_won"), r.get("need_total")] for r in fair.get("high_ec") or [] if isinstance(r, dict)]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/planning">Planung</a><a href="/loot">Loot</a><a href="/ec">EC-Verlauf</a><a href="/api/fairness">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot/EC</div><h1>⚖️ Fairness-Check</h1><p class="muted">Hinweise für Leitung. Kein automatisches Urteil und keine Datenänderung.</p></div><a class="btn" href="/api/fairness">API</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>Prüf-Hinweise</h2>{_table(['Spieler','Rolle','EC','Loot','Main','Secondary','Voice h','Hinweise'], flagged_rows, placeholder='Hinweise durchsuchen…')}</section>
    <section class="panel"><h2>Bedarfsdruck</h2>{_table(['Spieler','Score','Main','Secondary','Loot','EC'], pressure_rows, placeholder='Bedarfsdruck durchsuchen…')}</section>
    <section class="panel"><h2>Loot-Gewinner</h2>{_table(['Spieler','Loot','EC ausgegeben','EC aktuell','Zusagen','Voice h'], loot_rows, placeholder='Loot-Gewinner durchsuchen…')}</section>
    <section class="panel"><h2>Hohe EC-Konten</h2>{_table(['Spieler','EC','Verdient','Ausgegeben','Loot','Needs'], ec_rows, placeholder='EC durchsuchen…')}</section>
    """
    return _html_shell("Fairness · Ebo Dashboard", body)




# ---------------------------------------------------------------------------
# Step 3.12: Tiefere Auswertung für Mitglieder, Events, Voice und Audit
# ---------------------------------------------------------------------------

def _all_member_ids_from_snapshot(snap: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    for p in ((snap.get("profiles") or {}).get("items") or []):
        if isinstance(p, dict):
            uid = _user_id(p.get("user_id"))
            if uid:
                ids.add(uid)
    for b in (((snap.get("ec") or {}).get("balances") or {}).get("top") or []):
        if isinstance(b, dict):
            uid = _user_id(b.get("user_id") or b.get("member_id") or b.get("discord_id"))
            if uid:
                ids.add(uid)
    for n in (((snap.get("loot") or {}).get("needs") or {}).get("items") or []):
        if isinstance(n, dict):
            uid = _user_id(n.get("user_id") or n.get("member_id") or n.get("discord_id"))
            if uid:
                ids.add(uid)
    for v in ((snap.get("voice") or {}).get("by_user") or []):
        if isinstance(v, dict):
            uid = _user_id(v.get("user_id"))
            if uid:
                ids.add(uid)
    return ids


def _event_registered_ids(event: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    participants = event.get("participants") or {}
    for group in participants.get("yes") or []:
        if not isinstance(group, dict):
            continue
        for p in group.get("participants") or []:
            if isinstance(p, dict):
                uid = _user_id(p.get("user_id"))
                if uid:
                    ids.add(uid)
    for key in ("maybe", "no"):
        for p in participants.get(key) or []:
            if isinstance(p, dict):
                uid = _user_id(p.get("user_id"))
                if uid:
                    ids.add(uid)
    return ids


def _event_yes_ids(event: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    participants = event.get("participants") or {}
    for group in participants.get("yes") or []:
        if not isinstance(group, dict):
            continue
        for p in group.get("participants") or []:
            if isinstance(p, dict):
                uid = _user_id(p.get("user_id"))
                if uid:
                    ids.add(uid)
    return ids


def _event_response_counts_for_user(snap: dict[str, Any], user_id: int) -> dict[str, int]:
    counts = {"yes": 0, "maybe": 0, "no": 0, "total": 0, "events": 0, "no_response": 0}
    events = [e for e in ((snap.get("events") or {}).get("items") or []) if isinstance(e, dict)]
    counts["events"] = len(events)
    uid = int(user_id)
    for ev in events:
        seen = False
        participants = ev.get("participants") or {}
        for group in participants.get("yes") or []:
            if not isinstance(group, dict):
                continue
            for p in group.get("participants") or []:
                if isinstance(p, dict) and _user_id(p.get("user_id")) == uid:
                    counts["yes"] += 1
                    seen = True
                    break
            if seen:
                break
        if not seen:
            for p in participants.get("maybe") or []:
                if isinstance(p, dict) and _user_id(p.get("user_id")) == uid:
                    counts["maybe"] += 1
                    seen = True
                    break
        if not seen:
            for p in participants.get("no") or []:
                if isinstance(p, dict) and _user_id(p.get("user_id")) == uid:
                    counts["no"] += 1
                    seen = True
                    break
        if seen:
            counts["total"] += 1
        else:
            counts["no_response"] += 1
    return counts


def _voice_sessions_for_event(snap: dict[str, Any], event: dict[str, Any]) -> list[dict[str, Any]]:
    sessions = [v for v in ((snap.get("voice") or {}).get("recent_sessions") or []) if isinstance(v, dict)]
    if not sessions:
        return []
    channel_ids = set()
    for key in ("voice_channel_id", "voice_last_channel_id"):
        cid = _user_id(event.get(key))
        if cid:
            channel_ids.add(cid)
    if not channel_ids:
        return []

    ev_start = _dt_obj(event.get("when_iso"))
    out: list[dict[str, Any]] = []
    for sess in sessions:
        cid = _user_id(sess.get("channel_id") or sess.get("voice_channel_id"))
        if cid not in channel_ids:
            continue
        # Wenn ein Event-Zeitpunkt vorhanden ist, nicht irgendwelche Sessions von Monaten davor anzeigen.
        if ev_start:
            joined = _dt_obj(sess.get("joined_at"))
            left = _dt_obj(sess.get("left_at")) or joined
            if joined:
                delta_start = abs((joined - ev_start).total_seconds())
                delta_end = abs(((left or joined) - ev_start).total_seconds()) if left else delta_start
                # 8h Fenster um Eventstart reicht für normale Raids/Events und verhindert alte Treffer.
                if min(delta_start, delta_end) > 8 * 3600:
                    continue
        out.append(sess)
    out.sort(key=lambda x: int(_num(x.get("duration_seconds"), 0)), reverse=True)
    return out


def _voice_event_analysis(snap: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    names = _profile_name_map(snap)
    sessions = _voice_sessions_for_event(snap, event)
    voice_by_user: dict[int, int] = {}
    session_count: dict[int, int] = {}
    for sess in sessions:
        uid = _user_id(sess.get("user_id") or sess.get("member_id"))
        if not uid:
            continue
        voice_by_user[uid] = voice_by_user.get(uid, 0) + int(_num(sess.get("duration_seconds"), 0))
        session_count[uid] = session_count.get(uid, 0) + 1

    registered = _event_registered_ids(event)
    yes = _event_yes_ids(event)
    voice_ids = set(voice_by_user.keys())
    registered_not_voice = sorted(registered - voice_ids, key=lambda uid: names.get(uid, f"User {uid}").lower())
    yes_not_voice = sorted(yes - voice_ids, key=lambda uid: names.get(uid, f"User {uid}").lower())
    voice_not_registered = sorted(voice_ids - registered, key=lambda uid: names.get(uid, f"User {uid}").lower())
    rows = []
    for uid, seconds in sorted(voice_by_user.items(), key=lambda x: -x[1]):
        rows.append({
            "user_id": uid,
            "display_name": names.get(uid, f"User {uid}"),
            "minutes": round(seconds / 60, 1),
            "sessions": session_count.get(uid, 0),
            "registered": uid in registered,
            "signed_yes": uid in yes,
        })
    return {
        "voice_sessions": sessions,
        "voice_by_user": rows,
        "registered_not_voice": registered_not_voice,
        "yes_not_voice": yes_not_voice,
        "voice_not_registered": voice_not_registered,
        "voice_user_count": len(voice_ids),
        "registered_count": len(registered),
        "yes_count": len(yes),
    }


def _name_rows_from_ids(ids: list[int] | set[int], names: dict[int, str]) -> list[list[Any]]:
    return [[_member_link(uid, names.get(uid, f"User {uid}"))] for uid in ids]


def _member_event_rows(snap: dict[str, Any], user_id: int) -> list[list[Any]]:
    rows: list[list[Any]] = []
    uid = int(user_id)
    for ev in ((snap.get("events") or {}).get("items") or []):
        if not isinstance(ev, dict):
            continue
        status = "—"
        participants = ev.get("participants") or {}
        for group in participants.get("yes") or []:
            if not isinstance(group, dict):
                continue
            role = group.get("role") or "Zusage"
            if any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in group.get("participants") or []):
                status = f"✅ {role}"
                break
        if status == "—" and any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in participants.get("maybe") or []):
            status = "🟡 Vielleicht"
        if status == "—" and any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in participants.get("no") or []):
            status = "❌ Abgemeldet"
        if status != "—":
            rows.append([_event_link(ev.get("event_id"), ev.get("title")), _dt(ev.get("when_iso")), status])
    return rows[:80]


def _render_member_detail(data: dict[str, Any], user_id: int, current_user: Optional[dict[str, Any]] = None) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    balances = _balance_map(snap)
    needs_by_user = _needs_by_user(snap)
    names = _profile_name_map(snap)
    profile = None
    for p in profiles:
        if isinstance(p, dict) and _user_id(p.get("user_id")) == int(user_id):
            profile = p
            break
    if not profile:
        return _html_shell(
            "Mitglied nicht gefunden",
            "<section class='panel'><h1>❌ Mitglied nicht gefunden</h1><p class='muted'>Dieses Mitglied ist nicht im aktuellen Dashboard-Snapshot oder hat nicht die gesetzte Gildenrolle.</p><p><a class='btn' href='/members'>Zurück</a></p></section>",
        )

    display = profile.get("display_name") or profile.get("ingame_name") or f"User {user_id}"
    ec_value = balances.get(int(user_id))
    need_info = needs_by_user.get(int(user_id), {})
    main_needs = need_info.get("main") if isinstance(need_info, dict) else []
    secondary_needs = need_info.get("secondary") if isinstance(need_info, dict) else []
    response = _event_response_counts_for_user(snap, int(user_id))
    voice_user = next((v for v in ((snap.get("voice") or {}).get("by_user") or []) if isinstance(v, dict) and _user_id(v.get("user_id")) == int(user_id)), {})
    total_voice_seconds = int(_num(voice_user.get("total_seconds"), 0))

    tx_rows = []
    for tx in _tx_for_user(snap, user_id, limit=80):
        tx_rows.append([_dt(tx.get("created_at")), _fmt_ec(tx.get("amount")), tx.get("raw_type"), _short(tx.get("reason"), 160)])

    voice_rows = []
    for v in _voice_for_user(snap, user_id, limit=80):
        seconds = int(_num(v.get("duration_seconds"), 0))
        minutes = round(seconds / 60, 1) if seconds else "—"
        voice_rows.append([v.get("channel_name") or v.get("channel_id"), _dt(v.get("joined_at")), _dt(v.get("left_at")), minutes])

    auction_rows = []
    for a in _auctions_for_user(snap, user_id, limit=80):
        auction_rows.append([_auction_link(a.get("auction_id"), a.get("item_name")), a.get("status"), _phase_label(a), _fmt_ec(a.get("top_bid_amount")) if a.get("top_bid_amount") is not None else "—", _dt(a.get("ends_at"))])

    event_rows = _member_event_rows(snap, int(user_id))
    cards = "".join([
        _card("Ingame", profile.get("ingame_name") or "—", "Profil"),
        _card("Rolle", profile.get("main_role") or "—", "Main-Rolle"),
        _card("Gearscore", profile.get("gearscore") or "—", "Profilwert"),
        _card("EC", _fmt_ec(ec_value) if ec_value is not None else "—", "aktueller Kontostand"),
        _card("Eventantworten", f"{response['total']}/{response['events']}", f"Ja {response['yes']} · Vielleicht {response['maybe']} · Nein {response['no']}"),
        _card("Voice-Zeit", f"{round(total_voice_seconds/3600, 1)} h", f"{int(voice_user.get('sessions') or 0)} Sessions"),
    ])

    body = f"""
    <nav class="topnav"><a href="/members">← Mitglieder</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="#needs">Needs</a><a href="#events">Events</a><a href="#ec">EC</a><a href="#voice">Voice</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Mitglied · Tiefenauswertung</div>
        <h1>👤 {_e(display)}</h1>
        <p class="muted">User-ID: {_e(user_id)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/members">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    {_admin_member_panel(data, int(user_id), current_user)}
    <section class="panel" id="needs">
      <h2>🎁 Needliste</h2>
      <div class="split">
        <div>{_need_list_html('Main-Needs', main_needs)}</div>
        <div>{_need_list_html('Secondary-Needs', secondary_needs)}</div>
      </div>
    </section>
    <section class="panel" id="events"><h2>📅 Eventantworten</h2>{_table(['Event','Zeit','Status'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel" id="ec"><h2>🪙 Letzte EC-Buchungen</h2>{_table(['Zeit','Betrag','Typ','Grund'], tx_rows, placeholder='Buchungen durchsuchen…')}</section>
    <section class="panel"><h2>🎁 Auktionen mit aktueller Führung/Gewinn</h2>{_table(['Item','Status','Phase','Gebot','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel" id="voice"><h2>🎙️ Voice-Sessions</h2>{_table(['Kanal','Rein','Raus','Minuten'], voice_rows, placeholder='Voice durchsuchen…')}</section>
    """
    return _html_shell(f"{display} · Ebo Dashboard", body)


def _render_event_detail(data: dict[str, Any], event_id: str) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    event = _event_by_id(snap, event_id) or _event_stub_from_attendance_review(guild_id, event_id)
    if not event:
        return _html_shell(
            "Event nicht gefunden",
            "<section class='panel'><h1>❌ Event nicht gefunden</h1><p class='muted'>Dieses Event ist nicht im aktuellen Dashboard-Snapshot und hat keinen Review-Fallback.</p><p><a class='btn' href='/attendance'>Zur Anwesenheit</a></p></section>",
        )

    participants = event.get("participants") or {}
    maybe_rows = _participant_rows(participants.get("maybe") or [])
    no_rows = _participant_rows(participants.get("no") or [])
    yes_counts = event.get("yes_counts") or {}
    role_items = sorted([(str(k), int(_num(v))) for k, v in yes_counts.items()], key=lambda x: x[0].lower())
    names = _profile_name_map(snap)
    voice = _voice_event_analysis(snap, event)
    voice_rows = [[_member_link(r.get("user_id"), r.get("display_name")), r.get("minutes"), r.get("sessions"), "ja" if r.get("registered") else "nein", "ja" if r.get("signed_yes") else "nein"] for r in voice.get("voice_by_user") or []]
    missing_voice_rows = _name_rows_from_ids(voice.get("yes_not_voice") or [], names)
    extra_voice_rows = _name_rows_from_ids(voice.get("voice_not_registered") or [], names)

    cards = "".join([
        _card("Teilnehmer", event.get("participant_count", 0), "alle Rückmeldungen"),
        _card("Zusagen", voice.get("yes_count", 0), "für Voice-Abgleich"),
        _card("im Voice erkannt", voice.get("voice_user_count", 0), "gleicher Event-Voice"),
        _card("angemeldet ohne Voice", len(voice.get("yes_not_voice") or []), "zu prüfen"),
        _card("Voice ohne Anmeldung", len(voice.get("voice_not_registered") or []), "zu prüfen"),
        _card("Voice", "ja" if event.get("voice_enabled") else "nein", event.get("voice_channel_id") or event.get("voice_last_channel_id") or "kein Voice"),
    ])

    body = f"""
    <nav class="topnav"><a href="/planning">← Planung</a><a href="#signups">Zusagen</a><a href="#voicecheck">Voice-Abgleich</a><a href="#maybe">Vielleicht</a><a href="#no">Abgemeldet</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Event · Voice-/Teilnahme-Abgleich</div>
        <h1>📅 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Event-ID: {_e(event_id)} · Zeit: {_e(_dt(event.get('when_iso')))} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
        {f"<p>{_e(event.get('description'))}</p>" if event.get('description') else ""}
      </div>
      <a class="btn" href="/planning">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>📊 Rollenverteilung</h2>{_bars(role_items, max_items=12)}</section>
    <section class="panel" id="voicecheck">
      <h2>🎙️ Voice-Abgleich</h2>
      <div class="split">
        <div><h3>Angemeldet/Zusage, aber nicht im Voice erkannt</h3>{_table(['Spieler'], missing_voice_rows, placeholder='fehlende Voice durchsuchen…')}</div>
        <div><h3>Im Voice, aber nicht angemeldet</h3>{_table(['Spieler'], extra_voice_rows, placeholder='Extra Voice durchsuchen…')}</div>
      </div>
      <h3>Erkannte Voice-Zeiten</h3>
      {_table(['Spieler','Minuten','Sessions','angemeldet','Zusage'], voice_rows, placeholder='Voice-Zeiten durchsuchen…')}
      <p class="muted">Hinweis: Der Abgleich ist read-only und nutzt den gespeicherten Event-Voice bzw. letzten Event-Voice. EC wird dadurch nicht automatisch vergeben.</p>
    </section>
    <section class="panel" id="signups"><h2>✅ Zusagen nach Rolle</h2>{_role_signup_html(event)}</section>
    <section class="panel" id="maybe"><h2>🟡 Vielleicht</h2>{_table(['Spieler','Gildenrolle'], maybe_rows, placeholder='Vielleicht durchsuchen…')}</section>
    <section class="panel" id="no"><h2>❌ Abgemeldet</h2>{_table(['Spieler','Gildenrolle'], no_rows, placeholder='Abmeldungen durchsuchen…')}</section>
    """
    return _html_shell(f"{event.get('title') or 'Event'} · Ebo Dashboard", body)


def _render_voice_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>🎙️ Voice</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    voice = snap.get("voice") or {}
    by_user = [x for x in (voice.get("by_user") or []) if isinstance(x, dict)]
    sessions = [x for x in (voice.get("recent_sessions") or []) if isinstance(x, dict)]
    user_rows = []
    for v in by_user[:500]:
        uid = _user_id(v.get("user_id"))
        total = int(_num(v.get("total_seconds"), 0))
        user_rows.append([_member_link(uid, names.get(uid, f"User {uid}")), round(total/3600, 2), v.get("sessions"), _dt(v.get("last_joined_at")), _dt(v.get("last_left_at"))])
    session_rows = []
    for s in sessions[:500]:
        uid = _user_id(s.get("user_id") or s.get("member_id"))
        seconds = int(_num(s.get("duration_seconds"), 0))
        session_rows.append([_member_link(uid, names.get(uid, f"User {uid}")), s.get("channel_name") or s.get("channel_id"), _dt(s.get("joined_at")), _dt(s.get("left_at")), round(seconds/60, 1)])
    cards = "".join([
        _card("Sessions gesamt", voice.get("sessions_total", 0), "Runtime-DB"),
        _card("offene Sessions", voice.get("sessions_open", 0), "gerade laufend"),
        _card("geladen", voice.get("loaded_sessions", len(sessions)), "im Snapshot"),
        _card("Voice-Stunden", voice.get("total_hours_loaded", 0), "geladene Sessions"),
    ])
    body = f"""
    <nav class="topnav"><a href="/">← Start</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="#users">Spieler</a><a href="#sessions">Sessions</a></nav>
    <section class="hero"><div><div class="eyebrow">Voice-Attendance</div><h1>🎙️ Voice-Auswertung</h1><p class="muted">Read-only Übersicht der gemessenen Voice-Sessions. Snapshot: {_e(_dt(data.get('published_at')))}</p></div><a class="btn" href="/analytics">Analytics</a></section>
    <section class="grid">{cards}</section>
    <section class="panel" id="users"><h2>Voice-Zeit pro Spieler</h2>{_table(['Spieler','Stunden','Sessions','letzter Join','letztes Ende'], user_rows, placeholder='Spieler durchsuchen…')}</section>
    <section class="panel" id="sessions"><h2>Letzte Sessions</h2>{_table(['Spieler','Kanal','Rein','Raus','Minuten'], session_rows, placeholder='Sessions durchsuchen…')}</section>
    """
    return _html_shell("Voice · Ebo Dashboard", body)


def _audit_filtered_logs(logs: list[dict[str, Any]], action: str = "", actor: str = "", q: str = "") -> list[dict[str, Any]]:
    action = action.strip().lower()
    actor = actor.strip().lower()
    q = q.strip().lower()
    out = []
    for item in logs:
        if not isinstance(item, dict):
            continue
        if action and action not in str(item.get("action") or "").lower():
            continue
        if actor and actor not in str(item.get("actor_id") or "").lower() and actor not in str(item.get("actor_name") or "").lower():
            continue
        hay = " ".join(str(item.get(k) or "") for k in ("action", "summary", "actor_id", "actor_name", "target_id"))
        if q and q not in hay.lower():
            continue
        out.append(item)
    return out


def _render_audit_dashboard(data: dict[str, Any], *, action: str = "", actor: str = "", q: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>🧾 Audit</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    audit = snap.get("audit") or {}
    all_logs = [x for x in (audit.get("recent_logs") or []) if isinstance(x, dict)]
    logs = _audit_filtered_logs(all_logs, action=action, actor=actor, q=q)
    by_action = Counter(str(x.get("action") or "Unbekannt") for x in logs)
    by_actor = Counter(str(x.get("actor_id") or "Unbekannt") for x in logs)
    cards = "".join([
        _card("Audit gesamt", audit.get("logs_total", 0), "in Runtime-DB"),
        _card("geladen", len(all_logs), "im Snapshot"),
        _card("gefiltert", len(logs), "aktuelle Ansicht"),
        _card("Aktionen", len(by_action), "unterschiedliche Typen"),
    ])
    log_rows = []
    for a in logs:
        log_rows.append([_dt(a.get("created_at")), a.get("action"), a.get("actor_id"), _short(a.get("summary"), 220)])
    action_rows = [[k, v] for k, v in by_action.most_common(120)]
    actor_rows = [[k, v] for k, v in by_actor.most_common(120)]
    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/settings">Einstellungen</a><a href="/system">System</a><a href="#logs">Logs</a><a href="/api/audit">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Audit Trail · Filterbar</div><h1>🧾 Audit-Log</h1><p class="muted">Read-only Protokoll. Snapshot: {_e(_dt(data.get('published_at')))}</p></div><a class="btn" href="/">Zurück</a></section>
    <section class="panel">
      <h2>Filter</h2>
      <form method="get" class="filter-form">
        <input name="q" value="{_e(q)}" placeholder="Textsuche: EC, Auktion, Spieler…">
        <input name="action" value="{_e(action)}" placeholder="Aktion, z. B. slash_command">
        <input name="actor" value="{_e(actor)}" placeholder="Actor-ID oder Name">
        <button class="btn" type="submit">Filtern</button>
        <a class="btn ghost" href="/audit">Zurücksetzen</a>
      </form>
    </section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>Aktionen</h2>{_bars(by_action.most_common(12), max_items=12)}</div><div class="panel"><h2>Akteure</h2>{_bars(by_actor.most_common(12), max_items=12)}</div></section>
    <section class="panel"><h2>Aktionen als Tabelle</h2>{_table(['Aktion','Anzahl'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>Akteure als Tabelle</h2>{_table(['Actor ID','Anzahl'], actor_rows, placeholder='Akteure durchsuchen…')}</section>
    <section class="panel" id="logs"><h2>Letzte Audit-Einträge</h2>{_table(['Zeit','Aktion','Actor','Zusammenfassung'], log_rows, placeholder='Audit durchsuchen…')}</section>
    """
    return _html_shell("Audit · Ebo Dashboard", body)


@app.get("/voice", response_class=HTMLResponse)
def voice_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_voice_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/api/voice")
def api_voice(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    return JSONResponse({"ok": True, "voice": ((payload.get("snapshot") or {}).get("voice") or {})})

@app.get("/members", response_class=HTMLResponse)
def members_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_members_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/needs", response_class=HTMLResponse)
def needs_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_needs_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/loot", response_class=HTMLResponse)
def loot_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_loot_center(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)




@app.get("/loot-check", response_class=HTMLResponse)
def loot_check_page(item: str = "", _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_loot_check(_snapshot_payload(), item))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)

@app.get("/exports", response_class=HTMLResponse)
def exports_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_exports_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/members")
def api_members(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "members": _insight_members(snap), "quality": (_insights(snap).get("quality") or {})})


@app.get("/api/needs")
def api_needs(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "needs": ((snap.get("loot") or {}).get("needs") or {}), "insights": (_insights(snap).get("needs") or {})})


@app.get("/api/loot")
def api_loot(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "loot": (snap.get("loot") or {}), "insights": (_insights(snap).get("loot") or {})})


@app.get("/export/members.csv")
def export_members_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload(); snap = payload.get("snapshot") or {}
    rows = []
    for m in _insight_members(snap):
        rows.append([m.get("user_id"), m.get("display_name"), m.get("ingame_name"), m.get("main_role"), m.get("gearscore"), m.get("ec_balance"), m.get("main_need_count"), m.get("secondary_need_count"), m.get("event_responses"), m.get("voice_hours"), m.get("loot_won_count"), _risk_flags_text(m)])
    return _csv_response("members.csv", ["user_id","display_name","ingame_name","role","gearscore","ec","main_needs","secondary_needs","event_responses","voice_hours","loot_won","hinweise"], rows)


@app.get("/export/ec.csv")
def export_ec_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload(); snap = payload.get("snapshot") or {}
    rows = []
    for b in ((((snap.get("ec") or {}).get("balances") or {}).get("top") or [])):
        if isinstance(b, dict):
            rows.append([b.get("user_id"), b.get("display_name"), b.get("balance")])
    return _csv_response("ec.csv", ["user_id","display_name","ec"], rows)


@app.get("/export/needs.csv")
def export_needs_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload(); snap = payload.get("snapshot") or {}
    rows = []
    for n in ((((snap.get("loot") or {}).get("needs") or {}).get("items") or [])):
        if isinstance(n, dict):
            rows.append([n.get("user_id"), n.get("display_name"), n.get("main_count"), n.get("secondary_count"), " | ".join(str(x) for x in (n.get("main") or [])), " | ".join(str(x) for x in (n.get("secondary") or []))])
    return _csv_response("needs.csv", ["user_id","display_name","main_count","secondary_count","main_needs","secondary_needs"], rows)


@app.get("/export/auctions.csv")
def export_auctions_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload(); snap = payload.get("snapshot") or {}
    rows = []
    for a in ((((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])):
        if isinstance(a, dict):
            rows.append([a.get("auction_id"), a.get("item_name"), _phase_label(a), a.get("status"), a.get("bid_count"), a.get("top_bid_user_name"), a.get("top_bid_amount"), a.get("winner_name"), a.get("ends_at")])
    return _csv_response("auctions.csv", ["auction_id","item","phase","status","bids","leader","leader_bid","winner","ends_at"], rows)




@app.get("/api/loot-check")
def api_loot_check(item: str = "", _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    check = _loot_check_payload_from_snapshot(snap, item)
    safe = {}
    for key, value in check.items():
        if isinstance(value, list):
            safe[key] = [{k: v for k, v in row.items() if k != "raw"} if isinstance(row, dict) else row for row in value]
        else:
            safe[key] = value
    return JSONResponse({"ok": True, "loot_check": safe})

@app.get("/api/loot-center")
def api_loot_center(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    center = _loot_center_payload_from_snapshot(snap)
    # JSON-tauglich machen: raw Auktionsobjekte rausnehmen, weil /api/loot sie schon vollständig liefert.
    safe = {}
    for key, value in center.items():
        if isinstance(value, list):
            safe[key] = [{k: v for k, v in row.items() if k != "raw"} if isinstance(row, dict) else row for row in value]
        elif key != "raw_loot":
            safe[key] = value
    return JSONResponse({"ok": True, "loot_center": safe})




@app.get("/export/loot_check.csv")
def export_loot_check_csv(item: str = "", _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    snap = payload.get("snapshot") or {}
    check = _loot_check_payload_from_snapshot(snap, item)
    rows = []
    for n in check.get("need_matches") or []:
        rows.append([
            check.get("query") or "",
            n.get("item"),
            n.get("score") if check.get("query") else "",
            n.get("main_count"),
            ", ".join(str(p.get("display_name") or p.get("user_id") or "") for p in (n.get("main") or []) if isinstance(p, dict)),
            n.get("secondary_count"),
            ", ".join(str(p.get("display_name") or p.get("user_id") or "") for p in (n.get("secondary") or []) if isinstance(p, dict)),
            check.get("verdict"),
            check.get("next_step"),
        ])
    return _csv_response("loot_check.csv", ["suche","item","match","main_count","main_spieler","secondary_count","secondary_spieler","ergebnis","naechster_schritt"], rows)

@app.get("/export/loot_center.csv")
def export_loot_center_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    snap = payload.get("snapshot") or {}
    center = _loot_center_payload_from_snapshot(snap)
    rows = []
    for a in center.get("next_actions") or []:
        rows.append([
            a.get("auction_id"),
            a.get("item"),
            a.get("mode"),
            a.get("status"),
            a.get("bid_count"),
            a.get("leader"),
            a.get("winner_name") or a.get("winner_user_id") or "",
            _dt(a.get("ends_at")),
            a.get("main_need_count"),
            a.get("secondary_need_count"),
            a.get("next_step"),
        ])
    return _csv_response("loot_center.csv", ["auction_id","item","bereich","status","gebote","fuehrend","gewinner","ende","main_needs","secondary_needs","naechster_schritt"], rows)


@app.get("/planning", response_class=HTMLResponse)
def planning_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_planning_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/fairness", response_class=HTMLResponse)
def fairness_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_fairness_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/api/planning")
def api_planning(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    return JSONResponse({"ok": True, "planning": _planning_analytics(payload.get("snapshot") or {})})


@app.get("/api/fairness")
def api_fairness(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    return JSONResponse({"ok": True, "fairness": _fairness_analytics(payload.get("snapshot") or {})})


@app.get("/export/fairness.csv")
def export_fairness_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload(); snap = payload.get("snapshot") or {}
    fair = _fairness_analytics(snap)
    rows = []
    for r in fair.get("rows") or []:
        if isinstance(r, dict):
            rows.append([r.get("user_id"), r.get("display_name"), r.get("role"), r.get("ec_balance"), r.get("ec_earned"), r.get("ec_spent"), r.get("loot_won"), r.get("main_needs"), r.get("secondary_needs"), r.get("event_yes"), r.get("voice_hours"), " | ".join(str(x) for x in (r.get("flags") or []))])
    return _csv_response("fairness.csv", ["user_id","display_name","role","ec","earned","spent","loot_won","main_needs","secondary_needs","event_yes","voice_hours","hinweise"], rows)




# ---------------------------------------------------------------------------
# Step 3.11: Führungsstartseite / Kommandoübersicht
# ---------------------------------------------------------------------------

def _dt_obj(value: Any) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_active_auction(auction: dict[str, Any]) -> bool:
    status = str(auction.get("status") or "").strip().lower()
    phase = str(auction.get("phase") or "").strip().lower()

    # Status schlägt Phase. Eine gelieferte/geschlossene Auktion darf nicht
    # nur wegen phase="free" oder phase="sale" weiter als aktiv zählen.
    closed_words = (
        "delivered", "sold", "closed", "expired", "done", "ended", "finished",
        "cancelled", "canceled", "deleted", "abbruch", "abgebrochen",
        "abgeschlossen", "completed",
    )
    if any(x in status for x in closed_words):
        return False

    active_statuses = {"active", "open", "running", "bidding", "pending", "roll"}
    if status in active_statuses:
        return True

    # Nur wenn kein aussagekräftiger Status gesetzt ist, darf die Phase helfen.
    if not status:
        active_phases = {"need", "free", "sale", "roll", "main", "secondary", "müll", "muell", "junk"}
        return phase in active_phases

    return False


def _is_running_event(event: dict[str, Any]) -> bool:
    """Dashboard-Definition: erstellt und nicht beendet/gelöscht/abgebrochen."""
    if not isinstance(event, dict):
        return False

    status = str(
        event.get("status")
        or event.get("state")
        or event.get("phase")
        or event.get("event_status")
        or ""
    ).strip().lower()

    closed_statuses = {
        "done", "ended", "finished", "closed", "cancelled", "canceled",
        "deleted", "archived", "completed", "aborted", "expired",
        "beendet", "gelöscht", "geloescht", "abgebrochen",
    }
    if status in closed_statuses:
        return False

    for key in (
        "is_done", "done", "ended", "is_ended", "closed", "is_closed",
        "cancelled", "canceled", "deleted", "archived", "aborted",
    ):
        if bool(event.get(key)):
            return False

    if event.get("ended_at") or event.get("deleted_at") or event.get("cancelled_at") or event.get("canceled_at"):
        return False

    return True


def _event_check_items(snap: dict[str, Any]) -> list[dict[str, Any]]:
    """Robust geladene EC-/DKP-Anwesenheitschecks aus dem Snapshot.

    Unterstützt den neuen Dashboard-Snapshot (`event_checks.items`) und alte
    Zwischenstände (`event_checks.events`).
    """
    raw = snap.get("event_checks") or {}
    items = raw.get("items") if isinstance(raw, dict) else []
    out: list[dict[str, Any]] = []
    if isinstance(items, list):
        out.extend([x for x in items if isinstance(x, dict)])

    legacy_events = raw.get("events") if isinstance(raw, dict) and isinstance(raw.get("events"), dict) else {}
    for eid, chk in legacy_events.items():
        if not isinstance(chk, dict):
            continue
        row = dict(chk)
        row.setdefault("event_id", str(eid))
        out.append(row)
    return out


def _is_pending_ec_check(chk: dict[str, Any]) -> bool:
    if not isinstance(chk, dict):
        return False
    status = str(chk.get("status") or chk.get("state") or "").strip().lower()
    closed = {"done", "awarded", "finished", "closed", "ignored", "cancelled", "canceled", "deleted", "rejected"}
    if status in closed:
        return False
    if bool(chk.get("awarded") or chk.get("ec_awarded") or chk.get("ignored") or chk.get("deleted")):
        return False

    # Idealfall: Bot-Snapshot liefert ausdrücklich, dass der DKP-/EC-Check gepostet wurde.
    if bool(chk.get("posted")) or chk.get("message_id") or chk.get("channel_id"):
        return True
    if status in {"open", "pending", "posted", "processing", "review", "check"}:
        return True

    # Dashboard-only Fallback:
    # Ältere bot/dashboard_data.py-Versionen exportieren bei dkp_event_checks oft nur
    # event_id/check_id, title, created_at, attendee_count und ec_awarded. Wenn ec_awarded
    # false ist und kein geschlossener Status gesetzt ist, behandeln wir den Check als offen,
    # damit das Event nicht verschwindet, bevor EC gebucht/ignoriert wurde.
    has_check_identity = bool(chk.get("event_id") or chk.get("check_id"))
    has_check_content = bool(chk.get("title") or chk.get("event_title") or chk.get("created_at") or chk.get("attendee_count"))
    if has_check_identity and has_check_content:
        return True
    return False


def _pending_ec_check_by_event(snap: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for chk in _event_check_items(snap):
        if not _is_pending_ec_check(chk):
            continue
        eid = str(chk.get("event_id") or chk.get("check_id") or "").strip()
        if eid:
            out[eid] = chk
    return out


def _pending_ec_check_label(event: dict[str, Any]) -> dict[str, str]:
    if event.get("_pending_ec_check"):
        return _raw("<span class='pill'>EC offen</span>")
    if event.get("_attendance_review_only"):
        return _raw("<span class='pill'>Review offen</span>")
    return _raw("<span class='pill'>—</span>")


def _events_with_pending_ec_checks(snap: dict[str, Any]) -> list[dict[str, Any]]:
    events = [e for e in ((snap.get("events") or {}).get("items") or []) if isinstance(e, dict)]
    by_id: dict[str, dict[str, Any]] = {}
    for ev in events:
        eid = str(ev.get("event_id") or ev.get("id") or "").strip()
        if eid:
            by_id[eid] = dict(ev)

    pending = _pending_ec_check_by_event(snap)
    for eid, chk in pending.items():
        if eid in by_id:
            by_id[eid]["_pending_ec_check"] = True
            by_id[eid]["_pending_ec_check_status"] = str(chk.get("status") or "posted")
            # Falls das Event selbst als beendet markiert ist, hält der offene
            # EC-Check es trotzdem sichtbar, bis EC vergeben/ignoriert wurde.
            continue
        by_id[eid] = {
            "event_id": eid,
            "title": chk.get("title") or chk.get("event_title") or "EC-Anwesenheit offen",
            "when_iso": chk.get("when_iso") or chk.get("event_when") or chk.get("created_at") or chk.get("posted_at") or "",
            "created_at": chk.get("created_at") or chk.get("posted_at") or "",
            "participant_count": chk.get("attendee_count") or chk.get("participant_count") or "—",
            "maybe_count": "—",
            "no_count": "—",
            "voice_enabled": False,
            "participants": {"yes": [], "maybe": [], "no": []},
            "source": "event_check",
            "_pending_ec_check": True,
            "_pending_ec_check_status": str(chk.get("status") or "posted"),
        }
    return list(by_id.values())


def _event_stub_from_attendance_review(guild_id: int, event_id: str) -> Optional[dict[str, Any]]:
    """Erzeugt ein kleines Event aus einem gespeicherten Attendance-Review.

    Das ist der Fallback für genau den Fall, dass Discord/Bot das eigentliche
    Event schon aus dem Snapshot entfernt hat, der Dashboard-Review aber noch
    existiert und EC noch nicht gebucht wurde.
    """
    if not guild_id or not event_id:
        return None
    review = _attendance_review_load(guild_id, str(event_id))
    payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not payload:
        return None
    items = [x for x in (payload.get("items") or []) if isinstance(x, dict)]
    if not items:
        return None

    yes_people: list[dict[str, Any]] = []
    maybe_people: list[dict[str, Any]] = []
    no_people: list[dict[str, Any]] = []
    for item in items:
        uid = _user_id(item.get("user_id"))
        person = {
            "user_id": uid,
            "display_name": item.get("display_name") or f"User {uid}",
            "is_dashboard_member": True,
        }
        status = str(item.get("status") or "open").strip().lower()
        signup = str(item.get("signup") or "").strip().lower()
        if status == "ignore":
            continue
        if status == "absent" or "abgemeldet" in signup:
            no_people.append(person)
        elif status == "partial" or "vielleicht" in signup:
            maybe_people.append(person)
        else:
            yes_people.append(person)

    title = payload.get("event_title") or payload.get("title") or f"Review offen: {event_id}"
    when_iso = payload.get("event_when") or payload.get("when_iso") or review.get("updated_at") or payload.get("updated_at") or payload.get("created_at") or ""
    return {
        "event_id": str(event_id),
        "id": str(event_id),
        "title": title,
        "when_iso": when_iso,
        "created_at": payload.get("created_at") or review.get("updated_at") or "",
        "participant_count": len(items),
        "maybe_count": len(maybe_people),
        "no_count": len(no_people),
        "voice_enabled": any(_num(i.get("voice_minutes"), 0) > 0 for i in items),
        "participants": {
            "yes": [{"role": "Review", "participants": yes_people}] if yes_people else [],
            "maybe": maybe_people,
            "no": no_people,
        },
        "yes_counts": {"Review": len(yes_people)},
        "source": "attendance_review",
        "_attendance_review_only": True,
        "_attendance_review_status": review.get("status") or "reviewed",
    }


def _attendance_review_still_needs_ec(snap: dict[str, Any], guild_id: int, review: dict[str, Any]) -> bool:
    """Soll ein gespeicherter Review wieder unter /attendance auftauchen?"""
    if not guild_id or not isinstance(review, dict):
        return False
    eid = str(review.get("event_id") or ((review.get("payload") or {}).get("event_id")) or "").strip()
    if not eid:
        return False
    payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not (payload.get("items") or []):
        return False
    status = str(review.get("status") or "").strip().lower()
    if status not in {"draft", "open", "reviewed", "locked"}:
        return False
    # Wenn bereits echte EC-Buchungen oder eine erledigte Queue-Anfrage existieren,
    # ist der Review abgeschlossen und soll nicht wieder die normale Attendance-Liste füllen.
    try:
        if (_event_award_state(snap, eid) or {}).get("awarded"):
            return False
    except Exception:
        pass
    try:
        latest = _latest_ec_award_request(guild_id, eid) or {}
        if str(latest.get("status") or "").strip().lower() == "done":
            return False
    except Exception:
        pass
    return True


def _attendance_events_with_review_fallbacks(snap: dict[str, Any], guild_id: int) -> list[dict[str, Any]]:
    """Events für /attendance: aktuelle Events + offene EC-Checks + Review-Fallbacks."""
    events = _events_with_pending_ec_checks(snap)
    by_id: dict[str, dict[str, Any]] = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue
        eid = str(ev.get("event_id") or ev.get("id") or "").strip()
        if eid:
            by_id[eid] = ev

    if guild_id:
        for review in _attendance_all_reviews(guild_id, limit=300):
            if not _attendance_review_still_needs_ec(snap, guild_id, review):
                continue
            eid = str(review.get("event_id") or ((review.get("payload") or {}).get("event_id")) or "").strip()
            if not eid or eid in by_id:
                continue
            stub = _event_stub_from_attendance_review(guild_id, eid)
            if stub:
                by_id[eid] = stub

    return list(by_id.values())


def _open_attendance_review_events_for_homepage(snap: dict[str, Any], guild_id: int, *, limit: int = 12) -> list[dict[str, Any]]:
    """Review-Fallbacks für die Startseite.

    Wenn ein Discord-Event aus dem Snapshot verschwunden ist, der gespeicherte
    Attendance-Review aber noch EC braucht, bleibt es auf der Startseite sichtbar.
    """
    out: list[dict[str, Any]] = []
    if not guild_id:
        return out
    for review in _attendance_all_reviews(guild_id, limit=300):
        if not _attendance_review_still_needs_ec(snap, guild_id, review):
            continue
        eid = str(review.get("event_id") or ((review.get("payload") or {}).get("event_id")) or "").strip()
        if not eid:
            continue
        stub = _event_stub_from_attendance_review(guild_id, eid)
        if stub:
            out.append({**stub, "_dt": _dt_obj(stub.get("when_iso") or stub.get("created_at"))})
    out.sort(key=lambda x: (x.get("_dt") is None, x.get("_dt") or datetime.max.replace(tzinfo=timezone.utc)))
    return out[:limit]


def _running_events_from_snapshot(snap: dict[str, Any], *, limit: int = 12) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ev in _events_with_pending_ec_checks(snap):
        if not isinstance(ev, dict):
            continue
        if not _is_running_event(ev) and not ev.get("_pending_ec_check"):
            continue
        out.append({**ev, "_dt": _dt_obj(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at"))})
    out.sort(key=lambda x: (x.get("_dt") is None, x.get("_dt") or datetime.max.replace(tzinfo=timezone.utc)))
    return out[:limit]


def _auction_leader_text(auction: dict[str, Any], names: dict[int, str]) -> str:
    uid = _user_id(auction.get("top_bid_user_id") or auction.get("leader_user_id") or auction.get("winner_user_id"))
    amount = auction.get("top_bid_amount") if auction.get("top_bid_amount") is not None else auction.get("current_bid")
    if uid and amount is not None:
        return f"{names.get(uid, f'User {uid}')} mit {_fmt_ec(amount)} EC"
    if uid:
        return names.get(uid, f"User {uid}")
    winner = auction.get("winner_name") or auction.get("recipient_name")
    return str(winner or "niemand")


def _leadership_insights(snap: dict[str, Any]) -> dict[str, Any]:
    analytics = _analytics_from_snapshot(snap)
    planning = _planning_analytics(snap)
    fairness = _fairness_analytics(snap)
    activity = _activity_analytics(snap)
    names = _profile_name_map(snap)
    guild = snap.get("guild") or {}
    member_filter = guild.get("member_filter") if isinstance(guild.get("member_filter"), dict) else {}
    role_member_count = int(_num(member_filter.get("eligible_count"), analytics.get("role_member_count", 0)))

    running_events = _running_events_from_snapshot(snap, limit=12)

    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    active_auctions = []
    for a in auctions:
        if not isinstance(a, dict) or not _is_active_auction(a):
            continue
        active_auctions.append({**a, "_dt": _dt_obj(a.get("ends_at") or a.get("end_at") or a.get("expires_at"))})
    active_auctions.sort(key=lambda x: (x.get("_dt") is None, x.get("_dt") or datetime.max.replace(tzinfo=timezone.utc)))

    quality = (_insights(snap).get("quality") if isinstance(_insights(snap).get("quality"), dict) else {})
    missing_profile = int(_num(quality.get("missing_profile"), analytics.get("missing_profiles", 0)))
    missing_ec = int(_num(quality.get("missing_ec"), analytics.get("missing_ec", 0)))
    missing_needs = int(_num(quality.get("missing_needs"), analytics.get("missing_needs", 0)))
    no_event_response = int(_num(quality.get("no_event_response"), 0))
    no_voice_time = int(_num(quality.get("no_voice_time"), 0))

    flagged_members = fairness.get("flagged") if isinstance(fairness.get("flagged"), list) else []
    risk_members = _insights(snap).get("risk_members") if isinstance(_insights(snap).get("risk_members"), list) else []
    if not flagged_members and risk_members:
        flagged_members = risk_members

    tasks: list[dict[str, Any]] = []
    if not member_filter or member_filter.get("mode") != "discord_role":
        tasks.append({"prio": "hoch", "area": "Setup", "task": "Gildenrolle festlegen", "detail": "Ohne Gildenrolle sind Dashboard-Zahlen nicht massentauglich.", "link": "/settings"})
    if missing_needs:
        tasks.append({"prio": "mittel", "area": "Needs", "task": f"{missing_needs} Mitglieder ohne Needliste", "detail": "Für Lootplanung und Fairness nachpflegen lassen.", "link": "/needs"})
    if missing_profile:
        tasks.append({"prio": "mittel", "area": "Profile", "task": f"{missing_profile} Mitglieder ohne Profil", "detail": "Rolle/GS fehlen für Planung und Analytics.", "link": "/members"})
    if missing_ec:
        tasks.append({"prio": "niedrig", "area": "EC", "task": f"{missing_ec} Mitglieder ohne EC-Konto", "detail": "Kann normal sein, sollte aber geprüft werden.", "link": "/ec"})
    if int(planning.get("events_at_risk") or 0):
        tasks.append({"prio": "hoch", "area": "Events", "task": f"{planning.get('events_at_risk')} Event(s) mit Rollen-/Teilnehmer-Risiko", "detail": "Tank/Heiler/Teilnehmer prüfen.", "link": "/planning"})
    if active_auctions:
        tasks.append({"prio": "mittel", "area": "Loot", "task": f"{len(active_auctions)} aktive Auktion(en)", "detail": "Endzeiten und Führende prüfen.", "link": "/loot"})
    if flagged_members:
        tasks.append({"prio": "mittel", "area": "Fairness", "task": f"{len(flagged_members)} Fairness-/Datenhinweis(e)", "detail": "Keine automatische Bewertung, nur Leitungs-Hinweis.", "link": "/fairness"})
    if no_event_response:
        tasks.append({"prio": "mittel", "area": "Aktivität", "task": f"{no_event_response} Mitglieder ohne Eventantwort", "detail": "Anmeldedisziplin prüfen.", "link": "/analytics"})
    if no_voice_time:
        tasks.append({"prio": "niedrig", "area": "Voice", "task": f"{no_voice_time} Mitglieder ohne gemessene Voice-Zeit", "detail": "Nur relevant, wenn Voice-Attendance genutzt wird.", "link": "/analytics"})

    prio_order = {"hoch": 0, "mittel": 1, "niedrig": 2}
    tasks.sort(key=lambda x: (prio_order.get(str(x.get("prio")), 9), str(x.get("area"))))

    return {
        "member_count": role_member_count,
        "tasks": tasks,
        "running_events": running_events,
        "active_auctions": active_auctions[:12],
        "flagged_members": flagged_members[:12],
        "planning": planning,
        "fairness": fairness,
        "activity": activity,
        "analytics": analytics,
        "quality": {
            "missing_profile": missing_profile,
            "missing_ec": missing_ec,
            "missing_needs": missing_needs,
            "no_event_response": no_event_response,
            "no_voice_time": no_voice_time,
        },
        "names": names,
    }


def _prio_pill(priority: Any) -> dict[str, str]:
    p = str(priority or "").lower()
    label = {"hoch": "🔴 hoch", "mittel": "🟡 mittel", "niedrig": "⚪ niedrig"}.get(p, p or "—")
    return _raw(f"<span class='pill'>{_e(label)}</span>")


def _render_leadership_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell(
            "Ebo Dashboard",
            f"""
            <section class="panel">
              <h1>🏰 Gildenleitung</h1>
              <p class="muted">{_e(data.get('error'))}</p>
              <p>Starte den Bot mit der aktuellen Version und warte bis zu 5 Minuten. Oder nutze im Discord <code>/dashboard_status</code>, damit direkt ein Snapshot veröffentlicht wird.</p>
            </section>
            """,
        )

    snap: dict[str, Any] = data.get("snapshot") or {}
    guild = snap.get("guild") or {}
    li = _leadership_insights(snap)
    analytics = li.get("analytics") or {}
    quality = li.get("quality") or {}
    names = li.get("names") or {}
    member_filter = (guild.get("member_filter") or {}) if isinstance(guild.get("member_filter"), dict) else {}
    role_line = "Gildenrolle nicht gesetzt"
    if member_filter.get("mode") == "discord_role":
        role_line = f"Rolle: {member_filter.get('role_name')} · {member_filter.get('eligible_count', 0)} Mitglieder"

    tasks = li.get("tasks") or []
    urgent_count = sum(1 for t in tasks if str(t.get("prio")) == "hoch")
    running_events = list(li.get("running_events") or [])
    active_auctions = li.get("active_auctions") or []
    flagged_members = li.get("flagged_members") or []
    guild_id = _safe_guild_id(data)
    if guild_id:
        seen_event_ids = {str(ev.get("event_id") or ev.get("id") or "") for ev in running_events if isinstance(ev, dict)}
        for ev in _open_attendance_review_events_for_homepage(snap, guild_id, limit=12):
            eid = str(ev.get("event_id") or ev.get("id") or "")
            if eid and eid not in seen_event_ids:
                running_events.append(ev)
                seen_event_ids.add(eid)
    queue_rows = _ec_award_requests_for_dashboard(guild_id, limit=40) if guild_id else []
    queue_counts = _ec_queue_counts(queue_rows)
    queue_open = queue_counts.get("pending", 0) + queue_counts.get("processing", 0)

    cards = "".join([
        _card("Offene Aufgaben", len(tasks), f"hoch: {urgent_count}"),
        _card("Rollenmitglieder", li.get("member_count", 0), role_line),
        _card("Laufende Events", len(running_events), "inkl. offener Reviews"),
        _card("Aktive Auktionen", len(active_auctions), "Auktionshaus/DKP-Log"),
        _card("ohne Needliste", quality.get("missing_needs", 0), "nachpflegen lassen"),
        _card("EC gesamt", _fmt_ec(analytics.get("total_ec")), f"Ø {_fmt_ec(analytics.get('avg_ec'))}"),
        _card("EC-Queue", queue_open, f"offen/verarbeitend · erledigt: {queue_counts.get('done', 0)}"),
        _card("Voice-Stunden", f"{_num(analytics.get('recent_voice_hours'), 0):.1f} h", "geladene Sessions"),
        _card("Snapshot", _dt(data.get("published_at")), "read-only"),
    ])

    task_rows = []
    for t in tasks:
        link = str(t.get("link") or "/")
        task_rows.append([
            _prio_pill(t.get("prio")),
            t.get("area"),
            _raw(f"<a class='link' href='{_e(link)}'>{_e(t.get('task'))}</a>"),
            t.get("detail"),
        ])

    event_rows = []
    for ev in running_events:
        if not isinstance(ev, dict):
            continue
        eid = str(ev.get("event_id") or ev.get("id") or "")
        if ev.get("_attendance_review_only"):
            title_cell = _raw(f'<a class="link" href="/attendance/{_e(eid)}">{_e(ev.get("title") or ev.get("name") or eid)}</a> <span class="pill">aus Review</span>')
        else:
            title_cell = _event_link(eid, ev.get("title") or ev.get("name"))
        event_rows.append([
            title_cell,
            _dt(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at")),
            ev.get("participant_count", ev.get("participants", "—")),
            ev.get("maybe_count", ev.get("maybe", "—")),
            ev.get("no_count", ev.get("no", "—")),
            "ja" if ev.get("voice_enabled") else "nein",
            _pending_ec_check_label(ev),
        ])

    auction_rows = []
    for a in active_auctions:
        if not isinstance(a, dict):
            continue
        auction_rows.append([
            _auction_link(a.get("auction_id"), a.get("item_name") or a.get("title")),
            a.get("status") or "—",
            a.get("phase") or "—",
            _auction_leader_text(a, names),
            _dt(a.get("ends_at") or a.get("end_at") or a.get("expires_at")),
        ])

    member_rows = []
    for m in flagged_members:
        if not isinstance(m, dict):
            continue
        flags = m.get("flags") if isinstance(m.get("flags"), list) else m.get("risk_flags") if isinstance(m.get("risk_flags"), list) else []
        member_rows.append([
            _member_link(m.get("user_id"), m.get("display_name")),
            m.get("role") or m.get("main_role") or "—",
            _fmt_ec(m.get("ec_balance")) if m.get("ec_balance") is not None else "—",
            m.get("main_needs") if m.get("main_needs") is not None else m.get("main_need_count", "—"),
            m.get("loot_won") if m.get("loot_won") is not None else m.get("loot_won_count", "—"),
            ", ".join(str(x) for x in flags) or "—",
        ])

    quick_links = _table([
        "Bereich", "Wofür"
    ], [
        [_raw('<a class="link" href="/planning">📅 Planung</a>'), "Events mit Rollen-/Teilnehmer-Hinweisen"],
        [_raw('<a class="link" href="/members">👥 Mitglieder</a>'), "Roster, Profile, Datenqualität"],
        [_raw('<a class="link" href="/needs">🎁 Needs</a>'), "Top-Needs und Mitglieder ohne Needliste"],
        [_raw('<a class="link" href="/loot">🏆 Loot</a>'), "Aktive Auktionen, Gewinner, Roll-/Bid-Historie"],
        [_raw('<a class="link" href="/fairness">⚖️ Fairness</a>'), "Need/EC/Loot-Hinweise"],
        [_raw('<a class="link" href="/attendance">✅ Anwesenheit</a>'), "Event-Review und EC-Buchung"],
        [_raw('<a class="link" href="/attendance-stats">📊 Anwesenheit-Stats</a>'), "Spielerquoten, offene Reviews und CSV-Export"],
        [_raw('<a class="link" href="/attendance-archive">📦 Attendance-Archiv</a>'), "Reviews abschließen, öffnen und alte Events wiederfinden"],
        [_raw('<a class="link" href="/ec-queue">🌐 EC-Queue</a>'), "Dashboard-Buchungen prüfen, abbrechen oder neu öffnen"],
        [_raw('<a class="link" href="/audit">🧾 Audit</a>'), "Wer hat was gemacht"],
        [_raw('<a class="link" href="/admin">🛡️ Leitung</a>'), "interne Notizen und Prüfmarkierungen"],
        [_raw('<a class="link" href="/overview">📊 Gesamtübersicht</a>'), "alte Tabellen-Startseite"],
    ], searchable=False)

    body = f"""
    <nav class="topnav">
      <a href="/">Kommando</a>
      <a href="/planning">Planung</a>
      <a href="/members">Mitglieder</a>
      <a href="/needs">Needs</a>
      <a href="/loot">Loot</a>
      <a href="/fairness">Fairness</a>
      <a href="/analytics">Analytics</a><a href="/voice">Voice</a>
      <a href="/ec">EC</a>
      <a href="/ec-queue">EC-Queue</a>
      <a href="/attendance">Anwesenheit</a>
      <a href="/attendance-stats">Anwesenheit-Stats</a>
      <a href="/attendance-archive">Attendance-Archiv</a>
      <a href="/audit">Audit</a>
      <a href="/admin">Leitung</a>
      <a href="/settings">Einstellungen</a>
      <a href="/system">System</a>
      <a href="/exports">Exports</a>
    </nav>

    <section class="hero">
      <div>
        <div class="eyebrow">Führungsstartseite · read-only</div>
        <h1>🏰 {_e(guild.get('name') or data.get('guild_name') or 'Gilde')}</h1>
        <p>Was heute wichtig ist: offene Aufgaben, laufende Events, aktive Auktionen und auffällige Mitglieder.</p>
        <p class="muted">{_e(role_line)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/overview">Gesamtübersicht</a>
    </section>

    <section class="grid">{cards}</section>

    <section class="panel">
      <h2>✅ Offene Aufgaben</h2>
      <p class="muted">Priorisierte Leitungs-Hinweise. Es wird nichts automatisch geändert.</p>
      {_table(['Priorität','Bereich','Aufgabe','Details'], task_rows, placeholder='Aufgaben durchsuchen…')}
    </section>

    <section class="split">
      <div class="panel">
        <h2>📅 Laufende Events</h2>
        {_table(['Event','Zeit','Teilnehmer','Vielleicht','Abgemeldet','Voice','EC-Check'], event_rows, placeholder='Laufende Events durchsuchen…')}
      </div>
      <div class="panel">
        <h2>🎁 Aktive Auktionen</h2>
        {_table(['Item','Status','Phase','Führend','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}
      </div>
    </section>

    <section class="split">
      <div class="panel">
        <h2>⚠️ Mitglieder mit Hinweisen</h2>
        {_table(['Spieler','Rolle','EC','Main','Loot','Hinweise'], member_rows, placeholder='Mitglieder durchsuchen…')}
      </div>
      <div class="panel">
        <h2>🧭 Schnellzugriff</h2>
        {quick_links}
      </div>
    </section>
    """
    return _html_shell("Kommando · Ebo Dashboard", body)


@app.get("/api/leadership")
def api_leadership(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "leadership": _leadership_insights(snap)})



@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/"):
    discord_ready = _discord_oauth_enabled()
    basic_ready = bool(_env("DASHBOARD_PASSWORD"))
    role_id = _env("DASHBOARD_MEMBER_ROLE_ID") or _configured_member_role_id_from_snapshot()
    role_name = _configured_member_role_name_from_snapshot() or "gesetzte Gildenrolle"
    discord_block = ""
    if discord_ready:
        discord_block = f"""
        <div class="panel">
          <h2>Discord Login</h2>
          <p class="muted">Zugriff wird über die Discord-Mitgliedschaft und Rollen geprüft.</p>
          <p>Erlaubte Rolle: <strong>{_e(role_name)}</strong> <span class="muted">{_e(role_id or 'keine Rollen-ID erkannt')}</span></p>
          <a class="btn" href="/auth/discord/start?next={_e(next or '/')}">Mit Discord einloggen</a>
        </div>
        """
    else:
        discord_block = """
        <div class="warn">Discord Login ist noch nicht eingerichtet. Setze DASHBOARD_DISCORD_CLIENT_ID und DASHBOARD_DISCORD_CLIENT_SECRET beim Dashboard-Service.</div>
        """
    basic_block = ""
    if basic_ready:
        basic_block = """
        <div class="panel">
          <h2>Passwort-Fallback</h2>
          <p class="muted">Der alte Basic-Auth Login bleibt als Fallback aktiv, solange DASHBOARD_AUTH_MODE nicht auf <code>discord</code> steht.</p>
          <p>Wenn der Browser nach Benutzer/Passwort fragt: <code>DASHBOARD_USERNAME</code> und <code>DASHBOARD_PASSWORD</code> nutzen.</p>
        </div>
        """
    body = f"""
    <section class="hero"><div><div class="eyebrow">Ebo Dashboard</div><h1>🔐 Login</h1><p class="muted">Read-only Dashboard für Gildenleitung und berechtigte Mitglieder.</p></div></section>
    {discord_block}
    {basic_block}
    """
    return HTMLResponse(_html_shell("Login · Ebo Dashboard", body))


@app.get("/auth/discord/debug")
def discord_debug(request: Request):
    redirect_uri = _redirect_uri(request)
    base_url = _base_url(request)
    authorize_params = {
        "client_id": _env("DASHBOARD_DISCORD_CLIENT_ID"),
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "identify",
        "state": "debug",
    }
    authorize_url = "https://discord.com/oauth2/authorize?" + urllib.parse.urlencode(authorize_params)
    body = f"""
    <section class='panel'>
      <h1>🔐 Discord Login Debug</h1>
      <p class='muted'>Diese Werte müssen exakt zu Discord Developer Portal und Railway passen.</p>
      <table>
        <tr><th>Wert</th><th>Inhalt</th></tr>
        <tr><td>DASHBOARD_PUBLIC_BASE_URL / erkannt</td><td><code>{_e(base_url)}</code></td></tr>
        <tr><td>Redirect URI, die diese Website an Discord sendet</td><td><code>{_e(redirect_uri)}</code></td></tr>
        <tr><td>Discord Developer Portal → Redirects</td><td><code>{_e(redirect_uri)}</code></td></tr>
        <tr><td>Railway Variable DASHBOARD_DISCORD_REDIRECT_URI</td><td><code>{_e(_env('DASHBOARD_DISCORD_REDIRECT_URI') or 'nicht gesetzt')}</code></td></tr>
        <tr><td>Client ID gesetzt</td><td><code>{_e('ja' if _env('DASHBOARD_DISCORD_CLIENT_ID') else 'nein')}</code></td></tr>
        <tr><td>Client Secret gesetzt</td><td><code>{_e('ja · Länge ' + str(len(_env('DASHBOARD_DISCORD_CLIENT_SECRET'))) if _env('DASHBOARD_DISCORD_CLIENT_SECRET') else 'nein')}</code></td></tr>
        <tr><td>Token Endpoint</td><td><code>{_e(DISCORD_OAUTH_TOKEN_URL)}</code></td></tr>
        <tr><td>Scope</td><td><code>identify</code></td></tr>
      </table>
      <p><a class='btn' href='/login'>Zurück</a> <a class='btn secondary' href='{_e(authorize_url)}'>Test-Authorize-URL öffnen</a></p>
    </section>
    """
    return HTMLResponse(_html_shell("Discord Login Debug", body))


@app.get("/auth/discord/start")
def discord_start(request: Request, next: str = "/"):
    if not _discord_oauth_enabled():
        return RedirectResponse("/login", status_code=303)
    state_payload = {"nonce": secrets.token_urlsafe(24), "next": next or "/", "exp": int(time.time()) + 600}
    state = _make_token(state_payload)
    params = {
        "client_id": _env("DASHBOARD_DISCORD_CLIENT_ID"),
        "redirect_uri": _redirect_uri(request),
        "response_type": "code",
        "scope": "identify",
        "state": state,
    }
    url = "https://discord.com/oauth2/authorize?" + urllib.parse.urlencode(params)
    resp = RedirectResponse(url, status_code=303)
    resp.set_cookie(STATE_COOKIE, state, max_age=600, httponly=True, secure=_cookie_secure(), samesite="lax")
    return resp


@app.get("/auth/discord/callback")
def discord_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(_html_shell("Discord Login", f"<section class='panel'><h1>❌ Discord Login abgebrochen</h1><p>{_e(error)}</p><p><a class='btn' href='/login'>Zurück</a></p></section>"), status_code=400)
    stored = request.cookies.get(STATE_COOKIE, "")
    state_data = _read_token(state)
    if not code or not state or not stored or stored != state or not state_data:
        return HTMLResponse(_html_shell("Discord Login", "<section class='panel'><h1>❌ Ungültiger Login-State</h1><p class='muted'>Bitte Login erneut starten.</p><p><a class='btn' href='/login'>Zum Login</a></p></section>"), status_code=400)

    try:
        token_data = _request_json(
            DISCORD_OAUTH_TOKEN_URL,
            method="POST",
            data={
                "client_id": _env("DASHBOARD_DISCORD_CLIENT_ID"),
                "client_secret": _env("DASHBOARD_DISCORD_CLIENT_SECRET"),
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": _redirect_uri(request),
            },
        )
        access_token = str(token_data.get("access_token") or "")
        if not access_token:
            raise RuntimeError(f"Discord OAuth hat keinen access_token geliefert: {token_data}")
        user = _request_json(f"{DISCORD_API_BASE}/users/@me", token=access_token)
        uid = str(user.get("id") or "")
        username = str(user.get("global_name") or user.get("username") or uid)
        auth_lists = _snapshot_auth_lists()
        guild_id = _env("DASHBOARD_GUILD_ID") or str(auth_lists.get("guild_id") or "")
        if not uid:
            raise RuntimeError("Discord hat keine User-ID zurückgegeben.")
        if not guild_id:
            raise RuntimeError("DASHBOARD_GUILD_ID ist nicht gesetzt und konnte nicht aus dem Snapshot gelesen werden.")

        allowed_ids = set(auth_lists.get("allowed_member_ids") or set())
        admin_ids = set(auth_lists.get("admin_member_ids") or set())
        require_admin = _env("DASHBOARD_REQUIRE_ADMIN", "0").lower() in {"1", "true", "yes", "ja"}
        is_admin = uid in admin_ids
        is_allowed = uid in allowed_ids

        if require_admin and not is_admin:
            return HTMLResponse(_html_shell("Kein Zugriff", "<section class='panel'><h1>⛔ Kein Zugriff</h1><p class='muted'>Dieses Dashboard erlaubt aktuell nur die gesetzte Dashboard-Adminrolle.</p><p><a class='btn' href='/logout'>Logout</a></p></section>"), status_code=403)
        if not (is_admin or is_allowed):
            auth = auth_lists.get("auth") or {}
            member_role = (auth.get("member_role") or {}) if isinstance(auth, dict) else {}
            role_hint = member_role.get("role_name") or member_role.get("role_id") or "keine Gildenrolle im Snapshot"
            return HTMLResponse(_html_shell("Kein Zugriff", f"<section class='panel'><h1>⛔ Kein Zugriff</h1><p class='muted'>Deine Discord-ID ist im aktuellen Dashboard-Snapshot nicht als Gildenmitglied/Admin enthalten.</p><p>Erlaubte Gildenrolle laut Snapshot: <code>{_e(role_hint)}</code></p><p class='muted'>Falls du die Rolle gerade erst gesetzt hast: im Discord <code>/dashboard_status</code> ausführen und dann erneut einloggen.</p><p><a class='btn' href='/logout'>Logout</a></p></section>"), status_code=403)

        session = {
            "user_id": uid,
            "username": username,
            "role": "admin" if is_admin else "member",
            "roles": ["snapshot_admin"] if is_admin else ["snapshot_member"],
            "guild_id": str(guild_id),
            "iat": int(time.time()),
            "exp": int(time.time()) + 7 * 24 * 3600,
        }
        resp = RedirectResponse(str(state_data.get("next") or "/"), status_code=303)
        resp.delete_cookie(STATE_COOKIE)
        resp.set_cookie(SESSION_COOKIE, _make_token(session), max_age=7 * 24 * 3600, httponly=True, secure=_cookie_secure(), samesite="lax")
        return resp
    except Exception as exc:
        return HTMLResponse(_html_shell("Discord Login Fehler", f"<section class='panel'><h1>❌ Discord Login fehlgeschlagen</h1><p><strong>{_e(type(exc).__name__)}</strong>: {_e(exc)}</p><p class='muted'>Prüfe Client Secret, Snapshot und gesetzte Dashboard-Rollen. Rollen werden jetzt aus dem Bot-Snapshot gelesen.</p><p><a class='btn' href='/login'>Zurück</a></p></section>"), status_code=500)


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(SESSION_COOKIE)
    resp.delete_cookie(STATE_COOKIE)
    return resp


@app.get("/me", response_class=HTMLResponse)
def me(request: Request, _: bool = Depends(_auth)):
    user = _current_user(request)
    if not user:
        body = "<section class='panel'><h1>🔐 Login</h1><p class='muted'>Du nutzt aktuell den Basic-Auth Fallback.</p><p><a class='btn' href='/login'>Login-Seite</a></p></section>"
    else:
        rows = [[k, v if k != "roles" else ", ".join(v[:12]) + (" …" if len(v) > 12 else "")] for k, v in user.items()]
        body = f"<nav class='topnav'><a href='/'>← Übersicht</a><a href='/logout'>Logout</a></nav><section class='panel'><h1>👤 Mein Dashboard-Login</h1>{_table(['Key','Wert'], rows, searchable=False)}</section>"
    return HTMLResponse(_html_shell("Mein Login · Ebo Dashboard", body))

@app.get("/healthz")
def healthz():
    return {"ok": True, "database_url": bool(_database_url())}


@app.get("/api/snapshot")
def api_snapshot(_: bool = Depends(_auth)):
    return JSONResponse(_snapshot_payload())


@app.get("/api/analytics")
def api_analytics(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "analytics": _analytics_from_snapshot(snap), "activity": _activity_analytics(snap)})


@app.get("/analytics", response_class=HTMLResponse)
def analytics_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_activity_analytics(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )



@app.get("/ec", response_class=HTMLResponse)
def ec_dashboard(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_ec_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )



@app.get("/api/ec-award-requests")
def api_ec_award_requests(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    rows = _ec_award_requests_for_dashboard(guild_id, limit=120) if guild_id else []
    safe_rows = []
    for r in rows:
        safe_rows.append({
            "request_id": r.get("request_id"),
            "event_id": r.get("event_id"),
            "event_type": r.get("event_type"),
            "status": r.get("status"),
            "full_ec": r.get("full_ec"),
            "partial_ec": r.get("partial_ec"),
            "actor_name": r.get("actor_name"),
            "requested_at": str(r.get("requested_at") or ""),
            "claimed_at": str(r.get("claimed_at") or ""),
            "processed_at": str(r.get("processed_at") or ""),
            "result": r.get("result") if isinstance(r.get("result"), dict) else {},
            "payload_summary": {
                "event_title": (r.get("payload") or {}).get("event_title") if isinstance(r.get("payload"), dict) else "",
                "recipient_count": (r.get("payload") or {}).get("recipient_count") if isinstance(r.get("payload"), dict) else 0,
                "total_ec": (r.get("payload") or {}).get("total_ec") if isinstance(r.get("payload"), dict) else 0,
            },
        })
    return JSONResponse({"ok": True, "guild_id": guild_id, "count": len(safe_rows), "items": safe_rows})


@app.get("/api/ec")
def api_ec(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "ec": (snap.get("ec") or {})})


@app.get("/api/quality")
def api_quality(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    analytics = _analytics_from_snapshot(snap)
    return JSONResponse({
        "ok": True,
        "role_member_count": analytics.get("role_member_count", 0),
        "missing_profiles": analytics.get("missing_profiles", 0),
        "missing_ec": analytics.get("missing_ec", 0),
        "missing_needs": analytics.get("missing_needs", 0),
        "profile_coverage": analytics.get("profile_coverage"),
        "ec_coverage": analytics.get("ec_coverage"),
        "need_coverage": analytics.get("need_coverage"),
    })


@app.get("/auction/{auction_id}", response_class=HTMLResponse)
def auction_detail(request: Request, auction_id: str, msg: str = "", _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_auction_detail(_snapshot_payload(), str(auction_id), _current_user(request), msg=msg))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.post("/admin/auction/{auction_id}/bid")
async def auction_dashboard_bid(request: Request, auction_id: str, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    snap = payload.get("snapshot") or {}
    auction = _auction_by_id(snap, str(auction_id)) if payload.get("ok") else None
    msg = ""
    try:
        raw = (await request.body()).decode("utf-8", errors="replace")
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        amount = int(str((form.get("quick_amount") or form.get("amount") or ["0"])[0]).strip())
        if not auction:
            msg = "❌ Auktion nicht im Snapshot gefunden."
        elif _loot_is_sale_like(auction):
            msg = "❌ Diese Auktion ist ein Sale/Müll-Item. Nutze Kaufen/Würfeln."
        elif amount < _loot_min_next_bid(auction):
            msg = f"❌ Mindestgebot ist aktuell {_loot_min_next_bid(auction)} EC."
        else:
            actor = _current_user(request) or {}
            result = _enqueue_loot_action_request(int(guild_id), auction, "bid", int(amount), actor)
            msg = "✅ Gebot wurde an den Bot gesendet." if result.get("ok") else f"❌ {result.get('error') or 'Gebot konnte nicht gesendet werden.'}"
    except Exception as exc:
        msg = f"❌ Fehler: {type(exc).__name__}: {exc}"
    return RedirectResponse(f"/auction/{urllib.parse.quote(str(auction_id))}?msg={urllib.parse.quote(msg)}", status_code=303)


@app.post("/admin/auction/{auction_id}/sale")
async def auction_dashboard_sale(request: Request, auction_id: str, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    snap = payload.get("snapshot") or {}
    auction = _auction_by_id(snap, str(auction_id)) if payload.get("ok") else None
    msg = ""
    try:
        raw = (await request.body()).decode("utf-8", errors="replace")
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        action_type = str((form.get("action_type") or ["sale_buy"])[0]).strip().lower()
        if action_type not in {"sale_buy", "junk_roll"}:
            action_type = "sale_buy"
        if not auction:
            msg = "❌ Auktion nicht im Snapshot gefunden."
        elif not _loot_is_sale_like(auction):
            msg = "❌ Diese Auktion ist kein Sale/Müll-Item."
        else:
            price = int(_num(auction.get("fixed_price") if auction.get("fixed_price") is not None else auction.get("start_bid"), 0))
            actor = _current_user(request) or {}
            result = _enqueue_loot_action_request(int(guild_id), auction, action_type, price, actor)
            msg = "✅ Aktion wurde an den Bot gesendet." if result.get("ok") else f"❌ {result.get('error') or 'Aktion konnte nicht gesendet werden.'}"
    except Exception as exc:
        msg = f"❌ Fehler: {type(exc).__name__}: {exc}"
    return RedirectResponse(f"/auction/{urllib.parse.quote(str(auction_id))}?msg={urllib.parse.quote(msg)}", status_code=303)


@app.get("/api/auction/{auction_id}")
def api_auction(auction_id: str, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    auc = _auction_by_id(payload.get("snapshot") or {}, str(auction_id))
    if not auc:
        return JSONResponse({"ok": False, "error": "Auktion nicht gefunden"}, status_code=404)
    return JSONResponse({"ok": True, "auction": auc})


@app.get("/api/auction/{auction_id}/dashboard-actions")
def api_auction_dashboard_actions(auction_id: str, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    return JSONResponse({"ok": True, "items": _loot_action_requests_for_auction(int(guild_id), str(auction_id), limit=50)})


@app.get("/event/{event_id}", response_class=HTMLResponse)
def event_detail(event_id: str, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_event_detail(_snapshot_payload(), str(event_id)))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/member/{user_id}", response_class=HTMLResponse)
def member_detail(user_id: int, request: Request, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_member_detail(_snapshot_payload(), int(user_id), _current_user(request)))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )




@app.get("/admin", response_class=HTMLResponse)
def admin_actions_page(_: bool = Depends(_admin_auth)):
    try:
        return HTMLResponse(_render_admin_actions_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )




@app.get("/ec-queue", response_class=HTMLResponse)
def ec_queue_dashboard(request: Request, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_ec_queue_dashboard(_snapshot_payload(), _current_user(request), str(request.query_params.get("msg") or "")))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.post("/admin/ec-award-requests/{request_id}/cancel")
def admin_ec_award_request_cancel(request_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    _ok, msg = _update_ec_award_request_status(
        guild_id,
        request_id,
        "cancelled",
        _current_user(request) or {},
        allowed_current={"pending"},
        result_patch={"ok": False, "cancelled_by_dashboard": True, "message": "Durch Dashboard-Admin abgebrochen."},
    )
    suffix = urllib.parse.urlencode({"msg": msg})
    return RedirectResponse(f"/ec-queue?{suffix}", status_code=303)


@app.post("/admin/ec-award-requests/{request_id}/retry")
def admin_ec_award_request_retry(request_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    _ok, msg = _update_ec_award_request_status(
        guild_id,
        request_id,
        "pending",
        _current_user(request) or {},
        allowed_current={"failed", "rejected", "cancelled"},
        result_patch={"ok": False, "requeued_by_dashboard": True, "message": "Durch Dashboard-Admin erneut geöffnet."},
    )
    suffix = urllib.parse.urlencode({"msg": msg})
    return RedirectResponse(f"/ec-queue?{suffix}", status_code=303)


@app.post("/admin/ec-award-requests/{request_id}/requeue")
def admin_ec_award_request_requeue(request_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    current = _ec_award_request_by_request_id(guild_id, request_id) if guild_id else {}
    if current and not _ec_award_request_is_stale(current):
        msg = "Processing-Anfrage ist noch nicht alt genug. Nicht erneut geöffnet."
    else:
        _ok, msg = _update_ec_award_request_status(
            guild_id,
            request_id,
            "pending",
            _current_user(request) or {},
            allowed_current={"processing"},
            result_patch={"ok": False, "requeued_stale_by_dashboard": True, "message": "Stale processing durch Dashboard-Admin erneut geöffnet."},
        )
    suffix = urllib.parse.urlencode({"msg": msg})
    return RedirectResponse(f"/ec-queue?{suffix}", status_code=303)


@app.post("/admin/member/{user_id}/save")
async def admin_member_save(user_id: int, request: Request, _: bool = Depends(_admin_auth)):
    raw = (await request.body()).decode("utf-8", errors="replace")
    form = urllib.parse.parse_qs(raw, keep_blank_values=True)
    status = (form.get("status") or ["ok"])[0]
    note = (form.get("note") or [""])[0]
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    _save_member_admin_state(guild_id, int(user_id), status, note, _current_user(request) or {})
    return RedirectResponse(f"/member/{int(user_id)}#leitung", status_code=303)


@app.post("/admin/member/{user_id}/clear")
async def admin_member_clear(user_id: int, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    _delete_member_admin_state(guild_id, int(user_id), _current_user(request) or {})
    return RedirectResponse(f"/member/{int(user_id)}#leitung", status_code=303)


@app.get("/api/admin/member-states")
def api_admin_member_states(_: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    return JSONResponse({"ok": True, "guild_id": guild_id, "states": _all_member_admin_states(guild_id), "actions": _admin_action_log(guild_id, 200)})


@app.get("/settings", response_class=HTMLResponse)
def settings_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_settings_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/audit", response_class=HTMLResponse)
def audit_page(q: str = "", action: str = "", actor: str = "", _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_audit_dashboard(_snapshot_payload(), q=q, action=action, actor=actor))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/system", response_class=HTMLResponse)
def system_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_system_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/api/settings")
def api_settings(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "settings": snap.get("settings") or {}})


@app.get("/api/audit")
def api_audit(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "audit": snap.get("audit") or {}})


@app.get("/api/system")
def api_system(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "guild": snap.get("guild") or {}, "storage": snap.get("storage") or {}, "source_health": snap.get("source_health") or {}})

@app.get("/overview", response_class=HTMLResponse)
def overview(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/", response_class=HTMLResponse)
def index(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_leadership_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


# ---------------------------------------------------------------------------
# Ebene 3 / Schritt 2: Attendance Review im Dashboard
# ---------------------------------------------------------------------------
# Sicherer Zwischenschritt: Admins können auf Basis von Anmeldung + Voice einen
# Anwesenheits-Review speichern. Es wird noch KEIN EC gebucht und KEINE Bot-JSON
# wird verändert. Diese Daten liegen nur in der Dashboard-Postgres-Tabelle.


def _ensure_attendance_review_tables() -> None:
    if not _database_url():
        return
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_event_attendance_review (
                    guild_id BIGINT NOT NULL,
                    event_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_by_id TEXT,
                    updated_by_name TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, event_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_admin_action_log (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    actor_id TEXT,
                    actor_name TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_ec_award_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    full_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    partial_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    actor_name TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    processed_at TIMESTAMPTZ,
                    result_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_ec_award_requests_lookup
                ON dashboard_ec_award_requests (guild_id, event_id, status, requested_at DESC)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _attendance_review_load(guild_id: int, event_id: str) -> dict[str, Any]:
    if not _database_url() or not guild_id or not event_id:
        return {}
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT guild_id, event_id, status, payload_json, updated_by_id, updated_by_name, updated_at
                FROM dashboard_event_attendance_review
                WHERE guild_id = %s AND event_id = %s
                """,
                (guild_id, str(event_id)),
            )
            row = cur.fetchone()
            if not row:
                return {}
            out = dict(row)
            try:
                out["payload"] = json.loads(out.get("payload_json") or "{}")
            except Exception:
                out["payload"] = {}
            return out
    finally:
        conn.close()


def _attendance_review_save(guild_id: int, event_id: str, payload: dict[str, Any], actor: dict[str, Any], status: str = "draft") -> None:
    if not _database_url() or not guild_id or not event_id:
        raise RuntimeError("DATABASE_URL/Guild/Event fehlt")
    status = str(status or "draft").strip().lower()
    if status not in {"draft", "reviewed", "locked", "closed", "archived"}:
        status = "draft"
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("user_id") or "")
    raw = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_event_attendance_review
                    (guild_id, event_id, status, payload_json, updated_by_id, updated_by_name, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (guild_id, event_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    payload_json = EXCLUDED.payload_json,
                    updated_by_id = EXCLUDED.updated_by_id,
                    updated_by_name = EXCLUDED.updated_by_name,
                    updated_at = NOW()
                """,
                (guild_id, str(event_id), status, raw, actor_id, actor_name),
            )
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (guild_id, "attendance_review_save", "event", str(event_id), actor_id, actor_name, json.dumps({"status": status, "items": len((payload or {}).get("items") or [])}, ensure_ascii=False)),
            )
        conn.commit()
    finally:
        conn.close()


def _attendance_status_label(value: Any) -> str:
    v = str(value or "open").strip().lower()
    return {
        # Spieler-Zeilen
        "present": "✅ War da",
        "partial": "🟡 Teilweise / prüfen",
        "absent": "❌ Nicht da",
        "ignore": "⚪ Ignorieren",
        "open": "— offen",
        # Review-Status
        "draft": "📝 Entwurf",
        "reviewed": "✅ Review gespeichert",
        "locked": "🔒 Freigegeben",
        "closed": "📦 Abgeschlossen",
        "archived": "📚 Archiviert",
    }.get(v, "— offen")


def _attendance_candidate_map(snap: dict[str, Any], event: dict[str, Any]) -> dict[int, dict[str, Any]]:
    names = _profile_name_map(snap)
    voice = _voice_event_analysis(snap, event)
    candidates: dict[int, dict[str, Any]] = {}

    def add(uid: int, name: str = "", signup: str = "", default_status: str = "open", source: str = "") -> None:
        if not uid:
            return
        entry = candidates.setdefault(uid, {"user_id": uid, "display_name": names.get(uid, name or f"User {uid}"), "signup": "", "source": set(), "voice_minutes": 0.0, "voice_sessions": 0, "suggested_status": "open"})
        if name and str(entry.get("display_name", "")).startswith("User "):
            entry["display_name"] = name
        if signup and not entry.get("signup"):
            entry["signup"] = signup
        if source:
            entry["source"].add(source)
        if default_status != "open" and entry.get("suggested_status") in {"open", "absent"}:
            entry["suggested_status"] = default_status

    parts = event.get("participants") or {}
    for group in parts.get("yes") or []:
        if not isinstance(group, dict):
            continue
        role = str(group.get("role") or "Zusage")
        for p in group.get("participants") or []:
            if isinstance(p, dict):
                add(_user_id(p.get("user_id")), str(p.get("display_name") or ""), role, "present", "Zusage")
    for key, label, status in (("maybe", "Vielleicht", "partial"), ("no", "Abgemeldet", "absent")):
        for p in parts.get(key) or []:
            if isinstance(p, dict):
                add(_user_id(p.get("user_id")), str(p.get("display_name") or ""), label, status, label)

    for r in voice.get("voice_by_user") or []:
        if not isinstance(r, dict):
            continue
        uid = _user_id(r.get("user_id"))
        add(uid, str(r.get("display_name") or ""), "", "partial", "Voice")
        if uid in candidates:
            candidates[uid]["voice_minutes"] = _num(r.get("minutes"), 0)
            candidates[uid]["voice_sessions"] = int(_num(r.get("sessions"), 0))
            # Dashboard-Vorschlag bewusst konservativ: mit Voice und Zusage = war da,
            # Voice ohne Anmeldung = teilweise prüfen.
            if candidates[uid].get("signup") and candidates[uid].get("signup") not in {"Abgemeldet"}:
                candidates[uid]["suggested_status"] = "present"
            elif _num(r.get("minutes"), 0) >= 20:
                candidates[uid]["suggested_status"] = "partial"

    for uid, entry in candidates.items():
        if isinstance(entry.get("source"), set):
            entry["source"] = ", ".join(sorted(entry["source"]))
    return candidates


def _attendance_review_payload_from_event(snap: dict[str, Any], event: dict[str, Any], mode: str = "voice") -> dict[str, Any]:
    candidates = _attendance_candidate_map(snap, event)
    items = []
    for uid, c in sorted(candidates.items(), key=lambda kv: str(kv[1].get("display_name") or kv[0]).lower()):
        status = c.get("suggested_status") or "open"
        if mode == "signup":
            signup = str(c.get("signup") or "")
            if signup == "Abgemeldet":
                status = "absent"
            elif signup == "Vielleicht":
                status = "partial"
            elif signup:
                status = "present"
        items.append({
            "user_id": uid,
            "display_name": c.get("display_name"),
            "signup": c.get("signup") or "—",
            "voice_minutes": c.get("voice_minutes") or 0,
            "voice_sessions": c.get("voice_sessions") or 0,
            "source": c.get("source") or "—",
            "status": status,
            "note": "",
        })
    return {
        "event_id": str(event.get("event_id") or event.get("id") or ""),
        "event_title": event.get("title") or "Event",
        "event_when": event.get("when_iso"),
        "mode": mode,
        "items": items,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Attendance-Statistik / Review-Historie
# ---------------------------------------------------------------------------
# Read-only Auswertung der gespeicherten Dashboard-Reviews. Keine EC-Buchung,
# keine Bot-JSON-Schreiberei. Dient nur der Leitungskontrolle.


def _attendance_all_reviews(guild_id: int, limit: int = 500) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id:
        return []
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT guild_id, event_id, status, payload_json, updated_by_id, updated_by_name, updated_at
                FROM dashboard_event_attendance_review
                WHERE guild_id = %s
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (int(guild_id), int(limit)),
            )
            rows = []
            for r in (cur.fetchall() or []):
                row = dict(r)
                try:
                    payload = json.loads(row.get("payload_json") or "{}")
                except Exception:
                    payload = {}
                row["payload"] = payload if isinstance(payload, dict) else {}
                rows.append(row)
            return rows
    finally:
        conn.close()


def _norm_attendance_status(value: Any) -> str:
    v = str(value or "open").strip().lower()
    if v in {"present", "partial", "absent", "ignore"}:
        return v
    return "open"


def _attendance_rate(present: int, partial: int, absent: int) -> str:
    total = int(present) + int(partial) + int(absent)
    if total <= 0:
        return "—"
    # Teilweise zählt bewusst halb. Das ist nur ein Leitungswert, keine EC-Regel.
    rate = ((int(present) + int(partial) * 0.5) / total) * 100.0
    return f"{rate:.0f} %"


def _attendance_stats_payload(data: dict[str, Any]) -> dict[str, Any]:
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    reviews = _attendance_all_reviews(guild_id)
    event_map: dict[str, dict[str, Any]] = {}
    for ev in ((snap.get("events") or {}).get("items") or []):
        if isinstance(ev, dict):
            eid = str(ev.get("event_id") or ev.get("id") or "")
            if eid:
                event_map[eid] = ev

    players: dict[int, dict[str, Any]] = {}
    event_rows: list[dict[str, Any]] = []
    total_lines = 0
    open_lines = 0
    locked_count = 0
    reviewed_count = 0
    draft_count = 0
    closed_count = 0

    for rev in reviews:
        eid = str(rev.get("event_id") or "")
        payload = rev.get("payload") if isinstance(rev.get("payload"), dict) else {}
        ev = event_map.get(eid) or {}
        items = [x for x in (payload.get("items") or []) if isinstance(x, dict)]
        counts = {"present": 0, "partial": 0, "absent": 0, "ignore": 0, "open": 0}
        status_review = str(rev.get("status") or "draft").lower()
        if status_review == "locked":
            locked_count += 1
        elif status_review == "reviewed":
            reviewed_count += 1
        elif status_review in {"closed", "archived"}:
            closed_count += 1
        else:
            draft_count += 1

        for item in items:
            total_lines += 1
            uid = _user_id(item.get("user_id"))
            st = _norm_attendance_status(item.get("status"))
            counts[st] = counts.get(st, 0) + 1
            if st == "open":
                open_lines += 1
            if not uid:
                continue
            p = players.setdefault(uid, {
                "user_id": uid,
                "display_name": str(item.get("display_name") or f"User {uid}"),
                "reviews": 0,
                "present": 0,
                "partial": 0,
                "absent": 0,
                "ignore": 0,
                "open": 0,
                "voice_minutes": 0.0,
                "last_event_id": "",
                "last_event_title": "",
                "last_status": "",
                "last_updated_at": "",
                "notes": 0,
            })
            name = str(item.get("display_name") or "")
            if name and str(p.get("display_name") or "").startswith("User "):
                p["display_name"] = name
            p["reviews"] += 1
            p[st] = int(p.get(st, 0) or 0) + 1
            p["voice_minutes"] = float(p.get("voice_minutes", 0) or 0) + _num(item.get("voice_minutes"), 0)
            if str(item.get("note") or "").strip():
                p["notes"] += 1
            # Reviews kommen sortiert DESC, erster Treffer je Spieler ist der aktuellste.
            if not p.get("last_updated_at"):
                p["last_event_id"] = eid
                p["last_event_title"] = str(payload.get("event_title") or ev.get("title") or eid)
                p["last_status"] = st
                p["last_updated_at"] = str(rev.get("updated_at") or "")

        event_title = str(payload.get("event_title") or ev.get("title") or eid or "Event")
        event_when = payload.get("event_when") or ev.get("when_iso")
        queue = _event_ec_queue_status(guild_id, eid) if eid else {}
        queue_latest = queue.get("latest") if isinstance(queue, dict) else {}
        queue_status_value = str((queue_latest or {}).get("status") or "") or ("pending" if (queue or {}).get("has_active") else "done" if (queue or {}).get("has_done") else "—")
        event_rows.append({
            "event_id": eid,
            "event_title": event_title,
            "event_when": event_when,
            "review_status": status_review,
            "updated_at": rev.get("updated_at"),
            "updated_by": rev.get("updated_by_name") or rev.get("updated_by_id") or "—",
            "rows": len(items),
            "present": counts.get("present", 0),
            "partial": counts.get("partial", 0),
            "absent": counts.get("absent", 0),
            "ignore": counts.get("ignore", 0),
            "open": counts.get("open", 0),
            "rate": _attendance_rate(counts.get("present", 0), counts.get("partial", 0), counts.get("absent", 0)),
            "queue_status": queue_status_value,
        })

    player_rows = []
    for p in players.values():
        p = dict(p)
        p["rate"] = _attendance_rate(p.get("present", 0), p.get("partial", 0), p.get("absent", 0))
        p["voice_hours"] = float(p.get("voice_minutes", 0) or 0) / 60.0
        player_rows.append(p)
    player_rows.sort(key=lambda x: (-int(x.get("present", 0) or 0), str(x.get("display_name") or "").lower()))

    problem_rows = []
    for p in player_rows:
        notes = []
        if int(p.get("open", 0) or 0) > 0:
            notes.append(f"{int(p.get('open', 0))} offen")
        if int(p.get("absent", 0) or 0) >= 2:
            notes.append(f"{int(p.get('absent', 0))}× nicht da")
        present = int(p.get("present", 0) or 0)
        partial = int(p.get("partial", 0) or 0)
        absent = int(p.get("absent", 0) or 0)
        denom = present + partial + absent
        if denom >= 3 and present == 0:
            notes.append("keine bestätigte Teilnahme")
        if notes:
            row = dict(p)
            row["hint"] = ", ".join(notes)
            problem_rows.append(row)
    problem_rows = problem_rows[:40]

    return {
        "guild_id": guild_id,
        "snapshot_at": data.get("published_at"),
        "review_count": len(reviews),
        "locked_count": locked_count,
        "reviewed_count": reviewed_count,
        "draft_count": draft_count,
        "closed_count": closed_count,
        "total_lines": total_lines,
        "open_lines": open_lines,
        "player_count": len(player_rows),
        "event_rows": event_rows,
        "player_rows": player_rows,
        "problem_rows": problem_rows,
    }


def _render_attendance_stats_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Anwesenheit-Stats · Ebo Dashboard", f"<section class='panel'><h1>📊 Anwesenheit-Stats</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    payload = _attendance_stats_payload(data)
    cards = "".join([
        _card("Reviews", payload.get("review_count", 0), f"locked: {payload.get('locked_count', 0)} · reviewed: {payload.get('reviewed_count', 0)}"),
        _card("Review-Zeilen", payload.get("total_lines", 0), f"offen: {payload.get('open_lines', 0)}"),
        _card("Spieler", payload.get("player_count", 0), "in gespeicherten Reviews"),
        _card("Drafts", payload.get("draft_count", 0), "noch nicht abgeschlossen"),
        _card("Abgeschlossen", payload.get("closed_count", 0), "im Archiv"),
    ])

    event_table = []
    for ev in payload.get("event_rows") or []:
        event_table.append([
            _raw(f'<a class="link" href="/attendance/{_e(str(ev.get("event_id") or ""))}">{_e(ev.get("event_title") or ev.get("event_id") or "Event")}</a>'),
            _dt(ev.get("event_when")),
            ev.get("review_status"),
            ev.get("rows", 0),
            ev.get("present", 0),
            ev.get("partial", 0),
            ev.get("absent", 0),
            ev.get("open", 0),
            ev.get("rate"),
            ev.get("queue_status"),
            _dt(ev.get("updated_at")),
        ])

    player_table = []
    for p in payload.get("player_rows") or []:
        uid = _user_id(p.get("user_id"))
        player_table.append([
            _member_link(uid, p.get("display_name")),
            p.get("reviews", 0),
            p.get("present", 0),
            p.get("partial", 0),
            p.get("absent", 0),
            p.get("ignore", 0),
            p.get("open", 0),
            p.get("rate"),
            f"{_num(p.get('voice_minutes'), 0):.0f} min",
            _attendance_status_label(p.get("last_status")),
            _event_link(p.get("last_event_id"), p.get("last_event_title") or p.get("last_event_id")),
        ])

    problem_table = []
    for p in payload.get("problem_rows") or []:
        uid = _user_id(p.get("user_id"))
        problem_table.append([
            _member_link(uid, p.get("display_name")),
            p.get("hint"),
            p.get("reviews", 0),
            p.get("present", 0),
            p.get("partial", 0),
            p.get("absent", 0),
            p.get("open", 0),
            p.get("rate"),
        ])

    body = f"""
    <nav class="topnav"><a href="/">Kommando</a><a href="/attendance">Anwesenheit</a><a href="/attendance-archive">Archiv</a><a href="/ec-queue">EC-Queue</a><a href="/planning">Planung</a><a href="/voice">Voice</a><a href="/export/attendance_stats.csv">CSV</a><a href="/api/attendance-stats">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Read-only · gespeicherte Attendance Reviews</div>
        <h1>📊 Anwesenheit-Stats</h1>
        <p>Auswertung aller gespeicherten Dashboard-Reviews. Keine EC-Buchung, keine Bot-JSON-Änderung.</p>
        <p class="muted">Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/export/attendance_stats.csv">CSV Export</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>⚠️ Prüfen</h2><p class="muted">Nur Leitungs-Hinweise aus gespeicherten Reviews. Keine automatische Bewertung.</p>{_table(['Spieler','Hinweis','Reviews','War da','Teilweise','Nicht da','Offen','Quote'], problem_table, placeholder='Hinweise durchsuchen…')}</section>
    <section class="panel"><h2>👥 Spieler-Statistik</h2>{_table(['Spieler','Reviews','War da','Teilweise','Nicht da','Ignoriert','Offen','Quote','Voice','Letzter Status','Letztes Event'], player_table, placeholder='Spieler durchsuchen…')}</section>
    <section class="panel"><h2>📅 Event-Reviews</h2>{_table(['Event','Zeit','Review','Zeilen','War da','Teilweise','Nicht da','Offen','Quote','Queue','Geändert'], event_table, placeholder='Events durchsuchen…')}</section>
    """
    return _html_shell("Anwesenheit-Stats · Ebo Dashboard", body)


def _render_attendance_list(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Anwesenheit · Ebo Dashboard", f"<section class='panel'><h1>📝 Anwesenheit</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    events = _attendance_events_with_review_fallbacks(snap, guild_id)
    rows = []
    for ev in events:
        eid = str(ev.get("event_id") or ev.get("id") or "")
        voice = _voice_event_analysis(snap, ev)
        review = _attendance_review_load(guild_id, eid) if eid else {}
        payload = review.get("payload") or {}
        items = payload.get("items") or []
        queue_badge = _event_ec_queue_badge(guild_id, eid) if eid else _raw("<span class='pill'>—</span>")
        rows.append([
            _raw(f'<a class="link" href="/attendance/{_e(eid)}">{_e(ev.get("title") or eid)}</a>' + (" <span class='pill'>aus Review</span>" if ev.get("_attendance_review_only") else "")),
            _dt(ev.get("when_iso")),
            ev.get("participant_count", 0),
            "ja" if ev.get("voice_enabled") else "nein",
            voice.get("voice_user_count", 0),
            _attendance_status_label(review.get("status") or ("reviewed" if items else "open")),
            len(items),
            queue_badge,
            _pending_ec_check_label(ev),
        ])
    body = f"""
    <nav class="topnav"><a href="/">Kommando</a><a href="/planning">Planung</a><a href="/attendance-stats">Anwesenheit-Stats</a><a href="/attendance-archive">Archiv</a><a href="/voice">Voice</a><a href="/ec-queue">EC-Queue</a><a href="/admin">Leitung</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Ebene 3 · sichere Admin-Aktion</div>
        <h1>📝 Anwesenheits-Review</h1>
        <p>Hier kann die Leitung Anmeldung und Voice-Zeit vergleichen und einen Review speichern. Es wird noch kein EC gebucht.</p>
        <p class="muted">Snapshot: {_e(_dt(data.get('published_at')))} · Alte Events mit gespeichertem Review bleiben sichtbar, solange EC noch nicht als erledigt erkannt wurde.</p>
      </div>
      <a class="btn" href="/planning">Planung</a>
    </section>
    <section class="panel">
      <h2>📅 Events</h2>
      {_table(['Event','Zeit','Anmeldungen','Voice','Voice-User','Review','Review-Zeilen','EC-Queue','EC-Check'], rows, placeholder='Events durchsuchen…')}
    </section>
    """
    return _html_shell("Anwesenheit · Ebo Dashboard", body)


def _attendance_review_control_panel(guild_id: int, event_id: str, review: dict[str, Any]) -> str:
    status = str((review or {}).get("status") or "draft").strip().lower()
    queue = _event_ec_queue_status(guild_id, event_id) if guild_id and event_id else {}
    latest = queue.get("latest") or {}
    queue_label = _ec_award_status_label(str(latest.get("status") or "")) if latest else "keine Anfrage"
    if status in {"closed", "archived"}:
        return f"""
        <section class="panel">
          <h2>📦 Review abgeschlossen</h2>
          <p class="muted">Dieser Review ist abgeschlossen und wird nicht mehr in der normalen Anwesenheitsliste angezeigt. Er bleibt in Stats/Archiv erhalten.</p>
          <p>Queue: <strong>{_e(queue_label)}</strong></p>
          <form method="post" action="/admin/attendance/{_e(event_id)}/reopen" onsubmit="return confirm('Review wieder in Anwesenheit öffnen?');">
            <button class="btn" type="submit">🔓 Wieder öffnen</button>
          </form>
        </section>
        """
    return f"""
    <section class="panel">
      <h2>📦 Abschlusssteuerung</h2>
      <p class="muted">Solange dieser Review offen ist und EC noch nicht erledigt wurde, bleibt er in Anwesenheit/Homepage sichtbar. Nach Abschluss landet er nur noch in Stats/Archiv.</p>
      <p>Aktueller Review-Status: <strong>{_e(_attendance_status_label(status))}</strong> · Queue: <strong>{_e(queue_label)}</strong></p>
      <div style="display:flex;gap:10px;flex-wrap:wrap">
        <form method="post" action="/admin/attendance/{_e(event_id)}/close" onsubmit="return confirm('Review abschließen und aus der normalen Anwesenheitsliste ausblenden?');">
          <button class="btn" type="submit">✅ Review abschließen</button>
        </form>
        <a class="btn" href="/attendance-archive">📚 Archiv öffnen</a>
      </div>
    </section>
    """


def _render_attendance_event(data: dict[str, Any], event_id: str, saved: bool = False) -> str:
    if not data.get("ok"):
        return _html_shell("Anwesenheit · Ebo Dashboard", f"<section class='panel'><h1>📝 Anwesenheit</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    event = _event_by_id(snap, event_id) or _event_stub_from_attendance_review(guild_id, event_id)
    if not event:
        return _html_shell("Event nicht gefunden", "<section class='panel'><h1>❌ Event nicht gefunden</h1><p>Dieses Event ist nicht im aktuellen Snapshot und es gibt keinen gespeicherten Review-Fallback.</p><p><a class='btn' href='/attendance'>Zurück</a></p></section>")
    review = _attendance_review_load(guild_id, event_id)
    payload = review.get("payload") or {}
    if not payload.get("items"):
        payload = _attendance_review_payload_from_event(snap, event, mode="voice")
    items = payload.get("items") or []
    present = sum(1 for i in items if str(i.get("status")) == "present")
    partial = sum(1 for i in items if str(i.get("status")) == "partial")
    absent = sum(1 for i in items if str(i.get("status")) == "absent")
    ignored = sum(1 for i in items if str(i.get("status")) == "ignore")
    open_count = sum(1 for i in items if str(i.get("status") or "open") not in {"present", "partial", "absent", "ignore"})
    cards = "".join([
        _card("War da", present, "Review-Status"),
        _card("Teilweise", partial, "prüfen/korrigieren"),
        _card("Nicht da", absent, "Review-Status"),
        _card("Ignoriert", ignored, "nicht EC-relevant"),
        _card("Offen", open_count, "noch nicht bewertet"),
    ])
    rows_html = []
    for i in items:
        uid = _user_id(i.get("user_id"))
        status = str(i.get("status") or "open")
        note = str(i.get("note") or "")
        options = []
        for val, label in [("present", "War da"), ("partial", "Teilweise"), ("absent", "Nicht da"), ("ignore", "Ignorieren")]:
            sel = " selected" if status == val else ""
            options.append(f'<option value="{val}"{sel}>{label}</option>')
        rows_html.append(f"""
        <tr>
          <td>{_member_link(uid, i.get('display_name')).get('__html__')}</td>
          <td>{_e(i.get('signup') or '—')}</td>
          <td>{_e(i.get('voice_minutes') or 0)} min · {_e(i.get('voice_sessions') or 0)}x</td>
          <td>{_e(i.get('source') or '—')}</td>
          <td>
            <input type="hidden" name="user_id" value="{uid}">
            <input type="hidden" name="display_name_{uid}" value="{_e(i.get('display_name') or '')}">
            <input type="hidden" name="signup_{uid}" value="{_e(i.get('signup') or '')}">
            <input type="hidden" name="voice_minutes_{uid}" value="{_e(i.get('voice_minutes') or 0)}">
            <input type="hidden" name="voice_sessions_{uid}" value="{_e(i.get('voice_sessions') or 0)}">
            <input type="hidden" name="source_{uid}" value="{_e(i.get('source') or '')}">
            <select name="status_{uid}">{''.join(options)}</select>
          </td>
          <td><input name="note_{uid}" value="{_e(note)}" placeholder="Notiz optional" style="width:100%;min-width:180px"></td>
        </tr>
        """)
    saved_note = "<div class='warn'>✅ Review gespeichert. EC wurde nicht automatisch gebucht.</div>" if saved else ""
    updated = ""
    if review:
        updated = f"<p class='muted'>Letzte Speicherung: {_e(_dt(review.get('updated_at')))} durch {_e(review.get('updated_by_name') or '—')} · Status: {_e(review.get('status') or 'draft')}</p>"
    body = f"""
    <nav class="topnav"><a href="/attendance">← Anwesenheit</a><a href="/attendance-stats">Stats</a><a href="/attendance-archive">Archiv</a><a href="/event/{_e(event_id)}">Eventdetails</a><a href="/attendance/{_e(event_id)}/ec-preview">EC-Vorschau</a><a href="/attendance/{_e(event_id)}/report">Abschlussbericht</a><a href="#event-ec-queue">EC-Queue</a><a href="/voice">Voice</a><a href="/ec">EC</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Anwesenheits-Review · keine EC-Buchung</div>
        <h1>📝 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Event-ID: {_e(event_id)} · Zeit: {_e(_dt(event.get('when_iso')))} · Voice: {_e(event.get('voice_channel_id') or event.get('voice_last_channel_id') or 'kein Voice')}</p>
        {updated}
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap"><form method="post" action="/admin/attendance/{_e(event_id)}/voice-suggest"><button class="btn" type="submit">🎙️ Voice-Vorschlag neu laden</button></form><a class="btn" href="/attendance/{_e(event_id)}/ec-preview">🪙 EC-Vorschau</a><a class="btn" href="/attendance/{_e(event_id)}/report">📋 Abschlussbericht</a></div>
    </section>
    {saved_note}
    {_raw(_attendance_review_control_panel(guild_id, str(event_id), review))}
    <section class="grid">{cards}</section>
    <section class="panel">
      <h2>⚙️ Schnellaktionen</h2>
      <p class="muted">Diese Aktionen ändern nur den gespeicherten Dashboard-Review. Es wird dadurch noch kein EC gebucht.</p>
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
        <form method="post" action="/admin/attendance/{_e(event_id)}/bulk" onsubmit="return confirm('Alle Review-Zeilen auf War da setzen?');">
          <input type="hidden" name="action" value="confirm_all">
          <button class="btn" type="submit">✅ Alle auf War da</button>
        </form>
        <form method="post" action="/admin/attendance/{_e(event_id)}/bulk" onsubmit="return confirm('Alle Spieler ohne Voice-Zeit auf Nicht da setzen?');">
          <input type="hidden" name="action" value="no_voice_absent">
          <button class="btn" type="submit">❌ Ohne Voice = Nicht da</button>
        </form>
        <form method="post" action="/admin/attendance/{_e(event_id)}/bulk" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
          <input type="hidden" name="action" value="voice_auto">
          <label class="muted">War da ab <input name="full_min" type="number" value="60" min="0" max="600" style="width:74px"> Min</label>
          <label class="muted">Teilweise ab <input name="partial_min" type="number" value="15" min="0" max="600" style="width:74px"> Min</label>
          <button class="btn" type="submit">🎙️ Voice-Automatik anwenden</button>
        </form>
        <form method="post" action="/admin/attendance/{_e(event_id)}/bulk" onsubmit="return confirm('Alle Notizen in diesem Review leeren?');">
          <input type="hidden" name="action" value="clear_notes">
          <button class="btn" type="submit">🧹 Notizen leeren</button>
        </form>
      </div>
    </section>
    <section class="panel">
      <h2>👥 Review-Liste</h2>
      <p class="muted">Diese Speicherung ist ein Dashboard-Review für die Leitung. Sie verändert noch keine EC-Konten und nicht die Discord-Anwesenheitskarte.</p>
      <form method="post" action="/admin/attendance/{_e(event_id)}/save">
        <div class='table-wrap'><table><thead><tr><th>Spieler</th><th>Anmeldung</th><th>Voice</th><th>Quelle</th><th>Status</th><th>Notiz</th></tr></thead><tbody>{''.join(rows_html)}</tbody></table></div>
        <p style="margin-top:14px"><button class="btn" type="submit">Review speichern</button></p>
      </form>
    </section>
    {_raw(_event_ec_queue_panel(guild_id, str(event_id)))}
    """
    return _html_shell(f"Anwesenheit · {event.get('title') or event_id}", body)



def _attendance_review_counts(payload: dict[str, Any]) -> dict[str, int]:
    items = [x for x in ((payload or {}).get("items") or []) if isinstance(x, dict)]
    counts = {"present": 0, "partial": 0, "absent": 0, "ignore": 0, "open": 0}
    for item in items:
        st = _norm_attendance_status(item.get("status"))
        counts[st] = counts.get(st, 0) + 1
    counts["rows"] = len(items)
    return counts


def _render_attendance_archive(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Attendance-Archiv · Ebo Dashboard", f"<section class='panel'><h1>📦 Attendance-Archiv</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    reviews = _attendance_all_reviews(guild_id, limit=500) if guild_id else []
    rows: list[list[Any]] = []
    open_count = 0
    closed_count = 0
    queue_pending = 0
    for rev in reviews:
        eid = str(rev.get("event_id") or "")
        payload = rev.get("payload") if isinstance(rev.get("payload"), dict) else {}
        counts = _attendance_review_counts(payload)
        status = str(rev.get("status") or "draft").strip().lower()
        still_needs = _attendance_review_still_needs_ec(snap, guild_id, rev)
        if still_needs:
            open_count += 1
        if status in {"closed", "archived"}:
            closed_count += 1
        queue = _event_ec_queue_status(guild_id, eid) if eid else {}
        latest = queue.get("latest") or {}
        qstatus = str(latest.get("status") or "")
        if qstatus in {"pending", "processing"}:
            queue_pending += 1
        action = _raw(f'<a class="btn mini-btn" href="/attendance/{_e(eid)}">Öffnen</a>')
        if status in {"closed", "archived"}:
            action = _raw(f"<form method=\"post\" action=\"/admin/attendance/{_e(eid)}/reopen\" style=\"display:inline\" onsubmit=\"return confirm('Review wieder öffnen?');\"><button class=\"btn mini-btn\" type=\"submit\">Wieder öffnen</button></form>")
        rows.append([
            _raw(f'<a class="link" href="/attendance/{_e(eid)}">{_e(payload.get("event_title") or eid)}</a>' + (" <span class='pill'>offen</span>" if still_needs else "")),
            _dt(payload.get("event_when") or rev.get("updated_at")),
            _attendance_status_label(status),
            _ec_award_status_label(qstatus) if qstatus else "—",
            counts.get("rows", 0),
            counts.get("present", 0),
            counts.get("partial", 0),
            counts.get("absent", 0),
            counts.get("open", 0),
            _dt(rev.get("updated_at")),
            action,
        ])
    cards = "".join([
        _card("Reviews", len(reviews), "gespeichert"),
        _card("Offen", open_count, "braucht noch EC/Abschluss"),
        _card("Abgeschlossen", closed_count, "aus normaler Liste raus"),
        _card("Queue offen", queue_pending, "pending/processing"),
    ])
    body = f"""
    <nav class="topnav"><a href="/">Kommando</a><a href="/attendance">Anwesenheit</a><a href="/attendance-stats">Stats</a><a href="/ec-queue">EC-Queue</a><a href="/audit">Audit</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Attendance-Archiv · Steuerung</div>
        <h1>📦 Attendance-Archiv</h1>
        <p>Hier bleiben alte Reviews auffindbar. Offene Reviews können wieder geöffnet oder abgeschlossen werden.</p>
        <p class="muted">Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/attendance">Offene Anwesenheit</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel">
      <h2>📋 Alle gespeicherten Reviews</h2>
      {_table(['Event','Zeit','Review','Queue','Zeilen','War da','Teilweise','Nicht da','Offen','Geändert','Aktion'], rows, placeholder='Archiv durchsuchen…')}
    </section>
    """
    return _html_shell("Attendance-Archiv · Ebo Dashboard", body)


@app.get("/attendance-archive", response_class=HTMLResponse)
def attendance_archive_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_attendance_archive(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.post("/admin/attendance/{event_id}/close")
def admin_attendance_close(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id))
    review = _attendance_review_load(guild_id, str(event_id))
    review_payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not review_payload.get("items") and event:
        review_payload = _attendance_review_payload_from_event(snap, event, mode="voice")
    if not review_payload:
        raise HTTPException(status_code=404, detail="Kein Review zum Abschließen gefunden")
    actor = _current_user(request) or {}
    review_payload["closed_at"] = datetime.now(timezone.utc).isoformat()
    review_payload["closed_by"] = {"id": str(actor.get("user_id") or ""), "name": str(actor.get("username") or actor.get("user_id") or "Dashboard")}
    review_payload.setdefault("event_id", str(event_id))
    _attendance_review_save(guild_id, str(event_id), review_payload, actor, status="closed")
    return RedirectResponse("/attendance-archive", status_code=303)


@app.post("/admin/attendance/{event_id}/reopen")
def admin_attendance_reopen(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    review = _attendance_review_load(guild_id, str(event_id))
    review_payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not review_payload:
        raise HTTPException(status_code=404, detail="Kein Review zum Öffnen gefunden")
    actor = _current_user(request) or {}
    review_payload["reopened_at"] = datetime.now(timezone.utc).isoformat()
    review_payload["reopened_by"] = {"id": str(actor.get("user_id") or ""), "name": str(actor.get("username") or actor.get("user_id") or "Dashboard")}
    _attendance_review_save(guild_id, str(event_id), review_payload, actor, status="reviewed")
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}?saved=1", status_code=303)


@app.get("/attendance-stats", response_class=HTMLResponse)
def attendance_stats_dashboard(_: bool = Depends(_auth)):
    return _render_attendance_stats_dashboard(_snapshot_payload())


@app.get("/api/attendance-stats")
def api_attendance_stats(_: bool = Depends(_auth)):
    data = _snapshot_payload()
    if not data.get("ok"):
        return JSONResponse(data, status_code=404)
    return JSONResponse(_attendance_stats_payload(data))


@app.get("/export/attendance_stats.csv")
def export_attendance_stats_csv(_: bool = Depends(_auth)):
    data = _snapshot_payload()
    payload = _attendance_stats_payload(data) if data.get("ok") else {"player_rows": [], "event_rows": []}
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Spieler", "User-ID", "Reviews", "War da", "Teilweise", "Nicht da", "Ignoriert", "Offen", "Quote", "Voice-Minuten", "Letzter Status", "Letztes Event"])
    for p in payload.get("player_rows") or []:
        writer.writerow([
            p.get("display_name"),
            p.get("user_id"),
            p.get("reviews", 0),
            p.get("present", 0),
            p.get("partial", 0),
            p.get("absent", 0),
            p.get("ignore", 0),
            p.get("open", 0),
            p.get("rate"),
            f"{_num(p.get('voice_minutes'), 0):.1f}",
            p.get("last_status"),
            p.get("last_event_title") or p.get("last_event_id"),
        ])
    writer.writerow([])
    writer.writerow(["Event", "Event-ID", "Zeit", "Review", "Zeilen", "War da", "Teilweise", "Nicht da", "Ignoriert", "Offen", "Quote", "Queue", "Geändert"])
    for ev in payload.get("event_rows") or []:
        writer.writerow([
            ev.get("event_title"),
            ev.get("event_id"),
            ev.get("event_when"),
            ev.get("review_status"),
            ev.get("rows", 0),
            ev.get("present", 0),
            ev.get("partial", 0),
            ev.get("absent", 0),
            ev.get("ignore", 0),
            ev.get("open", 0),
            ev.get("rate"),
            ev.get("queue_status"),
            ev.get("updated_at"),
        ])
    return Response(out.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=attendance_stats.csv"})


@app.get("/attendance", response_class=HTMLResponse)
def attendance_dashboard(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_attendance_list(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/attendance/{event_id}", response_class=HTMLResponse)
def attendance_event_page(event_id: str, saved: int = 0, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_attendance_event(_snapshot_payload(), str(event_id), saved=bool(saved)))
    except Exception as exc:
        return HTMLResponse(_html_shell("Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.post("/admin/attendance/{event_id}/voice-suggest")
async def admin_attendance_voice_suggest(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    event = _event_by_id(snap, str(event_id))
    if not event:
        raise HTTPException(status_code=404, detail="Event nicht gefunden")
    guild_id = _safe_guild_id(payload)
    review_payload = _attendance_review_payload_from_event(snap, event, mode="voice")
    _attendance_review_save(guild_id, str(event_id), review_payload, _current_user(request) or {}, status="draft")
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}?saved=1", status_code=303)


@app.post("/admin/attendance/{event_id}/save")
async def admin_attendance_save(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    raw = (await request.body()).decode("utf-8", errors="replace")
    form = urllib.parse.parse_qs(raw, keep_blank_values=True)
    user_ids = [_user_id(x) for x in form.get("user_id", [])]
    items = []
    for uid in user_ids:
        if not uid:
            continue
        status = (form.get(f"status_{uid}") or ["open"])[0]
        if status not in {"present", "partial", "absent", "ignore"}:
            status = "open"
        items.append({
            "user_id": uid,
            "display_name": (form.get(f"display_name_{uid}") or [f"User {uid}"])[0],
            "signup": (form.get(f"signup_{uid}") or ["—"])[0],
            "voice_minutes": _num((form.get(f"voice_minutes_{uid}") or [0])[0], 0),
            "voice_sessions": int(_num((form.get(f"voice_sessions_{uid}") or [0])[0], 0)),
            "source": (form.get(f"source_{uid}") or ["—"])[0],
            "status": status,
            "note": (form.get(f"note_{uid}") or [""])[0][:800],
        })
    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    old_review = _attendance_review_load(guild_id, str(event_id))
    old_payload = old_review.get("payload") if isinstance(old_review.get("payload"), dict) else {}
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id)) or {}
    review_payload = {
        "event_id": str(event_id),
        "event_title": event.get("title") or old_payload.get("event_title") or str(event_id),
        "event_when": event.get("when_iso") or old_payload.get("event_when"),
        "mode": "manual_review",
        "items": items,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _attendance_review_save(guild_id, str(event_id), review_payload, _current_user(request) or {}, status="reviewed")
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}?saved=1", status_code=303)


@app.post("/admin/attendance/{event_id}/bulk")
async def admin_attendance_bulk(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    raw = (await request.body()).decode("utf-8", errors="replace")
    form = urllib.parse.parse_qs(raw, keep_blank_values=True)
    action = str((form.get("action") or [""])[0] or "").strip().lower()
    if action not in {"confirm_all", "no_voice_absent", "voice_auto", "clear_notes"}:
        raise HTTPException(status_code=400, detail="Unbekannte Schnellaktion")

    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id))
    if not event:
        raise HTTPException(status_code=404, detail="Event nicht gefunden und kein gespeicherter Review-Fallback vorhanden")

    review = _attendance_review_load(guild_id, str(event_id))
    review_payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not review_payload.get("items"):
        review_payload = _attendance_review_payload_from_event(snap, event, mode="voice")

    items = [dict(x) for x in (review_payload.get("items") or []) if isinstance(x, dict)]
    full_min = max(0.0, min(_num((form.get("full_min") or [60])[0], 60), 600.0))
    partial_min = max(0.0, min(_num((form.get("partial_min") or [15])[0], 15), 600.0))
    if partial_min > full_min:
        partial_min = full_min

    changed = 0
    for item in items:
        old_status = str(item.get("status") or "open")
        new_status = old_status
        if action == "confirm_all":
            new_status = "present"
        elif action == "no_voice_absent":
            if _num(item.get("voice_minutes"), 0) <= 0:
                new_status = "absent"
        elif action == "voice_auto":
            minutes = _num(item.get("voice_minutes"), 0)
            if minutes >= full_min:
                new_status = "present"
            elif minutes >= partial_min:
                new_status = "partial"
            else:
                new_status = "absent"
        elif action == "clear_notes":
            if str(item.get("note") or ""):
                item["note"] = ""
                changed += 1
            continue

        if new_status != old_status:
            item["status"] = new_status
            changed += 1

    review_payload["items"] = items
    review_payload["mode"] = f"bulk_{action}"
    review_payload["bulk_last_action"] = {
        "action": action,
        "changed": changed,
        "full_min": full_min if action == "voice_auto" else None,
        "partial_min": partial_min if action == "voice_auto" else None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    review_payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    _attendance_review_save(guild_id, str(event_id), review_payload, _current_user(request) or {}, status="reviewed")
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}?saved=1", status_code=303)


@app.get("/api/attendance/{event_id}")
def api_attendance_review(event_id: str, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    guild_id = _safe_guild_id(payload)
    return JSONResponse({"ok": True, "event_id": str(event_id), "review": _attendance_review_load(guild_id, str(event_id))})


# ---------------------------------------------------------------------------
# Ebene 3 / Schritt 3+4: EC-Vorschau + echte EC-Buchung über Bot-Queue
# ---------------------------------------------------------------------------
# Wichtig: Dashboard-Web und Discord-Bot laufen als getrennte Railway-Services.
# Darum schreibt das Dashboard NICHT direkt in Bot-JSON-Dateien.
# Stattdessen legt es eine geprüfte Buchungsanfrage in Postgres ab.
# Der Bot verarbeitet diese Anfrage und bucht dann in seinem echten EC-/DKP-System.



def _event_dkp_type(event: dict[str, Any]) -> str:
    for key in ("dkp_event_type", "event_type", "dkp_type", "ec_event_type"):
        value = str((event or {}).get(key) or "").strip()
        if value and value != "Nicht DKP-relevant":
            return value
    return "Dashboard Attendance"


def _snapshot_event_points(snap: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    settings = ((snap.get("settings") or {}).get("settings") or [])
    if isinstance(settings, list):
        for row in settings:
            if not isinstance(row, dict):
                continue
            if str(row.get("source") or "") != "dkp_cfg":
                continue
            key = str(row.get("key") or "")
            if ".event_points." not in key:
                continue
            event_type = key.rsplit(".event_points.", 1)[-1].strip()
            if not event_type:
                continue
            amount = _num(row.get("value"), 0)
            if amount > 0:
                out[event_type] = amount
    # Fallbacks aus dem aktuellen Bot-Default. Bleibt rein als UI-Vorschlag.
    out.setdefault("Gildenboss", 20.0)
    out.setdefault("HM Raid", 12.0)
    out.setdefault("NM Raid", 12.0)
    out.setdefault("Normal Raid", 12.0)
    out.setdefault("Übungsrun HM Raid", 15.0)
    out.setdefault("Übungsrun Trials", 15.0)
    out.setdefault("Segensstein PvP", 5.0)
    return out


def _event_award_state(snap: dict[str, Any], event_id: str) -> dict[str, Any]:
    txs = _ec_transactions(snap)
    recent = txs.get("items") or txs.get("recent") or []
    hits = []
    for tx in recent:
        if not isinstance(tx, dict):
            continue
        if str(tx.get("event_id") or "") != str(event_id):
            continue
        if str(tx.get("raw_type") or "") != "event_award":
            continue
        hits.append(tx)
    total = sum(_num(tx.get("amount"), 0) for tx in hits)
    return {"awarded": bool(hits), "count": len(hits), "total": total, "latest": hits[0] if hits else {}}


def _latest_ec_award_request(guild_id: int, event_id: str) -> dict[str, Any]:
    if not _database_url() or not guild_id or not event_id:
        return {}
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, event_id, event_type, status, full_ec, partial_ec,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s AND event_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT 1
                """,
                (int(guild_id), str(event_id)),
            )
            row = cur.fetchone()
            if not row:
                return {}
            out = dict(row)
            try:
                out["result"] = json.loads(out.get("result_json") or "{}")
            except Exception:
                out["result"] = {}
            return out
    finally:
        conn.close()


def _ec_award_requests_for_event(guild_id: int, event_id: str, limit: int = 12) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id or not event_id:
        return []
    try:
        _ensure_admin_tables()
    except Exception:
        return []
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, event_id, event_type, status, full_ec, partial_ec,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, result_json, payload_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s AND event_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), str(event_id), int(limit)),
            )
            rows: list[dict[str, Any]] = []
            for row in cur.fetchall() or []:
                out = dict(row)
                try:
                    out["result"] = json.loads(out.get("result_json") or "{}")
                except Exception:
                    out["result"] = {}
                try:
                    out["payload"] = json.loads(out.get("payload_json") or "{}")
                except Exception:
                    out["payload"] = {}
                rows.append(out)
            return rows
    finally:
        conn.close()


def _event_ec_queue_status(guild_id: int, event_id: str) -> dict[str, Any]:
    rows = _ec_award_requests_for_event(guild_id, event_id, limit=20)
    latest = rows[0] if rows else {}
    has_active = any(str(r.get("status") or "").lower() in {"pending", "processing"} for r in rows)
    has_done = any(str(r.get("status") or "").lower() == "done" for r in rows)
    has_problem = any(str(r.get("status") or "").lower() in {"failed", "rejected"} for r in rows)
    return {"rows": rows, "latest": latest, "has_active": has_active, "has_done": has_done, "has_problem": has_problem, "count": len(rows)}


def _event_ec_queue_badge(guild_id: int, event_id: str) -> dict[str, str]:
    latest = (_event_ec_queue_status(guild_id, event_id).get("latest") or {})
    if not latest:
        return _raw("<span class='pill'>—</span>")
    status = str(latest.get("status") or "").lower()
    css = "queue-badge"
    if status == "done":
        css += " ok"
    elif status in {"failed", "rejected"}:
        css += " bad"
    elif status in {"pending", "processing"}:
        css += " wait"
    label = _ec_award_status_label(status)
    rid = _short(latest.get("request_id") or "", 10)
    return _raw(f"<a class='{css}' href='/ec-queue' title='Request {_e(latest.get('request_id') or '')}'>{_e(label)}<small>{_e(rid)}</small></a>")


def _event_ec_queue_panel(guild_id: int, event_id: str) -> str:
    rows = _ec_award_requests_for_event(guild_id, event_id, limit=12)
    if not rows:
        return "<section class='panel'><h2>🌐 EC-Queue für dieses Event</h2><div class='empty'>Noch keine Dashboard-EC-Anfrage für dieses Event.</div></section>"
    table_rows: list[list[Any]] = []
    for r in rows:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        status = str(r.get("status") or "")
        total = result.get("total_ec") if result.get("total_ec") is not None else payload.get("total_ec")
        applied = result.get("applied_count") if result.get("applied_count") is not None else payload.get("recipient_count")
        skipped = result.get("skipped_count") if result.get("skipped_count") is not None else "—"
        detail = result.get("error") or r.get("request_id") or "—"
        if _ec_award_request_is_stale(r):
            detail = f"HÄNGT? {detail}"
        table_rows.append([_dt(r.get("requested_at")), _ec_award_status_label(status), r.get("event_type") or "—", _fmt_ec(total), applied, skipped, r.get("actor_name") or r.get("actor_id") or "—", _short(detail, 140)])
    return f"""
    <section class="panel" id="event-ec-queue">
      <h2>🌐 EC-Queue für dieses Event</h2>
      <p class="muted">Direkter Status der Dashboard-Buchungen für genau dieses Event. Retry/Abbrechen läuft über die Queue-Zentrale.</p>
      {_table(['Angefragt','Status','Typ','EC','Gebucht','Übersprungen','Admin','Details'], table_rows, placeholder='Event-Queue durchsuchen…')}
      <p><a class="btn" href="/ec-queue">EC-Queue öffnen</a></p>
    </section>
    """


def _active_ec_award_request(guild_id: int, event_id: str) -> dict[str, Any]:
    if not _database_url() or not guild_id or not event_id:
        return {}
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, request_id, guild_id, event_id, event_type, status, full_ec, partial_ec,
                       actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s AND event_id = %s AND status IN ('pending', 'processing', 'done')
                ORDER BY requested_at DESC, id DESC
                LIMIT 1
                """,
                (int(guild_id), str(event_id)),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def _enqueue_ec_award_request(guild_id: int, event_id: str, event_type: str, preview: dict[str, Any], actor: dict[str, Any]) -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt. Ohne Postgres kann das Dashboard keine Bot-Buchung anstoßen."}
    if not guild_id or not event_id:
        return {"ok": False, "error": "Guild/Event fehlt."}
    rows = [r for r in (preview.get("rows") or []) if isinstance(r, dict) and _num(r.get("ec_gain"), 0) > 0 and _user_id(r.get("user_id"))]
    if not rows:
        return {"ok": False, "error": "Keine Spieler mit EC-Gutschrift in der Vorschau."}
    existing = _active_ec_award_request(guild_id, event_id)
    if existing:
        return {"ok": False, "error": f"Für dieses Event gibt es bereits eine EC-Anfrage mit Status {existing.get('status')}. Keine Doppelbuchung."}
    request_id = f"dash-ec-{int(time.time())}-{secrets.token_hex(6)}"
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("user_id") or "Dashboard")
    payload = {
        "event_id": str(event_id),
        "event_type": str(event_type or "Dashboard Attendance"),
        "event_title": str((preview.get("event") or {}).get("title") or event_id),
        "full_ec": _num(preview.get("full_ec"), 0),
        "partial_ec": _num(preview.get("partial_ec"), 0),
        "total_ec": _num(preview.get("total_ec"), 0),
        "recipient_count": len(rows),
        "review_status": str(preview.get("review_status") or ""),
        "review_updated_at": str(preview.get("review_updated_at") or ""),
        "rows": rows,
        "requested_by": {"id": actor_id, "name": actor_name},
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source": "dashboard_attendance_review",
    }
    _ensure_attendance_review_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_ec_award_requests
                    (request_id, guild_id, event_id, event_type, status, full_ec, partial_ec, payload_json, actor_id, actor_name, requested_at)
                VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s, %s, %s, NOW())
                RETURNING id, request_id, status
                """,
                (
                    request_id,
                    int(guild_id),
                    str(event_id),
                    str(event_type or "Dashboard Attendance"),
                    float(preview.get("full_ec") or 0),
                    float(preview.get("partial_ec") or 0),
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    actor_id,
                    actor_name,
                ),
            )
            row = dict(cur.fetchone() or {})
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (int(guild_id), "ec_award_request_create", "event", str(event_id), actor_id, actor_name, json.dumps({"request_id": request_id, "event_type": event_type, "recipients": len(rows), "total_ec": preview.get("total_ec")}, ensure_ascii=False)),
            )
        conn.commit()
        return {"ok": True, "request": row, "request_id": request_id}
    finally:
        conn.close()

def _event_ec_defaults(snap: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """Versucht den EC-Wert aus Eventtyp/Snapshot/Eventfeldern zu erkennen.

    Der Wert bleibt in der UI überschreibbar. Echte Buchung läuft später über
    die Bot-Queue und wird dort erneut gegen Doppelbuchung geprüft.
    """
    event_type = _event_dkp_type(event)
    event_points = _snapshot_event_points(snap)
    full = float(event_points.get(event_type, 0.0) or 0.0)
    detected_from = "DKP-Konfig" if full > 0 else "manuell"

    if full <= 0:
        candidates: list[Any] = []
        for key in ("ec_value", "dkp_value", "points", "reward_points", "attendance_points", "full_ec", "ec_full", "dkp_points"):
            if isinstance(event, dict) and event.get(key) not in (None, ""):
                candidates.append(event.get(key))
        for val in candidates:
            n = _num(val, 0)
            if n > 0:
                full = n
                detected_from = "Eventdaten"
                break

    partial = 5.0 if full > 0 else 0.0
    # Ebolus-Regel aus dem Bot: Reserve/Teilweise bekommt fix 5 EC.
    return {"full_ec": full, "partial_ec": partial, "detected_from": detected_from, "event_type": event_type}


def _attendance_items_for_preview(snap: dict[str, Any], event: dict[str, Any], guild_id: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event_id = str(event.get("event_id") or "")
    review = _attendance_review_load(guild_id, event_id) if event_id else {}
    payload = review.get("payload") or {}
    if not payload.get("items"):
        payload = _attendance_review_payload_from_event(snap, event, mode="voice")
    items = [x for x in (payload.get("items") or []) if isinstance(x, dict)]
    return review, items


def _ec_gain_for_status(status: Any, full_ec: float, partial_ec: float) -> float:
    s = str(status or "").lower()
    if s == "present":
        return float(full_ec or 0)
    if s == "partial":
        return float(partial_ec or 0)
    return 0.0


def _attendance_ec_preview_payload(data: dict[str, Any], event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None) -> dict[str, Any]:
    if not data.get("ok"):
        return {"ok": False, "error": data.get("error") or "Kein Snapshot"}
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id))
    if not event:
        return {"ok": False, "error": "Event nicht gefunden und kein gespeicherter Attendance-Review vorhanden"}
    review, items = _attendance_items_for_preview(snap, event, guild_id)
    defaults = _event_ec_defaults(snap, event)
    event_type = str(defaults.get("event_type") or _event_dkp_type(event))
    award_state = _event_award_state(snap, str(event_id))
    latest_request = _latest_ec_award_request(guild_id, str(event_id)) if guild_id else {}
    fe = _num(full_ec if full_ec is not None else defaults.get("full_ec"), 0)
    pe = _num(partial_ec if partial_ec is not None else defaults.get("partial_ec"), 0)
    balances = _balance_map(snap)
    rows: list[dict[str, Any]] = []
    total = 0.0
    counts = {"present": 0, "partial": 0, "absent": 0, "ignore": 0, "open": 0}
    for item in items:
        uid = _user_id(item.get("user_id"))
        status = str(item.get("status") or "open").lower()
        if status not in counts:
            status = "open"
        counts[status] = counts.get(status, 0) + 1
        before = balances.get(uid, 0.0)
        gain = _ec_gain_for_status(status, fe, pe)
        total += gain
        rows.append({
            "user_id": uid,
            "display_name": str(item.get("display_name") or f"User {uid}"),
            "signup": str(item.get("signup") or "—"),
            "status": status,
            "status_label": _attendance_status_label(status),
            "voice_minutes": _num(item.get("voice_minutes"), 0),
            "voice_sessions": int(_num(item.get("voice_sessions"), 0)),
            "ec_before": before,
            "ec_gain": gain,
            "ec_after": before + gain,
            "note": str(item.get("note") or ""),
            "source": str(item.get("source") or ""),
        })
    rows.sort(key=lambda r: (0 if _num(r.get("ec_gain"), 0) > 0 else 1, str(r.get("display_name") or "").lower()))
    recipients = sum(1 for r in rows if _num(r.get("ec_gain"), 0) > 0)
    return {
        "ok": True,
        "event": event,
        "event_id": str(event_id),
        "review_status": review.get("status") or ("draft" if rows else "open"),
        "review_updated_at": str(review.get("updated_at") or ""),
        "defaults": defaults,
        "event_type": event_type,
        "award_state": award_state,
        "latest_request": latest_request,
        "full_ec": fe,
        "partial_ec": pe,
        "counts": counts,
        "rows": rows,
        "recipient_count": recipients,
        "total_ec": total,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _render_attendance_ec_preview(data: dict[str, Any], event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, saved: bool = False, locked: bool = False) -> str:
    preview = _attendance_ec_preview_payload(data, str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not preview.get("ok"):
        return _html_shell("EC-Vorschau", f"<section class='panel'><h1>❌ EC-Vorschau</h1><p>{_e(preview.get('error'))}</p><p><a class='btn' href='/attendance'>Zurück</a></p></section>")
    event = preview.get("event") or {}
    rows = preview.get("rows") or []
    guild_id = _safe_guild_id(data)
    fe = _num(preview.get("full_ec"), 0)
    pe = _num(preview.get("partial_ec"), 0)
    total = _num(preview.get("total_ec"), 0)
    recipients = int(_num(preview.get("recipient_count"), 0))
    review_status = str(preview.get("review_status") or "open")
    row_data = []
    for r in rows:
        gain = _num(r.get("ec_gain"), 0)
        row_data.append([
            _member_link(_user_id(r.get("user_id")), r.get("display_name")),
            r.get("signup") or "—",
            r.get("status_label") or _attendance_status_label(r.get("status")),
            f"{_fmt_ec(r.get('voice_minutes'))} min",
            _fmt_ec(r.get("ec_before")),
            _raw(f"<strong>+{_fmt_ec(gain)}</strong>" if gain else "0"),
            _fmt_ec(r.get("ec_after")),
            _short(r.get("note") or "", 120),
        ])
    copy_lines = [
        f"EC-Vorschau: {event.get('title') or event_id}",
        f"War da: +{_fmt_ec(fe)} EC · Teilweise: +{_fmt_ec(pe)} EC",
        f"Empfänger: {recipients} · Gesamt: { _fmt_ec(total) } EC",
        "",
    ]
    for r in rows:
        gain = _num(r.get("ec_gain"), 0)
        if gain > 0:
            copy_lines.append(f"+{_fmt_ec(gain)} EC — {r.get('display_name')} ({_attendance_status_label(r.get('status'))})")
    copy_text = "\n".join(copy_lines)
    award_state = preview.get("award_state") or {}
    latest_request = preview.get("latest_request") or {}
    event_type = str(preview.get("event_type") or "Dashboard Attendance")
    notice = ""
    if saved:
        notice = "<div class='warn'>✅ Aktion gespeichert/angefragt.</div>"
    if locked:
        notice = "<div class='warn'>🔒 Review ist als freigegeben markiert.</div>"
    if award_state.get("awarded"):
        notice += f"<div class='warn'>⚠️ Dieses Event hat laut aktuellem Snapshot bereits EC-Buchungen: {_e(award_state.get('count'))} Buchungen / {_e(_fmt_ec(award_state.get('total')))} EC. Button bleibt gesperrt.</div>"
    if latest_request:
        notice += f"<div class='warn'>📌 Letzte Dashboard-EC-Anfrage: <strong>{_e(latest_request.get('status'))}</strong> · Request <code>{_e(latest_request.get('request_id'))}</code> · {_e(_dt(latest_request.get('requested_at')))}</div>"
    body = f"""
    <nav class="topnav"><a href="/attendance/{_e(event_id)}">← Review</a><a href="/attendance/{_e(event_id)}/report">Abschlussbericht</a><a href="/attendance">Anwesenheit</a><a href="/attendance-archive">Archiv</a><a href="/ec">EC-Verlauf</a><a href="/ec-queue">EC-Queue</a><a href="/event/{_e(event_id)}">Eventdetails</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">EC-Vorschau + Bot-Buchung über sichere Queue</div>
        <h1>🪙 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Review-Status: {_e(review_status)} · Event: {_e(_dt(event.get('when_iso')))} · Typ: {_e(event_type)} · Erkennung: {_e((preview.get('defaults') or {}).get('detected_from') or 'manuell')}</p>
      </div>
      <a class="btn" href="/export/attendance/{_e(event_id)}.csv?full_ec={_e(fe)}&partial_ec={_e(pe)}">CSV herunterladen</a>
    </section>
    {notice}
    {_raw(_event_ec_queue_panel(guild_id, str(event_id)))}
    <section class="grid">
      {_card('Empfänger', recipients, 'bekommen EC laut Review')}
      {_card('Gesamt-EC', _fmt_ec(total), 'würde insgesamt vergeben')}
      {_card('Eventtyp', event_type, 'für Doppelbuchungs-Schutz')}
      {_card('War da', (preview.get('counts') or {}).get('present', 0), f'+{_fmt_ec(fe)} EC')}
      {_card('Teilweise', (preview.get('counts') or {}).get('partial', 0), f'+{_fmt_ec(pe)} EC')}
    </section>
    <section class="panel">
      <h2>⚙️ Werte für Vorschau</h2>
      <p class="muted">Diese Werte gelten für Vorschau und Buchungsanfrage. Echte EC werden erst gebucht, wenn der Bot die Postgres-Anfrage verarbeitet.</p>
      <form method="get" action="/attendance/{_e(event_id)}/ec-preview" style="display:flex;gap:12px;flex-wrap:wrap;align-items:end">
        <label>War da EC<br><input name="full_ec" value="{_e(fe)}" style="width:120px"></label>
        <label>Teilweise EC<br><input name="partial_ec" value="{_e(pe)}" style="width:120px"></label>
        <button class="btn" type="submit">Vorschau berechnen</button>
      </form>
    </section>
    <section class="panel">
      <h2>👥 EC-Vorschau pro Spieler</h2>
      {_table(['Spieler','Anmeldung','Review','Voice','EC vorher','Plus','EC danach','Notiz'], row_data, placeholder='EC-Vorschau durchsuchen…')}
    </section>
    <section class="panel">
      <h2>📋 Copy-Text für DKP-Log</h2>
      <textarea readonly style="width:100%;min-height:180px;background:#101116;color:#f2ead7;border:1px solid var(--line);border-radius:12px;padding:12px">{_e(copy_text)}</textarea>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px">
        <form method="post" action="/admin/attendance/{_e(event_id)}/lock">
          <input type="hidden" name="full_ec" value="{_e(fe)}"><input type="hidden" name="partial_ec" value="{_e(pe)}">
          <button class="btn" type="submit">🔒 Review freigeben</button>
        </form>
        <form method="post" action="/admin/attendance/{_e(event_id)}/ec-award" onsubmit="return confirm('EC wirklich buchen? Der Bot verarbeitet diese Anfrage und schreibt danach in die echten EC-Daten.');">
          <input type="hidden" name="full_ec" value="{_e(fe)}"><input type="hidden" name="partial_ec" value="{_e(pe)}"><input type="hidden" name="event_type" value="{_e(event_type)}">
          <button class="btn" type="submit" {'disabled' if award_state.get('awarded') or str(latest_request.get('status') or '') in {'pending','processing','done'} or recipients <= 0 or fe <= 0 else ''}>✅ EC wirklich buchen</button>
        </form>
      </div>
      <p class="muted">Doppelbuchungs-Schutz: Dashboard blockt bereits bekannte/pending Buchungen. Der Bot prüft vor dem Schreiben zusätzlich nochmal seine echten EC-Transaktionen.</p>
    </section>
    """
    return _html_shell(f"EC-Vorschau · {event.get('title') or event_id}", body)




# ---------------------------------------------------------------------------
# Attendance Abschlussbericht
# ---------------------------------------------------------------------------
# Der Bericht ist eine reine Kontroll-/Exportseite. Er liest Review, Voice,
# EC-Vorschau und Queue-Status zusammen, schreibt aber nichts.


def _attendance_report_payload(data: dict[str, Any], event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None) -> dict[str, Any]:
    preview = _attendance_ec_preview_payload(data, str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not preview.get("ok"):
        return preview
    rows = [r for r in (preview.get("rows") or []) if isinstance(r, dict)]
    counts = dict(preview.get("counts") or {})
    recipients = sum(1 for r in rows if _num(r.get("ec_gain"), 0) > 0)
    total_voice = sum(_num(r.get("voice_minutes"), 0) for r in rows)
    voice_users = sum(1 for r in rows if _num(r.get("voice_minutes"), 0) > 0)
    open_rows = [r for r in rows if str(r.get("status") or "open").lower() == "open"]
    no_voice_present = [r for r in rows if str(r.get("status") or "").lower() == "present" and _num(r.get("voice_minutes"), 0) <= 0]
    voice_but_absent = [r for r in rows if str(r.get("status") or "").lower() in {"absent", "ignore"} and _num(r.get("voice_minutes"), 0) > 0]
    partial_rows = [r for r in rows if str(r.get("status") or "").lower() == "partial"]
    problems: list[dict[str, Any]] = []
    for r in open_rows:
        problems.append({**r, "warning": "offen – vor Buchung prüfen"})
    for r in no_voice_present:
        problems.append({**r, "warning": "War da ohne Voice-Zeit"})
    for r in voice_but_absent:
        problems.append({**r, "warning": "Voice vorhanden, aber Nicht da/Ignorieren"})
    for r in partial_rows:
        problems.append({**r, "warning": "Teilweise – manuell prüfen"})
    guild_id = _safe_guild_id(data)
    queue = _event_ec_queue_status(guild_id, str(event_id)) if guild_id else {"rows": [], "latest": {}}
    latest = queue.get("latest") if isinstance(queue.get("latest"), dict) else {}
    ready = bool(rows) and not open_rows and not (preview.get("award_state") or {}).get("awarded") and str(latest.get("status") or "").lower() not in {"pending", "processing", "done"}
    return {
        **preview,
        "ok": True,
        "report": {
            "ready": ready,
            "total_rows": len(rows),
            "recipients": recipients,
            "total_voice_minutes": total_voice,
            "voice_users": voice_users,
            "open_count": len(open_rows),
            "problem_count": len(problems),
            "problems": problems,
            "queue": queue,
            "counts": counts,
        },
    }


def _render_attendance_report(data: dict[str, Any], event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None) -> str:
    payload = _attendance_report_payload(data, str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not payload.get("ok"):
        return _html_shell("Attendance Bericht", f"<section class='panel'><h1>❌ Attendance-Bericht</h1><p>{_e(payload.get('error'))}</p><p><a class='btn' href='/attendance'>Zurück</a></p></section>")
    event = payload.get("event") or {}
    report = payload.get("report") or {}
    rows = payload.get("rows") or []
    problems = report.get("problems") or []
    counts = payload.get("counts") or {}
    fe = _num(payload.get("full_ec"), 0)
    pe = _num(payload.get("partial_ec"), 0)
    total_ec = _num(payload.get("total_ec"), 0)
    recipients = int(_num(payload.get("recipient_count"), 0))
    guild_id = _safe_guild_id(data)
    latest = (report.get("queue") or {}).get("latest") if isinstance(report.get("queue"), dict) else {}
    latest_status = str((latest or {}).get("status") or "—")
    ready = bool(report.get("ready"))
    ready_notice = "<div class='warn'>✅ Bericht sieht buchungsbereit aus. Kein offener Review-Status, keine aktive/erledigte Queue für dieses Event.</div>" if ready else "<div class='warn'>⚠️ Vor EC-Buchung prüfen: offene Punkte, bestehende Queue oder bereits erkannte Buchung vorhanden.</div>"
    problem_rows = []
    for r in problems:
        problem_rows.append([
            _member_link(_user_id(r.get("user_id")), r.get("display_name")),
            r.get("warning") or "prüfen",
            r.get("status_label") or _attendance_status_label(r.get("status")),
            f"{_fmt_ec(r.get('voice_minutes'))} min",
            r.get("signup") or "—",
            _short(r.get("note") or "", 120),
        ])
    ec_rows = []
    for r in rows:
        gain = _num(r.get("ec_gain"), 0)
        if gain <= 0:
            continue
        ec_rows.append([
            _member_link(_user_id(r.get("user_id")), r.get("display_name")),
            r.get("status_label") or _attendance_status_label(r.get("status")),
            r.get("signup") or "—",
            f"{_fmt_ec(r.get('voice_minutes'))} min",
            _raw(f"<strong>+{_fmt_ec(gain)} EC</strong>"),
            _fmt_ec(r.get("ec_after")),
        ])
    copy_lines = [
        f"Attendance-Abschlussbericht: {event.get('title') or event_id}",
        f"Zeit: {_dt(event.get('when_iso'))}",
        f"Review: {payload.get('review_status') or '—'}",
        f"Empfänger: {recipients} · Gesamt: {_fmt_ec(total_ec)} EC",
        f"War da: {counts.get('present', 0)} · Teilweise: {counts.get('partial', 0)} · Nicht da: {counts.get('absent', 0)} · Ignoriert: {counts.get('ignore', 0)} · Offen: {counts.get('open', 0)}",
        f"Queue: {latest_status}",
        "",
        "EC-Empfänger:",
    ]
    for r in rows:
        gain = _num(r.get("ec_gain"), 0)
        if gain > 0:
            copy_lines.append(f"+{_fmt_ec(gain)} EC — {r.get('display_name')} — {r.get('status_label') or _attendance_status_label(r.get('status'))}")
    if problems:
        copy_lines.extend(["", "Prüfpunkte:"])
        for r in problems[:30]:
            copy_lines.append(f"- {r.get('display_name')}: {r.get('warning')}")
    copy_text = "\n".join(copy_lines)
    body = f"""
    <nav class="topnav"><a href="/attendance/{_e(event_id)}">← Review</a><a href="/attendance/{_e(event_id)}/ec-preview">EC-Vorschau</a><a href="/attendance">Anwesenheit</a><a href="/attendance-archive">Archiv</a><a href="/ec-queue">EC-Queue</a><a href="/event/{_e(event_id)}">Eventdetails</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Attendance-Abschlussbericht · read-only</div>
        <h1>📋 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Event-ID: {_e(event_id)} · Zeit: {_e(_dt(event.get('when_iso')))} · Review: {_e(payload.get('review_status') or '—')} · Queue: {_e(latest_status)}</p>
      </div>
      <a class="btn" href="/export/attendance/{_e(event_id)}_report.csv?full_ec={_e(fe)}&partial_ec={_e(pe)}">Bericht-CSV</a>
    </section>
    {ready_notice}
    <section class="grid mini-grid">
      {_card('Review-Zeilen', report.get('total_rows', 0), 'alle Kandidaten')}
      {_card('EC-Empfänger', recipients, f'gesamt {_fmt_ec(total_ec)} EC')}
      {_card('Offen', report.get('open_count', 0), 'muss vor Buchung weg')}
      {_card('Prüfpunkte', report.get('problem_count', 0), 'Voice/Status-Auffälligkeiten')}
      {_card('Voice-User', report.get('voice_users', 0), f"{_fmt_ec(report.get('total_voice_minutes', 0))} Minuten")}
      {_card('War da', counts.get('present', 0), f'+{_fmt_ec(fe)} EC')}
      {_card('Teilweise', counts.get('partial', 0), f'+{_fmt_ec(pe)} EC')}
      {_card('Queue', latest_status, 'letzte Anfrage')}
    </section>
    {_raw(_event_ec_queue_panel(guild_id, str(event_id)))}
    <section class="panel">
      <h2>⚠️ Prüfpunkte</h2>
      <p class="muted">Diese Liste ist bewusst streng. Sie hilft, Fehlbuchungen vor dem finalen EC-Button zu vermeiden.</p>
      {_table(['Spieler','Hinweis','Review','Voice','Anmeldung','Notiz'], problem_rows, placeholder='Prüfpunkte durchsuchen…')}
    </section>
    <section class="panel">
      <h2>🪙 EC-Empfänger</h2>
      {_table(['Spieler','Review','Anmeldung','Voice','Plus','EC danach'], ec_rows, placeholder='EC-Empfänger durchsuchen…')}
    </section>
    <section class="panel">
      <h2>📋 Copy-Text</h2>
      <textarea readonly style="width:100%;min-height:220px;background:#101116;color:#f2ead7;border:1px solid var(--line);border-radius:12px;padding:12px">{_e(copy_text)}</textarea>
      <p style="display:flex;gap:10px;flex-wrap:wrap"><a class="btn" href="/attendance/{_e(event_id)}/ec-preview?full_ec={_e(fe)}&partial_ec={_e(pe)}">Zur EC-Vorschau</a><a class="btn" href="/attendance/{_e(event_id)}">Review bearbeiten</a></p>
    </section>
    """
    return _html_shell(f"Attendance-Bericht · {event.get('title') or event_id}", body)


@app.get("/attendance/{event_id}/report", response_class=HTMLResponse)
def attendance_report_page(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_attendance_report(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec))
    except Exception as exc:
        return HTMLResponse(_html_shell("Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/attendance/{event_id}/report")
def api_attendance_report(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, _: bool = Depends(_auth)):
    payload = _attendance_report_payload(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    status = 200 if payload.get("ok") else 404
    return JSONResponse(payload, status_code=status)


@app.get("/export/attendance/{event_id}_report.csv")
def export_attendance_report_csv(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, _: bool = Depends(_auth)):
    payload = _attendance_report_payload(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not payload.get("ok"):
        return Response("error\n" + str(payload.get("error") or "unknown"), media_type="text/csv", status_code=404)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["event_id", "event_title", "user_id", "display_name", "signup", "review_status", "voice_minutes", "ec_gain", "warning", "note"])
    event = payload.get("event") or {}
    warnings_by_user: dict[int, list[str]] = {}
    for r in ((payload.get("report") or {}).get("problems") or []):
        uid = _user_id(r.get("user_id"))
        warnings_by_user.setdefault(uid, []).append(str(r.get("warning") or "prüfen"))
    for r in payload.get("rows") or []:
        uid = _user_id(r.get("user_id"))
        writer.writerow([
            payload.get("event_id"),
            event.get("title") or payload.get("event_id"),
            uid,
            r.get("display_name"),
            r.get("signup"),
            r.get("status_label"),
            r.get("voice_minutes"),
            r.get("ec_gain"),
            "; ".join(warnings_by_user.get(uid, [])),
            r.get("note"),
        ])
    return Response(output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": f"attachment; filename=attendance_report_{event_id}.csv"})

@app.get("/attendance/{event_id}/ec-preview", response_class=HTMLResponse)
def attendance_ec_preview_page(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, saved: int = 0, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_attendance_ec_preview(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec, saved=bool(saved)))
    except Exception as exc:
        return HTMLResponse(_html_shell("Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.post("/admin/attendance/{event_id}/lock")
async def admin_attendance_lock(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id))
    if not event:
        raise HTTPException(status_code=404, detail="Event nicht gefunden und kein gespeicherter Review-Fallback vorhanden")
    raw = (await request.body()).decode("utf-8", errors="replace")
    form = urllib.parse.parse_qs(raw, keep_blank_values=True)
    full_ec = _num((form.get("full_ec") or [0])[0], 0)
    partial_ec = _num((form.get("partial_ec") or [0])[0], 0)
    review, items = _attendance_items_for_preview(snap, event, guild_id)
    review_payload = review.get("payload") if isinstance(review.get("payload"), dict) else {}
    if not review_payload.get("items"):
        review_payload = _attendance_review_payload_from_event(snap, event, mode="voice")
    review_payload["ec_preview"] = {
        "full_ec": full_ec,
        "partial_ec": partial_ec,
        "locked_at": datetime.now(timezone.utc).isoformat(),
        "note": "Freigabe im Dashboard. Keine automatische EC-Buchung.",
    }
    _attendance_review_save(guild_id, str(event_id), review_payload, _current_user(request) or {}, status="locked")
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}/ec-preview?full_ec={urllib.parse.quote(str(full_ec))}&partial_ec={urllib.parse.quote(str(partial_ec))}&saved=1", status_code=303)



@app.post("/admin/attendance/{event_id}/ec-award")
async def admin_attendance_ec_award(event_id: str, request: Request, _: bool = Depends(_admin_auth)):
    raw = (await request.body()).decode("utf-8", errors="replace")
    form = urllib.parse.parse_qs(raw, keep_blank_values=True)
    full_ec = _num((form.get("full_ec") or [0])[0], 0)
    partial_ec = _num((form.get("partial_ec") or [0])[0], 0)
    event_type = str((form.get("event_type") or [""])[0] or "Dashboard Attendance").strip() or "Dashboard Attendance"

    payload = _snapshot_payload()
    snap: dict[str, Any] = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    event = _event_by_id(snap, str(event_id)) or _event_stub_from_attendance_review(guild_id, str(event_id))
    if not event:
        raise HTTPException(status_code=404, detail="Event nicht gefunden und kein gespeicherter Review-Fallback vorhanden")
    review = _attendance_review_load(guild_id, str(event_id))
    review_status = str(review.get("status") or "").lower()
    if review_status not in {"reviewed", "locked"}:
        return HTMLResponse(_html_shell("EC-Buchung blockiert", f"<section class='panel'><h1>❌ EC-Buchung blockiert</h1><p>Speichere den Attendance Review zuerst. Aktueller Status: <strong>{_e(review_status or 'kein Review')}</strong></p><p><a class='btn' href='/attendance/{_e(event_id)}'>Zurück zum Review</a></p></section>"), status_code=400)

    preview = _attendance_ec_preview_payload(payload, str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not preview.get("ok"):
        raise HTTPException(status_code=400, detail=str(preview.get("error") or "Preview fehlgeschlagen"))
    if (preview.get("award_state") or {}).get("awarded"):
        return HTMLResponse(_html_shell("EC-Buchung blockiert", f"<section class='panel'><h1>❌ Doppelbuchung blockiert</h1><p>Für dieses Event gibt es laut Snapshot bereits EC-Buchungen.</p><p><a class='btn' href='/attendance/{_e(event_id)}/ec-preview'>Zurück</a></p></section>"), status_code=409)

    result = _enqueue_ec_award_request(guild_id, str(event_id), event_type, preview, _current_user(request) or {})
    if not result.get("ok"):
        return HTMLResponse(_html_shell("EC-Buchung blockiert", f"<section class='panel'><h1>❌ EC-Buchung nicht angelegt</h1><p>{_e(result.get('error'))}</p><p><a class='btn' href='/attendance/{_e(event_id)}/ec-preview?full_ec={_e(full_ec)}&partial_ec={_e(partial_ec)}'>Zurück</a></p></section>"), status_code=409)
    return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}/ec-preview?full_ec={urllib.parse.quote(str(full_ec))}&partial_ec={urllib.parse.quote(str(partial_ec))}&saved=1", status_code=303)

@app.get("/api/attendance/{event_id}/ec-preview")
def api_attendance_ec_preview(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, _: bool = Depends(_auth)):
    payload = _attendance_ec_preview_payload(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    status = 200 if payload.get("ok") else 404
    return JSONResponse(payload, status_code=status)


@app.get("/export/attendance/{event_id}.csv")
def export_attendance_ec_preview_csv(event_id: str, full_ec: Optional[float] = None, partial_ec: Optional[float] = None, _: bool = Depends(_auth)):
    payload = _attendance_ec_preview_payload(_snapshot_payload(), str(event_id), full_ec=full_ec, partial_ec=partial_ec)
    if not payload.get("ok"):
        return Response("error\n" + str(payload.get("error") or "unknown"), media_type="text/csv", status_code=404)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["event_id", "event_title", "user_id", "display_name", "signup", "review_status", "voice_minutes", "ec_before", "ec_gain", "ec_after", "note"])
    event = payload.get("event") or {}
    for r in payload.get("rows") or []:
        writer.writerow([
            payload.get("event_id"),
            event.get("title") or payload.get("event_id"),
            r.get("user_id"),
            r.get("display_name"),
            r.get("signup"),
            r.get("status_label"),
            r.get("voice_minutes"),
            r.get("ec_before"),
            r.get("ec_gain"),
            r.get("ec_after"),
            r.get("note"),
        ])
    return Response(output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": f"attachment; filename=attendance_ec_preview_{event_id}.csv"})
