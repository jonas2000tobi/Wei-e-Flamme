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
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="Ebo Dashboard", version="1.0.0")
security = HTTPBasic(auto_error=False)

STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

ASSET_VER = "ebo-phase3-database-start"
DASHBOARD_RELEASE_VERSION = "1.1.9 · Phase 3.9 Online-DB Finalisierung"


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


def _auth_role_ids_from_snapshot(kind: str) -> set[str]:
    """Rollen-IDs aus dem Bot-Snapshot lesen. kind: admin, allowed, member."""
    out: set[str] = set()
    try:
        payload = _snapshot_payload()
        snap = payload.get("snapshot") or {}
        auth = snap.get("auth") or {}
        if not isinstance(auth, dict):
            return out
        if kind == "member":
            member_role = auth.get("member_role") if isinstance(auth.get("member_role"), dict) else {}
            rid = str(member_role.get("role_id") or "").strip()
            if rid:
                out.add(rid)
        if kind in {"allowed", "all"}:
            for row in auth.get("allowed_roles") or []:
                if isinstance(row, dict):
                    rid = str(row.get("role_id") or "").strip()
                    if rid:
                        out.add(rid)
        if kind in {"admin", "all"}:
            for row in auth.get("admin_roles") or []:
                if isinstance(row, dict):
                    rid = str(row.get("role_id") or "").strip()
                    if rid:
                        out.add(rid)
    except Exception:
        return out
    return out


def _allowed_role_ids() -> set[str]:
    explicit = _csv_ids(_env("DASHBOARD_ALLOWED_ROLE_IDS"))
    admin = _csv_ids(_env("DASHBOARD_ADMIN_ROLE_IDS"))
    member = _csv_ids(_env("DASHBOARD_MEMBER_ROLE_IDS"))
    configured = _env("DASHBOARD_MEMBER_ROLE_ID") or _configured_member_role_id_from_snapshot()
    out = set()
    out.update(explicit)
    out.update(admin)
    out.update(member)
    out.update(_auth_role_ids_from_snapshot("allowed"))
    out.update(_auth_role_ids_from_snapshot("admin"))
    out.update(_auth_role_ids_from_snapshot("member"))
    if configured:
        out.add(str(configured))
    return out


def _admin_role_ids() -> set[str]:
    out = _csv_ids(_env("DASHBOARD_ADMIN_ROLE_IDS"))
    out.update(_auth_role_ids_from_snapshot("admin"))
    return out


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
    payload = {
        "ok": True,
        "id": row.get("id"),
        "guild_id": row.get("guild_id"),
        "guild_name": row.get("guild_name"),
        "generated_at": row.get("generated_at"),
        "published_at": row.get("published_at"),
        "snapshot": snap,
    }
    return _apply_phase3_events_read_cutover(_apply_phase3_loot_read_cutover(_apply_phase3_ec_read_cutover(payload)))


# ---------------------------------------------------------------------------
# Phase 3.6: EC/DKP Read-Cutover
# ---------------------------------------------------------------------------
# Ziel: Das Dashboard liest EC-Konten, EC-Verlauf und EC-Eventchecks bevorzugt
# aus den Phase-3-Postgres-Tabellen. JSON/Snapshot bleibt Fallback.
# Der Bot schreibt weiterhin parallel JSON + Postgres; kein harter Write-Cutover.


def _phase3_json(value: Any, default: Any = None) -> Any:
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return default


def _json_safe(value: Any) -> Any:
    """Macht DB-/Snapshot-Daten sicher für JSONResponse.

    psycopg liefert bei TIMESTAMPTZ echte datetime-Objekte. JSONResponse kann
    die nicht selbst serialisieren. Diese Funktion hält Status-APIs stabil.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _phase3_ec_pg_payload(guild_id: Any, snap: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Lädt EC/DKP aus Postgres für den Dashboard-Read-Cutover.

    Rückgabe ist absichtlich snapshot-kompatibel:
    snap["ec"]["balances"]["top"]
    snap["ec"]["transactions"]["items"]
    snap["ec"]["event_checks"]["items"]
    """
    if not _database_url():
        return None
    gid = str(guild_id or "").strip()
    if not gid:
        return None
    names = _profile_name_map(snap)
    try:
        conn = _pg_connect()
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT user_id, balance, raw_json, source, updated_at
                    FROM phase3_ec_balances
                    WHERE guild_id = %s
                    ORDER BY balance DESC, user_id ASC
                    """,
                    (gid,),
                )
                balance_rows = cur.fetchall() or []
            except Exception:
                return None

            try:
                cur.execute(
                    """
                    SELECT transaction_id, user_id, amount, reason, event_id, created_at_text, raw_json, source, mirrored_at
                    FROM phase3_ec_transactions
                    WHERE guild_id = %s
                    ORDER BY COALESCE(NULLIF(created_at_text, ''), transaction_id) DESC, mirrored_at DESC
                    LIMIT 1000
                    """,
                    (gid,),
                )
                tx_rows = cur.fetchall() or []
            except Exception:
                tx_rows = []

            try:
                cur.execute(
                    """
                    SELECT event_id, status, awarded, posted, raw_json, source, updated_at
                    FROM phase3_ec_event_checks
                    WHERE guild_id = %s
                    ORDER BY updated_at DESC, event_id DESC
                    LIMIT 500
                    """,
                    (gid,),
                )
                check_rows = cur.fetchall() or []
            except Exception:
                check_rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not balance_rows and not tx_rows and not check_rows:
        return None

    balances: list[dict[str, Any]] = []
    for row in balance_rows:
        raw = _phase3_json(row.get("raw_json"), {}) if isinstance(row, dict) else {}
        uid = _user_id((row or {}).get("user_id"))
        name = raw.get("display_name") or raw.get("name") or names.get(uid) or f"User {uid}"
        balances.append({
            **raw,
            "user_id": uid,
            "display_name": name,
            "balance": _num((row or {}).get("balance"), 0),
            "source": "postgres_phase3",
        })

    tx_items: list[dict[str, Any]] = []
    total_earned = 0.0
    total_spent = 0.0
    by_user: dict[int, dict[str, Any]] = {}
    for row in tx_rows:
        raw = _phase3_json((row or {}).get("raw_json"), {}) if isinstance(row, dict) else {}
        uid = _user_id((row or {}).get("user_id") or raw.get("user_id"))
        amount = _num((row or {}).get("amount", raw.get("amount", 0)), 0)
        if amount >= 0:
            total_earned += amount
        else:
            total_spent += abs(amount)
        bucket = by_user.setdefault(uid, {
            "user_id": uid,
            "display_name": raw.get("display_name") or names.get(uid) or f"User {uid}",
            "earned": 0.0,
            "spent": 0.0,
            "net": 0.0,
            "count": 0,
        })
        if amount >= 0:
            bucket["earned"] += amount
        else:
            bucket["spent"] += abs(amount)
        bucket["net"] += amount
        bucket["count"] += 1
        tx_items.append({
            **raw,
            "transaction_id": (row or {}).get("transaction_id") or raw.get("transaction_id"),
            "user_id": uid,
            "display_name": raw.get("display_name") or names.get(uid) or f"User {uid}",
            "amount": amount,
            "reason": (row or {}).get("reason") or raw.get("reason") or raw.get("note") or "",
            "event_id": (row or {}).get("event_id") or raw.get("event_id") or raw.get("auction_id") or "",
            "created_at": (row or {}).get("created_at_text") or raw.get("created_at") or raw.get("time") or raw.get("timestamp") or "",
            "raw_type": raw.get("raw_type") or raw.get("type") or "Postgres",
            "source": "postgres_phase3",
        })

    users = [b for b in by_user.values() if b.get("user_id")]
    top_earned = sorted(users, key=lambda x: (_num(x.get("earned"), 0), _num(x.get("net"), 0)), reverse=True)
    top_spent = sorted(users, key=lambda x: (_num(x.get("spent"), 0), _num(x.get("count"), 0)), reverse=True)
    top_activity = sorted(users, key=lambda x: (_num(x.get("count"), 0), abs(_num(x.get("net"), 0))), reverse=True)

    check_items: list[dict[str, Any]] = []
    for row in check_rows:
        raw = _phase3_json((row or {}).get("raw_json"), {}) if isinstance(row, dict) else {}
        check_items.append({
            **raw,
            "event_id": (row or {}).get("event_id") or raw.get("event_id"),
            "status": (row or {}).get("status") or raw.get("status") or "",
            "awarded": bool((row or {}).get("awarded")),
            "posted": bool((row or {}).get("posted")),
            "source": "postgres_phase3",
        })

    return {
        "source": "postgres_phase3_read_cutover",
        "read_cutover": True,
        "balances": {
            "top": balances,
            "items": balances,
            "count": len(balances),
            "loaded_count": len(balances),
            "source": "postgres_phase3",
        },
        "transactions": {
            "items": tx_items,
            "recent": tx_items,
            "count": len(tx_items),
            "loaded_count": len(tx_items),
            "total_earned": total_earned,
            "total_spent": total_spent,
            "net_loaded": total_earned - total_spent,
            "top_earned": top_earned,
            "top_spent": top_spent,
            "top_activity": top_activity,
            "source": "postgres_phase3",
        },
        "event_checks": {
            "items": check_items,
            "count": len(check_items),
            "source": "postgres_phase3",
        },
    }


def _apply_phase3_ec_read_cutover(payload: dict[str, Any]) -> dict[str, Any]:
    """Wendet EC-Read-Cutover auf den Snapshot an, ohne den Snapshot selbst zu verlieren."""
    if not payload.get("ok"):
        return payload
    snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    guild_id = payload.get("guild_id") or snap.get("guild_id") or _env("DASHBOARD_GUILD_ID")
    pg_ec = _phase3_ec_pg_payload(guild_id, snap)
    if not pg_ec:
        payload["phase3_ec_read_cutover"] = {"active": False, "source": "snapshot_fallback", "reason": "Postgres-EC nicht verfügbar oder leer"}
        return payload
    old_ec = snap.get("ec") if isinstance(snap.get("ec"), dict) else {}
    snap["ec_snapshot_fallback"] = old_ec
    merged = dict(old_ec)
    merged.update(pg_ec)
    snap["ec"] = merged
    payload["snapshot"] = snap
    payload["phase3_ec_read_cutover"] = {
        "active": True,
        "source": "postgres_phase3",
        "balances": len((pg_ec.get("balances") or {}).get("top") or []),
        "transactions": len((pg_ec.get("transactions") or {}).get("items") or []),
        "event_checks": len((pg_ec.get("event_checks") or {}).get("items") or []),
    }
    return payload


# ---------------------------------------------------------------------------
# Phase 3.7: Loot/Needs/Auktionen Read-Cutover
# ---------------------------------------------------------------------------
# Ziel: Das Dashboard liest Loot-/Need-/Auktionsdaten bevorzugt aus den
# Phase-3-Postgres-Tabellen. JSON/Snapshot bleibt Fallback. Der Bot schreibt
# weiterhin parallel JSON + Postgres; kein harter Write-Cutover.


def _phase3_need_label(slot_name: Any, item_name: Any) -> str:
    item = str(item_name or "").strip()
    slot = str(slot_name or "").strip()
    if not item:
        return ""
    if slot and not item.lower().startswith((slot + ":").lower()):
        return f"{slot}: {item}"
    return item


def _phase3_member_name_map(guild_id: Any, snap: dict[str, Any]) -> dict[int, str]:
    names = dict(_profile_name_map(snap))
    if not _database_url():
        return names
    gid = str(guild_id or "").strip()
    if not gid:
        return names
    try:
        conn = _pg_connect()
    except Exception:
        return names
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT user_id, display_name, raw_json
                    FROM phase3_members
                    WHERE guild_id = %s
                    """,
                    (gid,),
                )
                for row in cur.fetchall() or []:
                    raw = _phase3_json((row or {}).get("raw_json"), {}) if isinstance(row, dict) else {}
                    uid = _user_id((row or {}).get("user_id"))
                    name = str((row or {}).get("display_name") or raw.get("display_name") or raw.get("name") or raw.get("username") or "").strip()
                    if uid and name:
                        names[uid] = name
            except Exception:
                pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return names


def _phase3_loot_pg_payload(guild_id: Any, snap: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Lädt Loot/Needs/Auktionen aus Postgres für den Dashboard-Read-Cutover.

    Rückgabe ist absichtlich snapshot-kompatibel:
    snap["loot"]["needs"]["items"]
    snap["loot"]["auctions"]["items"]
    snap["loot"]["history"]["items"]
    """
    if not _database_url():
        return None
    gid = str(guild_id or "").strip()
    if not gid:
        return None
    names = _phase3_member_name_map(guild_id, snap)
    try:
        conn = _pg_connect()
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT need_id, user_id, item_name, need_type, slot_name, status, raw_json, source, updated_at
                    FROM phase3_loot_needs
                    WHERE guild_id = %s
                    ORDER BY user_id ASC, need_type ASC, slot_name ASC, item_name ASC
                    """,
                    (gid,),
                )
                need_rows = [dict(r) for r in (cur.fetchall() or [])]
            except Exception:
                need_rows = []

            try:
                cur.execute(
                    """
                    SELECT auction_id, item_name, status, winner_user_id, current_bid, raw_json, source, updated_at
                    FROM phase3_loot_auctions
                    WHERE guild_id = %s
                    ORDER BY updated_at DESC, auction_id DESC
                    """,
                    (gid,),
                )
                auction_rows = [dict(r) for r in (cur.fetchall() or [])]
            except Exception:
                auction_rows = []

            try:
                cur.execute(
                    """
                    SELECT bid_id, auction_id, user_id, amount, raw_json, source, mirrored_at
                    FROM phase3_loot_bids
                    WHERE guild_id = %s
                    ORDER BY mirrored_at DESC, bid_id DESC
                    """,
                    (gid,),
                )
                bid_rows = [dict(r) for r in (cur.fetchall() or [])]
            except Exception:
                bid_rows = []

            try:
                cur.execute(
                    """
                    SELECT entry_id, user_id, item_name, amount, raw_json, source, mirrored_at
                    FROM phase3_loot_history
                    WHERE guild_id = %s
                    ORDER BY mirrored_at DESC, entry_id DESC
                    LIMIT 1000
                    """,
                    (gid,),
                )
                history_rows = [dict(r) for r in (cur.fetchall() or [])]
            except Exception:
                history_rows = []

            try:
                cur.execute(
                    """
                    SELECT log_id, request_id, actor_id, target_user_id, action_type, old_item, new_item, raw_json, source, created_at
                    FROM phase3_need_change_log
                    WHERE guild_id = %s
                    ORDER BY created_at DESC
                    LIMIT 500
                    """,
                    (gid,),
                )
                need_log_rows = [dict(r) for r in (cur.fetchall() or [])]
            except Exception:
                need_log_rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not need_rows and not auction_rows and not bid_rows and not history_rows and not need_log_rows:
        return None

    # Needs spielerkompatibel gruppieren: eine Zeile pro Spieler, Main/Secondary als Listen.
    players: dict[int, dict[str, Any]] = {}
    need_entries: list[dict[str, Any]] = []
    main_count = 0
    secondary_count = 0
    for row in need_rows:
        raw = _phase3_json(row.get("raw_json"), {})
        uid = _user_id(row.get("user_id") or raw.get("user_id"))
        item = str(row.get("item_name") or raw.get("item_name") or raw.get("item") or raw.get("name") or "").strip()
        if not uid or not item:
            continue
        slot = str(row.get("slot_name") or raw.get("slot_name") or raw.get("slot") or "").strip()
        need_type_raw = str(row.get("need_type") or raw.get("need_type") or raw.get("type") or "Main").strip().lower()
        kind = "secondary" if need_type_raw in {"secondary", "second", "off", "offspec", "secondary_need"} else "main"
        if kind == "main":
            main_count += 1
        else:
            secondary_count += 1
        display = str(raw.get("display_name") or raw.get("name") or names.get(uid) or f"User {uid}")
        p = players.setdefault(uid, {"user_id": uid, "display_name": display, "main": [], "secondary": []})
        if display and (not p.get("display_name") or str(p.get("display_name")).startswith("User ")):
            p["display_name"] = display
        entry = {
            "need_id": row.get("need_id") or raw.get("need_id"),
            "item_name": item,
            "slot": slot,
            "slot_name": slot,
            "label": _phase3_need_label(slot, item),
            "status": row.get("status") or raw.get("status") or "",
            "source": "postgres_phase3",
        }
        # _loot_text bevorzugt item_name. Damit die Loot-Zentrale weiterhin Slot+Item gruppiert,
        # setzen wir item_name auf das sichtbare Label und bewahren das echte Item separat.
        entry["raw_item_name"] = item
        entry["item"] = item
        entry["item_name"] = entry["label"] or item
        p[kind].append(entry)
        need_entries.append({**entry, "user_id": uid, "display_name": display, "need_type": "Main" if kind == "main" else "Secondary"})

    need_items = sorted(players.values(), key=lambda p: str(p.get("display_name") or "").lower())

    # Gebote nach Auktion gruppieren.
    bids_by_auction: dict[str, list[dict[str, Any]]] = {}
    for row in bid_rows:
        raw = _phase3_json(row.get("raw_json"), {})
        aid = str(row.get("auction_id") or raw.get("auction_id") or raw.get("id") or "").strip()
        if not aid:
            continue
        uid = _user_id(row.get("user_id") or raw.get("user_id") or raw.get("bidder_user_id") or raw.get("actor_id"))
        amount = int(_num(row.get("amount") if row.get("amount") is not None else raw.get("amount"), 0))
        bid = {
            **raw,
            "bid_id": row.get("bid_id") or raw.get("bid_id"),
            "auction_id": aid,
            "user_id": uid,
            "display_name": raw.get("display_name") or raw.get("user_name") or raw.get("bidder_name") or names.get(uid) or (f"User {uid}" if uid else "Unbekannt"),
            "amount": amount,
            "created_at": raw.get("created_at") or raw.get("timestamp") or raw.get("time") or str(row.get("mirrored_at") or ""),
            "source": "postgres_phase3",
        }
        bids_by_auction.setdefault(aid, []).append(bid)
    for arr in bids_by_auction.values():
        arr.sort(key=lambda b: (int(_num(b.get("amount"), 0)), str(b.get("created_at") or "")), reverse=True)

    auction_items: list[dict[str, Any]] = []
    for row in auction_rows:
        raw = _phase3_json(row.get("raw_json"), {})
        aid = str(row.get("auction_id") or raw.get("auction_id") or raw.get("id") or "").strip()
        if not aid or aid.lower() in {"items", "count", "by_status", "status", "source", "read_cutover"}:
            continue
        bids = list(bids_by_auction.get(aid) or [])
        existing_bids = [b for b in (raw.get("bids") or []) if isinstance(b, dict)] if isinstance(raw, dict) else []
        # Phase3-Gebote sind aktueller. Fehlende alte Gebote ergänzen, aber nicht doppeln.
        seen_bid_keys = {(str(b.get("user_id") or b.get("bidder_user_id") or ""), str(b.get("amount") or ""), str(b.get("created_at") or b.get("timestamp") or "")) for b in bids}
        for b in existing_bids:
            key = (str(b.get("user_id") or b.get("bidder_user_id") or ""), str(b.get("amount") or ""), str(b.get("created_at") or b.get("timestamp") or ""))
            if key not in seen_bid_keys:
                bids.append(b)
        bids.sort(key=lambda b: (int(_num(b.get("amount"), 0)), str(b.get("created_at") or b.get("timestamp") or "")), reverse=True)
        top = bids[0] if bids else {}
        winner_uid = _user_id(row.get("winner_user_id") or raw.get("winner_user_id") or raw.get("delivered_to_user_id") or raw.get("sold_to_user_id"))
        item = str(row.get("item_name") or raw.get("item_name") or raw.get("item") or raw.get("name") or aid)
        auction = {
            **raw,
            "auction_id": aid,
            "item_name": item,
            "status": row.get("status") or raw.get("status") or "",
            "winner_user_id": winner_uid,
            "winner_name": raw.get("winner_name") or raw.get("delivered_to_name") or names.get(winner_uid) or (f"User {winner_uid}" if winner_uid else ""),
            "current_bid": int(_num(row.get("current_bid"), 0)),
            "top_bid_amount": int(_num(raw.get("top_bid_amount") or raw.get("leader_bid") or raw.get("highest_bid") or (top.get("amount") if top else row.get("current_bid")), 0)),
            "top_bid_user_id": _user_id(raw.get("top_bid_user_id") or raw.get("leader_user_id") or (top.get("user_id") if top else 0)),
            "top_bid_user_name": raw.get("top_bid_user_name") or raw.get("leader_name") or (top.get("display_name") if top else ""),
            "bid_count": len(bids),
            "bids": bids,
            "source": "postgres_phase3",
        }
        auction_items.append(auction)

    history_items: list[dict[str, Any]] = []
    for row in history_rows:
        raw = _phase3_json(row.get("raw_json"), {})
        uid = _user_id(row.get("user_id") or raw.get("user_id") or raw.get("winner_user_id") or raw.get("buyer_user_id"))
        item = str(row.get("item_name") or raw.get("item_name") or raw.get("item") or raw.get("name") or "").strip()
        history_items.append({
            **raw,
            "entry_id": row.get("entry_id") or raw.get("entry_id"),
            "user_id": uid,
            "display_name": raw.get("display_name") or raw.get("winner_name") or raw.get("buyer_name") or names.get(uid) or (f"User {uid}" if uid else ""),
            "item_name": item,
            "amount": int(_num(row.get("amount") if row.get("amount") is not None else raw.get("amount"), 0)),
            "created_at": raw.get("created_at") or raw.get("closed_at") or raw.get("timestamp") or str(row.get("mirrored_at") or ""),
            "source": "postgres_phase3",
        })

    need_log_items: list[dict[str, Any]] = []
    for row in need_log_rows:
        raw = _phase3_json(row.get("raw_json"), {})
        actor = _user_id(row.get("actor_id") or raw.get("actor_id"))
        target = _user_id(row.get("target_user_id") or raw.get("target_user_id") or raw.get("user_id"))
        need_log_items.append({
            **raw,
            "log_id": row.get("log_id") or raw.get("log_id"),
            "request_id": row.get("request_id") or raw.get("request_id"),
            "actor_id": actor,
            "actor_name": raw.get("actor_name") or names.get(actor) or (f"User {actor}" if actor else ""),
            "target_user_id": target,
            "target_name": raw.get("target_name") or raw.get("display_name") or names.get(target) or (f"User {target}" if target else ""),
            "action_type": row.get("action_type") or raw.get("action_type") or raw.get("type") or "",
            "old_item": row.get("old_item") or raw.get("old_item") or "",
            "new_item": row.get("new_item") or raw.get("new_item") or raw.get("item_name") or "",
            "created_at": str(row.get("created_at") or raw.get("created_at") or ""),
            "source": "postgres_phase3",
        })

    auction_items.sort(key=lambda a: str(a.get("ends_at") or a.get("updated_at") or a.get("created_at") or ""), reverse=True)

    return {
        "source": "postgres_phase3_loot_read_cutover",
        "read_cutover": True,
        "needs": {
            "items": need_items,
            "entries": need_entries,
            "entry_count": len(need_entries),
            "player_count": len(need_items),
            "main_count": main_count,
            "secondary_count": secondary_count,
            "source": "postgres_phase3",
        },
        "auctions": {
            "items": auction_items,
            "count": len(auction_items),
            "source": "postgres_phase3",
        },
        "bids": {
            "items": [b for arr in bids_by_auction.values() for b in arr],
            "count": sum(len(arr) for arr in bids_by_auction.values()),
            "source": "postgres_phase3",
        },
        "history": {
            "items": history_items,
            "count": len(history_items),
            "source": "postgres_phase3",
        },
        "need_change_log": {
            "items": need_log_items,
            "count": len(need_log_items),
            "source": "postgres_phase3",
        },
    }


def _apply_phase3_loot_read_cutover(payload: dict[str, Any]) -> dict[str, Any]:
    """Wendet Loot-Read-Cutover auf den Snapshot an, ohne den Snapshot selbst zu verlieren."""
    if not payload.get("ok"):
        return payload
    snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    guild_id = payload.get("guild_id") or snap.get("guild_id") or _env("DASHBOARD_GUILD_ID")
    pg_loot = _phase3_loot_pg_payload(guild_id, snap)
    if not pg_loot:
        payload["phase3_loot_read_cutover"] = {"active": False, "source": "snapshot_fallback", "reason": "Postgres-Loot nicht verfügbar oder leer"}
        return payload
    old_loot = snap.get("loot") if isinstance(snap.get("loot"), dict) else {}
    snap["loot_snapshot_fallback"] = old_loot
    merged = dict(old_loot)
    # Nur die geprüften Phase-3-Bereiche ersetzen. Andere Loot-Teile aus dem Snapshot bleiben erhalten.
    for key in ["needs", "auctions", "bids", "history", "need_change_log"]:
        merged[key] = pg_loot.get(key) or merged.get(key) or {}
    merged["source"] = "postgres_phase3_read_cutover"
    merged["read_cutover"] = True
    snap["loot"] = merged
    payload["snapshot"] = snap
    payload["phase3_loot_read_cutover"] = {
        "active": True,
        "source": "postgres_phase3",
        "need_entries": int((pg_loot.get("needs") or {}).get("entry_count") or 0),
        "need_players": int((pg_loot.get("needs") or {}).get("player_count") or 0),
        "auctions": len((pg_loot.get("auctions") or {}).get("items") or []),
        "bids": int((pg_loot.get("bids") or {}).get("count") or 0),
        "history": int((pg_loot.get("history") or {}).get("count") or 0),
    }
    return payload



# ---------------------------------------------------------------------------
# Phase 3.8: Events/Profile/RSVP Read-Cutover
# ---------------------------------------------------------------------------
# Ziel: Dashboard liest Mitglieder/Profile, Events, RSVPs und Abwesenheiten
# bevorzugt aus den Phase-3-Postgres-Tabellen. Snapshot bleibt Fallback.
# Der Bot/Snapshot bleibt weiterhin kompatibel; kein Write-Cutover.


def _phase3_events_pg_payload(guild_id: Any, snap: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not _database_url():
        return None
    gid = str(guild_id or "").strip()
    if not gid:
        return None
    try:
        conn = _pg_connect()
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT user_id, discord_name, ingame_name, roles_json, raw_json, source, updated_at
                    FROM phase3_members
                    WHERE guild_id = %s
                    ORDER BY COALESCE(NULLIF(ingame_name,''), NULLIF(discord_name,''), user_id) ASC
                    """,
                    (gid,),
                )
                member_rows = cur.fetchall() or []
            except Exception:
                member_rows = []

            try:
                cur.execute(
                    """
                    SELECT event_id, title, status, start_at_text, end_at_text, raw_json, source, updated_at
                    FROM phase3_events
                    WHERE guild_id = %s
                    ORDER BY COALESCE(NULLIF(start_at_text,''), event_id) DESC
                    LIMIT 1000
                    """,
                    (gid,),
                )
                event_rows = cur.fetchall() or []
            except Exception:
                event_rows = []

            try:
                cur.execute(
                    """
                    SELECT rsvp_id, event_id, user_id, response, role_name, display_name, raw_json, source, updated_at
                    FROM phase3_event_rsvps
                    WHERE guild_id = %s
                    ORDER BY event_id ASC, updated_at DESC, display_name ASC
                    LIMIT 5000
                    """,
                    (gid,),
                )
                rsvp_rows = cur.fetchall() or []
            except Exception:
                rsvp_rows = []

            try:
                cur.execute(
                    """
                    SELECT absence_id, user_id, status, start_at_text, end_at_text, raw_json, source, updated_at
                    FROM phase3_absences
                    WHERE guild_id = %s
                    ORDER BY COALESCE(NULLIF(start_at_text,''), absence_id) DESC
                    LIMIT 1000
                    """,
                    (gid,),
                )
                absence_rows = cur.fetchall() or []
            except Exception:
                absence_rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not member_rows and not event_rows and not rsvp_rows and not absence_rows:
        return None

    # Mitglieder/Profile aus DB vorbereiten.
    fallback_names = _profile_name_map(snap)
    member_items: list[dict[str, Any]] = []
    names: dict[int, str] = dict(fallback_names)
    member_ids: set[int] = set()
    for row in member_rows:
        raw = _phase3_json(row.get("raw_json"), {}) if isinstance(row, dict) else {}
        uid = _user_id((row or {}).get("user_id") or raw.get("user_id"))
        if not uid:
            continue
        roles = _phase3_json((row or {}).get("roles_json"), [])
        display = raw.get("display_name") or raw.get("name") or (row or {}).get("ingame_name") or (row or {}).get("discord_name") or f"User {uid}"
        item = {
            **raw,
            "user_id": uid,
            "display_name": display,
            "discord_name": (row or {}).get("discord_name") or raw.get("discord_name") or raw.get("username") or display,
            "ingame_name": (row or {}).get("ingame_name") or raw.get("ingame_name") or raw.get("tl_name") or display,
            "roles": roles if isinstance(roles, list) else [],
            "is_dashboard_member": True,
            "source": "postgres_phase3",
        }
        names[uid] = str(display)
        member_ids.add(uid)
        member_items.append(item)

    # Event-Basis: Snapshot als Fallback behalten, Postgres überschreibt/ergänzt.
    event_by_id: dict[str, dict[str, Any]] = {}
    for ev in (((snap.get("events") or {}).get("items") or [])):
        if not isinstance(ev, dict):
            continue
        eid = str(ev.get("event_id") or ev.get("id") or "").strip()
        if eid:
            event_by_id[eid] = dict(ev)

    for row in event_rows:
        raw = _phase3_json(row.get("raw_json"), {}) if isinstance(row, dict) else {}
        eid = str((row or {}).get("event_id") or raw.get("event_id") or raw.get("id") or "").strip()
        if not eid:
            continue
        base = dict(event_by_id.get(eid) or {})
        base.update(raw if isinstance(raw, dict) else {})
        base.update({
            "event_id": eid,
            "id": base.get("id") or eid,
            "title": (row or {}).get("title") or raw.get("title") or raw.get("name") or base.get("title") or eid,
            "status": (row or {}).get("status") or raw.get("status") or base.get("status") or "",
            "when_iso": (row or {}).get("start_at_text") or raw.get("when_iso") or raw.get("start_at") or base.get("when_iso") or "",
            "start_at": (row or {}).get("start_at_text") or raw.get("start_at") or base.get("start_at") or "",
            "end_at": (row or {}).get("end_at_text") or raw.get("end_at") or base.get("end_at") or "",
            "source": "postgres_phase3",
        })
        event_by_id[eid] = base

    # RSVPs gruppieren und snapshot-kompatibel an Events hängen.
    grouped: dict[str, dict[str, Any]] = {}
    all_rsvp_items: list[dict[str, Any]] = []

    def _rsvp_bucket(response: str, role: str) -> str:
        txt = (response or "").strip().lower()
        role_txt = (role or "").strip().lower()
        if txt in {"no", "nein", "absent", "abgemeldet", "cancelled", "canceled", "declined", "deny"} or "abmeld" in txt:
            return "no"
        if txt in {"maybe", "vielleicht", "tentative", "unsure"} or "vielleicht" in txt:
            return "maybe"
        if role_txt in {"maybe", "vielleicht"}:
            return "maybe"
        if role_txt in {"no", "nein", "abgemeldet"}:
            return "no"
        return "yes"

    for row in rsvp_rows:
        raw = _phase3_json(row.get("raw_json"), {}) if isinstance(row, dict) else {}
        eid = str((row or {}).get("event_id") or raw.get("event_id") or "").strip()
        if not eid:
            continue
        uid = _user_id((row or {}).get("user_id") or raw.get("user_id"))
        response = str((row or {}).get("response") or raw.get("response") or raw.get("status") or "").strip()
        role = str((row or {}).get("role_name") or raw.get("role_name") or raw.get("role") or raw.get("class") or response or "Teilnehmer").strip()
        display = str((row or {}).get("display_name") or raw.get("display_name") or names.get(uid) or (f"User {uid}" if uid else "Unbekannt")).strip()
        bucket = _rsvp_bucket(response, role)
        person = {
            **(raw if isinstance(raw, dict) else {}),
            "user_id": uid,
            "display_name": display,
            "name": display,
            "response": response or bucket,
            "role_name": role,
            "is_dashboard_member": bool(uid and (uid in member_ids or uid in names)),
            "source": "postgres_phase3",
        }
        all_rsvp_items.append({"event_id": eid, **person})
        g = grouped.setdefault(eid, {"yes_groups": {}, "maybe": [], "no": [], "yes_count": 0, "maybe_count": 0, "no_count": 0})
        if bucket == "yes":
            role_label = role or "Teilnehmer"
            g["yes_groups"].setdefault(role_label, []).append(person)
            g["yes_count"] += 1
        elif bucket == "maybe":
            g["maybe"].append(person)
            g["maybe_count"] += 1
        else:
            g["no"].append(person)
            g["no_count"] += 1

    for eid, g in grouped.items():
        ev = dict(event_by_id.get(eid) or {"event_id": eid, "id": eid, "title": f"Event {eid}"})
        yes_groups = []
        yes_counts = {}
        yes_dict = {}
        for role, people in sorted((g.get("yes_groups") or {}).items(), key=lambda kv: str(kv[0]).lower()):
            yes_groups.append({"role": role, "participants": people})
            yes_counts[role] = len(people)
            yes_dict[role] = people
        ev["participants"] = {"yes": yes_groups, "maybe": g.get("maybe") or [], "no": g.get("no") or []}
        ev["yes"] = yes_dict
        ev["yes_counts"] = yes_counts
        ev["participant_count"] = int(g.get("yes_count") or 0)
        ev["maybe_count"] = int(g.get("maybe_count") or 0)
        ev["no_count"] = int(g.get("no_count") or 0)
        ev["rsvp_count"] = int(g.get("yes_count") or 0) + int(g.get("maybe_count") or 0) + int(g.get("no_count") or 0)
        ev["source"] = "postgres_phase3"
        event_by_id[eid] = ev

    event_items = list(event_by_id.values())
    event_items.sort(key=lambda ev: str(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at") or ev.get("event_id") or ""), reverse=True)

    absence_items: list[dict[str, Any]] = []
    for row in absence_rows:
        raw = _phase3_json(row.get("raw_json"), {}) if isinstance(row, dict) else {}
        uid = _user_id((row or {}).get("user_id") or raw.get("user_id"))
        absence_items.append({
            **raw,
            "absence_id": (row or {}).get("absence_id") or raw.get("absence_id") or raw.get("id"),
            "user_id": uid,
            "display_name": raw.get("display_name") or names.get(uid) or (f"User {uid}" if uid else ""),
            "status": (row or {}).get("status") or raw.get("status") or "",
            "start_at": (row or {}).get("start_at_text") or raw.get("start_at") or raw.get("from") or "",
            "end_at": (row or {}).get("end_at_text") or raw.get("end_at") or raw.get("to") or "",
            "source": "postgres_phase3",
        })

    return {
        "source": "postgres_phase3_events_read_cutover",
        "read_cutover": True,
        "profiles": {"items": member_items, "count": len(member_items), "source": "postgres_phase3"},
        "members": {"items": member_items, "count": len(member_items), "source": "postgres_phase3"},
        "events": {"items": event_items, "count": len(event_items), "rsvp_count": len(all_rsvp_items), "source": "postgres_phase3"},
        "event_rsvps": {"items": all_rsvp_items, "count": len(all_rsvp_items), "source": "postgres_phase3"},
        "absences": {"items": absence_items, "count": len(absence_items), "source": "postgres_phase3"},
    }


def _apply_phase3_events_read_cutover(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload.get("ok"):
        return payload
    snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    guild_id = payload.get("guild_id") or snap.get("guild_id") or _env("DASHBOARD_GUILD_ID")
    pg_live = _phase3_events_pg_payload(guild_id, snap)
    if not pg_live:
        payload["phase3_events_read_cutover"] = {"active": False, "source": "snapshot_fallback", "reason": "Postgres-Events/Profile nicht verfügbar oder leer"}
        return payload

    # Fallbacks behalten, damit Debug/Notfall weiterhin möglich bleibt.
    snap["profiles_snapshot_fallback"] = snap.get("profiles") if isinstance(snap.get("profiles"), dict) else {}
    snap["members_snapshot_fallback"] = snap.get("members") if isinstance(snap.get("members"), dict) else {}
    snap["events_snapshot_fallback"] = snap.get("events") if isinstance(snap.get("events"), dict) else {}
    snap["absences_snapshot_fallback"] = snap.get("absences") if isinstance(snap.get("absences"), dict) else {}

    if (pg_live.get("profiles") or {}).get("items"):
        snap["profiles"] = pg_live.get("profiles") or snap.get("profiles") or {}
        snap["members"] = pg_live.get("members") or snap.get("members") or {}
    if (pg_live.get("events") or {}).get("items"):
        old_events = snap.get("events") if isinstance(snap.get("events"), dict) else {}
        merged_events = dict(old_events)
        merged_events.update(pg_live.get("events") or {})
        snap["events"] = merged_events
    if (pg_live.get("absences") or {}).get("items") is not None:
        snap["absences"] = pg_live.get("absences") or snap.get("absences") or {}
    snap["event_rsvps"] = pg_live.get("event_rsvps") or snap.get("event_rsvps") or {}

    payload["snapshot"] = snap
    payload["phase3_events_read_cutover"] = {
        "active": True,
        "source": "postgres_phase3",
        "members": int((pg_live.get("profiles") or {}).get("count") or 0),
        "events": int((pg_live.get("events") or {}).get("count") or 0),
        "event_rsvps": int((pg_live.get("event_rsvps") or {}).get("count") or 0),
        "absences": int((pg_live.get("absences") or {}).get("count") or 0),
    }
    return payload


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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_event_action_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    event_id TEXT NOT NULL DEFAULT '',
                    action_type TEXT NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_dashboard_event_action_requests_lookup
                ON dashboard_event_action_requests (guild_id, event_id, status, requested_at DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_settings_change_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    scope TEXT NOT NULL DEFAULT 'dkp',
                    action_type TEXT NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_dashboard_settings_change_requests_lookup
                ON dashboard_settings_change_requests (guild_id, scope, status, requested_at DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_need_change_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    target_user_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_dashboard_need_change_requests_lookup
                ON dashboard_need_change_requests (guild_id, target_user_id, status, requested_at DESC)
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


# ---------------------------------------------------------------------------
# Step 4: Admin- und Einstellungszentrale
# ---------------------------------------------------------------------------

def _loot_action_requests_for_dashboard(guild_id: int, limit: int = 120) -> list[dict[str, Any]]:
    """Letzte Dashboard-Loot-Aktionen über alle Auktionen.

    Nur Diagnose/Transparenz für die Admin-Zentrale. Keine Schreibzugriffe.
    """
    if not _database_url() or not guild_id:
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
                WHERE guild_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), int(limit)),
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


def _admin_flat_settings(snap: dict[str, Any]) -> list[dict[str, Any]]:
    settings = snap.get("settings") or {}
    out: list[dict[str, Any]] = []
    for row in settings.get("settings") or []:
        if isinstance(row, dict):
            out.append({
                "source": str(row.get("source") or "settings"),
                "key": str(row.get("key") or ""),
                "value": row.get("value"),
            })
    for row in settings.get("channels") or []:
        if isinstance(row, dict):
            out.append({"source": str(row.get("source") or "channels"), "key": str(row.get("key") or "channel"), "value": row.get("name") or row.get("channel_id")})
    for row in settings.get("roles") or []:
        if isinstance(row, dict):
            out.append({"source": str(row.get("source") or "roles"), "key": str(row.get("key") or "role"), "value": row.get("name") or row.get("role_id")})
    return out


def _admin_relevant_rule_rows(snap: dict[str, Any]) -> list[list[Any]]:
    terms = ("ec", "dkp", "loot", "auction", "auktion", "bid", "gebot", "sale", "müll", "trash", "weekly", "week", "limit", "cap", "decay", "verfall", "event", "attendance", "anwesen")
    rows: list[list[Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in _admin_flat_settings(snap):
        key = str(item.get("key") or "")
        source = str(item.get("source") or "")
        hay = (source + " " + key + " " + str(item.get("value") or "")).lower()
        if not any(t in hay for t in terms):
            continue
        ident = (source, key)
        if ident in seen:
            continue
        seen.add(ident)
        rows.append([source, key, _short(item.get("value"), 180)])
    return rows[:220]


def _queue_status_counts(rows: list[dict[str, Any]]) -> Counter:
    return Counter(str(r.get("status") or "unknown").lower() for r in rows if isinstance(r, dict))


def _queue_status_cards(prefix: str, counts: Counter) -> str:
    return "".join([
        _card(f"{prefix} offen", counts.get("pending", 0), "wartet auf Bot"),
        _card(f"{prefix} läuft", counts.get("processing", 0), "wird verarbeitet"),
        _card(f"{prefix} erledigt", counts.get("done", 0), "erfolgreich"),
        _card(f"{prefix} Fehler", counts.get("failed", 0) + counts.get("rejected", 0), "failed/rejected"),
    ])


def _admin_center_payload(data: dict[str, Any]) -> dict[str, Any]:
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    settings = snap.get("settings") or {}
    guild = snap.get("guild") or {}
    storage = snap.get("storage") or {}
    source_rows = _source_health_rows(snap)
    source_ok = sum(1 for r in source_rows if str(r[3]) == "OK")
    source_bad = len(source_rows) - source_ok
    ec_requests = _ec_award_requests_for_dashboard(guild_id, limit=120) if guild_id else []
    loot_requests = _loot_action_requests_for_dashboard(guild_id, limit=120) if guild_id else []
    settings_requests = _settings_change_requests_for_dashboard(guild_id, limit=80) if guild_id else []
    ec_counts = _queue_status_counts(ec_requests)
    loot_counts = _queue_status_counts(loot_requests)
    settings_counts = _queue_status_counts(settings_requests)
    admin_states = _all_member_admin_states(guild_id) if guild_id else []
    admin_log = _admin_action_log(guild_id, limit=160) if guild_id else []

    member_filter = settings.get("member_filter") or ((guild.get("member_filter") or {}))
    if isinstance(member_filter, dict) and member_filter.get("mode") == "discord_role":
        member_filter_text = f"{member_filter.get('role_name')} ({member_filter.get('role_id')})"
    else:
        member_filter_text = "nicht gesetzt"

    auth_rows = [
        {"setting": "DASHBOARD_AUTH_MODE", "value": _auth_mode(), "hint": "basic / hybrid / discord"},
        {"setting": "Discord OAuth", "value": "aktiv" if _discord_oauth_enabled() else "nicht eingerichtet", "hint": "Client ID + Secret"},
        {"setting": "Gildenrolle", "value": member_filter_text, "hint": "Mitgliederfilter"},
        {"setting": "Allowed Role IDs", "value": ", ".join(sorted(_allowed_role_ids())) or "—", "hint": "Login erlaubt"},
        {"setting": "Admin Role IDs", "value": ", ".join(sorted(_admin_role_ids())) or "—", "hint": "Adminrechte"},
        {"setting": "Public Base URL", "value": _env("DASHBOARD_PUBLIC_BASE_URL") or "auto", "hint": "Railway/Domain"},
        {"setting": "Redirect URI", "value": _env("DASHBOARD_DISCORD_REDIRECT_URI") or "auto: /auth/discord/callback", "hint": "Discord Developer Portal"},
        {"setting": "Session Secret", "value": "gesetzt" if _env("DASHBOARD_SESSION_SECRET") else "Fallback", "hint": "Cookie-Signatur"},
    ]

    next_steps = []
    if ec_counts.get("pending", 0):
        next_steps.append("EC-Queue hat offene Anfragen. Bot-Verarbeitung prüfen.")
    if loot_counts.get("pending", 0):
        next_steps.append("Loot-Queue hat offene Dashboard-Aktionen. Bot-Verarbeitung prüfen.")
    if source_bad:
        next_steps.append("Mindestens eine Datenquelle fehlt oder hat Fehler.")
    if not _discord_oauth_enabled():
        next_steps.append("Discord OAuth nicht vollständig eingerichtet. Dashboard-Bieten braucht Discord-Login.")
    if not next_steps:
        next_steps.append("Keine akuten Admin-Warnungen erkannt.")

    return {
        "ok": True,
        "guild_id": guild_id,
        "snapshot_id": data.get("id"),
        "published_at": data.get("published_at"),
        "schema_version": snap.get("schema_version"),
        "backend": storage.get("runtime_backend"),
        "database_url_kind": storage.get("database_url_kind"),
        "source_ok": source_ok,
        "source_bad": source_bad,
        "source_rows": source_rows,
        "auth_rows": auth_rows,
        "rule_rows": _admin_relevant_rule_rows(snap),
        "ec_requests": ec_requests,
        "loot_requests": loot_requests,
        "ec_counts": dict(ec_counts),
        "loot_counts": dict(loot_counts),
        "settings_requests": settings_requests,
        "settings_counts": dict(settings_counts),
        "admin_states": admin_states,
        "admin_log": admin_log,
        "next_steps": next_steps,
        "counts": settings.get("counts") or {},
    }


def _render_admin_center_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Admin · Ebo Dashboard", f"<section class='panel'><h1>🛡️ Admin-Zentrale</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    p = _admin_center_payload(data)
    guild_id = p.get("guild_id")
    ec_counts = Counter(p.get("ec_counts") or {})
    loot_counts = Counter(p.get("loot_counts") or {})
    settings_counts = Counter(p.get("settings_counts") or {})

    overview_cards = "".join([
        _card("Guild-ID", guild_id or "—", "Dashboard-Kontext"),
        _card("Snapshot", p.get("snapshot_id") or "—", _dt(p.get("published_at"))),
        _card("Backend", p.get("backend") or "—", p.get("database_url_kind") or "—"),
        _card("Quellen OK", p.get("source_ok", 0), f"Fehler/fehlen: {p.get('source_bad', 0)}"),
        _card("Admin-Markierungen", len(p.get("admin_states") or []), "interne Leitungsnotizen"),
        _card("Admin-Log", len(p.get("admin_log") or []), "letzte Web-Aktionen"),
        _card("Settings offen", settings_counts.get("pending", 0), f"erledigt: {settings_counts.get('done', 0)}"),
    ])
    queue_cards = _queue_status_cards("EC", ec_counts) + _queue_status_cards("Loot", loot_counts) + _queue_status_cards("Settings", settings_counts)

    auth_rows = [[r.get("setting"), r.get("value"), r.get("hint")] for r in p.get("auth_rows") or []]
    rule_rows = p.get("rule_rows") or []
    source_rows = p.get("source_rows") or []

    ec_rows = []
    for r in p.get("ec_requests") or []:
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        title = payload.get("event_title") or r.get("event_id")
        ec_rows.append([_dt(r.get("requested_at")), _ec_award_status_label(r.get("status")), _event_link(r.get("event_id"), title), _fmt_ec(result.get("total_ec") if result.get("total_ec") is not None else payload.get("total_ec")), r.get("actor_name") or r.get("actor_id") or "—", _short(result.get("error") or r.get("request_id"), 100)])

    loot_rows = []
    for r in p.get("loot_requests") or []:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        item = payload.get("item_name") or payload.get("item") or r.get("auction_id")
        status = _ec_award_status_label(r.get("status")).replace("EC", "")
        loot_rows.append([_dt(r.get("requested_at")), status, _auction_link(r.get("auction_id"), item), r.get("action_type"), _fmt_ec(r.get("amount")), r.get("actor_name") or r.get("actor_id") or "—", _short(result.get("error") or result.get("message") or r.get("request_id"), 100)])

    setting_req_rows = []
    for r in p.get("settings_requests") or []:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        detail = result.get("message") or result.get("error") or payload.get("event_type") or payload.get("weekly_event_limit") or payload.get("decay_percent") or r.get("request_id")
        setting_req_rows.append([_dt(r.get("requested_at")), _ec_award_status_label(r.get("status")).replace("EC", ""), r.get("action_type"), _short(detail, 140), r.get("actor_name") or r.get("actor_id") or "—"])

    state_rows = []
    for st in p.get("admin_states") or []:
        uid = _user_id(st.get("member_user_id"))
        state_rows.append([_member_link(uid, names.get(uid, f"User {uid}")), _status_label(st.get("status")), _short(st.get("note"), 160) or "—", st.get("updated_by_name") or st.get("updated_by_id") or "—", _dt(st.get("updated_at"))])

    log_rows = []
    for lg in p.get("admin_log") or []:
        uid = _user_id(lg.get("target_id"))
        log_rows.append([_dt(lg.get("created_at")), lg.get("action_type"), _member_link(uid, names.get(uid, f"User {uid}")) if uid else lg.get("target_id"), lg.get("actor_name") or lg.get("actor_id") or "—"])

    next_rows = [[x] for x in p.get("next_steps") or []]
    body = f"""
    <nav class="topnav"><a href="/">Kommando</a><a href="/members">Mitglieder</a><a href="/loot">Loot</a><a href="/ec">EC</a><a href="/ec-queue">EC-Queue</a><a href="/admin-settings">EC-Regeln bearbeiten</a><a href="/settings">Setup</a><a href="/system">System</a><a href="/audit">Audit</a><a href="/api/admin-center">API</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Schritt 4 · Admin & Einstellungen</div>
        <h1>🛡️ Admin-Zentrale</h1>
        <p class="muted">Ein Ort für Rechte, Bot-Setup, Quellenstatus, Dashboard-Queues und Leitungsnotizen. Keine Bot-JSON-Schreiberei.</p>
      </div>
      <a class="btn" href="/export/admin_center.csv">CSV</a>
    </section>
    <section class="grid">{overview_cards}</section>
    <section class="grid mini-grid">{queue_cards}</section>
    <section class="panel"><h2>🚦 Nächste Prüfpunkte</h2>{_table(['Hinweis'], next_rows, placeholder='Hinweise durchsuchen…')}</section>
    <section class="panel"><h2>🔐 Login & Rollen</h2><p class="muted">Diese Werte kommen aus Railway-Variablen bzw. dem Snapshot. Änderungen weiter über Railway/Discord, nicht direkt über diese Seite.</p>{_table(['Setting','Wert','Hinweis'], auth_rows, placeholder='Login durchsuchen…')}</section>
    <section class="panel"><h2>🧾 Erkannte EC-/Loot-/Event-Regeln</h2><p class="muted">Gefilterte Snapshot-Settings. Wenn hier eine Regel fehlt, exportiert der Bot sie aktuell nicht ins Dashboard.</p>{_table(['Quelle','Key','Wert'], rule_rows, placeholder='Regeln durchsuchen…')}</section>
    <section class="panel"><h2>⚙️ EC-Regeln bearbeiten</h2><p class="muted">EC-Werte, Wochenlimit und Verfall laufen sicher über eine Bot-Queue. Das Dashboard ändert keine Bot-JSON direkt.</p><a class="btn" href="/admin-settings">Zur Einstellungsseite</a></section>
    <section class="panel"><h2>🪙 EC-Queue zuletzt</h2>{_table(['Zeit','Status','Event','EC','Akteur','Resultat'], ec_rows, placeholder='EC-Queue durchsuchen…')}</section>
    <section class="panel"><h2>⚙️ Settings-Queue zuletzt</h2>{_table(['Zeit','Status','Aktion','Details','Akteur'], setting_req_rows, placeholder='Settings-Queue durchsuchen…')}</section>
    <section class="panel"><h2>🎁 Loot-Dashboard-Aktionen zuletzt</h2>{_table(['Zeit','Status','Auktion/Item','Aktion','EC','Akteur','Resultat'], loot_rows, placeholder='Loot-Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>👥 Markierte Mitglieder</h2>{_table(['Mitglied','Status','Notiz','Geändert von','Geändert am'], state_rows, placeholder='Mitglieder durchsuchen…')}</section>
    <section class="panel"><h2>🧩 Datenquellen</h2>{_table(['Key','Datei','vorhanden','Status','Bytes','Geändert'], source_rows, placeholder='Quellen durchsuchen…')}</section>
    <section class="panel"><h2>🧾 Web-Admin-Aktionslog</h2>{_table(['Zeit','Aktion','Ziel','Akteur'], log_rows, placeholder='Adminlog durchsuchen…')}</section>
    """
    return _html_shell("Admin-Zentrale · Ebo Dashboard", body)


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
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/ec">EC-Verlauf</a><a href="/member/{_e(user_id)}/loot">Loot-Verlauf</a><a href="#needs">Needs</a><a href="#ec">EC</a><a href="#voice">Voice</a><a href="/api/snapshot">JSON</a></nav>
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


def _junk_roll_entries_dashboard(auction: dict[str, Any]) -> list[dict[str, Any]]:
    """Dashboard-kompatible Müll-Würfe.

    Der Bot speichert neue Müll-Würfe als Dict nach User-ID:
    {"123": {"user_id": 123, "roll": 77, ...}}.
    Ältere/andere Dashboard-Ansichten erwarteten eine Liste. Diese Normalisierung
    verhindert, dass echte Würfe zwar gespeichert sind, aber im Dashboard leer wirken.
    """
    raw = auction.get("junk_rolls")
    entries: list[dict[str, Any]] = []

    if isinstance(raw, dict):
        for uid_key, value in raw.items():
            if isinstance(value, dict):
                entry = dict(value)
            else:
                entry = {"roll": value}
            if not entry.get("user_id"):
                entry["user_id"] = uid_key
            entries.append(entry)
    elif isinstance(raw, list):
        for value in raw:
            if isinstance(value, dict):
                entries.append(dict(value))

    def _sort_key(entry: dict[str, Any]) -> tuple[int, str]:
        try:
            roll = int(_num(entry.get("roll"), 0))
        except Exception:
            roll = 0
        return (roll, str(entry.get("created_at") or ""))

    entries.sort(key=_sort_key, reverse=True)
    return entries


def _junk_user_has_roll(auction: dict[str, Any], user_id: Any) -> bool:
    uid = str(_user_id(user_id) or "").strip()
    if not uid:
        return False
    for entry in _junk_roll_entries_dashboard(auction):
        if str(_user_id(entry.get("user_id")) or "") == uid:
            return True
    return False


def _snapshot_display_name(snap: dict[str, Any], user_id: Any, fallback: Any = "") -> str:
    uid = _user_id(user_id)
    text = str(fallback or "").strip()
    # Wenn nur eine nackte ID oder "User 123" aus der DB kommt, lieber den Discord-/Profilnamen aus dem Snapshot nehmen.
    weak_fallback = (not text) or text.isdigit() or (uid and text.lower() == f"user {uid}".lower()) or text.lower().startswith("user ")
    if uid:
        name = _profile_name_map(snap).get(uid)
        if name and weak_fallback:
            return name
        if name and not text:
            return name
    if text:
        return text
    return f"User {uid}" if uid else "—"


def _member_link_from_snapshot(snap: dict[str, Any], user_id: Any, fallback: Any = "") -> dict[str, str]:
    uid = _user_id(user_id)
    return _member_link(uid, _snapshot_display_name(snap, uid, fallback))


def _junk_roll_rows(auction: dict[str, Any], snap: Optional[dict[str, Any]] = None) -> list[list[Any]]:
    rows: list[list[Any]] = []
    snap = snap or {}
    for r in _junk_roll_entries_dashboard(auction):
        uid = _user_id(r.get("user_id"))
        rows.append([_member_link_from_snapshot(snap, uid, r.get("display_name") or r.get("name")), r.get("roll"), _dt(r.get("created_at"))])
    return rows


def _auction_roll_count(auction: dict[str, Any]) -> int:
    return len(_junk_roll_entries_dashboard(auction))


def _auction_count_label(auction: dict[str, Any]) -> tuple[str, int, str]:
    if auction.get("junk_drop"):
        rolls = _auction_roll_count(auction)
        return "Würfe", rolls, "Müll-Würfe"
    return "Gebote", _loot_bid_count(auction), "Gebote"


def _best_junk_roll_text(auction: dict[str, Any], snap: Optional[dict[str, Any]] = None) -> str:
    snap = snap or {}
    entries = _junk_roll_entries_dashboard(auction)
    if not entries:
        return "—"
    best = entries[0]
    uid = _user_id(best.get("user_id"))
    name = _snapshot_display_name(snap, uid, best.get("display_name") or best.get("name"))
    roll = best.get("roll")
    return f"{name} · {roll}"


def _auction_timer_dt(auction: dict[str, Any]) -> Optional[datetime]:
    # Normale Auktionen/Sales nutzen ends_at. Müll-Drops haben teils kein ends_at,
    # aber eine Würfelphase über junk_roll_until. Genau diese Zeit ist für den Nutzer relevant.
    for key in ("ends_at", "expires_at", "end_at"):
        dt = _dt_obj(auction.get(key))
        if dt:
            return dt
    if auction.get("junk_drop"):
        return _dt_obj(auction.get("junk_roll_until"))
    return None


def _auction_timer_text(auction: dict[str, Any]) -> str:
    dt = _auction_timer_dt(auction)
    if not dt:
        return "—"
    seconds = int((dt - datetime.now(timezone.utc)).total_seconds())
    if seconds <= 0:
        return "abgelaufen"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} Tage")
    if hours or days:
        parts.append(f"{hours} Std")
    parts.append(f"{minutes} Min")
    parts.append(f"{secs} Sek")
    return " ".join(parts)


def _auction_timer_subtext(auction: dict[str, Any]) -> str:
    dt = _auction_timer_dt(auction)
    if not dt:
        return f"Start: {_dt(auction.get('created_at'))}"
    if auction.get("junk_drop") and not _dt_obj(auction.get("ends_at")):
        return f"Würfelphase bis: {_dt(dt.isoformat())}"
    return f"Ende: {_dt(dt.isoformat())}"


def _auction_leader_or_roll_text(auction: dict[str, Any], snap: Optional[dict[str, Any]] = None) -> str:
    snap = snap or {}
    if auction.get("junk_drop"):
        return _best_junk_roll_text(auction, snap)
    uid = _user_id(auction.get("top_bid_user_id"))
    amount = auction.get("top_bid_amount")
    if uid and amount is not None:
        return f"{_snapshot_display_name(snap, uid, auction.get('top_bid_user_name'))} · {_fmt_ec(amount)} EC"
    return "—"


def _auction_eligible_card_value(auction: dict[str, Any]) -> tuple[str, str]:
    mode = str(auction.get("eligibility_mode") or "").strip().lower()
    phase = str(auction.get("phase") or "").strip().lower()
    if mode in {"all", "free", "sale"} or phase in {"free", "sale"} or auction.get("junk_drop"):
        return "Alle", "alle Gildenmitglieder"
    return str(auction.get("eligible_count", 0)), auction.get("eligibility_mode") or "—"


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
        "loot_drop": "Loot gedroppt",
        "junk_drop": "Müll gedroppt",
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


def _loot_is_ended(auction: dict[str, Any]) -> bool:
    end_dt = _dt_obj(auction.get("ends_at"))
    if not end_dt:
        return False
    return datetime.now(timezone.utc) >= end_dt


def _loot_top_bid_user_id(auction: dict[str, Any]) -> int:
    top_amount = None
    top_user = 0
    if auction.get("top_bid_user_id") is not None:
        top_user = _user_id(auction.get("top_bid_user_id"))
        try:
            top_amount = int(_num(auction.get("top_bid_amount"), 0))
        except Exception:
            top_amount = None
    for b in auction.get("bids") or []:
        if not isinstance(b, dict):
            continue
        try:
            amount = int(_num(b.get("amount"), 0))
        except Exception:
            continue
        if top_amount is None or amount > top_amount:
            top_amount = amount
            top_user = _user_id(b.get("user_id") or b.get("member_id") or b.get("discord_id"))
    return int(top_user or 0)


def _loot_bid_precheck(guild_id: int, auction: dict[str, Any], amount: int, actor: Optional[dict[str, Any]], snap: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    auction_id = str(auction.get("auction_id") or auction.get("id") or "").strip()
    uid = _user_id((actor or {}).get("user_id"))
    if not uid:
        errors.append("Discord-Login erforderlich. Mit Basic-Login kann das Dashboard nicht wissen, wer bietet.")
    if not auction_id:
        errors.append("Auktion fehlt.")
    if not _loot_is_active(auction):
        errors.append("Auktion ist nicht mehr aktiv.")
    if _loot_is_sale_like(auction):
        errors.append("Diese Auktion ist ein Sale/Müll-Item. Nutze Kaufen/Würfeln.")
    if _loot_is_ended(auction):
        errors.append("Auktion ist bereits abgelaufen.")
    try:
        amount = int(amount)
    except Exception:
        amount = 0
    if amount <= 0:
        errors.append("Gebot muss größer als 0 EC sein.")
    if amount > 1_000_000:
        errors.append("Gebot ist unplausibel hoch.")
    min_next = _loot_min_next_bid(auction)
    if amount < min_next:
        errors.append(f"Mindestgebot ist aktuell {min_next} EC.")
    mode = _loot_status(auction.get("eligibility_mode"))
    eligible = _loot_auction_eligible_user_ids(auction)
    if uid and mode in {"main_need", "secondary_need"}:
        label = "Main-Need-Spieler" if mode == "main_need" else "Second-Need-Spieler"
        if not eligible:
            errors.append(f"Diese Need-Auktion hat aktuell keine berechtigten {label} im Snapshot.")
        elif uid not in eligible:
            errors.append(f"Aktuell nur für berechtigte {label}.")
    top_uid = _loot_top_bid_user_id(auction)
    if uid and top_uid and top_uid == uid:
        errors.append("Du bist bereits führend. Warte, bis dich jemand überbietet.")
    balances = _balance_map(snap)
    if uid and uid in balances and amount > balances.get(uid, 0):
        errors.append(f"Du hast aktuell nur {_fmt_ec(balances.get(uid, 0))} EC.")
    if uid:
        active_req = _loot_action_active_for_actor(int(guild_id), str(auction_id), str(uid))
        if active_req:
            errors.append(f"Du hast bereits eine offene Dashboard-Aktion: {_loot_action_type_label(active_req.get('action_type'))} · {_loot_action_status_label(active_req.get('status'))}.")
    return errors


def _loot_sale_precheck(guild_id: int, auction: dict[str, Any], actor: Optional[dict[str, Any]], snap: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    auction_id = str(auction.get("auction_id") or auction.get("id") or "").strip()
    uid = _user_id((actor or {}).get("user_id"))
    if not uid:
        errors.append("Discord-Login erforderlich. Mit Basic-Login kann das Dashboard nicht wissen, wer kauft/würfelt.")
    if not auction_id:
        errors.append("Auktion fehlt.")
    if not _loot_is_active(auction):
        errors.append("Auktion ist nicht mehr aktiv.")
    if not _loot_is_sale_like(auction):
        errors.append("Diese Auktion ist kein Sale/Müll-Item.")
    if _loot_is_ended(auction) and not bool(auction.get("junk_drop")):
        errors.append("Sale-Item ist bereits abgelaufen.")
    price = int(_num(auction.get("fixed_price") if auction.get("fixed_price") is not None else auction.get("start_bid"), 0))
    balances = _balance_map(snap)
    if uid and price > 0 and uid in balances and balances.get(uid, 0) < price:
        errors.append(f"Du hast aktuell nur {_fmt_ec(balances.get(uid, 0))} EC, benötigt werden {price} EC.")
    if uid and bool(auction.get("junk_drop")) and price <= 0 and _junk_user_has_roll(auction, uid):
        errors.append("Du hast für dieses Müll-Item bereits gewürfelt.")
    if uid:
        active_req = _loot_action_active_for_actor(int(guild_id), str(auction_id), str(uid))
        if active_req:
            errors.append(f"Du hast bereits eine offene Dashboard-Aktion: {_loot_action_type_label(active_req.get('action_type'))} · {_loot_action_status_label(active_req.get('status'))}.")
    return errors


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
        warnings.append("Du bist laut Snapshot für diese Need-Phase nicht berechtigt.")
    if _loot_is_ended(auction):
        warnings.append("Diese Auktion ist laut Snapshot bereits abgelaufen.")
    active_req = _loot_action_active_for_actor(int(guild_id), auction_id, str(user_id))
    if active_req:
        warnings.append(f"Du hast bereits eine offene Dashboard-Aktion: {_loot_action_type_label(active_req.get('action_type'))} · {_loot_action_status_label(active_req.get('status'))}.")
    warn_html = "".join(f"<p class='muted'>⚠️ {_e(w)}</p>" for w in warnings)
    bal_text = _fmt_ec(balance) + " EC" if balance is not None else "im Snapshot nicht geladen"

    if _loot_is_sale_like(auction):
        price = int(_num(auction.get("fixed_price") if auction.get("fixed_price") is not None else auction.get("start_bid"), 0))
        is_junk = bool(auction.get("junk_drop")) and price <= 0
        action = "junk_roll" if is_junk else "sale_buy"
        already_rolled = bool(is_junk and _junk_user_has_roll(auction, user_id))
        label = ("✅ Bereits gewürfelt" if already_rolled else "🎲 Müll würfeln") if is_junk else ("Gratis nehmen" if price <= 0 else f"Sofort kaufen für {price} EC")
        sale_errors = _loot_sale_precheck(int(guild_id), auction, current_user, snap)
        if sale_errors:
            warn_html += "".join(f"<p class='muted'>⛔ {_e(w)}</p>" for w in sale_errors if w not in warnings)
        disabled = "disabled" if sale_errors else ""
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
    bid_errors = _loot_bid_precheck(int(guild_id), auction, int(min_next), current_user, snap)
    if bid_errors:
        warn_html += "".join(f"<p class='muted'>⛔ {_e(w)}</p>" for w in bid_errors if w not in warnings)
    disabled = "disabled" if bid_errors else ""
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



def _enqueue_loot_drop_request(guild_id: int, drop_type: str, item_query: str, actor: dict[str, Any]) -> dict[str, Any]:
    """Dashboard -> Bot Queue: normale Drops/Müll-Drops anlegen.

    Das Dashboard erstellt die Auktion nicht selbst. Es legt nur eine Anfrage in
    Postgres ab. Der Bot verarbeitet sie mit der bestehenden Discord-Logik
    (Need-Prüfung, Auktion erstellen, DMs, Discord-Nachrichten, Spiegelung).
    """
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt. Ohne Postgres kann das Dashboard keine Bot-Aktion anstoßen."}
    gid = int(guild_id or 0)
    if not gid:
        return {"ok": False, "error": "Guild fehlt."}
    item = str(item_query or "").strip()
    if not item:
        return {"ok": False, "error": "Itemname fehlt."}
    if len(item) > 160:
        item = item[:160]
    actor_id = str(actor.get("user_id") or actor.get("id") or "").strip()
    actor_name = str(actor.get("username") or actor.get("global_name") or actor_id or "Dashboard")
    if not actor_id:
        return {"ok": False, "error": "Discord-Login erforderlich. Das Dashboard muss wissen, welcher Admin den Drop meldet."}
    dtype = str(drop_type or "").strip().lower()
    if dtype in {"loot", "loot_drop", "normal", "item"}:
        action = "loot_drop"
        label = "Loot gedroppt"
    elif dtype in {"junk", "junk_drop", "muell", "müll", "trash"}:
        action = "junk_drop"
        label = "Müll gedroppt"
    else:
        return {"ok": False, "error": "Unbekannter Drop-Typ."}

    request_id = f"dash-drop-{int(time.time())}-{secrets.token_hex(6)}"
    virtual_auction_id = f"new:{action}:{request_id}"
    payload = {
        "action_type": action,
        "drop_type": dtype,
        "item_query": item,
        "item_name": item,
        "requested_by": {"id": actor_id, "name": actor_name},
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source": "dashboard_loot_drop",
    }
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_loot_action_requests
                (request_id, guild_id, auction_id, action_type, amount, status, payload_json, actor_id, actor_name)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s)
                RETURNING id, requested_at
                """,
                (
                    request_id,
                    gid,
                    virtual_auction_id,
                    action,
                    0,
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    actor_id,
                    actor_name,
                ),
            )
            row = cur.fetchone() or {}
        conn.commit()
        return {"ok": True, "request_id": request_id, "queue_id": row.get("id"), "requested_at": str(row.get("requested_at") or ""), "message": f"{label} wurde an den Bot gesendet."}
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
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

    count_title, count_value, count_sub = _auction_count_label(auction)
    leader = _auction_leader_or_roll_text(auction, snap)
    leader_label = "Bester Wurf" if auction.get("junk_drop") else "Führend"
    winner = "—"
    if auction.get("winner_user_id"):
        winner = _snapshot_display_name(snap, auction.get("winner_user_id"), auction.get("winner_name"))
    elif auction.get("winner_name"):
        winner = str(auction.get("winner_name"))
    eligible_value, eligible_sub = _auction_eligible_card_value(auction)

    cards = "".join([
        _card("Status", _loot_effective_status_key(auction), _phase_label(auction)),
        _card(count_title, count_value, f"{leader_label}: {leader}"),
        _card("Gewinner", winner, _dt(auction.get("delivered_at")) if auction.get("delivered_at") else "noch offen"),
        _card("Timer", _auction_timer_text(auction), _auction_timer_subtext(auction)),
        _card("Regel", _loot_phase_window_text(auction), "Soll-Laufzeit"),
        _card("Startgebot", _fmt_ec(auction.get("start_bid")), "EC"),
        _card("Mindestschritt", _fmt_ec(auction.get("min_increment")), "EC"),
        _card("Festpreis", _fmt_ec(auction.get("fixed_price")), "Sale"),
        _card("Berechtigt", eligible_value, eligible_sub),
    ])

    bid_rows = _bid_rows(auction)
    eligible_rows = _eligible_rows(auction)
    roll_rows = _junk_roll_rows(auction, snap)
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
          {_table(['Spieler','Wurf','Zeit'], roll_rows, placeholder='Würfe durchsuchen…')}
        </section>
        """

    guild_id = _safe_guild_id(data)
    action_panel = _loot_dashboard_action_panel(int(guild_id), auction, current_user, snap) if guild_id else ""
    queue_panel = _loot_action_queue_panel(int(guild_id), auction_id) if guild_id else ""
    recent_actions = _loot_action_requests_for_auction(int(guild_id), auction_id, limit=10) if guild_id else []
    queue_counts = Counter(str(r.get("status") or "").lower() for r in recent_actions)
    auto_refresh = bool(_loot_is_active(auction) or queue_counts.get("pending") or queue_counts.get("processing"))
    refresh_panel = ""
    if auto_refresh:
        refresh_panel = """
        <section class='panel'>
          <h2>🔄 Live-Aktualisierung</h2>
          <p class='muted'>Diese Seite lädt sich alle 15 Sekunden neu, solange die Auktion aktiv ist oder Dashboard-Aktionen offen sind.</p>
          <script>setTimeout(function(){ if(!document.hidden){ window.location.reload(); } }, 15000);</script>
        </section>
        """
    msg_panel = f"<section class='panel'><p>{_e(msg)}</p></section>" if msg else ""
    state_panel = ""
    if _loot_effective_status_key(auction) == "expired_waiting":
        state_panel = """
        <section class='panel'>
          <h2>⚠️ Abgelaufen, wartet auf Bot</h2>
          <p class='muted'>Diese Auktion hat laut Enddatum die Laufzeit überschritten, steht in Postgres/Snapshot aber noch auf aktiv. Der Bot sollte sie im nächsten Auktions-Loop schließen oder in die nächste Phase schieben.</p>
        </section>
        """

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
    {state_panel}
    {refresh_panel}
    <section class="grid">{cards}</section>
    {action_panel}
    {queue_panel}
    <section class="panel" id="bids"><h2>💰 Gebotshistorie</h2>{_table(['Spieler','Gebot','Zeit'], bid_rows, placeholder='Gebote durchsuchen…')}</section>
    {extra_roll_section}
    <section class="panel" id="eligible"><h2>✅ Berechtigte Spieler</h2><p class="muted">Bei freien Auktionen/Sale kann die Liste leer sein, weil dann alle berechtigt sind.</p>{_table(['Spieler'], eligible_rows, placeholder='Berechtigte durchsuchen…')}</section>
    <section class="panel" id="tech"><h2>🧾 Technische Infos</h2>{_table(['Bereich','Kanal-ID','Nachricht-ID'], channel_info, searchable=False)}</section>
    """
    return _html_shell(f"{auction.get('item_name') or 'Auktion'} · Ebo Dashboard", body)


def _sidebar_html() -> str:
    return f"""
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark"><img src="{_asset('ebolus_logo.png')}" alt="Ebolus"></div>
        <div><strong>Ebolus</strong><span>Gilden-Dashboard</span></div>
      </div>
      <button class="mobile-nav-toggle" type="button" onclick="document.body.classList.toggle('nav-open')">☰ Menü</button>

      <nav class="side-nav">
        <a href="/" data-nav="home">🏰 Kommando</a>
        <a href="/overview" data-nav="overview">📊 Gesamtübersicht</a>

        <details open>
          <summary>Gilde</summary>
          <a href="/members">👥 Mitglieder</a>
        <a href="/portal">👤 Mein Portal</a>
          <a href="/events">📅 Events</a>
          <a href="/attendance">✅ Anwesenheit</a>
          <a href="/attendance-stats">📈 Stats</a>
          <a href="/attendance-archive">📦 Archiv</a>
        </details>

        <details open>
          <summary>Loot & Auktionen</summary>
          <a href="/loot">🏆 Loot-Zentrale</a>
          <a href="/loot-check">🔎 Truhencheck</a>
          <a href="/loot-history">📜 Loot-Verlauf</a>
          <a href="/needs">🎁 Needs</a>
          <a href="/fairness">⚖️ Fairness</a>
        </details>

        <details open>
          <summary>EC</summary>
          <a href="/ec">🪙 EC-Verlauf</a>
          <a href="/ec-queue">🌐 EC-Queue</a>
        </details>

        <details>
          <summary>Auswertung</summary>
          <a href="/analytics">📊 Analytics</a>
          <a href="/voice">🎙️ Voice</a>
          <a href="/exports">📤 Exports</a>
        </details>

        <details>
          <summary>Leitung & System</summary>
          <a href="/admin">🛡️ Admin</a>
          <a href="/settings">⚙️ Einstellungen</a>
          <a href="/audit">🧾 Audit</a>
          <a href="/system">🧰 System</a>
        </details>
      </nav>

      <div class="sidebar-footer">
        <a href="/me">Mein Login</a>
        <a href="/release">Release</a>
        <a href="/logout">Logout</a>
        <span class="version-pill">v{_e(DASHBOARD_RELEASE_VERSION)}</span>
      </div>
    </aside>
    """




def _member_sidebar_html() -> str:
    return f"""
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark"><img src="{_asset('ebolus_logo.png')}" alt="Ebolus"></div>
        <div><strong>Ebolus</strong><span>Mitgliederbereich</span></div>
      </div>
      <button class="mobile-nav-toggle" type="button" onclick="document.body.classList.toggle('nav-open')">☰ Menü</button>

      <nav class="side-nav">
        <a href="/member" data-nav="member-home">🏠 Start</a>
        <a href="/member#members" data-nav="member-members">👥 Mitglieder</a>
        <a href="/member#events" data-nav="member-events">📅 Laufende Events</a>
        <a href="/member#auctions" data-nav="member-auctions">🏆 Laufende Auktionen</a>
        <a href="/portal" data-nav="member-profile">👤 Eigenes Profil</a>
        <a href="/portal#needs" data-nav="member-needs">🎁 Meine Needs</a>
      </nav>

      <div class="sidebar-footer">
        <a href="/me">Mein Login</a>
        <a href="/logout">Logout</a>
        <span class="version-pill">v{_e(DASHBOARD_RELEASE_VERSION)}</span>
      </div>
    </aside>
    """


def _html_shell(title: str, body: str, *, nav_mode: str = "admin") -> str:
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
    :root {{ --bg:#0f1014; --panel:#181a22; --panel2:#20232d; --text:#f1eadb; --muted:#a8a193; --gold:#d6a84f; --line:#333746; --red:#d96868; --green:#81c784; --side:#11121a; --side2:#171824; }}
    * {{ box-sizing:border-box; }} html {{ scroll-behavior:smooth; }}
    body {{ margin:0; font-family:Inter, system-ui, Segoe UI, sans-serif; background:linear-gradient(180deg,rgba(15,16,20,.78),rgba(15,16,20,.98)), url("{_asset('dashboard_bg.webp')}") center top / cover fixed no-repeat; color:var(--text); overflow-x:hidden; }}
    .app-shell {{ display:grid; grid-template-columns:260px minmax(0,1fr); min-height:100vh; }}
    .sidebar {{ position:sticky; top:0; height:100vh; overflow:auto; scrollbar-width:none; -ms-overflow-style:none; padding:18px 14px; background:linear-gradient(180deg,rgba(17,18,26,.97),rgba(11,12,18,.97)); border-right:1px solid rgba(214,168,79,.16); box-shadow:16px 0 45px rgba(0,0,0,.35); }}
    .sidebar::-webkit-scrollbar {{ width:0; height:0; display:none; }}
    .mobile-nav-toggle {{ display:none; border:1px solid rgba(214,168,79,.24); border-radius:12px; background:linear-gradient(180deg,rgba(32,35,45,.92),rgba(13,14,20,.86)); color:var(--text); font-weight:800; padding:10px 12px; cursor:pointer; }}
    .brand {{ display:flex; align-items:center; gap:12px; padding:8px 8px 18px; margin-bottom:8px; border-bottom:1px solid rgba(214,168,79,.14); }}
    .brand-mark {{ width:54px; height:54px; border-radius:16px; background:radial-gradient(circle at 35% 30%,rgba(214,168,79,.18),rgba(32,35,45,.9)); border:1px solid rgba(214,168,79,.24); display:grid; place-items:center; overflow:hidden; }}
    .brand-mark img {{ width:52px; height:52px; object-fit:contain; filter:drop-shadow(0 2px 8px rgba(0,0,0,.75)); }}
    .brand strong {{ display:block; font-size:18px; }} .brand span {{ display:block; color:var(--muted); font-size:12px; }}
    .side-nav {{ display:flex; flex-direction:column; gap:6px; scrollbar-width:none; -ms-overflow-style:none; }}
    .side-nav::-webkit-scrollbar, .sidebar-footer::-webkit-scrollbar {{ width:0; height:0; display:none; }}
    .side-nav a, .side-nav summary {{ color:var(--text); text-decoration:none; padding:10px 12px; border-radius:12px; display:flex; align-items:center; gap:9px; font-size:14px; cursor:pointer; }}
    .side-nav a:hover, .side-nav summary:hover {{ background:rgba(214,168,79,.09); color:var(--gold); }}
    .side-nav a.active {{ background:linear-gradient(90deg,rgba(214,168,79,.18),rgba(214,168,79,.06)); color:var(--gold); border:1px solid rgba(214,168,79,.24); }}
    .side-nav details {{ border-top:1px solid rgba(214,168,79,.10); padding-top:8px; margin-top:8px; }}
    .side-nav summary {{ color:var(--muted); text-transform:uppercase; letter-spacing:.08em; font-size:11px; font-weight:800; list-style:none; }}
    .side-nav summary::-webkit-details-marker {{ display:none; }}
    .side-nav details a {{ margin-left:8px; padding:9px 11px; font-size:13px; color:#ded7c8; }}
    .sidebar-footer {{ margin-top:18px; padding-top:14px; border-top:1px solid rgba(214,168,79,.12); display:grid; gap:8px; }}
    .sidebar-footer a {{ color:var(--muted); text-decoration:none; font-size:13px; padding:8px 10px; border-radius:10px; }} .sidebar-footer a:hover {{ color:var(--gold); background:rgba(214,168,79,.08); }}
    main.content {{ max-width:1380px; width:100%; margin:0 auto; padding:22px 24px 70px; }}
    .topnav {{ display:flex; gap:8px; flex-wrap:wrap; padding:0; margin:0 0 18px; background:transparent; border:0; box-shadow:none; }}
    .topnav a {{ color:var(--text); text-decoration:none; padding:8px 11px; border:1px solid var(--line); border-radius:12px; background:linear-gradient(180deg,rgba(32,35,45,.82),rgba(13,14,20,.78)); font-size:12px; display:inline-flex; align-items:center; gap:7px; box-shadow:inset 0 1px 0 rgba(255,255,255,.04); }}
    .topnav a::before {{ content:""; width:18px; height:18px; flex:0 0 18px; background:center / contain no-repeat; filter:drop-shadow(0 1px 3px rgba(0,0,0,.7)); display:none; }}
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
    .hero h1::before {{ content:""; display:inline-block; width:38px; height:38px; margin-right:10px; vertical-align:-8px; background:url("{_asset('ebolus_logo.png')}") center / contain no-repeat; filter:drop-shadow(0 2px 7px rgba(0,0,0,.8)); }}
    .hero-actions {{ display:grid; grid-template-columns:repeat(3,minmax(142px,1fr)); gap:12px; min-width:min(520px,100%); }}
    .hero-action {{ position:relative; overflow:hidden; color:var(--text); text-decoration:none; border:1px solid rgba(214,168,79,.22); border-radius:18px; padding:14px 15px; background:linear-gradient(135deg,rgba(32,35,45,.86),rgba(12,13,19,.78)); box-shadow:inset 0 1px 0 rgba(255,255,255,.05), 0 14px 26px rgba(0,0,0,.22); display:grid; gap:3px; min-height:82px; }}
    .hero-action::before {{ content:""; position:absolute; inset:-60% -30%; background:radial-gradient(circle at 25% 18%,rgba(214,168,79,.20),transparent 32%); opacity:.9; pointer-events:none; }}
    .hero-action:hover {{ transform:translateY(-2px); border-color:rgba(214,168,79,.55); box-shadow:inset 0 1px 0 rgba(255,255,255,.08), 0 18px 36px rgba(0,0,0,.34); }}
    .hero-action span,.hero-action strong,.hero-action small {{ position:relative; z-index:1; }}
    .hero-action span {{ font-size:22px; line-height:1; }}
    .hero-action strong {{ font-size:15px; letter-spacing:.01em; }}
    .hero-action small {{ color:var(--muted); font-size:11px; }}
    .hero-action.attendance {{ border-color:rgba(129,199,132,.28); }}
    .hero-action.loot {{ border-color:rgba(214,168,79,.34); }}
    .hero-action.members {{ border-color:rgba(150,130,230,.30); }}
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
    .home-layout {{ display:grid; grid-template-columns:minmax(0,2fr) minmax(300px,1fr); gap:18px; align-items:start; }}
    .home-stack {{ display:grid; gap:14px; }}
    .home-list {{ display:grid; gap:10px; margin-top:12px; }}
    .home-item {{ display:grid; grid-template-columns:44px minmax(0,1fr) auto; gap:12px; align-items:center; padding:13px 14px; border:1px solid rgba(214,168,79,.14); border-radius:14px; background:rgba(32,35,45,.55); }}
    .home-icon {{ width:44px; height:44px; border-radius:12px; display:grid; place-items:center; background:rgba(214,168,79,.12); border:1px solid rgba(214,168,79,.18); font-size:20px; }}
    .home-title {{ font-weight:800; color:var(--text); }} .home-meta {{ color:var(--muted); font-size:13px; margin-top:3px; }}
    .action-list {{ display:grid; gap:10px; margin-top:12px; }}
    .action-list .btn {{ display:block; text-align:center; background:transparent; color:var(--text); border:1px solid rgba(241,234,219,.55); }}
    .action-list .btn:hover {{ color:#111; background:var(--gold); border-color:var(--gold); }}
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
    .authbar {{ display:flex; gap:10px; align-items:center; justify-content:flex-end; background:rgba(24,26,34,.78); border:1px solid var(--line); border-radius:12px; padding:10px 12px; margin-bottom:14px; color:var(--muted); font-size:13px; }} .authbar a {{ color:var(--gold); text-decoration:none; font-weight:700; }}

    .version-pill {{ color:var(--muted); font-size:11px; border:1px solid rgba(214,168,79,.16); background:rgba(214,168,79,.05); border-radius:999px; padding:6px 9px; text-align:center; }}
    .release-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:12px 0 18px; }}
    .release-card {{ background:rgba(32,35,45,.72); border:1px solid var(--line); border-radius:15px; padding:14px; min-width:0; }}
    .release-card b {{ color:var(--gold); display:block; font-size:20px; margin-bottom:4px; }}
    .release-card span {{ color:var(--muted); font-size:12px; }}
    .mobile-note {{ border:1px solid rgba(129,199,132,.25); background:rgba(129,199,132,.07); color:#d7ffd8; border-radius:14px; padding:12px 14px; margin:12px 0; }}
    .page-actions {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .page-actions .btn, .page-actions a {{ margin:0; }}
    img {{ max-width:100%; height:auto; }}
    form {{ max-width:100%; }}
    input, select, textarea, button {{ font:inherit; max-width:100%; }}
    input[type=text], input[type=number], input[type=datetime-local], input[type=date], input[type=url], input[type=search], select, textarea {{ width:100%; border:1px solid var(--line); background:#08090d; color:var(--text); border-radius:10px; padding:10px 12px; outline:none; }}
    input:focus, select:focus, textarea:focus {{ border-color:var(--gold); box-shadow:0 0 0 3px rgba(214,168,79,.12); }}
    .form-row, .form-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; align-items:end; }}
    .actions-inline {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .actions-inline form {{ display:inline-flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .responsive-table {{ width:100%; overflow-x:auto; -webkit-overflow-scrolling:touch; border-radius:12px; }}
    .responsive-table > table {{ min-width:680px; }}
    .skip-mobile {{ display:inline; }}

    @media(max-width:1100px) {{ .app-shell {{ grid-template-columns:1fr; }} .sidebar {{ position:relative; height:auto; border-right:0; border-bottom:1px solid rgba(214,168,79,.16); }} .side-nav {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); }} .side-nav details {{ margin-top:0; }} .home-layout {{ grid-template-columns:1fr; }} }}
    @media(max-width:1000px) {{ .grid,.analytics-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .split {{ grid-template-columns:1fr; }} .hero {{ flex-direction:column; align-items:flex-start; }} .hero-actions {{ grid-template-columns:1fr; width:100%; }} }}
    @media(max-width:760px) {{
      body {{ background-attachment:scroll; }}
      .app-shell {{ display:block; min-height:100vh; }}
      .sidebar {{ position:sticky; top:0; z-index:60; height:auto; max-height:none; overflow:visible; padding:10px 12px; border-right:0; border-bottom:1px solid rgba(214,168,79,.18); box-shadow:0 14px 28px rgba(0,0,0,.38); backdrop-filter:blur(12px); }}
      .brand {{ padding:0; margin:0; border-bottom:0; padding-right:112px; min-height:46px; }}
      .brand-mark {{ width:42px; height:42px; border-radius:13px; }} .brand-mark img {{ width:31px; height:31px; }}
      .mobile-nav-toggle {{ display:inline-flex; align-items:center; gap:6px; position:absolute; right:12px; top:10px; }}
      .side-nav, .sidebar-footer {{ display:none; }}
      body.nav-open .side-nav {{ display:grid; grid-template-columns:1fr; gap:6px; max-height:calc(100vh - 86px); overflow:auto; scrollbar-width:none; -ms-overflow-style:none; padding-top:12px; margin-top:10px; border-top:1px solid rgba(214,168,79,.12); }}
      body.nav-open .side-nav::-webkit-scrollbar {{ width:0; height:0; display:none; }}
      body.nav-open .sidebar-footer {{ display:grid; grid-template-columns:1fr 1fr; margin-top:10px; padding-top:10px; }}
      .side-nav details {{ margin-top:4px; padding-top:6px; }}
      .side-nav details a {{ margin-left:0; padding:10px 12px; }}
      main.content {{ padding:12px 10px 46px; }}
      .authbar {{ justify-content:flex-start; flex-wrap:wrap; font-size:12px; }}
      .hero {{ padding:18px; border-radius:16px; margin-bottom:12px; }}
      .hero h1 {{ font-size:28px; line-height:1.1; }}
      .hero h1::before {{ width:30px; height:30px; margin-right:8px; vertical-align:-6px; }}
      .hero .btn, .hero a.btn {{ width:100%; text-align:center; margin-top:4px; }} .hero-actions {{ grid-template-columns:1fr; }} .hero-action {{ min-height:72px; }}
      .grid,.analytics-grid {{ grid-template-columns:1fr; gap:10px; }}
      .card {{ padding:14px; }} .card-value {{ font-size:25px; }}
      .home-layout {{ grid-template-columns:1fr; gap:12px; }}
      .panel {{ padding:14px; border-radius:15px; margin:12px 0; }}
      .home-item {{ grid-template-columns:38px minmax(0,1fr); gap:10px; padding:12px; }}
      .home-icon {{ width:38px; height:38px; font-size:18px; }}
      .home-title {{ overflow-wrap:anywhere; }}
      .home-meta {{ overflow-wrap:anywhere; font-size:12px; }}
      .home-item .pill {{ grid-column:2; justify-self:start; }}
      .action-list .btn {{ padding:11px 12px; }}
      .table-search {{ max-width:100%; }}
      .table-wrap {{ width:100%; overflow-x:auto; -webkit-overflow-scrolling:touch; }}
      table {{ font-size:13px; min-width:640px; }}
      th,td {{ padding:9px 7px; }}
      .btn {{ width:auto; max-width:100%; white-space:normal; text-align:center; }}
      .queue-badge,.pill {{ white-space:normal; }}

      .topnav {{ display:flex; flex-wrap:nowrap; overflow-x:auto; -webkit-overflow-scrolling:touch; gap:8px; padding-bottom:6px; margin-bottom:12px; scrollbar-width:none; }}
      .topnav::-webkit-scrollbar {{ display:none; }}
      .topnav a {{ flex:0 0 auto; font-size:12px; padding:8px 10px; }}
      .release-grid {{ grid-template-columns:1fr 1fr; }}
      .form-row, .form-grid {{ grid-template-columns:1fr; }}
      .actions-inline, .actions-inline form, .page-actions {{ display:grid; grid-template-columns:1fr; width:100%; }}
      .actions-inline .btn, .actions-inline button, .actions-inline a, .page-actions .btn, .page-actions a {{ width:100%; text-align:center; }}
      input[type=text], input[type=number], input[type=datetime-local], input[type=date], input[type=url], input[type=search], select, textarea {{ min-height:42px; font-size:16px; }}
      .responsive-table {{ margin-inline:-2px; padding-bottom:4px; }}
      .skip-mobile {{ display:none; }}

    }}
    @media(max-width:560px) {{ main.content {{ padding:12px 10px 42px; }} .grid,.analytics-grid,.side-nav {{ grid-template-columns:1fr; }} .bar-row {{ grid-template-columns:90px 1fr 38px; }} .home-item {{ grid-template-columns:38px minmax(0,1fr); }} .home-item .pill {{ grid-column:2; justify-self:start; }} .split {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body><div class="app-shell">{_member_sidebar_html() if nav_mode == "member" else _sidebar_html()}<main class="content">{auth_note}{body}</main></div><script>
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
(function markActiveNav() {{
  const path = window.location.pathname || '/';
  const links = document.querySelectorAll('.side-nav a');
  let best = null;
  for (const a of links) {{
    const href = a.getAttribute('href') || '/';
    if (href === path || (href !== '/' && path.startsWith(href + '/'))) {{
      if (!best || href.length > (best.getAttribute('href') || '').length) best = a;
    }}
  }}
  if (best) {{ best.classList.add('active'); const d = best.closest('details'); if (d) d.open = true; }}
}})();
(function mobileNavCleanup() {{
  document.querySelectorAll('.side-nav a').forEach(a => a.addEventListener('click', () => document.body.classList.remove('nav-open')));
  document.addEventListener('keydown', ev => {{ if (ev.key === 'Escape') document.body.classList.remove('nav-open'); }});
}})();
(function responsiveTables() {{
  document.querySelectorAll('table').forEach(tbl => {{
    if (tbl.closest('.table-wrap') || tbl.closest('.responsive-table')) return;
    const wrap = document.createElement('div');
    wrap.className = 'responsive-table';
    tbl.parentNode.insertBefore(wrap, tbl);
    wrap.appendChild(tbl);
  }});
}})();
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
    cut = data.get("phase3_ec_read_cutover") or {}
    ec_source_label = "Postgres Phase 3" if cut.get("active") else "Snapshot/JSON Fallback"
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
        _card("EC-Quelle", ec_source_label, "Read-Cutover aktiv" if cut.get("active") else "Fallback aktiv"),
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
        <p class="muted">Read-only Auswertung. EC-Quelle: <b>{_e(ec_source_label)}</b>. JSON/Snapshot bleibt Fallback. Snapshot: {_e(_dt(data.get('published_at')))}</p>
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
            r.get("display_name") or f"User {r.get('user_id')}",
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
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/admin">Admin-Zentrale</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="/ec">EC-Verlauf</a><a href="/audit">Audit</a><a href="/system">System</a><a href="/api/settings">API</a></nav>
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
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/admin">Admin-Zentrale</a><a href="/settings">Einstellungen</a><a href="/audit">Audit</a><a href="/api/system">API</a></nav>
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


# ---------------------------------------------------------------------------
# Schritt 3: Mitgliederzentrale
# ---------------------------------------------------------------------------
# Read-only Leitungsübersicht. Zieht Daten aus Snapshot + bereits vorhandenen
# Dashboard-Reviews/Queues. Keine EC-, Loot-, Need- oder Bot-JSON-Schreiberei.


def _member_center_role_bucket(value: Any) -> str:
    txt = str(value or "").strip().lower()
    if any(x in txt for x in ("tank", "wächter", "waechter")):
        return "Tank"
    if any(x in txt for x in ("heal", "heiler", "support")):
        return "Heiler"
    if any(x in txt for x in ("dps", "dd", "damage", "schaden")):
        return "DPS"
    if any(x in txt for x in ("reserve", "bank")):
        return "Reserve"
    return str(value or "Unklar") or "Unklar"


def _member_center_last_seen(m: dict[str, Any], att: dict[str, Any], loot: dict[str, Any]) -> str:
    candidates = []
    for key in ("last_updated_at", "last_event_when", "last_event_time"):
        if att.get(key):
            candidates.append(str(att.get(key)))
    if loot.get("last_activity"):
        candidates.append(str(loot.get("last_activity")))
    for key in ("updated_at", "last_seen", "last_active", "joined_at"):
        if m.get(key):
            candidates.append(str(m.get(key)))
    if not candidates:
        return ""
    return max(candidates)


def _member_center_hint(row: dict[str, Any]) -> str:
    hints: list[str] = []
    if row.get("admin_status") and row.get("admin_status") != "ok":
        hints.append(_status_label(row.get("admin_status")))
    if not row.get("has_profile"):
        hints.append("kein Profil")
    if row.get("ec_balance") is None:
        hints.append("kein EC-Stand")
    if int(row.get("main_need_count") or 0) == 0 and int(row.get("secondary_need_count") or 0) == 0:
        hints.append("keine Needs")
    if int(row.get("attendance_open") or 0) > 0:
        hints.append(f"{int(row.get('attendance_open') or 0)} Review offen")
    if int(row.get("attendance_absent") or 0) >= 2:
        hints.append(f"{int(row.get('attendance_absent') or 0)}× nicht da")
    if int(row.get("bid_count") or 0) > 0 and int(row.get("won_count") or 0) == 0:
        hints.append("bietet, gewinnt nicht")
    risk = str(row.get("risk_flags_text") or "").strip()
    if risk and risk != "—":
        hints.append(risk)
    return ", ".join(hints) if hints else "—"


def _member_center_payload(data: dict[str, Any]) -> dict[str, Any]:
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    members = _insight_members(snap)
    profiles = { _user_id(p.get("user_id")): p for p in (((snap.get("profiles") or {}).get("items") or [])) if isinstance(p, dict) and _user_id(p.get("user_id")) }
    balances = _balance_map(snap)
    needs_by_user = _needs_by_user(snap)
    quality = (_insights(snap).get("quality") or {}) if isinstance(_insights(snap).get("quality"), dict) else {}

    try:
        att_payload = _attendance_stats_payload(data)
    except Exception:
        att_payload = {"player_rows": [], "problem_rows": [], "review_count": 0}
    att_by_user = {int(p.get("user_id") or 0): p for p in (att_payload.get("player_rows") or []) if isinstance(p, dict) and int(p.get("user_id") or 0)}

    try:
        loot_payload = _loot_history_payload_from_snapshot(snap, int(guild_id or 0))
    except Exception:
        loot_payload = {"player_rows": []}
    loot_by_user = {int(p.get("user_id") or 0): p for p in (loot_payload.get("player_rows") or []) if isinstance(p, dict) and int(p.get("user_id") or 0)}

    state_rows = _all_member_admin_states(guild_id) if guild_id else []
    state_by_user = {int(s.get("member_user_id") or 0): s for s in state_rows if isinstance(s, dict) and int(s.get("member_user_id") or 0)}

    # Alle bekannten User zusammenführen: Gildenrolle/Snapshot, Attendance, Loot, Admin-Notizen.
    ids: set[int] = set()
    for m in members:
        uid = _user_id(m.get("user_id") or m.get("member_id") or m.get("discord_id"))
        if uid:
            ids.add(uid)
    ids.update(att_by_user.keys())
    ids.update(loot_by_user.keys())
    ids.update(state_by_user.keys())

    rows: list[dict[str, Any]] = []
    for uid in sorted(ids):
        base = next((m for m in members if _user_id(m.get("user_id") or m.get("member_id") or m.get("discord_id")) == uid), {}) or {}
        prof = profiles.get(uid, {})
        att = att_by_user.get(uid, {})
        loot = loot_by_user.get(uid, {})
        st = state_by_user.get(uid, {})
        need_info = needs_by_user.get(uid, {}) if isinstance(needs_by_user.get(uid, {}), dict) else {}
        main_needs = need_info.get("main") if isinstance(need_info.get("main"), list) else []
        sec_needs = need_info.get("secondary") if isinstance(need_info.get("secondary"), list) else []
        display = str(base.get("display_name") or prof.get("display_name") or att.get("display_name") or loot.get("display_name") or f"User {uid}")
        ingame = str(base.get("ingame_name") or prof.get("ingame_name") or "")
        role = str(base.get("main_role") or prof.get("main_role") or "")
        ec_balance = base.get("ec_balance") if base.get("ec_balance") is not None else balances.get(uid)
        present = int(att.get("present") or 0)
        partial = int(att.get("partial") or 0)
        absent = int(att.get("absent") or 0)
        open_count = int(att.get("open") or 0)
        attendance_total = present + partial + absent
        row = {
            "user_id": uid,
            "display_name": display,
            "ingame_name": ingame,
            "role": role,
            "role_bucket": _member_center_role_bucket(role),
            "gearscore": base.get("gearscore") or prof.get("gearscore") or "",
            "ec_balance": ec_balance,
            "main_need_count": int(base.get("main_need_count") if base.get("main_need_count") is not None else len(main_needs)),
            "secondary_need_count": int(base.get("secondary_need_count") if base.get("secondary_need_count") is not None else len(sec_needs)),
            "attendance_rate": att.get("rate") or _attendance_rate(present, partial, absent),
            "attendance_present": present,
            "attendance_partial": partial,
            "attendance_absent": absent,
            "attendance_open": open_count,
            "attendance_total": attendance_total,
            "last_event_title": att.get("last_event_title") or "",
            "last_status": att.get("last_status") or "",
            "voice_hours": float(att.get("voice_hours") or base.get("voice_hours") or 0),
            "won_count": int(loot.get("won_count") or base.get("loot_won_count") or 0),
            "spent_ec": int(_num(loot.get("spent_ec"), 0)),
            "bid_count": int(loot.get("bid_count") or 0),
            "has_profile": bool(base.get("has_profile", True if prof else False)),
            "has_ec": ec_balance is not None,
            "has_needs": (len(main_needs) + len(sec_needs)) > 0 or bool(base.get("has_needs")),
            "admin_status": str(st.get("status") or "ok").lower(),
            "admin_note": str(st.get("note") or ""),
            "admin_updated_at": st.get("updated_at") or "",
            "risk_score": int(_num(base.get("risk_score"), 0)),
            "risk_flags_text": _risk_flags_text(base) if isinstance(base, dict) else "—",
        }
        row["last_seen"] = _member_center_last_seen(base, att, loot)
        row["hint"] = _member_center_hint(row)
        rows.append(row)

    rows.sort(key=lambda r: (0 if r.get("admin_status") in {"critical", "watch", "check"} else 1, -int(r.get("risk_score") or 0), str(r.get("display_name") or "").lower()))

    by_role = Counter(str(r.get("role_bucket") or "Unklar") for r in rows)
    admin_open = sum(1 for r in rows if str(r.get("admin_status") or "ok") != "ok")
    active_attendance = sum(1 for r in rows if int(r.get("attendance_total") or 0) > 0)
    with_loot = sum(1 for r in rows if int(r.get("won_count") or 0) > 0 or int(r.get("bid_count") or 0) > 0)
    problem_rows = [r for r in rows if str(r.get("hint") or "—") != "—"][:120]

    return {
        "guild_id": guild_id,
        "snapshot_at": data.get("published_at"),
        "rows": rows,
        "problem_rows": problem_rows,
        "by_role": by_role,
        "quality": quality,
        "admin_marked": admin_open,
        "active_attendance": active_attendance,
        "with_loot": with_loot,
        "attendance_reviews": att_payload.get("review_count", 0),
    }


def _render_members_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Mitglieder · Ebo Dashboard", f"<section class='panel'><h1>👥 Mitglieder</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    payload = _member_center_payload(data)
    rows = payload.get("rows") or []

    member_rows = []
    for r in rows:
        uid = _user_id(r.get("user_id"))
        member_rows.append([
            _member_link(uid, r.get("display_name")),
            r.get("ingame_name") or "—",
            r.get("role") or "—",
            r.get("gearscore") or "—",
            _fmt_ec(r.get("ec_balance")) if r.get("ec_balance") is not None else "—",
            f"{r.get('main_need_count', 0)} / {r.get('secondary_need_count', 0)}",
            r.get("attendance_rate") or "—",
            f"✅ {r.get('attendance_present',0)} · 🟡 {r.get('attendance_partial',0)} · ❌ {r.get('attendance_absent',0)}",
            f"{_num(r.get('voice_hours'), 0):.1f} h",
            f"{r.get('won_count',0)} / {_fmt_ec(r.get('spent_ec',0))} EC",
            _status_label(r.get("admin_status")),
            _member_center_hint(r),
            _raw(f'<a class="link" href="/member/{uid}/loot">Loot</a>') if uid else "—",
        ])

    problem_rows = []
    for r in payload.get("problem_rows") or []:
        uid = _user_id(r.get("user_id"))
        problem_rows.append([
            _member_link(uid, r.get("display_name")),
            _status_label(r.get("admin_status")),
            r.get("attendance_rate") or "—",
            _fmt_ec(r.get("ec_balance")) if r.get("ec_balance") is not None else "—",
            r.get("hint") or "—",
            _dt(r.get("last_seen")),
        ])

    role_rows = [[role, count] for role, count in (payload.get("by_role") or Counter()).most_common()]
    top_ec = sorted([r for r in rows if r.get("ec_balance") is not None], key=lambda r: _num(r.get("ec_balance"), 0), reverse=True)[:20]
    ec_rows = [[_member_link(r.get("user_id"), r.get("display_name")), _fmt_ec(r.get("ec_balance")), r.get("role") or "—", r.get("attendance_rate") or "—", r.get("hint") or "—"] for r in top_ec]

    cards = "".join([
        _card("Mitglieder", len(rows), "alle bekannten Spieler"),
        _card("Review-Spieler", payload.get("active_attendance", 0), "in Attendance-Historie"),
        _card("Loot-Aktiv", payload.get("with_loot", 0), "Gebote/Gewinne"),
        _card("Markiert", payload.get("admin_marked", 0), "Leitungsnotizen"),
        _card("Reviews", payload.get("attendance_reviews", 0), "gespeicherte Events"),
        _card("Snapshot", _dt(payload.get("snapshot_at")), "letzter Stand"),
    ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/attendance-stats">Anwesenheit-Stats</a><a href="/loot-history">Loot-Verlauf</a><a href="/ec">EC</a><a href="/admin">Leitung</a><a href="/export/member_center.csv">CSV</a><a href="/api/member-center">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Mitgliederzentrale</div><h1>👥 Mitglieder</h1><p class="muted">Kombiniert Profil, EC, Needliste, Attendance-Reviews, Loot-Verlauf und Leitungsmarkierungen. Read-only – hier wird nichts am Bot verändert.</p></div><a class="btn" href="/export/member_center.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>Rollenverteilung</h2>{_bars(role_rows, max_items=8)}</div><div class="panel"><h2>Top EC</h2>{_table(['Spieler','EC','Rolle','Anwesenheit','Hinweis'], ec_rows, placeholder='Top EC durchsuchen…')}</div></section>
    <section class="panel"><h2>⚠️ Prüfliste</h2><p class="muted">Auffällige Mitglieder aus Profil-/EC-/Need-Daten, Attendance-Historie, Loot-Verlauf und internen Leitungsnotizen.</p>{_table(['Spieler','Leitung','Attendance','EC','Hinweis','Letzte Aktivität'], problem_rows, placeholder='Prüfliste durchsuchen…')}</section>
    <section class="panel"><h2>👥 Alle Mitglieder</h2>{_table(['Name','Ingame','Rolle','GS','EC','Needs M/S','Anwesenheit','Review-Zähler','Voice','Loot/EC','Leitung','Hinweise','Loot'], member_rows, placeholder='Mitglieder durchsuchen…')}</section>
    """
    return _html_shell("Mitgliederzentrale · Ebo Dashboard", body)

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
    <section class="hero"><div><div class="eyebrow">Need-Einträge</div><h1>🎁 Need-Analytics</h1><p class="muted">Zeigt, welche Items wie oft gebraucht werden. Read-only.</p></div><a class="btn" href="/export/needs.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="split"><div class="panel"><h2>Top Main-Needs</h2>{_table(['Item','Anzahl'], top_main_rows, placeholder='Main-Needs durchsuchen…')}</div><div class="panel"><h2>Top Secondary-Needs</h2>{_table(['Item','Anzahl'], top_secondary_rows, placeholder='Secondary-Needs durchsuchen…')}</div></section>
    <section class="panel"><h2>🧹 Mitglieder ohne Needliste</h2>{_table(['Spieler','Rolle','GS'], without_rows, placeholder='Ohne Needliste durchsuchen…')}</section>
    <section class="panel"><h2>Alle Need-Einträge</h2>{_table(['Spieler','Main','Secondary','Main-Needs','Secondary-Needs'], all_rows, placeholder='Need-Einträge durchsuchen…')}</section>
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
        leader = _auction_leader_or_roll_text(a, snap)
        _, count_value, _ = _auction_count_label(a)
        row = [_auction_link(a.get("auction_id"), a.get("item_name")), _phase_label(a), _loot_effective_status_label(a), count_value, leader, _member_link(a.get("winner_user_id"), _snapshot_display_name(snap, a.get("winner_user_id"), a.get("winner_name"))) if a.get("winner_user_id") else "—", _auction_timer_text(a)]
        if _loot_is_active(a):
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
    <section class="panel"><h2>🟢 Aktive Auktionen</h2>{_table(['Item','Bereich','Status','Gebote/Würfe','Führung/Wurf','Gewinner','Timer'], active_rows, placeholder='Aktive Auktionen durchsuchen…')}</section>
    <section class="panel"><h2>📜 Auktionshistorie</h2>{_table(['Item','Bereich','Status','Gebote/Würfe','Führung/Wurf','Gewinner','Timer'], closed_rows[:250], placeholder='Auktionshistorie durchsuchen…')}</section>
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
    end_dt = _dt_obj(auction.get("ends_at") or auction.get("end_at") or auction.get("expires_at"))
    if end_dt and datetime.now(timezone.utc) >= end_dt:
        return False
    return s in _LOOT_ACTIVE_STATUSES or (not s and phase in {"need", "free", "sale", "roll"})


def _loot_is_done(auction: dict[str, Any]) -> bool:
    s = _loot_status(auction.get("status"))
    return s in _LOOT_DONE_STATUSES or bool(auction.get("delivered_at"))


def _loot_effective_status_key(auction: dict[str, Any]) -> str:
    """Dashboard-Sicht auf den echten Zustand, auch wenn der Bot noch nicht geschlossen hat."""
    s = _loot_status(auction.get("status"))
    if s in _LOOT_CANCELLED_STATUSES:
        return "cancelled"
    if _loot_is_done(auction):
        return "done"
    if s in _LOOT_ACTIVE_STATUSES:
        if _loot_is_ended(auction):
            return "expired_waiting"
        return "active"
    if _loot_is_ended(auction):
        return "expired"
    return s or "unknown"


def _loot_effective_status_label(auction: dict[str, Any]) -> dict[str, str]:
    key = _loot_effective_status_key(auction)
    labels = {
        "active": "offen",
        "expired_waiting": "abgelaufen · wartet auf Bot",
        "expired": "abgelaufen",
        "done": "erledigt",
        "cancelled": "abgebrochen",
        "unknown": "unklar",
    }
    return _raw(f"<span class='pill'>{_e(labels.get(key, key))}</span>")


def _loot_phase_window_text(auction: dict[str, Any]) -> str:
    phase = _loot_status(auction.get("phase"))
    mode = _loot_status(auction.get("eligibility_mode"))
    if phase == "sale" or auction.get("fixed_price") is not None:
        return "Sale: 10 Tage"
    if phase == "free" or mode == "all":
        return "Frei: 24h"
    if mode in {"main_need", "secondary_need"} or phase == "need":
        return "Need: 48h"
    return "—"


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
    if _loot_is_ended(auction):
        if bids > 0:
            return "abgelaufen → Bot sollte Gewinner/Übergabe erstellen"
        if "sale" in mode:
            return "abgelaufen → Sale ohne Käufer schließen"
        if "freie" in mode or phase == "free":
            return "abgelaufen → Bot sollte Sale starten"
        return "abgelaufen → Bot sollte freie Auktion starten"
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
            "effective_status": _loot_effective_status_key(a),
            "status_label": _loot_effective_status_label(a),
            "phase": str(a.get("phase") or "—"),
            "window": _loot_phase_window_text(a),
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
            if entry.get("effective_status") == "expired_waiting":
                next_actions.append(entry)
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
            "status_label": _loot_effective_status_label(a),
            "phase": str(a.get("phase") or "—"),
            "mode": _loot_mode_bucket(a),
            "window": _loot_phase_window_text(a),
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
            a.get("status_label") or _loot_status_label(a.get("status")),
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

def _render_loot_center(data: dict[str, Any], request: Optional[Request] = None, msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Loot · Ebo Dashboard", f"<section class='panel'><h1>🎁 Loot</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    center = _loot_center_payload_from_snapshot(snap)

    loot_cut = data.get("phase3_loot_read_cutover") or {}
    loot_source_title = "Postgres Phase 3" if loot_cut.get("active") else "Snapshot/Fallback"
    loot_source_sub = "Read-Cutover aktiv" if loot_cut.get("active") else str(loot_cut.get("reason") or "Fallback aktiv")
    cards = "".join([
        _card("Loot-Quelle", loot_source_title, loot_source_sub),
        _card("Aktive Auktionen", len(center["active"]), "offen/läuft"),
        _card("Ohne Gebot", len(center["no_bid"]), "aktive Auktionen"),
        _card("Need-Items", len(center["needs"]), "aus Need-Einträge"),
    ])

    action_rows = []
    for a in center["next_actions"][:80]:
        aid = a.get("auction_id")
        action_rows.append([
            _auction_link(aid, a.get("item")) if aid else a.get("item"),
            a.get("mode"),
            a.get("status_label") or _loot_status_label(a.get("status")),
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
            a.get("status_label") or _loot_status_label(a.get("status")),
            a.get("bid_count"),
            a.get("leader"),
            f"{a.get('main_need_count', 0)} / {a.get('secondary_need_count', 0)}",
            a.get("window") or "—",
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

    drop_msg = f"<p class='muted'>{_e(msg)}</p>" if msg else ""
    if request is not None and _is_portal_admin(request):
        drop_panel = f"""
        <section class="panel"><h2>📦 Drop aus Dashboard melden</h2>
          {drop_msg}
          <p class="muted">Das Dashboard schreibt nicht direkt in Lootdaten. Es legt eine Bot-Anfrage in Postgres an. Der Bot erstellt danach mit der bestehenden Discord-Logik die Auktion, DMs und Auktionshaus-Nachrichten.</p>
          <div class="split">
            <form method="post" action="/admin/loot/drop" style="display:grid;gap:10px;">
              <h3>📦 Loot gedroppt</h3>
              <input type="hidden" name="drop_type" value="loot_drop">
              <label>Item aus Loot-Katalog<br><input name="item" required placeholder="z. B. Aridus Stab" style="width:100%;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);color:inherit"></label>
              <button class="btn" type="submit" onclick="return confirm('Loot-Drop an den Bot senden?');">Loot-Drop erstellen</button>
              <p class="muted">Bot prüft Main-Need → Second-Need → freie Auktion.</p>
            </form>
            <form method="post" action="/admin/loot/drop" style="display:grid;gap:10px;">
              <h3>🧹 Müll gedroppt</h3>
              <input type="hidden" name="drop_type" value="junk_drop">
              <label>Freier Itemname<br><input name="item" required placeholder="z. B. Restitem aus Truhe" style="width:100%;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);color:inherit"></label>
              <button class="btn" type="submit" onclick="return confirm('Müll-Drop an den Bot senden?');">Müll-Drop erstellen</button>
              <p class="muted">Bot erstellt ein kostenloses Müll-/Würfelitem.</p>
            </form>
          </div>
        </section>
        """
    else:
        drop_panel = f"""
        <section class="panel"><h2>📦 Drop aus Dashboard melden</h2>
          {drop_msg}
          <p class="muted">Nur Dashboard-Admins können Loot-/Müll-Drops aus dem Dashboard an den Bot senden.</p>
        </section>
        """

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/loot">Loot</a><a href="/loot-history">Loot-Verlauf</a><a href="/loot-check">Truhencheck</a><a href="/needs">Needs</a><a href="/members">Mitglieder</a><a href="/fairness">Fairness</a><a href="/exports">Exports</a><a href="/api/loot-center">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot-Zentrale</div><h1>🎁 Loot / Auktionen</h1><p class="muted">Kontrollzentrum für aktive Auktionen, offene Übergaben, Gebote und Need-Spieler. Read-only, keine Bot-Daten werden verändert.</p></div><div style="display:flex;gap:10px;flex-wrap:wrap"><a class="btn" href="/loot-history">Loot-Verlauf</a><a class="btn" href="/export/loot_center.csv">CSV herunterladen</a></div></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🔎 Schneller Truhencheck</h2><form method="get" action="/loot-check" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center"><input name="item" placeholder="Itemname eingeben, z. B. Aridus Stab" style="min-width:280px;flex:1;padding:12px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(0,0,0,.22);color:inherit"><button class="btn" type="submit">Need prüfen</button></form><p class="muted">Prüft Main-/Second-Needs und ob dazu schon eine Auktion läuft.</p></section>
    {drop_panel}
    <section class="panel"><h2>🔥 Nächste Loot-Aktionen</h2><p class="muted">Das ist die Arbeitsliste: Übergaben, offene Auktionen, Sale/Freie-Auktion-Kandidaten und aktive Auktionen ohne Gebote.</p>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende','Nächster Schritt'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>🟢 Aktive Auktionen</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Main/Second Need','Regel','Ende'], active_rows, placeholder='Aktive Auktionen durchsuchen…')}</section>
    <section class="split"><div class="panel"><h2>🏁 Übergabe offen</h2>{_table(['Item','Gewinner','Führend','Ende','Nächster Schritt'], handover_rows, placeholder='Übergaben durchsuchen…')}</div><div class="panel"><h2>🟡 Ohne Gebot / Sale prüfen</h2>{_table(['Item','Bereich','Main/Second Need','Ende','Hinweis'], no_bid_rows, placeholder='Ohne Gebot durchsuchen…')}</div></section>
    <section class="panel"><h2>🎯 Need-Spieler pro Item</h2><p class="muted">Damit sieht man direkt, ob ein Drop Main-/Second-Need-Spieler hat und ob schon eine Auktion dazu läuft.</p>{_table(['Item','Main','Main-Spieler','Second','Second-Spieler','aktive Auktionen'], need_rows, placeholder='Need-Item suchen…')}</section>
    <section class="panel"><h2>📜 Auktionshistorie</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','Ende'], history_rows, placeholder='Historie durchsuchen…')}</section>
    """
    return _html_shell("Loot · Ebo Dashboard", body)


# ---------------------------------------------------------------------------
# Step 2: Loot-Verlauf / Transparenz
# ---------------------------------------------------------------------------

def _loot_action_requests_all(guild_id: int, limit: int = 300) -> list[dict[str, Any]]:
    """Letzte Dashboard-Loot-Aktionen aus Postgres für Transparenz anzeigen."""
    if not _database_url() or not guild_id:
        return []
    try:
        _ensure_admin_tables()
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, request_id, guild_id, auction_id, action_type, amount, status,
                           payload_json, actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                    FROM dashboard_loot_action_requests
                    WHERE guild_id = %s
                    ORDER BY requested_at DESC
                    LIMIT %s
                    """,
                    (int(guild_id), int(limit)),
                )
                rows = [dict(r) for r in (cur.fetchall() or [])]
        finally:
            conn.close()
        for row in rows:
            try:
                row["payload"] = json.loads(row.get("payload_json") or "{}")
            except Exception:
                row["payload"] = {}
            try:
                row["result"] = json.loads(row.get("result_json") or "{}")
            except Exception:
                row["result"] = {}
        return rows
    except Exception:
        return []


def _loot_bid_user_id(bid: dict[str, Any]) -> int:
    for key in ("user_id", "bidder_user_id", "bidder_id", "discord_id", "member_id", "actor_id"):
        uid = _user_id(bid.get(key))
        if uid:
            return uid
    return 0


def _loot_bid_user_name(bid: dict[str, Any], names: dict[int, str]) -> str:
    uid = _loot_bid_user_id(bid)
    for key in ("display_name", "user_name", "username", "bidder_name", "name", "actor_name"):
        value = str(bid.get(key) or "").strip()
        if value:
            return value
    return names.get(uid, f"User {uid}" if uid else "Unbekannt")


def _loot_bid_amount(bid: dict[str, Any]) -> int:
    for key in ("amount", "bid_amount", "ec", "value", "price"):
        if bid.get(key) not in (None, ""):
            return int(_num(bid.get(key), 0))
    return 0


def _loot_bid_time(bid: dict[str, Any]) -> Any:
    for key in ("created_at", "timestamp", "time", "bid_at", "placed_at", "updated_at"):
        if bid.get(key):
            return bid.get(key)
    return ""


def _loot_winning_amount(auction: dict[str, Any]) -> int:
    for key in ("winning_bid", "winner_bid", "top_bid_amount", "leader_bid", "highest_bid", "fixed_price", "sale_price", "price"):
        if auction.get(key) not in (None, ""):
            return int(_num(auction.get(key), 0))
    # Fallback: höchste Gebotssumme aus der Liste.
    amounts = [_loot_bid_amount(b) for b in (auction.get("bids") or []) if isinstance(b, dict)]
    return max(amounts) if amounts else 0


def _loot_winner_user_id(auction: dict[str, Any]) -> int:
    for key in ("winner_user_id", "delivered_to_user_id", "sold_to_user_id", "buyer_user_id"):
        uid = _user_id(auction.get(key))
        if uid:
            return uid
    # Wenn die Auktion erledigt ist, aber nur der Top-Bidder gespeichert ist.
    if _loot_is_done(auction):
        return _user_id(auction.get("top_bid_user_id") or auction.get("leader_user_id"))
    return 0


def _loot_winner_name_text(auction: dict[str, Any], names: dict[int, str]) -> str:
    uid = _loot_winner_user_id(auction)
    for key in ("winner_name", "delivered_to_name", "sold_to_name", "buyer_name"):
        value = str(auction.get(key) or "").strip()
        if value:
            return value
    return names.get(uid, f"User {uid}" if uid else "—")


def _loot_history_event_time(auction: dict[str, Any]) -> Any:
    for key in ("delivered_at", "closed_at", "ended_at", "processed_at", "ends_at", "created_at"):
        if auction.get(key):
            return auction.get(key)
    return ""


def _loot_bid_status(auction: dict[str, Any], bid: dict[str, Any]) -> str:
    uid = _loot_bid_user_id(bid)
    amount = _loot_bid_amount(bid)
    top_uid = _loot_top_bid_user_id(auction)
    top_amount = int(_num(auction.get("top_bid_amount") or auction.get("leader_bid") or auction.get("highest_bid"), 0))
    winner_uid = _loot_winner_user_id(auction)
    if _loot_is_done(auction) and uid and winner_uid and uid == winner_uid and amount == _loot_winning_amount(auction):
        return "gewonnen"
    if uid and top_uid and uid == top_uid and (not top_amount or amount == top_amount):
        return "führt"
    return "überboten" if amount else "—"


def _loot_history_payload_from_snapshot(snap: dict[str, Any], guild_id: int = 0) -> dict[str, Any]:
    names = _profile_name_map(snap)
    auctions = [a for a in ((((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])) if isinstance(a, dict)]
    action_rows = _loot_action_requests_all(int(guild_id), limit=300) if guild_id else []

    auction_rows: list[dict[str, Any]] = []
    winner_rows: list[dict[str, Any]] = []
    bid_rows: list[dict[str, Any]] = []
    players: dict[int, dict[str, Any]] = {}

    def player(uid: int, name: str = "") -> dict[str, Any]:
        uid = int(uid or 0)
        p = players.setdefault(uid, {"user_id": uid, "display_name": name or names.get(uid, f"User {uid}" if uid else "Unbekannt"), "won_count": 0, "spent_ec": 0, "bid_count": 0, "highest_bid_count": 0, "sale_count": 0, "junk_count": 0, "last_activity": ""})
        if name and (not p.get("display_name") or str(p.get("display_name")).startswith("User ")):
            p["display_name"] = name
        return p

    for a in auctions:
        aid = str(a.get("auction_id") or a.get("id") or "").strip()
        item = _loot_text(a.get("item_name") or a.get("item") or a.get("name") or aid)
        mode = _loot_mode_bucket(a)
        status_raw = str(a.get("status") or "—")
        bid_count = _loot_bid_count(a)
        win_uid = _loot_winner_user_id(a)
        win_name = _loot_winner_name_text(a, names)
        win_amount = _loot_winning_amount(a)
        event_time = _loot_history_event_time(a)
        auction_entry = {
            "auction_id": aid,
            "item": item or aid or "—",
            "mode": mode,
            "status": status_raw,
            "status_label": _loot_status_label(status_raw),
            "bid_count": bid_count,
            "leader": _loot_leader_text(a, names),
            "winner_user_id": win_uid,
            "winner_name": win_name,
            "winning_amount": win_amount,
            "created_at": a.get("created_at"),
            "ends_at": a.get("ends_at") or a.get("expires_at") or a.get("end_at"),
            "closed_at": event_time,
            "active": _loot_is_active(a),
            "done": _loot_is_done(a),
            "raw": a,
        }
        auction_rows.append(auction_entry)

        if win_uid or (win_name and win_name != "—"):
            winner_rows.append(auction_entry)
            if win_uid:
                p = player(win_uid, win_name)
                p["won_count"] += 1
                p["spent_ec"] += int(win_amount or 0)
                if "sale" in str(mode).lower():
                    p["sale_count"] += 1
                if "müll" in str(mode).lower() or bool(a.get("junk_drop")):
                    p["junk_count"] += 1
                if event_time and str(event_time) > str(p.get("last_activity") or ""):
                    p["last_activity"] = event_time

        bids = [b for b in (a.get("bids") or []) if isinstance(b, dict)]
        for b in bids:
            uid = _loot_bid_user_id(b)
            bname = _loot_bid_user_name(b, names)
            amount = _loot_bid_amount(b)
            btime = _loot_bid_time(b) or a.get("created_at") or a.get("ends_at")
            bstatus = _loot_bid_status(a, b)
            bid_entry = {
                "auction_id": aid,
                "item": item or aid or "—",
                "mode": mode,
                "user_id": uid,
                "display_name": bname,
                "amount": amount,
                "created_at": btime,
                "status": bstatus,
            }
            bid_rows.append(bid_entry)
            if uid:
                p = player(uid, bname)
                p["bid_count"] += 1
                if bstatus in {"führt", "gewonnen"}:
                    p["highest_bid_count"] += 1
                if btime and str(btime) > str(p.get("last_activity") or ""):
                    p["last_activity"] = btime

    auction_rows.sort(key=lambda r: str(r.get("closed_at") or r.get("ends_at") or r.get("created_at") or ""), reverse=True)
    winner_rows.sort(key=lambda r: str(r.get("closed_at") or r.get("ends_at") or r.get("created_at") or ""), reverse=True)
    bid_rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    player_rows = [p for uid, p in players.items() if uid]
    player_rows.sort(key=lambda p: (-int(p.get("won_count") or 0), -int(p.get("spent_ec") or 0), -int(p.get("bid_count") or 0), str(p.get("display_name") or "").lower()))

    return {
        "auctions_total": len(auction_rows),
        "winners_total": len(winner_rows),
        "bids_total": len(bid_rows),
        "actions_total": len(action_rows),
        "auction_rows": auction_rows,
        "winner_rows": winner_rows,
        "bid_rows": bid_rows,
        "player_rows": player_rows,
        "action_rows": action_rows,
    }


def _loot_result_text(row: dict[str, Any]) -> str:
    result = row.get("result") if isinstance(row.get("result"), dict) else {}
    if result:
        for key in ("message", "error", "reason", "status"):
            if result.get(key):
                return _short(result.get(key), 140)
    return _short(row.get("result_json") or "", 140) or "—"


def _render_loot_history(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell("Loot-Verlauf · Ebo Dashboard", f"<section class='panel'><h1>📜 Loot-Verlauf</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    hist = _loot_history_payload_from_snapshot(snap, int(guild_id or 0))

    cards = "".join([
        _card("Auktionen", hist.get("auctions_total", 0), "gesamt im Snapshot"),
        _card("Gewonnene Items", hist.get("winners_total", 0), "mit Gewinner"),
        _card("Gebote", hist.get("bids_total", 0), "einzelne Gebote"),
        _card("Dashboard-Aktionen", hist.get("actions_total", 0), "Queue-Anfragen"),
    ])

    winner_rows = []
    for r in hist.get("winner_rows") or []:
        aid = r.get("auction_id")
        winner_rows.append([
            _auction_link(aid, r.get("item")) if aid else r.get("item"),
            r.get("mode"),
            _member_link(r.get("winner_user_id"), r.get("winner_name")) if r.get("winner_user_id") else r.get("winner_name"),
            _fmt_ec(r.get("winning_amount")),
            r.get("bid_count"),
            _loot_status_label(r.get("status")),
            _dt(r.get("closed_at")),
        ])

    bid_rows = []
    for b in (hist.get("bid_rows") or [])[:500]:
        aid = b.get("auction_id")
        bid_rows.append([
            _dt(b.get("created_at")),
            _auction_link(aid, b.get("item")) if aid else b.get("item"),
            b.get("mode"),
            _member_link(b.get("user_id"), b.get("display_name")) if b.get("user_id") else b.get("display_name"),
            _fmt_ec(b.get("amount")),
            b.get("status"),
        ])

    player_rows = []
    for p in hist.get("player_rows") or []:
        player_rows.append([
            _member_link(p.get("user_id"), p.get("display_name")),
            p.get("won_count"),
            _fmt_ec(p.get("spent_ec")),
            p.get("bid_count"),
            p.get("highest_bid_count"),
            p.get("sale_count"),
            p.get("junk_count"),
            _dt(p.get("last_activity")),
        ])

    action_rows = []
    for a in hist.get("action_rows") or []:
        payload = a.get("payload") if isinstance(a.get("payload"), dict) else {}
        item = payload.get("item_name") or payload.get("item") or a.get("auction_id")
        aid = str(a.get("auction_id") or payload.get("auction_id") or "")
        action_rows.append([
            _dt(a.get("requested_at")),
            a.get("actor_name") or a.get("actor_id") or "—",
            _loot_action_type_label(a.get("action_type")),
            _auction_link(aid, item) if aid else item,
            _fmt_ec(a.get("amount")),
            _loot_action_status_label(a.get("status")),
            _loot_result_text(a),
        ])

    all_auction_rows = []
    for r in hist.get("auction_rows") or []:
        aid = r.get("auction_id")
        all_auction_rows.append([
            _auction_link(aid, r.get("item")) if aid else r.get("item"),
            r.get("mode"),
            _loot_status_label(r.get("status")),
            r.get("bid_count"),
            r.get("leader"),
            _member_link(r.get("winner_user_id"), r.get("winner_name")) if r.get("winner_user_id") else r.get("winner_name"),
            _fmt_ec(r.get("winning_amount")),
            _dt(r.get("closed_at") or r.get("ends_at")),
        ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="/loot">Loot</a><a href="/loot-check">Truhencheck</a><a href="/members">Mitglieder</a><a href="/exports">Exports</a><a href="/api/loot-history">API</a></nav>
    <section class="hero"><div><div class="eyebrow">Loot-Transparenz</div><h1>📜 Loot-Verlauf</h1><p class="muted">Gewinner, Gebote und Dashboard-Aktionen an einem Ort. Read-only – hier wird nichts am Bot verändert.</p></div><a class="btn" href="/export/loot_history.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🏆 Gewonnene Items</h2>{_table(['Item','Bereich','Gewinner','EC','Gebote','Status','Zeit'], winner_rows, placeholder='Gewinner/Item suchen…')}</section>
    <section class="panel"><h2>🪙 Gebotsverlauf</h2><p class="muted">Zeigt die einzelnen bekannten Gebote aus dem Snapshot. Wenn alte Bot-Snapshots keine Gebotsliste enthalten, steht hier entsprechend weniger.</p>{_table(['Zeit','Item','Bereich','Spieler','Gebot','Status'], bid_rows, placeholder='Gebote durchsuchen…')}</section>
    <section class="panel"><h2>👥 Spielerübersicht</h2>{_table(['Spieler','Gewonnen','EC ausgegeben','Gebote','Führend/Gewonnen','Sale','Müll','Letzte Aktivität'], player_rows, placeholder='Spieler suchen…')}</section>
    <section class="panel"><h2>📨 Dashboard-Aktionen</h2><p class="muted">Queue-Anfragen aus dem Dashboard: Bieten, Sale kaufen oder Müll-Wurf. Hilft beim Nachvollziehen, wenn etwas blockiert/fehlgeschlagen ist.</p>{_table(['Zeit','Spieler','Aktion','Item','EC','Status','Ergebnis'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    <section class="panel"><h2>📦 Alle Auktionen</h2>{_table(['Item','Bereich','Status','Gebote','Führend','Gewinner','EC','Zeit'], all_auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    """
    return _html_shell("Loot-Verlauf · Ebo Dashboard", body)


def _loot_member_payload_from_snapshot(snap: dict[str, Any], user_id: int, guild_id: int = 0) -> dict[str, Any]:
    hist = _loot_history_payload_from_snapshot(snap, int(guild_id or 0))
    uid = int(user_id or 0)
    wins = [r for r in hist.get("winner_rows") or [] if int(r.get("winner_user_id") or 0) == uid]
    bids = [b for b in hist.get("bid_rows") or [] if int(b.get("user_id") or 0) == uid]
    actions = [a for a in hist.get("action_rows") or [] if str(a.get("actor_id") or "") == str(uid)]
    return {"wins": wins, "bids": bids, "actions": actions, "spent_ec": sum(int(_num(r.get("winning_amount"), 0)) for r in wins), "bid_count": len(bids), "won_count": len(wins)}


def _render_member_loot_history(data: dict[str, Any], user_id: int) -> str:
    if not data.get("ok"):
        return _html_shell("Mitglied-Loot · Ebo Dashboard", f"<section class='panel'><h1>🎁 Mitglied-Loot</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)
    guild_id = _safe_guild_id(data)
    user_id = int(user_id or 0)
    display = names.get(user_id, f"User {user_id}")
    payload = _loot_member_payload_from_snapshot(snap, user_id, int(guild_id or 0))

    cards = "".join([
        _card("Gewonnen", payload.get("won_count", 0), "Items"),
        _card("Ausgegeben", _fmt_ec(payload.get("spent_ec", 0)), "EC"),
        _card("Gebote", payload.get("bid_count", 0), "bekannte Gebote"),
        _card("Aktionen", len(payload.get("actions") or []), "Dashboard-Queue"),
    ])

    win_rows = []
    for r in payload.get("wins") or []:
        aid = r.get("auction_id")
        win_rows.append([_auction_link(aid, r.get("item")) if aid else r.get("item"), r.get("mode"), _fmt_ec(r.get("winning_amount")), r.get("bid_count"), _dt(r.get("closed_at"))])

    bid_rows = []
    for b in payload.get("bids") or []:
        aid = b.get("auction_id")
        bid_rows.append([_dt(b.get("created_at")), _auction_link(aid, b.get("item")) if aid else b.get("item"), b.get("mode"), _fmt_ec(b.get("amount")), b.get("status")])

    action_rows = []
    for a in payload.get("actions") or []:
        p = a.get("payload") if isinstance(a.get("payload"), dict) else {}
        item = p.get("item_name") or p.get("item") or a.get("auction_id")
        aid = str(a.get("auction_id") or p.get("auction_id") or "")
        action_rows.append([_dt(a.get("requested_at")), _loot_action_type_label(a.get("action_type")), _auction_link(aid, item) if aid else item, _fmt_ec(a.get("amount")), _loot_action_status_label(a.get("status")), _loot_result_text(a)])

    body = f"""
    <nav class="topnav"><a href="/member/{_e(user_id)}">← Mitglied</a><a href="/loot-history">Loot-Verlauf</a><a href="/loot">Loot</a><a href="/ec">EC</a></nav>
    <section class="hero"><div><div class="eyebrow">Spieler-Loot</div><h1>🎁 {_e(display)}</h1><p class="muted">Persönlicher Loot-, Gebots- und Dashboard-Aktionsverlauf.</p></div><a class="btn" href="/export/member_{_e(user_id)}_loot.csv">CSV herunterladen</a></section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>🏆 Gewonnene Items</h2>{_table(['Item','Bereich','EC','Gebote','Zeit'], win_rows, placeholder='Gewinne durchsuchen…')}</section>
    <section class="panel"><h2>🪙 Gebote</h2>{_table(['Zeit','Item','Bereich','EC','Status'], bid_rows, placeholder='Gebote durchsuchen…')}</section>
    <section class="panel"><h2>📨 Dashboard-Aktionen</h2>{_table(['Zeit','Aktion','Item','EC','Status','Ergebnis'], action_rows, placeholder='Aktionen durchsuchen…')}</section>
    """
    return _html_shell(f"{display} Loot · Ebo Dashboard", body)


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
        ["Loot-Verlauf", _raw('<a class="link" href="/export/loot_history.csv">loot_history.csv</a>'), "Gewinner, Gebote, Dashboard-Aktionen"],
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
    <nav class="topnav"><a href="/members">← Mitglieder</a><a href="/portal/member/{_e(user_id)}">Portal</a><a href="/member/{_e(user_id)}/loot">Loot-Verlauf</a><a href="/attendance-stats">Anwesenheit-Stats</a><a href="/analytics">Analytics</a><a href="/voice">Voice</a><a href="#needs">Needs</a><a href="#events">Events</a><a href="#ec">EC</a><a href="#voice">Voice</a></nav>
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
def loot_page(request: Request, msg: str = "", _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_loot_center(_snapshot_payload(), request, msg=msg))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)



@app.post("/admin/loot/drop")
async def admin_loot_drop_dashboard(request: Request, _: bool = Depends(_admin_auth)):
    try:
        raw = await request.body()
        form = _parse_urlencoded_body(raw)
        drop_type = str(form.get("drop_type") or "loot_drop").strip().lower()
        item = str(form.get("item") or "").strip()
        payload = _snapshot_payload()
        guild_id = _safe_guild_id(payload)
        actor = _current_user(request) or {}
        result = _enqueue_loot_drop_request(int(guild_id), drop_type, item, actor)
        if result.get("ok"):
            msg = "✅ " + str(result.get("message") or "Drop wurde an den Bot gesendet.")
        else:
            msg = "❌ " + str(result.get("error") or "Drop konnte nicht gesendet werden.")
    except Exception as exc:
        msg = f"❌ Fehler: {type(exc).__name__}: {exc}"
    return RedirectResponse(f"/loot?msg={urllib.parse.quote(msg)}", status_code=303)



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




@app.get("/api/member-center")
def api_member_center(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    return JSONResponse({"ok": True, "member_center": _member_center_payload(payload)})


@app.get("/export/member_center.csv")
def export_member_center_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return _csv_response("member_center.csv", ["error"], [[payload.get("error")]])
    center = _member_center_payload(payload)
    rows = []
    for r in center.get("rows") or []:
        rows.append([
            r.get("user_id"),
            r.get("display_name"),
            r.get("ingame_name"),
            r.get("role"),
            r.get("gearscore"),
            r.get("ec_balance"),
            r.get("main_need_count"),
            r.get("secondary_need_count"),
            r.get("attendance_rate"),
            r.get("attendance_present"),
            r.get("attendance_partial"),
            r.get("attendance_absent"),
            r.get("attendance_open"),
            r.get("voice_hours"),
            r.get("won_count"),
            r.get("spent_ec"),
            r.get("bid_count"),
            r.get("admin_status"),
            r.get("hint"),
            r.get("last_seen"),
        ])
    return _csv_response("member_center.csv", ["user_id","display_name","ingame_name","role","gearscore","ec","main_needs","secondary_needs","attendance_rate","present","partial","absent","open","voice_hours","loot_won","loot_spent_ec","bid_count","admin_status","hint","last_seen"], rows)

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
    return JSONResponse({"ok": True, "loot": (snap.get("loot") or {}), "insights": (_insights(snap).get("loot") or {}), "phase3_loot_read_cutover": payload.get("phase3_loot_read_cutover") or {}})


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



@app.get("/loot-history", response_class=HTMLResponse)
def loot_history_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_loot_history(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/loot-history")
def api_loot_history(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    guild_id = _safe_guild_id(payload)
    hist = _loot_history_payload_from_snapshot(payload.get("snapshot") or {}, int(guild_id or 0))
    safe = {}
    for key, value in hist.items():
        if isinstance(value, list):
            safe[key] = [{k: v for k, v in row.items() if k not in {"raw", "status_label"}} if isinstance(row, dict) else row for row in value]
        else:
            safe[key] = value
    return JSONResponse({"ok": True, "loot_history": safe})


@app.get("/export/loot_history.csv")
def export_loot_history_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    snap = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    hist = _loot_history_payload_from_snapshot(snap, int(guild_id or 0))
    rows = []
    for r in hist.get("winner_rows") or []:
        rows.append([
            "winner",
            r.get("auction_id"),
            r.get("item"),
            r.get("mode"),
            r.get("status"),
            r.get("winner_user_id"),
            r.get("winner_name"),
            r.get("winning_amount"),
            r.get("bid_count"),
            _dt(r.get("closed_at")),
            "",
            "",
        ])
    for b in hist.get("bid_rows") or []:
        rows.append([
            "bid",
            b.get("auction_id"),
            b.get("item"),
            b.get("mode"),
            b.get("status"),
            b.get("user_id"),
            b.get("display_name"),
            b.get("amount"),
            "",
            _dt(b.get("created_at")),
            "",
            "",
        ])
    for a in hist.get("action_rows") or []:
        p = a.get("payload") if isinstance(a.get("payload"), dict) else {}
        rows.append([
            "dashboard_action",
            a.get("auction_id"),
            p.get("item_name") or p.get("item") or "",
            p.get("phase") or "",
            a.get("status"),
            a.get("actor_id"),
            a.get("actor_name"),
            a.get("amount"),
            "",
            _dt(a.get("requested_at")),
            a.get("action_type"),
            _loot_result_text(a),
        ])
    return _csv_response("loot_history.csv", ["typ","auction_id","item","bereich","status","user_id","spieler","ec","gebote","zeit","aktion","ergebnis"], rows)



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




def _parse_urlencoded_body(raw: bytes) -> dict[str, str]:
    parsed = urllib.parse.parse_qs((raw or b"").decode("utf-8", errors="replace"), keep_blank_values=True)
    return {str(k): str(v[-1] if v else "") for k, v in parsed.items()}


# ---------------------------------------------------------------------------
# Schritt 3: Spieler- & Mitgliederportal / Need-Änderungen
# ---------------------------------------------------------------------------

def _is_portal_admin(request: Request) -> bool:
    if _is_dashboard_admin(request):
        return True
    if _auth_mode() in {"basic", "hybrid"} and not _discord_oauth_enabled():
        return True
    return False


def _current_user_id(request: Request) -> int:
    user = _current_user(request) or {}
    return _user_id(user.get("user_id") or user.get("id"))


def _portal_can_view(request: Request, user_id: int) -> bool:
    uid = int(user_id or 0)
    me = _current_user_id(request)
    return bool(uid and (me == uid or _is_portal_admin(request)))


def _portal_user_name(request: Request) -> str:
    u = _current_user(request) or {}
    return str(u.get("username") or u.get("global_name") or u.get("user_id") or "Dashboard")


def _need_change_status_label(status: Any) -> dict[str, str]:
    s = str(status or "pending").lower()
    labels = {
        "pending": "⏳ offen",
        "processing": "⚙️ Verarbeitung",
        "done": "✅ erledigt",
        "failed": "❌ Fehler",
        "rejected": "⛔ blockiert",
        "cancelled": "🚫 abgebrochen",
    }
    cls = "pill"
    if s in {"failed", "rejected"}:
        cls += " bad"
    elif s == "done":
        cls += " ok"
    return _raw(f'<span class="{cls}">{_e(labels.get(s, s))}</span>')


def _need_change_requests(guild_id: int, *, user_id: int = 0, limit: int = 80) -> list[dict[str, Any]]:
    if not _database_url() or not guild_id:
        return []
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if user_id:
                cur.execute(
                    """
                    SELECT id, request_id, guild_id, target_user_id, action_type, status, payload_json,
                           actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                    FROM dashboard_need_change_requests
                    WHERE guild_id = %s AND target_user_id = %s
                    ORDER BY requested_at DESC, id DESC
                    LIMIT %s
                    """,
                    (int(guild_id), int(user_id), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT id, request_id, guild_id, target_user_id, action_type, status, payload_json,
                           actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                    FROM dashboard_need_change_requests
                    WHERE guild_id = %s
                    ORDER BY requested_at DESC, id DESC
                    LIMIT %s
                    """,
                    (int(guild_id), int(limit)),
                )
            rows = []
            for r in (cur.fetchall() or []):
                d = dict(r)
                try:
                    d["payload"] = json.loads(d.get("payload_json") or "{}")
                except Exception:
                    d["payload"] = {}
                try:
                    d["result"] = json.loads(d.get("result_json") or "{}")
                except Exception:
                    d["result"] = {}
                rows.append(d)
            return rows
    finally:
        conn.close()


def _create_need_change_request(guild_id: int, target_user_id: int, action_type: str, payload: dict[str, Any], actor: dict[str, Any]) -> str:
    if not _database_url() or not guild_id:
        raise RuntimeError("DATABASE_URL fehlt. Need-Änderungen laufen über Postgres-Queue.")
    request_id = f"need_{int(time.time())}_{secrets.token_hex(5)}"
    actor_id = str(actor.get("user_id") or "")
    actor_name = str(actor.get("username") or actor.get("global_name") or actor_id or "Dashboard")
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_need_change_requests
                    (request_id, guild_id, target_user_id, action_type, status, payload_json, actor_id, actor_name)
                VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s)
                """,
                (request_id, int(guild_id), int(target_user_id), str(action_type), json.dumps(payload, ensure_ascii=False), actor_id, actor_name),
            )
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (int(guild_id), "need_change_request", "member", str(target_user_id), actor_id, actor_name, json.dumps({"request_id": request_id, "action_type": action_type, "slot": payload.get("slot"), "tab": payload.get("tab")}, ensure_ascii=False)),
            )
        conn.commit()
    finally:
        conn.close()
    return request_id


def _portal_active_events(snap: dict[str, Any], user_id: int = 0) -> list[dict[str, Any]]:
    events = []
    now = datetime.now(timezone.utc)
    for ev in ((snap.get("events") or {}).get("items") or []):
        if not isinstance(ev, dict):
            continue
        dt = _dt_obj(ev.get("when_iso") or ev.get("start_at"))
        if _is_running_event(ev) or (dt and dt >= now):
            events.append(ev)
    events.sort(key=lambda ev: _dt_obj(ev.get("when_iso") or ev.get("start_at")) or datetime.max.replace(tzinfo=timezone.utc))
    return events[:40]


def _portal_event_status_for_user(ev: dict[str, Any], user_id: int) -> str:
    uid = int(user_id or 0)
    parts = ev.get("participants") or {}
    for group in parts.get("yes") or []:
        if isinstance(group, dict) and any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in group.get("participants") or []):
            return "✅ " + str(group.get("role") or "Zusage")
    if any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in parts.get("maybe") or []):
        return "🟡 Vielleicht"
    if any(isinstance(p, dict) and _user_id(p.get("user_id")) == uid for p in parts.get("no") or []):
        return "❌ Abgemeldet"
    return "—"


def _portal_active_auctions(snap: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        return list((_loot_center_payload_from_snapshot(snap).get("active") or [])[:40])
    except Exception:
        return []


def _slot_options_html() -> str:
    return "".join(f'<option value="{_e(slot)}">{_e(slot)}</option>' for slot in ["Waffe 1","Waffe 2","Fähigkeitskern","Helm","Brust","Hose","Handschuhe","Schuhe","Brosche","Ohrringe","Kette","Armband","Ring 1","Ring 2","Gürtel","Umhang"])


def _render_need_editor_panel(user_id: int, current_user: Optional[dict[str, Any]], requests: list[dict[str, Any]]) -> str:
    req_rows = []
    for r in requests[:30]:
        p = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        res = r.get("result") if isinstance(r.get("result"), dict) else {}
        detail = res.get("message") or res.get("error") or p.get("item_name") or p.get("item_text") or p.get("item_id") or "—"
        req_rows.append([_dt(r.get("requested_at")), _need_change_status_label(r.get("status")), r.get("action_type"), p.get("tab"), p.get("slot"), _short(detail, 120)])
    actor_hint = "Du bearbeitest deine eigene Needliste." if current_user else "Discord-Login nötig."
    return f"""
    <section class="panel" id="need-editor">
      <h2>✍️ Needliste bearbeiten</h2>
      <p class="muted">{_e(actor_hint)} Das Dashboard stellt nur einen Änderungsantrag. Der Bot schreibt danach die echte Needliste und der Vorgang bleibt geloggt.</p>
      <div class="split">
        <form method="post" action="/portal/member/{int(user_id)}/need-change" style="display:grid; gap:10px;">
          <input type="hidden" name="action_type" value="set">
          <label>Bereich<br><select name="tab" style="width:100%; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);"><option value="Main">Main</option><option value="Secondary">Secondary</option></select></label>
          <label>Slot<br><select name="slot" style="width:100%; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);">{_slot_options_html()}</select></label>
          <label>Itemname oder Item-ID<br><input name="item_text" maxlength="160" placeholder="z. B. Aridus Stab" style="width:100%; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);"></label>
          <button class="btn" type="submit" style="border:0; cursor:pointer;">Need setzen</button>
        </form>
        <form method="post" action="/portal/member/{int(user_id)}/need-change" style="display:grid; gap:10px; align-content:start;">
          <input type="hidden" name="action_type" value="clear">
          <label>Bereich<br><select name="tab" style="width:100%; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);"><option value="Main">Main</option><option value="Secondary">Secondary</option></select></label>
          <label>Slot löschen<br><select name="slot" style="width:100%; padding:10px; border-radius:10px; background:#08090d; color:var(--text); border:1px solid var(--line);">{_slot_options_html()}</select></label>
          <button class="btn" type="submit" style="border:0; cursor:pointer; background:#303442; color:var(--text);">Need entfernen</button>
          <p class="muted">Bereits als erhalten gesperrte Slots kann nur die Leitung ändern.</p>
        </form>
      </div>
      <h3>Änderungslog</h3>
      {_table(['Zeit','Status','Aktion','Bereich','Slot','Ergebnis'], req_rows, placeholder='Need-Änderungen durchsuchen…')}
    </section>
    """


def _render_member_portal(data: dict[str, Any], user_id: int, request: Request, msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Portal · Ebo Dashboard", f"<section class='panel'><h1>👤 Portal</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    if not _portal_can_view(request, int(user_id)):
        raise HTTPException(status_code=403, detail="Du darfst nur dein eigenes Portal sehen. Leitung/Admins sehen alle Mitglieder.")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    names = _profile_name_map(snap)
    display = names.get(int(user_id), f"User {int(user_id)}")
    balances = _balance_map(snap)
    need_info = _needs_by_user(snap).get(int(user_id), {})
    main_needs = need_info.get("main") if isinstance(need_info, dict) else []
    sec_needs = need_info.get("secondary") if isinstance(need_info, dict) else []
    response = _event_response_counts_for_user(snap, int(user_id))
    loot_payload = _loot_member_payload_from_snapshot(snap, int(user_id), int(guild_id or 0))
    active_events = _portal_active_events(snap, int(user_id))
    active_auctions = _portal_active_auctions(snap)
    need_requests = _need_change_requests(guild_id, user_id=int(user_id), limit=60) if guild_id else []

    event_rows = []
    for ev in active_events:
        eid = str(ev.get("event_id") or ev.get("id") or "")
        event_rows.append([_event_link(eid, ev.get("title") or eid), _dt(ev.get("when_iso") or ev.get("start_at")), _event_status_text(ev), _portal_event_status_for_user(ev, int(user_id)), _raw(f'<a class="link" href="/attendance/{_e(eid)}">Review</a>') if eid else "—"])

    auction_rows = []
    for a in active_auctions:
        aid = str(a.get("auction_id") or "")
        auction_rows.append([_auction_link(aid, a.get("item")), a.get("mode"), _fmt_ec(a.get("start_bid") or 0), a.get("leader") or "—", _dt(a.get("ends_at")), _raw(f'<a class="btn" href="/auction/{_e(aid)}">Bieten</a>') if aid else "—"])

    tx_rows = [[_dt(tx.get("created_at")), _fmt_ec(tx.get("amount")), tx.get("raw_type"), _short(tx.get("reason"), 140)] for tx in _tx_for_user(snap, int(user_id), limit=30)]
    loot_rows = []
    for r in loot_payload.get("wins") or []:
        aid = r.get("auction_id")
        loot_rows.append([_auction_link(aid, r.get("item")) if aid else r.get("item"), r.get("mode"), _fmt_ec(r.get("winning_amount")), _dt(r.get("closed_at"))])

    admin_links = ""
    if _is_portal_admin(request):
        admin_links = f'<a class="btn" href="/member/{int(user_id)}">Leitungsprüfung</a><a class="btn" href="/member/{int(user_id)}/loot">Loot prüfen</a><a class="btn" href="/attendance-stats">Attendance prüfen</a>'
    msg_html = f'<div class="ok">{_e(msg)}</div>' if msg else ""
    cards = "".join([
        _card("EC", _fmt_ec(balances.get(int(user_id))) if balances.get(int(user_id)) is not None else "—", "aktueller Stand"),
        _card("Needs", f"{len(main_needs)} / {len(sec_needs)}", "Main / Secondary"),
        _card("Events", f"{response.get('yes',0)} Zusagen", f"{response.get('maybe',0)} vielleicht · {response.get('no',0)} nein"),
        _card("Loot", loot_payload.get("won_count", 0), f"ausgegeben: {_fmt_ec(loot_payload.get('spent_ec', 0))} EC"),
    ])
    portal_nav = '<nav class="topnav"><a href="/">Kommando</a><a href="/portal">Mein Portal</a><a href="/events">Events</a><a href="/loot">Loot</a><a href="/members">Mitglieder</a><a href="/ec">EC</a></nav>' if _is_portal_admin(request) else '<nav class="topnav"><a href="/member">Start</a><a href="/member#members">Mitglieder</a><a href="/member#events">Events</a><a href="/member#auctions">Auktionen</a><a href="/portal">Eigenes Profil</a><a href="#needs">Meine Needs</a></nav>'
    body = f"""
    {portal_nav}
    <section class="hero">
      <div><div class="eyebrow">Spieler- & Mitgliederportal</div><h1>👤 {_e(display)}</h1><p class="muted">Eigene Daten, laufende Events, laufende Auktionen, EC, Attendance, Loot und Needliste.</p></div>
      <div class="hero-actions">{admin_links}<a class="btn" href="/logout">Logout</a></div>
    </section>
    {msg_html}
    <section class="grid">{cards}</section>
    <section class="panel"><h2>📅 Laufende & kommende Events</h2>{_table(['Event','Zeit','Status','Deine Anmeldung','Aktion'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel"><h2>⚔️ Laufende Auktionen</h2><p class="muted">Zum Bieten die Auktion öffnen. Bieten läuft weiterhin sicher über Bot-Queue.</p>{_table(['Item','Bereich','Start/Preis','Führend','Ende','Aktion'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel" id="needs"><h2>🎁 Deine Needliste</h2><div class="split"><div>{_need_list_html('Main-Needs', main_needs)}</div><div>{_need_list_html('Secondary-Needs', sec_needs)}</div></div></section>
    {_render_need_editor_panel(int(user_id), _current_user(request), need_requests)}
    <section class="panel"><h2>🪙 EC-Verlauf</h2>{_table(['Zeit','Betrag','Typ','Grund'], tx_rows, placeholder='EC durchsuchen…')}</section>
    <section class="panel"><h2>🏆 Loot-Historie</h2>{_table(['Item','Bereich','EC','Zeit'], loot_rows, placeholder='Loot durchsuchen…')}</section>
    """
    shell_mode = "admin" if _is_portal_admin(request) else "member"
    return _html_shell(f"{display} Portal · Ebo Dashboard", body, nav_mode=shell_mode)



def _render_member_home(data: dict[str, Any], request: Request) -> str:
    if not data.get("ok"):
        return _html_shell("Mitgliederbereich · Ebo Dashboard", f"<section class='panel'><h1>🏠 Mitgliederbereich</h1><p class='muted'>{_e(data.get('error'))}</p></section>", nav_mode="member")
    user = _current_user(request) or {}
    uid = _current_user_id(request)
    snap: dict[str, Any] = data.get("snapshot") or {}
    names = _profile_name_map(snap)

    display = names.get(int(uid), str(user.get("username") or "Mitglied")) if uid else str(user.get("username") or "Mitglied")

    # Öffentliche, reduzierte Mitgliederliste: keine Adminnotizen, keine EC-Queue, keine internen Prüfmarkierungen.
    members_payload = _member_center_payload(data)
    member_rows: list[list[Any]] = []
    for r in (members_payload.get("rows") or [])[:120]:
        if not isinstance(r, dict):
            continue
        member_rows.append([
            _member_link(r.get("user_id"), r.get("display_name")),
            r.get("ingame_name") or "—",
            r.get("main_role") or r.get("role") or "—",
            r.get("gearscore") or "—",
        ])

    running_events = []
    for ev in _events_items(snap):
        if isinstance(ev, dict) and _is_running_event(ev):
            running_events.append(ev)
    running_events.sort(key=lambda ev: _dt_obj(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at")) or datetime.max.replace(tzinfo=timezone.utc))

    event_rows: list[list[Any]] = []
    for ev in running_events[:30]:
        eid = str(ev.get("event_id") or ev.get("id") or "")
        event_rows.append([
            _event_link(eid, ev.get("title") or ev.get("name") or eid),
            _dt(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at")),
            _event_status_text(ev),
            _portal_event_status_for_user(ev, int(uid or 0)) if uid else "—",
        ])

    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    active_auctions = [a for a in auctions if isinstance(a, dict) and _loot_is_active(a)]
    active_auctions.sort(key=lambda a: _dt_obj(a.get("ends_at") or a.get("end_at") or a.get("expires_at")) or datetime.max.replace(tzinfo=timezone.utc))
    auction_rows: list[list[Any]] = []
    for a in active_auctions[:30]:
        aid = str(a.get("auction_id") or "")
        auction_rows.append([
            _auction_link(aid, a.get("item_name") or a.get("item") or aid),
            _phase_label(a),
            _loot_effective_status_label(a),
            _auction_leader_text(a, names),
            _dt(a.get("ends_at") or a.get("end_at") or a.get("expires_at")),
            _raw(f'<a class="btn" href="/auction/{_e(aid)}">Öffnen</a>') if aid else "—",
        ])

    cards = "".join([
        _card("Mitglieder", len(member_rows), "Gildenübersicht"),
        _card("Laufende Events", len(running_events), "bis 1h nach Start"),
        _card("Laufende Auktionen", len(active_auctions), "Bieten/Kaufen möglich"),
        _card("Profil", display, "eigene Daten & Needs"),
    ])

    body = f"""
    <nav class="topnav"><a href="/member">Start</a><a href="#members">Mitglieder</a><a href="#events">Events</a><a href="#auctions">Auktionen</a><a href="/portal">Eigenes Profil</a><a href="/portal#needs">Meine Needs</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Mitgliederbereich</div>
        <h1>🏠 Willkommen, {_e(display)}</h1>
        <p class="muted">Reduzierte Ansicht für normale Mitglieder: Mitglieder, laufende Events, laufende Auktionen, eigenes Profil und eigene Needliste.</p>
      </div>
      <div class="hero-actions">
        <a class="hero-action" href="/portal"><span>👤</span><strong>Eigenes Profil</strong><small>Daten & Übersicht</small></a>
        <a class="hero-action" href="/portal#needs"><span>🎁</span><strong>Meine Needs</strong><small>ansehen/bearbeiten</small></a>
        <a class="hero-action" href="#auctions"><span>🏆</span><strong>Auktionen</strong><small>bieten/kaufen</small></a>
      </div>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel" id="events"><h2>📅 Laufende Events</h2><p class="muted">Nach deiner Regel sichtbar ab Erstellung bis 1 Stunde nach Eventbeginn.</p>{_table(['Event','Zeit','Status','Deine Anmeldung'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel" id="auctions"><h2>🏆 Laufende Auktionen</h2><p class="muted">Zum Bieten oder Kaufen die Auktion öffnen. Aktionen laufen weiter über die Bot-Queue.</p>{_table(['Item','Bereich','Status','Führend','Ende','Aktion'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel" id="members"><h2>👥 Mitglieder</h2>{_table(['Discord','Ingame','Rolle','Gearscore'], member_rows, placeholder='Mitglieder durchsuchen…')}</section>
    """
    return _html_shell("Mitgliederbereich · Ebo Dashboard", body, nav_mode="member")

    """Letzte Dashboard-Einstellungsanträge.

    Das Dashboard schreibt keine Bot-JSON. Es legt nur Änderungsanträge in Postgres ab.
    Der Bot verarbeitet sie und schreibt dann dkp_cfg.json.
    """
    if not _database_url() or not guild_id:
        return []
    try:
        _ensure_admin_tables()
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, request_id, guild_id, scope, action_type, status, payload_json,
                           actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                    FROM dashboard_settings_change_requests
                    WHERE guild_id = %s
                    ORDER BY requested_at DESC, id DESC
                    LIMIT %s
                    """,
                    (int(guild_id), int(limit)),
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
    except Exception:
        return []


def _snapshot_setting_value(snap: dict[str, Any], suffix: str, default: Any = "") -> Any:
    """Sucht einen Wert aus den vom Bot exportierten Settings-Zeilen.

    Beispiel: suffix='weekly_event_limit' findet dkp_cfg.weekly_event_limit.
    """
    wanted = str(suffix or "").strip().lower()
    for row in ((snap.get("settings") or {}).get("settings") or []):
        if not isinstance(row, dict):
            continue
        src = str(row.get("source") or "").lower()
        key = str(row.get("key") or "").lower()
        if src == "dkp_cfg" and (key == wanted or key.endswith("." + wanted) or wanted in key):
            return row.get("value")
    return default


def _current_dkp_settings_from_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    event_types = ["Gildenboss", "HM Raid", "NM Raid", "Normal Raid", "Übungsrun HM Raid", "Übungsrun Trials", "Segensstein PvP"]
    default_points = {
        "Gildenboss": 20,
        "HM Raid": 12,
        "NM Raid": 12,
        "Normal Raid": 12,
        "Übungsrun HM Raid": 15,
        "Übungsrun Trials": 15,
        "Segensstein PvP": 5,
    }
    rows = ((snap.get("settings") or {}).get("settings") or [])
    def _find_any(names: list[str], default: Any = "") -> Any:
        wanted = [str(n).lower() for n in names]
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").lower()
            src = str(row.get("source") or "").lower()
            for n in wanted:
                if key == n or key.endswith("." + n) or n in key or n in src:
                    return row.get("value")
        return default

    points: dict[str, Any] = {}
    for et in event_types:
        found = None
        needle = "event_points." + et.lower()
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").lower()
            src = str(row.get("source") or "").lower()
            if src == "dkp_cfg" and (key.endswith(needle) or needle in key):
                found = row.get("value")
                break
        points[et] = found if found is not None else default_points.get(et, 0)

    settings = snap.get("settings") or {}
    guild = snap.get("guild") or {}
    member_filter = settings.get("member_filter") or guild.get("member_filter") or {}
    auth = snap.get("auth") or {}
    member_role = auth.get("member_role") if isinstance(auth.get("member_role"), dict) else {}
    admin_roles = auth.get("admin_roles") if isinstance(auth.get("admin_roles"), list) else []
    allowed_roles = auth.get("allowed_roles") if isinstance(auth.get("allowed_roles"), list) else []
    auth_admin_ids = [str(r.get("role_id") or "").strip() for r in admin_roles if isinstance(r, dict) and str(r.get("role_id") or "").strip()]
    auth_allowed_ids = [str(r.get("role_id") or "").strip() for r in allowed_roles if isinstance(r, dict) and str(r.get("role_id") or "").strip()]

    return {
        "event_types": event_types,
        "event_points": points,
        "weekly_event_limit": _snapshot_setting_value(snap, "weekly_event_limit", 40),
        "decay_percent": _snapshot_setting_value(snap, "decay_percent", 15),
        "decay_protected_balance": _snapshot_setting_value(snap, "decay_protected_balance", 50),
        "log_channel_id": _snapshot_setting_value(snap, "log_channel_id", _find_any(["log_channel_id"], "")),
        "leader_role_id": _find_any(["leader_role_id", "admin_role_id"], ""),
        "member_role_id": str((member_filter or {}).get("role_id") or (member_role or {}).get("role_id") or _find_any(["member_role_id"], "")),
        "dashboard_admin_role_ids": ",".join(auth_admin_ids or sorted(_admin_role_ids())) or _find_any(["dashboard_admin_role_ids"], ""),
        "dashboard_allowed_role_ids": ",".join(auth_allowed_ids or sorted(_allowed_role_ids())) or _find_any(["dashboard_allowed_role_ids"], ""),
        "auth_snapshot": auth,
    }

def _enqueue_settings_change_request(guild_id: int, action_type: str, payload: dict[str, Any], actor: dict[str, Any]) -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt. Ohne Postgres kann das Dashboard keine Einstellungsänderung an den Bot übergeben."}
    if not guild_id:
        return {"ok": False, "error": "Guild-ID fehlt."}
    action = str(action_type or "").strip().lower()
    allowed_actions = {"set_event_points", "set_weekly_limit", "set_decay", "set_roles", "set_access_roles"}
    if action not in allowed_actions:
        return {"ok": False, "error": "Unbekannte Einstellungsaktion."}
    clean_payload = dict(payload or {})

    def _clean_id(value: Any, field: str, allow_empty: bool = True) -> str:
        raw = str(value or "").strip().replace(" ", "")
        if raw in {"", "0", "—", "-"}:
            return "0" if allow_empty else ""
        if not raw.isdigit() or len(raw) < 10 or len(raw) > 25:
            raise ValueError(f"{field} muss eine Discord-ID sein.")
        return raw

    def _clean_id_list(value: Any, field: str) -> str:
        parts = [p.strip() for p in str(value or "").replace(";", ",").split(",") if p.strip()]
        out = []
        for p in parts:
            if not p.isdigit() or len(p) < 10 or len(p) > 25:
                raise ValueError(f"{field} enthält eine ungültige Discord-ID: {p}")
            if p not in out:
                out.append(p)
        return ",".join(out)

    # Dashboard-Vorprüfung. Der Bot prüft später final nochmal.
    try:
        if action == "set_event_points":
            et = str(clean_payload.get("event_type") or "").strip()
            pts = int(float(str(clean_payload.get("points") or "0").replace(",", ".")))
            if not et:
                return {"ok": False, "error": "Eventtyp fehlt."}
            if pts < 0 or pts > 500:
                return {"ok": False, "error": "EC-Wert muss zwischen 0 und 500 liegen."}
            clean_payload = {"event_type": et, "points": pts}
        elif action == "set_weekly_limit":
            limit = int(float(str(clean_payload.get("weekly_event_limit") or "0").replace(",", ".")))
            if limit < 0 or limit > 1000:
                return {"ok": False, "error": "Wochenlimit muss zwischen 0 und 1000 liegen."}
            clean_payload = {"weekly_event_limit": limit}
        elif action == "set_decay":
            percent = float(str(clean_payload.get("decay_percent") or "0").replace(",", "."))
            protected = int(float(str(clean_payload.get("decay_protected_balance") or "0").replace(",", ".")))
            if percent < 0 or percent > 100:
                return {"ok": False, "error": "Verfall muss zwischen 0 und 100 Prozent liegen."}
            if protected < 0 or protected > 100000:
                return {"ok": False, "error": "Schutzbetrag ist unplausibel."}
            clean_payload = {"decay_percent": percent, "decay_protected_balance": protected}
        elif action == "set_roles":
            clean_payload = {
                "leader_role_id": _clean_id(clean_payload.get("leader_role_id"), "Leitungsrolle"),
                "member_role_id": _clean_id(clean_payload.get("member_role_id"), "Gildenrolle"),
                "log_channel_id": _clean_id(clean_payload.get("log_channel_id"), "EC-/Loot-Logkanal"),
            }
        elif action == "set_access_roles":
            clean_payload = {
                "dashboard_admin_role_ids": _clean_id_list(clean_payload.get("dashboard_admin_role_ids"), "Dashboard-Adminrollen"),
                "dashboard_allowed_role_ids": _clean_id_list(clean_payload.get("dashboard_allowed_role_ids"), "Dashboard-Zugriffsrollen"),
            }
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception:
        return {"ok": False, "error": "Ungültige Zahl oder Discord-ID in der Änderung."}

    actor_id = str(actor.get("user_id") or "").strip()
    actor_name = str(actor.get("username") or actor_id or "Dashboard")
    request_id = f"dash-settings-{int(time.time())}-{secrets.token_hex(6)}"
    clean_payload.update({
        "requested_by": {"id": actor_id, "name": actor_name},
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source": "dashboard_settings_change",
    })
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_settings_change_requests
                    (request_id, guild_id, scope, action_type, status, payload_json, actor_id, actor_name, requested_at)
                VALUES (%s, %s, 'settings', %s, 'pending', %s, %s, %s, NOW())
                RETURNING id, request_id, status
                """,
                (request_id, int(guild_id), action, json.dumps(clean_payload, ensure_ascii=False, separators=(",", ":")), actor_id, actor_name),
            )
            row = dict(cur.fetchone() or {})
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, actor_id, actor_name, action, target_type, target_id, before_json, after_json, created_at)
                VALUES (%s, %s, %s, %s, 'settings', %s, '{}', %s, NOW())
                """,
                (int(guild_id), actor_id, actor_name, f"settings_change_{action}", request_id, json.dumps(clean_payload, ensure_ascii=False)),
            )
        conn.commit()
        row["ok"] = True
        return row
    finally:
        conn.close()


def _settings_request_admin_action(guild_id: int, request_id: str, action: str, actor: dict[str, Any]) -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt."}
    if not guild_id:
        return {"ok": False, "error": "Guild-ID fehlt."}
    request_id = str(request_id or "").strip()
    action = str(action or "").strip().lower()
    if not request_id:
        return {"ok": False, "error": "Request-ID fehlt."}
    if action not in {"cancel", "retry"}:
        return {"ok": False, "error": "Unbekannte Queue-Aktion."}
    actor_id = str(actor.get("user_id") or "").strip()
    actor_name = str(actor.get("username") or actor_id or "Dashboard")
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status, action_type, payload_json FROM dashboard_settings_change_requests WHERE guild_id=%s AND request_id=%s", (int(guild_id), request_id))
            old = dict(cur.fetchone() or {})
            if not old:
                return {"ok": False, "error": "Änderungsantrag nicht gefunden."}
            status = str(old.get("status") or "").lower()
            if action == "cancel":
                if status not in {"pending"}:
                    return {"ok": False, "error": f"Nur offene Anträge können abgebrochen werden. Aktuell: {status}"}
                cur.execute("""
                    UPDATE dashboard_settings_change_requests
                    SET status='cancelled', processed_at=NOW(), result_json=%s
                    WHERE guild_id=%s AND request_id=%s AND status='pending'
                """, (json.dumps({"ok": True, "message": "Vom Dashboard abgebrochen", "actor": actor_name}, ensure_ascii=False), int(guild_id), request_id))
                new_status = "cancelled"
            else:
                if status not in {"failed", "rejected", "cancelled"}:
                    return {"ok": False, "error": f"Nur fehlgeschlagene/blockierte/abgebrochene Anträge können neu geöffnet werden. Aktuell: {status}"}
                cur.execute("""
                    UPDATE dashboard_settings_change_requests
                    SET status='pending', claimed_at=NULL, processed_at=NULL, result_json=NULL
                    WHERE guild_id=%s AND request_id=%s AND status IN ('failed','rejected','cancelled')
                """, (int(guild_id), request_id))
                new_status = "pending"
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, actor_id, actor_name, action, target_type, target_id, before_json, after_json, created_at)
                VALUES (%s, %s, %s, %s, 'settings_request', %s, %s, %s, NOW())
                """,
                (int(guild_id), actor_id, actor_name, f"settings_request_{action}", request_id, json.dumps(old, ensure_ascii=False), json.dumps({"status": new_status}, ensure_ascii=False)),
            )
        conn.commit()
        return {"ok": True, "status": new_status}
    finally:
        conn.close()

def _render_admin_settings_editor(data: dict[str, Any], msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Admin-Einstellungen · Ebo Dashboard", f"<section class='panel'><h1>⚙️ Admin-Einstellungen</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    cfg = _current_dkp_settings_from_snapshot(snap)
    requests = _settings_change_requests_for_dashboard(guild_id, 100) if guild_id else []
    counts = _queue_status_counts(requests)
    cards = "".join([
        _card("Offen", counts.get("pending", 0), "wartet auf Bot"),
        _card("In Arbeit", counts.get("processing", 0), "Bot verarbeitet"),
        _card("Erledigt", counts.get("done", 0), "übernommen"),
        _card("Problem", counts.get("failed", 0) + counts.get("rejected", 0) + counts.get("cancelled", 0), "failed/rejected/cancelled"),
    ])

    point_rows = []
    for et in cfg.get("event_types") or []:
        val = (cfg.get("event_points") or {}).get(et, "")
        form = f"""
        <form method='post' action='/admin/settings-change' style='display:flex; gap:8px; align-items:center; flex-wrap:wrap;'>
          <input type='hidden' name='action_type' value='set_event_points'>
          <input type='hidden' name='event_type' value='{_e(et)}'>
          <input name='points' value='{_e(val)}' inputmode='numeric' style='max-width:110px;'>
          <button class='btn compact' type='submit'>ändern</button>
        </form>
        """
        point_rows.append([et, val, _raw(form)])

    req_rows = []
    for r in requests:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        result = r.get("result") if isinstance(r.get("result"), dict) else {}
        detail = result.get("message") or result.get("error") or payload.get("event_type") or payload.get("weekly_event_limit") or payload.get("decay_percent") or payload.get("leader_role_id") or payload.get("member_role_id") or r.get("request_id")
        rid = _e(r.get("request_id") or "")
        status = str(r.get("status") or "").lower()
        actions = []
        if status == "pending":
            actions.append(f"<form method='post' action='/admin/settings-request/{rid}/cancel' style='display:inline'><button class='btn compact danger' type='submit' onclick=\"return confirm('Offenen Antrag abbrechen?')\">abbrechen</button></form>")
        if status in {"failed", "rejected", "cancelled"}:
            actions.append(f"<form method='post' action='/admin/settings-request/{rid}/retry' style='display:inline'><button class='btn compact' type='submit'>neu öffnen</button></form>")
        req_rows.append([_dt(r.get("requested_at")), _ec_award_status_label(r.get("status")).replace("EC", ""), r.get("action_type"), _short(detail, 180), r.get("actor_name") or r.get("actor_id") or "—", _raw(" ".join(actions) or "—")])

    msg_panel = f"<section class='panel'><p>{_e(msg)}</p></section>" if msg else ""
    role_hint = "Dashboard-Adminrollen brauchen zusätzlich einen frischen Snapshot und erneuten Login. Falls eure aktuelle Snapshot-Logik diese Rolle nicht exportiert, bleibt Railway-ENV weiterhin maßgeblich."
    body = f"""
    <nav class="topnav"><a href="/admin">← Admin</a><a href="/settings">Setup</a><a href="/ec">EC-Verlauf</a><a href="/api/admin-settings">API</a></nav>
    <section class="hero">
      <div><div class="eyebrow">Admin · Änderbar über Bot-Queue</div><h1>⚙️ Admin-Einstellungen</h1><p class="muted">Das Dashboard legt nur Änderungsanträge an. Der Bot übernimmt sie und schreibt das Ergebnis zurück.</p></div>
      <a class="btn" href="/admin">Admin-Zentrale</a>
    </section>
    {msg_panel}
    <section class="grid">{cards}</section>

    <section class="panel"><h2>🪙 EC-Werte pro Eventtyp</h2><p class="muted">Änderungen gelten erst, wenn der Bot den Antrag verarbeitet hat.</p>{_table(['Eventtyp','Aktuell','Ändern'], point_rows, placeholder='Eventtyp suchen…')}</section>

    <section class="split">
      <section class="panel"><h2>📅 Wochenlimit</h2><p class="muted">Aktuell: <strong>{_e(cfg.get('weekly_event_limit'))} EC</strong> aus Event-Buchungen pro Woche.</p>
        <form method="post" action="/admin/settings-change" style="display:grid; gap:10px; max-width:520px;">
          <input type="hidden" name="action_type" value="set_weekly_limit">
          <label>Neues Wochenlimit<br><input name="weekly_event_limit" value="{_e(cfg.get('weekly_event_limit'))}" inputmode="numeric"></label>
          <button class="btn" type="submit" onclick="return confirm('Wochenlimit als Bot-Antrag senden?')">Wochenlimit ändern</button>
        </form>
      </section>
      <section class="panel"><h2>📉 Wöchentlicher Verfall</h2><p class="muted">Aktuell: <strong>{_e(cfg.get('decay_percent'))}%</strong> nur über <strong>{_e(cfg.get('decay_protected_balance'))} EC</strong>.</p>
        <form method="post" action="/admin/settings-change" style="display:grid; gap:10px; max-width:520px;">
          <input type="hidden" name="action_type" value="set_decay">
          <label>Verfall in %<br><input name="decay_percent" value="{_e(cfg.get('decay_percent'))}" inputmode="decimal"></label>
          <label>Schutzbetrag<br><input name="decay_protected_balance" value="{_e(cfg.get('decay_protected_balance'))}" inputmode="numeric"></label>
          <button class="btn" type="submit" onclick="return confirm('Verfall-Regel als Bot-Antrag senden?')">Verfall ändern</button>
        </form>
      </section>
    </section>

    <section class="split">
      <section class="panel"><h2>🛡️ Rollen & Kanäle</h2><p class="muted">Leitungsrolle, Gildenrolle und EC-/Loot-Logkanal zentral als Bot-Antrag setzen.</p>
        <form method="post" action="/admin/settings-change" style="display:grid; gap:10px; max-width:620px;">
          <input type="hidden" name="action_type" value="set_roles">
          <label>Leitungsrolle / Adminrolle ID<br><input name="leader_role_id" value="{_e(cfg.get('leader_role_id'))}" placeholder="z. B. 123456789012345678"></label>
          <label>Gildenrolle / Memberrolle ID<br><input name="member_role_id" value="{_e(cfg.get('member_role_id'))}" placeholder="z. B. 123456789012345678"></label>
          <label>EC-/Loot-Logkanal ID<br><input name="log_channel_id" value="{_e(cfg.get('log_channel_id'))}" placeholder="z. B. 123456789012345678"></label>
          <button class="btn" type="submit" onclick="return confirm('Rollen/Kanal als Bot-Antrag senden?')">Rollen & Kanal ändern</button>
        </form>
      </section>
      <section class="panel"><h2>🔐 Dashboard-Zugriff</h2><p class="muted">{_e(role_hint)}</p>
        <form method="post" action="/admin/settings-change" style="display:grid; gap:10px; max-width:620px;">
          <input type="hidden" name="action_type" value="set_access_roles">
          <label>Dashboard-Adminrollen IDs, kommagetrennt<br><input name="dashboard_admin_role_ids" value="{_e(cfg.get('dashboard_admin_role_ids'))}" placeholder="ID,ID,ID"></label>
          <label>Dashboard-Zugriffsrollen IDs, kommagetrennt<br><input name="dashboard_allowed_role_ids" value="{_e(cfg.get('dashboard_allowed_role_ids'))}" placeholder="ID,ID,ID"></label>
          <button class="btn" type="submit" onclick="return confirm('Dashboard-Zugriffsrollen als Bot-Antrag senden?')">Zugriffsrollen speichern</button>
        </form>
      </section>
    </section>

    <section class="panel"><h2>🧾 Änderungsqueue</h2><p class="muted">Offene Anträge können abgebrochen werden. Fehlgeschlagene/blockierte/abgebrochene Anträge können neu geöffnet werden.</p>{_table(['Zeit','Status','Aktion','Details','Akteur','Aktion'], req_rows, placeholder='Änderungen durchsuchen…')}</section>
    """
    return _html_shell("Admin-Einstellungen · Ebo Dashboard", body)

def _dashboard_event_action_requests(guild_id: int, limit: int = 80, event_id: str = "") -> list[dict[str, Any]]:
    if not _database_url() or not guild_id:
        return []
    try:
        _ensure_admin_tables()
        conn = _pg_connect()
        try:
            with conn.cursor() as cur:
                if event_id:
                    cur.execute(
                        """
                        SELECT id, request_id, guild_id, event_id, action_type, status, payload_json, actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                        FROM dashboard_event_action_requests
                        WHERE guild_id = %s AND event_id = %s
                        ORDER BY requested_at DESC
                        LIMIT %s
                        """,
                        (int(guild_id), str(event_id), int(limit)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, request_id, guild_id, event_id, action_type, status, payload_json, actor_id, actor_name, requested_at, claimed_at, processed_at, result_json
                        FROM dashboard_event_action_requests
                        WHERE guild_id = %s
                        ORDER BY requested_at DESC
                        LIMIT %s
                        """,
                        (int(guild_id), int(limit)),
                    )
                return [dict(r) for r in (cur.fetchall() or [])]
        finally:
            conn.close()
    except Exception:
        return []


def _event_action_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    c: dict[str, int] = {}
    for r in rows or []:
        s = str((r or {}).get("status") or "unknown").lower()
        c[s] = c.get(s, 0) + 1
    return c


def _enqueue_event_action_request(guild_id: int, action_type: str, payload: dict[str, Any], actor: dict[str, Any]) -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt. Ohne Postgres kann das Dashboard keine Event-Aktion an den Bot übergeben."}
    if not guild_id:
        return {"ok": False, "error": "Guild-ID fehlt."}
    action = str(action_type or "").strip().lower()
    if action not in {"create", "edit", "delete"}:
        return {"ok": False, "error": "Unbekannte Event-Aktion."}
    actor_id = str(actor.get("user_id") or "").strip()
    actor_name = str(actor.get("username") or actor_id or "Dashboard")
    event_id = str(payload.get("event_id") or "").strip()
    if action in {"edit", "delete"} and not event_id:
        return {"ok": False, "error": "Event-ID fehlt."}
    if action == "create" and not str(payload.get("title") or "").strip():
        return {"ok": False, "error": "Titel fehlt."}
    if action == "create" and not str(payload.get("channel_id") or "").strip():
        return {"ok": False, "error": "Zielkanal-ID fehlt."}
    request_id = f"dash-event-{int(time.time())}-{secrets.token_hex(6)}"
    payload = dict(payload)
    payload.update({
        "action_type": action,
        "requested_by": {"id": actor_id, "name": actor_name},
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "source": "dashboard_event_action",
    })
    _ensure_admin_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboard_event_action_requests
                    (request_id, guild_id, event_id, action_type, status, payload_json, actor_id, actor_name, requested_at)
                VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s, NOW())
                RETURNING id, request_id, status
                """,
                (request_id, int(guild_id), event_id, action, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), actor_id, actor_name),
            )
            row = dict(cur.fetchone() or {})
            cur.execute(
                """
                INSERT INTO dashboard_admin_action_log
                    (guild_id, action_type, target_type, target_id, actor_id, actor_name, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (int(guild_id), f"event_action_{action}_create", "event", event_id or "new", actor_id, actor_name, json.dumps(payload, ensure_ascii=False)),
            )
        conn.commit()
        return {"ok": True, "request": row, "request_id": request_id}
    finally:
        conn.close()


def _event_status_text(ev: dict[str, Any]) -> str:
    if ev.get("_attendance_review_only"):
        return "Review offen"
    if ev.get("_pending_ec_check"):
        return "EC offen"
    if _is_running_event(ev):
        return "läuft/offen"
    return str(ev.get("status") or ev.get("state") or "geplant")


def _event_role_counts(ev: dict[str, Any]) -> str:
    yes = ev.get("yes") if isinstance(ev.get("yes"), dict) else {}
    if yes:
        return " · ".join(f"{k}: {len(v) if isinstance(v, list) else 0}" for k, v in yes.items()) or "—"
    return f"Teilnehmer: {ev.get('participant_count', ev.get('participants', '—'))}"


def _render_events_center(data: dict[str, Any], current_user: Optional[dict[str, Any]] = None, msg: str = "") -> str:
    if not data.get("ok"):
        return _html_shell("Events · Ebo Dashboard", f"<section class='panel'><h1>📅 Events</h1><p class='muted'>{_e(data.get('error'))}</p></section>")

    snap: dict[str, Any] = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    now = datetime.now(timezone.utc)

    raw_events = [dict(e) for e in ((snap.get("events") or {}).get("items") or []) if isinstance(e, dict)]
    by_id: dict[str, dict[str, Any]] = {}
    for ev in raw_events:
        eid = str(ev.get("event_id") or ev.get("id") or "").strip()
        if eid:
            by_id[eid] = ev

    # Events mit offenem DKP-/EC-Check und gespeicherte Reviews werden bewusst ergänzt.
    for ev in _events_with_pending_ec_checks(snap):
        eid = str(ev.get("event_id") or ev.get("id") or "").strip()
        if eid and eid not in by_id:
            by_id[eid] = dict(ev)
        elif eid:
            by_id[eid].update({k: v for k, v in ev.items() if str(k).startswith("_")})
    if guild_id:
        for ev in _open_attendance_review_events_for_homepage(snap, guild_id, limit=80):
            eid = str(ev.get("event_id") or ev.get("id") or "").strip()
            if eid and eid not in by_id:
                by_id[eid] = dict(ev)

    events = list(by_id.values())

    def _when(ev: dict[str, Any]) -> Optional[datetime]:
        return _dt_obj(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at"))

    def _sort_key_future(ev: dict[str, Any]):
        return _when(ev) or datetime.max.replace(tzinfo=timezone.utc)

    def _sort_key_past(ev: dict[str, Any]):
        dt = _when(ev) or datetime.min.replace(tzinfo=timezone.utc)
        return -dt.timestamp()

    running: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    past: list[dict[str, Any]] = []
    for ev in events:
        dt = _when(ev)
        if _is_running_event(ev) or ev.get("_pending_ec_check") or ev.get("_attendance_review_only"):
            running.append(ev)
        elif dt and dt >= now:
            upcoming.append(ev)
        else:
            past.append(ev)

    running.sort(key=_sort_key_future)
    upcoming.sort(key=_sort_key_future)
    past.sort(key=_sort_key_past)

    action_rows = _dashboard_event_action_requests(guild_id, limit=50) if guild_id else []
    ac = _event_action_counts(action_rows)

    def _event_buttons(eid: str) -> str:
        if not eid:
            return "—"
        return (
            f"<div class='actions-inline'>"
            f"<a class='link' href='/event/{_e(eid)}'>Details</a>"
            f"<a class='link' href='/attendance/{_e(eid)}'>Review</a>"
            f"<a class='link' href='/attendance/{_e(eid)}/report'>Bericht</a>"
            f"<a class='link' href='/attendance/{_e(eid)}/ec-preview'>EC</a>"
            f"</div>"
        )

    def _role_bar(ev: dict[str, Any]) -> str:
        summary = _event_role_summary(ev)
        parts = []
        for label, icon in (("Tank", "🛡️"), ("Heiler", "✚"), ("DPS", "⚔️"), ("Reserve", "🪑")):
            parts.append(f"<span class='pill'>{icon} {_e(label)}: {_e(summary.get(label, 0))}</span>")
        maybe = int(_num(ev.get("maybe_count"), 0))
        no = int(_num(ev.get("no_count"), 0))
        if maybe:
            parts.append(f"<span class='pill'>Vielleicht: {_e(maybe)}</span>")
        if no:
            parts.append(f"<span class='pill muted'>Abgemeldet: {_e(no)}</span>")
        return " ".join(parts)

    def _participant_preview(ev: dict[str, Any]) -> str:
        parts = ev.get("participants") or {}
        names: list[str] = []
        yes = parts.get("yes") or []
        if isinstance(yes, list):
            for group in yes:
                if not isinstance(group, dict):
                    continue
                for p in (group.get("participants") or [])[:3]:
                    if isinstance(p, dict):
                        names.append(str(p.get("display_name") or p.get("name") or p.get("user_id") or ""))
                    elif p:
                        names.append(str(p))
                    if len(names) >= 6:
                        break
                if len(names) >= 6:
                    break
        if not names:
            return "<span class='muted'>Noch keine Zusagen im Snapshot.</span>"
        more = int(_num(ev.get("participant_count"), 0)) - len(names)
        suffix = f" <span class='muted'>+{_e(more)} weitere</span>" if more > 0 else ""
        return ", ".join(_e(n) for n in names if n) + suffix

    def _event_card(ev: dict[str, Any], label: str) -> str:
        eid = str(ev.get("event_id") or ev.get("id") or "")
        title = str(ev.get("title") or ev.get("name") or eid or "Event")
        status = _event_status_text(ev)
        status_class = "ok" if label == "kommend" else "warn" if label == "offen" else "muted"
        queue_badge = _event_ec_queue_badge(guild_id, eid) if guild_id and eid else {"label": "—", "class": "muted"}
        return f"""
        <article class="event-card">
          <div class="event-card-head">
            <div>
              <div class="eyebrow">{_e(label.upper())}</div>
              <h3><a class="link" href="/event/{_e(eid)}">{_e(title)}</a></h3>
              <p class="muted">{_dt(ev.get('when_iso') or ev.get('start_at') or ev.get('created_at'))}</p>
            </div>
            <div style="text-align:right"><span class="pill {status_class}">{_e(status)}</span><br><span class="pill {_e(queue_badge.get('class'))}">{_e(queue_badge.get('label'))}</span></div>
          </div>
          <div class="role-strip">{_role_bar(ev)}</div>
          <p class="muted"><strong>Teilnehmer:</strong> {_participant_preview(ev)}</p>
          {_raw(_event_buttons(eid)).get('__html__')}
        </article>
        """

    def _section(title: str, items: list[dict[str, Any]], label: str, empty: str) -> str:
        cards_html = "".join(_event_card(ev, label) for ev in items[:30])
        if not cards_html:
            cards_html = f"<div class='empty'>{_e(empty)}</div>"
        return f"<section class='panel'><h2>{_e(title)}</h2><div class='event-card-grid'>{cards_html}</div></section>"

    action_table_rows = []
    for r in action_rows[:40]:
        result = ""
        try:
            result_obj = json.loads(str(r.get("result_json") or "{}"))
            result = result_obj.get("message") or result_obj.get("error") or ""
        except Exception:
            result = str(r.get("result_json") or "")[:160]
        action_table_rows.append([
            r.get("requested_at"),
            r.get("action_type"),
            r.get("event_id") or "neu",
            r.get("status"),
            r.get("actor_name"),
            result,
        ])

    event_cut = data.get("phase3_events_read_cutover") or {}
    event_source_label = "Postgres Phase 3" if event_cut.get("active") else "Snapshot/Fallback"
    cards = "".join([
        _card("Event-Quelle", event_source_label, "Read-Cutover aktiv" if event_cut.get("active") else "Fallback aktiv"),
        _card("Laufend/offen", len(running), "inkl. offener Review/EC"),
        _card("Kommend", len(upcoming), "geplante Events"),
        _card("Vergangen", len(past), "letzte Events im Snapshot"),
        _card("Queue offen", ac.get("pending", 0) + ac.get("processing", 0), f"erledigt: {ac.get('done', 0)}"),
    ])

    event_table_rows = []
    for ev in (running + upcoming + past)[:120]:
        eid = str(ev.get("event_id") or ev.get("id") or "")
        event_table_rows.append([
            _raw(f"<a class='link' href='/event/{_e(eid)}'>{_e(ev.get('title') or ev.get('name') or eid)}</a>"),
            _dt(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at")),
            _event_status_text(ev),
            _raw(_role_bar(ev)),
            ev.get("participant_count", ev.get("participants", "—")),
            _raw(_event_buttons(eid)),
        ])

    msg_panel = f"<section class='panel'><p>{_e(msg)}</p></section>" if msg else ""
    body = f"""
    <style>
      .event-card-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:14px; }}
      .event-card {{ border:1px solid rgba(255,255,255,.10); border-radius:18px; padding:16px; background:rgba(16,12,10,.55); box-shadow:0 12px 34px rgba(0,0,0,.22); }}
      .event-card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
      .event-card h3 {{ margin:.15rem 0 .1rem; }}
      .role-strip {{ display:flex; flex-wrap:wrap; gap:6px; margin:12px 0; }}
      .actions-inline {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:12px; }}
      .actions-inline .link {{ border:1px solid rgba(255,255,255,.12); border-radius:999px; padding:7px 10px; background:rgba(255,255,255,.04); text-decoration:none; }}
      .actions-inline .link:hover {{ background:rgba(234,179,8,.12); }}
      .event-form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; }}
      .event-form-grid label {{ display:block; }}
      .event-form-grid input, .event-form-grid textarea, .event-form-grid select {{ width:100%; }}
      @media(max-width:720px) {{ .event-card-head {{ flex-direction:column; }} .actions-inline .link {{ flex:1 1 auto; text-align:center; }} }}
    </style>
    <nav class="topnav"><a href="/">← Kommando</a><a href="/attendance">Anwesenheit</a><a href="/ec">EC</a><a href="/overview">Gesamtübersicht</a><a href="/api/events-center">API</a><a href="/export/events_center.csv">CSV</a></nav>
    <section class="hero">
      <div><div class="eyebrow">Event-Zentrale</div><h1>📅 Events & Planung</h1><p class="muted">Kommende Events, laufende Events, Eventstatus, Rollenverteilung, Teilnehmerübersicht und direkte Links zu Attendance, Review und EC.</p></div>
      <div class="hero-actions"><a class="hero-action attendance" href="#create"><span>➕</span><strong>Event erstellen</strong><small>Bot-Queue</small></a><a class="hero-action loot" href="#overview"><span>📋</span><strong>Events prüfen</strong><small>Status & Rollen</small></a><a class="hero-action members" href="#actions"><span>🧾</span><strong>Queue</strong><small>Erstellen/Bearbeiten/Löschen</small></a></div>
    </section>
    {msg_panel}
    <section class="grid">{cards}</section>
    <div id="overview"></div>
    {_section('🔥 Laufende / offene Events', running, 'offen', 'Keine laufenden oder offenen Events.')}
    {_section('📆 Kommende Events', upcoming, 'kommend', 'Keine kommenden Events im Snapshot.')}
    {_section('🕰️ Vergangene / abgeschlossene Events', past[:20], 'vergangen', 'Keine alten Events im Snapshot.')}
    <section class="panel"><h2>📋 Komplette Eventliste</h2><p class="muted">Suchbar. Enthält Snapshot-Events plus offene Review-/EC-Fallbacks.</p>{_table(['Event','Zeit','Status','Rollen','Teilnehmer','Links'], event_table_rows, placeholder='Event suchen…')}</section>
    <section class="panel" id="create"><h2>➕ Event erstellen</h2><p class="muted">Das Dashboard schreibt nicht direkt in Discord. Es legt einen Antrag an, den der Bot verarbeitet.</p>
      <form method="post" action="/admin/events/action" style="display:grid; gap:12px;">
        <input type="hidden" name="action_type" value="create">
        <div class="event-form-grid">
          <label>Titel<br><input name="title" required placeholder="z. B. Gildenbosse Sonntag"></label>
          <label>Datum<br><input name="date" type="date" required></label>
          <label>Uhrzeit<br><input name="time" type="time" required></label>
          <label>Eventtyp<br><input name="event_type" placeholder="gildenbosse / raid / pvp / hm"></label>
          <label>Zielkanal-ID<br><input name="channel_id" required placeholder="Discord Channel ID"></label>
          <label>Zielrollen-ID optional<br><input name="target_role_id" placeholder="Discord Role ID"></label>
        </div>
        <label>Beschreibung<br><textarea name="description" rows="4" placeholder="Kurzbeschreibung"></textarea></label>
        <div class="event-form-grid">
          <label>Bild-URL optional<br><input name="image_url" placeholder="https://..."></label>
          <label style="display:flex; gap:8px; align-items:center; padding-top:22px;"><input type="checkbox" name="send_dms" value="1" checked> DMs an Zielgruppe senden</label>
        </div>
        <button class="btn" type="submit" onclick="return confirm('Event-Erstellung als Bot-Antrag senden?')">Event-Erstellung an Bot senden</button>
      </form>
    </section>
    <section class="split">
      <section class="panel"><h2>✏️ Event bearbeiten</h2><p class="muted">Nur ausgefüllte Felder werden geändert. Event-ID ist normalerweise die Discord-Message-ID des Eventposts.</p>
        <form method="post" action="/admin/events/action" style="display:grid; gap:10px;">
          <input type="hidden" name="action_type" value="edit">
          <label>Event-ID<br><input name="event_id" required placeholder="Message-ID"></label>
          <label>Neuer Titel optional<br><input name="title"></label>
          <div class="event-form-grid"><label>Neues Datum optional<br><input name="date" type="date"></label><label>Neue Uhrzeit optional<br><input name="time" type="time"></label></div>
          <label>Neue Beschreibung optional<br><textarea name="description" rows="3"></textarea></label>
          <label>Neue Bild-URL optional<br><input name="image_url"></label>
          <button class="btn" type="submit" onclick="return confirm('Änderung als Bot-Antrag senden?')">Bearbeitung an Bot senden</button>
        </form>
      </section>
      <section class="panel"><h2>🗑️ Event löschen</h2><p class="muted">Löscht nicht blind im Dashboard, sondern sendet einen sicheren Antrag an den Bot.</p>
        <form method="post" action="/admin/events/action" style="display:grid; gap:10px;">
          <input type="hidden" name="action_type" value="delete">
          <label>Event-ID<br><input name="event_id" required placeholder="Message-ID"></label>
          <label>Zur Sicherheit LÖSCHEN schreiben<br><input name="confirm" required placeholder="LÖSCHEN"></label>
          <button class="btn danger" type="submit" onclick="return confirm('Event wirklich löschen? Der Bot entfernt den Eventpost.')">Löschen an Bot senden</button>
        </form>
      </section>
    </section>
    <section class="panel" id="actions"><h2>🧾 Event-Aktionsqueue</h2><p class="muted">Zeigt Erstellen/Bearbeiten/Löschen aus dem Dashboard und den Bot-Status.</p>{_table(['Zeit','Aktion','Event','Status','Von','Ergebnis'], action_table_rows, placeholder='Queue durchsuchen…')}</section>
    """
    return _html_shell("Events · Ebo Dashboard", body)


@app.get("/events", response_class=HTMLResponse)
def events_page(request: Request, _: bool = Depends(_auth), msg: str = ""):
    try:
        return HTMLResponse(_render_events_center(_snapshot_payload(), _current_user(request), msg))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/events-center")
def api_events_center(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    now = datetime.now(timezone.utc)
    events = [dict(e) for e in ((snap.get("events") or {}).get("items") or []) if isinstance(e, dict)]
    running: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    past: list[dict[str, Any]] = []
    for ev in events:
        dt = _dt_obj(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at"))
        if _is_running_event(ev):
            running.append(ev)
        elif dt and dt >= now:
            upcoming.append(ev)
        else:
            past.append(ev)
    return JSONResponse({
        "ok": True,
        "counts": {"running": len(running), "upcoming": len(upcoming), "past": len(past), "total": len(events)},
        "running": running,
        "upcoming": upcoming,
        "past": past,
        "events": events,
        "actions": _dashboard_event_action_requests(guild_id, limit=80) if guild_id else [],
        "phase3_events_read_cutover": payload.get("phase3_events_read_cutover") or {},
    })


@app.get("/export/events_center.csv")
def export_events_center_csv(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return _csv_response("events_center.csv", ["error"], [[payload.get("error")]])
    snap = payload.get("snapshot") or {}
    rows: list[list[Any]] = []
    for ev in ((snap.get("events") or {}).get("items") or []):
        if not isinstance(ev, dict):
            continue
        eid = str(ev.get("event_id") or ev.get("id") or "")
        rows.append([
            eid,
            ev.get("title") or ev.get("name") or "",
            _dt(ev.get("when_iso") or ev.get("start_at") or ev.get("created_at")),
            _event_status_text(ev),
            _event_role_counts(ev),
            ev.get("participant_count", ""),
            ev.get("maybe_count", ""),
            ev.get("no_count", ""),
        ])
    return _csv_response("events_center.csv", ["event_id", "title", "time", "status", "roles", "participants", "maybe", "no"], rows)



@app.post("/admin/events/action")
async def admin_events_action(request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    actor = _current_user(request) or {"username": "Dashboard"}
    form = _parse_urlencoded_body(await request.body())
    action = str(form.get("action_type") or "").strip().lower()
    if action == "delete" and str(form.get("confirm") or "").strip().upper() != "LÖSCHEN":
        return RedirectResponse("/events?msg=" + urllib.parse.quote("Löschen abgebrochen: Sicherheitswort fehlt."), status_code=303)
    clean_payload = {
        "event_id": str(form.get("event_id") or "").strip(),
        "title": str(form.get("title") or "").strip(),
        "date": str(form.get("date") or "").strip(),
        "time": str(form.get("time") or "").strip(),
        "event_type": str(form.get("event_type") or "").strip(),
        "channel_id": str(form.get("channel_id") or "").strip(),
        "target_role_id": str(form.get("target_role_id") or "").strip(),
        "description": str(form.get("description") or "").strip(),
        "image_url": str(form.get("image_url") or "").strip(),
        "send_dms": str(form.get("send_dms") or "") == "1",
    }
    res = _enqueue_event_action_request(guild_id, action, clean_payload, actor)
    msg = "Event-Aktion wurde an den Bot gesendet." if res.get("ok") else f"Fehler: {res.get('error')}"
    return RedirectResponse("/events?msg=" + urllib.parse.quote(msg), status_code=303)


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
    end_dt = _dt_obj(auction.get("ends_at") or auction.get("end_at") or auction.get("expires_at"))
    if end_dt and datetime.now(timezone.utc) >= end_dt:
        return False
    if status in active_statuses:
        return True

    # Nur wenn kein aussagekräftiger Status gesetzt ist, darf die Phase helfen.
    if not status:
        active_phases = {"need", "free", "sale", "roll", "main", "secondary", "müll", "muell", "junk"}
        return phase in active_phases

    return False


def _is_running_event(event: dict[str, Any]) -> bool:
    """Laufend = erstellt, nicht manuell geschlossen und noch nicht 1h nach Start.

    Jonas-Definition:
    - Sobald ein Event erstellt ist, zählt es als laufend/aktiv.
    - Automatisch abgeschlossen ist es 60 Minuten nach Eventbeginn.
    - Manuell gelöschte/abgebrochene/geschlossene Events bleiben draußen.
    """
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

    start = _dt_obj(
        event.get("when_iso")
        or event.get("start_at")
        or event.get("start_time")
        or event.get("starts_at")
        or event.get("datetime")
        or event.get("date")
    )

    # Ohne lesbare Startzeit lieber anzeigen statt verstecken. Sonst verschwinden
    # manuell erstellte Alt-Events nur wegen alter Datenstruktur.
    if not start:
        return True

    now = datetime.now(timezone.utc)
    return now < (start + timedelta(hours=1))


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
        _card("Laufende Events", len(running_events), "bis 1h nach Start"),
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

    # Kompakte Startseite im Guild-Manager-Stil: Hauptnavigation links, Startseite nur mit aktuellen To-dos.
    def _home_item(icon: str, title: Any, meta: Any, href: str = "", badge: str = "") -> str:
        title_html = f"<a class='link' href='{_e(href)}'>{_e(title)}</a>" if href else _e(title)
        badge_html = f"<span class='pill'>{_e(badge)}</span>" if badge else ""
        return f"<div class='home-item'><div class='home-icon'>{_e(icon)}</div><div><div class='home-title'>{title_html}</div><div class='home-meta'>{_cell(meta)}</div></div>{badge_html}</div>"

    home_events_html = "".join(
        _home_item(
            "📅",
            ev.get("title") or ev.get("name") or ev.get("event_id") or "Event",
            f"{_dt(ev.get('when_iso') or ev.get('start_at') or ev.get('created_at'))} · Teilnehmer: {ev.get('participant_count', ev.get('participants', '—'))} · {'EC offen' if ev.get('_pending_ec_check') else ('Review offen' if ev.get('_attendance_review_only') else 'EC: —')}",
            f"/attendance/{_e(str(ev.get('event_id') or ev.get('id') or ''))}" if ev.get("_attendance_review_only") else f"/event/{_e(str(ev.get('event_id') or ev.get('id') or ''))}",
            "Review offen" if ev.get("_attendance_review_only") else "läuft",
        )
        for ev in running_events[:6]
        if isinstance(ev, dict)
    ) or '<div class="empty">Keine laufenden Events oder offenen Reviews.</div>'

    home_auctions_html = "".join(
        _home_item(
            "🏆",
            a.get("item_name") or a.get("title") or a.get("auction_id") or "Auktion",
            f"{a.get('status') or '—'} · führend: {_auction_leader_text(a, names)} · Ende: {_dt(a.get('ends_at') or a.get('end_at') or a.get('expires_at'))}",
            f"/auction/{_e(str(a.get('auction_id') or ''))}",
            str(a.get("phase") or "Auktion"),
        )
        for a in active_auctions[:5]
        if isinstance(a, dict)
    ) or '<div class="empty">Keine aktiven Auktionen.</div>'

    try:
        member_payload = _member_center_payload(data)
        member_rows_home = member_payload.get("rows") or []
    except Exception:
        member_rows_home = []
    top_members = sorted(
        [r for r in member_rows_home if isinstance(r, dict)],
        key=lambda r: (int(r.get("attendance_present") or 0) + int(r.get("won_count") or 0), _num(r.get("voice_hours"), 0), _num(r.get("ec_balance"), 0)),
        reverse=True,
    )[:5]
    top_members_html = "".join(
        _home_item(
            "👤",
            r.get("display_name") or f"User {r.get('user_id')}",
            f"Quote: {r.get('attendance_rate') or '—'} · Voice: {_num(r.get('voice_hours'), 0):.1f} h · EC: {_fmt_ec(r.get('ec_balance')) if r.get('ec_balance') is not None else '—'}",
            f"/member/{_e(str(r.get('user_id') or ''))}",
            f"{r.get('won_count', 0)} Loot",
        )
        for r in top_members
    ) or '<div class="empty">Noch keine Top-Mitglieder-Daten.</div>'

    activity_bits: list[str] = []
    for t in tasks[:4]:
        if isinstance(t, dict):
            activity_bits.append(_home_item("✅", t.get("task") or "Aufgabe", f"{t.get('area') or 'Bereich'} · {t.get('detail') or ''}", str(t.get("link") or "/"), str(t.get("prio") or "")))
    for q in queue_rows[:3]:
        if isinstance(q, dict):
            activity_bits.append(_home_item("🪙", q.get("event_id") or q.get("request_id") or "EC-Anfrage", f"Status: {_ec_award_status_label(q.get('status'))} · {_dt(q.get('requested_at'))}", "/ec-queue", "EC"))
    home_activity_html = "".join(activity_bits[:6]) or '<div class="empty">Keine aktuellen Aufgaben oder Queue-Aktivitäten.</div>'

    cards = "".join([
        _card("Mitglieder", li.get("member_count", 0), role_line),
        _card("Laufende Events", len(running_events), "bis 1h nach Start"),
        _card("Aktive Auktionen", len(active_auctions), "Bieten möglich"),
        _card("EC-Queue", queue_open, f"offen/verarbeitend · erledigt: {queue_counts.get('done', 0)}"),
    ])

    quick_action_html = """
      <div class="action-list">
        <a class="btn" href="/attendance">✅ Anwesenheit prüfen</a>
        <a class="btn" href="/loot">🏆 Auktionen öffnen</a>
        <a class="btn" href="/loot-check">🔎 Item prüfen</a>
        <a class="btn" href="/members">👥 Mitglieder ansehen</a>
        <a class="btn" href="/admin">🛡️ Admin-Zentrale</a>
      </div>
    """

    body = f"""
    <section class="hero">
      <div>
        <div class="eyebrow">Kommando · kompakt</div>
        <h1>{_e(guild.get('name') or data.get('guild_name') or 'Gilde')}</h1>
        <p>Startseite zeigt nur das, was jetzt gerade relevant ist. Alles andere ist links sauber gruppiert.</p>
        <p class="muted">{_e(role_line)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <div class="hero-actions">
        <a class="hero-action attendance" href="/attendance"><span>✅</span><strong>Anwesenheit</strong><small>Reviews, Voice & EC</small></a>
        <a class="hero-action loot" href="/loot"><span>🏆</span><strong>Loot</strong><small>Auktionen & Gebote</small></a>
        <a class="hero-action members" href="/members"><span>👥</span><strong>Mitglieder</strong><small>Roster & Aktivität</small></a>
      </div>
    </section>

    <section class="grid">{cards}</section>

    <section class="home-layout">
      <div class="home-stack">
        <section class="panel">
          <h2>📅 Laufende Events</h2>
          <p class="muted">Sichtbar ab Erstellung bis 1 Stunde nach Eventbeginn. Offene Reviews bleiben zusätzlich sichtbar.</p>
          <div class="home-list">{home_events_html}</div>
        </section>

        <section class="panel">
          <h2>🏆 Aktive Auktionen</h2>
          <p class="muted">Direkt öffnen, bieten und Status prüfen.</p>
          <div class="home-list">{home_auctions_html}</div>
        </section>

        <section class="panel">
          <h2>✅ Offene Aufgaben</h2>
          <p class="muted">Priorisierte Leitungs-Hinweise. Es wird nichts automatisch geändert.</p>
          {_table(['Priorität','Bereich','Aufgabe','Details'], task_rows, placeholder='Aufgaben durchsuchen…')}
        </section>
      </div>

      <aside class="home-stack">
        <section class="panel">
          <h2>⚡ Schnellaktionen</h2>
          <p class="muted">Die häufigsten Leitungswege ohne lange Menüleiste.</p>
          {quick_action_html}
        </section>

        <section class="panel">
          <h2>👑 Top-Mitglieder</h2>
          <p class="muted">Aus Attendance, Voice, Loot und EC zusammengeführt.</p>
          <div class="home-list">{top_members_html}</div>
        </section>

        <section class="panel">
          <h2>🧾 Aktuelle Aktivität</h2>
          <p class="muted">Aufgaben und EC-Queue in Kurzform.</p>
          <div class="home-list">{home_activity_html}</div>
        </section>
      </aside>
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
        target = str(state_data.get("next") or "/")
        # Normale Mitglieder landen nach dem Login automatisch im reduzierten Mitgliederbereich.
        # Admins behalten den vollen Dashboard-Einstieg.
        if not is_admin and target in {"", "/", "/overview", "/admin", "/settings", "/system", "/audit", "/ec", "/ec-queue", "/attendance", "/analytics", "/voice", "/exports"}:
            target = "/member"
        resp = RedirectResponse(target, status_code=303)
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
    return {
        "ok": True,
        "database_url": bool(_database_url()),
        "version": DASHBOARD_RELEASE_VERSION,
    }


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
    return JSONResponse({"ok": True, "ec": (snap.get("ec") or {}), "phase3_ec_read_cutover": payload.get("phase3_ec_read_cutover") or {}})


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
        actor = _current_user(request) or {}
        if not auction:
            msg = "❌ Auktion nicht im Snapshot gefunden."
        else:
            errors = _loot_bid_precheck(int(guild_id), auction, int(amount), actor, snap)
            if errors:
                msg = "❌ " + " ".join(str(e) for e in errors[:3])
            else:
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
        actor = _current_user(request) or {}
        if not auction:
            msg = "❌ Auktion nicht im Snapshot gefunden."
        else:
            price = int(_num(auction.get("fixed_price") if auction.get("fixed_price") is not None else auction.get("start_bid"), 0))
            errors = _loot_sale_precheck(int(guild_id), auction, actor, snap)
            if errors:
                msg = "❌ " + " ".join(str(e) for e in errors[:3])
            else:
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



@app.get("/member", response_class=HTMLResponse)
def member_home(request: Request, _: bool = Depends(_auth)):
    try:
        user = _current_user(request) or {}
        # Admins können die Mitgliederansicht bewusst öffnen, bleiben sonst im vollen Dashboard.
        return HTMLResponse(_render_member_home(_snapshot_payload(), request))
    except Exception as exc:
        return HTMLResponse(_html_shell("Mitgliederbereich Fehler", f"<section class='panel'><h1>❌ Mitgliederbereich-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>", nav_mode="member"), status_code=500)


@app.get("/portal", response_class=HTMLResponse)
def portal_home(request: Request, _: bool = Depends(_auth), msg: str = ""):
    user = _current_user(request)
    uid = _current_user_id(request)
    if uid:
        return HTMLResponse(_render_member_portal(_snapshot_payload(), uid, request, msg))
    if _is_portal_admin(request):
        return RedirectResponse(url="/members", status_code=303)
    body = """
    <section class='panel'><h1>👤 Mein Portal</h1><p class='muted'>Für das persönliche Portal brauchst du Discord-Login, damit das Dashboard deine Discord-ID kennt.</p><p><a class='btn' href='/auth/discord/start'>Mit Discord einloggen</a></p></section>
    """
    return HTMLResponse(_html_shell("Mein Portal · Ebo Dashboard", body))


@app.get("/portal/member/{user_id}", response_class=HTMLResponse)
def member_portal_page(user_id: int, request: Request, _: bool = Depends(_auth), msg: str = ""):
    return HTMLResponse(_render_member_portal(_snapshot_payload(), int(user_id), request, msg))


@app.post("/portal/member/{user_id}/need-change")
async def portal_need_change(user_id: int, request: Request, _: bool = Depends(_auth)):
    if not _portal_can_view(request, int(user_id)):
        raise HTTPException(status_code=403, detail="Keine Berechtigung für diese Needliste.")
    data = _snapshot_payload()
    guild_id = _safe_guild_id(data)
    if not guild_id:
        raise HTTPException(status_code=400, detail="Guild-ID fehlt.")
    form = _parse_urlencoded_body(await request.body())
    action_type = str(form.get("action_type") or "set").strip().lower()
    if action_type not in {"set", "clear"}:
        action_type = "set"
    tab = str(form.get("tab") or "Main").strip()
    slot = str(form.get("slot") or "").strip()
    item_text = str(form.get("item_text") or "").strip()
    if tab not in {"Main", "Secondary"}:
        tab = "Main"
    valid_slots = {"Waffe 1","Waffe 2","Fähigkeitskern","Helm","Brust","Hose","Handschuhe","Schuhe","Brosche","Ohrringe","Kette","Armband","Ring 1","Ring 2","Gürtel","Umhang"}
    if slot not in valid_slots:
        raise HTTPException(status_code=400, detail="Ungültiger Slot.")
    if action_type == "set" and not item_text:
        raise HTTPException(status_code=400, detail="Itemname fehlt.")
    actor = _current_user(request) or {"user_id": "basic-admin", "username": "Basic Admin"}
    payload = {
        "target_user_id": int(user_id),
        "tab": tab,
        "slot": slot,
        "item_text": item_text,
        "source": "dashboard_portal",
        "admin_override": bool(_is_portal_admin(request) and _current_user_id(request) != int(user_id)),
    }
    rid = _create_need_change_request(guild_id, int(user_id), action_type, payload, actor)
    msg = urllib.parse.quote(f"Need-Änderung angelegt: {rid}")
    return RedirectResponse(url=f"/portal/member/{int(user_id)}?msg={msg}#need-editor", status_code=303)


@app.get("/api/portal/member/{user_id}")
def api_member_portal(user_id: int, request: Request, _: bool = Depends(_auth)):
    if not _portal_can_view(request, int(user_id)):
        raise HTTPException(status_code=403, detail="Keine Berechtigung.")
    data = _snapshot_payload()
    snap = data.get("snapshot") or {}
    guild_id = _safe_guild_id(data)
    return {
        "ok": bool(data.get("ok")),
        "user_id": int(user_id),
        "name": _profile_name_map(snap).get(int(user_id), f"User {int(user_id)}"),
        "ec": _balance_map(snap).get(int(user_id)),
        "needs": _needs_by_user(snap).get(int(user_id), {}),
        "events": _member_event_rows(snap, int(user_id)),
        "loot": _loot_member_payload_from_snapshot(snap, int(user_id), int(guild_id or 0)),
        "need_change_requests": _need_change_requests(guild_id, user_id=int(user_id), limit=80) if guild_id else [],
    }


@app.get("/api/need-change-requests")
def api_need_change_requests(_: bool = Depends(_admin_auth)):
    data = _snapshot_payload()
    guild_id = _safe_guild_id(data)
    return {"ok": True, "items": _need_change_requests(guild_id, limit=150) if guild_id else []}


@app.get("/member/{user_id}/loot", response_class=HTMLResponse)
def member_loot_page(user_id: int, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_member_loot_history(_snapshot_payload(), int(user_id)))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/member/{user_id}/loot")
def api_member_loot(user_id: int, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    guild_id = _safe_guild_id(payload)
    return JSONResponse({"ok": True, "member_loot": _loot_member_payload_from_snapshot(payload.get("snapshot") or {}, int(user_id), int(guild_id or 0))})


@app.get("/export/member_{user_id}_loot.csv")
def export_member_loot_csv(user_id: int, _: bool = Depends(_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    p = _loot_member_payload_from_snapshot(payload.get("snapshot") or {}, int(user_id), int(guild_id or 0))
    rows = []
    for r in p.get("wins") or []:
        rows.append(["winner", r.get("auction_id"), r.get("item"), r.get("mode"), r.get("winning_amount"), _dt(r.get("closed_at")), ""])
    for b in p.get("bids") or []:
        rows.append(["bid", b.get("auction_id"), b.get("item"), b.get("mode"), b.get("amount"), _dt(b.get("created_at")), b.get("status")])
    for a in p.get("actions") or []:
        payload_json = a.get("payload") if isinstance(a.get("payload"), dict) else {}
        rows.append(["dashboard_action", a.get("auction_id"), payload_json.get("item_name") or payload_json.get("item") or "", a.get("action_type"), a.get("amount"), _dt(a.get("requested_at")), str(a.get("status") or "") + " " + _loot_result_text(a)])
    return _csv_response(f"member_{int(user_id)}_loot.csv", ["typ","auction_id","item","bereich_aktion","ec","zeit","status_ergebnis"], rows)


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
        return HTMLResponse(_render_admin_center_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/admin-legacy", response_class=HTMLResponse)
def admin_legacy_page(_: bool = Depends(_admin_auth)):
    try:
        return HTMLResponse(_render_admin_actions_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/api/admin-center")
def api_admin_center(_: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    return JSONResponse(_admin_center_payload(payload))


@app.get("/export/admin_center.csv")
def export_admin_center_csv(_: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return Response("error\n" + str(payload.get("error") or "unknown"), media_type="text/csv", status_code=404)
    p = _admin_center_payload(payload)
    rows = []
    for r in p.get("auth_rows") or []:
        rows.append(["auth", r.get("setting"), r.get("value"), r.get("hint")])
    for r in p.get("rule_rows") or []:
        rows.append(["rule", r[0], r[1], r[2]])
    for r in p.get("source_rows") or []:
        rows.append(["source", r[0], r[1], r[3]])
    for k, v in (p.get("ec_counts") or {}).items():
        rows.append(["ec_queue", k, v, ""])
    for k, v in (p.get("loot_counts") or {}).items():
        rows.append(["loot_queue", k, v, ""])
    return _csv_response("admin_center.csv", ["bereich","key","wert","hinweis"], rows)



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


@app.get("/admin-settings", response_class=HTMLResponse)
def admin_settings_page(_: bool = Depends(_admin_auth), msg: str = ""):
    try:
        return HTMLResponse(_render_admin_settings_editor(_snapshot_payload(), msg=msg))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/api/admin-settings")
def api_admin_settings(_: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    guild_id = _safe_guild_id(payload)
    return JSONResponse({
        "ok": True,
        "guild_id": guild_id,
        "current": _current_dkp_settings_from_snapshot(snap),
        "requests": _settings_change_requests_for_dashboard(guild_id, 120) if guild_id else [],
    })


@app.post("/admin/settings-change")
async def admin_settings_change(request: Request, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    actor = _current_user(request) or {"username": "Dashboard"}
    form = _parse_urlencoded_body(await request.body())
    action = str(form.get("action_type") or "").strip().lower()
    clean_payload: dict[str, Any] = {}
    if action == "set_event_points":
        clean_payload = {"event_type": form.get("event_type"), "points": form.get("points")}
    elif action == "set_weekly_limit":
        clean_payload = {"weekly_event_limit": form.get("weekly_event_limit")}
    elif action == "set_decay":
        clean_payload = {"decay_percent": form.get("decay_percent"), "decay_protected_balance": form.get("decay_protected_balance")}
    elif action == "set_roles":
        clean_payload = {"leader_role_id": form.get("leader_role_id"), "member_role_id": form.get("member_role_id"), "log_channel_id": form.get("log_channel_id")}
    elif action == "set_access_roles":
        clean_payload = {"dashboard_admin_role_ids": form.get("dashboard_admin_role_ids"), "dashboard_allowed_role_ids": form.get("dashboard_allowed_role_ids")}
    res = _enqueue_settings_change_request(guild_id, action, clean_payload, actor)
    msg = "Einstellungsänderung wurde an den Bot gesendet." if res.get("ok") else f"Fehler: {res.get('error')}"
    return RedirectResponse("/admin-settings?msg=" + urllib.parse.quote(msg), status_code=303)



@app.post("/admin/settings-request/{request_id}/{queue_action}")
async def admin_settings_request_action(request: Request, request_id: str, queue_action: str, _: bool = Depends(_admin_auth)):
    payload = _snapshot_payload()
    guild_id = _safe_guild_id(payload)
    actor = _current_user(request) or {"username": "Dashboard"}
    res = _settings_request_admin_action(guild_id, request_id, queue_action, actor)
    msg = "Queue-Aktion ausgeführt." if res.get("ok") else f"Fehler: {res.get('error')}"
    return RedirectResponse("/admin-settings?msg=" + urllib.parse.quote(msg), status_code=303)


def _release_status_payload(data: dict[str, Any]) -> dict[str, Any]:
    snap = data.get("snapshot") or {} if isinstance(data, dict) else {}
    events = ((snap.get("events") or {}).get("items") or []) if isinstance((snap.get("events") or {}), dict) else []
    members_raw = (snap.get("members") or {})
    members = (members_raw.get("items") or members_raw.get("profiles") or []) if isinstance(members_raw, dict) else []
    if isinstance(members, dict):
        member_count = len(members)
    else:
        member_count = len(members or [])
    loot_raw = snap.get("loot") or {}
    auctions = []
    if isinstance(loot_raw, dict):
        auctions = loot_raw.get("auctions") or loot_raw.get("items") or []
        if isinstance(auctions, dict):
            auctions = list(auctions.values())
    ec_raw = snap.get("ec") or {}
    balances = (ec_raw.get("balances") or {}) if isinstance(ec_raw, dict) else {}
    warnings = []
    if not data.get("ok"):
        warnings.append(str(data.get("error") or "Kein Snapshot verfügbar"))
    if not _database_url():
        warnings.append("DATABASE_URL fehlt oder ist leer")
    if not _discord_oauth_enabled():
        warnings.append("Discord OAuth ist nicht aktiv, Fallback-Login wird genutzt")
    return {
        "ok": bool(data.get("ok")),
        "version": DASHBOARD_RELEASE_VERSION,
        "generated_at": data.get("generated_at"),
        "published_at": data.get("published_at"),
        "guild_id": data.get("guild_id"),
        "guild_name": data.get("guild_name"),
        "counts": {
            "events": len(events or []),
            "members": member_count,
            "auctions": len(auctions or []),
            "ec_accounts": len(balances or {}),
        },
        "checks": {
            "snapshot": bool(data.get("ok")),
            "database_url": bool(_database_url()),
            "discord_oauth": bool(_discord_oauth_enabled()),
            "basic_password": bool(_env("DASHBOARD_PASSWORD")),
            "static_dir": bool(STATIC_DIR.exists()),
        },
        "warnings": warnings,
    }


def _render_release_dashboard(data: dict[str, Any]) -> str:
    p = _release_status_payload(data)
    counts = p.get("counts") or {}
    checks = p.get("checks") or {}
    warnings = p.get("warnings") or []
    cards = "".join([
        f"<div class='release-card'><b>{_e(counts.get('events', 0))}</b><span>Events im Snapshot</span></div>",
        f"<div class='release-card'><b>{_e(counts.get('members', 0))}</b><span>Mitglieder/Profile</span></div>",
        f"<div class='release-card'><b>{_e(counts.get('auctions', 0))}</b><span>Auktionen/Loot-Einträge</span></div>",
        f"<div class='release-card'><b>{_e(counts.get('ec_accounts', 0))}</b><span>EC-Konten</span></div>",
    ])
    rows = []
    labels = {
        "snapshot": "Snapshot vorhanden",
        "database_url": "Postgres/DATABASE_URL",
        "discord_oauth": "Discord OAuth",
        "basic_password": "Passwort-Fallback",
        "static_dir": "Static-Ordner",
    }
    for key, label in labels.items():
        ok = bool(checks.get(key))
        rows.append([label, "✅ ok" if ok else "⚠️ prüfen"])
    warn_html = "" if not warnings else "<section class='panel'><h2>Warnungen</h2><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></section>"
    return _html_shell("Release & Stabilität · Ebo Dashboard", f"""
    <nav class='topnav'><a href='/'>← Kommando</a><a href='/system'>System</a><a href='/admin'>Admin</a><a href='/api/release-status'>API</a></nav>
    <section class='hero'><div><h1>Release & Stabilität</h1><p>Version {_e(p.get('version'))} · kompakte Systemübersicht für den Livebetrieb.</p></div><div class='page-actions'><a class='btn' href='/'>Startseite</a><a class='btn' href='/system'>System prüfen</a></div></section>
    <section class='panel'><h2>Live-Status</h2><div class='release-grid'>{cards}</div><div class='mobile-note'>Mobile Optimierung ist aktiv: Sidebar klappt ein, Tabellen werden horizontal scrollbar, Aktionen werden auf Handybreite sauber gestapelt.</div></section>
    <section class='panel'><h2>Prüfpunkte</h2>{_table(['Bereich','Status'], rows, searchable=False)}</section>
    {warn_html}
    <section class='panel'><h2>Finale Bereiche</h2><div class='grid'>
      <a class='btn' href='/events'>Events</a><a class='btn' href='/portal'>Portal</a><a class='btn' href='/loot'>Loot</a><a class='btn' href='/admin-settings'>Admin-Einstellungen</a>
    </div></section>
    """)


@app.get("/release", response_class=HTMLResponse)
def release_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_release_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/release-status")
def api_release_status(_: bool = Depends(_auth)):
    return JSONResponse(_release_status_payload(_snapshot_payload()))



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
def index(request: Request, _: bool = Depends(_auth)):
    user = _current_user(request) or {}
    if str(user.get("role") or "") == "member":
        return RedirectResponse("/member", status_code=303)
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
      <div style="display:flex;gap:8px;flex-wrap:wrap"><form method="post" action="/admin/attendance/{_e(event_id)}/voice-suggest"><button class="btn" type="submit">🎙️ Voice-Vorschlag neu laden</button></form><a class="btn" href="#review-save">✅ 1. Überprüfen</a><a class="btn" href="/attendance/{_e(event_id)}/report">📋 Abschlussbericht</a></div>
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
      <form id="review-save" method="post" action="/admin/attendance/{_e(event_id)}/save">
        <div class='table-wrap'><table><thead><tr><th>Spieler</th><th>Anmeldung</th><th>Voice</th><th>Quelle</th><th>Status</th><th>Notiz</th></tr></thead><tbody>{''.join(rows_html)}</tbody></table></div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;align-items:center">
          <button class="btn" type="submit" name="next" value="preview">✅ 1. Überprüfen</button>
          <button class="btn" type="submit" name="next" value="save">Review nur speichern</button>
          <span class="muted">Überprüfen speichert den Review und öffnet direkt die EC-Vorschau. Danach ist nur noch „EC wirklich buchen“ nötig.</span>
        </div>
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
    next_action = str((form.get("next") or [""])[0] or "").strip().lower()
    if next_action == "preview":
        return RedirectResponse(f"/attendance/{urllib.parse.quote(str(event_id))}/ec-preview?saved=1", status_code=303)
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
    latest_status = str(latest_request.get("status") or "").lower()
    book_blocked = bool(award_state.get("awarded")) or latest_status in {"pending", "processing", "done"} or recipients <= 0 or fe <= 0
    book_disabled_attr = "disabled" if book_blocked else ""
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
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
        <form method="post" action="/admin/attendance/{_e(event_id)}/ec-award" onsubmit="return confirm('EC wirklich buchen? Der Bot verarbeitet diese Anfrage und schreibt danach in die echten EC-Daten.');">
          <input type="hidden" name="full_ec" value="{_e(fe)}"><input type="hidden" name="partial_ec" value="{_e(pe)}"><input type="hidden" name="event_type" value="{_e(event_type)}">
          <button class="btn" type="submit" {book_disabled_attr}>✅ 2. EC wirklich buchen</button>
        </form>
        <a class="btn" href="/export/attendance/{_e(event_id)}.csv?full_ec={_e(fe)}&partial_ec={_e(pe)}">CSV herunterladen</a>
      </div>
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
          <button class="btn" type="submit" {book_disabled_attr}>✅ 2. EC wirklich buchen</button>
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


# ---------------------------------------------------------------------------
# Phase 3 · Postgres-Datenbasis vorbereiten
# ---------------------------------------------------------------------------
# Ziel dieser Phase:
# - Dashboard liest Phase-3-Tabellen Postgres-first.
# - Bot speichert weiterhin JSON und spiegelt direkt nach Postgres.
# - JSON bleibt Backup/Fallback und lokale Bot-Sicherheitskopie.
# - Es wird nichts gelöscht.

PHASE3_TABLES = [
    "phase3_members",
    "phase3_ec_balances",
    "phase3_ec_transactions",
    "phase3_ec_event_checks",
    "phase3_loot_needs",
    "phase3_events",
    "phase3_event_rsvps",
    "phase3_loot_auctions",
    "phase3_loot_bids",
    "phase3_loot_history",
    "phase3_need_change_log",
    "phase3_absences",
    "phase3_settings",
    "phase3_migration_runs",
]


def _phase3_jsonb(value: Any):
    from psycopg.types.json import Jsonb  # type: ignore

    return Jsonb(value if value is not None else {})


def _phase3_now() -> datetime:
    return datetime.now(timezone.utc)


def _phase3_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        # Manche Snapshot-Bereiche liegen als Dict user_id -> Objekt vor.
        out = []
        for k, v in value.items():
            if isinstance(v, dict):
                item = dict(v)
                item.setdefault("id", str(k))
                item.setdefault("user_id", str(k))
                out.append(item)
            else:
                out.append({"id": str(k), "value": v})
        return out
    return []


def _phase3_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _phase3_first_id(item: Any, keys: list[str], fallback: str = "") -> str:
    if not isinstance(item, dict):
        return fallback
    for k in keys:
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return fallback


def _phase3_hash_id(prefix: str, raw: Any) -> str:
    blob = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + "_" + hashlib.sha1(blob.encode("utf-8")).hexdigest()[:18]


def _phase3_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return default


def _phase3_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", "."))
    except Exception:
        return default


def _phase3_dt_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).strip()


def _phase3_ensure_schema() -> dict[str, Any]:
    if not _database_url():
        return {"ok": False, "error": "DATABASE_URL fehlt."}
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_members (
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    discord_name TEXT,
                    ingame_name TEXT,
                    roles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_balances (
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    balance INTEGER NOT NULL DEFAULT 0,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_transactions (
                    guild_id TEXT NOT NULL,
                    transaction_id TEXT NOT NULL,
                    user_id TEXT,
                    amount INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    event_id TEXT,
                    created_at_text TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, transaction_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_event_checks (
                    guild_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    status TEXT,
                    awarded BOOLEAN NOT NULL DEFAULT FALSE,
                    posted BOOLEAN NOT NULL DEFAULT FALSE,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, event_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_loot_needs (
                    guild_id TEXT NOT NULL,
                    need_id TEXT NOT NULL,
                    user_id TEXT,
                    item_name TEXT,
                    need_type TEXT,
                    slot_name TEXT,
                    status TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, need_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_events (
                    guild_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    title TEXT,
                    status TEXT,
                    start_at_text TEXT,
                    end_at_text TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, event_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_event_rsvps (
                    guild_id TEXT NOT NULL,
                    rsvp_id TEXT NOT NULL,
                    event_id TEXT,
                    user_id TEXT,
                    response TEXT,
                    role_name TEXT,
                    display_name TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, rsvp_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_loot_auctions (
                    guild_id TEXT NOT NULL,
                    auction_id TEXT NOT NULL,
                    item_name TEXT,
                    status TEXT,
                    winner_user_id TEXT,
                    current_bid INTEGER NOT NULL DEFAULT 0,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, auction_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_loot_bids (
                    guild_id TEXT NOT NULL,
                    bid_id TEXT NOT NULL,
                    auction_id TEXT,
                    user_id TEXT,
                    amount INTEGER NOT NULL DEFAULT 0,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, bid_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_loot_history (
                    guild_id TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    user_id TEXT,
                    item_name TEXT,
                    amount INTEGER NOT NULL DEFAULT 0,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, entry_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_need_change_log (
                    guild_id TEXT NOT NULL,
                    log_id TEXT NOT NULL,
                    request_id TEXT,
                    actor_id TEXT,
                    target_user_id TEXT,
                    action_type TEXT,
                    old_item TEXT,
                    new_item TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'bot',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, log_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_absences (
                    guild_id TEXT NOT NULL,
                    absence_id TEXT NOT NULL,
                    user_id TEXT,
                    status TEXT,
                    start_at_text TEXT,
                    end_at_text TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, absence_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_settings (
                    guild_id TEXT NOT NULL,
                    setting_key TEXT NOT NULL,
                    value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, setting_key)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_migration_runs (
                    run_id TEXT PRIMARY KEY,
                    guild_id TEXT,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    counts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    notes TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_members_guild ON phase3_members (guild_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_event_rsvps_event ON phase3_event_rsvps (guild_id, event_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_event_rsvps_user ON phase3_event_rsvps (guild_id, user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_ec_tx_user ON phase3_ec_transactions (guild_id, user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_ec_checks_status ON phase3_ec_event_checks (guild_id, awarded, posted)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_needs_item ON phase3_loot_needs (guild_id, item_name)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_events_status ON phase3_events (guild_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_auctions_status ON phase3_loot_auctions (guild_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_need_log_target ON phase3_need_change_log (guild_id, target_user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_runs_created ON phase3_migration_runs (created_at DESC)")
        conn.commit()
        return {"ok": True, "tables": PHASE3_TABLES}
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        conn.close()


def _phase3_status_payload() -> dict[str, Any]:
    out: dict[str, Any] = {
        "ok": False,
        "database_url": bool(_database_url()),
        "phase": "3.9",
        "mode": "Dashboard Postgres-first · Bot JSON+Postgres-Spiegelung",
        "tables": {},
        "counts": {},
        "latest_runs": [],
        "warnings": [],
    }
    if not _database_url():
        out["warnings"].append("DATABASE_URL fehlt. Postgres ist nicht erreichbar.")
        return out
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            for table in PHASE3_TABLES:
                cur.execute("SELECT to_regclass(%s) AS reg", (table,))
                exists = bool((cur.fetchone() or {}).get("reg"))
                out["tables"][table] = exists
                if exists:
                    cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
                    out["counts"][table] = int((cur.fetchone() or {}).get("c") or 0)
                else:
                    out["counts"][table] = 0
            if out["tables"].get("phase3_migration_runs"):
                cur.execute("""
                    SELECT run_id, guild_id, mode, status, counts_json, notes, created_at
                    FROM phase3_migration_runs
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                out["latest_runs"] = [dict(r) for r in (cur.fetchall() or [])]
        out["ok"] = all(out["tables"].values())
        if not out["ok"]:
            out["warnings"].append("Phase-3-Tabellen sind noch nicht vollständig angelegt.")
        return out
    except Exception as exc:
        out["warnings"].append(f"DB-Status fehlgeschlagen: {type(exc).__name__}: {exc}")
        return out
    finally:
        conn.close()




def _phase3_need_entry_rows_from_snapshot(snap: dict[str, Any]) -> list[dict[str, Any]]:
    """Flattened Need-Einträge aus dem Dashboard-Snapshot.

    Wichtig: snap['loot']['needs'] ist je nach Bot-Version nicht eine Liste von
    Need-Slots, sondern oft ein Container mit Spieler-Zeilen oder bereits
    gruppierten Item-Zeilen. Diese Funktion zählt echte Need-Einträge statt nur
    Top-Level-Zeilen.
    """
    if not isinstance(snap, dict):
        return []
    loot_raw = snap.get("loot") or {}
    if not isinstance(loot_raw, dict):
        return []
    needs_obj = loot_raw.get("needs") or loot_raw.get("needlist") or loot_raw.get("needlists") or []
    if isinstance(needs_obj, dict):
        raw_rows = needs_obj.get("items") or needs_obj.get("rows") or needs_obj.get("users") or needs_obj.get("profiles") or needs_obj
    else:
        raw_rows = needs_obj

    names = _profile_name_map(snap)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_need(*, user_id: Any, display_name: str, need_type: str, slot_name: str, item_value: Any, raw: Any) -> None:
        label = _loot_text(item_value)
        if not label:
            return
        if label in {"—", "-", "none", "None"}:
            return
        uid = str(_user_id(user_id) or user_id or "").strip()
        need_type_s = str(need_type or "").strip() or "Main"
        slot_s = str(slot_name or "").strip()
        raw_json = raw if isinstance(raw, dict) else {"value": raw}
        if not uid:
            uid = str(raw_json.get("user_id") or raw_json.get("discord_id") or raw_json.get("member_id") or "").strip()
        disp = str(display_name or raw_json.get("display_name") or raw_json.get("name") or (names.get(_user_id(uid), "") if uid else "") or "").strip()
        base = f"{uid}|{disp}|{need_type_s}|{slot_s}|{label}"
        need_id = "need_" + hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:22]
        if need_id in seen:
            return
        seen.add(need_id)
        out.append({
            "need_id": need_id,
            "user_id": uid,
            "display_name": disp,
            "item_name": label,
            "need_type": need_type_s,
            "slot_name": slot_s,
            "status": str(raw_json.get("status") or raw_json.get("state") or ("received" if raw_json.get("received") else "open")),
            "raw_json": raw_json | {"phase3_source_shape": "snapshot_flattened", "display_name": disp},
        })

    def iter_entries(value: Any):
        if not value:
            return []
        if isinstance(value, list):
            return list(enumerate(value, start=1))
        if isinstance(value, dict):
            return list(value.items())
        return [("", value)]

    for row_index, row in enumerate(_phase3_list(raw_rows), start=1):
        if not isinstance(row, dict):
            continue
        uid = _phase3_first_id(row, ["user_id", "discord_id", "member_id", "id"])
        display_name = str(row.get("display_name") or row.get("name") or row.get("member_name") or (names.get(_user_id(uid), "") if uid else "") or "")

        # Form A: Spieler-Zeile: {user_id, main:[...], secondary:[...]}
        handled_player_shape = False
        for need_type, keys in (
            ("Main", ["main", "Main", "main_needs", "main_items", "main_need"]),
            ("Secondary", ["secondary", "Secondary", "secondary_needs", "secondary_items", "second", "second_needs"]),
        ):
            entries = None
            for key in keys:
                if row.get(key):
                    entries = row.get(key)
                    break
            if entries:
                handled_player_shape = True
                for slot, entry in iter_entries(entries):
                    label_val = entry
                    slot_name = str(slot)
                    if isinstance(entry, dict):
                        label_val = entry.get("item_name") or entry.get("item") or entry.get("name") or entry.get("label") or entry.get("value") or entry.get("text") or entry
                        slot_name = str(entry.get("slot") or entry.get("slot_name") or slot)
                    add_need(user_id=uid, display_name=display_name, need_type=need_type, slot_name=slot_name, item_value=label_val, raw=entry)

        if handled_player_shape:
            continue

        # Form B: Gruppierte Item-Zeile: {item, main:[people], secondary:[people]}
        item_label = row.get("item_name") or row.get("item") or row.get("name") or row.get("label")
        if item_label and (row.get("main") is not None or row.get("secondary") is not None or row.get("second") is not None):
            for need_type, people_val in (("Main", row.get("main") or []), ("Secondary", row.get("secondary") or row.get("second") or [])):
                for p_idx, p in iter_entries(people_val):
                    if isinstance(p, dict):
                        p_uid = _phase3_first_id(p, ["user_id", "discord_id", "member_id", "id"])
                        p_name = str(p.get("display_name") or p.get("name") or p.get("member_name") or (names.get(_user_id(p_uid), "") if p_uid else ""))
                        raw = dict(p)
                    else:
                        p_uid = ""
                        p_name = str(p)
                        raw = {"display_name": p_name}
                    add_need(user_id=p_uid, display_name=p_name, need_type=need_type, slot_name=str(row.get("slot") or row.get("slot_name") or ""), item_value=item_label, raw=raw | {"item": item_label})
            continue

        # Form C: Einzelner Need-Datensatz.
        direct_item = row.get("item_name") or row.get("item") or row.get("name") or row.get("label") or row.get("value")
        if direct_item:
            add_need(
                user_id=uid,
                display_name=display_name,
                need_type=str(row.get("need_type") or row.get("type") or row.get("kind") or "Main"),
                slot_name=str(row.get("slot") or row.get("slot_name") or row_index),
                item_value=direct_item,
                raw=row,
            )

    return out


def _phase3_member_rows_from_snapshot(snap: dict[str, Any]) -> list[dict[str, Any]]:
    """Liefert echte Mitglieder/Profile aus allen aktuell bekannten Snapshot-Bereichen.

    Wichtig: Der alte Top-Level-Bereich snapshot.members ist bei uns oft leer. Die
    brauchbaren Mitglieder liegen meist unter snapshot.insights.members oder
    snapshot.profiles.items. Darum sammeln und deduplizieren wir hier gezielt.
    """
    if not isinstance(snap, dict):
        return []
    candidates: list[Any] = []
    members_raw = snap.get("members") or {}
    if isinstance(members_raw, dict):
        candidates.extend(_phase3_list(members_raw.get("items") or members_raw.get("profiles") or members_raw.get("members") or []))
    else:
        candidates.extend(_phase3_list(members_raw))
    profiles_raw = snap.get("profiles") or {}
    if isinstance(profiles_raw, dict):
        candidates.extend(_phase3_list(profiles_raw.get("items") or profiles_raw.get("profiles") or profiles_raw.get("members") or []))
    insights_raw = snap.get("insights") or {}
    if isinstance(insights_raw, dict):
        candidates.extend(_phase3_list(insights_raw.get("members") or []))
        quality = insights_raw.get("needs") if isinstance(insights_raw.get("needs"), dict) else {}
        candidates.extend(_phase3_list(quality.get("users_without_needs") or []))
        risk_members = insights_raw.get("risk_members") or []
        candidates.extend(_phase3_list(risk_members))
    out: dict[str, dict[str, Any]] = {}
    for row in candidates:
        if not isinstance(row, dict):
            continue
        uid = _phase3_first_id(row, ["user_id", "discord_id", "id", "member_id"])
        if not uid:
            continue
        current = out.get(uid, {})
        merged = dict(current)
        merged.update(row)
        merged["user_id"] = uid
        out[uid] = merged
    return list(out.values())


def _phase3_event_rows_from_snapshot(snap: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(snap, dict):
        return []
    events_raw = snap.get("events") or {}
    if isinstance(events_raw, dict):
        return [x for x in _phase3_list(events_raw.get("items") or events_raw.get("events") or []) if isinstance(x, dict)]
    return [x for x in _phase3_list(events_raw) if isinstance(x, dict)]


def _phase3_event_rsvp_rows_from_snapshot(snap: dict[str, Any]) -> list[dict[str, Any]]:
    """Extrahiert RSVP-/Teilnehmerzeilen aus den Event-Snapshots.

    Unterstützt die vom Bot exportierte Struktur:
    event.participants = {tank:[...], heal:[...], dps:[...], reserve:[...], maybe:[...], no:[...]}
    sowie ältere Varianten mit yes/accepted/maybe/no.
    """
    out: list[dict[str, Any]] = []
    for ev in _phase3_event_rows_from_snapshot(snap):
        event_id = _phase3_first_id(ev, ["event_id", "id", "message_id"]) or _phase3_hash_id("event", ev)
        title = _phase3_str(ev.get("title") or ev.get("name"))
        participants = ev.get("participants") or ev.get("rsvps") or {}
        # Falls yes_counts existiert, enthält participants meist die eigentlichen Listen.
        if isinstance(participants, dict):
            for response_key, values in participants.items():
                for person in _phase3_list(values):
                    if isinstance(person, dict):
                        uid = _phase3_first_id(person, ["user_id", "discord_id", "id", "member_id"])
                        display = _phase3_str(person.get("display_name") or person.get("name") or person.get("username"))
                        role_name = _phase3_str(person.get("role") or person.get("role_name") or response_key)
                        response = _phase3_str(person.get("response") or person.get("status") or response_key)
                        raw = dict(person)
                    else:
                        uid = _phase3_str(person)
                        display = ""
                        role_name = _phase3_str(response_key)
                        response = _phase3_str(response_key)
                        raw = {"value": person}
                    if not uid and not display:
                        continue
                    raw.setdefault("event_id", event_id)
                    raw.setdefault("event_title", title)
                    raw.setdefault("response", response)
                    raw.setdefault("role_name", role_name)
                    out.append({
                        "event_id": event_id,
                        "user_id": uid,
                        "display_name": display,
                        "response": response,
                        "role_name": role_name,
                        "raw_json": raw,
                    })
        # Fallback: manche Event-JSONs halten direkt yes/maybe/no.
        for response_key in ["yes", "accepted", "maybe", "no", "declined", "reserve", "tentative"]:
            values = ev.get(response_key)
            if not values:
                continue
            if isinstance(values, dict):
                iterable = []
                for role_name, sub in values.items():
                    for person in _phase3_list(sub):
                        iterable.append((role_name, person))
            else:
                iterable = [(response_key, person) for person in _phase3_list(values)]
            for role_name, person in iterable:
                if isinstance(person, dict):
                    uid = _phase3_first_id(person, ["user_id", "discord_id", "id", "member_id"])
                    display = _phase3_str(person.get("display_name") or person.get("name") or person.get("username"))
                    raw = dict(person)
                else:
                    uid = _phase3_str(person)
                    display = ""
                    raw = {"value": person}
                if not uid and not display:
                    continue
                raw.setdefault("event_id", event_id)
                raw.setdefault("event_title", title)
                raw.setdefault("response", response_key)
                raw.setdefault("role_name", role_name)
                out.append({
                    "event_id": event_id,
                    "user_id": uid,
                    "display_name": display,
                    "response": _phase3_str(response_key),
                    "role_name": _phase3_str(role_name),
                    "raw_json": raw,
                })
    # Deduplizieren, falls participants und yes/no dieselben Leute liefern.
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in out:
        key = (row.get("event_id") or "", row.get("user_id") or row.get("display_name") or "", row.get("response") or "", row.get("role_name") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _phase3_absence_rows_from_snapshot(snap: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(snap, dict):
        return []
    candidates: list[Any] = []
    for key in ["absences", "absence", "absence_reports", "away"]:
        raw = snap.get(key)
        if isinstance(raw, dict):
            candidates.extend(_phase3_list(raw.get("items") or raw.get("absences") or raw.get("reports") or raw))
        else:
            candidates.extend(_phase3_list(raw))
    profiles = snap.get("profiles") or {}
    if isinstance(profiles, dict):
        candidates.extend(_phase3_list(profiles.get("absences") or []))
    out: dict[str, dict[str, Any]] = {}
    for row in candidates:
        if not isinstance(row, dict):
            continue
        aid = _phase3_first_id(row, ["absence_id", "id", "request_id"]) or _phase3_hash_id("absence", row)
        out[aid] = row
    return list(out.values())

def _phase3_extract_snapshot_counts(payload: dict[str, Any]) -> dict[str, int]:
    snap = payload.get("snapshot") or {} if isinstance(payload, dict) else {}
    events = _phase3_event_rows_from_snapshot(snap)
    members = _phase3_member_rows_from_snapshot(snap)
    event_rsvps = _phase3_event_rsvp_rows_from_snapshot(snap)
    ec_raw = snap.get("ec") or {} if isinstance(snap, dict) else {}
    balances = ec_raw.get("balances") or {} if isinstance(ec_raw, dict) else {}
    transactions = ec_raw.get("transactions") or ec_raw.get("history") or [] if isinstance(ec_raw, dict) else []
    loot_raw = snap.get("loot") or {} if isinstance(snap, dict) else {}
    need_entries = _phase3_need_entry_rows_from_snapshot(snap)
    auctions = []
    history = []
    if isinstance(loot_raw, dict):
        auctions = loot_raw.get("auctions") or loot_raw.get("items") or []
        history = loot_raw.get("history") or loot_raw.get("loot_history") or loot_raw.get("transactions") or []
    absences = _phase3_absence_rows_from_snapshot(snap)
    return {
        "members": len(_phase3_list(members)),
        "ec_balances": len(balances) if isinstance(balances, dict) else len(_phase3_list(balances)),
        "ec_transactions": len(_phase3_list(transactions)),
        "needs": len(need_entries),
        "events": len(_phase3_list(events)),
        "event_rsvps": len(event_rsvps),
        "auctions": len(_phase3_list(auctions)),
        "loot_history": len(_phase3_list(history)),
        "absences": len(_phase3_list(absences)),
    }


def _phase3_mirror_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    schema = _phase3_ensure_schema()
    if not schema.get("ok"):
        return schema
    snap = payload.get("snapshot") or {} if isinstance(payload, dict) else {}
    guild_id = _phase3_str(payload.get("guild_id") or (snap.get("guild_id") if isinstance(snap, dict) else "") or _env("GUILD_ID") or "default")
    counts = {"members": 0, "ec_balances": 0, "ec_transactions": 0, "ec_event_checks": 0, "needs": 0, "events": 0, "event_rsvps": 0, "auctions": 0, "bids": 0, "loot_history": 0, "absences": 0, "settings": 0}
    conn = _pg_connect()
    run_id = "phase3_" + hashlib.sha1(f"{guild_id}:{time.time()}".encode()).hexdigest()[:18]
    try:
        with conn.cursor() as cur:
            # Members/Profile
            members = _phase3_member_rows_from_snapshot(snap)
            for item in _phase3_list(members):
                if not isinstance(item, dict):
                    continue
                user_id = _phase3_first_id(item, ["user_id", "discord_id", "id", "member_id"])
                if not user_id:
                    continue
                discord_name = _phase3_str(item.get("display_name") or item.get("discord_name") or item.get("name") or item.get("username"))
                ingame_name = _phase3_str(item.get("ingame_name") or item.get("character") or item.get("char_name") or item.get("main_name"))
                roles = item.get("roles") or item.get("role_names") or []
                cur.execute("""
                    INSERT INTO phase3_members (guild_id, user_id, discord_name, ingame_name, roles_json, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET
                      discord_name=EXCLUDED.discord_name,
                      ingame_name=EXCLUDED.ingame_name,
                      roles_json=EXCLUDED.roles_json,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, user_id, discord_name, ingame_name, _phase3_jsonb(roles if isinstance(roles, list) else [roles]), _phase3_jsonb(item)))
                counts["members"] += 1

            # EC balances and transactions
            ec_raw = (snap.get("ec") or {}) if isinstance(snap, dict) else {}
            balances = (ec_raw.get("balances") or {}) if isinstance(ec_raw, dict) else {}
            if isinstance(balances, dict):
                bal_items = balances.items()
            else:
                bal_items = [(None, x) for x in _phase3_list(balances)]
            for uid, val in bal_items:
                raw = val if isinstance(val, dict) else {"balance": val}
                user_id = _phase3_str(uid) or _phase3_first_id(raw, ["user_id", "discord_id", "id", "member_id"])
                if not user_id:
                    continue
                balance = _phase3_int(raw.get("balance") if isinstance(raw, dict) else val)
                if isinstance(raw, dict):
                    balance = _phase3_int(raw.get("balance", raw.get("dkp", raw.get("ec", balance))), balance)
                cur.execute("""
                    INSERT INTO phase3_ec_balances (guild_id, user_id, balance, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET
                      balance=EXCLUDED.balance,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, user_id, balance, _phase3_jsonb(raw)))
                counts["ec_balances"] += 1
            txs = []
            if isinstance(ec_raw, dict):
                txs = ec_raw.get("transactions") or ec_raw.get("history") or ec_raw.get("items") or []
            for raw in _phase3_list(txs):
                if not isinstance(raw, dict):
                    continue
                tx_id = _phase3_first_id(raw, ["transaction_id", "tx_id", "id"]) or _phase3_hash_id("tx", raw)
                user_id = _phase3_first_id(raw, ["user_id", "discord_id", "member_id"])
                amount = _phase3_int(raw.get("amount", raw.get("value", raw.get("delta", 0))))
                reason = _phase3_str(raw.get("reason") or raw.get("note") or raw.get("type"))
                event_id = _phase3_str(raw.get("event_id") or raw.get("source_event_id"))
                created = _phase3_dt_string(raw.get("created_at") or raw.get("timestamp") or raw.get("time") or raw.get("date"))
                cur.execute("""
                    INSERT INTO phase3_ec_transactions (guild_id, transaction_id, user_id, amount, reason, event_id, created_at_text, raw_json, source, mirrored_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, transaction_id) DO UPDATE SET
                      user_id=EXCLUDED.user_id,
                      amount=EXCLUDED.amount,
                      reason=EXCLUDED.reason,
                      event_id=EXCLUDED.event_id,
                      created_at_text=EXCLUDED.created_at_text,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      mirrored_at=now()
                """, (guild_id, tx_id, user_id, amount, reason, event_id, created, _phase3_jsonb(raw)))
                counts["ec_transactions"] += 1

            # EC event checks
            checks_raw = {}
            if isinstance(snap, dict):
                checks_raw = snap.get("dkp_event_checks") or snap.get("ec_event_checks") or {}
                if not checks_raw and isinstance(ec_raw, dict):
                    checks_raw = ec_raw.get("event_checks") or ec_raw.get("checks") or {}
            check_events = {}
            if isinstance(checks_raw, dict):
                if isinstance(checks_raw.get("events"), dict):
                    check_events = checks_raw.get("events") or {}
                elif guild_id in checks_raw and isinstance(checks_raw.get(guild_id), dict):
                    gobj = checks_raw.get(guild_id) or {}
                    check_events = gobj.get("events") if isinstance(gobj.get("events"), dict) else gobj
                else:
                    check_events = checks_raw
            if isinstance(check_events, dict):
                for event_id, raw in list(check_events.items()):
                    if not isinstance(raw, dict):
                        continue
                    awarded = bool(raw.get("awarded", False))
                    posted = bool(raw.get("posted", False))
                    status = "awarded" if awarded else ("posted" if posted else _phase3_str(raw.get("status") or "open"))
                    cur.execute("""
                        INSERT INTO phase3_ec_event_checks (guild_id, event_id, status, awarded, posted, raw_json, source, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,'snapshot',now())
                        ON CONFLICT (guild_id, event_id) DO UPDATE SET
                          status=EXCLUDED.status,
                          awarded=EXCLUDED.awarded,
                          posted=EXCLUDED.posted,
                          raw_json=EXCLUDED.raw_json,
                          source='snapshot',
                          updated_at=now()
                    """, (guild_id, str(event_id), status, awarded, posted, _phase3_jsonb(raw)))
                    counts["ec_event_checks"] += 1

            # Loot needs, auctions, bids, history
            loot_raw = (snap.get("loot") or {}) if isinstance(snap, dict) else {}
            need_entries = _phase3_need_entry_rows_from_snapshot(snap)
            auctions = []
            history = []
            if isinstance(loot_raw, dict):
                auctions = loot_raw.get("auctions") or loot_raw.get("items") or []
                history = loot_raw.get("history") or loot_raw.get("loot_history") or loot_raw.get("transactions") or []
            cur.execute("DELETE FROM phase3_loot_needs WHERE guild_id=%s AND source='snapshot'", (guild_id,))
            for raw in need_entries:
                if not isinstance(raw, dict):
                    continue
                need_id = _phase3_str(raw.get("need_id")) or _phase3_hash_id("need", raw)
                user_id = _phase3_str(raw.get("user_id"))
                item_name = _phase3_str(raw.get("item_name"))
                need_type = _phase3_str(raw.get("need_type"))
                slot = _phase3_str(raw.get("slot_name"))
                status = _phase3_str(raw.get("status") or "open")
                cur.execute("""
                    INSERT INTO phase3_loot_needs (guild_id, need_id, user_id, item_name, need_type, slot_name, status, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, need_id) DO UPDATE SET
                      user_id=EXCLUDED.user_id,
                      item_name=EXCLUDED.item_name,
                      need_type=EXCLUDED.need_type,
                      slot_name=EXCLUDED.slot_name,
                      status=EXCLUDED.status,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, need_id, user_id, item_name, need_type, slot, status, _phase3_jsonb(raw.get("raw_json") or raw)))
                counts["needs"] += 1
            for raw in _phase3_list(auctions):
                if not isinstance(raw, dict):
                    continue
                auction_id = _phase3_first_id(raw, ["auction_id", "id", "message_id"]) or _phase3_hash_id("auc", raw)
                item_name = _phase3_str(raw.get("item_name") or raw.get("item") or raw.get("name") or raw.get("title"))
                status = _phase3_str(raw.get("status") or raw.get("state"))
                winner = _phase3_first_id(raw, ["winner_user_id", "winner_id", "winner", "user_id"])
                current_bid = _phase3_int(raw.get("current_bid") or raw.get("highest_bid") or raw.get("price") or raw.get("amount"))
                cur.execute("""
                    INSERT INTO phase3_loot_auctions (guild_id, auction_id, item_name, status, winner_user_id, current_bid, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, auction_id) DO UPDATE SET
                      item_name=EXCLUDED.item_name,
                      status=EXCLUDED.status,
                      winner_user_id=EXCLUDED.winner_user_id,
                      current_bid=EXCLUDED.current_bid,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, auction_id, item_name, status, winner, current_bid, _phase3_jsonb(raw)))
                counts["auctions"] += 1
                bids = raw.get("bids") or raw.get("bid_history") or []
                for bid in _phase3_list(bids):
                    if not isinstance(bid, dict):
                        continue
                    bid_raw = dict(bid)
                    bid_raw.setdefault("auction_id", auction_id)
                    bid_id = _phase3_first_id(bid_raw, ["bid_id", "id"]) or _phase3_hash_id("bid", bid_raw)
                    bidder = _phase3_first_id(bid_raw, ["user_id", "discord_id", "bidder_id", "member_id"])
                    amount = _phase3_int(bid_raw.get("amount") or bid_raw.get("bid") or bid_raw.get("value"))
                    cur.execute("""
                        INSERT INTO phase3_loot_bids (guild_id, bid_id, auction_id, user_id, amount, raw_json, source, mirrored_at)
                        VALUES (%s,%s,%s,%s,%s,%s,'snapshot',now())
                        ON CONFLICT (guild_id, bid_id) DO UPDATE SET
                          auction_id=EXCLUDED.auction_id,
                          user_id=EXCLUDED.user_id,
                          amount=EXCLUDED.amount,
                          raw_json=EXCLUDED.raw_json,
                          source='snapshot',
                          mirrored_at=now()
                    """, (guild_id, bid_id, auction_id, bidder, amount, _phase3_jsonb(bid_raw)))
                    counts["bids"] += 1
            for raw in _phase3_list(history):
                if not isinstance(raw, dict):
                    continue
                entry_id = _phase3_first_id(raw, ["entry_id", "history_id", "id"]) or _phase3_hash_id("loot", raw)
                user_id = _phase3_first_id(raw, ["user_id", "discord_id", "winner_user_id", "member_id"])
                item_name = _phase3_str(raw.get("item_name") or raw.get("item") or raw.get("name"))
                amount = _phase3_int(raw.get("amount") or raw.get("price") or raw.get("bid") or raw.get("ec"))
                cur.execute("""
                    INSERT INTO phase3_loot_history (guild_id, entry_id, user_id, item_name, amount, raw_json, source, mirrored_at)
                    VALUES (%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, entry_id) DO UPDATE SET
                      user_id=EXCLUDED.user_id,
                      item_name=EXCLUDED.item_name,
                      amount=EXCLUDED.amount,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      mirrored_at=now()
                """, (guild_id, entry_id, user_id, item_name, amount, _phase3_jsonb(raw)))
                counts["loot_history"] += 1

            # Events
            events = _phase3_event_rows_from_snapshot(snap)
            for raw in _phase3_list(events):
                if not isinstance(raw, dict):
                    continue
                event_id = _phase3_first_id(raw, ["event_id", "id", "message_id"]) or _phase3_hash_id("event", raw)
                title = _phase3_str(raw.get("title") or raw.get("name"))
                status = _phase3_str(raw.get("status") or raw.get("state"))
                start_at = _phase3_dt_string(raw.get("start_at") or raw.get("start") or raw.get("start_time") or raw.get("date"))
                end_at = _phase3_dt_string(raw.get("end_at") or raw.get("end") or raw.get("end_time"))
                cur.execute("""
                    INSERT INTO phase3_events (guild_id, event_id, title, status, start_at_text, end_at_text, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, event_id) DO UPDATE SET
                      title=EXCLUDED.title,
                      status=EXCLUDED.status,
                      start_at_text=EXCLUDED.start_at_text,
                      end_at_text=EXCLUDED.end_at_text,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, event_id, title, status, start_at, end_at, _phase3_jsonb(raw)))
                counts["events"] += 1

            # Event RSVPs / Teilnehmer
            for raw in _phase3_event_rsvp_rows_from_snapshot(snap):
                if not isinstance(raw, dict):
                    continue
                event_id = _phase3_str(raw.get("event_id"))
                user_id = _phase3_str(raw.get("user_id"))
                response = _phase3_str(raw.get("response"))
                role_name = _phase3_str(raw.get("role_name"))
                display_name = _phase3_str(raw.get("display_name"))
                rsvp_id = _phase3_hash_id("rsvp", {"event_id": event_id, "user_id": user_id or display_name, "response": response, "role": role_name})
                cur.execute("""
                    INSERT INTO phase3_event_rsvps (guild_id, rsvp_id, event_id, user_id, response, role_name, display_name, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, rsvp_id) DO UPDATE SET
                      event_id=EXCLUDED.event_id,
                      user_id=EXCLUDED.user_id,
                      response=EXCLUDED.response,
                      role_name=EXCLUDED.role_name,
                      display_name=EXCLUDED.display_name,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, rsvp_id, event_id, user_id, response, role_name, display_name, _phase3_jsonb(raw.get("raw_json") or raw)))
                counts["event_rsvps"] += 1

            # Absences
            absences = _phase3_absence_rows_from_snapshot(snap)
            for raw in _phase3_list(absences):
                if not isinstance(raw, dict):
                    continue
                absence_id = _phase3_first_id(raw, ["absence_id", "id"]) or _phase3_hash_id("absence", raw)
                user_id = _phase3_first_id(raw, ["user_id", "discord_id", "member_id"])
                status = _phase3_str(raw.get("status") or raw.get("state"))
                start_at = _phase3_dt_string(raw.get("start_at") or raw.get("from") or raw.get("start"))
                end_at = _phase3_dt_string(raw.get("end_at") or raw.get("to") or raw.get("end"))
                cur.execute("""
                    INSERT INTO phase3_absences (guild_id, absence_id, user_id, status, start_at_text, end_at_text, raw_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, absence_id) DO UPDATE SET
                      user_id=EXCLUDED.user_id,
                      status=EXCLUDED.status,
                      start_at_text=EXCLUDED.start_at_text,
                      end_at_text=EXCLUDED.end_at_text,
                      raw_json=EXCLUDED.raw_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, absence_id, user_id, status, start_at, end_at, _phase3_jsonb(raw)))
                counts["absences"] += 1

            # Settings snapshot copy
            settings_candidates = {}
            for key in ["settings", "dkp_settings", "config", "cfg"]:
                if isinstance(snap, dict) and isinstance(snap.get(key), dict):
                    settings_candidates[key] = snap.get(key)
            for key, value in settings_candidates.items():
                cur.execute("""
                    INSERT INTO phase3_settings (guild_id, setting_key, value_json, source, updated_at)
                    VALUES (%s,%s,%s,'snapshot',now())
                    ON CONFLICT (guild_id, setting_key) DO UPDATE SET
                      value_json=EXCLUDED.value_json,
                      source='snapshot',
                      updated_at=now()
                """, (guild_id, key, _phase3_jsonb(value)))
                counts["settings"] += 1

            cur.execute("""
                INSERT INTO phase3_migration_runs (run_id, guild_id, mode, status, counts_json, notes, created_at)
                VALUES (%s,%s,'snapshot_mirror','done',%s,%s,now())
            """, (run_id, guild_id, _phase3_jsonb(counts), "Sichere Spiegelung aus Dashboard-Snapshot. JSON bleibt Hauptquelle."))
        conn.commit()
        return {"ok": True, "run_id": run_id, "guild_id": guild_id, "counts": counts}
    except Exception as exc:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO phase3_migration_runs (run_id, guild_id, mode, status, counts_json, notes, created_at)
                    VALUES (%s,%s,'snapshot_mirror','failed',%s,%s,now())
                    ON CONFLICT (run_id) DO NOTHING
                """, (run_id, guild_id, _phase3_jsonb(counts), f"{type(exc).__name__}: {exc}"))
            conn.commit()
        except Exception:
            conn.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "run_id": run_id, "counts": counts}
    finally:
        conn.close()



def _phase3_ec_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Phase 3.2: EC/DKP Vergleich. Dashboard-Snapshot ist nur Auszug; Postgres ist Bot-Spiegelung."""
    snap_counts = _phase3_extract_snapshot_counts(payload)
    db_status = _phase3_status_payload()
    counts = db_status.get("counts") or {}
    warnings: list[str] = []
    ec_pairs = {
        "ec_balances": (snap_counts.get("ec_balances", 0), int(counts.get("phase3_ec_balances", 0) or 0)),
        "ec_transactions": (snap_counts.get("ec_transactions", 0), int(counts.get("phase3_ec_transactions", 0) or 0)),
        "ec_event_checks": ("—", int(counts.get("phase3_ec_event_checks", 0) or 0)),
    }
    if not db_status.get("tables", {}).get("phase3_ec_event_checks"):
        warnings.append("Tabelle phase3_ec_event_checks fehlt noch. Bitte Tabellen vorbereiten ausführen.")
    if not _database_url():
        warnings.append("DATABASE_URL fehlt. Postgres kann nicht genutzt werden.")
    warnings.append("Hinweis: Dashboard-Snapshot ist kein vollständiges JSON. Für EC zählt die Bot-Spiegelung in Postgres, nicht der kleine Snapshot-Auszug.")
    ready = bool(_database_url()) and db_status.get("tables", {}).get("phase3_ec_balances") and db_status.get("tables", {}).get("phase3_ec_transactions")
    return {"ok": bool(ready), "pairs": ec_pairs, "database": db_status, "warnings": warnings}


def _render_phase3_ec_panel(payload: dict[str, Any]) -> str:
    info = _phase3_ec_status_payload(payload)
    pairs = info.get("pairs") or {}
    rows = [
        ["EC-Konten", pairs.get("ec_balances", (0, 0))[0], pairs.get("ec_balances", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["EC-Verlauf", pairs.get("ec_transactions", (0, 0))[0], pairs.get("ec_transactions", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["EC-Eventchecks", "—", pairs.get("ec_event_checks", ("—", 0))[1], "DB-Prüftabelle"],
    ]
    warnings = info.get("warnings") or []
    warn_html = "" if not warnings else "<div class='notice'><b>Hinweise</b><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></div>"
    return f"""
    <section class='panel'>
      <h2>Phase 3.2 · EC/DKP</h2>
      <p>EC läuft im Dashboard Postgres-first. Der Bot spiegelt EC-Konten, Transaktionen und Eventchecks nach jedem Speichern in Postgres.</p>
      <div class='grid'>
        <div class='card'><b>Dashboard-Snapshot</b><p>Kann unvollständig sein und zeigt nur exportierte Ausschnitte.</p></div>
        <div class='card'><b>Postgres</b><p>Enthält die vom Bot gespiegelten EC-Konten und EC-Verläufe.</p></div>
        <div class='card'><b>Read-Cutover aktiv</b><p>Dashboard nutzt Postgres, wenn die Phase-3-Tabellen gefüllt sind.</p></div>
        <div class='card'><b>JSON bleibt sicher</b><p>Alte JSON-Daten werden nicht gelöscht.</p></div>
      </div>
      {_table(['Bereich','Dashboard-Snapshot','Postgres/Bot-Spiegelung','Status'], rows, searchable=False)}
      {warn_html}
    </section>
    """


def _phase3_loot_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Phase 3.3: Loot/Needs/Auktionen/Gebote als Postgres-Spiegelung prüfen."""
    snap_counts = _phase3_extract_snapshot_counts(payload)
    db_status = _phase3_status_payload()
    counts = db_status.get("counts") or {}
    pairs = {
        "needs": (snap_counts.get("needs", 0), int(counts.get("phase3_loot_needs", 0) or 0)),
        "auctions": (snap_counts.get("auctions", 0), int(counts.get("phase3_loot_auctions", 0) or 0)),
        "bids": ("—", int(counts.get("phase3_loot_bids", 0) or 0)),
        "history": (snap_counts.get("loot_history", 0), int(counts.get("phase3_loot_history", 0) or 0)),
        "need_log": ("—", int(counts.get("phase3_need_change_log", 0) or 0)),
    }
    warnings: list[str] = []
    if not db_status.get("tables", {}).get("phase3_loot_needs"):
        warnings.append("Tabelle phase3_loot_needs fehlt noch. Bitte Tabellen vorbereiten ausführen.")
    if not db_status.get("tables", {}).get("phase3_loot_auctions"):
        warnings.append("Tabelle phase3_loot_auctions fehlt noch. Bitte Tabellen vorbereiten ausführen.")
    if not _database_url():
        warnings.append("DATABASE_URL fehlt. Postgres kann nicht genutzt werden.")
    warnings.append("Hinweis: Auch hier ist der Dashboard-Snapshot nur ein Ausschnitt. Für Phase 3.3 zählt die Bot-Spiegelung in Postgres.")
    ready = bool(_database_url()) and db_status.get("tables", {}).get("phase3_loot_needs") and db_status.get("tables", {}).get("phase3_loot_auctions")
    return {"ok": bool(ready), "pairs": pairs, "database": db_status, "warnings": warnings}


def _render_phase3_loot_panel(payload: dict[str, Any]) -> str:
    info = _phase3_loot_status_payload(payload)
    pairs = info.get("pairs") or {}
    rows = [
        ["Need-Einträge", pairs.get("needs", (0, 0))[0], pairs.get("needs", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["Auktionen", pairs.get("auctions", (0, 0))[0], pairs.get("auctions", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["Gebote", "—", pairs.get("bids", ("—", 0))[1], "DB-Prüftabelle"],
        ["Loot-Historie", pairs.get("history", (0, 0))[0], pairs.get("history", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["Need-Änderungslog", "—", pairs.get("need_log", ("—", 0))[1], "DB-Prüftabelle"],
    ]
    warnings = info.get("warnings") or []
    warn_html = "" if not warnings else "<div class='notice'><b>Hinweise</b><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></div>"
    return f"""
    <section class='panel'>
      <h2>Phase 3.3 · Loot / Needs / Auktionen</h2>
      <p>Loot/Needs/Auktionen laufen im Dashboard Postgres-first. Bot speichert JSON und spiegelt Needs, Auktionen, Gebote und Historie nach Postgres.</p>
      <div class='grid'>
        <div class='card'><b>Needs</b><p>Aktuelle Need-Slots werden in phase3_loot_needs gespiegelt.</p></div>
        <div class='card'><b>Auktionen</b><p>Auktionen und Status landen in phase3_loot_auctions.</p></div>
        <div class='card'><b>Gebote</b><p>Gebotslisten landen einzeln in phase3_loot_bids.</p></div>
        <div class='card'><b>Historie</b><p>Gewinner/Sale/Müll und Need-Änderungen werden prüfbar.</p></div>
      </div>
      {_table(['Bereich','Dashboard-Snapshot','Postgres/Bot-Spiegelung','Status'], rows, searchable=False)}
      {warn_html}
    </section>
    """


def _phase3_live_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Phase 3.4: Events, Profile/Mitglieder und Abwesenheiten prüfen."""
    snap_counts = _phase3_extract_snapshot_counts(payload)
    db_status = _phase3_status_payload()
    counts = db_status.get("counts") or {}
    pairs = {
        "members": (snap_counts.get("members", 0), int(counts.get("phase3_members", 0) or 0)),
        "events": (snap_counts.get("events", 0), int(counts.get("phase3_events", 0) or 0)),
        "event_rsvps": (snap_counts.get("event_rsvps", 0), int(counts.get("phase3_event_rsvps", 0) or 0)),
        "absences": (snap_counts.get("absences", 0), int(counts.get("phase3_absences", 0) or 0)),
    }
    warnings: list[str] = []
    if not db_status.get("tables", {}).get("phase3_members"):
        warnings.append("Tabelle phase3_members fehlt noch. Bitte Tabellen vorbereiten ausführen.")
    if not db_status.get("tables", {}).get("phase3_event_rsvps"):
        warnings.append("Tabelle phase3_event_rsvps fehlt noch. Bitte Tabellen vorbereiten ausführen.")
    if not _database_url():
        warnings.append("DATABASE_URL fehlt. Postgres kann nicht genutzt werden.")
    warnings.append("Hinweis: Dashboard liest Events/Profile Postgres-first. JSON bleibt lokale Bot-Sicherheitskopie und Fallback.")
    ready = bool(_database_url()) and db_status.get("tables", {}).get("phase3_members") and db_status.get("tables", {}).get("phase3_events") and db_status.get("tables", {}).get("phase3_event_rsvps") and db_status.get("tables", {}).get("phase3_absences")
    return {"ok": bool(ready), "pairs": pairs, "database": db_status, "warnings": warnings}


def _render_phase3_live_panel(payload: dict[str, Any]) -> str:
    info = _phase3_live_status_payload(payload)
    pairs = info.get("pairs") or {}
    rows = [
        ["Mitglieder/Profile", pairs.get("members", (0, 0))[0], pairs.get("members", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["Events", pairs.get("events", (0, 0))[0], pairs.get("events", (0, 0))[1], "ℹ️ Postgres prüfen"],
        ["Event-Teilnehmer/RSVPs", pairs.get("event_rsvps", (0, 0))[0], pairs.get("event_rsvps", (0, 0))[1], "DB-Prüftabelle"],
        ["Abwesenheiten", pairs.get("absences", (0, 0))[0], pairs.get("absences", (0, 0))[1], "ℹ️ Postgres prüfen"],
    ]
    warnings = info.get("warnings") or []
    warn_html = "" if not warnings else "<div class='notice'><b>Hinweise</b><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></div>"
    return f"""
    <section class='panel'>
      <h2>Phase 3.4 · Events / Profile / Abwesenheiten</h2>
      <p>Events/Profile/Abwesenheiten werden nach Postgres gespiegelt. Das Dashboard liest diese Bereiche bevorzugt aus Postgres.</p>
      <div class='grid'>
        <div class='card'><b>Profile</b><p>Mitglieder/Profile werden aus Snapshot-Profilen und Insights zusammengeführt.</p></div>
        <div class='card'><b>Events</b><p>Event-Stammdaten landen in phase3_events.</p></div>
        <div class='card'><b>Teilnehmer</b><p>RSVPs/Rollenverteilung werden in phase3_event_rsvps prüfbar.</p></div>
        <div class='card'><b>Abwesenheiten</b><p>Abwesenheiten werden vorbereitet und gespiegelt, sobald Einträge vorhanden sind.</p></div>
      </div>
      {_table(['Bereich','Dashboard-Snapshot','Postgres/Bot-Spiegelung','Status'], rows, searchable=False)}
      {warn_html}
    </section>
    """

def _phase3_cutover_summary(payload: dict[str, Any]) -> dict[str, Any]:
    ec = payload.get("phase3_ec_read_cutover") or {}
    loot = payload.get("phase3_loot_read_cutover") or {}
    events = payload.get("phase3_events_read_cutover") or {}
    active = {
        "ec": bool(ec.get("active")),
        "loot": bool(loot.get("active")),
        "events": bool(events.get("active")),
    }
    return {
        "ok": all(active.values()),
        "database_url": bool(_database_url()),
        "read_mode": "postgres_first",
        "bot_write_mode": "json_plus_postgres_mirror",
        "json_role": "backup_fallback_and_bot_local_store",
        "active": active,
        "sources": {
            "ec": ec.get("source") or "snapshot_fallback",
            "loot": loot.get("source") or "snapshot_fallback",
            "events": events.get("source") or "snapshot_fallback",
        },
        "counts": {
            "ec_balances": ec.get("balances"),
            "loot_needs": loot.get("need_entries"),
            "loot_auctions": loot.get("auctions"),
            "members": events.get("members"),
            "events": events.get("events"),
            "event_rsvps": events.get("event_rsvps"),
        },
        "warnings": [
            msg for ok, msg in [
                (active["ec"], "EC/DKP liest noch Snapshot-Fallback."),
                (active["loot"], "Loot/Needs/Auktionen liest noch Snapshot-Fallback."),
                (active["events"], "Events/Profile liest noch Snapshot-Fallback."),
            ] if not ok
        ],
    }


def _render_phase3_database_page(payload: dict[str, Any]) -> str:
    cutover = _phase3_cutover_summary(payload)
    snap_counts = _phase3_extract_snapshot_counts(payload)
    db_status = _phase3_status_payload()
    table_rows = []
    for table in PHASE3_TABLES:
        table_rows.append([
            table,
            "✅ vorhanden" if db_status.get("tables", {}).get(table) else "⚠️ fehlt",
            db_status.get("counts", {}).get(table, 0),
        ])
    compare_rows = [
        ["Mitglieder/Profile", snap_counts.get("members", 0), db_status.get("counts", {}).get("phase3_members", 0)],
        ["EC-Konten", snap_counts.get("ec_balances", 0), db_status.get("counts", {}).get("phase3_ec_balances", 0)],
        ["EC-Verlauf", snap_counts.get("ec_transactions", 0), db_status.get("counts", {}).get("phase3_ec_transactions", 0)],
        ["EC-Eventchecks", "—", db_status.get("counts", {}).get("phase3_ec_event_checks", 0)],
        ["Need-Einträge", snap_counts.get("needs", 0), db_status.get("counts", {}).get("phase3_loot_needs", 0)],
        ["Events", snap_counts.get("events", 0), db_status.get("counts", {}).get("phase3_events", 0)],
        ["Event-Teilnehmer/RSVPs", snap_counts.get("event_rsvps", 0), db_status.get("counts", {}).get("phase3_event_rsvps", 0)],
        ["Auktionen", snap_counts.get("auctions", 0), db_status.get("counts", {}).get("phase3_loot_auctions", 0)],
        ["Loot-Historie", snap_counts.get("loot_history", 0), db_status.get("counts", {}).get("phase3_loot_history", 0)],
        ["Need-Änderungslog", "—", db_status.get("counts", {}).get("phase3_need_change_log", 0)],
        ["Abwesenheiten", snap_counts.get("absences", 0), db_status.get("counts", {}).get("phase3_absences", 0)],
    ]
    run_rows = []
    for r in db_status.get("latest_runs") or []:
        counts = r.get("counts_json") or {}
        if not isinstance(counts, dict):
            counts = {}
        run_rows.append([
            r.get("created_at") or "",
            r.get("mode") or "",
            r.get("status") or "",
            ", ".join(f"{k}:{v}" for k, v in counts.items())[:160],
            r.get("notes") or "",
        ])
    warnings = db_status.get("warnings") or []
    warn_html = "" if not warnings else "<section class='panel'><h2>Warnungen</h2><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></section>"
    status_text = "✅ Online-Datenbank aktiv" if cutover.get("ok") else ("✅ Datenbank bereit" if db_status.get("ok") else "⚠️ Datenbank vorbereiten")
    cut_rows = [
        ["EC/DKP", "✅ Postgres" if (cutover.get("active") or {}).get("ec") else "⚠️ Fallback", (cutover.get("sources") or {}).get("ec", "—"), (cutover.get("counts") or {}).get("ec_balances") or 0],
        ["Loot/Needs/Auktionen", "✅ Postgres" if (cutover.get("active") or {}).get("loot") else "⚠️ Fallback", (cutover.get("sources") or {}).get("loot", "—"), (cutover.get("counts") or {}).get("loot_needs") or 0],
        ["Events/Profile", "✅ Postgres" if (cutover.get("active") or {}).get("events") else "⚠️ Fallback", (cutover.get("sources") or {}).get("events", "—"), (cutover.get("counts") or {}).get("members") or 0],
    ]
    cut_warn = cutover.get("warnings") or []
    cut_warn_html = "" if not cut_warn else "<div class='notice'><b>Cutover-Hinweise</b><ul>" + "".join(f"<li>{_e(w)}</li>" for w in cut_warn) + "</ul></div>"
    return _html_shell("Phase 3 · Datenbank · Ebo Dashboard", f"""
    <nav class='topnav'><a href='/'>← Kommando</a><a href='/release'>Release</a><a href='/admin'>Admin</a><a href='/database-audit'>Cutover-Prüfung</a><a href='/api/database-cutover-status'>Cutover API</a><a href='/api/database-status'>Status API</a><a href='/api/database-live-status'>Live API</a></nav>
    <section class='hero'><div><h1>Phase 3.9 · Online-Datenbank</h1><p>{_e(status_text)} · Dashboard liest Postgres-first. Bot schreibt sicher weiter lokal JSON und spiegelt direkt nach Postgres, damit JSON als Backup/Fallback erhalten bleibt.</p></div><div class='page-actions'><a class='btn' href='/database/init'>Tabellen vorbereiten</a><a class='btn' href='/database/mirror-snapshot'>Snapshot nachspiegeln</a></div></section>
    <section class='panel'><h2>Cutover-Stand</h2><p>Das ist der relevante Zustand: Nicht ob JSON-Dateien noch existieren, sondern ob das Dashboard die Live-Bereiche aus Postgres liest.</p>{_table(['Bereich','Status','Quelle','Zeilen/Einträge'], cut_rows, searchable=False)}{cut_warn_html}</section>
    <section class='panel'><h2>Jetzt gültige Architektur</h2><div class='grid'>
      <div class='card'><b>Dashboard</b><p>Liest EC, Loot, Events und Profile bevorzugt aus Postgres.</p></div>
      <div class='card'><b>Bot</b><p>Schreibt weiterhin JSON und spiegelt beim Speichern nach Postgres.</p></div>
      <div class='card'><b>Dashboard-Aktionen</b><p>EC, Gebote, Käufe, Würfe und Drops laufen über Postgres-Queues zum Bot.</p></div>
      <div class='card'><b>JSON</b><p>Bleibt Backup/Fallback und lokale Bot-Sicherheitskopie. Nicht löschen.</p></div>
    </div></section>
    {_render_phase3_ec_panel(payload)}
    {_render_phase3_loot_panel(payload)}
    {_render_phase3_live_panel(payload)}
    <section class='panel'><h2>Dashboard-Snapshot vs. Postgres</h2><p>Der Snapshot ist nur der exportierte Dashboard-Ausschnitt. Postgres zeigt die Phase-3-Spiegelung aus Bot/Dashboard.</p>{_table(['Bereich','Dashboard-Snapshot','Postgres Phase 3'], compare_rows, searchable=False)}</section>
    <section class='panel'><h2>Phase-3-Tabellen</h2>{_table(['Tabelle','Status','Zeilen'], table_rows, searchable=False)}</section>
    {warn_html}
    <section class='panel'><h2>Letzte Spiegelungen</h2>{_table(['Zeit','Modus','Status','Zahlen','Notiz'], run_rows or [['—','—','—','Noch keine Spiegelung','']], searchable=False)}</section>
    <section class='panel'><h2>Sicherheitsregeln</h2><ul><li>Diese Seite löscht keine Daten.</li><li>Dashboard ist Postgres-first, sobald die Phase-3-Tabellen Daten liefern.</li><li>JSON bleibt bewusst erhalten und darf nicht überschrieben oder gelöscht werden.</li><li>Spiegelung ist idempotent: erneutes Ausführen aktualisiert vorhandene DB-Zeilen.</li></ul></section>
    """)


@app.get("/database", response_class=HTMLResponse)
def database_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_phase3_database_page(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(_html_shell("Datenbank Fehler", f"<section class='panel'><h1>❌ Datenbank-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/database-status")
def api_database_status(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    return JSONResponse(_json_safe({"ok": True, "snapshot_counts": _phase3_extract_snapshot_counts(payload), "cutover": _phase3_cutover_summary(payload), "database": _phase3_status_payload()}))


@app.get("/api/database-cutover-status")
def api_database_cutover_status(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    return JSONResponse(_json_safe(_phase3_cutover_summary(payload)))


@app.get("/api/database-ec-status")
def api_database_ec_status(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    return JSONResponse(_json_safe(_phase3_ec_status_payload(payload)))


@app.get("/api/database-loot-status")
def api_database_loot_status(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    return JSONResponse(_json_safe(_phase3_loot_status_payload(payload)))


@app.get("/api/database-live-status")
def api_database_live_status(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    return JSONResponse(_json_safe(_phase3_live_status_payload(payload)))


@app.get("/database/init")
def database_init(_: bool = Depends(_auth)):
    res = _phase3_ensure_schema()
    msg = "Phase-3-Tabellen vorbereitet." if res.get("ok") else f"Fehler: {res.get('error')}"
    return RedirectResponse("/database?msg=" + urllib.parse.quote(msg), status_code=303)


@app.get("/database/mirror-snapshot")
def database_mirror_snapshot(_: bool = Depends(_auth)):
    res = _phase3_mirror_snapshot(_snapshot_payload())
    if res.get("ok"):
        msg = "Snapshot gespiegelt: " + ", ".join(f"{k}={v}" for k, v in (res.get("counts") or {}).items())
    else:
        msg = f"Fehler: {res.get('error')}"
    return RedirectResponse("/database?msg=" + urllib.parse.quote(msg), status_code=303)


# ---------------------------------------------------------------------------
# Phase 3.5 - Cutover-/Datenprüfung
# ---------------------------------------------------------------------------

def _phase3_fetch_rows(cur, sql: str, params: tuple[Any, ...] = (), limit: int = 50) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    return [dict(r) for r in rows[:limit]]


def _phase3_count_query(cur, sql: str, params: tuple[Any, ...] = ()) -> int:
    cur.execute(sql, params)
    return int((cur.fetchone() or {}).get("c") or 0)


def _phase3_audit_payload() -> dict[str, Any]:
    out: dict[str, Any] = {
        "ok": False,
        "ready": False,
        "warnings": [],
        "blockers": [],
        "counts": {},
        "orphans": {},
        "samples": {},
        "notes": [],
    }
    if not _database_url():
        out["blockers"].append("DATABASE_URL fehlt.")
        return out
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            # Schema sicherstellen, damit die Prüfung nicht an fehlenden Tabellen scheitert.
            schema = _phase3_ensure_schema()
            if not schema.get("ok"):
                out["blockers"].append(f"Tabellen konnten nicht vorbereitet werden: {schema.get('error')}")

            for table in [
                "phase3_members", "phase3_ec_balances", "phase3_ec_transactions", "phase3_ec_event_checks",
                "phase3_loot_needs", "phase3_loot_auctions", "phase3_loot_bids", "phase3_loot_history",
                "phase3_event_rsvps", "phase3_absences", "phase3_need_change_log",
            ]:
                cur.execute("SELECT to_regclass(%s) AS reg", (table,))
                exists = bool((cur.fetchone() or {}).get("reg"))
                out["counts"][table + "_exists"] = 1 if exists else 0
                if exists:
                    cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
                    out["counts"][table] = int((cur.fetchone() or {}).get("c") or 0)
                else:
                    out["counts"][table] = 0
                    out["blockers"].append(f"Tabelle fehlt: {table}")

            # Kernprüfungen: Datensätze, deren user_id nicht in phase3_members existiert.
            checks = {
                "ec_balances_without_member": """
                    SELECT b.user_id, b.balance, b.source, b.updated_at::text AS updated_at
                    FROM phase3_ec_balances b
                    LEFT JOIN phase3_members m ON m.guild_id=b.guild_id AND m.user_id=b.user_id
                    WHERE COALESCE(b.user_id,'') <> '' AND m.user_id IS NULL
                    ORDER BY b.balance DESC, b.user_id
                """,
                "ec_transactions_without_member": """
                    SELECT t.user_id, COUNT(*) AS entries, COALESCE(SUM(t.amount),0) AS total_amount, MAX(t.mirrored_at)::text AS last_seen
                    FROM phase3_ec_transactions t
                    LEFT JOIN phase3_members m ON m.guild_id=t.guild_id AND m.user_id=t.user_id
                    WHERE COALESCE(t.user_id,'') <> '' AND m.user_id IS NULL
                    GROUP BY t.user_id
                    ORDER BY entries DESC, total_amount DESC
                """,
                "needs_without_member": """
                    SELECT n.user_id, COUNT(*) AS entries, STRING_AGG(DISTINCT COALESCE(n.item_name,'?'), ', ' ORDER BY COALESCE(n.item_name,'?')) AS items
                    FROM phase3_loot_needs n
                    LEFT JOIN phase3_members m ON m.guild_id=n.guild_id AND m.user_id=n.user_id
                    WHERE COALESCE(n.user_id,'') <> '' AND m.user_id IS NULL
                    GROUP BY n.user_id
                    ORDER BY entries DESC, n.user_id
                """,
                "bids_without_member": """
                    SELECT b.user_id, COUNT(*) AS entries, COALESCE(SUM(b.amount),0) AS total_bid
                    FROM phase3_loot_bids b
                    LEFT JOIN phase3_members m ON m.guild_id=b.guild_id AND m.user_id=b.user_id
                    WHERE COALESCE(b.user_id,'') <> '' AND m.user_id IS NULL
                    GROUP BY b.user_id
                    ORDER BY entries DESC, total_bid DESC
                """,
                "loot_history_without_member": """
                    SELECT h.user_id, COUNT(*) AS entries, COALESCE(SUM(h.amount),0) AS total_amount
                    FROM phase3_loot_history h
                    LEFT JOIN phase3_members m ON m.guild_id=h.guild_id AND m.user_id=h.user_id
                    WHERE COALESCE(h.user_id,'') <> '' AND m.user_id IS NULL
                    GROUP BY h.user_id
                    ORDER BY entries DESC, total_amount DESC
                """,
                "event_rsvps_without_member": """
                    SELECT r.user_id, COUNT(*) AS entries, STRING_AGG(DISTINCT COALESCE(r.response,'?'), ', ' ORDER BY COALESCE(r.response,'?')) AS responses
                    FROM phase3_event_rsvps r
                    LEFT JOIN phase3_members m ON m.guild_id=r.guild_id AND m.user_id=r.user_id
                    WHERE COALESCE(r.user_id,'') <> '' AND m.user_id IS NULL
                    GROUP BY r.user_id
                    ORDER BY entries DESC, r.user_id
                """,
            }
            for key, sql in checks.items():
                rows = _phase3_fetch_rows(cur, sql)
                out["orphans"][key] = len(rows)
                out["samples"][key] = rows[:20]

            # Leere/komische IDs separat anzeigen.
            empty_checks = {
                "ec_balances_empty_user": "SELECT COUNT(*) AS c FROM phase3_ec_balances WHERE COALESCE(user_id,'')=''",
                "ec_transactions_empty_user": "SELECT COUNT(*) AS c FROM phase3_ec_transactions WHERE COALESCE(user_id,'')=''",
                "needs_empty_user": "SELECT COUNT(*) AS c FROM phase3_loot_needs WHERE COALESCE(user_id,'')=''",
                "rsvps_empty_user": "SELECT COUNT(*) AS c FROM phase3_event_rsvps WHERE COALESCE(user_id,'')=''",
                "bids_empty_user": "SELECT COUNT(*) AS c FROM phase3_loot_bids WHERE COALESCE(user_id,'')=''",
            }
            for key, sql in empty_checks.items():
                out["counts"][key] = _phase3_count_query(cur, sql)

            # Events ohne Teilnehmer sind nicht zwingend Fehler, aber für Cutover prüfenswert.
            event_no_rsvp = _phase3_fetch_rows(cur, """
                SELECT e.event_id, e.title, e.status, e.start_at_text
                FROM phase3_events e
                LEFT JOIN phase3_event_rsvps r ON r.guild_id=e.guild_id AND r.event_id=e.event_id
                GROUP BY e.guild_id, e.event_id, e.title, e.status, e.start_at_text
                HAVING COUNT(r.rsvp_id)=0
                ORDER BY e.start_at_text DESC NULLS LAST, e.title
            """)
            out["orphans"]["events_without_rsvps"] = len(event_no_rsvp)
            out["samples"]["events_without_rsvps"] = event_no_rsvp[:20]

            member_count = int(out["counts"].get("phase3_members") or 0)
            ec_count = int(out["counts"].get("phase3_ec_balances") or 0)
            if member_count <= 0:
                out["blockers"].append("Keine Mitglieder in phase3_members. Profile/Mitglieder müssen vor Cutover gespiegelt sein.")
            if ec_count > member_count and member_count > 0:
                out["blockers"].append(f"EC-Konten ({ec_count}) sind mehr als Mitglieder/Profile ({member_count}).")
            if out["orphans"].get("ec_balances_without_member"):
                out["blockers"].append(f"{out['orphans']['ec_balances_without_member']} EC-Konten ohne aktuelles Mitglied.")
            if out["orphans"].get("event_rsvps_without_member"):
                out["warnings"].append(f"{out['orphans']['event_rsvps_without_member']} RSVP-Spieler ohne aktuelles Mitglied.")
            if out["orphans"].get("needs_without_member"):
                out["warnings"].append(f"{out['orphans']['needs_without_member']} Need-Spieler ohne aktuelles Mitglied.")
            if out["counts"].get("phase3_loot_needs", 0) <= 0:
                out["warnings"].append("Keine Need-Einträge in phase3_loot_needs.")
            if out["counts"].get("phase3_event_rsvps", 0) <= 0 and out["counts"].get("phase3_events", 0) > 0:
                out["warnings"].append("Events vorhanden, aber keine RSVPs gespiegelt.")

            out["notes"].append("Diese Prüfung löscht nichts. Sie zeigt nur, ob ein harter Cutover schon sinnvoll wäre.")
            out["notes"].append("JSON bleibt Hauptquelle, bis die Blocker weg sind.")
            out["ready"] = not out["blockers"]
            out["ok"] = True
            return out
    except Exception as exc:
        out["blockers"].append(f"{type(exc).__name__}: {exc}")
        return out
    finally:
        conn.close()


def _phase3_audit_table(title: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return f"<section class='panel'><h2>{_e(title)}</h2><p class='muted'>Keine Treffer.</p></section>"
    table_rows = []
    for row in rows:
        table_rows.append([row.get(key, "") for key, _label in columns])
    return f"<section class='panel'><h2>{_e(title)}</h2>{_table([label for _key, label in columns], table_rows, searchable=True)}</section>"


def _render_phase3_audit_page() -> str:
    audit = _phase3_audit_payload()
    counts = audit.get("counts") or {}
    orphans = audit.get("orphans") or {}
    samples = audit.get("samples") or {}
    status_label = "✅ Cutover grundsätzlich möglich" if audit.get("ready") else "⚠️ Noch nicht Cutover-bereit"
    status_hint = "Keine harten Blocker gefunden." if audit.get("ready") else "Erst die Blocker prüfen. JSON bleibt Hauptquelle."
    cards = [
        ["Mitglieder", counts.get("phase3_members", 0), "phase3_members"],
        ["EC-Konten", counts.get("phase3_ec_balances", 0), "phase3_ec_balances"],
        ["EC ohne Mitglied", orphans.get("ec_balances_without_member", 0), "muss 0 sein"],
        ["RSVPs ohne Mitglied", orphans.get("event_rsvps_without_member", 0), "prüfen"],
        ["Need-Spieler ohne Mitglied", orphans.get("needs_without_member", 0), "prüfen"],
        ["Events ohne RSVPs", orphans.get("events_without_rsvps", 0), "kann okay sein"],
    ]
    card_html = "".join(f"<div class='card'><small>{_e(str(label))}</small><div class='metric'>{_e(str(value))}</div><p>{_e(str(hint))}</p></div>" for label, value, hint in cards)
    blockers = audit.get("blockers") or []
    warnings = audit.get("warnings") or []
    block_html = "" if not blockers else "<section class='panel'><h2>Blocker</h2><ul>" + "".join(f"<li>{_e(b)}</li>" for b in blockers) + "</ul></section>"
    warn_html = "" if not warnings else "<section class='panel'><h2>Warnungen</h2><ul>" + "".join(f"<li>{_e(w)}</li>" for w in warnings) + "</ul></section>"
    count_rows = [
        ["Mitglieder/Profile", counts.get("phase3_members", 0)],
        ["EC-Konten", counts.get("phase3_ec_balances", 0)],
        ["EC-Transaktionen", counts.get("phase3_ec_transactions", 0)],
        ["EC-Eventchecks", counts.get("phase3_ec_event_checks", 0)],
        ["Need-Einträge", counts.get("phase3_loot_needs", 0)],
        ["Auktionen", counts.get("phase3_loot_auctions", 0)],
        ["Gebote", counts.get("phase3_loot_bids", 0)],
        ["Loot-Historie", counts.get("phase3_loot_history", 0)],
        ["Events", counts.get("phase3_events", 0)],
        ["Event-RSVPs", counts.get("phase3_event_rsvps", 0)],
        ["Abwesenheiten", counts.get("phase3_absences", 0)],
    ]
    empty_rows = [
        ["EC-Konten ohne User-ID", counts.get("ec_balances_empty_user", 0)],
        ["EC-Transaktionen ohne User-ID", counts.get("ec_transactions_empty_user", 0)],
        ["Needs ohne User-ID", counts.get("needs_empty_user", 0)],
        ["RSVPs ohne User-ID", counts.get("rsvps_empty_user", 0)],
        ["Gebote ohne User-ID", counts.get("bids_empty_user", 0)],
    ]
    return _html_shell("Phase 3.5 · Cutover-Prüfung · Ebo Dashboard", f"""
    <nav class='topnav'><a href='/database'>← Datenbank</a><a href='/'>Kommando</a><a href='/release'>Release</a><a href='/api/database-audit'>API</a></nav>
    <section class='hero'><div><h1>Phase 3.5 · Cutover-Prüfung</h1><p>{_e(status_label)} · { _e(status_hint) }</p></div><div class='page-actions'><a class='btn' href='/database'>Datenbank</a><a class='btn' href='/api/database-audit'>API öffnen</a></div></section>
    <section class='panel'><h2>Bereitschaft</h2><div class='grid'>{card_html}</div></section>
    {block_html}
    {warn_html}
    <section class='panel'><h2>Phase-3-Zahlen</h2>{_table(['Bereich','Postgres'], count_rows, searchable=False)}</section>
    <section class='panel'><h2>Leere User-IDs</h2>{_table(['Bereich','Anzahl'], empty_rows, searchable=False)}</section>
    {_phase3_audit_table('EC-Konten ohne aktuelles Mitglied', samples.get('ec_balances_without_member') or [], [('user_id','User-ID'), ('balance','EC'), ('source','Quelle'), ('updated_at','Aktualisiert')])}
    {_phase3_audit_table('EC-Transaktionen ohne aktuelles Mitglied', samples.get('ec_transactions_without_member') or [], [('user_id','User-ID'), ('entries','Buchungen'), ('total_amount','Summe'), ('last_seen','Zuletzt')])}
    {_phase3_audit_table('Need-Einträge ohne aktuelles Mitglied', samples.get('needs_without_member') or [], [('user_id','User-ID'), ('entries','Einträge'), ('items','Items')])}
    {_phase3_audit_table('Gebote ohne aktuelles Mitglied', samples.get('bids_without_member') or [], [('user_id','User-ID'), ('entries','Gebote'), ('total_bid','Summe')])}
    {_phase3_audit_table('Loot-Historie ohne aktuelles Mitglied', samples.get('loot_history_without_member') or [], [('user_id','User-ID'), ('entries','Einträge'), ('total_amount','Summe')])}
    {_phase3_audit_table('Event-RSVPs ohne aktuelles Mitglied', samples.get('event_rsvps_without_member') or [], [('user_id','User-ID'), ('entries','Antworten'), ('responses','Status')])}
    {_phase3_audit_table('Events ohne RSVPs', samples.get('events_without_rsvps') or [], [('event_id','Event-ID'), ('title','Titel'), ('status','Status'), ('start_at_text','Start')])}
    <section class='panel'><h2>Sicherheitsregeln</h2><ul><li>Diese Seite löscht nichts.</li><li>Diese Seite stellt nichts auf Postgres um.</li><li>Cutover erst, wenn die Blocker geklärt sind.</li><li>JSON bleibt bis dahin Hauptquelle und Backup.</li></ul></section>
    """)


@app.get("/database-audit", response_class=HTMLResponse)
def database_audit_page(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_phase3_audit_page())
    except Exception as exc:
        return HTMLResponse(_html_shell("Cutover-Prüfung Fehler", f"<section class='panel'><h1>❌ Cutover-Prüfung Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"), status_code=500)


@app.get("/api/database-audit")
def api_database_audit(_: bool = Depends(_auth)):
    return JSONResponse(_phase3_audit_payload())
