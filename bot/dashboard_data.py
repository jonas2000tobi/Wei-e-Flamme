from __future__ import annotations

import json
import os
import asyncio
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

try:
    from bot import runtime_db  # type: ignore
except Exception:  # pragma: no cover - root fallback for old starts
    import runtime_db  # type: ignore

try:
    from bot import guild_config as central_guild_config  # type: ignore
except Exception:  # pragma: no cover
    try:
        import guild_config as central_guild_config  # type: ignore
    except Exception:
        central_guild_config = None  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = DATA_DIR / "dashboard_exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

DASHBOARD_SCHEMA_VERSION = 1

JSON_SOURCES: dict[str, str] = {
    "member_profiles": "member_profiles.json",
    "member_portal_cfg": "member_portal_cfg.json",
    "events": "event_rsvp.json",
    "event_attendance": "event_attendance.json",
    "dkp_cfg": "dkp_cfg.json",
    "dkp_balances": "dkp_balances.json",
    "dkp_transactions": "dkp_transactions.json",
    "dkp_event_checks": "dkp_event_checks.json",
    "loot_items": "loot_items.json",
    "loot_needs": "loot_needs.json",
    "loot_cfg": "loot_cfg.json",
    "loot_auctions": "loot_auctions.json",
    "loot_auction_cfg": "loot_auction_cfg.json",
    "guild_chest": "guild_chest.json",
    "raid_templates": "raid_templates.json",
    "alliance_config": "alliance_config.json",
    "weekly_report_cfg": "weekly_report_cfg.json",
    "leader_contact_cfg": "leader_contact_cfg.json",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_text(value: Any, limit: int = 300) -> str:
    txt = str(value or "").replace("@", "@\u200b").strip()
    if len(txt) > limit:
        return txt[: limit - 1] + "…"
    return txt


def _stable_discord_media_url(value: Any) -> str:
    """Discord-Medien-URL inklusive Signatur behalten.

    Discord schützt Attachment-/Proxy-URLs inzwischen mit Query-Signaturen.
    Werden ``ex/is/hm`` entfernt, liefert Discord häufig 403/404. Frische URLs
    werden unten direkt aus dem Eventpost gelesen und bei jedem Snapshot erneuert.
    """
    url = str(value or "").strip()
    if not (url.startswith("https://") or url.startswith("http://")):
        return ""
    return url


def _event_image_url_from_raw(event: Any) -> str:
    """Titelbild aus alten und neuen Event-Strukturen lesen."""
    if not isinstance(event, dict):
        return ""

    candidates: list[Any] = []
    for key in (
        "image_url", "title_image_url", "event_image_url", "banner_url",
        "thumbnail_url", "cover_url", "image", "thumbnail", "banner", "cover",
    ):
        candidates.append(event.get(key))

    for key in ("embed", "discord_embed", "message_embed", "media"):
        nested = event.get(key)
        if isinstance(nested, dict):
            for sub_key in ("image_url", "thumbnail_url", "banner_url", "cover_url", "url", "proxy_url", "image", "thumbnail"):
                candidates.append(nested.get(sub_key))
        elif isinstance(nested, list):
            candidates.extend(nested[:4])

    attachments = event.get("attachments")
    if isinstance(attachments, list):
        candidates.extend(attachments[:6])

    def unpack(raw: Any) -> list[Any]:
        if isinstance(raw, dict):
            return [raw.get("url"), raw.get("proxy_url"), raw.get("image_url"), raw.get("thumbnail_url")]
        if isinstance(raw, list):
            out: list[Any] = []
            for item in raw[:6]:
                out.extend(unpack(item))
            return out
        return [raw]

    for candidate in candidates:
        for raw in unpack(candidate):
            url = _stable_discord_media_url(raw)
            if url:
                return url
    return ""


def _member_is_active(guild: discord.Guild, user_id: int) -> bool:
    """True, wenn der User aktuell auf diesem Discord-Server gefunden wird.

    Alte JSON-Daten behalten wir bewusst, aber fürs Dashboard sollen
    ehemalige Mitglieder standardmäßig nicht mehr in Listen/Counts auftauchen.
    """
    try:
        return guild.get_member(int(user_id)) is not None
    except Exception:
        return False


DASHBOARD_MEMBER_ROLE_SETTING = "dashboard_member_role_id"
DASHBOARD_ADMIN_ROLE_SETTING = "dashboard_admin_role_ids"
DASHBOARD_ALLOWED_ROLE_SETTING = "dashboard_allowed_role_ids"
DASHBOARD_NEWS_CHANNEL_NAME_SETTING = "dashboard_news_channel_name"
DASHBOARD_GUIDES_CHANNEL_NAME_SETTING = "dashboard_guides_channel_name"
DASHBOARD_ANNOUNCEMENTS_CHANNEL_NAME_SETTING = "dashboard_announcements_channel_name"


def _dashboard_member_role_config_value(guild_id: int) -> Any:
    """Serverbezogene Dashboard-Gildenrolle aus Postgres/Runtime-DB lesen."""
    try:
        return runtime_db.get_guild_setting(int(guild_id), DASHBOARD_MEMBER_ROLE_SETTING, None)
    except Exception:
        return None


def _dashboard_member_role(guild: discord.Guild) -> Optional[discord.Role]:
    """Rolle, die fürs Dashboard als echte Gildenmitgliedschaft zählt.

    Für Vermietung/Multi-Guild gibt es keinen festen Default wie `Ebolus`.
    Jede Gilde setzt ihre Mitgliederrolle aktiv über:

        /dashboard_set_member_role role:<Rolle>

    Optionaler Notfall-Fallback per Railway:
    - DASHBOARD_MEMBER_ROLE_ID=123...
    - DASHBOARD_MEMBER_ROLE_NAME=Gildenmitglied
    """
    raw_setting = _dashboard_member_role_config_value(guild.id)
    if raw_setting not in (None, ""):
        try:
            role = guild.get_role(int(raw_setting))
            if role is not None:
                return role
        except Exception:
            pass

    raw_role_id = os.getenv("DASHBOARD_MEMBER_ROLE_ID", "").strip()
    if raw_role_id:
        try:
            role = guild.get_role(int(raw_role_id))
            if role is not None:
                return role
        except Exception:
            pass

    # Kein Ebolus-Default mehr. Rollenname nur, wenn Betreiber ihn bewusst setzt.
    role_name = os.getenv("DASHBOARD_MEMBER_ROLE_NAME", "").strip().lower()
    if role_name:
        for role in getattr(guild, "roles", []):
            try:
                if str(getattr(role, "name", "")).strip().lower() == role_name:
                    return role
            except Exception:
                continue
    return None


def _dashboard_member_roles(guild: discord.Guild) -> list[discord.Role]:
    """Alle Rollen, die eine aktive Gildenmitgliedschaft abbilden.

    Die normale Mitgliederrolle bleibt die Hauptrolle. Zusätzlich zählen bewusst
    konfigurierte Leitungsrollen (Leader/Berater/Wächter), damit Führungskräfte
    nicht aus Mitgliederzahl, Dashboard und EC-Scope verschwinden, nur weil sie
    statt ``@MEMBER`` ausschließlich ``@LEITUNG`` tragen.
    """
    out: list[discord.Role] = []

    def add(role: Optional[discord.Role]) -> None:
        if role is not None and role not in out:
            out.append(role)

    add(_dashboard_member_role(guild))
    try:
        if central_guild_config is not None:
            for kind in ("leader", "advisor", "guardian"):
                for rid in central_guild_config.role_ids(int(guild.id), kind):
                    add(guild.get_role(int(rid)))
    except Exception:
        pass
    return out


def _dashboard_member_ids(guild: discord.Guild) -> set[int]:
    ids: set[int] = set()
    for role in _dashboard_member_roles(guild):
        for member in getattr(role, "members", []) or []:
            if not getattr(member, "id", None) or bool(getattr(member, "bot", False)):
                continue
            ids.add(int(member.id))
    # Absichtlich kein Fallback auf alle Servermitglieder. Gäste, Bewerber und
    # Allianzspieler dürfen nicht automatisch als Gildenmitglieder zählen.
    return ids


def _parse_role_id_values(raw: Any) -> list[int]:
    values: list[int] = []
    if isinstance(raw, list):
        seq = raw
    elif isinstance(raw, str):
        try:
            loaded = json.loads(raw)
            seq = loaded if isinstance(loaded, list) else [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]
        except Exception:
            seq = [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]
    elif raw not in (None, ""):
        seq = [raw]
    else:
        seq = []

    for item in seq:
        try:
            rid = int(item)
            if rid and rid not in values:
                values.append(rid)
        except Exception:
            continue
    return values


def _dashboard_role_config_values(guild_id: int, setting_name: str, env_name: str) -> list[int]:
    """Rollen-IDs fürs Dashboard aus Runtime-DB plus optionalem Railway-Fallback."""
    raw = None
    try:
        raw = runtime_db.get_guild_setting(int(guild_id), setting_name, None)
    except Exception as exc:
        print(f"⚠️ Dashboard-Rollen-Setting konnte nicht gelesen werden ({setting_name}): {exc!r}", flush=True)
        raw = None

    values = _parse_role_id_values(raw)

    env_ids = os.getenv(env_name, "").strip()
    for rid in _parse_role_id_values(env_ids):
        if rid not in values:
            values.append(rid)
    return values


def _portal_position_role_ids(guild_id: int) -> list[int]:
    """Leitungsrollen aus der Gildenzentrale lesen.

    Gildenmeister, Gildenberater und Gildenwächter sollen im Dashboard dieselben
    Adminrechte erhalten wie eine explizit gesetzte Dashboard-Adminrolle. Die
    Rollen werden aus ``member_portal_cfg.json`` gelesen, damit keine zweite
    Rollenpflege nötig ist.
    """
    try:
        raw = _load_json_file(JSON_SOURCES["member_portal_cfg"], {})
        scoped = raw.get(str(int(guild_id))) if isinstance(raw, dict) else {}
        if not isinstance(scoped, dict):
            scoped = {}
        roles = scoped.get("position_roles") or {}
        if not isinstance(roles, dict):
            return []
        out: list[int] = []
        for key in ("leader", "advisor", "guardian"):
            try:
                rid = int(roles.get(key, 0) or 0)
            except Exception:
                rid = 0
            if rid and rid not in out:
                out.append(rid)
        return out
    except Exception as exc:
        print(f"⚠️ Portal-Positionsrollen konnten nicht gelesen werden: {exc!r}", flush=True)
        return []


def _dashboard_admin_role_config_values(guild_id: int) -> list[int]:
    values = _dashboard_role_config_values(guild_id, DASHBOARD_ADMIN_ROLE_SETTING, "DASHBOARD_ADMIN_ROLE_IDS")
    for rid in _portal_position_role_ids(guild_id):
        if rid not in values:
            values.append(rid)
    return values


def _dashboard_allowed_role_config_values(guild_id: int) -> list[int]:
    return _dashboard_role_config_values(guild_id, DASHBOARD_ALLOWED_ROLE_SETTING, "DASHBOARD_ALLOWED_ROLE_IDS")


def _roles_from_ids(guild: discord.Guild, role_ids: list[int]) -> list[discord.Role]:
    out: list[discord.Role] = []
    for rid in role_ids:
        role = guild.get_role(int(rid))
        if role is not None and role not in out:
            out.append(role)
    return out


def _dashboard_admin_roles(guild: discord.Guild) -> list[discord.Role]:
    return _roles_from_ids(guild, _dashboard_admin_role_config_values(guild.id))


def _dashboard_allowed_roles(guild: discord.Guild) -> list[discord.Role]:
    return _roles_from_ids(guild, _dashboard_allowed_role_config_values(guild.id))


def _dashboard_auth_info(guild: discord.Guild) -> dict[str, Any]:
    """Auth-Lesemodell fürs Web-Dashboard.

    Wichtig: Die Website muss damit für den Discord-Login keine Rollen direkt
    über Discord OAuth abfragen. Der Bot ist ohnehin im Server und schreibt
    die erlaubten User-IDs in den Snapshot. Das ist robuster und vermeidet
    403-Probleme bei guilds.members.read.
    """
    member_role = _dashboard_member_role(guild)
    member_ids = set(_dashboard_member_ids(guild))
    member_id_strings = sorted(str(x) for x in member_ids)

    allowed_roles = _dashboard_allowed_roles(guild)
    allowed_role_rows: list[dict[str, Any]] = []
    role_allowed_ids: set[int] = set()
    for role in allowed_roles:
        members = [m for m in getattr(role, "members", []) if getattr(m, "id", None)]
        role_allowed_ids.update(int(m.id) for m in members)
        allowed_role_rows.append({
            "role_id": str(role.id),
            "role_name": str(role.name),
            "member_count": len(members),
        })

    allowed_ids = sorted(str(x) for x in (member_ids | role_allowed_ids))

    admin_roles = _dashboard_admin_roles(guild)
    admin_ids: set[int] = set()
    admin_role_rows: list[dict[str, Any]] = []
    for role in admin_roles:
        members = [m for m in getattr(role, "members", []) if getattr(m, "id", None)]
        admin_ids.update(int(m.id) for m in members)
        admin_role_rows.append({
            "role_id": str(role.id),
            "role_name": str(role.name),
            "member_count": len(members),
        })
    return {
        "mode": "snapshot_role_check",
        "member_role": {
            "role_id": str(member_role.id) if member_role else "",
            "role_name": str(member_role.name) if member_role else "",
            "member_count": len(member_ids),
            "member_ids": member_id_strings,
            "configured": member_role is not None,
        },
        "allowed_roles": allowed_role_rows,
        "admin_roles": admin_role_rows,
        "allowed_member_ids": allowed_ids,
        "admin_member_ids": sorted(str(x) for x in admin_ids),
        "counts": {
            "allowed_members": len(allowed_ids),
            "member_role_members": len(member_ids),
            "allowed_role_members": len(role_allowed_ids),
            "admin_members": len(admin_ids),
            "allowed_roles": len(allowed_role_rows),
            "admin_roles": len(admin_role_rows),
        },
    }


def _dashboard_member_filter_info(guild: discord.Guild) -> dict[str, Any]:
    roles = _dashboard_member_roles(guild)
    role = _dashboard_member_role(guild)
    ids = _dashboard_member_ids(guild)
    configured = _dashboard_member_role_config_value(guild.id)
    if role is not None:
        return {
            "mode": "discord_roles",
            "role_id": int(role.id),
            "role_ids": [int(r.id) for r in roles],
            "role_name": " + ".join(str(r.name) for r in roles),
            "role_names": [str(r.name) for r in roles],
            "eligible_count": len(ids),
            "configured": True,
            "setting_value": str(configured or os.getenv("DASHBOARD_MEMBER_ROLE_ID") or os.getenv("DASHBOARD_MEMBER_ROLE_NAME") or ""),
        }
    return {
        "mode": "role_not_configured",
        "role_id": 0,
        "role_name": "",
        "eligible_count": 0,
        "configured": False,
        "setting_value": str(configured or ""),
        "hint": "Mit /dashboard_set_member_role role:<Rolle> eine Gildenrolle setzen.",
    }


def _is_dashboard_member(guild: discord.Guild, user_id: int) -> bool:
    try:
        return int(user_id) in _dashboard_member_ids(guild)
    except Exception:
        return False


def _active_member_ids(guild: discord.Guild) -> set[int]:
    try:
        return {int(m.id) for m in getattr(guild, "members", []) if getattr(m, "id", None)}
    except Exception:
        return set()


def _load_json_file(name: str, default: Any) -> Any:
    path = DATA_DIR / name
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"__dashboard_error__": f"{type(exc).__name__}: {exc}"}


def _source_health() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for key, filename in JSON_SOURCES.items():
        path = DATA_DIR / filename
        if not path.exists():
            out[key] = {"file": filename, "exists": False, "ok": True, "size_bytes": 0}
            continue
        try:
            json.loads(path.read_text(encoding="utf-8"))
            ok = True
            error = ""
        except Exception as exc:
            ok = False
            error = f"{type(exc).__name__}: {exc}"
        out[key] = {
            "file": filename,
            "exists": True,
            "ok": ok,
            "error": error,
            "size_bytes": path.stat().st_size,
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        }
    return out


def _guild_dict(data: Any, guild_id: int) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    g = data.get(str(int(guild_id)))
    return g if isinstance(g, dict) else {}


def _guild_list(data: Any, guild_id: int) -> list[Any]:
    if not isinstance(data, dict):
        return []
    g = data.get(str(int(guild_id)))
    return g if isinstance(g, list) else []


def _participant_user_id(raw: Any) -> int:
    try:
        if isinstance(raw, dict):
            for key in ("id", "user_id", "member_id", "discord_id"):
                if raw.get(key) not in (None, ""):
                    return int(raw.get(key) or 0)
        return int(raw or 0)
    except Exception:
        return 0


def _participant_display_name(guild: discord.Guild, user_id: int, raw: Any = None) -> str:
    member = guild.get_member(int(user_id)) if user_id else None
    if member is not None:
        return _safe_text(getattr(member, "display_name", "") or getattr(member, "name", ""), 120)
    if isinstance(raw, dict):
        for key in ("display_name", "name", "nick", "username", "ingame_name"):
            if raw.get(key):
                return _safe_text(raw.get(key), 120)
    return f"User {user_id}" if user_id else "Unbekannt"


def _participant_obj(guild: discord.Guild, raw: Any) -> Optional[dict[str, Any]]:
    uid = _participant_user_id(raw)
    if not uid:
        return None
    return {
        "user_id": uid,
        "display_name": _participant_display_name(guild, uid, raw),
        "is_dashboard_member": _is_dashboard_member(guild, uid),
    }


def _event_user_count(event: dict[str, Any]) -> int:
    ids: set[int] = set()
    yes = event.get("yes") if isinstance(event.get("yes"), dict) else {}
    for arr in yes.values():
        if not isinstance(arr, list):
            continue
        for raw in arr:
            uid = _participant_user_id(raw)
            if uid:
                ids.add(uid)
    maybe = event.get("maybe") if isinstance(event.get("maybe"), dict) else {}
    for uid in maybe.keys():
        try:
            ids.add(int(uid))
        except Exception:
            pass
    no = event.get("no") if isinstance(event.get("no"), list) else []
    for raw in no:
        uid = _participant_user_id(raw)
        if uid:
            ids.add(uid)
    return len(ids)


def _event_participants(guild: discord.Guild, event: dict[str, Any]) -> dict[str, Any]:
    yes_detail: list[dict[str, Any]] = []
    yes = event.get("yes") if isinstance(event.get("yes"), dict) else {}
    for role_name, arr in yes.items():
        people: list[dict[str, Any]] = []
        if isinstance(arr, list):
            for raw in arr:
                obj = _participant_obj(guild, raw)
                if obj:
                    people.append(obj)
        people.sort(key=lambda x: str(x.get("display_name") or "").lower())
        yes_detail.append({"role": _safe_text(role_name, 80), "count": len(people), "participants": people})
    yes_detail.sort(key=lambda x: str(x.get("role") or "").lower())

    maybe_people: list[dict[str, Any]] = []
    maybe = event.get("maybe") if isinstance(event.get("maybe"), dict) else {}
    for uid_raw, raw_val in maybe.items():
        raw = raw_val if isinstance(raw_val, dict) else {"id": uid_raw}
        if isinstance(raw, dict):
            raw.setdefault("id", uid_raw)
        obj = _participant_obj(guild, raw)
        if obj:
            maybe_people.append(obj)
    maybe_people.sort(key=lambda x: str(x.get("display_name") or "").lower())

    no_people: list[dict[str, Any]] = []
    no = event.get("no") if isinstance(event.get("no"), list) else []
    for raw in no:
        obj = _participant_obj(guild, raw)
        if obj:
            no_people.append(obj)
    no_people.sort(key=lambda x: str(x.get("display_name") or "").lower())
    return {"yes": yes_detail, "maybe": maybe_people, "no": no_people}


def _summarize_events(data: Any, guild: discord.Guild, *, limit: int = 200) -> dict[str, Any]:
    guild_id = int(guild.id)
    events: list[dict[str, Any]] = []
    if isinstance(data, dict):
        for message_id, ev in data.items():
            if not isinstance(ev, dict):
                continue
            try:
                home_gid = int(ev.get("guild_id", 0) or 0)
            except Exception:
                home_gid = 0
            mirror_match = False
            mirrors = ev.get("mirrors") if isinstance(ev.get("mirrors"), list) else []
            for m in mirrors:
                if isinstance(m, dict):
                    try:
                        if int(m.get("guild_id", 0) or 0) == int(guild_id):
                            mirror_match = True
                            break
                    except Exception:
                        pass
            if home_gid != int(guild_id) and not mirror_match:
                continue
            yes = ev.get("yes") if isinstance(ev.get("yes"), dict) else {}
            role_counts = {str(k): len(v) for k, v in yes.items() if isinstance(v, list)}
            participant_detail = _event_participants(guild, ev)
            events.append({
                "event_id": str(message_id),
                "guild_id": int(guild_id),
                "message_id": int(message_id) if str(message_id).isdigit() else 0,
                "title": _safe_text(ev.get("title") or ev.get("name") or ev.get("event_name"), 160),
                "when_iso": str(ev.get("when_iso") or ev.get("start_time") or ev.get("time") or ""),
                "end_at": str(ev.get("end_at") or ev.get("end_time") or ""),
                "duration_minutes": int(ev.get("duration_minutes", 0) or 0),
                "location": _safe_text(ev.get("location") or "", 120),
                "scheduled_event_id": int(ev.get("scheduled_event_id", 0) or 0),
                "scheduled_event_url": str(ev.get("scheduled_event_url") or ""),
                "scheduled_event_error": _safe_text(ev.get("scheduled_event_error") or "", 300),
                "channel_id": int(ev.get("channel_id", 0) or 0),
                "scope": str(ev.get("scope") or "single"),
                "is_mirror_for_this_guild": bool(mirror_match and home_gid != int(guild_id)),
                "voice_enabled": bool(ev.get("voice_enabled")),
                "voice_channel_id": int(ev.get("voice_channel_id", 0) or 0),
                "voice_last_channel_id": int(ev.get("voice_last_channel_id", 0) or 0),
                "dkp_enabled": bool(ev.get("dkp_enabled", False)),
                "dkp_event_type": _safe_text(ev.get("dkp_event_type") or ev.get("event_type") or ev.get("dkp_type") or "", 80),
                "yes_counts": role_counts,
                "maybe_count": len(participant_detail.get("maybe") or []),
                "no_count": len(participant_detail.get("no") or []),
                "participant_count": _event_user_count(ev),
                "participants": participant_detail,
                "description": _safe_text(ev.get("description") or ev.get("desc") or "", 600),
                "image_url": _event_image_url_from_raw(ev),
            })
    events.sort(key=lambda x: str(x.get("when_iso") or ""), reverse=True)
    return {
        "count": len(events),
        "items": events[:limit],
    }


def _summarize_profiles(data: Any, guild: discord.Guild, *, limit: int = 500) -> dict[str, Any]:
    """Aktive Gildenmitglieder aus der gesetzten Discord-Rolle.

    Der alte Code lief primär über vorhandene Profil-JSONs. Dadurch konnten alte
    Profile aus Postgres wieder in die Mitgliederliste geraten und aktive Spieler
    ohne Profil fehlten. Jetzt ist die Discord-Rolle die Wahrheit; Profildaten
    werden nur dazugemischt.
    """
    g = _guild_dict(data, guild.id)
    users = g.get("users") if isinstance(g.get("users"), dict) else {}
    absences = g.get("absences") if isinstance(g.get("absences"), dict) else {}
    member_role = _dashboard_member_role(guild)
    active_ids = _dashboard_member_ids(guild)
    active_members = [
        m for m in (getattr(guild, "members", []) or [])
        if getattr(m, "id", None)
        and int(m.id) in active_ids
        and not bool(getattr(m, "bot", False))
    ]
    stale_count = sum(1 for uid in users.keys() if str(uid).isdigit() and int(uid) not in active_ids)
    items: list[dict[str, Any]] = []

    for member in active_members:
        user_id = int(member.id)
        profile = users.get(str(user_id)) if isinstance(users.get(str(user_id)), dict) else {}
        server_name = _safe_text(getattr(member, "display_name", "") or getattr(member, "name", "") or f"User {user_id}", 120)
        discord_name = _safe_text(getattr(member, "name", "") or server_name, 120)
        ingame_name = _safe_text(profile.get("ingame_name") or profile.get("name") or "", 120)
        display_name = ingame_name or server_name
        avatar_url = _safe_text(str(getattr(getattr(member, "display_avatar", None), "url", "") or ""), 500)
        joined_at = getattr(member, "joined_at", None)
        roles = [
            {"role_id": int(r.id), "role_name": str(r.name)}
            for r in getattr(member, "roles", [])
            if getattr(r, "id", None) and not bool(getattr(r, "is_default", lambda: False)())
        ]
        items.append({
            "user_id": user_id,
            "display_name": display_name,
            "server_name": server_name,
            "discord_name": discord_name,
            "discord_username": discord_name,
            "avatar_url": avatar_url,
            "ingame_name": ingame_name,
            "profile_name_set": bool(ingame_name),
            "class_name": _safe_text(profile.get("class_name"), 80),
            "main_role": _safe_text(profile.get("main_role"), 80),
            "gearscore": _safe_text(profile.get("gearscore"), 40),
            "created_at": str(profile.get("created_at") or ""),
            "joined_at": joined_at.isoformat() if joined_at else "",
            "roles": roles,
            "is_dashboard_member": True,
            "is_active": True,
            "profile_exists": bool(profile),
            "in_discord_cache": True,
            "profile": profile,
        })

    items.sort(key=lambda x: (str(x.get("display_name") or "")).casefold())
    return {
        "count": len(items),
        "total_json_count": len(users),
        "stale_count": stale_count,
        "without_profile_count": sum(1 for x in items if not x.get("profile_exists")),
        "absences_count": len(absences),
        "member_role_id": int(getattr(member_role, "id", 0) or 0),
        "member_role_name": str(getattr(member_role, "name", "") or ""),
        "active_member_ids": [str(x.get("user_id")) for x in items],
        "items": items[:limit],
        "filter": "dashboard_member_role_only",
    }


def _sync_member_directory(guild: discord.Guild, profile_summary: dict[str, Any]) -> dict[str, Any]:
    # Ohne bewusst gesetzte Mitgliederrolle niemals alle bisherigen Mitglieder
    # deaktivieren. Das schützt Multi-Guild-Installationen vor Fehlkonfiguration.
    member_role_id = int(profile_summary.get("member_role_id") or 0)
    if not member_role_id:
        return {
            "ok": False,
            "skipped": True,
            "error": "Keine Dashboard-Gildenrolle gesetzt. Nutze /dashboard_set_member_role.",
            "active": 0,
            "deactivated": 0,
        }
    try:
        rows = []
        for item in profile_summary.get("items") or []:
            if not isinstance(item, dict):
                continue
            rows.append({
                "user_id": int(item.get("user_id") or 0),
                "server_name": item.get("server_name") or item.get("display_name") or "",
                "discord_username": item.get("discord_username") or item.get("discord_name") or "",
                "avatar_url": item.get("avatar_url") or "",
                "ingame_name": item.get("ingame_name") or "",
                "main_role": item.get("main_role") or "",
                "gearscore": item.get("gearscore") or "",
                "joined_at": item.get("joined_at") or "",
                "roles": item.get("roles") or [],
                "profile": item.get("profile") or {},
            })
        return runtime_db.sync_guild_members(
            guild_id=int(guild.id),
            members=rows,
            member_role_id=member_role_id,
        )
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

def _summarize_balances(data: Any, guild: discord.Guild, *, limit: int = 500) -> dict[str, Any]:
    g = _guild_dict(data, guild.id)
    users = g.get("users") if isinstance(g.get("users"), dict) else {}
    items: list[dict[str, Any]] = []
    stale_count = 0
    for uid, raw in users.items():
        try:
            user_id = int(uid)
        except Exception:
            continue
        member = guild.get_member(user_id)
        if not _is_dashboard_member(guild, user_id):
            stale_count += 1
            continue
        bal = raw
        if isinstance(raw, dict):
            bal = raw.get("balance", raw.get("dkp", raw.get("ec", 0)))
        try:
            balance = float(bal or 0)
        except Exception:
            balance = 0.0
        items.append({
            "user_id": user_id,
            "display_name": _safe_text(getattr(member, "display_name", "") or f"User {user_id}", 120),
            "balance": balance,
        })
    items.sort(key=lambda x: float(x.get("balance") or 0), reverse=True)
    return {
        "count": len(items),
        "total_json_count": len(users),
        "stale_count": stale_count,
        "top": items[:limit],
        "filter": "dashboard_member_role_only",
    }


def _parse_amount(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        if isinstance(value, str):
            txt = value.strip().replace(" ", "")
            # Deutsch/Discord-tolerant: "1.234,5" -> 1234.5, "12,5" -> 12.5
            if "," in txt and "." in txt:
                txt = txt.replace(".", "").replace(",", ".")
            elif "," in txt:
                txt = txt.replace(",", ".")
            return float(txt)
        return float(value)
    except Exception:
        return 0.0


def _tx_user_id(tx: dict[str, Any]) -> int:
    for key in ("user_id", "target_user_id", "member_id", "discord_id", "to_user_id", "recipient_id"):
        try:
            if tx.get(key) not in (None, ""):
                return int(tx.get(key) or 0)
        except Exception:
            continue
    return 0


def _transactions_for_guild(data: Any, guild_id: int) -> list[Any]:
    """Tolerant gegen alte DKP-JSON-Strukturen.

    Unterstützt u. a.:
    - {guild_id: [tx, tx]}
    - {guild_id: {"transactions": [tx]}}
    - {guild_id: {"items": [tx]}}
    - {"transactions": {guild_id: [tx]}}
    """
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    gid = str(int(guild_id))
    g = data.get(gid)
    if isinstance(g, list):
        return g
    if isinstance(g, dict):
        for key in ("transactions", "items", "logs", "history"):
            if isinstance(g.get(key), list):
                return g.get(key) or []
    for top_key in ("transactions", "items", "logs", "history"):
        top = data.get(top_key)
        if isinstance(top, dict):
            x = top.get(gid)
            if isinstance(x, list):
                return x
            if isinstance(x, dict):
                for key in ("transactions", "items", "logs", "history"):
                    if isinstance(x.get(key), list):
                        return x.get(key) or []
        elif isinstance(top, list):
            return top
    return []


def _summarize_transactions(data: Any, guild: discord.Guild, *, limit: int = 1000) -> dict[str, Any]:
    arr = _transactions_for_guild(data, guild.id)
    items: list[dict[str, Any]] = []
    stale_count = 0

    def member_name(uid: int, tx: dict[str, Any]) -> str:
        member = guild.get_member(int(uid)) if uid else None
        if member is not None:
            return _safe_text(getattr(member, "display_name", "") or getattr(member, "name", "") or f"User {uid}", 120)
        for key in ("display_name", "target_name", "member_name", "name", "username"):
            if tx.get(key):
                return _safe_text(tx.get(key), 120)
        return f"User {uid}" if uid else "Unbekannt"

    # Neuste Einträge zuerst, weil alte Dateien meist append-only sind.
    for idx, tx in enumerate(reversed(arr)):
        if not isinstance(tx, dict):
            continue
        uid = _tx_user_id(tx)
        if uid and not _is_dashboard_member(guild, uid):
            stale_count += 1
            continue
        amount_raw = tx.get("amount", tx.get("delta", tx.get("change", tx.get("value", 0))))
        amount = _parse_amount(amount_raw)
        created_at = str(tx.get("created_at") or tx.get("at") or tx.get("timestamp") or tx.get("time") or "")
        item = {
            "created_at": created_at,
            "user_id": uid,
            "display_name": member_name(uid, tx),
            "amount": amount,
            "amount_raw": amount_raw,
            "reason": _safe_text(tx.get("reason") or tx.get("summary") or tx.get("note") or tx.get("description") or tx.get("type") or "", 300),
            "raw_type": _safe_text(tx.get("type") or tx.get("kind") or tx.get("action") or "", 80),
            "actor_id": int(tx.get("actor_id", 0) or tx.get("admin_id", 0) or tx.get("created_by", 0) or 0),
            "event_id": str(tx.get("event_id") or tx.get("source_event_id") or ""),
            "auction_id": str(tx.get("auction_id") or tx.get("source_auction_id") or ""),
        }
        items.append(item)
        if len(items) >= limit:
            # Für Dashboard reichen 1000 neueste; Count unten bleibt aber echter Rohcount.
            break

    by_user: dict[int, dict[str, Any]] = {}
    for item in items:
        uid = int(item.get("user_id") or 0)
        if not uid:
            continue
        bucket = by_user.setdefault(uid, {
            "user_id": uid,
            "display_name": item.get("display_name") or f"User {uid}",
            "earned": 0.0,
            "spent": 0.0,
            "net": 0.0,
            "count": 0,
        })
        amount = _parse_amount(item.get("amount"))
        bucket["net"] += amount
        bucket["count"] += 1
        if amount >= 0:
            bucket["earned"] += amount
        else:
            bucket["spent"] += abs(amount)

    user_rows = list(by_user.values())
    top_earned = sorted(user_rows, key=lambda x: float(x.get("earned") or 0), reverse=True)[:25]
    top_spent = sorted(user_rows, key=lambda x: float(x.get("spent") or 0), reverse=True)[:25]
    top_activity = sorted(user_rows, key=lambda x: int(x.get("count") or 0), reverse=True)[:25]
    total_earned = sum(float(x.get("earned") or 0) for x in user_rows)
    total_spent = sum(float(x.get("spent") or 0) for x in user_rows)

    return {
        "count": len(arr),
        "loaded_count": len(items),
        "stale_count": stale_count,
        "recent": items[:250],
        "items": items,
        "total_earned": total_earned,
        "total_spent": total_spent,
        "net_loaded": total_earned - total_spent,
        "by_user": user_rows,
        "top_earned": top_earned,
        "top_spent": top_spent,
        "top_activity": top_activity,
        "filter": "dashboard_member_role_only",
    }


def _summarize_auctions(data: Any, guild: discord.Guild, *, limit: int = 300) -> dict[str, Any]:
    """Auktionsdaten fürs Dashboard normalisieren.

    Neu: nicht nur Übersicht, sondern genug Detaildaten für Auktions-Detailseiten:
    - Gebotshistorie
    - Gewinner/Empfänger
    - Startgebot/Mindestschritt/Festpreis
    - berechtigte Spieler
    - Müll-Würfe
    """
    guild_id = int(guild.id)
    g = _guild_dict(data, guild_id)
    auctions = g.get("auctions") if isinstance(g.get("auctions"), dict) else {}
    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    def member_name(uid: Any, fallback: str = "") -> str:
        try:
            user_id = int(uid or 0)
        except Exception:
            user_id = 0
        if user_id:
            m = guild.get_member(user_id)
            if m is not None:
                return _safe_text(getattr(m, "display_name", "") or getattr(m, "name", "") or f"User {user_id}", 120)
        return _safe_text(fallback or (f"User {user_id}" if user_id else ""), 120)

    for aid, auc in auctions.items():
        if not isinstance(auc, dict):
            continue
        status = str(auc.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        bids_raw = auc.get("bids") if isinstance(auc.get("bids"), list) else []
        bids: list[dict[str, Any]] = []
        for b in bids_raw:
            if not isinstance(b, dict):
                continue
            uid = int(b.get("user_id", 0) or 0)
            bids.append({
                "user_id": uid,
                "display_name": member_name(uid, str(b.get("name") or "")),
                "amount": b.get("amount"),
                "created_at": str(b.get("created_at") or b.get("at") or ""),
            })
        bids.sort(key=lambda x: (float(x.get("amount") or 0), str(x.get("created_at") or "")), reverse=True)
        top_bid = bids[0] if bids else None

        winner_id = int(
            auc.get("winner_user_id", 0)
            or auc.get("delivered_to", 0)
            or auc.get("sold_to", 0)
            or auc.get("junk_roll_winner_id", 0)
            or auc.get("junk_lottery_winner_id", 0)
            or 0
        )
        if not winner_id and status in {"delivered", "sold", "ended"} and top_bid:
            winner_id = int(top_bid.get("user_id") or 0)

        eligible_raw = auc.get("eligible_user_ids") if isinstance(auc.get("eligible_user_ids"), list) else []
        eligible = []
        for uid in eligible_raw[:250]:
            try:
                iuid = int(uid or 0)
            except Exception:
                iuid = 0
            if iuid:
                eligible.append({"user_id": iuid, "display_name": member_name(iuid)})

        junk_rolls_raw = auc.get("junk_rolls") if isinstance(auc.get("junk_rolls"), dict) else {}
        junk_rolls = []
        for uid_raw, roll_raw in junk_rolls_raw.items():
            try:
                iuid = int(uid_raw or 0)
                roll = int(roll_raw or 0)
            except Exception:
                continue
            junk_rolls.append({"user_id": iuid, "display_name": member_name(iuid), "roll": roll})
        junk_rolls.sort(key=lambda x: int(x.get("roll") or 0), reverse=True)

        local_item_id = str(auc.get("item_id") or "")
        raw_item_name = _safe_text(auc.get("item_name") or auc.get("item") or auc.get("title"), 180)
        catalog_link = None
        try:
            catalog_link = runtime_db.resolve_catalog_item_reference(
                guild_id=guild_id,
                local_item_id=local_item_id,
                item_name=raw_item_name,
                catalog_item_id=int(auc.get("catalog_item_id") or 0),
                source_item_id=str(auc.get("catalog_source_item_id") or ""),
            )
        except Exception:
            catalog_link = None
        items.append({
            "auction_id": str(aid),
            "item_id": local_item_id,
            "item_name": raw_item_name,
            "catalog_item_id": int((catalog_link or {}).get("id") or auc.get("catalog_item_id") or 0),
            "catalog_source_item_id": str((catalog_link or {}).get("source_item_id") or auc.get("catalog_source_item_id") or ""),
            "catalog_item_name": str((catalog_link or {}).get("name") or auc.get("catalog_item_name") or ""),
            "catalog_source_url": str((catalog_link or {}).get("source_url") or auc.get("catalog_source_url") or ""),
            "catalog_image_url": str((catalog_link or {}).get("manual_image_url") or (catalog_link or {}).get("image_url") or (catalog_link or {}).get("icon_url") or auc.get("catalog_image_url") or ""),
            "catalog_match_method": str((catalog_link or {}).get("match_method") or auc.get("catalog_match_method") or ""),
            "catalog_match_confidence": float((catalog_link or {}).get("match_confidence") or auc.get("catalog_match_confidence") or 0),
            "status": status,
            "kind": _safe_text(auc.get("kind") or "", 80),
            "phase": _safe_text(auc.get("phase") or auc.get("mode") or auc.get("auction_type") or "", 80),
            "eligibility_mode": _safe_text(auc.get("eligibility_mode") or "", 80),
            "created_at": str(auc.get("created_at") or ""),
            "created_by": int(auc.get("created_by", 0) or 0),
            "created_by_name": member_name(auc.get("created_by")),
            "ends_at": str(auc.get("ends_at") or auc.get("end_at") or ""),
            "start_bid": auc.get("start_bid"),
            "min_increment": auc.get("min_increment"),
            "fixed_price": auc.get("fixed_price"),
            "bid_count": len(bids),
            "bids": bids[:100],
            "top_bid_user_id": int((top_bid or {}).get("user_id", 0) or 0) if isinstance(top_bid, dict) else 0,
            "top_bid_user_name": (top_bid or {}).get("display_name") if isinstance(top_bid, dict) else "",
            "top_bid_amount": (top_bid or {}).get("amount") if isinstance(top_bid, dict) else None,
            "winner_user_id": winner_id,
            "winner_name": member_name(winner_id) if winner_id else "",
            "delivered_at": str(auc.get("delivered_at") or auc.get("sold_at") or ""),
            "delivered_by": int(auc.get("delivered_by", 0) or 0),
            "delivered_by_name": member_name(auc.get("delivered_by")),
            "message_id": int(auc.get("message_id", 0) or 0),
            "channel_id": int(auc.get("channel_id", 0) or 0),
            "market_message_id": int(auc.get("market_message_id", 0) or 0),
            "active_message_id": int(auc.get("active_message_id", 0) or 0),
            "eligible_count": len(eligible_raw),
            "eligible_users": eligible,
            "junk_drop": bool(auc.get("junk_drop")),
            "junk_roll_until": str(auc.get("junk_roll_until") or auc.get("junk_interest_until") or ""),
            "junk_rolls": junk_rolls[:150],
            "junk_roll_winner_roll": int(auc.get("junk_roll_winner_roll", 0) or 0),
        })
    items.sort(key=lambda x: str(x.get("created_at") or x.get("ends_at") or ""), reverse=True)
    return {"count": len(auctions), "by_status": counts, "items": items[:limit]}

def _catalog_items_for_guild(data: Any, guild_id: int) -> dict[str, Any]:
    """Item-Katalog dieser Gilde normalisieren.

    loot_items.json ist bei uns normalerweise:
    {guild_id: {"items": {item_id: {"name": ..., "slot": ..., "weapon_type": ...}}}}
    Diese Funktion ist bewusst tolerant, falls alte Daten anders liegen.
    """
    if not isinstance(data, dict):
        return {}
    g = _guild_dict(data, guild_id)
    if isinstance(g.get("items"), dict):
        return g.get("items") or {}
    # Fallback, falls die Datei direkt Items enthält.
    if "items" in data and isinstance(data.get("items"), dict):
        return data.get("items") or {}
    # Sehr alter Fallback: Wenn Werte wie Item-Objekte aussehen.
    if any(isinstance(v, dict) and ("name" in v or "slot" in v or "weapon_type" in v) for v in data.values()):
        return data
    return {}


def _item_label_from_catalog(catalog: dict[str, Any], item_id: Any) -> str:
    iid = str(item_id or "").strip()
    if not iid:
        return ""
    item = catalog.get(iid) if isinstance(catalog, dict) else None
    if not isinstance(item, dict):
        return _safe_text(iid, 180)
    name = str(item.get("name") or item.get("item_name") or iid).strip()
    weapon_type = str(item.get("weapon_type") or "").strip()
    if weapon_type:
        return _safe_text(f"{name} ({weapon_type})", 180)
    return _safe_text(name, 180)


def _is_received_slot(value: dict[str, Any]) -> bool:
    return bool(value.get("received") or value.get("locked") or value.get("obtained") or value.get("done"))


def _need_roots(raw: dict[str, Any]) -> list[dict[str, Any]]:
    roots = [raw]
    for key in ("needs", "needlist", "need_list", "data", "tabs"):
        val = raw.get(key)
        if isinstance(val, dict) and val not in roots:
            roots.append(val)
    return roots


def _simplify_need_value(value: Any, catalog: Optional[dict[str, Any]] = None, *, slot_name: str = "") -> list[str]:
    """Need-Einträge aus verschiedenen alten JSON-Strukturen lesbar machen.

    Der wichtige Fix hier: echte Need-Slots speichern nur `item_id`.
    Vorher konnte das Dashboard daraus keinen schönen Main-/Secondary-Need machen.
    Jetzt wird `item_id` über loot_items.json zu Itemnamen aufgelöst.
    """
    catalog = catalog or {}
    out: list[str] = []
    if value in (None, "", False):
        return out
    if isinstance(value, str):
        txt = _safe_text(value, 180)
        if txt:
            out.append(txt)
        return out
    if isinstance(value, (int, float)):
        if value:
            out.append(str(value))
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(_simplify_need_value(item, catalog, slot_name=slot_name))
        return out
    if isinstance(value, dict):
        # Standard-Slotobjekt: {item_id, received, locked, ...}
        raw_item_id = value.get("item_id") or value.get("itemId") or value.get("item")
        if raw_item_id:
            if _is_received_slot(value):
                return out
            label = _item_label_from_catalog(catalog, raw_item_id)
            if label:
                out.append(f"{_safe_text(slot_name, 80)}: {label}" if slot_name else label)
            return out

        if _is_received_slot(value):
            # erhaltene Needs im Dashboard nicht als offenen Need zählen
            return out

        direct = value.get("item_name") or value.get("name") or value.get("title")
        if direct:
            label = _safe_text(direct, 180)
            out.append(f"{_safe_text(slot_name, 80)}: {label}" if slot_name else label)
            return out

        meta_keys = {"updated_at", "created_at", "user_id", "display_name", "received", "locked", "received_at", "received_by", "obtained", "done"}
        for key, val in value.items():
            key_s = str(key)
            if key_s.lower() in meta_keys or val in (None, "", False):
                continue
            if isinstance(val, dict):
                sub = _simplify_need_value(val, catalog, slot_name=key_s)
                out.extend(sub)
            elif isinstance(val, list):
                for x in _simplify_need_value(val, catalog):
                    out.append(f"{_safe_text(key_s, 80)}: {x}")
            elif isinstance(val, str):
                # Bei Slot: "item_id"-Altwerten ist der String meistens direkt eine Item-ID.
                label = _item_label_from_catalog(catalog, val)
                out.append(f"{_safe_text(key_s, 80)}: {label}")
            elif val:
                out.append(f"{_safe_text(key_s, 80)}: {_safe_text(val, 180)}")
        return out
    return out


def _extract_need_tab(raw: dict[str, Any], tab_names: set[str], catalog: dict[str, Any]) -> list[str]:
    names = {x.lower() for x in tab_names}
    for root in _need_roots(raw):
        for key, val in root.items():
            if str(key).strip().lower() in names:
                return _simplify_need_value(val, catalog)
    return []


def _extract_need_items(raw: dict[str, Any], tab_names: set[str], catalog: dict[str, Any], guild_id: int) -> list[dict[str, Any]]:
    names = {x.casefold() for x in tab_names}
    out: list[dict[str, Any]] = []
    for root in _need_roots(raw):
        for key, bucket in root.items():
            if str(key).strip().casefold() not in names or not isinstance(bucket, dict):
                continue
            for slot_name, value in bucket.items():
                obj = value if isinstance(value, dict) else {"item_id": value}
                if not isinstance(obj, dict) or _is_received_slot(obj):
                    continue
                local_id = str(obj.get("item_id") or obj.get("itemId") or obj.get("item") or "").strip()
                direct_name = str(obj.get("item_name") or obj.get("name") or obj.get("title") or "").strip()
                item_name = _item_label_from_catalog(catalog, local_id) if local_id else _safe_text(direct_name, 180)
                if not item_name:
                    continue
                linked = None
                try:
                    linked = runtime_db.resolve_catalog_item_reference(
                        guild_id=int(guild_id), local_item_id=local_id, item_name=item_name,
                        catalog_item_id=int(obj.get("catalog_item_id") or 0),
                        source_item_id=str(obj.get("catalog_source_item_id") or ""),
                    )
                except Exception:
                    linked = None
                out.append({
                    "slot_name": _safe_text(slot_name, 80),
                    "item_id": local_id,
                    "item_name": item_name,
                    "catalog_item_id": int((linked or {}).get("id") or obj.get("catalog_item_id") or 0),
                    "catalog_source_item_id": str((linked or {}).get("source_item_id") or obj.get("catalog_source_item_id") or ""),
                    "catalog_item_name": str((linked or {}).get("name") or obj.get("catalog_item_name") or ""),
                    "catalog_source_url": str((linked or {}).get("source_url") or obj.get("catalog_source_url") or ""),
                    "catalog_image_url": str((linked or {}).get("manual_image_url") or (linked or {}).get("image_url") or (linked or {}).get("icon_url") or obj.get("catalog_image_url") or ""),
                    "received": False,
                })
            return out
    return out


def _summarize_needs(data: Any, guild: discord.Guild, item_catalog_data: Any = None, *, limit: int = 500) -> dict[str, Any]:
    g = _guild_dict(data, guild.id)
    users = g.get("users") if isinstance(g.get("users"), dict) else g
    catalog = _catalog_items_for_guild(item_catalog_data, guild.id)
    total_json_users = len(users) if isinstance(users, dict) else 0
    stale_count = 0
    active_user_count = 0
    total_entries = 0
    items: list[dict[str, Any]] = []
    if isinstance(users, dict):
        for uid, raw in users.items():
            if not isinstance(raw, dict):
                continue
            try:
                user_id = int(uid)
            except Exception:
                user_id = 0
            if not user_id or not _is_dashboard_member(guild, user_id):
                stale_count += 1
                continue
            member = guild.get_member(user_id)

            main_items = _extract_need_items(raw, {"main", "Main", "main_needs", "mainNeeds", "haupt", "mainspec"}, catalog, int(guild.id))
            secondary_items = _extract_need_items(raw, {"secondary", "Secondary", "sec", "secondary_needs", "secondaryNeeds", "zweite", "zweitspec", "offspec"}, catalog, int(guild.id))
            main_needs = [str(x.get("item_name") or "") for x in main_items if x.get("item_name")]
            secondary_needs = [str(x.get("item_name") or "") for x in secondary_items if x.get("item_name")]

            # Fallback für sehr alte Strukturen ohne Main/Secondary: nur dann alles als Main interpretieren.
            if not main_needs and not secondary_needs:
                for key, val in raw.items():
                    if str(key).lower() in {"updated_at", "created_at", "user_id", "display_name", "needs", "needlist", "need_list", "data", "tabs"}:
                        continue
                    main_needs.extend(_simplify_need_value({key: val}, catalog))

            # Duplikate entfernen, Reihenfolge behalten
            def dedupe(arr: list[str]) -> list[str]:
                seen: set[str] = set()
                out: list[str] = []
                for x in arr:
                    x = _safe_text(x, 220)
                    if not x or x in seen:
                        continue
                    seen.add(x)
                    out.append(x)
                return out

            main_needs = dedupe(main_needs)
            secondary_needs = dedupe(secondary_needs)
            cnt = len(main_needs) + len(secondary_needs)
            if cnt <= 0:
                continue
            active_user_count += 1
            total_entries += cnt
            if len(items) < limit:
                items.append({
                    "user_id": user_id,
                    "display_name": _safe_text(getattr(member, "display_name", "") or f"User {user_id}", 120),
                    "main": main_needs,
                    "secondary": secondary_needs,
                    "main_items": main_items,
                    "secondary_items": secondary_items,
                    "main_count": len(main_needs),
                    "secondary_count": len(secondary_needs),
                    "need_entries_estimated": cnt,
                })
    items.sort(key=lambda x: str(x.get("display_name") or "").lower())
    return {
        "user_count": active_user_count,
        "total_json_user_count": total_json_users,
        "stale_count": stale_count,
        "need_entries_estimated": total_entries,
        "items": items,
        "sample": items[:limit],
        "filter": "dashboard_member_role_only",
        "catalog_items_known": len(catalog),
    }




def _looks_like_id(value: Any) -> bool:
    try:
        txt = str(value or "").strip()
        return txt.isdigit() and len(txt) >= 15
    except Exception:
        return False


def _resolve_channel_name(guild: discord.Guild, raw: Any) -> str:
    try:
        cid = int(str(raw).strip())
        ch = guild.get_channel(cid)
        if ch is not None:
            return _safe_text(getattr(ch, "name", "") or str(cid), 120)
    except Exception:
        pass
    return ""


def _resolve_role_name(guild: discord.Guild, raw: Any) -> str:
    try:
        rid = int(str(raw).strip())
        role = guild.get_role(rid)
        if role is not None:
            return _safe_text(getattr(role, "name", "") or str(rid), 120)
    except Exception:
        pass
    return ""


def _flatten_config(data: Any, prefix: str = "", *, max_depth: int = 4) -> list[tuple[str, Any]]:
    """Kleine read-only Flatten-Hilfe für Settings-Seiten.

    Keine Daten werden verändert. Die Funktion ist bewusst defensiv,
    weil die alten JSON-Dateien je nach Modul unterschiedlich aufgebaut sind.
    """
    out: list[tuple[str, Any]] = []
    if max_depth < 0:
        return out
    if isinstance(data, dict):
        for key, value in data.items():
            k = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, (dict, list)):
                out.extend(_flatten_config(value, k, max_depth=max_depth - 1))
            else:
                out.append((k, value))
    elif isinstance(data, list):
        for idx, value in enumerate(data[:80]):
            k = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            if isinstance(value, (dict, list)):
                out.extend(_flatten_config(value, k, max_depth=max_depth - 1))
            else:
                out.append((k, value))
    return out


def _interesting_setting_key(key: str) -> bool:
    k = key.lower()
    needles = (
        "channel", "role", "admin", "leader", "officer", "member",
        "dkp", "ec", "point", "decay", "verfall", "auction", "market",
        "log", "loot", "need", "voice", "event", "enabled", "active",
        "template", "alliance", "weekly", "report", "contact", "price",
        "bid", "roll", "sale", "guild"
    )
    return any(n in k for n in needles)


def _summarize_settings(sources: dict[str, Any], guild: discord.Guild) -> dict[str, Any]:
    guild_id = int(guild.id)
    member_filter = _dashboard_member_filter_info(guild)
    config_keys = [
        "member_portal_cfg",
        "dkp_cfg",
        "loot_cfg",
        "loot_auction_cfg",
        "raid_templates",
        "alliance_config",
        "weekly_report_cfg",
        "leader_contact_cfg",
        "guild_chest",
    ]
    channels: dict[str, dict[str, Any]] = {}
    roles: dict[str, dict[str, Any]] = {}
    settings_rows: list[dict[str, Any]] = []
    module_rows: list[dict[str, Any]] = []

    for source_key in config_keys:
        raw = sources.get(source_key)
        scoped = _guild_dict(raw, guild_id)
        if not scoped and isinstance(raw, dict):
            # Fallback für ältere Configs, die nicht nach guild_id gruppiert sind.
            scoped = raw
        exists = not (raw in ({}, [], None))
        module_rows.append({
            "module": source_key,
            "configured": bool(scoped),
            "source_exists": bool(exists),
            "top_level_keys": len(scoped) if isinstance(scoped, dict) else (len(scoped) if isinstance(scoped, list) else 0),
        })
        if not isinstance(scoped, (dict, list)):
            continue
        flat = _flatten_config(scoped, source_key, max_depth=4)
        for key, value in flat:
            lk = key.lower()
            if not _interesting_setting_key(key):
                continue
            text_value = _safe_text(value, 240)
            row = {"source": source_key, "key": key, "value": text_value}
            settings_rows.append(row)
            if "channel" in lk and _looks_like_id(value):
                channels[key] = {
                    "source": source_key,
                    "key": key,
                    "channel_id": str(value),
                    "name": _resolve_channel_name(guild, value),
                    "resolved": bool(_resolve_channel_name(guild, value)),
                }
            if "role" in lk and _looks_like_id(value):
                roles[key] = {
                    "source": source_key,
                    "key": key,
                    "role_id": str(value),
                    "name": _resolve_role_name(guild, value),
                    "resolved": bool(_resolve_role_name(guild, value)),
                }

    channels_rows = list(channels.values())[:250]
    roles_rows = list(roles.values())[:250]
    settings_rows = settings_rows[:600]
    return {
        "member_filter": member_filter,
        "modules": module_rows,
        "channels": channels_rows,
        "roles": roles_rows,
        "settings": settings_rows,
        "counts": {
            "modules": len(module_rows),
            "channels": len(channels_rows),
            "roles": len(roles_rows),
            "settings": len(settings_rows),
        },
    }

def _summarize_event_checks(data: Any, guild_id: int, *, limit: int = 200) -> dict[str, Any]:
    g = _guild_dict(data, guild_id)
    checks = g.get("checks") if isinstance(g.get("checks"), dict) else g
    if not isinstance(checks, dict):
        checks = {}
    items = []
    for cid, chk in checks.items():
        if not isinstance(chk, dict):
            continue
        attendees = chk.get("attendees") if isinstance(chk.get("attendees"), dict) else {}
        items.append({
            "check_id": str(cid),
            "event_id": str(chk.get("event_id") or cid),
            "title": _safe_text(chk.get("title") or chk.get("event_title") or "", 160),
            "created_at": str(chk.get("created_at") or ""),
            "status": _safe_text(chk.get("status") or "", 80),
            "attendee_count": len(attendees),
            "ec_awarded": bool(chk.get("ec_awarded") or chk.get("awarded")),
        })
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return {"count": len(checks), "items": items[:limit]}


def _voice_summary(guild_id: int) -> dict[str, Any]:
    try:
        # Für das Dashboard/Analytics laden wir mehr als nur die letzten 20 Sessions.
        # Das ist weiterhin read-only und verändert keine Voice- oder Eventdaten.
        sessions = runtime_db.fetch_voice_sessions(guild_id, limit=1000)
        by_user: dict[int, dict[str, Any]] = {}
        for sess in sessions or []:
            if not isinstance(sess, dict):
                continue
            try:
                uid = int(sess.get("user_id") or sess.get("member_id") or 0)
            except Exception:
                uid = 0
            if not uid:
                continue
            try:
                dur = int(float(sess.get("duration_seconds") or sess.get("duration") or 0))
            except Exception:
                dur = 0
            bucket = by_user.setdefault(uid, {
                "user_id": uid,
                "sessions": 0,
                "total_seconds": 0,
                "last_joined_at": "",
                "last_left_at": "",
            })
            bucket["sessions"] += 1
            bucket["total_seconds"] += max(0, dur)
            joined = str(sess.get("joined_at") or "")
            left = str(sess.get("left_at") or "")
            if joined and joined > str(bucket.get("last_joined_at") or ""):
                bucket["last_joined_at"] = joined
            if left and left > str(bucket.get("last_left_at") or ""):
                bucket["last_left_at"] = left

        by_user_rows = list(by_user.values())
        by_user_rows.sort(key=lambda x: int(x.get("total_seconds") or 0), reverse=True)
        total_seconds = sum(int(x.get("total_seconds") or 0) for x in by_user_rows)
        return {
            "sessions_total": runtime_db.count_voice_sessions(guild_id),
            "sessions_open": runtime_db.count_voice_sessions(guild_id, open_only=True),
            "recent_sessions": sessions[:250],
            "loaded_sessions": len(sessions or []),
            "total_seconds_loaded": total_seconds,
            "total_hours_loaded": round(total_seconds / 3600, 2),
            "by_user": by_user_rows[:500],
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "sessions_total": 0, "sessions_open": 0, "recent_sessions": [], "by_user": []}


def _audit_summary(guild_id: int) -> dict[str, Any]:
    try:
        return {
            "logs_total": runtime_db.count_audit_logs(guild_id),
            "recent_logs": runtime_db.fetch_audit_logs(guild_id, limit=300),
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "logs_total": 0, "recent_logs": []}




def _dashboard_insights(guild: discord.Guild, snapshot: dict[str, Any]) -> dict[str, Any]:
    """Zusätzliche read-only Auswertungen für Dashboard/Vertrieb/Massennutzung.

    Wichtig: Diese Funktion verändert keine Bot-Daten. Sie nutzt nur den gerade
    gebauten Snapshot plus die gesetzte Dashboard-Gildenrolle.
    """
    try:
        member_role = _dashboard_member_role(guild)
        role_member_map = {
            int(member.id): member
            for member in (getattr(member_role, "members", []) if member_role is not None else [])
            if getattr(member, "id", None)
        }
        role_ids = sorted(role_member_map.keys())
    except Exception:
        role_member_map = {}
        role_ids = []

    profiles = ((snapshot.get("profiles") or {}).get("items") or [])
    balances = (((snapshot.get("ec") or {}).get("balances") or {}).get("top") or [])
    needs_items = (((snapshot.get("loot") or {}).get("needs") or {}).get("items") or [])
    events = ((snapshot.get("events") or {}).get("items") or [])
    auctions = (((snapshot.get("loot") or {}).get("auctions") or {}).get("items") or [])
    tx_items = ((((snapshot.get("ec") or {}).get("transactions") or {}).get("items") or [])
                or (((snapshot.get("ec") or {}).get("transactions") or {}).get("recent") or []))
    voice_by_user = ((snapshot.get("voice") or {}).get("by_user") or [])

    profile_by_uid: dict[int, dict[str, Any]] = {}
    for p in profiles:
        if isinstance(p, dict):
            try:
                profile_by_uid[int(p.get("user_id") or 0)] = p
            except Exception:
                pass

    balance_by_uid: dict[int, float] = {}
    for b in balances:
        if not isinstance(b, dict):
            continue
        try:
            balance_by_uid[int(b.get("user_id") or 0)] = float(b.get("balance") or 0)
        except Exception:
            continue

    need_by_uid: dict[int, dict[str, Any]] = {}
    main_need_counter: dict[str, int] = {}
    secondary_need_counter: dict[str, int] = {}
    for n in needs_items:
        if not isinstance(n, dict):
            continue
        try:
            uid = int(n.get("user_id") or 0)
        except Exception:
            uid = 0
        if uid:
            need_by_uid[uid] = n
        for label in n.get("main") or []:
            key = str(label or "").strip()
            if key:
                main_need_counter[key] = main_need_counter.get(key, 0) + 1
        for label in n.get("secondary") or []:
            key = str(label or "").strip()
            if key:
                secondary_need_counter[key] = secondary_need_counter.get(key, 0) + 1

    event_stats: dict[int, dict[str, Any]] = {}
    def touch_event_user(uid: int) -> dict[str, Any]:
        return event_stats.setdefault(uid, {"responses": 0, "yes": 0, "maybe": 0, "no": 0})

    for ev in events:
        if not isinstance(ev, dict):
            continue
        parts = ev.get("participants") or {}
        for group in (parts.get("yes") or []):
            if not isinstance(group, dict):
                continue
            for p in group.get("participants") or []:
                if not isinstance(p, dict):
                    continue
                try:
                    uid = int(p.get("user_id") or 0)
                except Exception:
                    uid = 0
                if uid:
                    st = touch_event_user(uid)
                    st["responses"] += 1
                    st["yes"] += 1
        for key, field in (("maybe", "maybe"), ("no", "no")):
            for p in parts.get(key) or []:
                if not isinstance(p, dict):
                    continue
                try:
                    uid = int(p.get("user_id") or 0)
                except Exception:
                    uid = 0
                if uid:
                    st = touch_event_user(uid)
                    st["responses"] += 1
                    st[field] += 1

    voice_by_uid: dict[int, dict[str, Any]] = {}
    for v in voice_by_user:
        if isinstance(v, dict):
            try:
                voice_by_uid[int(v.get("user_id") or 0)] = v
            except Exception:
                pass

    won_by_uid: dict[int, dict[str, Any]] = {}
    leading_by_uid: dict[int, int] = {}
    active_statuses = {"open", "active", "running", "bidding", "roll", "sale", "free", "main", "secondary"}
    for a in auctions:
        if not isinstance(a, dict):
            continue
        try:
            winner_id = int(a.get("winner_user_id") or 0)
        except Exception:
            winner_id = 0
        if winner_id:
            bucket = won_by_uid.setdefault(winner_id, {"user_id": winner_id, "won_count": 0, "items": []})
            bucket["won_count"] += 1
            if len(bucket["items"]) < 30:
                bucket["items"].append({"auction_id": a.get("auction_id"), "item_name": a.get("item_name"), "status": a.get("status")})
        status = str(a.get("status") or "").lower()
        if status in active_statuses:
            try:
                leader_id = int(a.get("top_bid_user_id") or 0)
            except Exception:
                leader_id = 0
            if leader_id:
                leading_by_uid[leader_id] = leading_by_uid.get(leader_id, 0) + 1

    tx_by_uid: dict[int, dict[str, Any]] = {}
    for tx in tx_items:
        if not isinstance(tx, dict):
            continue
        try:
            uid = int(tx.get("user_id") or 0)
        except Exception:
            uid = 0
        if not uid:
            continue
        amount = _parse_amount(tx.get("amount"))
        b = tx_by_uid.setdefault(uid, {"earned": 0.0, "spent": 0.0, "transactions": 0})
        b["transactions"] += 1
        if amount >= 0:
            b["earned"] += amount
        else:
            b["spent"] += abs(amount)

    members: list[dict[str, Any]] = []
    for uid in role_ids:
        member = role_member_map.get(uid) or guild.get_member(uid)
        p = profile_by_uid.get(uid, {})
        n = need_by_uid.get(uid, {})
        evs = event_stats.get(uid, {"responses": 0, "yes": 0, "maybe": 0, "no": 0})
        voice = voice_by_uid.get(uid, {})
        won = won_by_uid.get(uid, {"won_count": 0})
        txs = tx_by_uid.get(uid, {"earned": 0.0, "spent": 0.0, "transactions": 0})
        main_count = int(n.get("main_count") or len(n.get("main") or []) or 0) if isinstance(n, dict) else 0
        secondary_count = int(n.get("secondary_count") or len(n.get("secondary") or []) or 0) if isinstance(n, dict) else 0
        has_profile = uid in profile_by_uid
        has_ec = uid in balance_by_uid
        has_needs = uid in need_by_uid
        voice_seconds = int(voice.get("total_seconds") or 0) if isinstance(voice, dict) else 0
        risk_flags: list[str] = []
        if not has_profile:
            risk_flags.append("kein Profil")
        if not has_ec:
            risk_flags.append("kein EC-Konto")
        if not has_needs:
            risk_flags.append("keine Needliste")
        if int(evs.get("responses") or 0) <= 0:
            risk_flags.append("keine Eventantwort")
        if voice_seconds <= 0:
            risk_flags.append("keine Voice-Zeit")
        discord_display = _safe_text(getattr(member, "display_name", "") or "", 120)
        avatar_url = _safe_text(str(getattr(getattr(member, "display_avatar", None), "url", "") or ""), 500)
        members.append({
            "user_id": uid,
            "display_name": discord_display or _safe_text(p.get("display_name") or p.get("ingame_name") or f"User {uid}", 120),
            "server_name": discord_display,
            "discord_name": _safe_text(getattr(member, "name", "") or "", 120),
            "avatar_url": avatar_url,
            "ingame_name": _safe_text(p.get("ingame_name") if isinstance(p, dict) else "", 120),
            "class_name": _safe_text(p.get("class_name") if isinstance(p, dict) else "", 80),
            "main_role": _safe_text(p.get("main_role") if isinstance(p, dict) else "", 80),
            "gearscore": _safe_text(p.get("gearscore") if isinstance(p, dict) else "", 40),
            "ec_balance": balance_by_uid.get(uid),
            "has_profile": has_profile,
            "has_ec": has_ec,
            "has_needs": has_needs,
            "main_need_count": main_count,
            "secondary_need_count": secondary_count,
            "event_responses": int(evs.get("responses") or 0),
            "event_yes": int(evs.get("yes") or 0),
            "event_maybe": int(evs.get("maybe") or 0),
            "event_no": int(evs.get("no") or 0),
            "voice_seconds": voice_seconds,
            "voice_hours": round(voice_seconds / 3600, 2),
            "voice_sessions": int(voice.get("sessions") or 0) if isinstance(voice, dict) else 0,
            "ec_earned_loaded": round(float(txs.get("earned") or 0), 2),
            "ec_spent_loaded": round(float(txs.get("spent") or 0), 2),
            "ec_transactions_loaded": int(txs.get("transactions") or 0),
            "loot_won_count": int(won.get("won_count") or 0),
            "active_leads": int(leading_by_uid.get(uid, 0)),
            "risk_flags": risk_flags,
            "risk_score": len(risk_flags),
        })

    members.sort(key=lambda x: (-int(x.get("risk_score") or 0), str(x.get("display_name") or "").lower()))
    top_main = sorted(main_need_counter.items(), key=lambda x: (-x[1], x[0].lower()))[:80]
    top_secondary = sorted(secondary_need_counter.items(), key=lambda x: (-x[1], x[0].lower()))[:80]
    winners = list(won_by_uid.values())
    winners.sort(key=lambda x: (-int(x.get("won_count") or 0), str(x.get("user_id"))))
    missing_profile = sum(1 for m in members if not m.get("has_profile"))
    missing_ec = sum(1 for m in members if not m.get("has_ec"))
    missing_needs = sum(1 for m in members if not m.get("has_needs"))
    no_event_response = sum(1 for m in members if int(m.get("event_responses") or 0) <= 0)
    no_voice = sum(1 for m in members if int(m.get("voice_seconds") or 0) <= 0)

    return {
        "generated_at": _now_iso(),
        "member_count": len(members),
        "quality": {
            "missing_profile": missing_profile,
            "missing_ec": missing_ec,
            "missing_needs": missing_needs,
            "no_event_response": no_event_response,
            "no_voice_time": no_voice,
        },
        "members": members[:800],
        "risk_members": [m for m in members if int(m.get("risk_score") or 0) > 0][:200],
        "needs": {
            "top_main": [{"label": k, "count": v} for k, v in top_main],
            "top_secondary": [{"label": k, "count": v} for k, v in top_secondary],
            "users_without_needs": [m for m in members if not m.get("has_needs")][:200],
            "main_total": sum(main_need_counter.values()),
            "secondary_total": sum(secondary_need_counter.values()),
        },
        "loot": {
            "winner_rows": winners[:200],
            "active_leaders": [{"user_id": uid, "lead_count": cnt} for uid, cnt in sorted(leading_by_uid.items(), key=lambda x: -x[1])[:100]],
        },
    }


def _dashboard_feed_channel_name(kind: str) -> str:
    """Discord-Kanalname für Dashboard-News/Guides aus Runtime-DB oder Env lesen.

    Absichtlich name-basiert, damit der Betreiber per Slash-Command z. B.
    `news` oder `guides` setzen kann, ohne Kanal-IDs aus Discord kopieren zu müssen.
    """
    kind_up = str(kind or "").upper()
    setting = DASHBOARD_GUIDES_CHANNEL_NAME_SETTING if kind_up == "GUIDES" else DASHBOARD_NEWS_CHANNEL_NAME_SETTING
    env_names = [f"DASHBOARD_{kind_up}_CHANNEL_NAME", f"DISCORD_{kind_up}_CHANNEL_NAME", f"TNL_{kind_up}_CHANNEL_NAME"]
    if kind_up == "NEWS":
        env_names.extend(["NEWS_CHANNEL_NAME", "DASHBOARD_TNL_NEWS_CHANNEL_NAME"])
    if kind_up == "GUIDES":
        env_names.extend(["GUIDES_CHANNEL_NAME", "DASHBOARD_TNL_GUIDES_CHANNEL_NAME", "GUIDE_CHANNEL_NAME"])

    # Runtime-DB ist guildbezogen; weil diese Helper-Funktion keine guild_id kennt,
    # wird die eigentliche DB-Lesung in _dashboard_feed_channel_config gemacht.
    return ""


def _dashboard_feed_channel_id(kind: str) -> int:
    kind = str(kind or "").upper()
    names = [f"DASHBOARD_{kind}_CHANNEL_ID", f"DISCORD_{kind}_CHANNEL_ID", f"TNL_{kind}_CHANNEL_ID"]
    if kind == "NEWS":
        names.extend(["NEWS_CHANNEL_ID", "DASHBOARD_TNL_NEWS_CHANNEL_ID"])
    if kind == "GUIDES":
        names.extend(["GUIDES_CHANNEL_ID", "DASHBOARD_TNL_GUIDES_CHANNEL_ID", "GUIDE_CHANNEL_ID"])
    if kind == "ANNOUNCEMENTS":
        names.extend([
            "ANNOUNCEMENTS_CHANNEL_ID",
            "ANNOUNCEMENT_CHANNEL_ID",
            "DASHBOARD_ANNOUNCEMENTS_CHANNEL_ID",
            "GUILD_ANNOUNCEMENTS_CHANNEL_ID",
        ])
    for name in names:
        raw = os.getenv(name, "").strip()
        if not raw:
            continue
        try:
            return int(raw)
        except Exception:
            continue
    return 0


def _dashboard_feed_channel_config(guild_id: int, kind: str) -> dict[str, Any]:
    kind_up = str(kind or "").upper()
    if kind_up == "GUIDES":
        name_setting = DASHBOARD_GUIDES_CHANNEL_NAME_SETTING
        central_id_setting = "guild_channel_guides_id"
    elif kind_up == "ANNOUNCEMENTS":
        name_setting = DASHBOARD_ANNOUNCEMENTS_CHANNEL_NAME_SETTING
        central_id_setting = "guild_channel_announcements_id"
    else:
        name_setting = DASHBOARD_NEWS_CHANNEL_NAME_SETTING
        central_id_setting = "guild_channel_news_id"
    configured_name = ""
    central_id = 0
    try:
        central_id = int(runtime_db.get_guild_setting(int(guild_id), central_id_setting, 0) or 0)
    except Exception:
        central_id = 0
    try:
        configured_name = str(runtime_db.get_guild_setting(int(guild_id), name_setting, "") or "").strip()
    except Exception:
        configured_name = ""

    if not configured_name:
        env_names = [f"DASHBOARD_{kind_up}_CHANNEL_NAME", f"DISCORD_{kind_up}_CHANNEL_NAME", f"TNL_{kind_up}_CHANNEL_NAME"]
        if kind_up == "NEWS":
            env_names.extend(["NEWS_CHANNEL_NAME", "DASHBOARD_TNL_NEWS_CHANNEL_NAME"])
        if kind_up == "GUIDES":
            env_names.extend(["GUIDES_CHANNEL_NAME", "DASHBOARD_TNL_GUIDES_CHANNEL_NAME", "GUIDE_CHANNEL_NAME"])
        if kind_up == "ANNOUNCEMENTS":
            env_names.extend([
                "ANNOUNCEMENTS_CHANNEL_NAME",
                "ANNOUNCEMENT_CHANNEL_NAME",
                "DASHBOARD_ANNOUNCEMENTS_CHANNEL_NAME",
                "GUILD_ANNOUNCEMENTS_CHANNEL_NAME",
            ])
        for env in env_names:
            raw = os.getenv(env, "").strip()
            if raw:
                configured_name = raw
                break

    return {"name": configured_name, "id": central_id or _dashboard_feed_channel_id(kind_up)}


def _normal_channel_name(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("#").replace(" ", "-").replace("_", "-")


def _find_feed_channel_by_name(guild: discord.Guild, raw_name: str) -> Optional[Any]:
    wanted = _normal_channel_name(raw_name)
    if not wanted:
        return None
    # Erst exakter normalisierter Treffer, dann contains-Fallback.
    channels = [ch for ch in getattr(guild, "channels", []) if hasattr(ch, "history")]
    for ch in channels:
        if _normal_channel_name(getattr(ch, "name", "")) == wanted:
            return ch
    for ch in channels:
        if wanted in _normal_channel_name(getattr(ch, "name", "")):
            return ch
    return None

def _message_jump_url(guild_id: int, channel_id: int, message_id: int) -> str:
    if not guild_id or not channel_id or not message_id:
        return ""
    return f"https://discord.com/channels/{int(guild_id)}/{int(channel_id)}/{int(message_id)}"


async def _dashboard_fetch_channel_feed(guild: discord.Guild, *, kind: str, limit: int = 30) -> dict[str, Any]:
    cfg = _dashboard_feed_channel_config(int(guild.id), kind)
    channel_name = str(cfg.get("name") or "").strip()
    channel_id = int(cfg.get("id") or 0)
    out: dict[str, Any] = {"kind": str(kind or "").lower(), "channel_id": int(channel_id or 0), "channel_name": channel_name, "configured_name": channel_name, "configured": bool(channel_name or channel_id), "fetched_at": _now_iso(), "messages": []}
    channel = None
    if channel_name:
        channel = _find_feed_channel_by_name(guild, channel_name)
        if channel is not None:
            channel_id = int(getattr(channel, "id", 0) or 0)
            out["channel_id"] = channel_id
            out["channel_name"] = str(getattr(channel, "name", "") or channel_name)
    if channel is None and channel_id:
        channel = guild.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await guild.fetch_channel(int(channel_id))  # type: ignore[attr-defined]
            except Exception as exc:
                out["error"] = f"Kanal konnte nicht geladen werden: {type(exc).__name__}: {exc}"
                return out
        out["channel_name"] = str(getattr(channel, "name", "") or channel_id)
    if channel is None:
        out["error"] = f"Kein Kanal gesetzt. Nutze das Dashboard unter Gilde & Discord oder /guild set_channel."
        return out
    if not hasattr(channel, "history"):
        out["error"] = "Kanal unterstützt keine Nachrichten-History."
        return out
    rows: list[dict[str, Any]] = []
    try:
        async for msg in channel.history(limit=int(os.getenv("DASHBOARD_FEED_LIMIT", str(limit)) or limit), oldest_first=False):  # type: ignore[attr-defined]
            content = _safe_text(getattr(msg, "clean_content", "") or getattr(msg, "content", ""), limit=2500)
            attachments = []
            for a in getattr(msg, "attachments", []) or []:
                attachments.append({"filename": _safe_text(getattr(a, "filename", "Anhang"), 160), "url": str(getattr(a, "url", "") or ""), "content_type": str(getattr(a, "content_type", "") or ""), "size": int(getattr(a, "size", 0) or 0)})
            embeds = []
            for e in getattr(msg, "embeds", []) or []:
                try:
                    embeds.append({"title": _safe_text(getattr(e, "title", "") or "", 240), "description": _safe_text(getattr(e, "description", "") or "", 1600), "url": str(getattr(e, "url", "") or ""), "image_url": str(getattr(getattr(e, "image", None), "url", "") or ""), "thumbnail_url": str(getattr(getattr(e, "thumbnail", None), "url", "") or "")})
                except Exception:
                    continue
            if not content and not attachments and not embeds:
                continue
            author = getattr(msg, "author", None)
            avatar_url = ""
            try:
                avatar_url = str(getattr(getattr(author, "display_avatar", None), "url", "") or "")
            except Exception:
                avatar_url = ""
            rows.append({"message_id": int(getattr(msg, "id", 0) or 0), "channel_id": int(getattr(getattr(msg, "channel", None), "id", channel_id) or channel_id), "created_at": getattr(getattr(msg, "created_at", None), "isoformat", lambda: "")(), "edited_at": getattr(getattr(msg, "edited_at", None), "isoformat", lambda: "")() if getattr(msg, "edited_at", None) else "", "author_id": int(getattr(author, "id", 0) or 0), "author_name": _safe_text(getattr(author, "display_name", None) or getattr(author, "name", None) or "Discord", 120), "author_avatar_url": avatar_url, "content": content, "attachments": attachments[:6], "embeds": embeds[:4], "jump_url": _message_jump_url(int(guild.id), int(channel_id), int(getattr(msg, "id", 0) or 0))})
    except Exception as exc:
        out["error"] = f"Nachrichten konnten nicht gelesen werden: {type(exc).__name__}: {exc}"
    out["messages"] = rows
    out["count"] = len(rows)
    return out


async def _dashboard_discord_feeds_snapshot(guild: discord.Guild) -> dict[str, Any]:
    return {
        "news": await _dashboard_fetch_channel_feed(guild, kind="news"),
        "guides": await _dashboard_fetch_channel_feed(guild, kind="guides"),
        "announcements": await _dashboard_fetch_channel_feed(guild, kind="announcements"),
    }


_EVENT_MEDIA_CACHE: dict[int, tuple[float, str]] = {}


def _message_event_image_url(message: Any) -> str:
    """Bevorzugt Discords frische Proxy-URL des tatsächlich geposteten Bildes."""
    candidates: list[Any] = []
    for attachment in getattr(message, "attachments", []) or []:
        content_type = str(getattr(attachment, "content_type", "") or "").lower()
        filename = str(getattr(attachment, "filename", "") or "").lower()
        if content_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
            candidates.extend([getattr(attachment, "proxy_url", None), getattr(attachment, "url", None)])
    for embed in getattr(message, "embeds", []) or []:
        image = getattr(embed, "image", None)
        thumbnail = getattr(embed, "thumbnail", None)
        candidates.extend([
            getattr(image, "proxy_url", None), getattr(image, "url", None),
            getattr(thumbnail, "proxy_url", None), getattr(thumbnail, "url", None),
        ])
    for raw in candidates:
        url = _stable_discord_media_url(raw)
        if url:
            return url
    return ""


async def _dashboard_hydrate_event_media(guild: discord.Guild, snapshot: dict[str, Any]) -> None:
    """Event-Titelbilder aus den echten Discord-Eventposts in den Snapshot übernehmen.

    Der Event-JSON-Eintrag kann eine alte/abgelaufene Bild-URL enthalten. Der
    Discord-Post selbst besitzt dagegen eine aktuelle Embed-Proxy-URL. Es werden
    nur die neuesten Events geprüft und Ergebnisse 25 Minuten gecacht, damit die
    5-Minuten-Snapshot-Schleife Discord nicht unnötig belastet.
    """
    events = ((snapshot.get("events") or {}).get("items") or [])
    if not isinstance(events, list):
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    try:
        max_events = max(1, min(80, int(os.getenv("DASHBOARD_EVENT_IMAGE_FETCH_LIMIT", "40") or 40)))
    except Exception:
        max_events = 40
    for event in events[:max_events]:
        if not isinstance(event, dict):
            continue
        try:
            message_id = int(event.get("event_id") or event.get("message_id") or 0)
            channel_id = int(event.get("channel_id") or 0)
        except Exception:
            continue
        if not message_id or not channel_id:
            continue
        cached = _EVENT_MEDIA_CACHE.get(message_id)
        if cached and now_ts - float(cached[0]) < 1500 and cached[1]:
            event["image_url"] = cached[1]
            continue
        channel = guild.get_channel(channel_id)
        if channel is None:
            try:
                channel = await guild.fetch_channel(channel_id)  # type: ignore[attr-defined]
            except Exception:
                channel = None
        if channel is None or not hasattr(channel, "fetch_message"):
            continue
        try:
            message = await channel.fetch_message(message_id)  # type: ignore[attr-defined]
            image_url = _message_event_image_url(message)
            if image_url:
                event["image_url"] = image_url
                event["discord_image_url"] = image_url
                event["jump_url"] = str(getattr(message, "jump_url", "") or _message_jump_url(int(guild.id), channel_id, message_id))
                _EVENT_MEDIA_CACHE[message_id] = (now_ts, image_url)
        except Exception:
            # Gespeicherte image_url bleibt als Fallback erhalten.
            continue


async def _publish_snapshot_with_feeds(bot: commands.Bot, guild: discord.Guild) -> int:
    snapshot = await asyncio.to_thread(build_dashboard_snapshot, bot, guild)
    try:
        await _dashboard_hydrate_event_media(guild, snapshot)
    except Exception as media_exc:
        print(f"⚠️ Eventbilder konnten nicht aktualisiert werden: {media_exc!r}")
    try:
        snapshot["discord_feeds"] = await _dashboard_discord_feeds_snapshot(guild)
    except Exception as feed_exc:
        snapshot["discord_feeds"] = {"error": f"{type(feed_exc).__name__}: {feed_exc}"}
    return int(await asyncio.to_thread(runtime_db.save_dashboard_snapshot, guild_id=int(guild.id), guild_name=str(((snapshot.get("guild") or {}).get("name") or guild.name)), snapshot=snapshot) or 0)

def _central_guild_snapshot(guild: discord.Guild) -> dict[str, Any]:
    try:
        if central_guild_config is not None:
            return central_guild_config.guild_config_snapshot(guild)
    except Exception as exc:
        print(f"⚠️ Zentrale Gildenkonfiguration nicht lesbar: {exc!r}", flush=True)
    profile = {
        "guild_id": int(guild.id),
        "discord_name": str(guild.name),
        "display_name": str(guild.name),
        "short_name": str(guild.name),
        "bot_display_name": f"{guild.name} Knecht",
        "timezone": "Europe/Berlin",
        "logo_url": "",
        "banner_url": "",
        "accent_color": "#d6a84f",
        "invite_url": "",
        "status": "active",
    }
    return {"profile": profile, "roles": {}, "channels": {}}


def _discord_configuration_catalog(guild: discord.Guild) -> dict[str, Any]:
    roles = []
    for role in sorted(getattr(guild, "roles", []), key=lambda r: int(getattr(r, "position", 0)), reverse=True):
        if getattr(role, "is_default", lambda: False)():
            continue
        roles.append({
            "id": int(role.id),
            "name": str(role.name),
            "position": int(getattr(role, "position", 0)),
            "member_count": sum(1 for m in (getattr(role, "members", []) or []) if not bool(getattr(m, "bot", False))),
        })
    channels = []
    for ch in getattr(guild, "channels", []):
        kind = "other"
        if isinstance(ch, discord.TextChannel):
            kind = "text"
        elif isinstance(ch, discord.VoiceChannel):
            kind = "voice"
        elif isinstance(ch, discord.CategoryChannel):
            kind = "category"
        elif isinstance(ch, discord.ForumChannel):
            kind = "forum"
        channels.append({
            "id": int(ch.id),
            "name": str(ch.name),
            "kind": kind,
            "position": int(getattr(ch, "position", 0)),
        })
    channels.sort(key=lambda row: (row.get("kind", ""), int(row.get("position", 0)), row.get("name", "")))
    return {"roles": roles, "channels": channels}


def build_dashboard_snapshot(bot: commands.Bot, guild: discord.Guild) -> dict[str, Any]:
    """Read-only Daten-Snapshot für das spätere Web-Dashboard.

    Diese Funktion schreibt keine Bot-Daten um. Sie liest nur bestehende JSON-Dateien
    und die Runtime/Postgres-Datenbank. Das Dashboard kann diese Funktion später direkt
    importieren oder denselben Schema-Output über eine Web-API ausliefern.
    """
    guild_id = int(guild.id)
    central_config = _central_guild_snapshot(guild)
    guild_profile = central_config.get("profile") if isinstance(central_config, dict) else {}
    if not isinstance(guild_profile, dict):
        guild_profile = {}
    configured_name = str(guild_profile.get("display_name") or guild.name)
    sources = {key: _load_json_file(filename, {}) for key, filename in JSON_SOURCES.items()}
    status = runtime_db.db_status()

    profile_summary = _summarize_profiles(sources.get("member_profiles"), guild)
    member_sync = _sync_member_directory(guild, profile_summary)

    snapshot = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "guild": {
            "id": guild_id,
            "name": configured_name,
            "discord_name": guild.name,
            "profile": guild_profile,
            "configuration": central_config,
            "discord_catalog": _discord_configuration_catalog(guild),
            "member_count_cache": getattr(guild, "member_count", None),
            "cached_members_loaded": len(_active_member_ids(guild)),
            "member_filter": _dashboard_member_filter_info(guild),
            "bot_user_id": int(getattr(getattr(bot, "user", None), "id", 0) or 0),
        },
        "storage": {
            "runtime_backend": status.get("backend"),
            "runtime_path": status.get("path"),
            "database_url_configured": bool(status.get("database_url_configured")),
            "database_url_kind": status.get("database_url_kind"),
        },
        "source_health": _source_health(),
        "guild_config": central_config,
        "auth": _dashboard_auth_info(guild),
        "profiles": profile_summary,
        "members": profile_summary,
        "member_directory": member_sync,
        "events": _summarize_events(sources.get("events"), guild),
        "event_checks": _summarize_event_checks(sources.get("dkp_event_checks"), guild_id),
        "ec": {
            "balances": _summarize_balances(sources.get("dkp_balances"), guild),
            "transactions": _summarize_transactions(sources.get("dkp_transactions"), guild),
        },
        "loot": {
            "needs": _summarize_needs(sources.get("loot_needs"), guild, sources.get("loot_items")),
            "auctions": _summarize_auctions(sources.get("loot_auctions"), guild),
            "items_known": len(_catalog_items_for_guild(sources.get("loot_items"), guild_id)),
        },
        "settings": _summarize_settings(sources, guild),
        "voice": _voice_summary(guild_id),
        "audit": _audit_summary(guild_id),
    }
    snapshot["insights"] = _dashboard_insights(guild, snapshot)
    return snapshot


def publish_dashboard_snapshot(bot: commands.Bot, guild: discord.Guild) -> int:
    """Schreibt den aktuellen read-only Snapshot in Postgres/Runtime-DB.

    Der separate Web-Service liest genau diese Tabelle. Produktive Bot-Daten werden
    nicht verändert; die JSON-Dateien bleiben weiterhin Quelle der alten Systeme.
    """
    snapshot = build_dashboard_snapshot(bot, guild)
    return int(runtime_db.save_dashboard_snapshot(guild_id=int(guild.id), guild_name=str(((snapshot.get("guild") or {}).get("name") or guild.name)), snapshot=snapshot) or 0)


def write_dashboard_export(bot: commands.Bot, guild: discord.Guild) -> Path:
    snapshot = build_dashboard_snapshot(bot, guild)
    # Zusätzlich in die Runtime-DB schreiben, damit das separate Web-Dashboard
    # nach einem manuellen Export sofort aktuelle Daten bekommt.
    try:
        runtime_db.save_dashboard_snapshot(guild_id=int(guild.id), guild_name=str(((snapshot.get("guild") or {}).get("name") or guild.name)), snapshot=snapshot)
    except Exception as exc:
        print(f"⚠️ Dashboard-Snapshot konnte nicht in Runtime-DB veröffentlicht werden: {exc!r}")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = EXPORT_DIR / f"dashboard_snapshot_{guild.id}_{ts}.json"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
    return path


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


@tasks.loop(minutes=5)
async def _dashboard_publish_loop(bot: commands.Bot):
    await bot.wait_until_ready()
    if not bot.guilds:
        return
    for guild in list(bot.guilds):
        try:
            # Snapshot-Erstellung liest viele JSON-/DB-Daten. Im Thread ausführen,
            # damit Discord-Interaktionen nicht blockieren und nicht mit
            # "Die Anwendung reagiert nicht" enden. Discord-News/Guides werden
            # danach asynchron aus den konfigurierten Kanälen ergänzt.
            snapshot = await asyncio.to_thread(build_dashboard_snapshot, bot, guild)
            try:
                await _dashboard_hydrate_event_media(guild, snapshot)
            except Exception as media_exc:
                print(f"⚠️ Eventbilder für {getattr(guild, 'id', '?')} konnten nicht aktualisiert werden: {media_exc!r}")
            try:
                snapshot["discord_feeds"] = await _dashboard_discord_feeds_snapshot(guild)
            except Exception as feed_exc:
                snapshot["discord_feeds"] = {"error": f"{type(feed_exc).__name__}: {feed_exc}"}
            sid = await asyncio.to_thread(runtime_db.save_dashboard_snapshot, guild_id=int(guild.id), guild_name=str(((snapshot.get("guild") or {}).get("name") or guild.name)), snapshot=snapshot)
            print(f"📊 Dashboard-Snapshot veröffentlicht: {guild.name} ({guild.id}) snapshot_id={sid}")
        except Exception as exc:
            print(f"⚠️ Dashboard-Snapshot für {getattr(guild, 'id', '?')} fehlgeschlagen: {exc!r}")


@_dashboard_publish_loop.before_loop
async def _dashboard_publish_before_loop():
    # Kurz warten, damit andere Module/JSON-Lader nach on_ready vollständig stehen.
    import asyncio
    await asyncio.sleep(20)


def start_dashboard_publisher(bot: commands.Bot) -> None:
    if getattr(bot, "_ebo_dashboard_publisher_started", False):
        return
    try:
        _dashboard_publish_loop.start(bot)
        setattr(bot, "_ebo_dashboard_publisher_started", True)
        print("📊 Dashboard-Snapshot-Publisher gestartet: alle 5 Minuten.")
    except RuntimeError:
        pass


async def setup_dashboard_data(bot: commands.Bot, tree: app_commands.CommandTree):
    start_dashboard_publisher(bot)
    @tree.command(name="dashboard_set_member_role", description="Legt die Gildenrolle fest, die im Dashboard als Mitglied zählt.")
    @app_commands.describe(role="Rolle, die echte Gildenmitglieder haben müssen")
    async def dashboard_set_member_role(inter: discord.Interaction, role: discord.Role):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)
        try:
            runtime_db.set_guild_setting(int(inter.guild.id), DASHBOARD_MEMBER_ROLE_SETTING, int(role.id))
            try:
                runtime_db.write_audit_log(
                    guild_id=int(inter.guild.id),
                    actor_id=int(inter.user.id),
                    action="dashboard_member_role_set",
                    target_type="role",
                    target_id=str(role.id),
                    summary=f"Dashboard-Gildenrolle gesetzt: {role.name}",
                    new_value={"role_id": int(role.id), "role_name": role.name},
                )
            except Exception:
                pass
            snap = await asyncio.to_thread(build_dashboard_snapshot, bot, inter.guild)
            try:
                await asyncio.to_thread(runtime_db.save_dashboard_snapshot, guild_id=int(inter.guild.id), guild_name=str(((snap.get("guild") or {}).get("name") or inter.guild.name)), snapshot=snap)
            except Exception:
                pass
            count = (snap.get("guild", {}).get("member_filter") or {}).get("eligible_count", len(getattr(role, "members", []) or []))
            await inter.followup.send(
                f"✅ Dashboard-Gildenrolle gesetzt: {role.mention}\n"
                f"Rollenmitglieder im Dashboard: **{count}**\n"
                "Dashboard wurde aktualisiert.",
                ephemeral=True,
            )
        except Exception as exc:
            await inter.followup.send(f"❌ Konnte Rolle nicht speichern: `{type(exc).__name__}: {exc}`", ephemeral=True)



    @tree.command(name="dashboard_set_admin_role", description="Legt die Dashboard-Adminrolle fest.")
    @app_commands.describe(role="Rolle, die im Dashboard als Admin/Leitung zählt")
    async def dashboard_set_admin_role(inter: discord.Interaction, role: discord.Role):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)
        try:
            # Als Liste speichern, damit später mehrere Adminrollen möglich sind.
            runtime_db.set_guild_setting(int(inter.guild.id), DASHBOARD_ADMIN_ROLE_SETTING, [int(role.id)])
            try:
                runtime_db.write_audit_log(
                    guild_id=int(inter.guild.id),
                    actor_id=int(inter.user.id),
                    action="dashboard_admin_role_set",
                    target_type="role",
                    target_id=str(role.id),
                    summary=f"Dashboard-Adminrolle gesetzt: {role.name}",
                    new_value={"role_ids": [int(role.id)], "role_name": role.name},
                )
            except Exception:
                pass
            snap = await asyncio.to_thread(build_dashboard_snapshot, bot, inter.guild)
            try:
                await asyncio.to_thread(runtime_db.save_dashboard_snapshot, guild_id=int(inter.guild.id), guild_name=str(((snap.get("guild") or {}).get("name") or inter.guild.name)), snapshot=snap)
            except Exception:
                pass
            admin_info = (snap.get("auth") or {}).get("counts") or {}
            await inter.followup.send(
                f"✅ Dashboard-Adminrolle gesetzt: {role.mention}\n"
                f"Admin-Mitglieder im Dashboard: **{admin_info.get('admin_members', len(getattr(role, 'members', []) or []))}**\n"
                "Dashboard wurde aktualisiert.",
                ephemeral=True,
            )
        except Exception as exc:
            await inter.followup.send(f"❌ Konnte Adminrolle nicht speichern: `{type(exc).__name__}: {exc}`", ephemeral=True)

    @tree.command(name="dashboard_set_feed_channel", description="Setzt den Discord-Kanal für News, Guides oder Ankündigungen im Dashboard.")
    @app_commands.describe(feed="News, Guides oder Ankündigungen", channel_name="Kanalname ohne ID, z. B. news, guides oder ankündigungen")
    @app_commands.choices(feed=[
        app_commands.Choice(name="News", value="news"),
        app_commands.Choice(name="Guides", value="guides"),
        app_commands.Choice(name="Ankündigungen", value="announcements"),
    ])
    async def dashboard_set_feed_channel(inter: discord.Interaction, feed: app_commands.Choice[str], channel_name: str):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        clean_name = str(channel_name or "").strip().lstrip("#")
        if not clean_name:
            await inter.response.send_message("❌ Bitte einen Kanalnamen angeben, z. B. `news` oder `guides`.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        kind = str(feed.value or "news").lower()
        if kind == "guides":
            setting = DASHBOARD_GUIDES_CHANNEL_NAME_SETTING
        elif kind == "announcements":
            setting = DASHBOARD_ANNOUNCEMENTS_CHANNEL_NAME_SETTING
        else:
            setting = DASHBOARD_NEWS_CHANNEL_NAME_SETTING
        channel = _find_feed_channel_by_name(inter.guild, clean_name)
        if channel is None:
            names = sorted(str(getattr(ch, "name", "")) for ch in getattr(inter.guild, "channels", []) if hasattr(ch, "history") and getattr(ch, "name", ""))
            sample = ", ".join(f"#{n}" for n in names[:15]) or "keine lesbaren Textkanäle gefunden"
            await inter.followup.send(f"❌ Kanal `#{clean_name}` nicht gefunden. Sichtbare Textkanäle: {sample}", ephemeral=True)
            return

        try:
            runtime_db.set_guild_setting(int(inter.guild.id), setting, str(getattr(channel, "name", clean_name)))
            try:
                runtime_db.write_audit_log(
                    guild_id=int(inter.guild.id),
                    actor_id=int(inter.user.id),
                    action="dashboard_feed_channel_set",
                    target_type="channel",
                    target_id=str(getattr(channel, "id", "")),
                    summary=f"Dashboard-{kind}-Kanal gesetzt: #{getattr(channel, 'name', clean_name)}",
                    new_value={"feed": kind, "channel_name": str(getattr(channel, "name", clean_name)), "channel_id_resolved": int(getattr(channel, "id", 0) or 0)},
                )
            except Exception:
                pass
            try:
                await _publish_snapshot_with_feeds(bot, inter.guild)
            except Exception as pub_exc:
                await inter.followup.send(f"✅ Dashboard-{kind}-Kanal gesetzt: {getattr(channel, 'mention', '#' + clean_name)}\n⚠️ Snapshot konnte noch nicht aktualisiert werden: `{type(pub_exc).__name__}: {pub_exc}`", ephemeral=True)
                return
            await inter.followup.send(
                f"✅ Dashboard-{kind}-Kanal gesetzt: {getattr(channel, 'mention', '#' + clean_name)}\n"
                "Die Website liest ab jetzt diesen Kanal nach Namen. Nachrichten erscheinen nach dem nächsten Snapshot bzw. jetzt direkt nach der Aktualisierung.",
                ephemeral=True,
            )
        except Exception as exc:
            await inter.followup.send(f"❌ Konnte Feed-Kanal nicht speichern: `{type(exc).__name__}: {exc}`", ephemeral=True)

    @tree.command(name="dashboard_status", description="Zeigt den Status der read-only Dashboard-Datenbasis.")
    async def dashboard_status(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        try:
            # Nicht im Discord-Eventloop bauen: der Snapshot kann bei vielen
            # Profilen/Needs/Auktionen/Voice-Sessions kurz dauern.
            snap = await asyncio.to_thread(build_dashboard_snapshot, bot, inter.guild)
            await _dashboard_hydrate_event_media(inter.guild, snap)
        except Exception as exc:
            await inter.followup.send(f"❌ Dashboard-Status fehlgeschlagen: `{type(exc).__name__}: {exc}`", ephemeral=True)
            return
        try:
            await asyncio.to_thread(runtime_db.save_dashboard_snapshot, guild_id=int(inter.guild.id), guild_name=str(((snap.get("guild") or {}).get("name") or inter.guild.name)), snapshot=snap)
        except Exception as exc:
            print(f"⚠️ Dashboard-Status Snapshot-Publish fehlgeschlagen: {exc!r}")
        bad_sources = [k for k, v in snap.get("source_health", {}).items() if v.get("exists") and not v.get("ok")]
        missing_sources = [k for k, v in snap.get("source_health", {}).items() if not v.get("exists")]

        emb = discord.Embed(
            title="📊 Dashboard-Datenbasis",
            description="Read-only Snapshot für das spätere Web-Dashboard. Bestehende Bot-Daten werden dabei nicht verändert.",
            color=0xD6A84F,
        )
        emb.add_field(name="Backend", value=str(snap["storage"].get("runtime_backend")), inline=True)
        emb.add_field(name="DATABASE_URL", value="gesetzt" if snap["storage"].get("database_url_configured") else "nicht gesetzt", inline=True)
        emb.add_field(name="Guild", value=f"{inter.guild.name}\n`{inter.guild.id}`", inline=False)
        mf = snap.get("guild", {}).get("member_filter") or {}
        if isinstance(mf, dict) and mf.get("mode") == "discord_role":
            filter_head = f"Nur Mitglieder mit Rolle: {mf.get('role_name')} (`{mf.get('role_id')}`)\nRollenmitglieder: {mf.get('eligible_count', 0)}"
        else:
            filter_head = f"Nur aktuelle Discord-Servermitglieder fallback\nGefunden: {mf.get('eligible_count', 0) if isinstance(mf, dict) else '?'}"
        emb.add_field(name="Filter", value=f"{filter_head}\nAlt/ausgefiltert: Profile {snap['profiles'].get('stale_count', 0)} · EC {snap['ec']['balances'].get('stale_count', 0)} · Needs {snap['loot']['needs'].get('stale_count', 0)}", inline=False)
        emb.add_field(name="Profile", value=str(snap["profiles"].get("count", 0)), inline=True)
        emb.add_field(name="Events", value=str(snap["events"].get("count", 0)), inline=True)
        emb.add_field(name="EC-Konten", value=str(snap["ec"]["balances"].get("count", 0)), inline=True)
        emb.add_field(name="Auktionen", value=str(snap["loot"]["auctions"].get("count", 0)), inline=True)
        emb.add_field(name="Need-User", value=str(snap["loot"]["needs"].get("user_count", 0)), inline=True)
        emb.add_field(name="Voice-Sessions", value=str(snap["voice"].get("sessions_total", 0)), inline=True)
        emb.add_field(name="Audit-Logs", value=str(snap["audit"].get("logs_total", 0)), inline=True)
        try:
            latest_pub = runtime_db.fetch_latest_dashboard_snapshot(inter.guild.id)
            pub_count = runtime_db.count_dashboard_snapshots(inter.guild.id)
            pub_txt = f"Snapshots: {pub_count}\nLetzter: {(latest_pub or {}).get('published_at') or 'noch keiner'}"
        except Exception as exc:
            pub_txt = f"Fehler: {type(exc).__name__}"
        emb.add_field(name="Web-Dashboard", value=pub_txt, inline=True)
        emb.add_field(name="JSON-Quellen", value=f"OK: {len(snap.get('source_health', {})) - len(bad_sources)}\nFehler: {len(bad_sources)}\nFehlen: {len(missing_sources)}", inline=True)
        if bad_sources:
            emb.add_field(name="Fehlerhafte Quellen", value="\n".join(f"• {x}" for x in bad_sources[:10]), inline=False)
        emb.set_footer(text="Dashboard ist read-only. Alte JSON-Daten werden nur gefiltert, nicht gelöscht.")
        await inter.followup.send(embed=emb, ephemeral=True)

    @tree.command(name="dashboard_export", description="Erstellt einen read-only JSON-Export für das spätere Dashboard.")
    async def dashboard_export(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        try:
            path = await asyncio.to_thread(write_dashboard_export, bot, inter.guild)
            await inter.followup.send(
                "✅ Dashboard-Snapshot erstellt. Das ist nur ein Export/Lesemodell, keine Datenmigration.",
                file=discord.File(str(path), filename=path.name),
                ephemeral=True,
            )
        except Exception as exc:
            await inter.followup.send(f"❌ Dashboard-Export fehlgeschlagen: `{type(exc).__name__}: {exc}`", ephemeral=True)

    @tree.command(name="dashboard_sources", description="Zeigt, welche JSON-Quellen fürs Dashboard gefunden werden.")
    async def dashboard_sources(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        health = _source_health()
        lines = []
        for key, info in health.items():
            if info.get("exists") and info.get("ok"):
                icon = "✅"
            elif info.get("exists"):
                icon = "⚠️"
            else:
                icon = "—"
            size = int(info.get("size_bytes") or 0)
            lines.append(f"{icon} `{info.get('file')}` — {key} — {size} B")
        txt = "\n".join(lines) or "Keine Quellen definiert."
        if len(txt) > 3900:
            txt = txt[:3900] + "\n… gekürzt"
        emb = discord.Embed(title="📁 Dashboard-Quellen", description=txt, color=0xD6A84F)
        await inter.response.send_message(embed=emb, ephemeral=True)

    print("📊 Dashboard-Datenlayer registriert: Mitglieder-/Adminrolle, News/Guides/Ankündigungen-Feeds, Status und Export")
