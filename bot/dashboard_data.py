from __future__ import annotations

import json
import os
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


def _dashboard_member_ids(guild: discord.Guild) -> set[int]:
    role = _dashboard_member_role(guild)
    if role is not None:
        return {int(m.id) for m in getattr(role, "members", []) if getattr(m, "id", None)}
    # Absichtlich kein Fallback auf alle Servermitglieder. Für Massentauglichkeit
    # muss eine Gildenrolle gesetzt sein.
    return set()


def _dashboard_member_filter_info(guild: discord.Guild) -> dict[str, Any]:
    role = _dashboard_member_role(guild)
    ids = _dashboard_member_ids(guild)
    configured = _dashboard_member_role_config_value(guild.id)
    if role is not None:
        return {
            "mode": "discord_role",
            "role_id": int(role.id),
            "role_name": str(role.name),
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
                "title": _safe_text(ev.get("title") or ev.get("name") or ev.get("event_name"), 160),
                "when_iso": str(ev.get("when_iso") or ev.get("start_time") or ev.get("time") or ""),
                "channel_id": int(ev.get("channel_id", 0) or 0),
                "scope": str(ev.get("scope") or "single"),
                "is_mirror_for_this_guild": bool(mirror_match and home_gid != int(guild_id)),
                "voice_enabled": bool(ev.get("voice_enabled")),
                "voice_channel_id": int(ev.get("voice_channel_id", 0) or 0),
                "voice_last_channel_id": int(ev.get("voice_last_channel_id", 0) or 0),
                "yes_counts": role_counts,
                "maybe_count": len(participant_detail.get("maybe") or []),
                "no_count": len(participant_detail.get("no") or []),
                "participant_count": _event_user_count(ev),
                "participants": participant_detail,
                "description": _safe_text(ev.get("description") or ev.get("desc") or "", 600),
            })
    events.sort(key=lambda x: str(x.get("when_iso") or ""), reverse=True)
    return {
        "count": len(events),
        "items": events[:limit],
    }


def _summarize_profiles(data: Any, guild: discord.Guild, *, limit: int = 500) -> dict[str, Any]:
    g = _guild_dict(data, guild.id)
    users = g.get("users") if isinstance(g.get("users"), dict) else {}
    absences = g.get("absences") if isinstance(g.get("absences"), dict) else {}
    items: list[dict[str, Any]] = []
    stale_count = 0
    for uid, profile in users.items():
        if not isinstance(profile, dict):
            continue
        try:
            user_id = int(uid)
        except Exception:
            continue
        member = guild.get_member(user_id)
        if not _is_dashboard_member(guild, user_id):
            stale_count += 1
            continue
        items.append({
            "user_id": user_id,
            "display_name": _safe_text(getattr(member, "display_name", "") if member is not None else profile.get("ingame_name") or f"User {user_id}", 120),
            "ingame_name": _safe_text(profile.get("ingame_name"), 120),
            "main_role": _safe_text(profile.get("main_role"), 80),
            "gearscore": _safe_text(profile.get("gearscore"), 40),
            "created_at": str(profile.get("created_at") or ""),
            "in_discord_cache": True,
        })
    items.sort(key=lambda x: (str(x.get("display_name") or "")).lower())
    return {
        "count": len(items),
        "total_json_count": len(users),
        "stale_count": stale_count,
        "absences_count": len(absences),
        "items": items[:limit],
        "filter": "dashboard_member_role_only",
    }


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

        items.append({
            "auction_id": str(aid),
            "item_id": str(auc.get("item_id") or ""),
            "item_name": _safe_text(auc.get("item_name") or auc.get("item") or auc.get("title"), 180),
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

            main_needs = _extract_need_tab(raw, {"main", "Main", "main_needs", "mainNeeds", "haupt", "mainspec"}, catalog)
            secondary_needs = _extract_need_tab(raw, {"secondary", "Secondary", "sec", "secondary_needs", "secondaryNeeds", "zweite", "zweitspec", "offspec"}, catalog)

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
        return {
            "sessions_total": runtime_db.count_voice_sessions(guild_id),
            "sessions_open": runtime_db.count_voice_sessions(guild_id, open_only=True),
            "recent_sessions": runtime_db.fetch_voice_sessions(guild_id, limit=20),
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "sessions_total": 0, "sessions_open": 0, "recent_sessions": []}


def _audit_summary(guild_id: int) -> dict[str, Any]:
    try:
        return {
            "logs_total": runtime_db.count_audit_logs(guild_id),
            "recent_logs": runtime_db.fetch_audit_logs(guild_id, limit=20),
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "logs_total": 0, "recent_logs": []}


def build_dashboard_snapshot(bot: commands.Bot, guild: discord.Guild) -> dict[str, Any]:
    """Read-only Daten-Snapshot für das spätere Web-Dashboard.

    Diese Funktion schreibt keine Bot-Daten um. Sie liest nur bestehende JSON-Dateien
    und die Runtime/Postgres-Datenbank. Das Dashboard kann diese Funktion später direkt
    importieren oder denselben Schema-Output über eine Web-API ausliefern.
    """
    guild_id = int(guild.id)
    sources = {key: _load_json_file(filename, {}) for key, filename in JSON_SOURCES.items()}
    status = runtime_db.db_status()

    snapshot = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "guild": {
            "id": guild_id,
            "name": guild.name,
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
        "profiles": _summarize_profiles(sources.get("member_profiles"), guild),
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
        "voice": _voice_summary(guild_id),
        "audit": _audit_summary(guild_id),
    }
    return snapshot


def publish_dashboard_snapshot(bot: commands.Bot, guild: discord.Guild) -> int:
    """Schreibt den aktuellen read-only Snapshot in Postgres/Runtime-DB.

    Der separate Web-Service liest genau diese Tabelle. Produktive Bot-Daten werden
    nicht verändert; die JSON-Dateien bleiben weiterhin Quelle der alten Systeme.
    """
    snapshot = build_dashboard_snapshot(bot, guild)
    return int(runtime_db.save_dashboard_snapshot(guild_id=int(guild.id), guild_name=guild.name, snapshot=snapshot) or 0)


def write_dashboard_export(bot: commands.Bot, guild: discord.Guild) -> Path:
    snapshot = build_dashboard_snapshot(bot, guild)
    # Zusätzlich in die Runtime-DB schreiben, damit das separate Web-Dashboard
    # nach einem manuellen Export sofort aktuelle Daten bekommt.
    try:
        runtime_db.save_dashboard_snapshot(guild_id=int(guild.id), guild_name=guild.name, snapshot=snapshot)
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
            sid = publish_dashboard_snapshot(bot, guild)
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
            snap = build_dashboard_snapshot(bot, inter.guild)
            try:
                runtime_db.save_dashboard_snapshot(guild_id=int(inter.guild.id), guild_name=inter.guild.name, snapshot=snap)
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

    @tree.command(name="dashboard_status", description="Zeigt den Status der read-only Dashboard-Datenbasis.")
    async def dashboard_status(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)
        snap = build_dashboard_snapshot(bot, inter.guild)
        try:
            runtime_db.save_dashboard_snapshot(guild_id=int(inter.guild.id), guild_name=inter.guild.name, snapshot=snap)
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

        await inter.response.defer(ephemeral=True)
        try:
            path = write_dashboard_export(bot, inter.guild)
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

    print("📊 Dashboard-Datenlayer registriert: /dashboard_set_member_role, /dashboard_status, /dashboard_export, /dashboard_sources")
