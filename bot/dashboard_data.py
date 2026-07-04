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


def _event_user_count(event: dict[str, Any]) -> int:
    ids: set[int] = set()
    yes = event.get("yes") if isinstance(event.get("yes"), dict) else {}
    for arr in yes.values():
        if not isinstance(arr, list):
            continue
        for raw in arr:
            try:
                uid = int(raw.get("id") if isinstance(raw, dict) else raw)
                if uid:
                    ids.add(uid)
            except Exception:
                continue
    maybe = event.get("maybe") if isinstance(event.get("maybe"), dict) else {}
    for uid in maybe.keys():
        try:
            ids.add(int(uid))
        except Exception:
            pass
    no = event.get("no") if isinstance(event.get("no"), list) else []
    for raw in no:
        try:
            uid = int(raw.get("id") if isinstance(raw, dict) else raw)
            if uid:
                ids.add(uid)
        except Exception:
            continue
    return len(ids)


def _summarize_events(data: Any, guild_id: int, *, limit: int = 30) -> dict[str, Any]:
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
            events.append({
                "event_id": str(message_id),
                "title": _safe_text(ev.get("title"), 160),
                "when_iso": str(ev.get("when_iso") or ""),
                "channel_id": int(ev.get("channel_id", 0) or 0),
                "scope": str(ev.get("scope") or "single"),
                "is_mirror_for_this_guild": bool(mirror_match and home_gid != int(guild_id)),
                "voice_enabled": bool(ev.get("voice_enabled")),
                "voice_channel_id": int(ev.get("voice_channel_id", 0) or 0),
                "voice_last_channel_id": int(ev.get("voice_last_channel_id", 0) or 0),
                "yes_counts": role_counts,
                "maybe_count": len(ev.get("maybe") or {}) if isinstance(ev.get("maybe"), dict) else 0,
                "no_count": len(ev.get("no") or []) if isinstance(ev.get("no"), list) else 0,
                "participant_count": _event_user_count(ev),
            })
    events.sort(key=lambda x: str(x.get("when_iso") or ""), reverse=True)
    return {
        "count": len(events),
        "items": events[:limit],
    }


def _summarize_profiles(data: Any, guild: discord.Guild, *, limit: int = 50) -> dict[str, Any]:
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


def _summarize_balances(data: Any, guild: discord.Guild, *, limit: int = 50) -> dict[str, Any]:
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


def _summarize_transactions(data: Any, guild_id: int, *, limit: int = 50) -> dict[str, Any]:
    arr = _guild_list(data, guild_id)
    items = []
    for tx in reversed(arr[-limit:]):
        if isinstance(tx, dict):
            items.append({
                "created_at": str(tx.get("created_at") or tx.get("at") or ""),
                "user_id": int(tx.get("user_id", 0) or tx.get("target_user_id", 0) or 0),
                "amount": tx.get("amount", tx.get("delta", "")),
                "reason": _safe_text(tx.get("reason") or tx.get("summary") or tx.get("type") or "", 240),
                "raw_type": _safe_text(tx.get("type") or tx.get("kind") or "", 80),
            })
    return {"count": len(arr), "recent": items}


def _summarize_auctions(data: Any, guild_id: int, *, limit: int = 50) -> dict[str, Any]:
    g = _guild_dict(data, guild_id)
    auctions = g.get("auctions") if isinstance(g.get("auctions"), dict) else {}
    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for aid, auc in auctions.items():
        if not isinstance(auc, dict):
            continue
        status = str(auc.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        bids = auc.get("bids") if isinstance(auc.get("bids"), list) else []
        top_bid = None
        if bids:
            try:
                top_bid = max(bids, key=lambda b: float((b or {}).get("amount") or 0))
            except Exception:
                top_bid = None
        items.append({
            "auction_id": str(aid),
            "item_name": _safe_text(auc.get("item_name") or auc.get("item") or auc.get("title"), 180),
            "status": status,
            "phase": _safe_text(auc.get("phase") or auc.get("mode") or auc.get("auction_type") or "", 80),
            "created_at": str(auc.get("created_at") or ""),
            "ends_at": str(auc.get("ends_at") or auc.get("end_at") or ""),
            "bid_count": len(bids),
            "top_bid_user_id": int((top_bid or {}).get("user_id", 0) or 0) if isinstance(top_bid, dict) else 0,
            "top_bid_amount": (top_bid or {}).get("amount") if isinstance(top_bid, dict) else None,
        })
    items.sort(key=lambda x: str(x.get("created_at") or x.get("ends_at") or ""), reverse=True)
    return {"count": len(auctions), "by_status": counts, "items": items[:limit]}


def _summarize_needs(data: Any, guild: discord.Guild, *, limit: int = 50) -> dict[str, Any]:
    g = _guild_dict(data, guild.id)
    users = g.get("users") if isinstance(g.get("users"), dict) else g
    total_json_users = len(users) if isinstance(users, dict) else 0
    stale_count = 0
    active_user_count = 0
    total_entries = 0
    sample: list[dict[str, Any]] = []
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
            active_user_count += 1
            cnt = 0
            for key in ("main", "secondary", "Main", "Secondary", "needs"):
                v = raw.get(key)
                if isinstance(v, dict):
                    cnt += len([x for x in v.values() if x])
                elif isinstance(v, list):
                    cnt += len(v)
            # fallback: count slot-like non-empty dict/list entries if old structure differs
            if cnt == 0:
                for v in raw.values():
                    if isinstance(v, list):
                        cnt += len(v)
                    elif isinstance(v, dict):
                        cnt += len([x for x in v.values() if x])
            total_entries += cnt
            if len(sample) < limit:
                sample.append({"user_id": user_id, "need_entries_estimated": cnt})
    return {
        "user_count": active_user_count,
        "total_json_user_count": total_json_users,
        "stale_count": stale_count,
        "need_entries_estimated": total_entries,
        "sample": sample,
        "filter": "dashboard_member_role_only",
    }


def _summarize_event_checks(data: Any, guild_id: int, *, limit: int = 50) -> dict[str, Any]:
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
        "events": _summarize_events(sources.get("events"), guild_id),
        "event_checks": _summarize_event_checks(sources.get("dkp_event_checks"), guild_id),
        "ec": {
            "balances": _summarize_balances(sources.get("dkp_balances"), guild),
            "transactions": _summarize_transactions(sources.get("dkp_transactions"), guild_id),
        },
        "loot": {
            "needs": _summarize_needs(sources.get("loot_needs"), guild),
            "auctions": _summarize_auctions(sources.get("loot_auctions"), guild_id),
            "items_known": len(sources.get("loot_items") or {}) if isinstance(sources.get("loot_items"), dict) else 0,
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
        emb.set_footer(text="Nächster Schritt: Web-Dashboard liest dieses Schema, zuerst read-only.")
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
