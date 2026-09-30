from __future__ import annotations
import asyncio
import json
import random
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Any

try:
    from bot.json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore
except Exception:
    from json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore

import discord
from discord import app_commands

try:
    from bot.module_registry import FeatureGroup, is_module_enabled, set_module_enabled, any_guild_has_module  # type: ignore
except Exception:
    from module_registry import FeatureGroup, is_module_enabled, set_module_enabled, any_guild_has_module  # type: ignore

try:
    from bot import runtime_db  # type: ignore
except Exception:
    import runtime_db  # type: ignore

try:
    from bot.aion2_game import AION2_CLASS_META, AION2_CLASSES, AION2_FACTIONS, role_for_class as _aion2_role_for_class, role_label as _aion2_role_label, normalize_class as _aion2_normalize_class, normalize_faction as _aion2_normalize_faction  # type: ignore
except Exception:
    from aion2_game import AION2_CLASS_META, AION2_CLASSES, AION2_FACTIONS, role_for_class as _aion2_role_for_class, role_label as _aion2_role_label, normalize_class as _aion2_normalize_class, normalize_faction as _aion2_normalize_faction  # type: ignore

try:
    from guild_modules import DEFAULT_ONBOARDING_WELCOME_SLOGANS
except Exception:
    DEFAULT_ONBOARDING_WELCOME_SLOGANS = (
        "Ein neuer Held betritt das Schlachtfeld.",
        "Ein wildes {user} ist erschienen!",
        "Möge dein Loot besser sein als dein Würfelglück.",
    )
from discord.ui import View, button, Select, Modal, TextInput
from discord.enums import ButtonStyle

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"
SESSIONS_FILE = DATA_DIR / "onboarding_sessions.json"
WELCOME_STATE_FILE = DATA_DIR / "onboarding_welcome_messages.json"
APPLICATION_STATE_FILE = DATA_DIR / "onboarding_application_channels.json"
SERVER_STATE_FILE = DATA_DIR / "onboarding_server_channels.json"

# cfg[guild_id] = {
#   "enabled": bool,
#   "review_channel": int,
#   "require_review": bool,
#   "category_roles": {"guild": int, "ally": int, "friend": int, "applicant": int},
#   "primary_roles":  {"TANK": int, "SUPPORT": int, "DPS": int},
#   "experience_roles": {"experienced": int, "newbie": int}
# }

def _load_cfg() -> dict:
    return load_json_file(CFG_FILE, {}, context=__name__)

def _save_cfg(obj: dict) -> None:
    save_json_atomic(CFG_FILE, obj, context=__name__)

cfg: dict = _load_cfg()


def _load_welcome_state() -> dict[str, dict[str, dict[str, Any]]]:
    raw = load_json_file(WELCOME_STATE_FILE, {}, context=f"{__name__}.welcome")
    return raw if isinstance(raw, dict) else {}


def _save_welcome_state() -> None:
    save_json_atomic(WELCOME_STATE_FILE, _welcome_state, context=f"{__name__}.welcome")


_welcome_state: dict[str, dict[str, dict[str, Any]]] = _load_welcome_state()


def _load_application_state() -> dict[str, dict[str, dict[str, Any]]]:
    raw = load_json_file(APPLICATION_STATE_FILE, {}, context=f"{__name__}.applications")
    return raw if isinstance(raw, dict) else {}


def _save_application_state() -> None:
    save_json_atomic(APPLICATION_STATE_FILE, _application_state, context=f"{__name__}.applications")


_application_state: dict[str, dict[str, dict[str, Any]]] = _load_application_state()


def _load_server_state() -> dict[str, dict[str, dict[str, Any]]]:
    raw = load_json_file(SERVER_STATE_FILE, {}, context=f"{__name__}.server")
    return raw if isinstance(raw, dict) else {}


def _save_server_state() -> None:
    save_json_atomic(SERVER_STATE_FILE, _server_state, context=f"{__name__}.server")


_server_state: dict[str, dict[str, dict[str, Any]]] = _load_server_state()


def _onboarding_mode(guild_id: int) -> str:
    try:
        raw = str(runtime_db.get_module_setting(int(guild_id), "onboarding", "mode", "pm") or "pm").strip().lower()
    except Exception:
        raw = "pm"
    return "server" if raw in {"server", "serverside", "server-side"} else "pm"


def _server_record(guild_id: int, member_id: int) -> dict[str, Any]:
    rows = _server_state.get(str(int(guild_id))) or {}
    raw = rows.get(str(int(member_id))) or {}
    return dict(raw) if isinstance(raw, dict) else {}


def _set_server_record(guild_id: int, member_id: int, record: dict[str, Any]) -> None:
    gid = str(int(guild_id))
    uid = str(int(member_id))
    rows = _server_state.get(gid)
    if not isinstance(rows, dict):
        rows = {}
        _server_state[gid] = rows
    rows[uid] = dict(record)
    _save_server_state()


def _clear_server_record(guild_id: int, member_id: int) -> None:
    gid = str(int(guild_id))
    uid = str(int(member_id))
    rows = _server_state.get(gid)
    if isinstance(rows, dict) and uid in rows:
        rows.pop(uid, None)
        _save_server_state()


def _server_channel_name(member: discord.Member) -> str:
    raw = str(member.display_name or member.name or member.id).casefold()
    slug = re.sub(r"[^a-z0-9äöüß_-]+", "-", raw, flags=re.IGNORECASE)
    slug = re.sub(r"-+", "-", slug).strip("-_") or str(member.id)
    return (f"onboarding-{slug}")[:95]


def _server_staff_role(guild: discord.Guild) -> Optional[discord.Role]:
    ids = []
    try:
        ids.append(int(runtime_db.get_module_setting(int(guild.id), "onboarding", "server_staff_role_id", 0) or 0))
    except Exception:
        pass
    try:
        ids.append(int(runtime_db.get_guild_setting(int(guild.id), "guild_role_leader_id", 0) or 0))
    except Exception:
        pass
    try:
        ids.append(int(runtime_db.get_module_setting(int(guild.id), "onboarding", "application_lead_role_id", 0) or 0))
    except Exception:
        pass
    for rid in ids:
        if rid:
            role = guild.get_role(rid)
            if isinstance(role, discord.Role):
                return role
    return None


async def _ensure_server_gate_role(guild: discord.Guild) -> tuple[Optional[discord.Role], list[str]]:
    errors: list[str] = []
    role_id = 0
    try:
        role_id = int(runtime_db.get_module_setting(int(guild.id), "onboarding", "server_gate_role_id", 0) or 0)
    except Exception:
        role_id = 0
    role = guild.get_role(role_id) if role_id else None
    if role is None:
        role = next((r for r in guild.roles if str(r.name or "").casefold() == "onboarding offen"), None)
    if role is None:
        try:
            role = await guild.create_role(name="Onboarding offen", reason="Server-Onboarding Gate")
        except Exception as exc:
            return None, [f"Gate-Rolle konnte nicht erstellt werden ({type(exc).__name__})"]
    try:
        runtime_db.set_module_setting(int(guild.id), "onboarding", "server_gate_role_id", int(role.id))
    except Exception as exc:
        errors.append(f"Gate-Rolle konnte nicht gespeichert werden ({type(exc).__name__})")
    return role, errors


async def _ensure_server_category(guild: discord.Guild) -> tuple[Optional[discord.CategoryChannel], list[str]]:
    errors: list[str] = []
    category_id = 0
    try:
        category_id = int(runtime_db.get_module_setting(int(guild.id), "onboarding", "server_category_id", 0) or 0)
    except Exception:
        category_id = 0
    category = guild.get_channel(category_id) if category_id else None
    if not isinstance(category, discord.CategoryChannel):
        category = next((c for c in guild.categories if str(c.name or "").casefold() == "onboarding"), None)
    staff_role = _server_staff_role(guild)
    me = guild.me
    if not isinstance(category, discord.CategoryChannel):
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
        }
        if staff_role is not None:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(view_channel=True, manage_channels=True, send_messages=True, read_message_history=True)
        try:
            category = await guild.create_category("ONBOARDING", overwrites=overwrites, reason="Server-Onboarding")
        except Exception as exc:
            return None, [f"Onboarding-Kategorie konnte nicht erstellt werden ({type(exc).__name__})"]
    try:
        runtime_db.set_module_setting(int(guild.id), "onboarding", "server_category_id", int(category.id))
    except Exception as exc:
        errors.append(f"Onboarding-Kategorie konnte nicht gespeichert werden ({type(exc).__name__})")
    return category, errors


async def _apply_server_gate_permissions(guild: discord.Guild, gate_role: discord.Role, onboarding_category_id: int) -> list[str]:
    errors: list[str] = []
    for channel in list(guild.channels):
        try:
            if isinstance(channel, discord.CategoryChannel):
                if int(channel.id) == int(onboarding_category_id):
                    continue
                await channel.set_permissions(gate_role, view_channel=False, reason="Server-Onboarding: öffentlicher Bereich gesperrt")
                continue
            category = getattr(channel, "category", None)
            if category is not None and int(getattr(category, "id", 0) or 0) == int(onboarding_category_id):
                continue
            if category is None or not bool(getattr(channel, "permissions_synced", False)):
                await channel.set_permissions(gate_role, view_channel=False, reason="Server-Onboarding: öffentlicher Bereich gesperrt")
        except Exception as exc:
            errors.append(f"{getattr(channel, 'name', channel.id)}: {type(exc).__name__}")
    return errors


async def _server_channel_for(member: discord.Member) -> Optional[discord.TextChannel]:
    record = _server_record(member.guild.id, member.id)
    channel = member.guild.get_channel(int(record.get("channel_id") or 0))
    return channel if isinstance(channel, discord.TextChannel) else None


async def _delete_server_channel_later(guild: discord.Guild, member_id: int, delay: float = 10.0) -> None:
    await asyncio.sleep(max(0.0, float(delay)))
    record = _server_record(guild.id, member_id)
    channel = guild.get_channel(int(record.get("channel_id") or 0))
    if isinstance(channel, discord.TextChannel):
        try:
            await channel.delete(reason="Server-Onboarding abgeschlossen")
        except Exception:
            return
    _clear_server_record(guild.id, member_id)


async def _complete_server_gate(member: discord.Member, *, accepted: bool) -> None:
    record = _server_record(member.guild.id, member.id)
    channel = member.guild.get_channel(int(record.get("channel_id") or 0))
    if accepted:
        gate_role_id = int(record.get("gate_role_id") or 0)
        if not gate_role_id:
            try:
                gate_role_id = int(runtime_db.get_module_setting(int(member.guild.id), "onboarding", "server_gate_role_id", 0) or 0)
            except Exception:
                gate_role_id = 0
        gate_role = member.guild.get_role(gate_role_id) if gate_role_id else None
        if gate_role is not None and gate_role in member.roles:
            try:
                await member.remove_roles(gate_role, reason="Server-Onboarding akzeptiert")
            except Exception:
                pass
        if isinstance(channel, discord.TextChannel):
            try:
                await channel.send("✅ **Freigeschaltet!** Dein Onboarding wurde akzeptiert. Du kannst jetzt den öffentlichen Serverbereich sehen. Dieser Kanal wird gleich geschlossen.")
            except Exception:
                pass
        asyncio.create_task(_delete_server_channel_later(member.guild, member.id, 10.0))
    else:
        if isinstance(channel, discord.TextChannel):
            try:
                await channel.send("❌ Dein Onboarding wurde **nicht freigegeben**. Der öffentliche Serverbereich bleibt gesperrt. Bei Rückfragen kannst du hier auf die Leitung warten.")
            except Exception:
                pass
        record["status"] = "rejected"
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        _set_server_record(member.guild.id, member.id, record)


def _application_setting(guild_id: int, key: str, default: Any) -> Any:
    try:
        return runtime_db.get_module_setting(int(guild_id), "onboarding", str(key), default)
    except Exception:
        return default


def _application_cfg(guild: discord.Guild) -> dict[str, Any]:
    lead_role_id = int(_application_setting(guild.id, "application_lead_role_id", 0) or 0)
    if not lead_role_id:
        try:
            lead_role_id = int(runtime_db.get_guild_setting(int(guild.id), "guild_role_leader_id", 0) or 0)
        except Exception:
            lead_role_id = 0
    return {
        "enabled": bool(_application_setting(guild.id, "application_chat_enabled", True)),
        "category_id": int(_application_setting(guild.id, "application_category_id", 0) or 0),
        "lead_role_id": lead_role_id,
    }


def _application_record(guild_id: int, member_id: int) -> dict[str, Any]:
    guild_rows = _application_state.get(str(int(guild_id))) or {}
    raw = guild_rows.get(str(int(member_id))) or {}
    return dict(raw) if isinstance(raw, dict) else {}


def _set_application_record(guild_id: int, member_id: int, record: dict[str, Any]) -> None:
    gid = str(int(guild_id))
    uid = str(int(member_id))
    guild_rows = _application_state.get(gid)
    if not isinstance(guild_rows, dict):
        guild_rows = {}
        _application_state[gid] = guild_rows
    guild_rows[uid] = dict(record)
    _save_application_state()


def _application_channel(guild: discord.Guild, member_id: int) -> Optional[discord.TextChannel]:
    record = _application_record(guild.id, member_id)
    channel = guild.get_channel(int(record.get("channel_id") or 0))
    return channel if isinstance(channel, discord.TextChannel) else None


def _application_channel_name(member: discord.Member) -> str:
    raw = str(member.display_name or member.name or member.id).casefold()
    slug = re.sub(r"[^a-z0-9äöüß_-]+", "-", raw, flags=re.IGNORECASE)
    slug = re.sub(r"-+", "-", slug).strip("-_") or str(member.id)
    return (f"bewerbung-{slug}")[:95]


async def _ensure_application_channel(
    member: discord.Member,
    *,
    category: str | None,
    primary: str | None,
    experienced: bool | None,
) -> tuple[Optional[discord.TextChannel], str]:
    """Erstellt/recycelt den privaten Bewerbungsraum für Bewerber.

    Sichtbar sind @everyone ausdrücklich nicht, der Bewerber, die konfigurierte
    Lead-Rolle und der Bot. Discord-Administratoren können Kanal-Overwrites
    technisch immer umgehen; das ist eine Discord-Eigenschaft.
    """
    if str(category or "") != "applicant":
        return None, "Keine Bewerber-Kategorie."
    conf = _application_cfg(member.guild)
    if not conf.get("enabled"):
        return None, "Bewerbungs-Chat deaktiviert."

    existing = _application_channel(member.guild, member.id)
    if existing is not None:
        return existing, "Vorhandener Bewerbungs-Chat verwendet."

    category_obj = member.guild.get_channel(int(conf.get("category_id") or 0))
    if not isinstance(category_obj, discord.CategoryChannel):
        return None, "Keine Bewerbungs-Kategorie konfiguriert."
    lead_role = member.guild.get_role(int(conf.get("lead_role_id") or 0))
    if not isinstance(lead_role, discord.Role):
        return None, "Keine Lead-Rolle für Bewerbungs-Chats konfiguriert."

    me = member.guild.me
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        member.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
        lead_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
    }
    if me is not None:
        overwrites[me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
        )

    try:
        channel = await member.guild.create_text_channel(
            _application_channel_name(member),
            category=category_obj,
            overwrites=overwrites,
            topic=f"Private Bewerbung von {member} · Discord-ID {member.id}",
            reason=f"Onboarding-Bewerbung von {member} ({member.id})",
        )
    except discord.Forbidden:
        return None, "Dem Bot fehlen Rechte zum Erstellen des Bewerbungs-Chats."
    except Exception as exc:
        return None, f"Bewerbungs-Chat konnte nicht erstellt werden: {type(exc).__name__}"

    _set_application_record(member.guild.id, member.id, {
        "channel_id": int(channel.id),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "open",
    })

    cat_txt, pri_txt, exp_txt = _onboarding_labels(category, primary, experienced)
    embed = discord.Embed(
        title=f"📝 Bewerbung · {member.display_name}",
        description=(
            "Dieser Kanal ist für die Bewerbung und Rückfragen zwischen Bewerber und Gildenleitung gedacht.\n"
            "Die Onboarding-Angaben sind unten zusammengefasst."
        ),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    embed.add_field(name="Bewerber", value=f"{member.mention}\n`{member.id}`", inline=True)
    embed.add_field(name="Rolle", value=pri_txt, inline=True)
    embed.add_field(name="Erfahrung", value=exp_txt, inline=True)
    embed.add_field(name="Kategorie", value=cat_txt, inline=True)
    embed.set_footer(text="Privater Bewerbungs-Chat · wird nicht automatisch gelöscht")
    try:
        await channel.send(content=f"{lead_role.mention} · {member.mention}", embed=embed)
    except Exception:
        pass
    return channel, "Bewerbungs-Chat erstellt."


async def _post_application_status(
    member: discord.Member,
    text: str,
    *,
    status: str,
    actor: Optional[discord.abc.User] = None,
) -> None:
    channel = _application_channel(member.guild, member.id)
    if channel is None:
        return
    actor_txt = f" · von {getattr(actor, 'mention', '')}" if actor is not None else ""
    try:
        await channel.send(f"{text}{actor_txt}")
        record = _application_record(member.guild.id, member.id)
        record["status"] = str(status)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        _set_application_record(member.guild.id, member.id, record)
    except Exception:
        pass


def _welcome_setting(guild_id: int, key: str, default: Any) -> Any:
    try:
        return runtime_db.get_module_setting(int(guild_id), "onboarding", str(key), default)
    except Exception:
        return default


def _welcome_channel_id(guild_id: int) -> int:
    try:
        return int(runtime_db.get_guild_setting(int(guild_id), "guild_channel_welcome_id", 0) or 0)
    except Exception:
        return 0


def _welcome_cfg(guild: discord.Guild) -> dict[str, Any]:
    raw_slogans = _welcome_setting(guild.id, "welcome_slogans", list(DEFAULT_ONBOARDING_WELCOME_SLOGANS))
    if isinstance(raw_slogans, str):
        raw_slogans = [line.strip() for line in raw_slogans.splitlines() if line.strip()]
    slogans = [str(x).strip()[:300] for x in (raw_slogans or []) if str(x).strip()]
    if not slogans:
        slogans = list(DEFAULT_ONBOARDING_WELCOME_SLOGANS)
    return {
        "enabled": bool(_welcome_setting(guild.id, "welcome_enabled", True)),
        "update_on_leave": bool(_welcome_setting(guild.id, "welcome_update_on_leave", True)),
        "channel_id": _welcome_channel_id(guild.id),
        "slogans": slogans[:50],
    }


def _welcome_record(guild_id: int, member_id: int) -> dict[str, Any]:
    guild_rows = _welcome_state.get(str(int(guild_id))) or {}
    raw = guild_rows.get(str(int(member_id))) or {}
    return dict(raw) if isinstance(raw, dict) else {}


def _set_welcome_record(guild_id: int, member_id: int, record: dict[str, Any]) -> None:
    gid = str(int(guild_id))
    uid = str(int(member_id))
    guild_rows = _welcome_state.get(gid)
    if not isinstance(guild_rows, dict):
        guild_rows = {}
        _welcome_state[gid] = guild_rows
    guild_rows[uid] = dict(record)
    _save_welcome_state()


def _welcome_text(text: str, member: discord.Member) -> str:
    return (
        str(text or "")
        .replace("{user}", member.display_name)
        .replace("{guild}", member.guild.name)
        .replace("{member_count}", str(getattr(member.guild, "member_count", 0) or len(getattr(member.guild, "members", []) or [])))
    )


def _onboarding_labels(category: str | None, primary: str | None, experienced: bool | None) -> tuple[str, str, str]:
    category_txt = {
        "guild": "Gildenmitglied",
        "ally": "Allianzmitglied",
        "friend": "Freund",
        "applicant": "Bewerber",
    }.get(str(category or ""), "—")
    primary_txt = {"TANK": "Tank", "SUPPORT": "Support", "HEAL": "Support", "HEALER": "Support", "DPS": "DPS"}.get(str(primary or "").upper(), "—")
    experience_txt = "—" if experienced is None else ("Erfahren" if bool(experienced) else "Unerfahren")
    return category_txt, primary_txt, experience_txt


def _welcome_embed(
    member: discord.Member,
    *,
    status: str,
    slogan: str,
    category: str | None = None,
    primary: str | None = None,
    experienced: bool | None = None,
    reason: str = "",
) -> discord.Embed:
    status_map: dict[str, tuple[str, discord.Color]] = {
        "running": ("🟡 Onboarding läuft", discord.Color.gold()),
        "review": ("🟠 Wartet auf Freigabe", discord.Color.orange()),
        "completed": ("🟢 Onboarding abgeschlossen", discord.Color.green()),
        "rejected": ("🔴 Onboarding abgelehnt", discord.Color.red()),
        "dm_blocked": ("🔴 Onboarding wartet – Direktnachrichten aktivieren", discord.Color.red()),
        "left": ("⚫ Server verlassen", discord.Color.from_rgb(80, 80, 80)),
    }
    status_text, color = status_map.get(str(status), status_map["running"])
    embed = discord.Embed(
        title=f"Willkommen bei {member.guild.name}!",
        description=_welcome_text(slogan, member),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    try:
        avatar_url = member.display_avatar.url
        embed.set_author(name=member.display_name, icon_url=avatar_url)
        embed.set_thumbnail(url=avatar_url)
    except Exception:
        embed.set_author(name=member.display_name)
    embed.add_field(name="Status", value=status_text, inline=False)
    if status in {"review", "completed", "rejected"}:
        cat_txt, pri_txt, exp_txt = _onboarding_labels(category, primary, experienced)
        embed.add_field(name="Kategorie", value=cat_txt, inline=True)
        embed.add_field(name="Rolle", value=pri_txt, inline=True)
        embed.add_field(name="Erfahrung", value=exp_txt, inline=True)
    if reason:
        embed.add_field(name="Hinweis", value=str(reason)[:1000], inline=False)
    try:
        member_count = getattr(member.guild, "member_count", None)
        footer = f"Discord: @{member.name}"
        if member_count:
            footer += f" · Mitglied #{member_count}"
        embed.set_footer(text=footer)
    except Exception:
        pass
    return embed


async def _fetch_welcome_message(guild: discord.Guild, record: dict[str, Any]) -> tuple[Optional[discord.abc.Messageable], Optional[discord.Message]]:
    channel_id = int(record.get("channel_id") or 0)
    message_id = int(record.get("message_id") or 0)
    if not channel_id or not message_id:
        return None, None
    channel = guild.get_channel(channel_id)
    if channel is None and hasattr(guild, "get_thread"):
        channel = guild.get_thread(channel_id)
    if not channel or not hasattr(channel, "fetch_message"):
        return None, None
    try:
        return channel, await channel.fetch_message(message_id)  # type: ignore[attr-defined]
    except Exception:
        return channel, None


async def ensure_welcome_card(member: discord.Member, *, force_new: bool = False) -> tuple[bool, str]:
    if member.bot or not is_module_enabled(member.guild.id, "onboarding"):
        return False, "Onboarding nicht aktiv."
    wc = _welcome_cfg(member.guild)
    if not wc.get("enabled"):
        return False, "Welcome Card deaktiviert."
    channel_id = int(wc.get("channel_id") or 0)
    if not channel_id:
        return False, "Kein Welcome-Kanal konfiguriert."
    channel = member.guild.get_channel(channel_id)
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return False, "Welcome-Kanal nicht gefunden."

    record = _welcome_record(member.guild.id, member.id)
    if record and not force_new and str(record.get("status") or "") not in {"left", "rejected"}:
        _, message = await _fetch_welcome_message(member.guild, record)
        if message:
            return True, "Vorhandene Welcome Card verwendet."

    slogans = list(wc.get("slogans") or DEFAULT_ONBOARDING_WELCOME_SLOGANS)
    slogan = random.choice(slogans) if slogans else "Willkommen!"
    embed = _welcome_embed(member, status="running", slogan=slogan)
    try:
        message = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception as exc:
        print(f"[onboarding] Welcome Card für {member.id} fehlgeschlagen: {exc!r}", flush=True)
        return False, f"Welcome Card konnte nicht gesendet werden: {type(exc).__name__}"
    _set_welcome_record(member.guild.id, member.id, {
        "channel_id": int(channel.id),
        "message_id": int(message.id),
        "status": "running",
        "slogan": slogan,
        "joined_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return True, "Welcome Card gesendet."


async def update_welcome_card(
    member: discord.Member,
    status: str,
    *,
    category: str | None = None,
    primary: str | None = None,
    experienced: bool | None = None,
    reason: str = "",
) -> bool:
    if member.bot or not is_module_enabled(member.guild.id, "onboarding"):
        return False
    wc = _welcome_cfg(member.guild)
    if not wc.get("enabled"):
        return False
    record = _welcome_record(member.guild.id, member.id)
    if not record:
        ok, _ = await ensure_welcome_card(member)
        if not ok:
            return False
        record = _welcome_record(member.guild.id, member.id)
    _, message = await _fetch_welcome_message(member.guild, record)
    if not message:
        if status == "left":
            return False
        ok, _ = await ensure_welcome_card(member, force_new=True)
        if not ok:
            return False
        record = _welcome_record(member.guild.id, member.id)
        _, message = await _fetch_welcome_message(member.guild, record)
        if not message:
            return False
    slogan = str(record.get("slogan") or "Willkommen!")
    try:
        await message.edit(embed=_welcome_embed(
            member,
            status=status,
            slogan=slogan,
            category=category,
            primary=primary,
            experienced=experienced,
            reason=reason,
        ))
    except Exception as exc:
        print(f"[onboarding] Welcome Card Update für {member.id} fehlgeschlagen: {exc!r}", flush=True)
        return False
    record.update({
        "status": str(status),
        "category": category,
        "primary": primary,
        "experienced": experienced,
        "reason": str(reason or ""),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    _set_welcome_record(member.guild.id, member.id, record)
    return True


async def mark_welcome_member_left(member: discord.Member) -> None:
    try:
        if _onboarding_mode(member.guild.id) == "server":
            record = _server_record(member.guild.id, member.id)
            channel = member.guild.get_channel(int(record.get("channel_id") or 0))
            if isinstance(channel, discord.TextChannel):
                try:
                    await channel.delete(reason="Mitglied hat Server während Onboarding verlassen")
                except Exception:
                    pass
            _clear_server_record(member.guild.id, member.id)
        if not is_module_enabled(member.guild.id, "onboarding"):
            return
        wc = _welcome_cfg(member.guild)
        if not wc.get("enabled") or not wc.get("update_on_leave"):
            return
        record = _welcome_record(member.guild.id, member.id)
        if not record:
            return
        await update_welcome_card(
            member,
            "left",
            category=record.get("category"),
            primary=record.get("primary"),
            experienced=record.get("experienced"),
        )
    except Exception as exc:
        print(f"[onboarding] Welcome Leave Update für {getattr(member, 'id', 0)} fehlgeschlagen: {exc!r}", flush=True)

def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

def _gcfg(guild: discord.Guild) -> dict:
    c = cfg.get(str(guild.id)) or {}
    c.setdefault("enabled", True)
    c.setdefault("review_channel", 0)
    c.setdefault("require_review", False)
    c.setdefault("category_roles", {})
    c.setdefault("primary_roles", {})
    prim = c.get("primary_roles") if isinstance(c.get("primary_roles"), dict) else {}
    migrated_roles = False
    if not prim.get("SUPPORT") and prim.get("HEAL"):
        prim["SUPPORT"] = prim.get("HEAL")
        migrated_roles = True
    if "HEAL" in prim:
        prim.pop("HEAL", None)
        migrated_roles = True
    c["primary_roles"] = prim
    c.setdefault("experience_roles", {})
    cfg[str(guild.id)] = c
    if migrated_roles:
        _save_cfg(cfg)
    return c

def _role(guild: discord.Guild, rid: int | None) -> Optional[discord.Role]:
    return guild.get_role(int(rid or 0)) if rid else None

async def _set_pending_applicant_role(member: discord.Member, enabled: bool) -> Optional[discord.Role]:
    c = _gcfg(member.guild)
    role = _role(member.guild, (c.get("category_roles") or {}).get("applicant"))
    if role is None:
        return None
    try:
        if enabled and role not in member.roles:
            await member.add_roles(role, reason="Onboarding: Bewerberstatus")
        elif not enabled and role in member.roles:
            await member.remove_roles(role, reason="Onboarding: Bewerbung abgelehnt")
    except Exception as exc:
        print(
            f"[onboarding] Bewerberrolle {role.name} für {member} konnte nicht "
            f"{'gesetzt' if enabled else 'entfernt'} werden: {exc!r}",
            flush=True,
        )
    return role


async def _assign_roles(member: discord.Member, category_key: str, primary_key: str, experienced: bool | None) -> tuple[List[discord.Role], List[str]]:
    """Vergibt Kategorie-, Primär- und Erfahrungsrolle einzeln und meldet Fehler zurück."""
    g = member.guild
    c = _gcfg(g)
    category_labels = {
        "guild": "Gildenmitglied",
        "ally": "Allianzmitglied",
        "friend": "Freund",
        "applicant": "Bewerber",
    }

    wanted: list[tuple[str, int | None]] = []
    cat_map = (c.get("category_roles") or {})
    wanted.append((category_labels.get(category_key, "Kategorie"), cat_map.get(category_key)))

    prim_map = (c.get("primary_roles") or {})
    pkey = str(primary_key or "").upper()
    # Aion-2-Support ist als Profilrolle gültig. Eine separate Discord-Supportrolle
    # bleibt optional, damit ein Kantor-Onboarding nicht unnötig fehlschlägt.
    if pkey != "SUPPORT" or prim_map.get("SUPPORT"):
        wanted.append((pkey or "Primärrolle", prim_map.get(pkey)))

    if experienced is not None:
        exp_map = (c.get("experience_roles") or {})
        exp_key = "experienced" if experienced else "newbie"
        wanted.append(("Erfahren" if experienced else "Unerfahren", exp_map.get(exp_key)))

    granted: List[discord.Role] = []
    errors: List[str] = []
    seen: set[int] = set()
    bot_member = g.me
    bot_top = getattr(bot_member, "top_role", None)

    for label, rid in wanted:
        if not rid:
            errors.append(f"{label}: keine Rolle konfiguriert")
            continue
        role = _role(g, int(rid))
        if role is None:
            errors.append(f"{label}: konfigurierte Rolle `{rid}` existiert nicht mehr")
            continue
        if role.id in seen:
            continue
        seen.add(role.id)

        if role.managed:
            errors.append(f"{label}: {role.mention} ist eine verwaltete Discord-Rolle")
            continue
        if bot_top is not None and role >= bot_top:
            errors.append(f"{label}: {role.mention} liegt über/gleich der höchsten Bot-Rolle")
            continue
        try:
            if role not in member.roles:
                await member.add_roles(role, reason=f"Onboarding: {label}")
            granted.append(role)
        except discord.Forbidden:
            errors.append(f"{label}: keine Berechtigung für {role.mention} (Bot-Rolle/Rechte prüfen)")
        except discord.HTTPException as exc:
            errors.append(f"{label}: Discord-Fehler bei {role.mention} ({getattr(exc, 'status', 'HTTP')})")
        except Exception as exc:
            errors.append(f"{label}: {role.mention} konnte nicht gesetzt werden ({type(exc).__name__})")

    if errors:
        print(f"[onboarding] Rollenfehler für {member} ({member.id}): " + " | ".join(errors), flush=True)
    return granted, errors


def _review_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id = int((_gcfg(guild).get("review_channel") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


async def _ensure_server_review_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    current = _review_channel(guild)
    if isinstance(current, discord.TextChannel):
        return current
    category, _ = await _ensure_server_category(guild)
    if category is None:
        return None
    staff_role = _server_staff_role(guild)
    me = guild.me
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
    }
    if staff_role is not None:
        overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    if me is not None:
        overwrites[me] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True)
    try:
        channel = await guild.create_text_channel(
            "onboarding-review",
            category=category,
            overwrites=overwrites,
            topic="Interner Review-Kanal für Server-Onboarding",
            reason="Server-Onboarding Review-Kanal",
        )
    except Exception:
        return None
    c = _gcfg(guild)
    c["review_channel"] = int(channel.id)
    cfg[str(guild.id)] = c
    _save_cfg(cfg)
    return channel


def _guild_custom_emoji(guild: discord.Guild | None, *names: str):
    """Findet ein Server-Emoji case-insensitive; bei fehlendem Emoji bleibt die UI nutzbar."""
    if guild is None:
        return None
    wanted = {str(name or "").casefold() for name in names if str(name or "").strip()}
    for emoji in list(getattr(guild, "emojis", []) or []):
        if str(getattr(emoji, "name", "") or "").casefold() in wanted:
            return emoji
    return None


def _aion2_enabled(guild_id:int)->bool:
    try: return bool(runtime_db.get_module_setting(int(guild_id),'onboarding','aion2_enabled',False))
    except Exception: return False


def _aion2_class_role_name(class_name: str | None) -> str:
    canonical = _aion2_normalize_class(class_name)
    meta = AION2_CLASS_META.get(canonical)
    return str(meta[0]) if meta else ""


async def _assign_aion2_class_role(
    member: discord.Member,
    class_name: str | None,
) -> tuple[Optional[discord.Role], List[str]]:
    """Vergibt die englische Aion-2-Klassenrolle (z. B. Chanter).

    Die Rolle wird automatisch angelegt, wenn sie auf dem Server noch nicht
    existiert. Bereits vorhandene andere Aion-2-Klassenrollen werden entfernt,
    damit ein Mitglied nicht gleichzeitig mehrere Klassenrollen behält.
    """
    canonical = _aion2_normalize_class(class_name)
    role_name = _aion2_class_role_name(canonical)
    if not canonical or not role_name:
        return None, []

    guild = member.guild
    errors: List[str] = []
    role = next(
        (r for r in guild.roles if str(r.name or "").casefold() == role_name.casefold()),
        None,
    )

    if role is None:
        try:
            role = await guild.create_role(
                name=role_name,
                reason=f"Aion 2 Onboarding: Klassenrolle {role_name}",
            )
        except discord.Forbidden:
            return None, [f"Aion-Klasse {role_name}: Bot darf keine Rollen erstellen"]
        except discord.HTTPException as exc:
            return None, [f"Aion-Klasse {role_name}: Discord-Fehler beim Erstellen ({getattr(exc, 'status', 'HTTP')})"]
        except Exception as exc:
            return None, [f"Aion-Klasse {role_name}: Rolle konnte nicht erstellt werden ({type(exc).__name__})"]

    bot_member = guild.me
    bot_top = getattr(bot_member, "top_role", None)
    if role.managed:
        return None, [f"Aion-Klasse {role_name}: Rolle ist von Discord verwaltet"]
    if bot_top is not None and role >= bot_top:
        return None, [f"Aion-Klasse {role_name}: Rolle liegt über/gleich der höchsten Bot-Rolle"]

    class_role_names = {str(meta[0]).casefold() for meta in AION2_CLASS_META.values()}
    old_roles = [
        r for r in member.roles
        if r.id != role.id and str(r.name or "").casefold() in class_role_names
    ]
    if old_roles:
        try:
            await member.remove_roles(*old_roles, reason=f"Aion 2 Onboarding: Klassenwechsel zu {role_name}")
        except discord.Forbidden:
            errors.append("alte Aion-Klassenrolle konnte wegen fehlender Berechtigung nicht entfernt werden")
        except discord.HTTPException as exc:
            errors.append(f"alte Aion-Klassenrolle konnte nicht entfernt werden ({getattr(exc, 'status', 'HTTP')})")
        except Exception as exc:
            errors.append(f"alte Aion-Klassenrolle konnte nicht entfernt werden ({type(exc).__name__})")

    try:
        if role not in member.roles:
            await member.add_roles(role, reason=f"Aion 2 Onboarding: Klasse {role_name}")
    except discord.Forbidden:
        errors.append(f"Aion-Klasse {role_name}: Bot darf die Rolle nicht vergeben")
        return None, errors
    except discord.HTTPException as exc:
        errors.append(f"Aion-Klasse {role_name}: Discord-Fehler beim Vergeben ({getattr(exc, 'status', 'HTTP')})")
        return None, errors
    except Exception as exc:
        errors.append(f"Aion-Klasse {role_name}: Rolle konnte nicht gesetzt werden ({type(exc).__name__})")
        return None, errors

    return role, errors


def _aion2_faction_role_name(faction: str | None) -> str:
    key = _aion2_normalize_faction(faction)
    meta = AION2_FACTIONS.get(key)
    return str(meta[0]) if meta else ""


async def _assign_aion2_faction_role(
    member: discord.Member,
    faction: str | None,
) -> tuple[Optional[discord.Role], List[str]]:
    """Vergibt Elyos/Asmodia und entfernt die jeweils andere Fraktionsrolle.

    Wie bei den Klassenrollen wird die Rolle bei Bedarf automatisch angelegt.
    Nicknamen werden ausdrücklich nicht verändert.
    """
    faction_key = _aion2_normalize_faction(faction)
    role_name = _aion2_faction_role_name(faction_key)
    if not faction_key or not role_name:
        return None, []

    guild = member.guild
    errors: List[str] = []
    role = next(
        (r for r in guild.roles if str(r.name or "").casefold() == role_name.casefold()),
        None,
    )

    if role is None:
        try:
            role = await guild.create_role(
                name=role_name,
                reason=f"Aion 2 Onboarding: Fraktionsrolle {role_name}",
            )
        except discord.Forbidden:
            return None, [f"Aion-Fraktion {role_name}: Bot darf keine Rollen erstellen"]
        except discord.HTTPException as exc:
            return None, [f"Aion-Fraktion {role_name}: Discord-Fehler beim Erstellen ({getattr(exc, 'status', 'HTTP')})"]
        except Exception as exc:
            return None, [f"Aion-Fraktion {role_name}: Rolle konnte nicht erstellt werden ({type(exc).__name__})"]

    bot_member = guild.me
    bot_top = getattr(bot_member, "top_role", None)
    if role.managed:
        return None, [f"Aion-Fraktion {role_name}: Rolle ist von Discord verwaltet"]
    if bot_top is not None and role >= bot_top:
        return None, [f"Aion-Fraktion {role_name}: Rolle liegt über/gleich der höchsten Bot-Rolle"]

    faction_role_names = {str(meta[0]).casefold() for meta in AION2_FACTIONS.values()}
    old_roles = [
        r for r in member.roles
        if r.id != role.id and str(r.name or "").casefold() in faction_role_names
    ]
    if old_roles:
        try:
            await member.remove_roles(*old_roles, reason=f"Aion 2 Onboarding: Fraktionswechsel zu {role_name}")
        except discord.Forbidden:
            errors.append("alte Aion-Fraktionsrolle konnte wegen fehlender Berechtigung nicht entfernt werden")
        except discord.HTTPException as exc:
            errors.append(f"alte Aion-Fraktionsrolle konnte nicht entfernt werden ({getattr(exc, 'status', 'HTTP')})")
        except Exception as exc:
            errors.append(f"alte Aion-Fraktionsrolle konnte nicht entfernt werden ({type(exc).__name__})")

    try:
        if role not in member.roles:
            await member.add_roles(role, reason=f"Aion 2 Onboarding: Fraktion {role_name}")
    except discord.Forbidden:
        errors.append(f"Aion-Fraktion {role_name}: Bot darf die Rolle nicht vergeben")
        return None, errors
    except discord.HTTPException as exc:
        errors.append(f"Aion-Fraktion {role_name}: Discord-Fehler beim Vergeben ({getattr(exc, 'status', 'HTTP')})")
        return None, errors
    except Exception as exc:
        errors.append(f"Aion-Fraktion {role_name}: Rolle konnte nicht gesetzt werden ({type(exc).__name__})")
        return None, errors

    return role, errors


class StepContext:
    def __init__(
        self,
        member_id: int,
        guild_id: int,
        *,
        message_id: int = 0,
        stage: str = "category",
        category: str | None = None,
        primary: str | None = None,
        experienced: bool | None = None,
        aion_class: str | None = None,
        aion_character: str | None = None,
        aion_faction: str | None = None,
        mode: str | None = None,
    ):
        self.member_id = int(member_id)
        self.guild_id = int(guild_id)
        self.message_id = int(message_id or 0)
        self.stage = str(stage or "category")
        self.category = category
        self.primary = primary
        self.experienced = experienced
        self.aion_class = aion_class
        self.aion_character = aion_character
        self.aion_faction = aion_faction
        self.mode = str(mode or "").strip().lower() or None

    def to_dict(self) -> dict:
        return {
            "member_id": self.member_id,
            "guild_id": self.guild_id,
            "message_id": self.message_id,
            "stage": self.stage,
            "category": self.category,
            "primary": self.primary,
            "experienced": self.experienced,
            "aion_class": self.aion_class,
            "aion_character": self.aion_character,
            "aion_faction": self.aion_faction,
            "mode": self.mode,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "StepContext":
        return cls(
            int(raw.get("member_id", 0) or 0),
            int(raw.get("guild_id", 0) or 0),
            message_id=int(raw.get("message_id", 0) or 0),
            stage=str(raw.get("stage") or "category"),
            category=raw.get("category"),
            primary=raw.get("primary"),
            experienced=raw.get("experienced"),
            aion_class=raw.get("aion_class"),
            aion_character=raw.get("aion_character"),
            aion_faction=raw.get("aion_faction"),
            mode=raw.get("mode"),
        )


def _load_sessions() -> dict[str, dict]:
    raw = load_json_file(SESSIONS_FILE, {}, context=__name__)
    return raw if isinstance(raw, dict) else {}


def _save_sessions() -> None:
    save_json_atomic(SESSIONS_FILE, _session_records, context=__name__)


def _remember_ctx(ctx: StepContext) -> None:
    if ctx.message_id <= 0:
        return
    _sessions[ctx.member_id] = ctx
    _session_records[str(ctx.message_id)] = ctx.to_dict()
    _save_sessions()


def _forget_message(message_id: int) -> None:
    raw = _session_records.pop(str(int(message_id or 0)), None)
    if isinstance(raw, dict):
        member_id = int(raw.get("member_id", 0) or 0)
        current = _sessions.get(member_id)
        if current and current.message_id == int(message_id or 0):
            _sessions.pop(member_id, None)
    _save_sessions()


def _forget_member_sessions(member_id: int) -> None:
    member_id = int(member_id)
    changed = False
    for message_id, raw in list(_session_records.items()):
        if isinstance(raw, dict) and int(raw.get("member_id", 0) or 0) == member_id:
            _session_records.pop(message_id, None)
            changed = True
    _sessions.pop(member_id, None)
    if changed:
        _save_sessions()


_session_records: dict[str, dict] = _load_sessions()
_sessions: dict[int, StepContext] = {}
for _raw_ctx in list(_session_records.values()):
    try:
        _ctx = StepContext.from_dict(_raw_ctx)
        if _ctx.member_id and _ctx.guild_id and _ctx.message_id:
            _sessions[_ctx.member_id] = _ctx
    except Exception:
        continue


class OnboardingFeatureView(View):
    async def interaction_check(self, inter: discord.Interaction) -> bool:
        ctx = getattr(self, "ctx", None)
        guild_id = int(getattr(getattr(inter, "guild", None), "id", 0) or getattr(self, "guild_id", 0) or getattr(ctx, "guild_id", 0) or 0)
        if guild_id and not is_module_enabled(guild_id, "onboarding"):
            message = "ℹ️ Das Onboarding-System ist für diese Gilde deaktiviert."
            if inter.response.is_done():
                await inter.followup.send(message, ephemeral=True)
            else:
                await inter.response.send_message(message, ephemeral=True)
            return False
        if ctx is not None and int(getattr(ctx, "member_id", 0) or 0) and int(inter.user.id) != int(ctx.member_id):
            message = "ℹ️ Dieses Onboarding gehört einem anderen Mitglied."
            if inter.response.is_done():
                await inter.followup.send(message, ephemeral=True)
            else:
                await inter.response.send_message(message, ephemeral=True)
            return False
        return True


class CategoryView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, cat: str):
        self.ctx.category = cat
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        if _aion2_enabled(self.ctx.guild_id):
            self.ctx.stage = "aion2_class"
            _remember_ctx(self.ctx)
            guild = inter.client.get_guild(self.ctx.guild_id)
            await inter.response.edit_message(
                content="🎮 **Aion 2:** Welche Klasse spielst du?",
                view=Aion2ClassView(self.ctx, guild),
            )
        else:
            self.ctx.stage = "primary"
            _remember_ctx(self.ctx)
            await inter.response.edit_message(content="Welche **Spielrolle** spielst du?", view=PrimaryView(self.ctx))

    @button(label="📝 Bewerber", style=ButtonStyle.primary, custom_id="onboarding_category_applicant")
    async def btn_applicant(self, inter: discord.Interaction, _):
        await self._next(inter, "applicant")

    @button(label="🫱 Freund", style=ButtonStyle.success, custom_id="onboarding_category_friend")
    async def btn_friend(self, inter: discord.Interaction, _):
        await self._next(inter, "friend")

    @button(label="🏰 Allianz", style=ButtonStyle.secondary, custom_id="onboarding_category_ally")
    async def btn_ally(self, inter: discord.Interaction, _):
        await self._next(inter, "ally")


class PrimaryView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, primary: str):
        self.ctx.primary = primary
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        self.ctx.stage = "submit"
        _remember_ctx(self.ctx)
        await inter.response.edit_message(content=_submission_text(self.ctx), view=SubmitView(self.ctx))

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="onboarding_primary_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._next(inter, "TANK")

    @button(label="🎵 Support", style=ButtonStyle.secondary, custom_id="onboarding_primary_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._next(inter, "SUPPORT")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="onboarding_primary_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._next(inter, "DPS")


class Aion2CharacterModal(Modal):
    def __init__(self, ctx: StepContext):
        super().__init__(title="Aion 2 Charakter", timeout=300)
        self.ctx=ctx
        self.character=TextInput(label="Charaktername",placeholder="Dein Aion-2-Charaktername",required=True,max_length=120)
        self.add_item(self.character)
    async def on_submit(self, inter:discord.Interaction):
        # Die Aion-2-Angaben bleiben bis zum erfolgreichen Abschluss nur in der
        # Onboarding-Session. Ein dauerhaftes Profil wird erst nach Annahme bzw.
        # nach erfolgreichem Auto-Onboarding angelegt.
        self.ctx.aion_character=str(self.character.value).strip()
        self.ctx.stage="submit"
        _remember_ctx(self.ctx)
        await inter.response.edit_message(content=_submission_text(self.ctx), view=SubmitView(self.ctx))

class Aion2FactionView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext, guild: discord.Guild | None = None):
        super().__init__(timeout=None)
        self.ctx = ctx
        for item in self.children:
            cid = str(getattr(item, "custom_id", "") or "")
            if cid == "onboarding_aion2_faction_elyos":
                item.emoji = _guild_custom_emoji(guild, "Elyos")
            elif cid == "onboarding_aion2_faction_asmodia":
                item.emoji = _guild_custom_emoji(guild, "Asmodia", "Asmodian")

    async def _choose(self, inter: discord.Interaction, faction: str):
        self.ctx.aion_faction = faction
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        self.ctx.stage = "aion2_class"
        _remember_ctx(self.ctx)
        guild = inter.client.get_guild(self.ctx.guild_id)
        label = AION2_FACTIONS.get(faction, (faction, ""))[0]
        await inter.response.edit_message(
            content=f"🎮 **Aion 2 · {label}:** Welche Klasse spielst du?",
            view=Aion2ClassView(self.ctx, guild),
        )

    @button(label="Elyos", style=ButtonStyle.primary, custom_id="onboarding_aion2_faction_elyos")
    async def btn_elyos(self, inter: discord.Interaction, _):
        await self._choose(inter, "ELYOS")

    @button(label="Asmodia", style=ButtonStyle.secondary, custom_id="onboarding_aion2_faction_asmodia")
    async def btn_asmodia(self, inter: discord.Interaction, _):
        await self._choose(inter, "ASMODIA")


class Aion2ClassSelect(Select):
    def __init__(self, ctx: StepContext, guild: discord.Guild | None = None):
        self.ctx = ctx
        labels = {"TANK":"Tank", "HEAL":"Support", "HEALER":"Support", "DPS":"DPS", "SUPPORT":"Support"}
        opts = []
        for name in AION2_CLASSES:
            en, role, emoji_name = AION2_CLASS_META[name]
            emoji = _guild_custom_emoji(guild, emoji_name)
            opts.append(discord.SelectOption(
                label=f"{name} ({en})",
                value=name,
                description=labels.get(role, role),
                emoji=emoji,
            ))
        super().__init__(placeholder="Aion-2-Klasse auswählen…", min_values=1, max_values=1, options=opts, custom_id="onboarding_aion2_class_select")

    async def callback(self, inter: discord.Interaction):
        self.ctx.aion_class = self.values[0]
        self.ctx.primary = _aion2_role_for_class(self.ctx.aion_class)
        _remember_ctx(self.ctx)
        await inter.response.send_modal(Aion2CharacterModal(self.ctx))


class Aion2ClassView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext, guild: discord.Guild | None = None):
        super().__init__(timeout=None)
        self.ctx = ctx
        self.add_item(Aion2ClassSelect(ctx, guild))


def _submission_text(ctx: StepContext) -> str:
    cat_txt = {
        "ally": "Allianz",
        "friend": "Freund",
        "applicant": "Bewerber",
    }.get(str(ctx.category or ""), "—")
    role_txt = _aion2_role_label(ctx.primary) if ctx.primary else "—"
    lines = [
        "✅ **Fast geschafft. Bitte prüfe deine Angaben:**",
        f"• **Typ:** {cat_txt}",
    ]
    if ctx.aion_class or ctx.aion_character:
        lines.extend([
            f"• **Aion-2-Klasse:** {ctx.aion_class or '—'}",
            f"• **Charaktername:** {ctx.aion_character or '—'}",
            f"• **Rolle:** {role_txt}",
        ])
    elif ctx.primary:
        lines.append(f"• **Rolle:** {role_txt}")
    lines.append("\nWenn alles stimmt, schicke das Onboarding ab.")
    return "\n".join(lines)


class SubmitView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    @button(label="✅ Onboarding absenden", style=ButtonStyle.success, custom_id="onboarding_submit")
    async def btn_submit(self, inter: discord.Interaction, _):
        await ExperienceView(self.ctx)._finish(inter, None)


class ServerStartView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    @button(label="🚀 Onboarding starten", style=ButtonStyle.primary, custom_id="onboarding_server_start")
    async def btn_start(self, inter: discord.Interaction, _):
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        self.ctx.stage = "category"
        self.ctx.mode = "server"
        _remember_ctx(self.ctx)
        record = _server_record(self.ctx.guild_id, self.ctx.member_id)
        record.update({"status": "running", "updated_at": datetime.now(timezone.utc).isoformat()})
        _set_server_record(self.ctx.guild_id, self.ctx.member_id, record)
        await inter.response.edit_message(
            content=(
                f"👋 **Willkommen <@{self.ctx.member_id}>!**\n\n"
                "Bevor du Zugriff auf den Server bekommst, brauchen wir kurz ein paar Angaben.\n"
                "Wähle zuerst aus, weshalb du hier bist:"
            ),
            view=CategoryView(self.ctx),
            embed=None,
        )


def _save_accepted_aion2_profile(
    guild_id: int,
    member_id: int,
    *,
    aion_class: str | None,
    aion_character: str | None,
    aion_faction: str | None,
    primary: str | None,
) -> bool:
    """Persist Aion-2 data only after onboarding was successfully accepted."""
    class_name = str(aion_class or "").strip()
    character_name = str(aion_character or "").strip()
    if not class_name and not character_name:
        return True
    role = _aion2_role_for_class(class_name) or str(primary or "").strip().upper()
    faction = str(aion_faction or "").strip().upper()
    if not faction:
        try:
            existing = runtime_db.get_aion2_profile(int(guild_id), int(member_id)) or {}
            faction = str(existing.get("faction") or "").strip().upper()
        except Exception:
            faction = ""
    try:
        return bool(
            runtime_db.upsert_aion2_profile(
                int(guild_id),
                int(member_id),
                character_name=character_name,
                class_name=class_name,
                main_role=role,
                faction=faction,
            )
        )
    except Exception as exc:
        print(f"[onboarding] Angenommenes Aion2-Profil konnte nicht gespeichert werden: {exc!r}", flush=True)
        return False


class ReviewView(OnboardingFeatureView):
    def __init__(
        self,
        member_id: int,
        category: str,
        primary: str,
        experienced: bool | None,
        *,
        message_id: int = 0,
        guild_id: int = 0,
        aion_class: str | None = None,
        aion_character: str | None = None,
        aion_faction: str | None = None,
        mode: str | None = None,
    ):
        super().__init__(timeout=None)
        self.member_id = int(member_id)
        self.category = category
        self.primary = primary
        self.experienced = experienced
        self.message_id = int(message_id or 0)
        self.guild_id = int(guild_id or 0)
        self.aion_class = str(aion_class or "")
        self.aion_character = str(aion_character or "")
        self.aion_faction = str(aion_faction or "")
        self.mode = "server" if str(mode or "").lower() == "server" else "pm"

    def _is_admin(self, inter: discord.Interaction) -> bool:
        p = getattr(inter.user, "guild_permissions", None)
        return bool(p and (p.administrator or p.manage_guild))

    async def _get_member(self, guild: discord.Guild) -> Optional[discord.Member]:
        m = guild.get_member(self.member_id)
        if not m:
            try:
                m = await guild.fetch_member(self.member_id)
            except Exception:
                m = None
        return m

    @button(label="✅ Akzeptieren", style=ButtonStyle.success, custom_id="onboarding_review_accept")
    async def btn_accept(self, inter: discord.Interaction, _):
        if not self._is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return
        await inter.response.defer()
        member = await self._get_member(inter.guild)
        if not member:
            await inter.followup.send("Mitglied nicht gefunden.", ephemeral=True)
            return

        roles, role_errors = await _assign_roles(member, self.category, self.primary, self.experienced)
        class_role, class_role_errors = await _assign_aion2_class_role(member, self.aion_class)
        if class_role is not None and all(r.id != class_role.id for r in roles):
            roles.append(class_role)
        role_errors.extend(class_role_errors)
        aion_profile_saved = _save_accepted_aion2_profile(
            self.guild_id or int(inter.guild.id),
            self.member_id,
            aion_class=self.aion_class,
            aion_character=self.aion_character,
            aion_faction=self.aion_faction,
            primary=self.primary,
        )
        await update_welcome_card(member, "completed", category=self.category, primary=self.primary, experienced=self.experienced)
        if self.category == "applicant" and self.mode == "pm":
            await _post_application_status(
                member,
                "✅ **Bewerbung/Onboarding akzeptiert.** Die Gildenleitung kann hier die nächsten Schritte mit dir klären.",
                status="accepted",
                actor=inter.user,
            )
        if self.mode == "server":
            await _complete_server_gate(member, accepted=True)

        member_name = discord.utils.escape_markdown(member.display_name or member.name)
        role_error_text = ""
        if role_errors:
            role_error_text = "\n⚠️ **Nicht gesetzt:** " + " · ".join(role_errors)
        if (self.aion_class or self.aion_character) and not aion_profile_saved:
            role_error_text += "\n⚠️ **Aion-2-Profil:** konnte nicht dauerhaft gespeichert werden."
        await inter.edit_original_response(
            content=(
                f"✅ **Akzeptiert:** **{member_name}** ({member.mention}) – Rollen: "
                f"{', '.join(r.mention for r in roles) if roles else '—'}{role_error_text}"
            ),
            view=None,
        )
        _forget_message(self.message_id or (inter.message.id if inter.message else 0))
        if self.mode == "pm":
            try:
                await member.send("✅ Deine Anfrage wurde **akzeptiert**. Willkommen!")
            except Exception:
                pass

    @button(label="❌ Ablehnen", style=ButtonStyle.danger, custom_id="onboarding_review_deny")
    async def btn_deny(self, inter: discord.Interaction, _):
        if not self._is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return
        await inter.response.defer()
        member = await self._get_member(inter.guild)
        if member is not None:
            member_name = discord.utils.escape_markdown(member.display_name or member.name)
            deny_text = f"❌ **Abgelehnt:** **{member_name}** ({member.mention})."
        else:
            deny_text = f"❌ **Abgelehnt:** <@{self.member_id}>."
        await inter.edit_original_response(content=deny_text, view=None)
        _forget_message(self.message_id or (inter.message.id if inter.message else 0))
        if member:
            await update_welcome_card(member, "rejected", category=self.category, primary=self.primary, experienced=self.experienced, reason="Die Gildenleitung hat das Onboarding nicht freigegeben.")
            if self.mode == "server":
                await _complete_server_gate(member, accepted=False)
            else:
                if self.category == "applicant":
                    await _set_pending_applicant_role(member, False)
                    await _post_application_status(
                        member,
                        "❌ **Bewerbung/Onboarding abgelehnt.** Rückfragen können in diesem Kanal geklärt werden.",
                        status="rejected",
                        actor=inter.user,
                    )
                try:
                    await member.send("❌ Deine Anfrage wurde **abgelehnt**.")
                except Exception:
                    pass


class ExperienceView(OnboardingFeatureView):
    """Legacy-Kompatibilität. Neue Onboardings fragen Erfahrung nicht mehr ab."""
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _finish(self, inter: discord.Interaction, experienced: bool | None):
        try:
            if not inter.response.is_done():
                await inter.response.defer()
            self.ctx.experienced = experienced
            if inter.message:
                self.ctx.message_id = int(inter.message.id)
            guild = inter.client.get_guild(self.ctx.guild_id)
            if not guild:
                await inter.edit_original_response(content="⚠️ Server nicht gefunden.", view=None)
                return

            mode = "server" if str(self.ctx.mode or _onboarding_mode(guild.id)) == "server" else "pm"
            self.ctx.mode = mode
            c = _gcfg(guild)
            review_ch = await _ensure_server_review_channel(guild) if mode == "server" else _review_channel(guild)
            require = True if mode == "server" else bool(c.get("require_review"))

            member = guild.get_member(self.ctx.member_id)
            if not member:
                try:
                    member = await guild.fetch_member(self.ctx.member_id)
                except Exception:
                    member = None

            cat_txt = {"ally": "Allianz", "friend": "Freund", "applicant": "Bewerber"}.get(self.ctx.category, "—")
            pri_txt = {"TANK": "Tank", "HEAL": "Support", "HEALER": "Support", "DPS": "DPS", "SUPPORT": "Support"}.get(str(self.ctx.primary or "").upper(), "—")

            if require and not review_ch:
                await inter.edit_original_response(content="❌ Es ist kein Review-Kanal gesetzt. Bitte informiere die Gildenleitung.", view=None)
                return

            application_channel: Optional[discord.TextChannel] = None
            application_info = ""
            if member and self.ctx.category == "applicant" and mode == "pm":
                await _set_pending_applicant_role(member, True)
                application_channel, application_info = await _ensure_application_channel(
                    member, category=self.ctx.category, primary=self.ctx.primary, experienced=experienced
                )

            if require:
                desc = (
                    f"**Onboarding-Review:** {member.mention if member else f'<@{self.ctx.member_id}>'}\n"
                    f"**Typ:** {cat_txt}\n"
                    f"**Rolle:** {pri_txt}"
                )
                if self.ctx.aion_character or self.ctx.aion_class:
                    desc += (
                        f"\n**Aion-2-Charakter:** {self.ctx.aion_character or '—'}"
                        f"\n**Aion-2-Klasse:** {self.ctx.aion_class or '—'}"
                    )
                if application_channel is not None:
                    desc += f"\n**Bewerbungs-Chat:** {application_channel.mention}"
                elif application_info and mode == "pm":
                    desc += f"\n⚠️ **Bewerbungs-Chat:** {application_info}"
                if mode == "server":
                    server_channel = await _server_channel_for(member) if member else None
                    if server_channel is not None:
                        desc += f"\n**Server-Onboarding:** {server_channel.mention}"

                review_view = ReviewView(
                    self.ctx.member_id,
                    str(self.ctx.category or ""),
                    str(self.ctx.primary or ""),
                    experienced,
                    guild_id=self.ctx.guild_id,
                    aion_class=self.ctx.aion_class,
                    aion_character=self.ctx.aion_character,
                    aion_faction=self.ctx.aion_faction,
                    mode=mode,
                )
                review_message = await review_ch.send(desc, view=review_view)
                review_view.message_id = int(review_message.id)
                review_ctx = StepContext(
                    self.ctx.member_id,
                    self.ctx.guild_id,
                    message_id=int(review_message.id),
                    stage="review",
                    category=self.ctx.category,
                    primary=self.ctx.primary,
                    experienced=experienced,
                    aion_class=self.ctx.aion_class,
                    aion_character=self.ctx.aion_character,
                    aion_faction=self.ctx.aion_faction,
                    mode=mode,
                )
                _session_records[str(review_message.id)] = review_ctx.to_dict()
                _save_sessions()
                if member:
                    await update_welcome_card(member, "review", category=self.ctx.category, primary=self.ctx.primary, experienced=experienced)
                if mode == "server":
                    record = _server_record(self.ctx.guild_id, self.ctx.member_id)
                    record.update({"status": "review", "updated_at": datetime.now(timezone.utc).isoformat()})
                    _set_server_record(self.ctx.guild_id, self.ctx.member_id, record)
                user_done_text = "✅ Danke! Deine Angaben wurden zur **Prüfung** an die Gildenleitung gesendet."
                if mode == "server":
                    user_done_text += "\nDu bleibst bis zur Freigabe in diesem Kanal. Danach wird der öffentliche Serverbereich automatisch freigeschaltet."
                elif application_channel is not None:
                    user_done_text += f"\n📝 Dein privater Bewerbungs-Chat wurde erstellt: {application_channel.mention}"
                await inter.edit_original_response(content=user_done_text, view=None)
            else:
                role_errors: list[str] = []
                aion_profile_saved = True
                if member:
                    roles, role_errors = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)
                    class_role, class_role_errors = await _assign_aion2_class_role(member, self.ctx.aion_class)
                    if class_role is not None and all(r.id != class_role.id for r in roles):
                        roles.append(class_role)
                    role_errors.extend(class_role_errors)
                    aion_profile_saved = _save_accepted_aion2_profile(
                        self.ctx.guild_id,
                        self.ctx.member_id,
                        aion_class=self.ctx.aion_class,
                        aion_character=self.ctx.aion_character,
                        aion_faction=self.ctx.aion_faction,
                        primary=self.ctx.primary,
                    )
                    await update_welcome_card(member, "completed", category=self.ctx.category, primary=self.ctx.primary, experienced=experienced)
                auto_done_text = "✅ Danke! Deine Rollen wurden vergeben."
                if member and role_errors:
                    auto_done_text += "\n⚠️ Einige Rollen konnten nicht gesetzt werden. Die Gildenleitung wurde informiert."
                if member and (self.ctx.aion_class or self.ctx.aion_character) and not aion_profile_saved:
                    auto_done_text += "\n⚠️ Dein Aion-2-Profil konnte nicht gespeichert werden. Bitte informiere die Gildenleitung."
                await inter.edit_original_response(content=auto_done_text, view=None)
            _forget_message(self.ctx.message_id or (inter.message.id if inter.message else 0))
        except Exception as e:
            try:
                if not inter.response.is_done():
                    await inter.response.send_message(f"❌ Fehler im Onboarding: {e}", ephemeral=True)
                else:
                    await inter.followup.send(f"❌ Fehler im Onboarding: {e}", ephemeral=True)
            except Exception:
                pass
            print(f"[onboarding] Abschlussfehler: {e!r}")

    @button(label="🧠 Erfahren", style=ButtonStyle.primary, custom_id="onboarding_experience_yes")
    async def btn_exp(self, inter: discord.Interaction, _):
        await self._finish(inter, True)

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary, custom_id="onboarding_experience_no")
    async def btn_new(self, inter: discord.Interaction, _):
        await self._finish(inter, False)


async def send_pm_onboarding(member: discord.Member) -> tuple[bool, str]:
    try:
        if member.bot:
            return False, "Botkonten werden nicht onboardet."
        if not is_module_enabled(member.guild.id, "onboarding"):
            return False, "Onboarding ist für diesen Server deaktiviert."
        await ensure_welcome_card(member)
        await update_welcome_card(member, "running")
        _forget_member_sessions(member.id)
        ctx = StepContext(member.id, member.guild.id, mode="pm")
        message = await member.send(
            f"👋 **Willkommen {member.display_name}!**\n\nWähle bitte zuerst aus, weshalb du hier bist.",
            view=CategoryView(ctx),
        )
        ctx.message_id = int(message.id)
        ctx.stage = "category"
        _remember_ctx(ctx)
        return True, "PM-Onboarding gesendet."
    except discord.Forbidden:
        try:
            await update_welcome_card(member, "dm_blocked", reason="Direktnachrichten sind deaktiviert. Bitte DMs für diesen Server aktivieren und das Onboarding erneut starten.")
        except Exception:
            pass
        return False, "PM konnte nicht zugestellt werden. Das Mitglied hat Direktnachrichten vermutlich deaktiviert."
    except Exception as exc:
        print(f"[onboarding] PM an {getattr(member, 'id', 0)} fehlgeschlagen: {exc!r}", flush=True)
        return False, f"{type(exc).__name__}: {str(exc)[:240]}"


async def send_server_onboarding(member: discord.Member) -> tuple[bool, str]:
    if member.bot:
        return False, "Botkonten werden nicht onboardet."
    if not is_module_enabled(member.guild.id, "onboarding"):
        return False, "Onboarding ist für diesen Server deaktiviert."
    existing = await _server_channel_for(member)
    if existing is not None:
        return True, f"Vorhandener Server-Onboarding-Kanal verwendet: {existing.name}"

    gate_role, role_errors = await _ensure_server_gate_role(member.guild)
    category, category_errors = await _ensure_server_category(member.guild)
    errors = [*role_errors, *category_errors]
    if gate_role is None or category is None:
        return False, "; ".join(errors) or "Server-Onboarding konnte nicht vorbereitet werden."
    try:
        if gate_role not in member.roles:
            await member.add_roles(gate_role, reason="Server-Onboarding offen")
    except Exception as exc:
        return False, f"Gate-Rolle konnte nicht vergeben werden ({type(exc).__name__})"
    permission_errors = await _apply_server_gate_permissions(member.guild, gate_role, category.id)
    if permission_errors:
        print(f"[onboarding] Gate-Permissions: {' | '.join(permission_errors[:8])}", flush=True)

    staff_role = _server_staff_role(member.guild)
    me = member.guild.me
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        member.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True, embed_links=True),
    }
    if staff_role is not None:
        overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    if me is not None:
        overwrites[me] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True, manage_messages=True)
    try:
        channel = await member.guild.create_text_channel(
            _server_channel_name(member),
            category=category,
            overwrites=overwrites,
            topic=f"Server-Onboarding für {member} · Discord-ID {member.id}",
            reason=f"Server-Onboarding für {member}",
        )
    except Exception as exc:
        return False, f"Privater Onboarding-Kanal konnte nicht erstellt werden ({type(exc).__name__})"

    _forget_member_sessions(member.id)
    ctx = StepContext(member.id, member.guild.id, mode="server", stage="server_start")
    embed = discord.Embed(
        title="👋 Willkommen – Onboarding erforderlich",
        description=(
            f"{member.mention}, bevor du Zugriff auf den öffentlichen Serverbereich bekommst, musst du kurz das Onboarding abschließen.\n\n"
            "Klicke unten auf **Onboarding starten**. Danach beantwortest du nur die nötigen Fragen und die Gildenleitung prüft deine Angaben."
        ),
        color=discord.Color.gold(),
    )
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    message = await channel.send(content=member.mention, embed=embed, view=ServerStartView(ctx), allowed_mentions=discord.AllowedMentions(users=True))
    ctx.message_id = int(message.id)
    _remember_ctx(ctx)
    _set_server_record(member.guild.id, member.id, {
        "channel_id": int(channel.id),
        "message_id": int(message.id),
        "gate_role_id": int(gate_role.id),
        "status": "waiting_start",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return True, f"Server-Onboarding-Kanal erstellt: {channel.name}"


async def send_onboarding_dm(member: discord.Member) -> tuple[bool, str]:
    """Kompatibler Join-Hook-Einstieg: dispatcht je nach gewähltem Modus."""
    if _onboarding_mode(member.guild.id) == "server":
        return await send_server_onboarding(member)
    return await send_pm_onboarding(member)


async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree) -> None:
    onboarding_group = FeatureGroup(module_key="onboarding", 
        name="onboarding",
        description="Mitglieder-Onboarding verwalten",
    )
    tree.add_command(onboarding_group)

    # Persistente Onboarding- und Review-Buttons nach einem Neustart wieder anbinden.
    for message_id, raw_ctx in list(_session_records.items()):
        try:
            ctx = StepContext.from_dict(raw_ctx)
            mid = int(message_id)
            if ctx.stage == "server_start":
                client.add_view(ServerStartView(ctx), message_id=mid)
            elif ctx.stage == "category":
                client.add_view(CategoryView(ctx), message_id=mid)
            elif ctx.stage == "primary":
                client.add_view(PrimaryView(ctx), message_id=mid)
            elif ctx.stage == "submit":
                client.add_view(SubmitView(ctx), message_id=mid)
            elif ctx.stage == "experience":
                client.add_view(ExperienceView(ctx), message_id=mid)
            elif ctx.stage == "aion2_faction":
                # Legacy-Sessions aus alten Deployments: direkt auf Klassenauswahl weiterführen.
                ctx.stage = "aion2_class"
                _remember_ctx(ctx)
                client.add_view(Aion2ClassView(ctx, client.get_guild(ctx.guild_id)), message_id=mid)
            elif ctx.stage == "aion2_class":
                client.add_view(Aion2ClassView(ctx, client.get_guild(ctx.guild_id)), message_id=mid)
            elif ctx.stage == "review":
                client.add_view(
                    ReviewView(
                        ctx.member_id,
                        str(ctx.category or ""),
                        str(ctx.primary or ""),
                        ctx.experienced,
                        message_id=mid,
                        guild_id=ctx.guild_id,
                        aion_class=ctx.aion_class,
                        aion_character=ctx.aion_character,
                        aion_faction=ctx.aion_faction,
                        mode=ctx.mode,
                    ),
                    message_id=mid,
                )
        except Exception as exc:
            print(f"[onboarding] Persistente View {message_id} konnte nicht geladen werden: {exc!r}")

    @onboarding_group.command(name="toggle", description="(Admin) Onboarding ein-/ausschalten")
    @app_commands.describe(enabled="true = an, false = aus")
    async def onboarding_toggle(inter: discord.Interaction, enabled: bool):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        set_module_enabled(int(inter.guild_id), "onboarding", bool(enabled))
        await inter.response.send_message(
            f"✅ Onboarding {'aktiviert' if enabled else 'deaktiviert'}. Die Slash-Commands werden automatisch synchronisiert.",
            ephemeral=True,
        )

    @onboarding_group.command(name="set_categories", description="(Admin) Rollen für Kategorien setzen")
    async def onboarding_set_categories(
        inter: discord.Interaction,
        allianzmitglied: discord.Role,
        freund: discord.Role,
        bewerber: discord.Role,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        previous = c.get("category_roles") or {}
        c["category_roles"] = {
            "guild": int(previous.get("guild") or 0),
            "ally": allianzmitglied.id,
            "friend": freund.id,
            "applicant": bewerber.id,
        }

        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(
            f"✅ Onboarding-Typen gesetzt:\n"
            f"• Allianz: {allianzmitglied.mention}\n"
            f"• Freund: {freund.mention}\n"
            f"• Bewerber: {bewerber.mention}",
            ephemeral=True
        )

    @onboarding_group.command(name="set_primaries", description="(Admin) Primärrollen für Tank/Support/DPS setzen")
    async def onboarding_set_primaries(
        inter: discord.Interaction,
        tank: discord.Role,
        support: discord.Role,
        dps: discord.Role,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        c["primary_roles"] = {"TANK": tank.id, "SUPPORT": support.id, "DPS": dps.id}
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(
            f"✅ Primärrollen gesetzt:\n• 🛡️ {tank.mention}\n• 🎵 {support.mention}\n• 🗡️ {dps.mention}",
            ephemeral=True
        )

    @onboarding_group.command(name="set_review_channel", description="(Admin) Kanal für Review/Logs setzen")
    async def onboarding_set_review_channel(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            c = _gcfg(pick_inter.guild)
            c["review_channel"] = int(channel.id)
            cfg[str(pick_inter.guild_id)] = c
            _save_cfg(cfg)
            await pick_inter.response.edit_message(
                content=f"✅ Review-/Log-Kanal gesetzt: {channel.mention}",
                view=None,
            )

        await send_text_channel_picker(inter, "📝 Onboarding-Review-Kanal auswählen", _picked)

    @onboarding_group.command(name="application_chat", description="(Admin) Privaten Bewerbungs-Chat konfigurieren")
    @app_commands.describe(
        enabled="Privaten Bewerbungs-Chat für Kategorie Bewerber aktivieren",
        category="Discord-Kategorie, in der Bewerbungs-Chats erstellt werden",
        lead_role="Rolle, die zusammen mit dem Bewerber Zugriff bekommt",
    )
    async def onboarding_application_chat(
        inter: discord.Interaction,
        enabled: bool,
        category: Optional[discord.CategoryChannel] = None,
        lead_role: Optional[discord.Role] = None,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        guild_id = int(inter.guild_id)
        runtime_db.set_module_setting(guild_id, "onboarding", "application_chat_enabled", bool(enabled))
        if category is not None:
            runtime_db.set_module_setting(guild_id, "onboarding", "application_category_id", int(category.id))
        if lead_role is not None:
            runtime_db.set_module_setting(guild_id, "onboarding", "application_lead_role_id", int(lead_role.id))

        conf = _application_cfg(inter.guild)
        cat_obj = inter.guild.get_channel(int(conf.get("category_id") or 0))
        role_obj = inter.guild.get_role(int(conf.get("lead_role_id") or 0))
        warnings = []
        if enabled and not isinstance(cat_obj, discord.CategoryChannel):
            warnings.append("keine Bewerbungs-Kategorie gesetzt")
        if enabled and not isinstance(role_obj, discord.Role):
            warnings.append("keine Lead-Rolle gesetzt")
        text = (
            f"✅ Bewerbungs-Chat: **{'aktiv' if enabled else 'deaktiviert'}**\n"
            f"• Kategorie: {getattr(cat_obj, 'mention', '—')}\n"
            f"• Lead-Rolle: {role_obj.mention if role_obj else '—'}"
        )
        if warnings:
            text += "\n⚠️ " + ", ".join(warnings) + "."
        await inter.response.send_message(text, ephemeral=True)

    @onboarding_group.command(name="require_review", description="(Admin) Review durch Staff erzwingen")
    async def onboarding_require_review(inter: discord.Interaction, require: bool):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        c["require_review"] = bool(require)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Review erforderlich: {'Ja' if require else 'Nein'}", ephemeral=True)

    @onboarding_group.command(name="send", description="(Admin) Gewähltes Onboarding manuell für ein Mitglied starten")
    async def onboarding_send(inter: discord.Interaction, member: discord.Member):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        ok, reason = await send_onboarding_dm(member)
        if ok:
            await inter.followup.send(f"✅ Onboarding für {member.mention} gestartet ({_onboarding_mode(inter.guild_id).upper()}).", ephemeral=True)
        else:
            await inter.followup.send(f"❌ Onboarding für {member.mention} fehlgeschlagen: {reason}", ephemeral=True)

    @onboarding_group.command(name="sync_aion_roles", description="(Admin) Aion-2-Klassenrollen aus Profilen synchronisieren")
    async def onboarding_sync_aion_roles(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        synced = 0
        skipped = 0
        failures: list[str] = []
        for member in list(inter.guild.members):
            if member.bot:
                continue
            try:
                profile = runtime_db.get_aion2_profile(int(inter.guild_id), int(member.id)) or {}
            except Exception as exc:
                failures.append(f"{member.display_name}: Profilfehler {type(exc).__name__}")
                continue
            class_name = str(profile.get("class_name") or "").strip()
            if not _aion2_normalize_class(class_name):
                skipped += 1
                continue
            class_role, class_errors = await _assign_aion2_class_role(member, class_name)
            if class_role is not None:
                synced += 1
            if class_errors:
                failures.append(f"{member.display_name}: {'; '.join(class_errors)}")

        text = f"✅ Aion-Klassenrollen synchronisiert: **{synced}** · ohne Aion-Klasse: **{skipped}**"
        if failures:
            preview = "\n".join(f"• {line}" for line in failures[:8])
            text += f"\n⚠️ Fehler: **{len(failures)}**\n{preview}"
            if len(failures) > 8:
                text += f"\n… und {len(failures) - 8} weitere."
        await inter.followup.send(text, ephemeral=True)

    @onboarding_group.command(name="status", description="(Admin) Zeigt aktuelle Onboarding-Konfiguration")
    async def onboarding_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        cat = c.get("category_roles") or {}
        pri = c.get("primary_roles") or {}
        rch = _review_channel(inter.guild)
        wc = _welcome_cfg(inter.guild)
        wch = inter.guild.get_channel(int(wc.get("channel_id") or 0))
        app_cfg = _application_cfg(inter.guild)
        app_category = inter.guild.get_channel(int(app_cfg.get("category_id") or 0))
        app_lead_role = inter.guild.get_role(int(app_cfg.get("lead_role_id") or 0))

        def _m(rid):
            r = _role(inter.guild, rid)
            return r.mention if r else "—"

        mode_label = "Server-Onboarding" if _onboarding_mode(inter.guild_id) == "server" else "PM-Onboarding"
        text = (
            f"**Onboarding:** {'aktiv' if is_module_enabled(inter.guild_id, 'onboarding') else 'inaktiv'}\n"
            f"**Modus:** {mode_label}\n"
            f"**Welcome Card:** {'aktiv' if wc.get('enabled') else 'inaktiv'}\n"
            f"**Welcome-Kanal:** {wch.mention if wch else '—'}\n"
            f"**Welcome-Sprüche:** {len(wc.get('slogans') or [])}\n"
            f"**Leave-Update:** {'Ja' if wc.get('update_on_leave') else 'Nein'}\n"
            f"**Review erforderlich:** {'Ja (Server-Onboarding)' if _onboarding_mode(inter.guild_id) == 'server' else ('Ja' if c.get('require_review') else 'Nein')}\n"
            f"**Review/Log-Kanal:** {rch.mention if rch else 'wird bei Server-Onboarding automatisch erstellt'}\n"
            f"**Bewerbungs-Chat (PM):** {'aktiv' if app_cfg.get('enabled') else 'inaktiv'}\n"
            f"**Bewerbungs-Kategorie:** {app_category.mention if isinstance(app_category, discord.CategoryChannel) else '—'}\n"
            f"**Bewerbungs-Lead:** {app_lead_role.mention if app_lead_role else '—'}\n\n"
            f"**Typen**\n"
            f"• Allianz: {_m(cat.get('ally'))}\n"
            f"• Freund: {_m(cat.get('friend'))}\n"
            f"• Bewerber: {_m(cat.get('applicant'))}\n\n"
            f"**Primärrollen**\n"
            f"• 🛡️ {_m(pri.get('TANK'))}\n"
            f"• 🎵 {_m(pri.get('SUPPORT') or pri.get('HEAL'))}\n"
            f"• 🗡️ {_m(pri.get('DPS'))}\n\n"
            f"**Aion 2:** {'aktiv' if _aion2_enabled(inter.guild_id) else 'inaktiv'} · Klasse + Charaktername"
        )

        await inter.response.send_message(text, ephemeral=True)
