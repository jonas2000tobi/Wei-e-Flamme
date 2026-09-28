from __future__ import annotations
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
    from guild_modules import DEFAULT_ONBOARDING_WELCOME_SLOGANS
except Exception:
    DEFAULT_ONBOARDING_WELCOME_SLOGANS = (
        "Ein neuer Held betritt das Schlachtfeld.",
        "Ein wildes {user} ist erschienen!",
        "Möge dein Loot besser sein als dein Würfelglück.",
    )
from discord.ui import View, button
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

# cfg[guild_id] = {
#   "enabled": bool,
#   "review_channel": int,
#   "require_review": bool,
#   "category_roles": {"guild": int, "ally": int, "friend": int, "applicant": int},
#   "primary_roles":  {"TANK": int, "HEAL": int, "DPS": int},
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
    primary_txt = {"TANK": "Tank", "HEAL": "Heal", "DPS": "DPS"}.get(str(primary or "").upper(), "—")
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
    c.setdefault("experience_roles", {})
    cfg[str(guild.id)] = c
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


async def _assign_roles(member: discord.Member, category_key: str, primary_key: str, experienced: bool) -> tuple[List[discord.Role], List[str]]:
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
    wanted.append((pkey or "Primärrolle", prim_map.get(pkey)))

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
    ):
        self.member_id = int(member_id)
        self.guild_id = int(guild_id)
        self.message_id = int(message_id or 0)
        self.stage = str(stage or "category")
        self.category = category
        self.primary = primary
        self.experienced = experienced

    def to_dict(self) -> dict:
        return {
            "member_id": self.member_id,
            "guild_id": self.guild_id,
            "message_id": self.message_id,
            "stage": self.stage,
            "category": self.category,
            "primary": self.primary,
            "experienced": self.experienced,
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
        return True


class CategoryView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, cat: str):
        self.ctx.category = cat
        self.ctx.stage = "primary"
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        _remember_ctx(self.ctx)
        await inter.response.edit_message(
            content="Welche **Spielrolle** spielst du?",
            view=PrimaryView(self.ctx)
        )

    @button(label="⚔️ Gildenmitglied", style=ButtonStyle.primary, custom_id="onboarding_category_guild")
    async def btn_guild(self, inter: discord.Interaction, _):
        await self._next(inter, "guild")

    @button(label="🏰 Allianzmitglied", style=ButtonStyle.secondary, custom_id="onboarding_category_ally")
    async def btn_ally(self, inter: discord.Interaction, _):
        await self._next(inter, "ally")

    @button(label="🫱 Freund", style=ButtonStyle.success, custom_id="onboarding_category_friend")
    async def btn_friend(self, inter: discord.Interaction, _):
        await self._next(inter, "friend")

    @button(label="📝 Bewerber", style=ButtonStyle.secondary, custom_id="onboarding_category_applicant")
    async def btn_applicant(self, inter: discord.Interaction, _):
        await self._next(inter, "applicant")

class PrimaryView(OnboardingFeatureView):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, primary: str):
        self.ctx.primary = primary
        self.ctx.stage = "experience"
        if inter.message:
            self.ctx.message_id = int(inter.message.id)
        _remember_ctx(self.ctx)
        await inter.response.edit_message(
            content="Bist du **erfahren** oder **unerfahren**?",
            view=ExperienceView(self.ctx)
        )

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="onboarding_primary_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._next(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="onboarding_primary_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._next(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="onboarding_primary_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._next(inter, "DPS")

class ReviewView(OnboardingFeatureView):
    def __init__(self, member_id: int, category: str, primary: str, experienced: bool, *, message_id: int = 0, guild_id: int = 0):
        super().__init__(timeout=None)
        self.member_id = int(member_id)
        self.category = category
        self.primary = primary
        self.experienced = experienced
        self.message_id = int(message_id or 0)
        self.guild_id = int(guild_id or 0)

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
        await update_welcome_card(
            member,
            "completed",
            category=self.category,
            primary=self.primary,
            experienced=self.experienced,
        )
        if self.category == "applicant":
            await _post_application_status(
                member,
                "✅ **Bewerbung/Onboarding akzeptiert.** Die Gildenleitung kann hier die nächsten Schritte mit dir klären.",
                status="accepted",
                actor=inter.user,
            )
        member_name = discord.utils.escape_markdown(member.display_name or member.name)
        role_error_text = ""
        if role_errors:
            role_error_text = "\n⚠️ **Nicht gesetzt:** " + " · ".join(role_errors)
        await inter.edit_original_response(
            content=(
                f"✅ **Akzeptiert:** **{member_name}** ({member.mention}) – Rollen: "
                f"{', '.join(r.mention for r in roles) if roles else '—'}"
                f"{role_error_text}"
            ),
            view=None
        )
        _forget_message(self.message_id or (inter.message.id if inter.message else 0))

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
            await update_welcome_card(
                member,
                "rejected",
                category=self.category,
                primary=self.primary,
                experienced=self.experienced,
                reason="Die Gildenleitung hat das Onboarding nicht freigegeben.",
            )
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
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=None)
        self.ctx = ctx

    async def _finish(self, inter: discord.Interaction, experienced: bool):
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

            c = _gcfg(guild)
            review_ch = _review_channel(guild)
            require = bool(c.get("require_review"))

            member = guild.get_member(self.ctx.member_id)
            if not member:
                try:
                    member = await guild.fetch_member(self.ctx.member_id)
                except Exception:
                    member = None

            cat_txt = {
                "guild": "Gildenmitglied",
                "ally": "Allianzmitglied",
                "friend": "Freund",
                "applicant": "Bewerber",
            }.get(self.ctx.category, "—")

            pri_txt = {
                "TANK": "Tank",
                "HEAL": "Heal",
                "DPS": "DPS",
            }.get(self.ctx.primary, "—")

            exp_txt = "Erfahren" if experienced else "Unerfahren"

            if require and not review_ch:
                await inter.edit_original_response(
                    content="❌ Review ist aktiviert, aber kein Review-Kanal gesetzt.",
                    view=None
                )
                return

            application_channel: Optional[discord.TextChannel] = None
            application_info = ""
            if member and self.ctx.category == "applicant":
                await _set_pending_applicant_role(member, True)
                application_channel, application_info = await _ensure_application_channel(
                    member,
                    category=self.ctx.category,
                    primary=self.ctx.primary,
                    experienced=experienced,
                )

            if require:
                desc = (
                    f"**Onboarding-Review:** {member.mention if member else f'<@{self.ctx.member_id}>'}\n"
                    f"**Kategorie:** {cat_txt}\n"
                    f"**Rolle:** {pri_txt}\n"
                    f"**Erfahrung:** {exp_txt}"
                )
                if self.ctx.category == "applicant":
                    if application_channel is not None:
                        desc += f"\n**Bewerbungs-Chat:** {application_channel.mention}"
                    elif application_info:
                        desc += f"\n⚠️ **Bewerbungs-Chat:** {application_info}"

                review_view = ReviewView(
                    self.ctx.member_id,
                    self.ctx.category,
                    self.ctx.primary,
                    experienced,
                    guild_id=self.ctx.guild_id,
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
                )
                _session_records[str(review_message.id)] = review_ctx.to_dict()
                _save_sessions()

                if member:
                    await update_welcome_card(
                        member,
                        "review",
                        category=self.ctx.category,
                        primary=self.ctx.primary,
                        experienced=experienced,
                    )
                user_done_text = "✅ Danke! Deine Angaben wurden zur **Prüfung** an die Gildenleitung gesendet."
                if application_channel is not None:
                    user_done_text += f"\n📝 Dein privater Bewerbungs-Chat wurde erstellt: {application_channel.mention}"
                await inter.edit_original_response(
                    content=user_done_text,
                    view=None
                )
            else:
                if member:
                    roles, role_errors = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)
                    await update_welcome_card(
                        member,
                        "completed",
                        category=self.ctx.category,
                        primary=self.ctx.primary,
                        experienced=experienced,
                    )

                    if review_ch:
                        err_line = ("\n⚠️ Nicht gesetzt: " + " · ".join(role_errors)) if role_errors else ""
                        await review_ch.send(
                            f"📝 **Auto-Onboarding:** {member.mention} – {cat_txt}, {pri_txt}, {exp_txt}\n"
                            f"Rollen: {', '.join(r.mention for r in roles) if roles else '—'}{err_line}"
                        )

                auto_done_text = "✅ Danke! Deine Rollen wurden vergeben."
                if member and role_errors:
                    auto_done_text += "\n⚠️ Einige Rollen konnten nicht gesetzt werden. Die Gildenleitung wurde informiert."
                if application_channel is not None:
                    auto_done_text += f"\n📝 Dein privater Bewerbungs-Chat wurde erstellt: {application_channel.mention}"
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

            print(f"[onboarding] ExperienceView _finish Fehler: {e!r}")

    @button(label="🧠 Erfahren", style=ButtonStyle.primary, custom_id="onboarding_experience_yes")
    async def btn_exp(self, inter: discord.Interaction, _):
        await self._finish(inter, True)

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary, custom_id="onboarding_experience_no")
    async def btn_new(self, inter: discord.Interaction, _):
        await self._finish(inter, False)

async def send_onboarding_dm(member: discord.Member) -> tuple[bool, str]:
    try:
        if member.bot:
            return False, "Botkonten werden nicht onboardet."

        c = _gcfg(member.guild)
        if not is_module_enabled(member.guild.id, "onboarding"):
            return False, "Onboarding ist für diesen Server deaktiviert."

        await ensure_welcome_card(member)
        await update_welcome_card(member, "running")
        _forget_member_sessions(member.id)
        ctx = StepContext(member.id, member.guild.id)

        text = (
            f"👋 **Willkommen {member.display_name}!**\n\n"
            f"Wähle bitte zuerst deine **Kategorie**."
        )

        message = await member.send(text, view=CategoryView(ctx))
        ctx.message_id = int(message.id)
        ctx.stage = "category"
        _remember_ctx(ctx)
        return True, "Onboarding-DM gesendet."

    except discord.Forbidden:
        try:
            await update_welcome_card(
                member,
                "dm_blocked",
                reason="Direktnachrichten sind deaktiviert. Bitte DMs für diesen Server aktivieren und das Onboarding erneut starten.",
            )
        except Exception:
            pass
        return False, "DM konnte nicht zugestellt werden. Das Mitglied hat Direktnachrichten vermutlich deaktiviert."
    except Exception as exc:
        print(f"[onboarding] DM an {getattr(member, 'id', 0)} fehlgeschlagen: {exc!r}", flush=True)
        return False, f"{type(exc).__name__}: {str(exc)[:240]}"


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
            if ctx.stage == "category":
                client.add_view(CategoryView(ctx), message_id=mid)
            elif ctx.stage == "primary":
                client.add_view(PrimaryView(ctx), message_id=mid)
            elif ctx.stage == "experience":
                client.add_view(ExperienceView(ctx), message_id=mid)
            elif ctx.stage == "review":
                client.add_view(
                    ReviewView(
                        ctx.member_id,
                        str(ctx.category or ""),
                        str(ctx.primary or ""),
                        bool(ctx.experienced),
                        message_id=mid,
                        guild_id=ctx.guild_id,
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
        gildenmitglied: discord.Role,
        allianzmitglied: discord.Role,
        freund: discord.Role,
        bewerber: discord.Role,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        c["category_roles"] = {
            "guild": gildenmitglied.id,
            "ally": allianzmitglied.id,
            "friend": freund.id,
            "applicant": bewerber.id,
        }

        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(
            f"✅ Kategorien gesetzt:\n"
            f"• Gildenmitglied: {gildenmitglied.mention}\n"
            f"• Allianzmitglied: {allianzmitglied.mention}\n"
            f"• Freund: {freund.mention}\n"
            f"• Bewerber: {bewerber.mention}",
            ephemeral=True
        )

    @onboarding_group.command(name="set_primaries", description="(Admin) Primärrollen für Tank/Heal/DPS setzen")
    async def onboarding_set_primaries(
        inter: discord.Interaction,
        tank: discord.Role,
        heal: discord.Role,
        dps: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        c["primary_roles"] = {"TANK": tank.id, "HEAL": heal.id, "DPS": dps.id}
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(
            f"✅ Primärrollen gesetzt:\n• 🛡️ {tank.mention}\n• 💚 {heal.mention}\n• 🗡️ {dps.mention}",
            ephemeral=True
        )

    @onboarding_group.command(name="set_experience", description="(Admin) Rollen für Erfahren/Unerfahren setzen")
    async def onboarding_set_experience(
        inter: discord.Interaction,
        experienced_role: Optional[discord.Role] = None,
        newbie_role: Optional[discord.Role] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        c["experience_roles"] = {
            "experienced": int(experienced_role.id) if experienced_role else 0,
            "newbie": int(newbie_role.id) if newbie_role else 0
        }

        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(
            f"✅ Erfahrungsrollen gesetzt:\n"
            f"• 🧠 {experienced_role.mention if experienced_role else '—'}\n"
            f"• 🌱 {newbie_role.mention if newbie_role else '—'}",
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

    @onboarding_group.command(name="send", description="(Admin) Onboarding-DM manuell an ein Mitglied senden")
    async def onboarding_send(inter: discord.Interaction, member: discord.Member):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        ok, reason = await send_onboarding_dm(member)
        if ok:
            await inter.followup.send(f"✅ Onboarding-DM an {member.mention} geschickt.", ephemeral=True)
        else:
            await inter.followup.send(f"❌ Onboarding-DM an {member.mention} fehlgeschlagen: {reason}", ephemeral=True)

    @onboarding_group.command(name="status", description="(Admin) Zeigt aktuelle Onboarding-Konfiguration")
    async def onboarding_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild)
        cat = c.get("category_roles") or {}
        pri = c.get("primary_roles") or {}
        exp = c.get("experience_roles") or {}
        rch = _review_channel(inter.guild)
        wc = _welcome_cfg(inter.guild)
        wch = inter.guild.get_channel(int(wc.get("channel_id") or 0))
        app_cfg = _application_cfg(inter.guild)
        app_category = inter.guild.get_channel(int(app_cfg.get("category_id") or 0))
        app_lead_role = inter.guild.get_role(int(app_cfg.get("lead_role_id") or 0))

        def _m(rid):
            r = _role(inter.guild, rid)
            return r.mention if r else "—"

        text = (
            f"**Onboarding:** {'aktiv' if is_module_enabled(inter.guild_id, 'onboarding') else 'inaktiv'}\n"
            f"**Welcome Card:** {'aktiv' if wc.get('enabled') else 'inaktiv'}\n"
            f"**Welcome-Kanal:** {wch.mention if wch else '—'}\n"
            f"**Welcome-Sprüche:** {len(wc.get('slogans') or [])}\n"
            f"**Leave-Update:** {'Ja' if wc.get('update_on_leave') else 'Nein'}\n"
            f"**Review erforderlich:** {'Ja' if c.get('require_review') else 'Nein'}\n"
            f"**Review/Log-Kanal:** {rch.mention if rch else '—'}\n"
            f"**Bewerbungs-Chat:** {'aktiv' if app_cfg.get('enabled') else 'inaktiv'}\n"
            f"**Bewerbungs-Kategorie:** {app_category.mention if isinstance(app_category, discord.CategoryChannel) else '—'}\n"
            f"**Bewerbungs-Lead:** {app_lead_role.mention if app_lead_role else '—'}\n\n"
            f"**Kategorien**\n"
            f"• Gildenmitglied: {_m(cat.get('guild'))}\n"
            f"• Allianzmitglied: {_m(cat.get('ally'))}\n"
            f"• Freund: {_m(cat.get('friend'))}\n"
            f"• Bewerber: {_m(cat.get('applicant'))}\n\n"
            f"**Primärrollen**\n"
            f"• 🛡️ {_m(pri.get('TANK'))}\n"
            f"• 💚 {_m(pri.get('HEAL'))}\n"
            f"• 🗡️ {_m(pri.get('DPS'))}\n\n"
            f"**Erfahrung**\n"
            f"• 🧠 {_m(exp.get('experienced'))}\n"
            f"• 🌱 {_m(exp.get('newbie'))}"
        )

        await inter.response.send_message(text, ephemeral=True)
