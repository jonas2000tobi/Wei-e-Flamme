from __future__ import annotations
import json
import random
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

async def _assign_roles(member: discord.Member, category_key: str, primary_key: str, experienced: bool) -> List[discord.Role]:
    out: List[discord.Role] = []
    g = member.guild
    c = _gcfg(g)

    cat_map = (c.get("category_roles") or {})
    cat_rid = {
        "guild": cat_map.get("guild"),
        "ally": cat_map.get("ally"),
        "friend": cat_map.get("friend"),
        "applicant": cat_map.get("applicant"),
    }.get(category_key)

    r = _role(g, cat_rid)
    out += [r] if r else []

    prim_map = (c.get("primary_roles") or {})
    r = _role(g, prim_map.get(primary_key.upper()))
    out += [r] if r else []

    exp_map = (c.get("experience_roles") or {})
    r = _role(g, exp_map.get("experienced" if experienced else "newbie"))
    out += [r] if r else []

    granted = []
    for role in out:
        try:
            if role and role not in member.roles:
                await member.add_roles(role, reason="Onboarding")
            if role:
                granted.append(role)
        except Exception:
            pass

    return granted

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

        roles = await _assign_roles(member, self.category, self.primary, self.experienced)
        await update_welcome_card(
            member,
            "completed",
            category=self.category,
            primary=self.primary,
            experienced=self.experienced,
        )
        await inter.edit_original_response(
            content=f"✅ **Akzeptiert** – Rollen: {', '.join(r.mention for r in roles) if roles else '—'}",
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
        await inter.edit_original_response(content="❌ **Abgelehnt**.", view=None)
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

            if require:
                if not review_ch:
                    await inter.edit_original_response(
                        content="❌ Review ist aktiviert, aber kein Review-Kanal gesetzt.",
                        view=None
                    )
                    return

                desc = (
                    f"**Onboarding-Review:** {member.mention if member else f'<@{self.ctx.member_id}>'}\n"
                    f"**Kategorie:** {cat_txt}\n"
                    f"**Rolle:** {pri_txt}\n"
                    f"**Erfahrung:** {exp_txt}"
                )

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
                await inter.edit_original_response(
                    content="✅ Danke! Deine Angaben wurden zur **Prüfung** an die Gildenleitung gesendet.",
                    view=None
                )
            else:
                if member:
                    roles = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)
                    await update_welcome_card(
                        member,
                        "completed",
                        category=self.ctx.category,
                        primary=self.ctx.primary,
                        experienced=experienced,
                    )

                    if review_ch:
                        await review_ch.send(
                            f"📝 **Auto-Onboarding:** {member.mention} – {cat_txt}, {pri_txt}, {exp_txt}\n"
                            f"Rollen: {', '.join(r.mention for r in roles) if roles else '—'}"
                        )

                await inter.edit_original_response(content="✅ Danke! Deine Rollen wurden vergeben.", view=None)

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
            f"**Review/Log-Kanal:** {rch.mention if rch else '—'}\n\n"
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
