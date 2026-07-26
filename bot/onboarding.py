from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, List

try:
    from bot.json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore
except Exception:
    from json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore

import discord
from discord import app_commands
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

class CategoryView(View):
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

class PrimaryView(View):
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

class ReviewView(View):
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
            try:
                await member.send("❌ Deine Anfrage wurde **abgelehnt**.")
            except Exception:
                pass

class ExperienceView(View):
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

                await inter.edit_original_response(
                    content="✅ Danke! Deine Angaben wurden zur **Prüfung** an die Gildenleitung gesendet.",
                    view=None
                )
            else:
                if member:
                    roles = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)

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
        if not c.get("enabled", True):
            return False, "Onboarding ist für diesen Server deaktiviert."

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
        return False, "DM konnte nicht zugestellt werden. Das Mitglied hat Direktnachrichten vermutlich deaktiviert."
    except Exception as exc:
        print(f"[onboarding] DM an {getattr(member, 'id', 0)} fehlgeschlagen: {exc!r}", flush=True)
        return False, f"{type(exc).__name__}: {str(exc)[:240]}"


async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree) -> None:
    onboarding_group = app_commands.Group(
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

        c = _gcfg(inter.guild)
        c["enabled"] = bool(enabled)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Onboarding {'aktiviert' if enabled else 'deaktiviert'}.", ephemeral=True)

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

        def _m(rid):
            r = _role(inter.guild, rid)
            return r.mention if r else "—"

        text = (
            f"**Onboarding:** {'aktiv' if c.get('enabled', True) else 'inaktiv'}\n"
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
