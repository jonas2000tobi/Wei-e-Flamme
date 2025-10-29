# bot/onboarding.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, List

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"

# cfg[guild_id] = {
#   "enabled": bool,
#   "review_channel": int,               # Kanal-ID für Review/Logs
#   "require_review": bool,              # ob Staff bestätigen muss
#   "category_roles": {"guild": int, "ally": int, "friend": int},
#   "primary_roles":  {"TANK": int, "HEAL": int, "DPS": int},
#   "experience_roles": {"experienced": int, "newbie": int}
# }

def _load_cfg() -> dict:
    try:
        return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cfg(obj: dict) -> None:
    CFG_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

cfg: dict = _load_cfg()

# ----------------- Helpers -----------------

def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

def _gcfg(guild: discord.Guild) -> dict:
    c = cfg.get(str(guild.id)) or {}
    # Defaults
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

    # Kategorie
    cat_map = (c.get("category_roles") or {})
    cat_rid = {"guild": cat_map.get("guild"), "ally": cat_map.get("ally"), "friend": cat_map.get("friend")}.get(category_key)
    r = _role(g, cat_rid);  out += [r] if r else []

    # Primärrolle
    prim_map = (c.get("primary_roles") or {})
    r = _role(g, prim_map.get(primary_key.upper()));  out += [r] if r else []

    # Erfahrungsrolle
    exp_map = (c.get("experience_roles") or {})
    r = _role(g, exp_map.get("experienced" if experienced else "newbie"));  out += [r] if r else []

    granted = []
    for role in out:
        try:
            if role and role not in member.roles:
                await member.add_roles(role, reason="Onboarding")
            if role:
                granted.append(role)
        except Exception:
            # fehlende Berechtigungen/Hierarchie – ignorieren
            pass
    return granted

def _review_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id = int((_gcfg(guild).get("review_channel") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None

# ----------------- DM Flow (3 Schritte) -----------------

class StepContext:
    def __init__(self, member_id: int, guild_id: int):
        self.member_id = member_id
        self.guild_id = guild_id
        self.category: str | None = None     # "guild" | "ally" | "friend"
        self.primary: str  | None = None     # "TANK" | "HEAL" | "DPS"
        self.experienced: bool | None = None # True | False

_sessions: dict[int, StepContext] = {}

class CategoryView(View):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=180)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, cat: str):
        self.ctx.category = cat
        await inter.response.edit_message(
            content="Welche **Spielrolle** spielst du?",
            view=PrimaryView(self.ctx)
        )

    @button(label="⚔️ Gildenmitglied", style=ButtonStyle.primary)
    async def btn_guild(self, inter: discord.Interaction, _):
        await self._next(inter, "guild")

    @button(label="🏰 Allianzmitglied", style=ButtonStyle.secondary)
    async def btn_ally(self, inter: discord.Interaction, _):
        await self._next(inter, "ally")

    @button(label="🫱 Freund", style=ButtonStyle.success)
    async def btn_friend(self, inter: discord.Interaction, _):
        await self._next(inter, "friend")

class PrimaryView(View):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=180)
        self.ctx = ctx

    async def _next(self, inter: discord.Interaction, primary: str):
        self.ctx.primary = primary
        await inter.response.edit_message(
            content="Bist du **erfahren** oder **unerfahren**?",
            view=ExperienceView(self.ctx)
        )

    @button(label="🛡️ Tank", style=ButtonStyle.primary)
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._next(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary)
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._next(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary)
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._next(inter, "DPS")

# -------- Review-Buttons (optional) --------

class ReviewView(View):
    def __init__(self, member_id: int, category: str, primary: str, experienced: bool):
        super().__init__(timeout=None)
        self.member_id = member_id
        self.category = category
        self.primary = primary
        self.experienced = experienced

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

    @button(label="✅ Akzeptieren", style=ButtonStyle.success)
    async def btn_accept(self, inter: discord.Interaction, _):
        if not self._is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        member = await self._get_member(inter.guild)
        if not member:
            await inter.response.send_message("Mitglied nicht gefunden.", ephemeral=True); return
        roles = await _assign_roles(member, self.category, self.primary, self.experienced)
        await inter.response.edit_message(content=f"✅ **Akzeptiert** – Rollen: {', '.join(r.mention for r in roles) if roles else '—'}", view=None)
        try:
            await member.send("✅ Deine Anfrage wurde **akzeptiert**. Willkommen!")
        except Exception:
            pass

    @button(label="❌ Ablehnen", style=ButtonStyle.danger)
    async def btn_deny(self, inter: discord.Interaction, _):
        if not self._is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        member = await self._get_member(inter.guild)
        await inter.response.edit_message(content="❌ **Abgelehnt**.", view=None)
        if member:
            try:
                await member.send("❌ Deine Anfrage wurde **abgelehnt**.")
            except Exception:
                pass

class ExperienceView(View):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=180)
        self.ctx = ctx

    async def _finish(self, inter: discord.Interaction, experienced: bool):
        self.ctx.experienced = experienced
        guild = inter.client.get_guild(self.ctx.guild_id)
        if not guild:
            await inter.response.edit_message(content="⚠️ Server nicht gefunden.", view=None); return

        # Review oder Auto-Assign?
        c = _gcfg(guild)
        review_ch = _review_channel(guild)
        require = bool(c.get("require_review"))

        # Member referenzieren
        member = guild.get_member(self.ctx.member_id)
        if not member:
            try:
                member = await guild.fetch_member(self.ctx.member_id)
            except Exception:
                member = None

        # Compose Zusammenfassung
        cat_txt = {"guild": "Gildenmitglied", "ally": "Allianzmitglied", "friend": "Freund"}.get(self.ctx.category, "—")
        pri_txt = {"TANK": "Tank", "HEAL": "Heal", "DPS": "DPS"}.get(self.ctx.primary, "—")
        exp_txt = "Erfahren" if experienced else "Unerfahren"

        if require and review_ch:
            # Post mit Accept/Deny
            desc = (
                f"**Onboarding-Review:** {member.mention if member else f'<@{self.ctx.member_id}>'}\n"
                f"**Kategorie:** {cat_txt}\n"
                f"**Rolle:** {pri_txt}\n"
                f"**Erfahrung:** {exp_txt}"
            )
            await review_ch.send(desc, view=ReviewView(self.ctx.member_id, self.ctx.category, self.ctx.primary, experienced))
            await inter.response.edit_message(content="✅ Danke! Deine Angaben wurden zur **Prüfung** an die Gildenleitung gesendet.", view=None)
        else:
            # Auto-Assign
            if member:
                roles = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)
                if review_ch:
                    await review_ch.send(
                        f"📝 **Auto-Onboarding:** {member.mention} – {cat_txt}, {pri_txt}, {exp_txt}\n"
                        f"Rollen: {', '.join(r.mention for r in roles) if roles else '—'}"
                    )
            await inter.response.edit_message(content="✅ Danke! Deine Rollen wurden vergeben.", view=None)

        _sessions.pop(self.ctx.member_id, None)

    @button(label="🧠 Erfahren", style=ButtonStyle.primary)
    async def btn_exp(self, inter: discord.Interaction, _):
        await self._finish(inter, True)

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary)
    async def btn_new(self, inter: discord.Interaction, _):
        await self._finish(inter, False)

# ----------------- Public API -----------------

async def send_onboarding_dm(member: discord.Member) -> None:
    try:
        if member.bot:
            return
        c = _gcfg(member.guild)
        if not c.get("enabled", True):
            return
        ctx = StepContext(member.id, member.guild.id)
        _sessions[member.id] = ctx
        text = (
            f"👋 **Willkommen {member.display_name}!**\n\n"
            f"Wähle bitte zuerst deine **Kategorie**."
        )
        await member.send(text, view=CategoryView(ctx))
    except Exception:
        # DMs evtl. geschlossen – ignoriere
        pass

# ----------------- Slash-Commands -----------------

async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree) -> None:
    @tree.command(name="onboarding_toggle", description="(Admin) Onboarding ein-/ausschalten")
    @app_commands.describe(enabled="true = an, false = aus")
    async def onboarding_toggle(inter: discord.Interaction, enabled: bool):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild); c["enabled"] = bool(enabled); cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Onboarding {'aktiviert' if enabled else 'deaktiviert'}.", ephemeral=True)

    @tree.command(name="onboarding_set_categories", description="(Admin) Rollen für Gildenmitglied / Allianzmitglied / Freund setzen")
    async def onboarding_set_categories(
        inter: discord.Interaction,
        gildenmitglied: discord.Role,
        allianzmitglied: discord.Role,
        freund: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["category_roles"] = {"guild": gildenmitglied.id, "ally": allianzmitglied.id, "friend": freund.id}
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(
            f"✅ Kategorien gesetzt:\n• Gildenmitglied: {gildenmitglied.mention}\n• Allianzmitglied: {allianzmitglied.mention}\n• Freund: {freund.mention}",
            ephemeral=True
        )

    @tree.command(name="onboarding_set_primaries", description="(Admin) Primärrollen für Tank/Heal/DPS setzen")
    async def onboarding_set_primaries(
        inter: discord.Interaction,
        tank: discord.Role,
        heal: discord.Role,
        dps: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["primary_roles"] = {"TANK": tank.id, "HEAL": heal.id, "DPS": dps.id}
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(
            f"✅ Primärrollen gesetzt:\n• 🛡️ {tank.mention}\n• 💚 {heal.mention}\n• 🗡️ {dps.mention}",
            ephemeral=True
        )

    @tree.command(name="onboarding_set_experience", description="(Admin) Rollen für Erfahren/Unerfahren setzen")
    async def onboarding_set_experience(
        inter: discord.Interaction,
        experienced_role: Optional[discord.Role] = None,
        newbie_role: Optional[discord.Role] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["experience_roles"] = {"experienced": int(experienced_role.id) if experienced_role else 0,
                                 "newbie": int(newbie_role.id) if newbie_role else 0}
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(
            f"✅ Erfahrungsrollen gesetzt:\n• 🧠 {experienced_role.mention if experienced_role else '—'}\n• 🌱 {newbie_role.mention if newbie_role else '—'}",
            ephemeral=True
        )

    @tree.command(name="onboarding_set_review_channel", description="(Admin) Kanal für Review/Logs setzen")
    async def onboarding_set_review_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["review_channel"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Review/Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onboarding_require_review", description="(Admin) Review durch Staff erzwingen (Accept/Deny)")
    async def onboarding_require_review(inter: discord.Interaction, require: bool):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["require_review"] = bool(require)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Review erforderlich: {'Ja' if require else 'Nein'}", ephemeral=True)

    @tree.command(name="onboarding_send", description="(Admin) Onboarding-DM manuell an ein Mitglied senden")
    async def onboarding_send(inter: discord.Interaction, member: discord.Member):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        await send_onboarding_dm(member)
        await inter.response.send_message(f"✉️ DM an {member.mention} geschickt (falls DMs offen).", ephemeral=True)

    @tree.command(name="onboarding_status", description="(Admin) Zeigt aktuelle Onboarding-Konfiguration")
    async def onboarding_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        cat = c.get("category_roles") or {}
        pri = c.get("primary_roles") or {}
        exp = c.get("experience_roles") or {}
        rch = _review_channel(inter.guild)
        def _m(rid):
            r = _role(inter.guild, rid);  return r.mention if r else "—"
        text = (
            f"**Onboarding:** {'aktiv' if c.get('enabled', True) else 'inaktiv'}\n"
            f"**Review erforderlich:** {'Ja' if c.get('require_review') else 'Nein'}\n"
            f"**Review/Log-Kanal:** {rch.mention if rch else '—'}\n\n"
            f"**Kategorien**\n• Gildenmitglied: {_m(cat.get('guild'))}\n• Allianzmitglied: {_m(cat.get('ally'))}\n• Freund: {_m(cat.get('friend'))}\n\n"
            f"**Primärrollen**\n• 🛡️ {_m(pri.get('TANK'))}\n• 💚 {_m(pri.get('HEAL'))}\n• 🗡️ {_m(pri.get('DPS'))}\n\n"
            f"**Erfahrung**\n• 🧠 {_m(exp.get('experienced'))}\n• 🌱 {_m(exp.get('newbie'))}"
        )
        await inter.response.send_message(text, ephemeral=True)
