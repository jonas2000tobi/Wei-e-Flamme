# bot/onboarding.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"

# cfg[guild_id] = {
#   "category_roles": {"guild": int, "ally": int, "friend": int},
#   "primary_roles":  {"TANK": int, "HEAL": int, "DPS": int},
#   "experience_roles": {"experienced": int, "newbie": int},
#   "enabled": true/false
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
    return cfg.get(str(guild.id)) or {}

def _role(guild: discord.Guild, rid: int | None) -> Optional[discord.Role]:
    return guild.get_role(int(rid or 0)) if rid else None

async def _assign_roles(member: discord.Member, category_key: str, primary_key: str, experienced: bool) -> list[discord.Role]:
    """Weist konfigurierten Rollen-Korb zu. Gibt tatsächlich zugewiesene Rollen zurück."""
    out: list[discord.Role] = []
    g = member.guild
    c = _gcfg(g)

    # Kategorie
    cat_map = (c.get("category_roles") or {})
    cat_rid = {
        "guild": cat_map.get("guild"),
        "ally":  cat_map.get("ally"),
        "friend":cat_map.get("friend"),
    }.get(category_key)
    r = _role(g, cat_rid)
    if r: out.append(r)

    # Primärrolle
    prim_map = (c.get("primary_roles") or {})
    r = _role(g, prim_map.get(primary_key.upper()))
    if r: out.append(r)

    # Erfahrungsrolle
    exp_map = (c.get("experience_roles") or {})
    r = _role(g, exp_map.get("experienced" if experienced else "newbie"))
    if r: out.append(r)

    # Zuweisen (nur Rollen, die der Bot auch setzen darf)
    granted = []
    for role in out:
        try:
            if role not in member.roles:
                await member.add_roles(role, reason="Onboarding")
            granted.append(role)
        except Exception:
            # fehlende Berechtigung / Rollen-Hierarchie – einfach überspringen
            pass
    return granted

# ----------------- DM Views (3 Schritte) -----------------

class StepContext:
    """Trägt die Zwischenauswahl während des Flows."""
    def __init__(self, member_id: int, guild_id: int):
        self.member_id = member_id
        self.guild_id = guild_id
        self.category: str | None = None     # "guild" | "ally" | "friend"
        self.primary: str  | None = None     # "TANK" | "HEAL" | "DPS"
        self.experienced: bool | None = None # True | False

# in-memory, reicht für den kurzen DM-Flow
_sessions: dict[int, StepContext] = {}  # key=user_id

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

class ExperienceView(View):
    def __init__(self, ctx: StepContext):
        super().__init__(timeout=180)
        self.ctx = ctx

    async def _finish(self, inter: discord.Interaction, experienced: bool):
        self.ctx.experienced = experienced

        # Member besorgen
        guild = inter.client.get_guild(self.ctx.guild_id)
        if not guild:
            await inter.response.edit_message(content="⚠️ Server nicht gefunden.", view=None)
            return
        member = guild.get_member(self.ctx.member_id)
        if not member:
            try:
                member = await guild.fetch_member(self.ctx.member_id)
            except Exception:
                member = None
        if not member:
            await inter.response.edit_message(content="⚠️ Mitglied nicht gefunden.", view=None)
            return

        roles = await _assign_roles(member, self.ctx.category, self.ctx.primary, experienced)
        role_mentions = ", ".join(r.mention for r in roles) if roles else "keine (Fehlende Berechtigung?)"
        _sessions.pop(self.ctx.member_id, None)

        await inter.response.edit_message(
            content=f"✅ Danke! Rollen vergeben: {role_mentions}",
            view=None
        )
        # Optional: Willkommen im Kanal posten? (absichtlich weggelassen)

    @button(label="🧠 Erfahren", style=ButtonStyle.primary)
    async def btn_exp(self, inter: discord.Interaction, _):
        await self._finish(inter, True)

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary)
    async def btn_new(self, inter: discord.Interaction, _):
        await self._finish(inter, False)

# ----------------- Public API -----------------

async def send_onboarding_dm(member: discord.Member) -> None:
    """DM-Flow starten (wird von on_member_join aufgerufen oder per Slash)."""
    try:
        if member.bot:
            return
        c = _gcfg(member.guild)
        if not c.get("enabled", True):
            return
        # Session
        ctx = StepContext(member.id, member.guild.id)
        _sessions[member.id] = ctx
        text = (
            f"👋 **Willkommen {member.display_name}!**\n\n"
            f"Wähle bitte zuerst deine **Kategorie**."
        )
        await member.send(text, view=CategoryView(ctx))
    except Exception:
        # DMs evtl. geschlossen – stillschweigend ignorieren
        pass

# ----------------- Slash-Commands (Setup/Tools) -----------------

async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree) -> None:
    @tree.command(name="onboarding_toggle", description="(Admin) Onboarding ein-/ausschalten")
    @app_commands.describe(enabled="true = an, false = aus")
    async def onboarding_toggle(inter: discord.Interaction, enabled: bool):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = _gcfg(inter.guild)
        c["enabled"] = bool(enabled)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
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
        exp_txt = experienced_role.mention if experienced_role else "—"
        new_txt = newbie_role.mention if newbie_role else "—"
        await inter.response.send_message(
            f"✅ Erfahrungsrollen gesetzt:\n• 🧠 Erfahren: {exp_txt}\n• 🌱 Unerfahren: {new_txt}",
            ephemeral=True
        )

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
        def _m(rid): 
            r = _role(inter.guild, rid); 
            return r.mention if r else "—"
        text = (
            f"**Onboarding:** {'aktiv' if c.get('enabled', True) else 'inaktiv'}\n\n"
            f"**Kategorien**\n• Gildenmitglied: {_m(cat.get('guild'))}\n• Allianzmitglied: {_m(cat.get('ally'))}\n• Freund: {_m(cat.get('friend'))}\n\n"
            f"**Primärrollen**\n• 🛡️ {_m(pri.get('TANK'))}\n• 💚 {_m(pri.get('HEAL'))}\n• 🗡️ {_m(pri.get('DPS'))}\n\n"
            f"**Erfahrung**\n• 🧠 {_m(exp.get('experienced'))}\n• 🌱 {_m(exp.get('newbie'))}"
        )
        await inter.response.send_message(text, ephemeral=True)
