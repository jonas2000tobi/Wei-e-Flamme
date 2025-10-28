# bot/onboarding_dm.py
# -----------------------------------------------------------
# Automatische Willkommens-DM mit Rollenwahl und Weiterleitung
# - Kategorien: Gildenmitglied / Allianzmitglied / Freund
# - Bei Gilde/Allianz: Abfrage "Erfahren/Unerfahren" (Unerfahren => NEWBIE-Rolle)
# - Anfrage-Post an Staff-/Gildenleitungs-Channel mit Accept/Ablehnen
# - Rollenvergabe per gespeicherten Rollen-IDs (nicht Namen!)
# - Optionaler Welcome-Channel für Begrüßungspost
# -----------------------------------------------------------

from __future__ import annotations
import json
from pathlib import Path

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"

def _load_cfg():
    try:
        return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cfg(obj):
    CFG_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

# cfg[str(guild_id)] = {
#   "accept_ch": int,            # Kanal für Gildenleitungs-Review
#   "welcome_ch": int,           # optionaler Welcome-Kanal
#   "newbie_role": int,          # Rolle für Unerfahren
#   "guild_role": int,           # Rolle für Gildenmitglied
#   "alliance_role": int,        # Rolle für Allianzmitglied
#   "friend_role": int           # Rolle für Freund
# }

cfg = _load_cfg()

# ---------------------------- UI Views ----------------------------

class RoleSelectView(View):
    """1. Schritt: Kategorie auswählen."""
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=None)
        self.guild = guild

    async def _send_to_admins(self, member: discord.Member, category: str, experience: str):
        gcfg = cfg.get(str(member.guild.id)) or {}
        accept_id = int(gcfg.get("accept_ch", 0) or 0)
        ch = member.guild.get_channel(accept_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        txt = (f"**Onboarding-Review:** {member.mention}\n"
               f"• Kategorie: **{category}**\n"
               f"• Erfahrung: **{experience or '—'}**\n"
               f"Bitte **Akzeptieren** oder **Ablehnen**.")
        await ch.send(txt, view=AcceptView(member.id, category, experience))

    async def _handle_category(self, inter: discord.Interaction, category: str):
        # Für Gilde/Allianz danach die Erfahrung abfragen
        if category in ("Gildenmitglied", "Allianzmitglied"):
            await inter.response.edit_message(
                content=f"🧭 Du hast **{category}** gewählt.\nBist du **Erfahren** oder **Unerfahren**?",
                view=ExperienceView(self.guild, category)
            )
        else:
            # Freund: direkt an Gildenleitung schicken
            await inter.response.edit_message(
                content=f"🧭 Du hast **{category}** gewählt.\nDeine Angaben gehen jetzt an die Gildenleitung.",
                view=None
            )
            await self._send_to_admins(inter.user, category, "N/A")

    @button(label="⚔️ Gildenmitglied", style=ButtonStyle.primary)
    async def btn_gilde(self, inter: discord.Interaction, _):
        await self._handle_category(inter, "Gildenmitglied")

    @button(label="🏰 Allianzmitglied", style=ButtonStyle.secondary)
    async def btn_allianz(self, inter: discord.Interaction, _):
        await self._handle_category(inter, "Allianzmitglied")

    @button(label="🫱 Freund", style=ButtonStyle.success)
    async def btn_friend(self, inter: discord.Interaction, _):
        await self._handle_category(inter, "Freund")

class ExperienceView(View):
    """2. Schritt für Gilde/Allianz: Erfahrung auswählen."""
    def __init__(self, guild: discord.Guild, category: str):
        super().__init__(timeout=None)
        self.guild = guild
        self.category = category

    async def _send_to_admins(self, member: discord.Member, experience: str):
        gcfg = cfg.get(str(member.guild.id)) or {}
        accept_id = int(gcfg.get("accept_ch", 0) or 0)
        ch = member.guild.get_channel(accept_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        txt = (f"**Onboarding-Review:** {member.mention}\n"
               f"• Kategorie: **{self.category}**\n"
               f"• Erfahrung: **{experience}**\n"
               f"Bitte **Akzeptieren** oder **Ablehnen**.")
        await ch.send(txt, view=AcceptView(member.id, self.category, experience))

    @button(label="🧠 Erfahren", style=ButtonStyle.primary)
    async def btn_exp(self, inter: discord.Interaction, _):
        await inter.response.edit_message(content="✅ Gespeichert: **Erfahren**.\nDie Gildenleitung prüft kurz.", view=None)
        await self._send_to_admins(inter.user, "Erfahren")

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary)
    async def btn_new(self, inter: discord.Interaction, _):
        # NEWBIE-Rolle sofort vergeben
        gcfg = cfg.get(str(self.guild.id)) or {}
        newbie_role_id = int(gcfg.get("newbie_role", 0) or 0)
        r_new = self.guild.get_role(newbie_role_id) if newbie_role_id else None
        if r_new:
            try: await inter.user.add_roles(r_new, reason="Onboarding: Unerfahren")
            except Exception: pass

        await inter.response.edit_message(content="✅ Gespeichert: **Unerfahren**.\nDie Gildenleitung prüft kurz.", view=None)
        await self._send_to_admins(inter.user, "Unerfahren")

class AcceptView(View):
    """Review durch Gildenleitung (Server-Channel). Vergibt Rollen per ID, postet ggf. Welcome."""
    def __init__(self, user_id: int, category: str, experience: str):
        super().__init__(timeout=3600)
        self.user_id = user_id
        self.category = category
        self.experience = experience

    def _role_for_category(self, guild: discord.Guild) -> discord.Role | None:
        gcfg = cfg.get(str(guild.id)) or {}
        key = "guild_role" if self.category == "Gildenmitglied" else ("alliance_role" if self.category == "Allianzmitglied" else "friend_role")
        rid = int(gcfg.get(key, 0) or 0)
        return guild.get_role(rid) if rid else None

    @button(label="✅ Akzeptieren", style=ButtonStyle.success)
    async def accept(self, inter: discord.Interaction, _):
        if not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return

        member = inter.guild.get_member(self.user_id) or await inter.guild.fetch_member(self.user_id)
        base_role = self._role_for_category(inter.guild)
        if base_role:
            try: await member.add_roles(base_role, reason="Onboarding akzeptiert")
            except Exception: pass

        # Welcome-Post (optional)
        gcfg = cfg.get(str(inter.guild.id)) or {}
        welcome_id = int(gcfg.get("welcome_ch", 0) or 0)
        wch = inter.guild.get_channel(welcome_id) if welcome_id else None
        if isinstance(wch, discord.TextChannel):
            try:
                exp = self.experience if self.experience and self.experience != "N/A" else "—"
                await wch.send(f"🔥 Willkommen {member.mention}!\nKategorie: **{self.category}**, Erfahrung: **{exp}**.")
            except Exception:
                pass

        await inter.response.send_message(f"✅ {member.mention} akzeptiert. Rollen vergeben.", ephemeral=True)

    @button(label="❌ Ablehnen", style=ButtonStyle.danger)
    async def deny(self, inter: discord.Interaction, _):
        if not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return

        member = inter.guild.get_member(self.user_id) or await inter.guild.fetch_member(self.user_id)
        await inter.response.send_message(f"🛑 {member.mention} abgelehnt.", ephemeral=True)
        try:
            await member.send("🛑 Deine Onboarding-Anfrage wurde abgelehnt. Bitte melde dich bei der Gildenleitung.")
        except Exception:
            pass

# ---------------------- Öffentliche Funktionen ----------------------

async def send_onboarding_dm(member: discord.Member):
    """Wird bei on_member_join(member) aufgerufen."""
    try:
        if member.bot:
            return
        text = ("👋 **Willkommen!**\n"
                "Bitte wähle zuerst eine **Kategorie**. Danach (bei Gilde/Allianz) deine **Erfahrung**.")
        await member.send(text, view=RoleSelectView(member.guild))
    except Exception:
        # DMs evtl. geschlossen – dann schweigen
        pass

# --------------------------- Slash-Setup ----------------------------

async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree):
    """Slash-Commands zur Konfiguration"""

    def _is_admin(inter: discord.Interaction) -> bool:
        p = getattr(inter.user, "guild_permissions", None)
        return bool(p and (p.administrator or p.manage_guild))

    @tree.command(name="onboarding_set_channel", description="(Admin) Review-Kanal (Gildenleitung) setzen")
    async def onboarding_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["accept_ch"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Review-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onboarding_set_welcome", description="(Admin) Welcome-Kanal setzen (optional)")
    async def onboarding_set_welcome(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["welcome_ch"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Welcome-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onboarding_set_newbie", description="(Admin) NEWBIE-Rolle (für Unerfahren) setzen")
    async def onboarding_set_newbie(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["newbie_role"] = int(role.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ NEWBIE-Rolle gesetzt: {role.mention}", ephemeral=True)

    @tree.command(name="onboarding_set_roles", description="(Admin) Basisrollen setzen (Gildenmitglied/Allianz/Freund)")
    async def onboarding_set_roles(
        inter: discord.Interaction,
        guild_role: discord.Role,
        alliance_role: discord.Role,
        friend_role: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["guild_role"] = int(guild_role.id)
        c["alliance_role"] = int(alliance_role.id)
        c["friend_role"] = int(friend_role.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(
            f"✅ Rollen gespeichert:\n"
            f"• Gildenmitglied: {guild_role.mention}\n"
            f"• Allianzmitglied: {alliance_role.mention}\n"
            f"• Freund: {friend_role.mention}",
            ephemeral=True
        )
