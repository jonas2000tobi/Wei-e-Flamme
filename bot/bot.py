# bot/onboarding_dm.py
# -----------------------------------------------------------
# Automatische Willkommens-DM mit Rollenwahl und Weiterleitung
# -----------------------------------------------------------

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from pathlib import Path
import json

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

cfg = _load_cfg()

# cfg[guild_id] = {"log_ch": id, "newbie_role": id, "accept_ch": id}

# -----------------------------------------------------------
# DM Views
# -----------------------------------------------------------

class RoleSelectView(View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=None)
        self.guild = guild

    async def _send_to_admins(self, member: discord.Member, category: str, experience: str):
        gcfg = cfg.get(str(member.guild.id)) or {}
        log_id = int(gcfg.get("accept_ch", 0) or 0)
        ch = member.guild.get_channel(log_id)
        if not ch:
            return
        txt = (f"**Neue Anfrage:** {member.mention}\n"
               f"Rolle: {category}\n"
               f"Erfahrung: {experience}")
        await ch.send(txt, view=AcceptView(member, category, experience))

    async def handle_role(self, inter: discord.Interaction, category: str):
        if category in ["Gildenmitglied", "Allianzmitglied"]:
            view = ExperienceView(self.guild, category)
            await inter.response.edit_message(content=f"🧙 Du hast **{category}** gewählt. Bist du erfahren oder unerfahren?", view=view)
        else:
            await inter.response.edit_message(content=f"🧙 Du hast **{category}** gewählt.", view=None)
            await self._send_to_admins(inter.user, category, "N/A")

    @button(label="⚔️ Gildenmitglied", style=ButtonStyle.primary)
    async def btn_gilde(self, inter: discord.Interaction, _):
        await self.handle_role(inter, "Gildenmitglied")

    @button(label="🏰 Allianzmitglied", style=ButtonStyle.secondary)
    async def btn_allianz(self, inter: discord.Interaction, _):
        await self.handle_role(inter, "Allianzmitglied")

    @button(label="🫱 Freund", style=ButtonStyle.success)
    async def btn_friend(self, inter: discord.Interaction, _):
        await self.handle_role(inter, "Freund")

class ExperienceView(View):
    def __init__(self, guild: discord.Guild, category: str):
        super().__init__(timeout=None)
        self.guild = guild
        self.category = category

    async def _send_to_admins(self, member: discord.Member, experience: str):
        gcfg = cfg.get(str(member.guild.id)) or {}
        log_id = int(gcfg.get("accept_ch", 0) or 0)
        ch = member.guild.get_channel(log_id)
        if not ch:
            return
        txt = (f"**Neue Anfrage:** {member.mention}\n"
               f"Rolle: {self.category}\n"
               f"Erfahrung: {experience}")
        await ch.send(txt, view=AcceptView(member, self.category, experience))

    @button(label="🧠 Erfahren", style=ButtonStyle.primary)
    async def btn_exp(self, inter: discord.Interaction, _):
        await inter.response.edit_message(content="✅ Gespeichert: Erfahren.", view=None)
        await self._send_to_admins(inter.user, "Erfahren")

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary)
    async def btn_new(self, inter: discord.Interaction, _):
        gcfg = cfg.get(str(self.guild.id)) or {}
        newbie_role_id = int(gcfg.get("newbie_role", 0) or 0)
        role = self.guild.get_role(newbie_role_id)
        if role:
            try:
                await inter.user.add_roles(role)
            except Exception:
                pass
        await inter.response.edit_message(content="✅ Gespeichert: Unerfahren.", view=None)
        await self._send_to_admins(inter.user, "Unerfahren")

class AcceptView(View):
    def __init__(self, member: discord.Member, category: str, experience: str):
        super().__init__(timeout=None)
        self.member = member
        self.category = category
        self.experience = experience

    @button(label="✅ Akzeptieren", style=ButtonStyle.success)
    async def accept(self, inter: discord.Interaction, _):
        role_name = self.category
        role = discord.utils.get(inter.guild.roles, name=role_name)
        if role:
            await self.member.add_roles(role)
        await inter.response.send_message(f"✅ {self.member.mention} akzeptiert.", ephemeral=True)
        try:
            await self.member.send(f"Willkommen in der {role_name}!")
        except Exception:
            pass

    @button(label="❌ Ablehnen", style=ButtonStyle.danger)
    async def deny(self, inter: discord.Interaction, _):
        await inter.response.send_message(f"❌ {self.member.mention} wurde abgelehnt.", ephemeral=True)
        try:
            await self.member.send("Deine Anfrage wurde abgelehnt.")
        except Exception:
            pass

# -----------------------------------------------------------
# Onboarding-DM an neue Mitglieder
# -----------------------------------------------------------

async def send_onboarding_dm(member: discord.Member):
    try:
        if member.bot:
            return
        text = ("👋 Willkommen auf dem Server!\n"
                "Bitte wähle, was du bist:")
        await member.send(text, view=RoleSelectView(member.guild))
    except Exception:
        pass

# -----------------------------------------------------------
# Slash-Command Setup
# -----------------------------------------------------------

async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree):
    """Slash Commands für DM-Onboarding"""

    @tree.command(name="onboarding_set_channel", description="(Admin) Zielkanal für Anfragen setzen")
    async def onboarding_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not inter.user.guild_permissions.administrator:
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return
        c = cfg.get(str(inter.guild_id)) or {}
        c["accept_ch"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Channel gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onboarding_set_newbie", description="(Admin) Rolle für Newbies setzen")
    async def onboarding_set_newbie(inter: discord.Interaction, role: discord.Role):
        if not inter.user.guild_permissions.administrator:
            await inter.response.send_message("Nur Admins.", ephemeral=True)
            return
        c = cfg.get(str(inter.guild_id)) or {}
        c["newbie_role"] = int(role.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Newbie-Rolle gesetzt: {role.mention}", ephemeral=True)
