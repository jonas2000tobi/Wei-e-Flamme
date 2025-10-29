# /bot/onboarding.py
# Onboarding-DM mit Rollenwahl & Staff-Review
from __future__ import annotations
import json
from pathlib import Path

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"

def _load_cfg() -> dict:
    try:
        return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cfg(obj: dict) -> None:
    CFG_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

cfg: dict = _load_cfg()

def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

def _get_accept_channel(guild: discord.Guild) -> discord.TextChannel | None:
    gcfg = cfg.get(str(guild.id)) or {}
    ch_id = int(gcfg.get("accept_ch", 0) or 0)
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, discord.TextChannel) else None

def _get_newbie_role(guild: discord.Guild) -> discord.Role | None:
    gcfg = cfg.get(str(guild.id)) or {}
    rid = int(gcfg.get("newbie_role", 0) or 0)
    r = guild.get_role(rid)
    return r if isinstance(r, discord.Role) else None

class RoleSelectView(View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=None)
        self.guild = guild

    async def _send_to_staff(self, member: discord.Member, category: str, experience: str) -> None:
        ch = _get_accept_channel(member.guild)
        if not ch:
            try:
                await member.send("⚠️ Aktuell kann deine Anfrage nicht geprüft werden (kein Prüfkanal gesetzt).")
            except Exception:
                pass
            return
        desc = (
            f"**Onboarding-Review:** {member.mention}\n"
            f"**Kategorie:** {category}\n"
            f"**Erfahrung:** {experience}"
        )
        await ch.send(desc, view=AcceptView(member.id, category, experience))

    async def _go_experience(self, inter: discord.Interaction, category: str) -> None:
        await inter.response.edit_message(
            content=f"Du hast **{category}** gewählt.\nBist du **erfahren** oder **unerfahren**?",
            view=ExperienceView(self.guild, category)
        )

    @button(label="⚔️ Gildenmitglied", style=ButtonStyle.primary)
    async def btn_gildenmitglied(self, inter: discord.Interaction, _):
        await self._go_experience(inter, "Gildenmitglied")

    @button(label="🏰 Allianzmitglied", style=ButtonStyle.secondary)
    async def btn_allianzmitglied(self, inter: discord.Interaction, _):
        await self._go_experience(inter, "Allianzmitglied")

    @button(label="🫱 Freund", style=ButtonStyle.success)
    async def btn_friend(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            content="Du hast **Freund** gewählt. Deine Anfrage wurde an die Gildenleitung gesendet.",
            view=None
        )
        await self._send_to_staff(inter.user, "Freund", "N/A")

class ExperienceView(View):
    def __init__(self, guild: discord.Guild, category: str):
        super().__init__(timeout=None)
        self.guild = guild
        self.category = category

    async def _finish(self, inter: discord.Interaction, experience: str) -> None:
        if experience == "Unerfahren":
            nb = _get_newbie_role(self.guild)
            if nb:
                try:
                    await inter.user.add_roles(nb, reason="Onboarding: Unerfahren")
                except Exception:
                    pass
        ch = _get_accept_channel(self.guild)
        if ch:
            await ch.send(
                f"**Onboarding-Review:** {inter.user.mention}\n"
                f"**Kategorie:** {self.category}\n"
                f"**Erfahrung:** {experience}",
                view=AcceptView(inter.user.id, self.category, experience)
            )
        await inter.response.edit_message(
            content="✅ Danke! Deine Angaben wurden an die Gildenleitung gesendet.",
            view=None
        )

    @button(label="🧠 Erfahren", style=ButtonStyle.primary)
    async def btn_exp(self, inter: discord.Interaction, _):
        await self._finish(inter, "Erfahren")

    @button(label="🌱 Unerfahren", style=ButtonStyle.secondary)
    async def btn_new(self, inter: discord.Interaction, _):
        await self._finish(inter, "Unerfahren")

class AcceptView(View):
    def __init__(self, user_id: int, category: str, experience: str):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.category = category
        self.experience = experience

    def _target_role(self, guild: discord.Guild) -> discord.Role | None:
        return discord.utils.get(guild.roles, name=self.category)

    async def _get_member(self, guild: discord.Guild) -> discord.Member | None:
        m = guild.get_member(self.user_id)
        if m is None:
            try:
                m = await guild.fetch_member(self.user_id)
            except Exception:
                m = None
        return m

    @button(label="✅ Akzeptieren", style=ButtonStyle.success)
    async def btn_accept(self, inter: discord.Interaction, _):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins können das.", ephemeral=True); return
        member = await self._get_member(inter.guild)
        if not member:
            await inter.response.send_message("Mitglied nicht gefunden.", ephemeral=True); return
        role = self._target_role(inter.guild)
        if role:
            try:
                await member.add_roles(role, reason="Onboarding akzeptiert")
            except Exception:
                pass
        try:
            await member.send(f"🎉 Willkommen! Du wurdest als **{self.category}** akzeptiert.")
        except Exception:
            pass
        await inter.response.send_message(f"✅ {member.mention} akzeptiert.", ephemeral=True)

    @button(label="❌ Ablehnen", style=ButtonStyle.danger)
    async def btn_deny(self, inter: discord.Interaction, _):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        member = await self._get_member(inter.guild)
        if member:
            try:
                await member.send("❌ Deine Anfrage wurde abgelehnt.")
            except Exception:
                pass
        await inter.response.send_message("Abgelehnt.", ephemeral=True)

async def send_onboarding_dm(member: discord.Member) -> None:
    try:
        if member.bot:
            return
        text = (
            "👋 **Willkommen!**\n"
            "Wähle bitte eine **Kategorie**. Danach (bei Gilde/Allianz) frage ich deine **Erfahrung**."
        )
        await member.send(text, view=RoleSelectView(member.guild))
    except Exception:
        pass

async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree) -> None:
    @tree.command(name="onboarding_set_channel", description="(Admin) Zielkanal für Onboarding-Anfragen setzen")
    async def onboarding_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["accept_ch"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ Prüfkanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onboarding_set_newbie", description="(Admin) NEWBIE-Unterrolle für 'Unerfahren' setzen")
    async def onboarding_set_newbie(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["newbie_role"] = int(role.id)
        cfg[str(inter.guild_id)] = c; _save_cfg(cfg)
        await inter.response.send_message(f"✅ NEWBIE-Rolle gesetzt: {role.mention}", ephemeral=True)

    @tree.command(name="onboarding_test", description="(Admin) Schickt dir die Onboarding-DM zum Test")
    async def onboarding_test(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("Nur Admins.", ephemeral=True); return
        try:
            await inter.user.send("Test: Onboarding-DM", view=RoleSelectView(inter.guild))
            await inter.response.send_message("✉️ DM gesendet (prüfe Postfach).", ephemeral=True)
        except Exception:
            await inter.response.send_message("Konnte keine DM senden (ggf. DMs geschlossen).", ephemeral=True)
