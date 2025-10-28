# bot/onboarding_dm.py
# -----------------------------------------------------------
# DM-basiertes Onboarding-System mit automatischer Rollenvergabe
# -----------------------------------------------------------

import json
from pathlib import Path
from typing import Optional
import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "onboarding_cfg.json"  # {"guild_id": {"welcome_ch": id}}
def _load(): 
    try: return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception: return {}
def _save(obj): CFG_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
cfg = _load()

# -----------------------------------------------------------
#   Views
# -----------------------------------------------------------

class ExperienceSelect(View):
    def __init__(self, guild_id:int, role_choice:str, member:discord.Member):
        super().__init__(timeout=120)
        self.guild_id=guild_id; self.role_choice=role_choice; self.member=member

    @button(label="Erfahren", style=ButtonStyle.success)
    async def exp(self, inter:discord.Interaction, _):
        await self._assign(inter, experienced=True)

    @button(label="Unerfahren (Newbie)", style=ButtonStyle.secondary)
    async def newb(self, inter:discord.Interaction, _):
        await self._assign(inter, experienced=False)

    async def _assign(self, inter, experienced:bool):
        g=self.member.guild
        roles=[r for r in g.roles if not r.is_default()]
        target=None; newbie=None

        if self.role_choice=="Gilde":
            target=discord.utils.get(roles, name="Weiße Flamme")
        elif self.role_choice=="Allianz":
            target=discord.utils.get(roles, name="Allianz")

        newbie=discord.utils.get(roles, name="NEWBIE")

        if target:
            await self.member.add_roles(target, reason="Onboarding Auswahl")
        if not experienced and newbie:
            await self.member.add_roles(newbie, reason="Onboarding Unerfahren")

        await inter.response.send_message(f"✅ Willkommen {self.member.mention}! Rollen gesetzt.", ephemeral=True)
        await self._announce(g, target, experienced)

    async def _announce(self, g:discord.Guild, target:Optional[discord.Role], experienced:bool):
        ch_id = cfg.get(str(g.id), {}).get("welcome_ch", 0)
        ch = g.get_channel(int(ch_id)) if ch_id else None
        if not ch: return
        status="Erfahren" if experienced else "Unerfahren"
        msg=f"🎉 Willkommen {self.member.mention} als **{status}es Mitglied** der {target.mention if target else 'Gruppe'}!"
        try: await ch.send(msg)
        except Exception: pass


class RoleSelect(View):
    def __init__(self, guild_id:int, member:discord.Member):
        super().__init__(timeout=180)
        self.guild_id=guild_id; self.member=member

    @button(label="Gildenmitglied", style=ButtonStyle.primary)
    async def gilde(self, inter:discord.Interaction, _):
        await inter.response.send_message("Bist du erfahren oder unerfahren?", view=ExperienceSelect(self.guild_id,"Gilde",self.member), ephemeral=True)

    @button(label="Allianzmitglied", style=ButtonStyle.secondary)
    async def allianz(self, inter:discord.Interaction, _):
        await inter.response.send_message("Bist du erfahren oder unerfahren?", view=ExperienceSelect(self.guild_id,"Allianz",self.member), ephemeral=True)

    @button(label="Freund", style=ButtonStyle.success)
    async def freund(self, inter:discord.Interaction, _):
        g=self.member.guild
        roles=[r for r in g.roles if not r.is_default()]
        r=discord.utils.get(roles, name="Freund")
        if r: await self.member.add_roles(r, reason="Onboarding Freund")
        await inter.response.send_message(f"✅ Willkommen {self.member.mention}! Rolle **Freund** wurde hinzugefügt.", ephemeral=True)


# -----------------------------------------------------------
#   Join-Handler
# -----------------------------------------------------------

async def send_onboarding_dm(member:discord.Member):
    """Schickt DM bei Beitritt"""
    if member.bot: return
    try:
        text=("Willkommen auf dem Server!\n\n"
              "Bitte wähle unten, was du bist:")
        await member.send(text, view=RoleSelect(member.guild.id, member))
    except Exception:
        pass


# -----------------------------------------------------------
#   Slash-Command zum Setzen des Begrüßungskanals
# -----------------------------------------------------------

async def setup_onboarding(tree:app_commands.CommandTree):
    @tree.command(name="set_welcome_channel", description="(Admin) Kanal für Begrüßungen setzen")
    async def _cmd(inter:discord.Interaction, channel:discord.TextChannel):
        if not inter.user.guild_permissions.administrator:
            return await inter.response.send_message("Nur Admin.", ephemeral=True)
        g=str(inter.guild_id)
        c=cfg.get(g,{})
        c["welcome_ch"]=channel.id
        cfg[g]=c; _save(cfg)
        await inter.response.send_message(f"✅ Begrüßungskanal gesetzt: {channel.mention}", ephemeral=True)
