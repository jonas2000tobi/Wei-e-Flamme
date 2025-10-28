# bot/onboarding.py
from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

import discord
from discord import app_commands

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

ONB_CFG_FILE  = DATA_DIR / "onboarding_cfg.json"   # pro Guild: staff_channel_id, role ids
ONB_STATE_FILE= DATA_DIR / "onboarding_state.json" # pending states (pro user)

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

cfg: Dict[str, dict] = _load(ONB_CFG_FILE, {})
state: Dict[str, dict] = _load(ONB_STATE_FILE, {})

def save_cfg(): _save(ONB_CFG_FILE, cfg)
def save_state(): _save(ONB_STATE_FILE, state)

# cfg[str(guild_id)] = {
#   "staff_channel_id": 0,
#   "role_guild": 0,       # WeißeFlamme
#   "role_alliance": 0,
#   "role_friend": 0,
#   "role_newbie": 0       # optional
# }

def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

# -------- DM-View für User ----------
class OnbView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.choice: Optional[str] = None
        self.rules_ok: bool = False

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        return inter.user.id == self.user_id

    @discord.ui.button(label="Gildenmitglied (WeißeFlamme)", style=discord.ButtonStyle.primary, emoji="🏰")
    async def btn_guild(self, inter: discord.Interaction, _btn: discord.ui.Button):
        self.choice = "guild"
        await inter.response.send_message("Ausgewählt: **Gildenmitglied (WeißeFlamme)**", ephemeral=True)

    @discord.ui.button(label="Allianzmitglied", style=discord.ButtonStyle.secondary, emoji="🤝")
    async def btn_alliance(self, inter: discord.Interaction, _btn: discord.ui.Button):
        self.choice = "alliance"
        await inter.response.send_message("Ausgewählt: **Allianzmitglied**", ephemeral=True)

    @discord.ui.button(label="Freund", style=discord.ButtonStyle.secondary, emoji="🧑‍🤝‍🧑")
    async def btn_friend(self, inter: discord.Interaction, _btn: discord.ui.Button):
        self.choice = "friend"
        await inter.response.send_message("Ausgewählt: **Freund**", ephemeral=True)

    @discord.ui.button(label="Regeln akzeptieren", style=discord.ButtonStyle.success, emoji="📜")
    async def btn_rules(self, inter: discord.Interaction, _btn: discord.ui.Button):
        self.rules_ok = True
        await inter.response.send_message("✅ Regeln akzeptiert.", ephemeral=True)

    @discord.ui.button(label="Senden an Gildenleitung", style=discord.ButtonStyle.primary, emoji="📨")
    async def btn_submit(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not (self.choice and self.rules_ok):
            await inter.response.send_message("Bitte Kategorie wählen **und** Regeln akzeptieren.", ephemeral=True); return
        # Store pending
        key = f"{self.guild_id}:{self.user_id}"
        state[key] = {"choice": self.choice, "rules_ok": True}
        save_state()

        # Post to staff channel
        guild = inter.client.get_guild(self.guild_id)
        gcfg = cfg.get(str(self.guild_id)) or {}
        staff_id = int(gcfg.get("staff_channel_id", 0))
        ch = guild.get_channel(staff_id) if staff_id else None
        if not isinstance(ch, discord.TextChannel):
            await inter.response.send_message("❌ Kein Staff-/Gildenleitungskanal konfiguriert.", ephemeral=True); return

        user = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        desc = [
            f"👤 **User:** {user.mention} (`{user.id}`)",
            f"🧭 **Kategorie:** { 'WeißeFlamme' if self.choice=='guild' else ('Allianz' if self.choice=='alliance' else 'Freund') }",
            f"📜 **Regeln akzeptiert:** Ja",
            "",
            "Bitte **Akzeptieren** oder **Ablehnen**."
        ]
        view = StaffReviewView(self.guild_id, self.user_id)
        try:
            await ch.send(
                content=f"**Onboarding-Review:** {user.display_name}",
                embed=discord.Embed(description="\n".join(desc), color=discord.Color.orange()),
                view=view
            )
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Gildenleitungs-Post nicht senden: {e}", ephemeral=True); return

        await inter.response.send_message("Danke! Gildenleitung wurde informiert.", ephemeral=True)
        self.stop()

# -------- Staff-Review View ----------
class StaffReviewView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=3600)
        self.guild_id = guild_id
        self.user_id = user_id

    def _allowed(self, inter: discord.Interaction) -> bool:
        return _is_admin(inter)

    async def _close(self, inter: discord.Interaction, label: str):
        for c in self.children:
            if isinstance(c, discord.ui.Button): c.disabled = True
        try:
            await inter.message.edit(content=f"{inter.message.content}\n**Status:** {label}", view=self)
        except Exception:
            pass

    @discord.ui.button(label="Akzeptieren", style=discord.ButtonStyle.success, emoji="✅")
    async def approve(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return
        key = f"{self.guild_id}:{self.user_id}"
        pending = state.get(key)
        if not pending:
            await inter.response.send_message("❌ Kein offener Antrag.", ephemeral=True); return

        guild = inter.guild
        member = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        gcfg = cfg.get(str(self.guild_id)) or {}
        role_ids = {
            "guild": int(gcfg.get("role_guild", 0)),
            "alliance": int(gcfg.get("role_alliance", 0)),
            "friend": int(gcfg.get("role_friend", 0)),
            "newbie": int(gcfg.get("role_newbie", 0) or 0)
        }
        want = pending["choice"]  # 'guild' | 'alliance' | 'friend'
        to_add = []
        if want == "guild" and role_ids["guild"]:
            r = guild.get_role(role_ids["guild"])
            if r: to_add.append(r)
        if want == "alliance" and role_ids["alliance"]:
            r = guild.get_role(role_ids["alliance"])
            if r: to_add.append(r)
        if want == "friend" and role_ids["friend"]:
            r = guild.get_role(role_ids["friend"])
            if r: to_add.append(r)
        if role_ids["newbie"]:
            r = guild.get_role(role_ids["newbie"])
            if r: to_add.append(r)

        try:
            if to_add:
                await member.add_roles(*to_add, reason="Onboarding akzeptiert")
        except Exception as e:
            await inter.response.send_message(f"❌ Rollenvergabe fehlgeschlagen: {e}", ephemeral=True); return

        # Clean state
        state.pop(key, None); save_state()

        try:
            await member.send("✅ Du wurdest freigeschaltet. Willkommen!")
        except Exception:
            pass

        await inter.response.send_message("✅ Angenommen. Rollen vergeben.", ephemeral=True)
        await self._close(inter, "Angenommen")

    @discord.ui.button(label="Ablehnen", style=discord.ButtonStyle.danger, emoji="❌")
    async def reject(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return
        key = f"{self.guild_id}:{self.user_id}"
        state.pop(key, None); save_state()
        try:
            mem = inter.guild.get_member(self.user_id) or await inter.guild.fetch_member(self.user_id)
            await mem.send("❌ Dein Onboarding-Antrag wurde abgelehnt. Wende dich an die Gildenleitung.")
        except Exception:
            pass
        await inter.response.send_message("🛑 Abgelehnt.", ephemeral=True)
        await self._close(inter, "Abgelehnt")

# -------- Public API ----------
async def handle_member_join_onboarding(member: discord.Member):
    """Sendet Onboarding-DM an neuen Member."""
    gcfg = cfg.get(str(member.guild.id)) or {}
    # Wenn nichts konfiguriert -> nicht nerven
    if not (gcfg.get("staff_channel_id") and (gcfg.get("role_guild") or gcfg.get("role_alliance") or gcfg.get("role_friend"))):
        return
    await _send_onboarding_dm(member)

async def handle_member_update_onboarding(before: discord.Member, after: discord.Member):
    # Hier könntest du künftig reagieren, wenn sofort Rollen da sind – aktuell nicht zwingend nötig.
    return

async def _send_onboarding_dm(member: discord.Member):
    try:
        emb = discord.Embed(
            title="Willkommen!",
            description=(
                "Wähle bitte, was du bist, und akzeptiere die Regeln. "
                "Danach prüft die Gildenleitung und schaltet dich frei."
            ),
            color=discord.Color.orange()
        )
        view = OnbView(member.guild.id, member.id)
        await member.send(embed=emb, view=view)
    except discord.Forbidden:
        # DMs geschlossen – wir nerven nicht weiter
        pass
    except Exception:
        pass

# -------- Commands / Setup ----------
async def setup_onboarding(client: discord.Client, tree: app_commands.CommandTree):

    @tree.command(name="onb_set_staff_channel", description="(Admin) Staff-/Gildenleitungskanal für Onboarding setzen")
    @app_commands.describe(channel="Kanal, in dem Reviews gepostet werden")
    async def onb_set_staff_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        gcfg = cfg.get(str(inter.guild_id)) or {}
        gcfg["staff_channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = gcfg; save_cfg()
        await inter.response.send_message(f"✅ Staff-/Gildenleitungskanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onb_set_roles", description="(Admin) Onboarding-Rollen setzen")
    @app_commands.describe(
        guild_role="Rolle für Gildenmitglieder (z. B. WeißeFlamme)",
        alliance_role="Rolle für Allianz-Mitglieder",
        friend_role="Rolle für Freunde",
        newbie_role="Optionale Newbie-/Probe-Rolle"
    )
    async def onb_set_roles(
        inter: discord.Interaction,
        guild_role: discord.Role,
        alliance_role: discord.Role,
        friend_role: discord.Role,
        newbie_role: Optional[discord.Role] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        gcfg = cfg.get(str(inter.guild_id)) or {}
        gcfg["role_guild"] = int(guild_role.id)
        gcfg["role_alliance"] = int(alliance_role.id)
        gcfg["role_friend"] = int(friend_role.id)
        gcfg["role_newbie"] = int(newbie_role.id) if newbie_role else 0
        cfg[str(inter.guild_id)] = gcfg; save_cfg()
        await inter.response.send_message(
            "✅ Rollen gespeichert:\n"
            f"• Gilde: {guild_role.mention}\n"
            f"• Allianz: {alliance_role.mention}\n"
            f"• Freund: {friend_role.mention}\n"
            f"• Newbie: {(newbie_role.mention if newbie_role else '—')}",
            ephemeral=True
        )

    @tree.command(name="onb_test_dm", description="(Admin) Eigene Onboarding-DM testen")
    async def onb_test_dm(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        await _send_onboarding_dm(inter.user)
        await inter.response.send_message("✅ Onboarding-DM versendet (falls DMs offen).", ephemeral=True)
