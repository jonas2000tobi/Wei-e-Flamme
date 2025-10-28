# bot/onboarding.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Dict

import discord
from discord import app_commands

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

ONB_CFG_FILE   = DATA_DIR / "onboarding_cfg.json"   # pro Guild: Kanäle + Rollen
ONB_STATE_FILE = DATA_DIR / "onboarding_state.json" # pro User: Zwischenschritte

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

cfg:   Dict[str, dict] = _load(ONB_CFG_FILE, {})
state: Dict[str, dict] = _load(ONB_STATE_FILE, {})

def save_cfg():   _save(ONB_CFG_FILE, cfg)
def save_state(): _save(ONB_STATE_FILE, state)

# cfg[str(guild_id)] = {
#   "staff_channel_id": 0,
#   "welcome_channel_id": 0,
#   "role_guild": 0,        # WeißeFlamme
#   "role_alliance": 0,
#   "role_friend": 0,
#   "role_newbie": 0,
#   "role_tank": 0,
#   "role_heal": 0,
#   "role_dps":  0,
# }

def _is_admin(inter: discord.Interaction) -> bool:
    gp = getattr(inter.user, "guild_permissions", None)
    return bool(gp and (gp.administrator or gp.manage_guild))

# ============================ DM VIEWS ============================

async def _start_flow(member: discord.Member):
    """Schritt 1: Kategorie wählen (Gilde/Allianz/Freund)."""
    try:
        emb = discord.Embed(
            title="Willkommen!",
            description=("Wähle bitte **eine Kategorie**. Danach frage ich dich **deine Rolle** "
                         "und (bei Gilde/Allianz) ob du **erfahren** oder **unerfahren** bist."),
            color=discord.Color.orange()
        )
        await member.send(embed=emb, view=CategoryView(member.guild.id, member.id))
    except discord.Forbidden:
        pass
    except Exception:
        pass

class CategoryView(discord.ui.View):
    """Schritt 1 – Kategorie."""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id  = user_id

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        return inter.user.id == self.user_id

    async def _choose(self, inter: discord.Interaction, choice: str):
        key = f"{self.guild_id}:{self.user_id}"
        state[key] = {"choice": choice}   # choice: guild|alliance|friend
        save_state()
        await inter.response.send_message(f"Ausgewählt: **{'WeißeFlamme' if choice=='guild' else ('Allianz' if choice=='alliance' else 'Freund')}**", ephemeral=True)
        # Schritt 2 anstoßen
        try:
            emb = discord.Embed(
                title="Primärrolle",
                description="Welche Rolle spielst du? (Nur eine auswählen)",
                color=discord.Color.blurple()
            )
            await inter.followup.send(embed=emb, view=PrimaryRoleView(self.guild_id, self.user_id), ephemeral=False)
        except Exception:
            pass
        self.stop()

    @discord.ui.button(label="Gildenmitglied (WeißeFlamme)", emoji="🏰", style=discord.ButtonStyle.primary)
    async def btn_guild(self, inter: discord.Interaction, _):   await self._choose(inter, "guild")

    @discord.ui.button(label="Allianzmitglied", emoji="🤝", style=discord.ButtonStyle.secondary)
    async def btn_alliance(self, inter: discord.Interaction, _): await self._choose(inter, "alliance")

    @discord.ui.button(label="Freund", emoji="🧑‍🤝‍🧑", style=discord.ButtonStyle.secondary)
    async def btn_friend(self, inter: discord.Interaction, _):   await self._choose(inter, "friend")

class PrimaryRoleView(discord.ui.View):
    """Schritt 2 – Primärrolle (Tank/Heal/DPS)."""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id  = user_id

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        return inter.user.id == self.user_id

    async def _set_role(self, inter: discord.Interaction, role_label: str):
        key = f"{self.guild_id}:{self.user_id}"
        st = state.get(key) or {}
        st["primary"] = role_label  # 'TANK'|'HEAL'|'DPS'
        state[key] = st
        save_state()
        await inter.response.send_message(f"Primärrolle: **{role_label}**", ephemeral=True)

        # Bei Gilde/Allianz -> Schritt 3 (Erfahrung). Bei Freund -> direkt Staff-Post.
        choice = st.get("choice")
        if choice in ("guild", "alliance"):
            try:
                emb = discord.Embed(
                    title="Erfahrung",
                    description="Bist du **Erfahren** oder **Unerfahren**?",
                    color=discord.Color.green()
                )
                await inter.followup.send(embed=emb, view=ExperienceView(self.guild_id, self.user_id), ephemeral=False)
            except Exception:
                pass
        else:
            await _submit_to_staff(inter.client, self.guild_id, self.user_id)

        self.stop()

    @discord.ui.button(label="🛡️ Tank", style=discord.ButtonStyle.secondary)
    async def tank(self, inter: discord.Interaction, _):  await self._set_role(inter, "TANK")

    @discord.ui.button(label="💚 Heal", style=discord.ButtonStyle.secondary)
    async def heal(self, inter: discord.Interaction, _):  await self._set_role(inter, "HEAL")

    @discord.ui.button(label="🗡️ DPS",  style=discord.ButtonStyle.secondary)
    async def dps(self, inter: discord.Interaction, _):   await self._set_role(inter, "DPS")

class ExperienceView(discord.ui.View):
    """Schritt 3 – Erfahrung (nur für Gilde/Allianz)."""
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id  = user_id

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        return inter.user.id == self.user_id

    async def _exp(self, inter: discord.Interaction, experienced: bool):
        key = f"{self.guild_id}:{self.user_id}"
        st = state.get(key) or {}
        st["experienced"] = bool(experienced)
        st["rules_ok"] = True  # implizit akzeptiert
        state[key] = st; save_state()
        await _submit_to_staff(inter.client, self.guild_id, self.user_id)
        await inter.response.send_message("Danke! Deine Angaben wurden an die **Gildenleitung** gesendet.", ephemeral=True)
        self.stop()

    @discord.ui.button(label="Erfahren", emoji="🔥", style=discord.ButtonStyle.success)
    async def exp_yes(self, inter: discord.Interaction, _): await self._exp(inter, True)

    @discord.ui.button(label="Unerfahren", emoji="🌱", style=discord.ButtonStyle.secondary)
    async def exp_no(self, inter: discord.Interaction, _):  await self._exp(inter, False)

# ======================= STAFF REVIEW & SUBMIT =======================

async def _submit_to_staff(client: discord.Client, guild_id: int, user_id: int):
    """Erstellt den Review-Post in #gildenleitung mit ✅/❌."""
    key = f"{guild_id}:{user_id}"
    st = state.get(key) or {}
    guild = client.get_guild(guild_id)
    user  = guild.get_member(user_id) or await guild.fetch_member(user_id)

    gcfg = cfg.get(str(guild_id)) or {}
    staff_id = int(gcfg.get("staff_channel_id", 0))
    ch = guild.get_channel(staff_id) if staff_id else None
    if not isinstance(ch, discord.TextChannel):
        try:
            await user.send("❌ Gildenleitungskanal ist nicht konfiguriert. Melde dich bitte bei einem Admin.")
        except Exception:
            pass
        return

    choice = st.get("choice")
    primary = st.get("primary")
    experienced = st.get("experienced", None)

    label_choice = 'WeißeFlamme' if choice=='guild' else ('Allianz' if choice=='alliance' else 'Freund')
    label_exp = ("Erfahren" if experienced else ("Unerfahren" if experienced is not None else "—"))
    label_primary = {"TANK":"Tank","HEAL":"Heal","DPS":"DPS"}.get(primary, "—")

    lines = [
        f"👤 **User:** {user.mention} (`{user.id}`)",
        f"🧭 **Kategorie:** {label_choice}",
        f"🎭 **Primärrolle:** {label_primary}",
        f"📚 **Erfahrung:** {label_exp}",
        f"📜 **Regeln:** Akzeptiert (implizit)",
        "",
        "Bitte **Akzeptieren** oder **Ablehnen**."
    ]
    try:
        await ch.send(
            content=f"**Onboarding-Review:** {user.display_name}",
            embed=discord.Embed(description="\n".join(lines), color=discord.Color.orange()),
            view=StaffReviewView(guild_id, user_id)
        )
        try:
            await user.send("📨 Deine Angaben wurden an die **Gildenleitung** gesendet. Du bekommst gleich Bescheid.")
        except Exception:
            pass
    except Exception as e:
        try:
            await user.send(f"❌ Konnte Gildenleitung nicht informieren: {e}")
        except Exception:
            pass

class StaffReviewView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=3600)
        self.guild_id = guild_id
        self.user_id  = user_id

    def _allowed(self, inter: discord.Interaction) -> bool:
        return _is_admin(inter)

    async def _close(self, inter: discord.Interaction, label: str):
        for c in self.children:
            if isinstance(c, discord.ui.Button):
                c.disabled = True
        try:
            await inter.message.edit(content=f"{inter.message.content}\n**Status:** {label}", view=self)
        except Exception:
            pass

    @discord.ui.button(label="Akzeptieren", style=discord.ButtonStyle.success, emoji="✅")
    async def approve(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return

        key = f"{self.guild_id}:{self.user_id}"
        st = state.get(key)
        if not st:
            await inter.response.send_message("❌ Kein offener Antrag.", ephemeral=True); return

        guild = inter.guild
        member = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        gcfg = cfg.get(str(self.guild_id)) or {}
        # Zielrollen
        role_ids = {
            "guild":   int(gcfg.get("role_guild", 0)),
            "alliance":int(gcfg.get("role_alliance", 0)),
            "friend":  int(gcfg.get("role_friend", 0)),
            "newbie":  int(gcfg.get("role_newbie", 0) or 0),
            "tank":    int(gcfg.get("role_tank", 0) or 0),
            "heal":    int(gcfg.get("role_heal", 0) or 0),
            "dps":     int(gcfg.get("role_dps", 0)  or 0),
        }

        to_add = []
        # Kategorie
        if st.get("choice") == "guild" and role_ids["guild"]:
            r = guild.get_role(role_ids["guild"]);  to_add += [r] if r else []
        if st.get("choice") == "alliance" and role_ids["alliance"]:
            r = guild.get_role(role_ids["alliance"]); to_add += [r] if r else []
        if st.get("choice") == "friend" and role_ids["friend"]:
            r = guild.get_role(role_ids["friend"]);   to_add += [r] if r else []
        # Primärrolle
        primary = (st.get("primary") or "").upper()
        if primary == "TANK" and role_ids["tank"]:
            r = guild.get_role(role_ids["tank"]); to_add += [r] if r else []
        if primary == "HEAL" and role_ids["heal"]:
            r = guild.get_role(role_ids["heal"]); to_add += [r] if r else []
        if primary == "DPS"  and role_ids["dps"]:
            r = guild.get_role(role_ids["dps"]);  to_add += [r] if r else []
        # Newbie bei unerfahren
        if st.get("experienced") is False and role_ids["newbie"]:
            r = guild.get_role(role_ids["newbie"]); to_add += [r] if r else []

        try:
            if to_add:
                await member.add_roles(*to_add, reason="Onboarding akzeptiert")
        except Exception as e:
            await inter.response.send_message(f"❌ Rollenvergabe fehlgeschlagen: {e}", ephemeral=True); return

        # Welcome-Post (nur bei Gilde)
        if st.get("choice") == "guild":
            wcid = int(gcfg.get("welcome_channel_id", 0) or 0)
            ch = guild.get_channel(wcid) if wcid else None
            if isinstance(ch, discord.TextChannel):
                label_primary = {"TANK":"Tank","HEAL":"Heal","DPS":"DPS"}.get(primary, "—")
                label_exp = "Erfahren" if st.get("experienced") else "Unerfahren"
                try:
                    await ch.send(
                        f"🔥 Willkommen {member.mention} in **Weiße Flamme**!\n"
                        f"• Rolle: **{label_primary}**\n"
                        f"• Erfahrung: **{label_exp}**"
                    )
                except Exception:
                    pass

        # DM an User
        try:
            await member.send("✅ Du wurdest freigeschaltet. Viel Spaß!")
        except Exception:
            pass

        state.pop(key, None); save_state()
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
            await mem.send("❌ Dein Onboarding-Antrag wurde abgelehnt. Wende dich bitte an die Gildenleitung.")
        except Exception:
            pass
        await inter.response.send_message("🛑 Abgelehnt.", ephemeral=True)
        await self._close(inter, "Abgelehnt")

# ============================ PUBLIC API ============================

async def handle_member_join_onboarding(member: discord.Member):
    """DM-Flow starten, sobald jemand dem Server beitritt."""
    gcfg = cfg.get(str(member.guild.id)) or {}
    # Nur wenn Staffkanal + min. eine Kategorie-Rolle konfiguriert ist
    if not (gcfg.get("staff_channel_id") and (gcfg.get("role_guild") or gcfg.get("role_alliance") or gcfg.get("role_friend"))):
        return
    await _start_flow(member)

async def handle_member_update_onboarding(before: discord.Member, after: discord.Member):
    return  # aktuell nichts

# ============================ COMMANDS / SETUP ============================

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

    @tree.command(name="onb_set_welcome_channel", description="(Admin) Willkommenskanal setzen (für Gilden-Begrüßung)")
    @app_commands.describe(channel="Kanal für öffentliche Begrüßung neuer Gildenmitglieder")
    async def onb_set_welcome_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        gcfg = cfg.get(str(inter.guild_id)) or {}
        gcfg["welcome_channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = gcfg; save_cfg()
        await inter.response.send_message(f"✅ Willkommenskanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="onb_set_roles", description="(Admin) Onboarding-Rollen setzen (Kategorie + optional Primärrollen)")
    @app_commands.describe(
        guild_role="Rolle: Gildenmitglied (WeißeFlamme)",
        alliance_role="Rolle: Allianzmitglied",
        friend_role="Rolle: Freund",
        newbie_role="Rolle: NEWBIE (optional)",
        tank_role="Rolle: Tank (optional)",
        heal_role="Rolle: Heal (optional)",
        dps_role="Rolle: DPS (optional)"
    )
    async def onb_set_roles(
        inter: discord.Interaction,
        guild_role: discord.Role,
        alliance_role: discord.Role,
        friend_role: discord.Role,
        newbie_role: Optional[discord.Role] = None,
        tank_role: Optional[discord.Role] = None,
        heal_role: Optional[discord.Role] = None,
        dps_role: Optional[discord.Role] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        gcfg = cfg.get(str(inter.guild_id)) or {}
        gcfg["role_guild"]    = int(guild_role.id)
        gcfg["role_alliance"] = int(alliance_role.id)
        gcfg["role_friend"]   = int(friend_role.id)
        gcfg["role_newbie"]   = int(newbie_role.id) if newbie_role else 0
        gcfg["role_tank"]     = int(tank_role.id) if tank_role else 0
        gcfg["role_heal"]     = int(heal_role.id) if heal_role else 0
        gcfg["role_dps"]      = int(dps_role.id) if dps_role else 0
        cfg[str(inter.guild_id)] = gcfg; save_cfg()

        def _m(r): return r.mention if r else "—"
        await inter.response.send_message(
            "✅ Rollen gespeichert:\n"
            f"• Gilde: {guild_role.mention}\n"
            f"• Allianz: {alliance_role.mention}\n"
            f"• Freund: {friend_role.mention}\n"
            f"• NEWBIE: {_m(newbie_role)}\n"
            f"• Tank: {_m(tank_role)} • Heal: {_m(heal_role)} • DPS: {_m(dps_role)}",
            ephemeral=True
        )

    @tree.command(name="onb_test_dm", description="(Admin) Eigene Onboarding-DM testen")
    async def onb_test_dm(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        await _start_flow(inter.user)
        await inter.response.send_message("✅ Onboarding-DM verschickt (falls DMs offen).", ephemeral=True)
