# bot.py
# =====================================================================
# Monolithischer Discord-Bot:
# - RSVP per DM + Server-Übersicht
# - Auto-Resend für neue Member
# - Auto-Cleanup 2h nach Eventstart
# - Onboarding-DM (Gilde/Allianz/Freund -> Erfahrung -> NEWBIE -> Review)
# - Slash-Commands für Setup
# =====================================================================

from __future__ import annotations
import os
import json
from pathlib import Path
from typing import Dict, Optional

import discord
from discord.ext import tasks, commands
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# --------------------------- Grundsetup ---------------------------
TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# RSVP / Events
RSVP_FILE     = DATA_DIR / "event_rsvp.json"      # Events + Anmeldungen (Übersicht im Server)
DM_CFG_FILE   = DATA_DIR / "event_rsvp_cfg.json"  # Rollen-IDs (Tank/Heal/DPS) + Log-Channel

# Onboarding
ONBOARD_CFG_FILE = DATA_DIR / "onboarding_cfg.json"  # accept_ch, newbie_role

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
rsvp_cfg: Dict[str, dict] = _load(DM_CFG_FILE, {})
onb_cfg: Dict[str, dict] = _load(ONBOARD_CFG_FILE, {})

def save_store(): _save(RSVP_FILE, store)
def save_rsvp_cfg(): _save(DM_CFG_FILE, rsvp_cfg)
def save_onb_cfg(): _save(ONBOARD_CFG_FILE, onb_cfg)

# --------------------------- Bot/Intents ---------------------------
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# --------------------------- Utils ---------------------------

async def _log(guild_id: int, text: str):
    """Optionalen Log-Kanal für RSVP-DM benutzen, wenn gesetzt."""
    gcfg = rsvp_cfg.get(str(guild_id)) or {}
    ch_id = int(gcfg.get("LOG_CH", 0) or 0)
    if not ch_id:
        return
    g = bot.get_guild(guild_id)
    if not g:
        return
    ch = g.get_channel(ch_id)
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(f"[RSVP-DM] {text}")
        except Exception:
            pass

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def _init_event_shape(obj: dict):
    """Sichert die Struktur yes/maybe/no ab (defensiv gegen alte Saves)."""
    if "yes" not in obj or not isinstance(obj["yes"], dict):
        obj["yes"] = {"TANK": [], "HEAL": [], "DPS": []}
    for k in ("TANK", "HEAL", "DPS"):
        if k not in obj["yes"] or not isinstance(obj["yes"][k], list):
            obj["yes"][k] = []
    if "maybe" not in obj or not isinstance(obj["maybe"], dict):
        obj["maybe"] = {}
    if "no" not in obj or not isinstance(obj["no"], list):
        obj["no"] = []
    obj.setdefault("target_role_id", 0)

def get_role_ids_for_guild(guild_id: int) -> Dict[str, int]:
    g = rsvp_cfg.get(str(guild_id)) or {}
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS":  int(g.get("DPS",  0) or 0),
    }

def _member_from_event(inter: discord.Interaction, obj: dict) -> Optional[discord.Member]:
    """In DMs ist interaction.guild None → Member über guild_id aus dem Event holen."""
    try:
        if inter.guild is not None:
            return inter.guild.get_member(inter.user.id)
        gid = int(obj.get("guild_id", 0) or 0)
        if not gid:
            return None
        g = bot.get_guild(gid)
        if not g:
            return None
        return g.get_member(inter.user.id)
    except Exception:
        return None

def _primary_label(member: Optional[discord.Member], rid_map: Dict[str, int]) -> str:
    """Gibt 'Tank'/'Heal'/'DPS' zurück – robust, auch wenn member None ist."""
    if member is None:
        return ""
    r = member.guild.get_role(rid_map.get("TANK", 0) or 0)
    if r and r in getattr(member, "roles", []): return "Tank"
    r = member.guild.get_role(rid_map.get("HEAL", 0) or 0)
    if r and r in getattr(member, "roles", []): return "Heal"
    r = member.guild.get_role(rid_map.get("DPS", 0) or 0)
    if r and r in getattr(member, "roles", []): return "DPS"
    names = [getattr(rr, "name", "").lower() for rr in getattr(member, "roles", [])]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

def build_event_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    when = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=f"{obj.get('description','')}\n\n🕒 Zeit: {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple()
    )
    yes = obj["yes"]; maybe = obj["maybe"]; no = obj["no"]

    tank_names = [_mention(guild, int(u)) for u in yes.get("TANK", [])]
    heal_names = [_mention(guild, int(u)) for u in yes.get("HEAL", [])]
    dps_names  = [_mention(guild, int(u)) for u in yes.get("DPS",  [])]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})",  value="\n".join(dps_names)  or "—", inline=True)

    maybe_lines = []
    for uid_str, rlab in maybe.items():
        try:
            uid_i = int(uid_str)
        except Exception:
            continue
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, int(u)) for u in no]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        if r:
            emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="(An-/Abmeldung läuft per DM-Buttons)")
    return emb

# --------------------------- RSVP-DM View ---------------------------

class RaidView(View):
    """Diese View läuft **in der DM**. Sie editiert die Übersicht im Server-Channel."""
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _push_overview(self, inter: discord.Interaction, obj: dict):
        guild = bot.get_guild(obj["guild_id"])
        if not guild:
            return
        ch = guild.get_channel(obj["channel_id"])
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        try:
            msg = await ch.fetch_message(int(self.msg_id))
        except Exception:
            return
        emb = build_event_embed(guild, obj)
        try:
            await msg.edit(embed=emb)
        except Exception:
            pass

    async def _safe_reply(self, inter: discord.Interaction, text: str):
        try:
            await inter.response.send_message(text)
        except discord.InteractionResponded:
            try:
                await inter.followup.send(text)
            except Exception:
                pass
        except Exception:
            pass

    async def _update(self, inter: discord.Interaction, group: str):
        try:
            obj = store.get(self.msg_id)
            if not obj:
                await self._safe_reply(inter, "Dieses Event existiert nicht mehr.")
                return

            _init_event_shape(obj)
            uid = inter.user.id

            # User aus allen Buckets entfernen
            for k in ("TANK", "HEAL", "DPS"):
                obj["yes"][k] = [int(u) for u in obj["yes"].get(k, []) if int(u) != uid]
            obj["no"] = [int(u) for u in obj.get("no", []) if int(u) != uid]
            obj["maybe"].pop(str(uid), None)

            if group in ("TANK", "HEAL", "DPS"):
                obj["yes"][group].append(uid)
                text = f"Angemeldet als **{group}**."
            elif group == "MAYBE":
                member = _member_from_event(inter, obj)
                rid_map = get_role_ids_for_guild(obj["guild_id"])
                label = _primary_label(member, rid_map)  # "Tank"/"Heal"/"DPS" oder ""
                obj["maybe"][str(uid)] = label
                text = "Als **Vielleicht** eingetragen."
            elif group == "NO":
                obj["no"].append(uid)
                text = "Als **Abgemeldet** eingetragen."
            else:
                text = "Aktualisiert."

            save_store()
            await self._push_overview(inter, obj)
            await self._safe_reply(inter, text)

        except Exception as e:
            await _log(store.get(self.msg_id, {}).get("guild_id", 0), f"Button-Fehler: {e!r}")
            await self._safe_reply(inter, "❌ Unerwarteter Fehler bei der Anmeldung. Probier es bitte nochmal.")

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="dm_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._update(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._update(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="dm_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._update(inter, "DPS")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._update(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):
        await self._update(inter, "NO")

# --------------------------- Onboarding Views ---------------------------

def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

def _get_accept_channel(guild: discord.Guild) -> discord.TextChannel | None:
    gcfg = onb_cfg.get(str(guild.id)) or {}
    ch_id = int(gcfg.get("accept_ch", 0) or 0)
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, discord.TextChannel) else None

def _get_newbie_role(guild: discord.Guild) -> discord.Role | None:
    gcfg = onb_cfg.get(str(guild.id)) or {}
    rid = int(gcfg.get("newbie_role", 0) or 0)
    r = guild.get_role(rid)
    return r if isinstance(r, discord.Role) else None

class RoleSelectView(View):
    """DM-View: erster Schritt → Kategorie wählen."""
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
    """DM-View: zweiter Schritt → Erfahrung wählen; NEWBIE-Rolle optional vergeben."""
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
    """Staff-Kanal: Anfrage annehmen/ablehnen. Rollenzuweisung nach Kategorie."""
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
            await inter.response.send_message("Nur Admins können das.", ephemeral=True); return
        member = await self._get_member(inter.guild)
        if member:
            try:
                await member.send("❌ Deine Anfrage wurde abgelehnt.")
            except Exception:
                pass
        await inter.response.send_message("Abgelehnt.", ephemeral=True)

# --------------------------- Onboarding DM Sender ---------------------------

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
        # DMs evtl. geschlossen
        pass

# --------------------------- Slash-Commands ---------------------------

@tree.command(name="raid_set_roles_dm", description="(Admin) Primärrollen (Tank/Heal/DPS) für Maybe-Label setzen")
@app_commands.describe(tank_role="Rolle: Tank", heal_role="Rolle: Heal", dps_role="Rolle: DPS")
async def raid_set_roles_dm(
    inter: discord.Interaction,
    tank_role: discord.Role,
    heal_role: discord.Role,
    dps_role: discord.Role
):
    if not _is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    c = rsvp_cfg.get(str(inter.guild_id)) or {}
    c["TANK"] = int(tank_role.id)
    c["HEAL"] = int(heal_role.id)
    c["DPS"]  = int(dps_role.id)
    rsvp_cfg[str(inter.guild_id)] = c; save_rsvp_cfg()
    await inter.response.send_message(
        f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
        ephemeral=True
    )

@tree.command(name="raid_set_log_channel", description="(Admin) Log-Kanal für RSVP-DM-Fehler setzen (optional)")
@app_commands.describe(channel="Kanal für Log-Ausgaben")
async def raid_set_log_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not _is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    c = rsvp_cfg.get(str(inter.guild_id)) or {}
    c["LOG_CH"] = int(channel.id)
    rsvp_cfg[str(inter.guild_id)] = c; save_rsvp_cfg()
    await inter.response.send_message(f"✅ Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

@tree.command(name="raid_create_dm", description="(Admin) Raid/Anmeldung per DM erzeugen (mit Server-Übersicht)")
@app_commands.describe(
    title="Titel",
    date="Datum YYYY-MM-DD",
    time="Zeit HH:MM (24h)",
    channel="Server-Channel für die Übersicht",
    target_role="(Optional) Nur an diese Rolle DMs versenden",
    image_url="Optionales Bild fürs Embed"
)
async def raid_create_dm(
    inter: discord.Interaction,
    title: str,
    date: str,
    time: str,
    channel: Optional[discord.TextChannel] = None,
    target_role: Optional[discord.Role] = None,
    image_url: Optional[str] = None
):
    if not _is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    # Zeitpunkt parsen
    try:
        yyyy, mm, dd = [int(x) for x in date.split("-")]
        hh, mi = [int(x) for x in time.split(":")]
        when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
    except Exception:
        await inter.response.send_message("❌ Datum/Zeit ungültig. (YYYY-MM-DD / HH:MM)", ephemeral=True)
        return

    ch = channel or inter.channel
    if not isinstance(ch, discord.TextChannel):
        await inter.response.send_message("❌ Zielkanal ist kein Textkanal.", ephemeral=True); return

    obj = {
        "guild_id": inter.guild_id,
        "channel_id": ch.id,
        "title": title.strip(),
        "description": "",
        "when_iso": when.isoformat(),
        "image_url": (image_url or "").strip() or None,
        "yes": {"TANK": [], "HEAL": [], "DPS": []},
        "maybe": {},
        "no": [],
        "target_role_id": int(target_role.id) if target_role else 0
    }

    emb = build_event_embed(inter.guild, obj)
    msg = await ch.send(embed=emb)
    store[str(msg.id)] = obj
    save_store()

    # DMs versenden – an Zielrolle (falls gesetzt), sonst an alle Nicht-Bots.
    sent = 0
    tr_id = int(obj.get("target_role_id", 0) or 0)
    role_obj = inter.guild.get_role(tr_id) if tr_id else None

    for m in inter.guild.members:
        if m.bot:
            continue
        if role_obj and role_obj not in m.roles:
            continue
        try:
            dm_text = (f"**{title}** – Anmeldung\n"
                       f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                       f"• Übersicht im Server: #{ch.name}\n\n"
                       f"Wähle unten deine Teilnahme.")
            await m.send(dm_text, view=RaidView(int(msg.id)))
            sent += 1
        except Exception:
            pass

    ziel = role_obj.mention if role_obj else "alle Mitglieder (ohne Bots)"
    await inter.response.send_message(
        f"✅ Raid erstellt: {msg.jump_url}\n🎯 Zielgruppe: {ziel}\n✉️ DMs versendet: {sent}",
        ephemeral=True
    )

# Onboarding-Setup

@tree.command(name="onboarding_set_channel", description="(Admin) Zielkanal für Onboarding-Anfragen setzen")
async def onboarding_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not _is_admin(inter):
        await inter.response.send_message("Nur Admins.", ephemeral=True); return
    c = onb_cfg.get(str(inter.guild_id)) or {}
    c["accept_ch"] = int(channel.id)
    onb_cfg[str(inter.guild_id)] = c; save_onb_cfg()
    await inter.response.send_message(f"✅ Prüfkanal gesetzt: {channel.mention}", ephemeral=True)

@tree.command(name="onboarding_set_newbie", description="(Admin) NEWBIE-Unterrolle für 'Unerfahren' setzen")
async def onboarding_set_newbie(inter: discord.Interaction, role: discord.Role):
    if not _is_admin(inter):
        await inter.response.send_message("Nur Admins.", ephemeral=True); return
    c = onb_cfg.get(str(inter.guild_id)) or {}
    c["newbie_role"] = int(role.id)
    onb_cfg[str(inter.guild_id)] = c; save_onb_cfg()
    await inter.response.send_message(f"✅ NEWBIE-Rolle gesetzt: {role.mention}", ephemeral=True)

@tree.command(name="onboarding_test", description="(Admin) Schickt dir die Onboarding-DM zum Test")
async def onboarding_test(inter: discord.Interaction):
    if not _is_admin(inter):
        await inter.response.send_message("Nur Admins.", ephemeral=True); return
    try:
        await inter.user.send(
            "Test: Onboarding-DM",
            view=RoleSelectView(inter.guild)
        )
        await inter.response.send_message("✉️ DM gesendet (prüfe Postfach).", ephemeral=True)
    except Exception:
        await inter.response.send_message("Konnte keine DM senden (ggf. DMs geschlossen).", ephemeral=True)

# --------------------------- Auto-Resend für neue Member ---------------------------

async def auto_resend_for_new_member(member: discord.Member) -> None:
    """
    Schickt dem neuen Member die RSVP-DM für alle noch relevanten Events seiner Guild:
      - Event gehört zur gleichen Guild
      - Startzeit nicht länger als 2h her (Start <= now <= Start+2h)
      - oder Start liegt noch in der Zukunft
      - UND (falls gesetzt) Member besitzt die Zielrolle
    """
    try:
        if member.bot:
            return
        now = datetime.now(TZ)

        sent = 0
        for mid, obj in list(store.items()):
            try:
                if int(obj.get("guild_id", 0) or 0) != member.guild.id:
                    continue

                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    continue

                tr_id = int(obj.get("target_role_id", 0) or 0)
                if tr_id:
                    r = member.guild.get_role(tr_id)
                    if not (r and r in member.roles):
                        continue

                text = (f"**{obj.get('title','Event')}** – Anmeldung\n"
                        f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                        f"• Übersicht im Server: <#{obj.get('channel_id')}>")
                try:
                    await member.send(text, view=RaidView(int(mid)))
                    sent += 1
                except Exception:
                    pass
            except Exception:
                continue

        if sent:
            await _log(member.guild.id, f"Auto-Resend an {member} -> {sent} DM(s).")
    except Exception:
        pass

# --------------------------- Events ---------------------------

@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user}")
    try:
        await tree.sync()
        print("✅ Slash-Commands synchronisiert.")
    except Exception as e:
        print(f"⚠️ Slash-Command Sync Fehler: {e}")
    # DM-Views für existierende Events nach Neustart
    for mid in list(store.keys()):
        try:
            bot.add_view(RaidView(int(mid)))
        except Exception:
            pass
    cleanup_expired_events.start()

@bot.event
async def on_member_join(member: discord.Member):
    try:
        await send_onboarding_dm(member)
        await auto_resend_for_new_member(member)
    except Exception as e:
        print(f"[on_member_join] Fehler: {e}")

# --------------------------- Cleanup Task ---------------------------

@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        now = datetime.now(TZ)
        remove_ids = []
        for msg_id, obj in list(store.items()):
            try:
                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    guild = bot.get_guild(int(obj["guild_id"]))
                    if guild:
                        ch = guild.get_channel(int(obj["channel_id"]))
                        if ch:
                            try:
                                msg = await ch.fetch_message(int(msg_id))
                                await msg.delete()
                            except Exception:
                                pass
                    remove_ids.append(msg_id)
            except Exception:
                continue
        for mid in remove_ids:
            store.pop(mid, None)
        if remove_ids:
            save_store()
            print(f"🧹 Alte Events entfernt: {len(remove_ids)}")
    except Exception as e:
        print(f"[cleanup_expired_events] Fehler: {e}")

# --------------------------- Start ---------------------------

async def _setup_on_start():
    # nichts nötig – Commands werden per Decorator registriert
    pass

@bot.event
async def on_connect():
    bot.loop.create_task(_setup_on_start())

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    print("❌ Kein Token gefunden! Bitte Umgebungsvariable DISCORD_TOKEN setzen.")
else:
    bot.run(TOKEN)
