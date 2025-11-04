# event_rsvp_dm.py
# RSVP per DM (Tank/Heal/DPS/Maybe/No). Übersicht im Server-Kanal (Embed).
# Erweiterungen:
#  - dm_sent-Tracking, Resend-Commands
#  - Teilnahme X/Y im Embed
#  - DM-Message löscht sich direkt nach Button-Klick

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional, List, Set

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RSVP_FILE   = DATA_DIR / "event_rsvp.json"      # Events + Anmeldungen (Übersicht im Server)
DM_CFG_FILE = DATA_DIR / "event_rsvp_cfg.json"  # Rollen-IDs (Tank/Heal/DPS) + Log-Channel

#  store[str(message_id)] = {
#    "guild_id": int, "channel_id": int,
#    "title": str, "description": str, "when_iso": str, "image_url": str|None,
#    "yes":{"TANK":[uid], "HEAL":[uid], "DPS":[uid]}, "maybe":{"uid_str":"Tank/Heal/DPS/"},
#    "no":[uid],
#    "target_role_id": int,
#    "dm_sent":[uid, ...]   # <-- neu: an wen wurde DM versendet
#  }

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg:   Dict[str, dict] = _load(DM_CFG_FILE, {})

def save_store(): _save(RSVP_FILE, store)
def save_cfg():   _save(DM_CFG_FILE, cfg)

# --------------- Utils / Log ---------------

async def _log(client: discord.Client, guild_id: int, text: str):
    gcfg = cfg.get(str(guild_id)) or {}
    ch_id = int(gcfg.get("LOG_CH", 0) or 0)
    if not ch_id:
        return
    g = client.get_guild(guild_id)
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
    obj.setdefault("dm_sent", [])

def get_role_ids_for_guild(guild_id: int) -> Dict[str, int]:
    g = cfg.get(str(guild_id)) or {}
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS":  int(g.get("DPS",  0) or 0),
    }

def _member_from_event(inter: discord.Interaction, obj: dict) -> Optional[discord.Member]:
    try:
        if inter.guild is not None:
            return inter.guild.get_member(inter.user.id)
        gid = int(obj.get("guild_id", 0) or 0)
        if not gid:
            return None
        g = inter.client.get_guild(gid)
        if not g:
            return None
        return g.get_member(inter.user.id)
    except Exception:
        return None

def _primary_label(member: Optional[discord.Member], rid_map: Dict[str, int]) -> str:
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

def _count_baseline(guild: discord.Guild, obj: dict) -> int:
    """Y: Gesamtzahl der adressierten Leute (Zielrolle oder alle Nicht-Bots)."""
    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        return len(r.members) if r else 0
    return len([m for m in guild.members if not m.bot])

def _count_voted(obj: dict) -> int:
    """X: Unique Anzahl Yes/Maybe/No."""
    voters: Set[int] = set()
    for k in ("TANK", "HEAL", "DPS"):
        voters.update(int(u) for u in obj["yes"].get(k, []))
    voters.update(int(u) for u in obj.get("no", []))
    for k in obj.get("maybe", {}).keys():
        try:
            voters.add(int(k))
        except Exception:
            pass
    return len(voters)

def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
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

    # Teilnahme X/Y
    voted = _count_voted(obj)
    base  = _count_baseline(guild, obj)
    emb.add_field(name="🗳️ Teilnahme", value=f"{voted} / {base}", inline=False)

    # Zielrolle anzeigen
    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        if r:
            emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])
    emb.set_footer(text="(An-/Abmeldung läuft per DM-Buttons)")
    return emb

# --------------- DM View ---------------

class RaidView(View):
    """View in der DM. Nach Klick löscht der Bot die DM-Message."""
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _push_overview(self, inter: discord.Interaction, obj: dict):
        guild = inter.client.get_guild(obj["guild_id"])
        if not guild:
            return
        ch = guild.get_channel(obj["channel_id"])
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        try:
            msg = await ch.fetch_message(int(self.msg_id))
        except Exception:
            return
        emb = build_embed(guild, obj)
        try:
            await msg.edit(embed=emb)
        except Exception:
            pass

    async def _ack_and_delete(self, inter: discord.Interaction, text: str):
        # kurze Bestätigung, dann DM löschen
        try:
            await inter.response.send_message(text)
        except discord.InteractionResponded:
            try:
                await inter.followup.send(text)
            except Exception:
                pass
        except Exception:
            pass
        # DM-Message löschen
        try:
            if inter.message:
                await inter.message.delete()
        except Exception:
            pass

    async def _update(self, inter: discord.Interaction, group: str):
        try:
            obj = store.get(self.msg_id)
            if not obj:
                await self._ack_and_delete(inter, "Dieses Event existiert nicht mehr.")
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
                label = _primary_label(member, rid_map)
                obj["maybe"][str(uid)] = label
                text = "Als **Vielleicht** eingetragen."
            elif group == "NO":
                obj["no"].append(uid)
                text = "Als **Abgemeldet** eingetragen."
            else:
                text = "Aktualisiert."

            save_store()
            await self._push_overview(inter, obj)
            await self._ack_and_delete(inter, text)

        except Exception as e:
            try:
                await _log(inter.client, store.get(self.msg_id, {}).get("guild_id", 0), f"Button-Fehler: {e!r}")
            except Exception:
                pass
            await self._ack_and_delete(inter, "❌ Unerwarteter Fehler. Versuch's nochmal.")

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="dm_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):      await self._update(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):      await self._update(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="dm_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):       await self._update(inter, "DPS")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):     await self._update(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):        await self._update(inter, "NO")

# --------------- Commands / Setup ---------------

def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))

async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    """Slash-Commands & persistente Views registrieren."""
    # persistente Views (nach Restart)
    for msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
            client.add_view(RaidView(int(msg_id)))
        except Exception:
            pass

    @tree.command(name="raid_set_roles_dm", description="(Admin) Primärrollen für Maybe-Label setzen")
    @app_commands.describe(tank_role="Rolle: Tank", heal_role="Rolle: Heal", dps_role="Rolle: DPS")
    async def raid_set_roles_dm(
        inter: discord.Interaction,
        tank_role: discord.Role,
        heal_role: discord.Role,
        dps_role: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["TANK"] = int(tank_role.id)
        c["HEAL"] = int(heal_role.id)
        c["DPS"]  = int(dps_role.id)
        cfg[str(inter.guild_id)] = c; save_cfg()
        await inter.response.send_message(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

    @tree.command(name="raid_set_log_channel", description="(Admin) Log-Kanal für RSVP/DM setzen (optional)")
    async def raid_set_log_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["LOG_CH"] = int(channel.id)
        cfg[str(inter.guild_id)] = c; save_cfg()
        await inter.response.send_message(f"✅ Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="raid_create_dm", description="(Admin) Raid/Anmeldung per DM erzeugen")
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
            "target_role_id": int(target_role.id) if target_role else 0,
            "dm_sent": []
        }

        emb = build_embed(inter.guild, obj)
        msg = await ch.send(embed=emb)
        store[str(msg.id)] = obj
        save_store()

        # DMs versenden
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
                           f"Wähle unten deine Teilnahme (die Nachricht löscht sich nach Klick).")
                dm_msg = await m.send(dm_text, view=RaidView(int(msg.id)))
                # als „versendet“ markieren
                obj["dm_sent"].append(m.id)
                sent += 1
            except Exception:
                pass

        save_store()
        ziel = role_obj.mention if role_obj else "alle Mitglieder (ohne Bots)"
        await inter.response.send_message(
            f"✅ Raid erstellt: {msg.jump_url}\n🎯 Zielgruppe: {ziel}\n✉️ DMs versendet: {sent}",
            ephemeral=True
        )

    @tree.command(name="raid_resend_missing", description="(Admin) DMs an alle, die noch keine DM bekamen und nicht abgestimmt haben")
    @app_commands.describe(message_id="ID der Server-Übersichts-Nachricht")
    async def raid_resend_missing(inter: discord.Interaction, message_id: str):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        obj = store.get(message_id)
        if not obj:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        _init_event_shape(obj)

        guild = inter.guild
        tr_id = int(obj.get("target_role_id", 0) or 0)
        role_obj = guild.get_role(tr_id) if tr_id else None

        # Baseline: Zielgruppe
        candidates: List[discord.Member] = []
        if role_obj:
            candidates = [m for m in role_obj.members if not m.bot]
        else:
            candidates = [m for m in guild.members if not m.bot]

        # Wer hat schon „irgendwas“ getan / DM bekommen?
        voted: Set[int] = set()
        for k in ("TANK", "HEAL", "DPS"):
            voted.update(int(u) for u in obj["yes"].get(k, []))
        voted.update(int(u) for u in obj.get("no", []))
        voted.update(int(k) for k in obj.get("maybe", {}).keys() if k.isdigit())

        already_dm: Set[int] = set(int(u) for u in obj.get("dm_sent", []))

        target: List[discord.Member] = [m for m in candidates if m.id not in voted and m.id not in already_dm]

        # Zeit-Check: Nur senden, wenn Event nicht länger als 2h vorbei
        when = datetime.fromisoformat(obj["when_iso"])
        now = datetime.now(TZ)
        if now > when + timedelta(hours=2):
            await inter.response.send_message("ℹ️ Event ist älter als 2h – keine DMs mehr.", ephemeral=True)
            return

        sent = 0
        for m in target:
            try:
                dm_text = (f"**{obj.get('title','Event')}** – Anmeldung\n"
                           f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                           f"• Übersicht im Server: <#{obj.get('channel_id')}>")
                await m.send(dm_text, view=RaidView(int(message_id)))
                obj["dm_sent"].append(m.id)
                sent += 1
            except Exception:
                pass

        save_store()
        await inter.response.send_message(f"✅ Nachversand erledigt. Neu gesendet: {sent}", ephemeral=True)

    @tree.command(name="raid_resend_to", description="(Admin) DM gezielt an einen User für ein Event senden")
    @app_commands.describe(message_id="ID der Server-Übersichts-Nachricht", user="Ziel-User")
    async def raid_resend_to(inter: discord.Interaction, message_id: str, user: discord.Member):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        obj = store.get(message_id)
        if not obj:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        _init_event_shape(obj)

        when = datetime.fromisoformat(obj["when_iso"])
        now = datetime.now(TZ)
        if now > when + timedelta(hours=2):
            await inter.response.send_message("ℹ️ Event ist älter als 2h – keine DMs mehr.", ephemeral=True)
            return

        # Wenn target_role gesetzt ist, nur senden wenn User die Rolle hat
        tr_id = int(obj.get("target_role_id", 0) or 0)
        if tr_id:
            r = inter.guild.get_role(tr_id)
            if not (r and r in user.roles):
                await inter.response.send_message("❌ User gehört nicht zur Zielrolle.", ephemeral=True)
                return

        try:
            dm_text = (f"**{obj.get('title','Event')}** – Anmeldung\n"
                       f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                       f"• Übersicht im Server: <#{obj.get('channel_id')}>")
            await user.send(dm_text, view=RaidView(int(message_id)))
            if user.id not in obj["dm_sent"]:
                obj["dm_sent"].append(user.id)
            save_store()
            await inter.response.send_message(f"✅ DM an {user.mention} gesendet.", ephemeral=True)
        except Exception:
            await inter.response.send_message("❌ Konnte keine DM senden (privat blockiert?).", ephemeral=True)

# --------------- Auto-Resend bei Join ---------------

async def auto_resend_for_new_member(member: discord.Member) -> None:
    """Schickt neuen Mitgliedern DM-Einladungen für laufende/fresh Events (<= 2h nach Start)."""
    try:
        if member.bot:
            return
        now = datetime.now(TZ)
        sent_total = 0

        for mid, obj in list(store.items()):
            try:
                _init_event_shape(obj)
                if int(obj.get("guild_id", 0) or 0) != member.guild.id:
                    continue

                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    continue  # zu alt

                tr_id = int(obj.get("target_role_id", 0) or 0)
                if tr_id:
                    r = member.guild.get_role(tr_id)
                    if not (r and r in member.roles):
                        continue

                # nur, wenn nicht schon DM erhalten
                if member.id in obj.get("dm_sent", []):
                    continue

                text = (f"**{obj.get('title','Event')}** – Anmeldung\n"
                        f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                        f"• Übersicht im Server: <#{obj.get('channel_id')}>")
                try:
                    await member.send(text, view=RaidView(int(mid)))
                    obj["dm_sent"].append(member.id)
                    sent_total += 1
                except Exception:
                    pass
            except Exception:
                continue

        if sent_total:
            save_store()
            try:
                client = member._state._get_client()
                await _log(client, member.guild.id, f"Auto-Resend an {member} -> {sent_total} DM(s).")
            except Exception:
                pass

    except Exception:
        pass
