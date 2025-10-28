# bot/event_rsvp_dm.py
# -----------------------------------------------------------
# Raid-/Event-Anmeldungen per DM mit automatischer Übersicht
# -----------------------------------------------------------

from __future__ import annotations
import json
import asyncio
from pathlib import Path
from typing import Dict, Optional
import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RSVP_FILE = DATA_DIR / "event_rsvp.json"
DM_CFG_FILE = DATA_DIR / "event_rsvp_cfg.json"

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg: Dict[str, dict] = _load(DM_CFG_FILE, {})

def save_store(): _save(RSVP_FILE, store)
def save_cfg(): _save(DM_CFG_FILE, cfg)

# -----------------------------------------------------------
# Hilfsfunktionen
# -----------------------------------------------------------

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
    obj.setdefault("maybe", {})
    obj.setdefault("no", [])
    obj.setdefault("target_role_id", 0)
    obj.setdefault("sent_dm_ids", [])  # Neue Liste für spätere DM-Löschung

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
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})", value="\n".join(dps_names) or "—", inline=True)

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

    emb.set_footer(text="(An-/Abmeldung läuft per DM)")
    return emb

# -----------------------------------------------------------
# DM-View für Buttons
# -----------------------------------------------------------

class RaidView(View):
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

    async def _safe_reply(self, inter: discord.Interaction, text: str):
        try:
            await inter.response.send_message(text, ephemeral=True)
        except discord.InteractionResponded:
            await inter.followup.send(text, ephemeral=True)
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

            # User aus allen Kategorien entfernen
            for k in ("TANK", "HEAL", "DPS"):
                obj["yes"][k] = [u for u in obj["yes"].get(k, []) if u != uid]
            obj["no"] = [u for u in obj.get("no", []) if u != uid]
            obj["maybe"].pop(str(uid), None)

            if group in ("TANK", "HEAL", "DPS"):
                obj["yes"][group].append(uid)
                text = f"Angemeldet als **{group}**."
            elif group == "MAYBE":
                obj["maybe"][str(uid)] = ""
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
            await self._safe_reply(inter, f"Fehler: {e!r}")

    @button(label="🛡️ Tank", style=ButtonStyle.primary)
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._update(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary)
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._update(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary)
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._update(inter, "DPS")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary)
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._update(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger)
    async def btn_no(self, inter: discord.Interaction, _):
        await self._update(inter, "NO")

# -----------------------------------------------------------
# Slash-Commands
# -----------------------------------------------------------

async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    """Slash Commands für das RSVP-System"""

    # Persistente Views
    for msg_id in list(store.keys()):
        try:
            client.add_view(RaidView(int(msg_id)))
        except Exception:
            pass

    @tree.command(name="raid_create_dm", description="Raid mit DM-Anmeldung erstellen")
    async def raid_create_dm(inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        channel: Optional[discord.TextChannel] = None,
        target_role: Optional[discord.Role] = None):
        if not inter.user.guild_permissions.administrator:
            await inter.response.send_message("Nur Admins dürfen das.", ephemeral=True)
            return

        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("Ungültiges Datum oder Zeitformat.", ephemeral=True)
            return

        ch = channel or inter.channel
        obj = {
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": title.strip(),
            "when_iso": when.isoformat(),
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": [],
            "target_role_id": int(target_role.id) if target_role else 0,
            "sent_dm_ids": []
        }

        emb = build_embed(inter.guild, obj)
        msg = await ch.send(embed=emb)
        store[str(msg.id)] = obj
        save_store()

        sent = 0
        for m in inter.guild.members:
            if m.bot: continue
            if target_role and target_role not in m.roles: continue
            try:
                text = (f"**{title}**\n"
                        f"🕒 {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                        f"Server-Übersicht: {ch.mention}\n\n"
                        "Bitte wähle unten deine Rolle.")
                dm_msg = await m.send(text, view=RaidView(int(msg.id)))
                obj["sent_dm_ids"].append({"uid": m.id, "mid": dm_msg.id})
                sent += 1
            except Exception:
                pass

        save_store()
        await inter.response.send_message(f"✅ Raid erstellt. DMs versendet: {sent}", ephemeral=True)

        # Autolöschung der DMs 2 Stunden nach Eventstart
        async def delete_dms_later():
            await asyncio.sleep(max(0, (when - datetime.now(TZ)).total_seconds() + 7200))
            for entry in obj.get("sent_dm_ids", []):
                try:
                    user = await client.fetch_user(entry["uid"])
                    msg = await user.fetch_message(entry["mid"])
                    await msg.delete()
                except Exception:
                    continue
            await _log(client, inter.guild_id, f"DMs für '{title}' gelöscht (nach Eventende).")

        asyncio.create_task(delete_dms_later())

# -----------------------------------------------------------
# Auto-Resend für neue Mitglieder
# -----------------------------------------------------------

async def auto_resend_for_new_member(member: discord.Member):
    if member.bot:
        return
    now = datetime.now(TZ)
    for mid, obj in list(store.items()):
        try:
            when = datetime.fromisoformat(obj.get("when_iso"))
            if now > when + timedelta(hours=2):
                continue
            tr_id = int(obj.get("target_role_id", 0) or 0)
            if tr_id and (member.guild.get_role(tr_id) not in member.roles):
                continue
            text = (f"**{obj.get('title','Event')}**\n"
                    f"🕒 {when.strftime('%a, %d.%m.%Y %H:%M')}\n"
                    f"Server: <#{obj.get('channel_id')}>")
            await member.send(text, view=RaidView(int(mid)))
        except Exception:
            continue
