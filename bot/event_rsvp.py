# bot/event_rsvp.py
# RSVP/Raid-Signup mit Buttons, Rollen (Tank/Heal/DPS), Bild & Persistenz
# discord.py 2.4.x

from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
RSVP_FILE = DATA_DIR / "event_rsvp.json"
CFG_FILE  = DATA_DIR / "event_rsvp_cfg.json"

# -------- Persistenz --------
def _load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

# Struktur:
#  store = {
#    "<msg_id>": {
#      "guild_id": int, "channel_id": int,
#      "title": str, "when_iso": str, "image_url": str|None,
#      "yes": {"TANK": [uid...], "HEAL":[...], "DPS":[...]},
#      "maybe": {"<uid>": "Tank/Heal/DPS/"},
#      "no": [uid...],
#    }, ...
#  }
store: Dict[str, dict] = _load_json(RSVP_FILE, {})
cfg: Dict[str, dict]   = _load_json(CFG_FILE, {})  # pro guild: role_ids

def save_store():
    _save_json(RSVP_FILE, store)

def save_cfg():
    _save_json(CFG_FILE, cfg)

# -------- Rollen-Erkennung --------
def get_role_ids_for_guild(guild: discord.Guild) -> Dict[str, int]:
    g = cfg.get(str(guild.id)) or {}
    return {
        "TANK": int(g.get("TANK", 0)),
        "HEAL": int(g.get("HEAL", 0)),
        "DPS":  int(g.get("DPS", 0)),
    }

def label_from_member(member: discord.Member) -> str:
    rid = get_role_ids_for_guild(member.guild)
    # 1) per ID (stabil)
    if rid["TANK"] and discord.utils.get(member.roles, id=rid["TANK"]):
        return "Tank"
    if rid["HEAL"] and discord.utils.get(member.roles, id=rid["HEAL"]):
        return "Heal"
    if rid["DPS"]  and discord.utils.get(member.roles, id=rid["DPS"]):
        return "DPS"
    # 2) Fallback per Namens-Snippet
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps"  in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""  # unbekannt

# -------- Embed bauen --------
def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    dt = datetime.fromisoformat(obj["when_iso"])
    title = obj["title"]
    yes = obj["yes"]
    maybe = obj["maybe"]
    no = obj["no"]

    emb = discord.Embed(
        title=f"📅 {title}",
        description=f"**Zeit:** {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple(),
    )

    def mention(uid: int) -> str:
        m = guild.get_member(uid)
        return m.mention if m else f"<@{uid}>"

    # YES nach Rollen
    tank_names = [mention(u) for u in yes["TANK"]]
    heal_names = [mention(u) for u in yes["HEAL"]]
    dps_names  = [mention(u) for u in yes["DPS"]]

    emb.add_field(
        name=f"🛡️ Tank ({len(tank_names)})",
        value="\n".join(tank_names) if tank_names else "—",
        inline=True
    )
    emb.add_field(
        name=f"💚 Heal ({len(heal_names)})",
        value="\n".join(heal_names) if heal_names else "—",
        inline=True
    )
    emb.add_field(
        name=f"🗡️ DPS ({len(dps_names)})",
        value="\n".join(dps_names) if dps_names else "—",
        inline=True
    )

    # MAYBE mit (Rolle)
    maybe_lines = []
    for uid, rlab in maybe.items():
        uid_i = int(uid)
        m = guild.get_member(uid_i)
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{m.mention if m else f'<@{uid_i}>'}{label}")
    emb.add_field(
        name=f"❔ Vielleicht ({len(maybe_lines)})",
        value="\n".join(maybe_lines) if maybe_lines else "—",
        inline=False
    )

    # NO
    no_names = [mention(u) for u in no]
    emb.add_field(
        name=f"❌ Abgemeldet ({len(no_names)})",
        value="\n".join(no_names) if no_names else "—",
        inline=False
    )

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])
    emb.set_footer(text="Klicke auf die Buttons unten, um dich anzumelden.")
    return emb

# -------- UI / Buttons --------
class RaidView(View):
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _update(self, interaction: discord.Interaction, group: str):
        if self.msg_id not in store:
            await interaction.response.send_message("Dieses Event ist nicht mehr vorhanden.", ephemeral=True)
            return

        obj = store[self.msg_id]
        uid = interaction.user.id

        # remove user from all buckets
        for k in ("TANK", "HEAL", "DPS"):
            if uid in obj["yes"][k]:
                obj["yes"][k].remove(uid)
        obj["no"]    = [u for u in obj["no"] if u != uid]
        if str(uid) in obj["maybe"]:
            del obj["maybe"][str(uid)]

        # add to selected group
        if group in ("TANK","HEAL","DPS"):
            obj["yes"][group].append(uid)
            text = f"Angemeldet als **{group}**."
        elif group == "MAYBE":
            obj["maybe"][str(uid)] = label_from_member(interaction.user)  # (Tank/Heal/DPS) anzeigen
            text = "Als **Vielleicht** eingetragen."
        elif group == "NO":
            obj["no"].append(uid)
            text = "Als **Abgemeldet** eingetragen."
        else:
            text = "Aktualisiert."

        save_store()

        # refresh message
        guild = interaction.guild
        emb = build_embed(guild, obj)
        channel = guild.get_channel(obj["channel_id"])
        try:
            msg = await channel.fetch_message(int(self.msg_id))
            await msg.edit(embed=emb, view=self)
        except Exception:
            pass

        await interaction.response.send_message(text, ephemeral=True)

    @button(label="Tank", style=ButtonStyle.secondary, emoji="🛡️", custom_id="rsvp_tank")
    async def btn_tank(self, interaction: discord.Interaction, _):
        await self._update(interaction, "TANK")

    @button(label="Heal", style=ButtonStyle.secondary, emoji="💚", custom_id="rsvp_heal")
    async def btn_heal(self, interaction: discord.Interaction, _):
        await self._update(interaction, "HEAL")

    @button(label="DPS", style=ButtonStyle.secondary, emoji="🗡️", custom_id="rsvp_dps")
    async def btn_dps(self, interaction: discord.Interaction, _):
        await self._update(interaction, "DPS")

    @button(label="Vielleicht", style=ButtonStyle.secondary, emoji="❔", custom_id="rsvp_maybe")
    async def btn_maybe(self, interaction: discord.Interaction, _):
        await self._update(interaction, "MAYBE")

    @button(label="Abmelden", style=ButtonStyle.danger, emoji="❌", custom_id="rsvp_no")
    async def btn_no(self, interaction: discord.Interaction, _):
        await self._update(interaction, "NO")

# -------- Commands --------
def _is_admin(inter: discord.Interaction) -> bool:
    m = inter.user
    return bool(m and (m.guild_permissions.administrator or m.guild_permissions.manage_guild))

async def setup_rsvp(client: discord.Client, tree: app_commands.CommandTree):
    # beim Start alle offenen Views wieder registrieren
    for msg_id, obj in list(store.items()):
        g = client.get_guild(obj["guild_id"])
        if not g:
            continue
        client.add_view(RaidView(int(msg_id)), message_id=int(msg_id))

    @tree.command(name="raid_set_roles", description="Rollen für Tank/Heal/DPS festlegen (pro Server).")
    @app_commands.describe(tank_role="Rolle für Tank", heal_role="Rolle für Heal", dps_role="Rolle für DPS")
    async def raid_set_roles(
        inter: discord.Interaction,
        tank_role: discord.Role,
        heal_role: discord.Role,
        dps_role: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        cfg[str(inter.guild_id)] = {"TANK": tank_role.id, "HEAL": heal_role.id, "DPS": dps_role.id}
        save_cfg()
        await inter.response.send_message(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

    @tree.command(name="raid_create", description="Raid-/Event-Anmeldung mit Buttons erstellen.")
    @app_commands.describe(
        title="Titel (wird im Embed angezeigt)",
        date="Datum YYYY-MM-DD (Europe/Berlin)",
        time="Zeit HH:MM (24h)",
        channel="Zielkanal",
        image_url="Optionales Bild-URL für das Embed"
    )
    async def raid_create(
        inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        channel: Optional[discord.TextChannel] = None,
        image_url: Optional[str] = None,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig. Format: YYYY-MM-DD und HH:MM.", ephemeral=True)
            return

        ch = channel or inter.channel
        obj = {
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": title.strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": []
        }

        emb = build_embed(inter.guild, obj)
        view = RaidView(0)  # dummy, setzen wir nach dem Senden korrekt
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)  # fix
        # persist
        store[str(msg.id)] = obj
        save_store()
        # persistente View registrieren
        inter.client.add_view(RaidView(msg.id), message_id=msg.id)

        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}", ephemeral=True)

    @tree.command(name="raid_show", description="Embed/Listen neu aufbauen.")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_show(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        obj = store[message_id]
        emb = build_embed(inter.guild, obj)
        ch = inter.guild.get_channel(obj["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(embed=emb, view=RaidView(int(message_id)))
            await inter.response.send_message("✅ Aktualisiert.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    @tree.command(name="raid_close", description="Buttons sperren (nur Admin).")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_close(inter: discord.Interaction, message_id: str):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        ch = inter.guild.get_channel(store[message_id]["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(view=None)
            await inter.response.send_message("🔒 Gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)
