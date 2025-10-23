# bot/event_rsvp_dm.py
# DM-basiertes RSVP/Raid-System: Public-Übersicht im Server, Teilnahme + Rolle per DM.
# discord.py 2.4.x

from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ui import View, button, select
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RSVP_FILE = DATA_DIR / "event_rsvp.json"       # kompatibel zum bisherigen Store (wird erweitert)
CFG_FILE  = DATA_DIR / "event_rsvp_cfg.json"   # Tank/Heal/DPS Role-IDs pro Guild gespeichert

# ---------- Persistenz ----------
def _load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

# Struktur (erweitert):
# store = {
#   "<msg_id>": {
#     "guild_id": int, "channel_id": int,
#     "title": str, "description": str, "when_iso": str, "image_url": str|None,
#     "yes": {"TANK":[uid...],"HEAL":[...],"DPS":[...]},
#     "maybe": {"<uid>": "Tank/Heal/DPS/"},
#     "no": [uid...],
#     "target_role_id": int|None,
#     "invited": [uid...],                      # wem DM gesendet wurde
#   },
#   ...
# }
store: Dict[str, dict] = _load_json(RSVP_FILE, {})
cfg:   Dict[str, dict] = _load_json(CFG_FILE, {})

def save_store(): _save_json(RSVP_FILE, store)
def save_cfg():   _save_json(CFG_FILE, cfg)

# ---------- Rollen-Helfer ----------
def get_role_ids_for_guild(guild: discord.Guild) -> Dict[str, int]:
    g = cfg.get(str(guild.id)) or {}
    return {"TANK": int(g.get("TANK", 0) or 0),
            "HEAL": int(g.get("HEAL", 0) or 0),
            "DPS":  int(g.get("DPS",  0) or 0)}

def guess_label_from_member(member: discord.Member) -> str:
    rid = get_role_ids_for_guild(member.guild)
    if rid["TANK"] and discord.utils.get(member.roles, id=rid["TANK"]): return "Tank"
    if rid["HEAL"] and discord.utils.get(member.roles, id=rid["HEAL"]): return "Heal"
    if rid["DPS"]  and discord.utils.get(member.roles, id=rid["DPS"]):  return "DPS"
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any(("dps" in n) or ("dd" in n) for n in names): return "DPS"
    return ""

def _role_key(label: str) -> Optional[str]:
    label = (label or "").strip().lower()
    if label == "tank": return "TANK"
    if label == "heal": return "HEAL"
    if label == "dps":  return "DPS"
    return None

# ---------- Embed (öffentlich) ----------
def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def build_public_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    dt = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=(obj.get("description","") or "")
                    + f"\n\n🕒 Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple()
    )

    tank_names = [_mention(guild, u) for u in obj["yes"]["TANK"]]
    heal_names = [_mention(guild, u) for u in obj["yes"]["HEAL"]]
    dps_names  = [_mention(guild, u) for u in obj["yes"]["DPS"]]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})",  value="\n".join(dps_names)  or "—", inline=True)

    maybe_lines = []
    for uid, rlab in obj["maybe"].items():
        uid_i = int(uid); label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, u) for u in obj["no"]]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="Klicke unten „📬 Per DM anmelden“, um Teilnahme & Rolle privat zu setzen.")
    return emb

# ---------- Store-Update ----------
def _clear_user_from_all(obj: dict, uid: int):
    for k in ("TANK","HEAL","DPS"):
        if uid in obj["yes"][k]:
            obj["yes"][k].remove(uid)
    obj["no"]    = [u for u in obj["no"] if u != uid]
    obj["maybe"].pop(str(uid), None)

def _apply_rsvp(obj: dict, uid: int, participation: str, role_label: Optional[str]):
    """participation: 'YES'|'MAYBE'|'NO' ; role_label: 'Tank'|'Heal'|'DPS'|None"""
    _clear_user_from_all(obj, uid)
    if participation == "YES":
        rk = _role_key(role_label or "")
        if not rk:
            # wenn keine Rolle gewählt/gefunden, downgrade zu maybe mit Label (oder leer)
            obj["maybe"][str(uid)] = role_label or ""
        else:
            obj["yes"][rk].append(uid)
    elif participation == "MAYBE":
        obj["maybe"][str(uid)] = role_label or ""
    elif participation == "NO":
        obj["no"].append(uid)

# ---------- DM-View ----------
class DmRsvpView(View):
    def __init__(self, msg_id: int, user_id: int):
        super().__init__(timeout=3600)
        self.msg_id = str(msg_id)
        self.user_id = int(user_id)
        self._role_choice: Optional[str] = None  # "Tank"/"Heal"/"DPS"

    async def _update_and_refresh_public(self, interaction: discord.Interaction, participation: str):
        if self.msg_id not in store:
            await interaction.response.send_message("Dieses Event existiert nicht mehr.", ephemeral=True)
            return

        obj = store[self.msg_id]
        guild = interaction.client.get_guild(obj["guild_id"])
        if not guild:
            await interaction.response.send_message("Guild nicht verfügbar.", ephemeral=True)
            return

        # Rolle ableiten falls nicht manuell gewählt
        role_label = self._role_choice
        member = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        if not role_label:
            role_label = guess_label_from_member(member)

        # Wenn Teilnahme==YES und keine Rolle ableitbar, zwinge Rolle
        if participation == "YES" and not _role_key(role_label or ""):
            await interaction.response.send_message(
                "Bitte zuerst eine **Rolle** wählen (Tank/Heal/DPS), dann **Teilnahme: Ja**.",
                ephemeral=True
            )
            return

        _apply_rsvp(obj, self.user_id, participation, role_label)
        save_store()

        # Public Embed aktualisieren
        ch = guild.get_channel(obj["channel_id"])
        if isinstance(ch, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            try:
                msg = await ch.fetch_message(int(self.msg_id))
                await msg.edit(embed=build_public_embed(guild, obj), view=OpenDmRsvpView(int(self.msg_id)))
            except Exception:
                pass

        await interaction.response.send_message(
            f"Gespeichert: **{participation}**"
            + (f", Rolle **{role_label}**" if role_label else ""),
            ephemeral=True
        )

    @button(label="Rolle: Tank", style=ButtonStyle.secondary, emoji="🛡️", custom_id="dm_role_tank")
    async def role_tank(self, interaction: discord.Interaction, _):
        self._role_choice = "Tank"
        await interaction.response.send_message("Rolle gesetzt: **Tank**", ephemeral=True)

    @button(label="Rolle: Heal", style=ButtonStyle.secondary, emoji="💚", custom_id="dm_role_heal")
    async def role_heal(self, interaction: discord.Interaction, _):
        self._role_choice = "Heal"
        await interaction.response.send_message("Rolle gesetzt: **Heal**", ephemeral=True)

    @button(label="Rolle: DPS", style=ButtonStyle.secondary, emoji="🗡️", custom_id="dm_role_dps")
    async def role_dps(self, interaction: discord.Interaction, _):
        self._role_choice = "DPS"
        await interaction.response.send_message("Rolle gesetzt: **DPS**", ephemeral=True)

    @button(label="Teilnahme: Ja", style=ButtonStyle.success, emoji="✅", custom_id="dm_yes")
    async def yes(self, interaction: discord.Interaction, _):
        await self._update_and_refresh_public(interaction, "YES")

    @button(label="Teilnahme: Vielleicht", style=ButtonStyle.secondary, emoji="❔", custom_id="dm_maybe")
    async def maybe(self, interaction: discord.Interaction, _):
        await self._update_and_refresh_public(interaction, "MAYBE")

    @button(label="Teilnahme: Nein", style=ButtonStyle.danger, emoji="❌", custom_id="dm_no")
    async def no(self, interaction: discord.Interaction, _):
        await self._update_and_refresh_public(interaction, "NO")

# ---------- Public-View (1 Button: DM öffnen) ----------
class OpenDmRsvpView(View):
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    @button(label="Per DM anmelden / ändern", style=ButtonStyle.primary, emoji="📬", custom_id="open_dm_rsvp")
    async def open_dm(self, interaction: discord.Interaction, _):
        if self.msg_id not in store:
            await interaction.response.send_message("Dieses Event existiert nicht mehr.", ephemeral=True)
            return
        obj = store[self.msg_id]

        # DM senden
        try:
            emb = discord.Embed(
                title="Raid/Event – Private Anmeldung",
                description=f"**{obj['title']}**\n"
                            f"{obj.get('description','')}\n\n"
                            "Wähle zuerst (optional) deine **Rolle** und dann deine **Teilnahme**.",
                color=discord.Color.blurple()
            )
            view = DmRsvpView(int(self.msg_id), interaction.user.id)
            await interaction.user.send(embed=emb, view=view)
            await interaction.response.send_message("Ich habe dir eine DM geschickt ✔️", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(
                "Ich konnte dir **keine DM** schicken. Bitte DMs öffnen und erneut klicken.",
                ephemeral=True
            )

# ---------- Broadcast DMs ----------
async def _broadcast_role_dms(client: discord.Client, obj: dict):
    guild = client.get_guild(obj["guild_id"])
    if not guild: return
    role_id = obj.get("target_role_id") or 0
    role = guild.get_role(int(role_id)) if role_id else None
    if not role: return

    invited = set(obj.get("invited") or [])
    to_send = [m for m in role.members if not m.bot and m.id not in invited]
    if not to_send: return

    for i, mem in enumerate(to_send, start=1):
        try:
            emb = discord.Embed(
                title="Raid/Event – Private Anmeldung",
                description=f"**{obj['title']}**\n"
                            f"{obj.get('description','')}\n\n"
                            "Wähle zuerst (optional) deine **Rolle** und dann deine **Teilnahme**.",
                color=discord.Color.blurple()
            )
            await mem.send(embed=emb, view=DmRsvpView(int(obj["message_id"]), mem.id))
            invited.add(mem.id)
        except discord.Forbidden:
            # DM geschlossen – ignoriere
            pass
        except Exception:
            pass
        # Throttle (Rate-Limit freundlich)
        if i % 5 == 0:
            await asyncio.sleep(0.7)

    obj["invited"] = sorted(list(invited))
    save_store()

# ---------- Admin-Helfer ----------
def _is_admin(inter: discord.Interaction) -> bool:
    m = inter.user
    return bool(m and (m.guild_permissions.administrator or m.guild_permissions.manage_guild))

# ---------- Commands / Setup ----------
async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    # Persistente Public-Views wieder registrieren
    for msg_id, obj in list(store.items()):
        g = client.get_guild(obj.get("guild_id", 0))
        if not g:
            continue
        # sichere Felder
        obj.setdefault("description", "")
        obj.setdefault("target_role_id", None)
        obj.setdefault("invited", [])
        store[msg_id] = obj
        try:
            client.add_view(OpenDmRsvpView(int(msg_id)), message_id=int(msg_id))
        except Exception:
            pass
    save_store()

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

    @tree.command(name="raid_create", description="Raid-/Event mit DM-RSVP erstellen (Public-Übersicht bleibt im Channel).")
    @app_commands.describe(
        title="Titel",
        date="Datum YYYY-MM-DD",
        time="Zeit HH:MM (24h)",
        channel="Zielkanal (optional, sonst aktueller)",
        image_url="Bild-URL (optional)",
        target_role="Rolle, die per DM eingeladen wird (optional)",
        description="Beschreibung/Notizen (optional)"
    )
    async def raid_create(
        inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        channel: Optional[discord.TextChannel] = None,
        image_url: Optional[str] = None,
        target_role: Optional[discord.Role] = None,
        description: Optional[str] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig. Format: YYYY-MM-DD / HH:MM.", ephemeral=True)
            return

        ch = channel or inter.channel
        obj = {
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": title.strip(),
            "description": (description or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": [],
            "target_role_id": (target_role.id if target_role else None),
            "invited": []
        }

        # Public Nachricht
        emb = build_public_embed(inter.guild, obj)
        view = OpenDmRsvpView(0)  # placeholder
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)

        # persistieren
        obj["message_id"] = msg.id
        store[str(msg.id)] = obj
        save_store()

        # persistente View registrieren (Server-Neustart)
        inter.client.add_view(OpenDmRsvpView(msg.id), message_id=msg.id)

        # Optionale Broadcast-DM an target_role
        if target_role:
            await _broadcast_role_dms(inter.client, obj)

        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}", ephemeral=True)

    @tree.command(name="raid_show", description="Raid-Übersicht neu aufbauen.")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_show(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        obj = store[message_id]
        emb = build_public_embed(inter.guild, obj)
        ch = inter.guild.get_channel(obj["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(embed=emb, view=OpenDmRsvpView(int(message_id)))
            await inter.response.send_message("✅ Aktualisiert.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    @tree.command(name="raid_close", description="Public-Buttons sperren (DMs bleiben).")
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
            await inter.response.send_message("🔒 Öffentlicher Button gesperrt. (DMs bleiben funktionsfähig)", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)
