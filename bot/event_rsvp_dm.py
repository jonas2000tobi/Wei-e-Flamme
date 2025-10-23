# bot/event_rsvp_dm.py
# DM-basiertes RSVP für Raids.
# - Postet eine öffentliche Übersicht (OHNE Buttons) in den gewählten Channel.
# - Sendet DMs mit Buttons (Tank/Heal/DPS/Vielleicht/Abmelden) an eine Zielrolle.
# - Antworten in DMs aktualisieren die öffentliche Übersicht live.
# - Zielrolle kann beim Erstellen angegeben ODER vorab gespeichert werden.

from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DM_RSVP_FILE = DATA_DIR / "dm_rsvp_store.json"
DM_CFG_FILE  = DATA_DIR / "dm_rsvp_cfg.json"

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(DM_RSVP_FILE, {})
cfg:   Dict[str, dict] = _load(DM_CFG_FILE, {})

def save_store(): _save(DM_RSVP_FILE, store)
def save_cfg():   _save(DM_CFG_FILE, cfg)

# ---------------------- Datenmodell ----------------------
@dataclass
class DMEvent:
    guild_id: int
    channel_id: int
    title: str
    when_iso: str
    description: str = ""
    image_url: Optional[str] = None
    public_message_id: Optional[int] = None
    target_role_id: Optional[int] = None
    invited: List[int] = None
    yes_tank: List[int] = None
    yes_heal: List[int] = None
    yes_dps:  List[int] = None
    maybe: Dict[str, str] = None
    no: List[int] = None

    def to_dict(self) -> dict:
        return {
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "title": self.title,
            "when_iso": self.when_iso,
            "description": self.description,
            "image_url": self.image_url,
            "public_message_id": self.public_message_id,
            "target_role_id": self.target_role_id,
            "invited": self.invited or [],
            "yes": {"TANK": self.yes_tank or [], "HEAL": self.yes_heal or [], "DPS": self.yes_dps or []},
            "maybe": self.maybe or {},
            "no": self.no or []
        }

    @staticmethod
    def from_dict(d: dict) -> "DMEvent":
        yes = d.get("yes") or {"TANK": [], "HEAL": [], "DPS": []}
        return DMEvent(
            guild_id=d["guild_id"],
            channel_id=d["channel_id"],
            title=d["title"],
            when_iso=d["when_iso"],
            description=d.get("description",""),
            image_url=d.get("image_url"),
            public_message_id=d.get("public_message_id"),
            target_role_id=d.get("target_role_id"),
            invited=d.get("invited", []),
            yes_tank=yes.get("TANK", []),
            yes_heal=yes.get("HEAL", []),
            yes_dps=yes.get("DPS", []),
            maybe=d.get("maybe", {}),
            no=d.get("no", [])
        )

# ---------------------- Darstellung ----------------------
def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def build_public_embed(guild: discord.Guild, payload: dict) -> discord.Embed:
    dt = datetime.fromisoformat(payload["when_iso"])
    desc = payload.get("description","")
    emb = discord.Embed(
        title=f"📅 {payload['title']}",
        description=(desc + f"\n\n🕒 Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"),
        color=discord.Color.blurple()
    )
    yes = payload["yes"]
    tank = [_mention(guild, u) for u in yes["TANK"]]
    heal = [_mention(guild, u) for u in yes["HEAL"]]
    dps  = [_mention(guild, u) for u in yes["DPS"]]
    emb.add_field(name=f"🛡️ Tank ({len(tank)})", value="\n".join(tank) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal)})", value="\n".join(heal) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps)})",  value="\n".join(dps)  or "—", inline=True)

    maybe_lines = []
    for uid, rlab in (payload.get("maybe") or {}).items():
        uid_i = int(uid)
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, u) for u in (payload.get("no") or [])]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    if payload.get("image_url"):
        emb.set_image(url=payload["image_url"])

    rid = payload.get("target_role_id") or 0
    footer = "Antworten per DM; Übersicht aktualisiert sich automatisch."
    if rid:
        r = guild.get_role(int(rid))
        if r:
            invited = set(payload.get("invited") or [])
            answered = set(
                yes["TANK"] + yes["HEAL"] + yes["DPS"] +
                [int(k) for k in (payload.get("maybe") or {}).keys()] +
                (payload.get("no") or [])
            )
            footer = f"🎯 Zielrolle: {r.name} • 📨 DMs: {len(invited)} • Antworten: {len(answered)}"
    emb.set_footer(text=footer)
    return emb

def label_from_member(member: discord.Member) -> str:
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

# ---------------------- DM-Buttons ----------------------
class DMRSVPView(View):
    def __init__(self, guild_id: int, message_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.message_id = str(message_id)  # public message id

    async def _apply(self, inter: discord.Interaction, grp: str):
        if self.message_id not in store:
            await inter.response.send_message("Dieses Event existiert nicht mehr.", ephemeral=True)
            return
        payload = store[self.message_id]
        uid = inter.user.id

        # Reset
        for k in ("TANK","HEAL","DPS"):
            if uid in payload["yes"][k]:
                payload["yes"][k].remove(uid)
        payload["no"] = [u for u in payload["no"] if u != uid]
        payload["maybe"].pop(str(uid), None)

        # Apply
        if grp in ("TANK","HEAL","DPS"):
            payload["yes"][grp].append(uid)
            text = f"Angemeldet als **{grp}**."
        elif grp == "MAYBE":
            payload["maybe"][str(uid)] = label_from_member(inter.user)
            text = "Als **Vielleicht** eingetragen."
        elif grp == "NO":
            payload["no"].append(uid)
            text = "Als **Abgemeldet** eingetragen."
        else:
            text = "Aktualisiert."

        save_store()

        # Öffentliche Übersicht aktualisieren
        guild = inter.client.get_guild(self.guild_id)
        if guild:
            ch = guild.get_channel(payload["channel_id"])
            if isinstance(ch, discord.TextChannel):
                try:
                    msg = await ch.fetch_message(int(self.message_id))
                    await msg.edit(embed=build_public_embed(guild, payload), view=None)
                except Exception as e:
                    print("update public embed failed:", e)

        await inter.response.send_message(text, ephemeral=True)

    @button(label="🛡️ Tank", style=ButtonStyle.secondary, custom_id="dm_tank")
    async def tank(self, inter: discord.Interaction, _):   await self._apply(inter, "TANK")
    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_heal")
    async def heal(self, inter: discord.Interaction, _):   await self._apply(inter, "HEAL")
    @button(label="🗡️ DPS",  style=ButtonStyle.secondary, custom_id="dm_dps")
    async def dps(self, inter: discord.Interaction, _):    await self._apply(inter, "DPS")
    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_maybe")
    async def maybe(self, inter: discord.Interaction, _):  await self._apply(inter, "MAYBE")
    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_no")
    async def no(self, inter: discord.Interaction, _):     await self._apply(inter, "NO")

# ---------------------- Utils ----------------------
def _is_admin(inter: discord.Interaction) -> bool:
    p = getattr(inter.user, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild))

async def _iter_role_members(guild: discord.Guild, role: discord.Role) -> List[discord.Member]:
    cached = list(role.members)
    if cached:
        return cached
    out: List[discord.Member] = []
    try:
        async for m in guild.fetch_members(limit=None):
            if role in m.roles:
                out.append(m)
    except Exception:
        pass
    return out

async def _send_dm_for_event(user: discord.Member, guild_id: int, public_message_id: int, title: str, when_iso: str, description: str):
    try:
        dt = datetime.fromisoformat(when_iso)
        emb = discord.Embed(
            title=f"📩 Anmeldung: {title}",
            description=(description + f"\n\n🕒 {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"),
            color=discord.Color.green()
        )
        view = DMRSVPView(guild_id, public_message_id)
        await user.send(embed=emb, view=view)
        return True
    except discord.Forbidden:
        return False
    except Exception as e:
        print("send dm failed:", e)
        return False

# ---------------------- Setup + Commands ----------------------
async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):

    # Persistente DM-Views re-registrieren
    for msg_id, payload in list(store.items()):
        gid = payload.get("guild_id")
        if gid:
            try:
                client.add_view(DMRSVPView(gid, int(msg_id)))
            except Exception as e:
                print("re-register DM view failed:", e)

    # Standard-Zielrolle speichern (optional)
    @tree.command(name="raid_dm_set_role", description="(Admin) Standard-Zielrolle für DM-Einladungen speichern")
    @app_commands.describe(role="Rolle, die standardmäßig per DM eingeladen wird")
    async def raid_dm_set_role(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        cfg[str(inter.guild_id)] = {"TARGET_ROLE_ID": int(role.id)}
        save_cfg()
        await inter.response.send_message(f"✅ Standard-Zielrolle gespeichert: {role.mention}", ephemeral=True)

    # Raid erstellen – mit optionalem Rollen-Parameter
    @tree.command(name="raid_dm_create", description="(Admin) Raid erstellen – Anmeldung per DM")
    @app_commands.describe(
        title="Titel",
        date="Datum YYYY-MM-DD",
        time="Zeit HH:MM (24h)",
        description="Beschreibung (optional)",
        target_role="Zielrolle (optional; überschreibt Standardrolle)",
        channel="Kanal für die Übersicht (optional; default: aktueller Kanal)",
        image_url="Bild-URL (optional)"
    )
    async def raid_dm_create(
        inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        description: str = "",
        target_role: Optional[discord.Role] = None,
        channel: Optional[discord.TextChannel] = None,
        image_url: Optional[str] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return

        # Datum/Zeit parsen
        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig. Format: YYYY-MM-DD / HH:MM", ephemeral=True); return

        ch = channel or inter.channel
        if not isinstance(ch, discord.TextChannel):
            await inter.response.send_message("❌ Zielkanal ungültig.", ephemeral=True); return

        # Zielrolle bestimmen: Parameter > gespeicherte Standardrolle
        final_role = target_role
        if final_role is None:
            gcfg = cfg.get(str(inter.guild_id)) or {}
            rid = int(gcfg.get("TARGET_ROLE_ID", 0))
            final_role = inter.guild.get_role(rid) if rid else None

        if final_role is None:
            await inter.response.send_message("❌ Keine Zielrolle angegeben/gespeichert. Entweder `target_role` setzen oder vorher `/raid_dm_set_role` ausführen.", ephemeral=True)
            return

        # Öffentliche Übersicht initial posten
        obj = DMEvent(
            guild_id=inter.guild_id,
            channel_id=ch.id,
            title=title.strip(),
            when_iso=when.isoformat(),
            description=(description or "").strip(),
            image_url=(image_url or "").strip() or None,
            yes_tank=[], yes_heal=[], yes_dps=[], maybe={}, no=[],
            public_message_id=None,
            target_role_id=final_role.id,
            invited=[]
        )
        payload = obj.to_dict()
        msg = await ch.send(embed=build_public_embed(inter.guild, payload), view=None)
        payload["public_message_id"] = msg.id
        store[str(msg.id)] = payload
        save_store()

        # DMs senden
        members = await _iter_role_members(inter.guild, final_role)
        sent = 0
        for m in members:
            if m.bot:
                continue
            ok = await _send_dm_for_event(m, inter.guild_id, msg.id, obj.title, obj.when_iso, obj.description)
            if ok:
                payload["invited"].append(m.id)
                sent += 1
        save_store()

        # Übersicht aktualisieren (Footer mit Stats)
        try:
            await msg.edit(embed=build_public_embed(inter.guild, payload))
        except Exception as e:
            print("edit after dm failed:", e)

        await inter.response.send_message(
            f"✅ Raid erstellt: {msg.jump_url}\n🎯 Zielrolle: {final_role.mention}\n📨 DMs verschickt: {sent}",
            ephemeral=True
        )

    # Übersicht neu zeichnen
    @tree.command(name="raid_dm_show", description="Übersicht neu aufbauen (ohne Buttons)")
    @app_commands.describe(message_id="ID der öffentlichen Raid-Nachricht")
    async def raid_dm_show(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        payload = store[message_id]
        ch = inter.guild.get_channel(payload["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(embed=build_public_embed(inter.guild, payload), view=None)
            await inter.response.send_message("✅ Aktualisiert.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    # Übersicht schließen (Server hat keine Buttons, DMs bleiben nutzbar)
    @tree.command(name="raid_dm_close", description="Übersicht sperren (DMs bleiben nutzbar)")
    @app_commands.describe(message_id="ID der öffentlichen Raid-Nachricht")
    async def raid_dm_close(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        ch = inter.guild.get_channel(store[message_id]["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(view=None)
            await inter.response.send_message("🔒 Übersicht gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)
