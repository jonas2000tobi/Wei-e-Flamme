# bot/event_rsvp_dm.py
# DM-basiertes RSVP für Raids. Erstellt eine öffentliche Übersicht (ohne Buttons)
# und verschickt DMs an alle Mitglieder der Zielrolle. Antworten per DM-Buttons
# aktualisieren die Übersicht live.
#
# Erfordert Intents.members = True.

from __future__ import annotations
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple, Set

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

@dataclass
class DMEvent:
    guild_id: int
    channel_id: int
    title: str
    when_iso: str
    description: str = ""
    image_url: Optional[str] = None
    # Übersicht
    yes_tank: List[int] = None
    yes_heal: List[int] = None
    yes_dps:  List[int] = None
    maybe: Dict[str, str] = None  # uid -> label (Tank/Heal/DPS/"")
    no: List[int] = None
    # Technisch
    public_message_id: Optional[int] = None
    target_role_id: Optional[int] = None   # wen wir per DM anschreiben
    invited: List[int] = None              # wem DM geschickt wurde

    def to_dict(self):
        return {
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "title": self.title,
            "when_iso": self.when_iso,
            "description": self.description,
            "image_url": self.image_url,
            "yes": {
                "TANK": self.yes_tank or [],
                "HEAL": self.yes_heal or [],
                "DPS":  self.yes_dps or [],
            },
            "maybe": self.maybe or {},
            "no": self.no or [],
            "public_message_id": self.public_message_id,
            "target_role_id": self.target_role_id,
            "invited": self.invited or [],
        }

    @staticmethod
    def from_dict(d: dict) -> "DMEvent":
        yes = d.get("yes") or {"TANK": [], "HEAL": [], "DPS": []}
        ev = DMEvent(
            guild_id=d["guild_id"],
            channel_id=d["channel_id"],
            title=d["title"],
            when_iso=d["when_iso"],
            description=d.get("description",""),
            image_url=d.get("image_url"),
            yes_tank=yes.get("TANK", []),
            yes_heal=yes.get("HEAL", []),
            yes_dps=yes.get("DPS",  []),
            maybe=d.get("maybe", {}),
            no=d.get("no", []),
            public_message_id=d.get("public_message_id"),
            target_role_id=d.get("target_role_id"),
            invited=d.get("invited", []),
        )
        return ev

# --------- Embed (öffentliche Übersicht, ohne Server-Buttons) ----------
def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def build_public_embed(guild: discord.Guild, payload: dict) -> discord.Embed:
    dt = datetime.fromisoformat(payload["when_iso"])
    emb = discord.Embed(
        title=f"📅 {payload['title']}",
        description=(payload.get("description","") + 
                     f"\n\n🕒 Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"),
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

    role_info = ""
    rid = payload.get("target_role_id") or 0
    if rid:
        r = guild.get_role(int(rid))
        if r:
            invited = set(payload.get("invited") or [])
            answered = set(
                yes["TANK"] + yes["HEAL"] + yes["DPS"] +
                [int(k) for k in (payload.get("maybe") or {}).keys()] +
                (payload.get("no") or [])
            )
            role_info = f"🎯 Zielrolle: {r.mention}\n📨 DMs: {len(invited)} verschickt • Antworten: {len(answered)}"
    if role_info:
        emb.set_footer(text=role_info)
    else:
        emb.set_footer(text="Antworten kommen per DM; Übersicht aktualisiert sich automatisch.")
    return emb

# --------- DM-View (Buttons in privaten Nachrichten) ----------
def label_from_member(member: discord.Member) -> str:
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

class DMRSVPView(View):
    def __init__(self, guild_id: int, message_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.message_id = str(message_id)  # public message id als Schlüssel im store

    async def _apply(self, inter: discord.Interaction, grp: str):
        if self.message_id not in store:
            await inter.response.send_message("Dieses Event existiert nicht mehr.", ephemeral=True)
            return
        payload = store[self.message_id]
        uid = inter.user.id

        # Alle Buckets bereinigen
        for k in ("TANK", "HEAL", "DPS"):
            if uid in payload["yes"][k]:
                payload["yes"][k].remove(uid)
        payload["no"] = [u for u in payload["no"] if u != uid]
        payload["maybe"].pop(str(uid), None)

        # Eintragen
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
                    await msg.edit(embed=build_public_embed(guild, payload), view=None)  # keine Buttons im Server
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

# --------- Hilfen ----------
def _is_admin(inter: discord.Interaction) -> bool:
    m = inter.user
    return bool(m and (m.guild_permissions.administrator or m.guild_permissions.manage_guild))

async def _send_dm_for_event(client: discord.Client, guild: discord.Guild, user: discord.Member, public_message_id: int, title: str, when_iso: str, description: str):
    try:
        dt = datetime.fromisoformat(when_iso)
        emb = discord.Embed(
            title=f"📩 Anmeldung: {title}",
            description=(description + f"\n\n🕒 {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"),
            color=discord.Color.green()
        )
        view = DMRSVPView(guild.id, public_message_id)
        await user.send(embed=emb, view=view)
        return True
    except discord.Forbidden:
        # DMs zu -> ignoriere
        return False
    except Exception as e:
        print("send dm failed:", e)
        return False

async def _iter_role_members(guild: discord.Guild, role: discord.Role) -> List[discord.Member]:
    # cached members:
    out = list(role.members)
    if out:
        return out
    # fallback fetch
    out2: List[discord.Member] = []
    try:
        async for m in guild.fetch_members(limit=None):
            if role in m.roles:
                out2.append(m)
    except Exception:
        pass
    return out2

# --------- Setup + Commands ----------
async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):

    # Persistente DM-Views wieder registrieren
    for msg_id, payload in list(store.items()):
        guild = client.get_guild(payload.get("guild_id", 0))
        if guild:
            try:
                client.add_view(DMRSVPView(guild.id, int(msg_id)))
            except Exception as e:
                print("re-register DM view failed:", e)

    # Zielrolle setzen (wen per DM anschreiben)
    @tree.command(name="raid_dm_set_role", description="(Admin) Zielrolle für DM-Einladungen setzen")
    @app_commands.describe(role="Rolle, die per DM eingeladen wird (z. B. Gildenrolle oder Raid-Gruppe)")
    async def raid_dm_set_role(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        cfg[str(inter.guild_id)] = {"TARGET_ROLE_ID": int(role.id)}
        save_cfg()
        await inter.response.send_message(f"✅ Zielrolle für DM-Einladungen gesetzt: {role.mention}", ephemeral=True)

    # Raid erstellen (ohne Server-Buttons), DMs verschicken
    @tree.command(name="raid_dm_create", description="(Admin) Raid erstellen – Anmeldung nur per DM")
    @app_commands.describe(
        title="Titel",
        date="Datum YYYY-MM-DD",
        time="Zeit HH:MM (24h)",
        channel="Kanal für die Übersicht (standard: aktueller Kanal)",
        image_url="Optionales Bild fürs Embed"
    )
    async def raid_dm_create(
        inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        channel: Optional[discord.TextChannel] = None,
        image_url: Optional[str] = None
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        # Datum/Zeit parsen
        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig. Format: YYYY-MM-DD / HH:MM", ephemeral=True)
            return

        ch = channel or inter.channel
        if not isinstance(ch, discord.TextChannel):
            await inter.response.send_message("❌ Zielkanal ungültig.", ephemeral=True)
            return

        # Zielrolle ermitteln
        gcfg = cfg.get(str(inter.guild_id)) or {}
        target_role_id = int(gcfg.get("TARGET_ROLE_ID", 0))
        target_role = inter.guild.get_role(target_role_id) if target_role_id else None
        if not target_role:
            await inter.response.send_message("❌ Keine Zielrolle gesetzt. Nutze zuerst /raid_dm_set_role.", ephemeral=True)
            return

        # Öffentliche Übersicht initial posten
        obj = DMEvent(
            guild_id=inter.guild_id,
            channel_id=ch.id,
            title=title.strip(),
            when_iso=when.isoformat(),
            description="",
            image_url=(image_url or "").strip() or None,
            yes_tank=[], yes_heal=[], yes_dps=[],
            maybe={}, no=[],
            public_message_id=None,
            target_role_id=target_role.id,
            invited=[]
        )
        payload = obj.to_dict()
        embed = build_public_embed(inter.guild, payload)
        msg = await ch.send(embed=embed, view=None)  # keine Buttons im Server
        payload["public_message_id"] = msg.id
        store[str(msg.id)] = payload
        save_store()

        # DM an Zielrolle
        members = await _iter_role_members(inter.guild, target_role)
        sent = 0
        for m in members:
            if m.bot:
                continue
            ok = await _send_dm_for_event(inter.client, inter.guild, m, msg.id, title, when.isoformat(), obj.description)
            if ok:
                payload["invited"].append(m.id)
                sent += 1
        save_store()

        # Übersicht updaten (Footer mit DM/Antwortzahlen)
        try:
            await msg.edit(embed=build_public_embed(inter.guild, payload))
        except Exception as e:
            print("edit after dm failed:", e)

        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}\n📨 DMs verschickt: {sent}", ephemeral=True)

    # Öffentliche Übersicht neu zeichnen (falls nötig)
    @tree.command(name="raid_dm_show", description="Übersicht neu aufbauen (ohne Buttons)")
    @app_commands.describe(message_id="ID der öffentlichen Raidanzeige")
    async def raid_dm_show(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        payload = store[message_id]
        ch = inter.guild.get_channel(payload["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(embed=build_public_embed(inter.guild, payload), view=None)
            await inter.response.send_message("✅ Aktualisiert.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    # Event schließen (DM-Buttons bleiben in DMs sichtbar, aber Übersicht bleibt)
    @tree.command(name="raid_dm_close", description="Server-Übersicht sperren (keine Server-Buttons vorhanden).")
    @app_commands.describe(message_id="ID der öffentlichen Raidanzeige")
    async def raid_dm_close(inter: discord.Interaction, message_id: str):
        if message_id not in store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        ch = inter.guild.get_channel(store[message_id]["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(view=None)
            await inter.response.send_message("🔒 Übersicht gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)
