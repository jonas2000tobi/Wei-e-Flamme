# bot/bot.py
# TL Event Reminder + Raid/RSVP in EINER Datei
# discord.py 2.4.x

from __future__ import annotations
import os, json, threading, time, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time as time_cls, date as date_cls
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict

import discord
from discord import app_commands
from discord.ext import tasks
from flask import Flask
from zoneinfo import ZoneInfo

# ========= Grundkonfiguration =========
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TZ = ZoneInfo("Europe/Berlin")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "guild_configs.json"
POST_LOG_FILE = DATA_DIR / "post_log.json"

# ---- Mini-Webserver für Railway/Healthcheck ----
app = Flask(__name__)

@app.get("/")
def ok():
    return "ok"

def _run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

threading.Thread(target=_run_flask, daemon=True).start()

# ---- Optionales Self-Ping (Free-Pläne wach halten) ----
def keep_alive():
    url = os.getenv("KEEPALIVE_URL", "").strip()  # z.B. https://wei-e-flamme.up.railway.app
    if not url:
        return
    while True:
        try:
            requests.get(url, timeout=10)
        except Exception as e:
            print("Self-ping failed:", e)
        time.sleep(300)

threading.Thread(target=keep_alive, daemon=True).start()

# ========= Discord Setup =========
intents = discord.Intents.default()
intents.guilds = True
intents.members = True         # im Dev-Portal aktivieren (Privileged Intents)
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ========= Event-Reminder (dein ursprünglicher Bot) =========

# ---- Parsing ----
DOW_MAP = {
    "mon": 0, "monday": 0, "0": 0,
    "tue": 1, "tuesday": 1, "1": 1,
    "wed": 2, "wednesday": 2, "2": 2,
    "thu": 3, "thursday": 3, "3": 3,
    "fri": 4, "friday": 4, "4": 4,
    "sat": 5, "saturday": 5, "5": 5,
    "sun": 6, "sunday": 6, "6": 6,
}

def parse_weekdays(s: str) -> List[int]:
    if not s:
        return []
    out = []
    for p in [x.strip().lower() for x in s.split(",") if x.strip()]:
        if p not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{p}'. Use Mon..Sun or 0..6 (0=Mon).")
        out.append(DOW_MAP[p])
    return sorted(set(out))

def parse_time_hhmm(s: str) -> time_cls:
    try:
        hh, mm = s.strip().split(":")
        return time_cls(int(hh), int(mm))
    except Exception:
        raise ValueError("start_time must be 'HH:MM' 24h.")

def parse_premins(s: str) -> List[int]:
    if not s or not s.strip():
        return []
    mins = []
    for part in [p.strip() for p in s.split(",") if p.strip()]:
        mins.append(int(part))
    return sorted(set([m for m in mins if m > 0]))

def parse_date_yyyy_mm_dd(s: str) -> date_cls:
    try:
        y, m, d = s.strip().split("-")
        return date_cls(int(y), int(m), int(d))
    except Exception:
        raise ValueError("date must be 'YYYY-MM-DD'.")

# ---- Modelle ----
@dataclass
class Event:
    name: str
    weekdays: List[int]           # 0=Mon..6=Sun
    start_hhmm: str               # "HH:MM"
    duration_min: int
    pre_reminders: List[int]
    mention_role_id: Optional[int] = None
    channel_id: Optional[int] = None
    description: str = ""
    one_time_date: Optional[str] = None  # "YYYY-MM-DD"

    def next_occurrence_start(self, ref_dt: datetime) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            d = parse_date_yyyy_mm_dd(self.one_time_date)
            dt = datetime.combine(d, start_t, tzinfo=TZ)
            return dt if dt >= ref_dt else None
        today = ref_dt.date()
        for add in range(0, 8):
            d = today + timedelta(days=add)
            if d.weekday() in self.weekdays:
                dt = datetime.combine(d, start_t, tzinfo=TZ)
                if dt >= ref_dt:
                    return dt
        return None

    def occurrence_start_on_date(self, date_) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            d = parse_date_yyyy_mm_dd(self.one_time_date)
            if d != date_:
                return None
            return datetime.combine(d, start_t, tzinfo=TZ)
        if date_.weekday() not in self.weekdays:
            return None
        return datetime.combine(date_, start_t, tzinfo=TZ)

@dataclass
class GuildConfig:
    guild_id: int
    announce_channel_id: Optional[int] = None
    events: Dict[str, Event] = None

    def to_dict(self):
        return {
            "guild_id": self.guild_id,
            "announce_channel_id": self.announce_channel_id,
            "events": {k: asdict(v) for k, v in (self.events or {}).items()},
        }

    @staticmethod
    def from_dict(d):
        evs = {k: Event(**v) for k, v in (d.get("events") or {}).items()}
        return GuildConfig(
            guild_id=d["guild_id"],
            announce_channel_id=d.get("announce_channel_id"),
            events=evs,
        )

# ---- Persistenz (Reminder) ----
def load_all() -> Dict[int, GuildConfig]:
    if CFG_FILE.exists():
        raw = json.loads(CFG_FILE.read_text(encoding="utf-8"))
        return {int(gid): GuildConfig.from_dict(cfg) for gid, cfg in raw.items()}
    return {}

def save_all(cfgs: Dict[int, GuildConfig]):
    raw = {str(gid): cfg.to_dict() for gid, cfg in cfgs.items()}
    CFG_FILE.write_text(json.dumps(raw, indent=2), encoding="utf-8")

def load_post_log() -> Set[str]:
    if POST_LOG_FILE.exists():
        return set(json.loads(POST_LOG_FILE.read_text(encoding="utf-8")))
    return set()

def save_post_log(log: Set[str]):
    POST_LOG_FILE.write_text(json.dumps(sorted(list(log))), encoding="utf-8")

configs: Dict[int, GuildConfig] = load_all()
post_log: Set[str] = load_post_log()

def get_or_create_guild_cfg(guild_id: int) -> GuildConfig:
    cfg = configs.get(guild_id)
    if not cfg:
        cfg = GuildConfig(guild_id=guild_id, events={})
        configs[guild_id] = cfg
        save_all(configs)
    return cfg

async def ensure_text_channel(guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.TextChannel]:
    if not channel_id:
        return None
    ch = guild.get_channel(channel_id)
    return ch if isinstance(ch, discord.TextChannel) else None

def is_admin(interaction: discord.Interaction) -> bool:
    perms = interaction.user.guild_permissions if interaction.user else None
    return bool(perms and (perms.administrator or perms.manage_guild))

# ---- Slash-Commands (Reminder) ----
@tree.command(name="set_announce_channel", description="Standard-Kanal für Erinnerungen setzen.")
@app_commands.describe(channel="Ziel-Textkanal")
async def set_announce_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    cfg.announce_channel_id = channel.id
    save_all(configs)
    await interaction.response.send_message(f"✅ Standard-Kanal: {channel.mention}", ephemeral=True)

@tree.command(name="add_event", description="Event anlegen (wiederkehrend ODER einmalig).")
@app_commands.describe(
    name="Event-Name",
    weekdays='Kommagetrennt: "Mon,Wed,Sat" oder "0,3,5" (0=Mon..6=Sun). Ignoriert, wenn date gesetzt ist.',
    start_time='Start "HH:MM" (Europa/Berlin)',
    duration_min="Dauer in Minuten",
    pre_reminders='Vorab-Minuten, z. B. "30,10,5" (optional)',
    mention_role="(optional) Rolle, die gepingt werden soll",
    post_channel="(optional) Kanal für dieses Event (sonst Standard)",
    description="(optional) Zusatztext unter der Erinnerung",
    date='(optional) Einmalig: Datum "YYYY-MM-DD" – ignoriert weekdays',
)
async def add_event(
    interaction: discord.Interaction,
    name: str,
    weekdays: str,
    start_time: str,
    duration_min: int,
    pre_reminders: str = "",
    mention_role: Optional[discord.Role] = None,
    post_channel: Optional[discord.TextChannel] = None,
    description: str = "",
    date: str = "",
):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return

    try:
        t = parse_time_hhmm(start_time)
        pre = parse_premins(pre_reminders or "")
        one_time_date = None
        days: List[int] = []

        if date.strip():
            one_time_date = parse_date_yyyy_mm_dd(date.strip()).isoformat()
        else:
            days = parse_weekdays(weekdays)
            if not days:
                await interaction.response.send_message("❌ Entweder 'weekdays' angeben ODER 'date' setzen.", ephemeral=True)
                return
    except ValueError as e:
        await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        return

    cfg = get_or_create_guild_cfg(interaction.guild_id)
    ev = Event(
        name=name.strip(),
        weekdays=days,
        start_hhmm=f"{t.hour:02d}:{t.minute:02d}",
        duration_min=duration_min,
        pre_reminders=pre,
        mention_role_id=(mention_role.id if mention_role else None),
        channel_id=(post_channel.id if post_channel else None),
        description=(description or "").strip(),
        one_time_date=one_time_date,
    )
    cfg.events[name.lower()] = ev
    save_all(configs)

    when = one_time_date if one_time_date else ",".join(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d] for d in ev.weekdays)
    target = post_channel.mention if post_channel else (interaction.guild.get_channel(cfg.announce_channel_id).mention if cfg.announce_channel_id else "—")
    await interaction.response.send_message(
        f"✅ Event **{ev.name}** angelegt → {when} {ev.start_hhmm}, Dauer {ev.duration_min} Min, Kanal {target}.",
        ephemeral=True
    )

@tree.command(name="list_events", description="Alle Events anzeigen.")
async def list_events(interaction: discord.Interaction):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if not cfg.events:
        await interaction.response.send_message("ℹ️ Keine Events konfiguriert.", ephemeral=True)
        return
    lines = []
    dow_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    for ev in cfg.events.values():
        when = ev.one_time_date if ev.one_time_date else ",".join(dow_names[d] for d in ev.weekdays)
        role = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else "—"
        chan = f"<#{ev.channel_id}>" if ev.channel_id else (f"<#{cfg.announce_channel_id}>" if cfg.announce_channel_id else "—")
        pre = ", ".join(str(m) for m in ev.pre_reminders) if ev.pre_reminders else "—"
        desc = (ev.description[:60] + "…") if ev.description and len(ev.description) > 60 else (ev.description or "—")
        lines.append(f"• **{ev.name}** — {when} {ev.start_hhmm} ({ev.duration_min} Min), Pre: {pre}, Role: {role}, Channel: {chan}, Desc: {desc}")
    await interaction.response.send_message("\n".join(lines)[:1990], ephemeral=True)

@tree.command(name="remove_event", description="Event löschen.")
async def remove_event(interaction: discord.Interaction, name: str):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if name.lower() in cfg.events:
        del cfg.events[name.lower()]
        save_all(configs)
        await interaction.response.send_message(f"✅ Event **{name}** gelöscht.", ephemeral=True)
    else:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)

@tree.command(name="test_event_ping", description="Test-Ping (keine Planung).")
async def test_event_ping(interaction: discord.Interaction, name: str):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    ev = cfg.events.get(name.lower())
    if not ev:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
        return
    channel = await ensure_text_channel(interaction.guild, ev.channel_id or cfg.announce_channel_id)
    if not channel:
        await interaction.response.send_message("❌ Kein Zielkanal. Setze /set_announce_channel oder nutze post_channel beim Event.", ephemeral=True)
        return
    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
    body = f"🔔 **{ev.name}** — Test-Ping {role_mention}".strip()
    if ev.description:
        body += f"\n{ev.description}"
    await channel.send(body)
    await interaction.response.send_message("✅ Test-Ping raus.", ephemeral=True)

# ---- Scheduler (Reminder) ----
@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = datetime.now(TZ).replace(second=0, microsecond=0)
    changed = False
    for guild in client.guilds:
        cfg = configs.get(guild.id)
        if not cfg or not cfg.events:
            continue
        for ev in list(cfg.events.values()):
            channel_id = ev.channel_id or cfg.announce_channel_id
            channel = await ensure_text_channel(guild, channel_id)
            if not channel:
                continue

            start_dt = ev.occurrence_start_on_date(now.date())
            if not start_dt:
                continue
            end_dt = start_dt + timedelta(minutes=ev.duration_min)

            # Pre-Reminders
            for m in ev.pre_reminders:
                pre_dt = start_dt - timedelta(minutes=m)
                key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:pre{m}"
                if pre_dt == now and key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    body = f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip()
                    if ev.description:
                        body += f"\n{ev.description}"
                    await channel.send(body)
                    post_log.add(key)
                    changed = True

            # Start
            key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
            if start_dt == now and key not in post_log:
                role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                if ev.description:
                    body += f"\n{ev.description}"
                await channel.send(body)
                post_log.add(key)
                changed = True

            # Einmalige Events nach dem Tag aufräumen
            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        del cfg.events[ev.name.lower()]
                        save_all(configs)
                except Exception:
                    pass

    if changed:
        save_post_log(post_log)

@scheduler_loop.before_loop
async def _before_scheduler():
    await client.wait_until_ready()

# ========= Raid/RSVP (mit Bild, Buttons & Persistenz) =========

RSVP_STORE_FILE = DATA_DIR / "event_rsvp.json"
RSVP_CFG_FILE   = DATA_DIR / "event_rsvp_cfg.json"

def _load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

# store Struktur:
# { "<msg_id>": {"guild_id":int,"channel_id":int,"title":str,"when_iso":str,"image_url":str|None,
#                "yes":{"TANK":[uid],"HEAL":[uid],"DPS":[uid]}, "maybe":{"<uid>":"Tank/Heal/DPS"}, "no":[uid] } }
rsvp_store: Dict[str, dict] = _load_json(RSVP_STORE_FILE, {})
rsvp_cfg: Dict[str, dict]   = _load_json(RSVP_CFG_FILE, {})   # pro guild: {"TANK":role_id, ...}

def _save_store():
    _save_json(RSVP_STORE_FILE, rsvp_store)

def _save_cfg():
    _save_json(RSVP_CFG_FILE, rsvp_cfg)

def _get_role_ids(guild: discord.Guild) -> Dict[str, int]:
    g = rsvp_cfg.get(str(guild.id)) or {}
    return {"TANK": int(g.get("TANK", 0)), "HEAL": int(g.get("HEAL", 0)), "DPS": int(g.get("DPS", 0))}

def _label_from_member(member: discord.Member) -> str:
    rid = _get_role_ids(member.guild)
    # 1) IDs (stabil)
    if rid["TANK"] and discord.utils.get(member.roles, id=rid["TANK"]):
        return "Tank"
    if rid["HEAL"] and discord.utils.get(member.roles, id=rid["HEAL"]):
        return "Heal"
    if rid["DPS"] and discord.utils.get(member.roles, id=rid["DPS"]):
        return "DPS"
    # 2) Fallback per Name
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def _build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    dt = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
    title=f"{obj['title']}",
    description=f"{obj.get('description','')}\n\n⏰ Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
    color=discord.Color.blurple(),
)

    # YES nach Rollen
    tank_names = [_mention(guild, u) for u in obj["yes"]["TANK"]]
    heal_names = [_mention(guild, u) for u in obj["yes"]["HEAL"]]
    dps_names  = [_mention(guild, u) for u in obj["yes"]["DPS"]]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})", value="\n".join(dps_names) or "—", inline=True)

    # MAYBE
    maybe_lines = []
    for uid, rlab in obj["maybe"].items():
        uid_i = int(uid)
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    # NO
    no_names = [_mention(guild, u) for u in obj["no"]]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])
    emb.set_footer(text="Klicke unten auf die Buttons, um dich anzumelden.")
    return emb

class RaidView(discord.ui.View):
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)   # persistent
        self.msg_id = str(msg_id)

    async def _update(self, interaction: discord.Interaction, group: str):
        if self.msg_id not in rsvp_store:
            await interaction.response.send_message("Dieses Event ist nicht mehr vorhanden.", ephemeral=True)
            return

        obj = rsvp_store[self.msg_id]
        uid = interaction.user.id

        # aus allen Buckets entfernen
        for k in ("TANK","HEAL","DPS"):
            if uid in obj["yes"][k]:
                obj["yes"][k].remove(uid)
        obj["no"] = [u for u in obj["no"] if u != uid]
        obj["maybe"].pop(str(uid), None)

        # hinzufügen
        if group in ("TANK","HEAL","DPS"):
            obj["yes"][group].append(uid)
            txt = f"Angemeldet als **{group}**."
        elif group == "MAYBE":
            obj["maybe"][str(uid)] = _label_from_member(interaction.user)
            txt = "Als **Vielleicht** eingetragen."
        elif group == "NO":
            obj["no"].append(uid)
            txt = "Als **Abgemeldet** eingetragen."
        else:
            txt = "Aktualisiert."

        _save_store()

        # Nachricht aktualisieren
        guild = interaction.guild
        emb = _build_embed(guild, obj)
        ch = guild.get_channel(obj["channel_id"])
        try:
            msg = await ch.fetch_message(int(self.msg_id))
            await msg.edit(embed=emb, view=self)
        except Exception:
            pass
        await interaction.response.send_message(txt, ephemeral=True)

    @discord.ui.button(label="Tank", style=discord.ButtonStyle.secondary, emoji="🛡️", custom_id="rsvp_tank")
    async def btn_tank(self, interaction: discord.Interaction, _):
        await self._update(interaction, "TANK")

    @discord.ui.button(label="Heal", style=discord.ButtonStyle.secondary, emoji="💚", custom_id="rsvp_heal")
    async def btn_heal(self, interaction: discord.Interaction, _):
        await self._update(interaction, "HEAL")

    @discord.ui.button(label="DPS", style=discord.ButtonStyle.secondary, emoji="🗡️", custom_id="rsvp_dps")
    async def btn_dps(self, interaction: discord.Interaction, _):
        await self._update(interaction, "DPS")

    @discord.ui.button(label="Vielleicht", style=discord.ButtonStyle.secondary, emoji="❔", custom_id="rsvp_maybe")
    async def btn_maybe(self, interaction: discord.Interaction, _):
        await self._update(interaction, "MAYBE")

    @discord.ui.button(label="Abmelden", style=discord.ButtonStyle.danger, emoji="❌", custom_id="rsvp_no")
    async def btn_no(self, interaction: discord.Interaction, _):
        await self._update(interaction, "NO")

def register_rsvp_slash_commands():
    @tree.command(name="raid_set_roles", description="Rollen für Tank/Heal/DPS festlegen (pro Server).")
    @app_commands.describe(tank_role="Rolle für Tank", heal_role="Rolle für Heal", dps_role="Rolle für DPS")
    async def raid_set_roles(inter: discord.Interaction, tank_role: discord.Role, heal_role: discord.Role, dps_role: discord.Role):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        rsvp_cfg[str(inter.guild_id)] = {"TANK": tank_role.id, "HEAL": heal_role.id, "DPS": dps_role.id}
        _save_cfg()
        await inter.response.send_message(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

    @tree.command(name="raid_create", description="Raid-/Event-Anmeldung mit Buttons erstellen.")
    @app_commands.describe(
        title="Titel (im Embed)",
        description="Zusätzliche Info oder Beschreibung"   # <--- NEU
        date="Datum YYYY-MM-DD (Europe/Berlin)",
        time="Zeit HH:MM (24h)",
        channel="Zielkanal",
        image_url="Optionales Bild-URL fürs Embed"
    )
    async def raid_create(
    inter: discord.Interaction,
    title: str,
    description: Optional[str] = None,   # <— NEU
    date: str,
    time: str,
    channel: Optional[discord.TextChannel] = None,
    image_url: Optional[str] = None,

    ):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig (YYYY-MM-DD / HH:MM).", ephemeral=True)
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
            "no": []
        }
        emb = _build_embed(inter.guild, obj)
        view = RaidView(0)
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)

        rsvp_store[str(msg.id)] = obj
        _save_store()

        # persistent view registrieren (über Neustarts)
        client.add_view(RaidView(msg.id), message_id=msg.id)

        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}", ephemeral=True)

    @tree.command(name="raid_show", description="Embed/Listen neu aufbauen.")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_show(inter: discord.Interaction, message_id: str):
        if message_id not in rsvp_store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        obj = rsvp_store[message_id]
        emb = _build_embed(inter.guild, obj)
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
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        if message_id not in rsvp_store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True)
            return
        ch = inter.guild.get_channel(rsvp_store[message_id]["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(view=None)
            await inter.response.send_message("🔒 Gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

def reregister_persistent_views_on_start():
    # alle offenen RSVP-Events wieder anklemmen (Buttons funktionieren nach Neustart)
    for msg_id, obj in list(rsvp_store.items()):
        g = client.get_guild(obj["guild_id"])
        if not g:
            continue
        try:
            client.add_view(RaidView(int(msg_id)), message_id=int(msg_id))
        except Exception as e:
            print("add_view failed:", e)

# ========= on_ready: ALLES hier registrieren =========
@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    # RSVP: Buttons + Slash-Commands registrieren & persistente Views anklemmen
    reregister_persistent_views_on_start()
    register_rsvp_slash_commands()

    # globale Slash-Commands syncen
    try:
        synced = await tree.sync()
        print(f"Synced {len(synced)} commands.")
    except Exception as e:
        print("Command sync failed:", e)

    # Reminder-Scheduler starten
    scheduler_loop.start()

# ========= Start =========
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
