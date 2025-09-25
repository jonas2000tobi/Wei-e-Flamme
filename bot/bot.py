import os, threading
from flask import Flask

app = Flask(__name__)

@app.get("/")
def ok():
    return "ok"  # Healthcheck

def run_flask():
    # Render setzt PORT als Env-Var für Web Services
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# Webserver im Hintergrund starten
threading.Thread(target=run_flask, daemon=True).start()# TL Event Reminder Discord Bot
# Author: ChatGPT for Jonas
# --- Selbst-Ping, damit Render-Free nicht einschläft ---
import time, requests
def keep_alive():
    while True:
        try:
            requests.get("https://wei-e-flamme.onrender.com")  # deine Render-URL
        except Exception as e:
            print("Self-ping failed:", e)
        time.sleep(300)  # alle 5 Minuten
threading.Thread(target=keep_alive, daemon=True).start()
# --- Ende Selbst-Ping ---

# Features:
# - Slash-Commands, serverweit
# - Pro Event eigener Ziel-Channel (Channel-Auswahl beim Erstellen)
# - Beschreibungstext pro Event
# - Wiederkehrend (Wochentage) ODER einmalig (Datum)
# - Vorab-Erinnerungen X Minuten vorher
# - Zeitzone Europe/Berlin
# - JSON-Persistenz (ohne DB)

import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time, date as date_cls
from pathlib import Path
from typing import Dict, List, Optional, Set

import discord
from discord import app_commands
from discord.ext import tasks
from zoneinfo import ZoneInfo

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TZ = ZoneInfo("Europe/Berlin")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "guild_configs.json"
POST_LOG_FILE = DATA_DIR / "post_log.json"

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
    days = []
    for part in [p.strip().lower() for p in s.split(",") if p.strip()]:
        if part not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{part}'. Use Mon..Sun or 0..6 (0=Mon).")
        days.append(DOW_MAP[part])
    return sorted(set(days))

def parse_time_hhmm(s: str) -> time:
    try:
        hh, mm = s.strip().split(":")
        return time(int(hh), int(mm))
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
        raise ValueError("date must be 'YYYY-MM-DD' if provided.")

# ---- Modelle ----
@dataclass
class Event:
    name: str
    weekdays: List[int]           # 0=Mon..6=Sun
    start_hhmm: str               # "HH:MM"
    duration_min: int
    pre_reminders: List[int]      # Minuten vor Start
    mention_role_id: Optional[int] = None
    channel_id: Optional[int] = None
    description: str = ""
    one_time_date: Optional[str] = None  # "YYYY-MM-DD"

    def next_occurrence_start(self, ref_dt: datetime) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            dt_date = parse_date_yyyy_mm_dd(self.one_time_date)
            dt = datetime.combine(dt_date, start_t, tzinfo=TZ)
            return dt if dt >= ref_dt else None
        today = ref_dt.date()
        for add_days in range(0, 8):
            d = today + timedelta(days=add_days)
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
    events: Dict[str, Event] = None  # key: lower-name

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

# ---- Persistenz ----
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

# ---- Bot Setup ----
intents = discord.Intents.default()
intents.guilds = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

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

# ---- Commands ----
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
            # einmalig
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
    await interaction.response.send_message(f"✅ Event **{ev.name}** angelegt → {when} {ev.start_hhmm}, Dauer {ev.duration_min} Min, Kanal {target}.", ephemeral=True)

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
    msg = "\n".join(lines)
    await interaction.response.send_message(msg[:1990], ephemeral=True)

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

# ---- Scheduler ----
post_log: Set[str] = load_post_log()

@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = datetime.now(TZ).replace(second=0, microsecond=0)
    for guild in client.guilds:
        cfg = configs.get(guild.id)
        if not cfg or not cfg.events:
            continue
        for ev in list(cfg.events.values()):
            # Zielkanal bestimmen
            channel_id = ev.channel_id or cfg.announce_channel_id
            channel = await ensure_text_channel(guild, channel_id)
            if not channel:
                continue

            start_dt = ev.occurrence_start_on_date(now.date())
            if not start_dt:
                continue
            end_dt = start_dt + timedelta(minutes=ev.duration_min)

            # Vorab-Erinnerungen
            for m in ev.pre_reminders:
                pre_dt = start_dt - timedelta(minutes=m)
                if pre_dt == now:
                    key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:pre{m}"
                    if key not in post_log:
                        role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                        body = f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip()
                        if ev.description:
                            body += f"\n{ev.description}"
                        await channel.send(body)
                        post_log.add(key)

            # Start
            if start_dt == now:
                key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
                if key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                    if ev.description:
                        body += f"\n{ev.description}"
                    await channel.send(body)
                    post_log.add(key)

            # Einmalige Events nach dem Tag aufräumen (optional, simpel)
            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        # Event ist vorbei -> löschen
                        del cfg.events[ev.name.lower()]
                        save_all(configs)
                except Exception:
                    pass

    save_post_log(post_log)

@scheduler_loop.before_loop
async def _before_scheduler():
    await client.wait_until_ready()

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")
    try:
        synced = await tree.sync()
        print(f"Synced {len(synced)} commands.")
    except Exception as e:
        print("Command sync failed:", e)
    scheduler_loop.start()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
