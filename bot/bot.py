
# TL Event Reminder Discord Bot
# Author: ChatGPT for Jonas
# Python 3.11+ recommended
#
# Features:
# - Slash commands to configure event reminders per server
# - Reminds when an event starts and can also ping X minutes before start
# - Timezone-aware (Europe/Berlin by default)
# - Simple JSON persistence (no external DB)
#
# Commands (server admin only):
# /set_announce_channel <#channel>
# /add_event name:<text> weekdays:<text> start_time:<text> duration_min:<int> pre_reminders:<text> mention_role:<role optional>
#    - weekdays: comma list, e.g. "Mon,Wed,Sat" or "0,3,5" (0=Mon...6=Sun)
#    - start_time: "HH:MM" 24h, server local (Europe/Berlin)
#    - pre_reminders: comma minutes list like "30,10,5" (optional, can be empty)
# /list_events
# /remove_event name:<text>
# /test_event_ping name:<text>
#
# How it works:
# A background task runs every 30s, checks the schedule and posts to the configured channel.
# Duplicate prevention ensures one post per event occurrence and reminder moment.

import os
import asyncio
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Set

import discord
from discord import app_commands
from discord.ext import tasks
from zoneinfo import ZoneInfo

TOKEN = os.getenv("DISCORD_BOT_TOKEN")  # put your token in the environment
TZ = ZoneInfo("Europe/Berlin")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "guild_configs.json"
POST_LOG_FILE = DATA_DIR / "post_log.json"  # to avoid duplicates

# ---- Models ----
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
    days = []
    for part in [p.strip().lower() for p in s.split(",") if p.strip()]:
        if part not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{part}'. Use Mon..Sun or 0..6 (0=Mon).")
        days.append(DOW_MAP[part])
    days = sorted(set(days))
    if not days:
        raise ValueError("No weekdays parsed.")
    return days

def parse_time_hhmm(s: str) -> time:
    try:
        hh, mm = s.strip().split(":")
        return time(int(hh), int(mm))
    except Exception:
        raise ValueError("start_time must be 'HH:MM' 24h.")

def parse_premins(s: str) -> List[int]:
    if not s.strip():
        return []
    mins = []
    for part in [p.strip() for p in s.split(",") if p.strip()]:
        mins.append(int(part))
    mins = sorted(set([m for m in mins if m > 0]))
    return mins

@dataclass
class Event:
    name: str
    weekdays: List[int]  # 0=Mon..6=Sun
    start_hhmm: str      # "HH:MM"
    duration_min: int
    pre_reminders: List[int]  # minutes before start
    mention_role_id: Optional[int] = None

    def next_occurrence_start(self, ref_dt: datetime) -> datetime:
        """Return the next start datetime >= ref_dt in TZ."""
        today = ref_dt.date()
        start_t = parse_time_hhmm(self.start_hhmm)
        for add_days in range(0, 8):
            d = today + timedelta(days=add_days)
            dow = d.weekday()
            if dow in self.weekdays:
                dt = datetime.combine(d, start_t, tzinfo=TZ)
                if dt >= ref_dt:
                    return dt
        d = today + timedelta(days=7)
        return datetime.combine(d, start_t, tzinfo=TZ)

    def occurrence_start_on_date(self, date_) -> Optional[datetime]:
        if date_.weekday() not in self.weekdays:
            return None
        start_t = parse_time_hhmm(self.start_hhmm)
        return datetime.combine(date_, start_t, tzinfo=TZ)

@dataclass
class GuildConfig:
    guild_id: int
    announce_channel_id: Optional[int] = None
    events: Dict[str, Event] = None  # key by lower-name

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

# ---- Persistence ----
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
intents.guild_messages = True
intents.message_content = False  # not needed for slash commands
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

async def ensure_channel(guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.TextChannel]:
    if channel_id is None:
        return None
    ch = guild.get_channel(channel_id)
    if isinstance(ch, discord.TextChannel):
        return ch
    return None

# ---- Permissions helper ----
def is_admin(interaction: discord.Interaction) -> bool:
    if interaction.user is None:
        return False
    perms = interaction.user.guild_permissions
    return perms.administrator or perms.manage_guild

# ---- Commands ----
@tree.command(name="set_announce_channel", description="Set the channel for event reminders.")
@app_commands.describe(channel="Target text channel")
async def set_announce_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Du brauchst Administrator- oder Server-Verwaltungsrechte.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    cfg.announce_channel_id = channel.id
    save_all(configs)
    await interaction.response.send_message(f"✅ Announce-Channel auf {channel.mention} gesetzt.", ephemeral=True)

@tree.command(name="add_event", description="Add a recurring T&L event.")
@app_commands.describe(
    name="Event-Name (z. B. Siege, Dimensional Rift)",
    weekdays="Kommagetrennt: z. B. Mon,Wed,Sat oder 0,3,5 (0=Mon..6=Sun)",
    start_time="Start 'HH:MM' (Europa/Berlin)",
    duration_min="Dauer in Minuten",
    pre_reminders="Minuten vor Start, kommagetrennt (z. B. 30,10,5). Leer lassen, wenn keine Vorab-Pings.",
    mention_role="(Optional) Rolle, die gepingt werden soll"
)
async def add_event(
    interaction: discord.Interaction,
    name: str,
    weekdays: str,
    start_time: str,
    duration_min: int,
    pre_reminders: str = "",
    mention_role: Optional[discord.Role] = None
):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Du brauchst Administrator- oder Server-Verwaltungsrechte.", ephemeral=True)
        return
    try:
        days = parse_weekdays(weekdays)
        t = parse_time_hhmm(start_time)
        pre = parse_premins(pre_reminders or "")
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
    )
    cfg.events[name.lower()] = ev
    save_all(configs)
    await interaction.response.send_message(f"✅ Event **{name}** hinzugefügt: {ev.weekdays} {ev.start_hhmm} Dauer {duration_min} Min.", ephemeral=True)

@tree.command(name="list_events", description="List all configured events.")
async def list_events(interaction: discord.Interaction):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if not cfg.events:
        await interaction.response.send_message("ℹ️ Keine Events konfiguriert.", ephemeral=True)
        return
    lines = []
    dow_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    for ev in cfg.events.values():
        days = ",".join(dow_names[d] for d in ev.weekdays)
        role = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else "—"
        pre = ", ".join(str(m) for m in ev.pre_reminders) if ev.pre_reminders else "—"
        lines.append(f"• **{ev.name}** — {days} {ev.start_hhmm} ({ev.duration_min} Min), Pre: {pre}, Role: {role}")
    content = "\n".join(lines)
    if len(content) > 1900:
        content = content[:1900]
    await interaction.response.send_message(content or "—", ephemeral=True)

@tree.command(name="remove_event", description="Remove an event by name.")
async def remove_event(interaction: discord.Interaction, name: str):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Du brauchst Administrator- oder Server-Verwaltungsrechte.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if name.lower() in cfg.events:
        del cfg.events[name.lower()]
        save_all(configs)
        await interaction.response.send_message(f"✅ Event **{name}** entfernt.", ephemeral=True)
    else:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)

@tree.command(name="test_event_ping", description="Test ping for a given event (no schedule check).")
async def test_event_ping(interaction: discord.Interaction, name: str):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    ev = cfg.events.get(name.lower())
    if not ev:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
        return
    channel = await ensure_channel(interaction.guild, cfg.announce_channel_id)
    if not channel:
        await interaction.response.send_message("❌ Announce-Channel nicht gesetzt. Benutze /set_announce_channel.", ephemeral=True)
        return
    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
    await channel.send(f"🔔 **{ev.name}** — Test-Ping {role_mention}".strip())
    await interaction.response.send_message("✅ Test-Ping gesendet.", ephemeral=True)

# ---- Background Task ----
@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = datetime.now(TZ).replace(second=0, microsecond=0)
    for guild in client.guilds:
        cfg = configs.get(guild.id)
        if not cfg or not cfg.announce_channel_id or not cfg.events:
            continue
        channel = await ensure_channel(guild, cfg.announce_channel_id)
        if not channel:
            continue
        for ev in cfg.events.values():
            start_dt = ev.occurrence_start_on_date(now.date())
            if not start_dt:
                continue
            end_dt = start_dt + timedelta(minutes=ev.duration_min)

            # Pre-reminders
            for m in ev.pre_reminders:
                pre_dt = start_dt - timedelta(minutes=m)
                if pre_dt == now:
                    key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:pre{m}"
                    if key not in post_log:
                        role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                        await channel.send(f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip())
                        post_log.add(key)

            # Start announcement
            if start_dt == now:
                key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
                if key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    await channel.send(f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip())
                    post_log.add(key)

    # Persist post log periodically
    save_post_log(post_log)

@scheduler_loop.before_loop
async def before_scheduler():
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
