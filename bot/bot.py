from pathlib import Path
from textwrap import dedent

base = "/mnt/data/tl_event_bot_updated"
Path(base).mkdir(exist_ok=True)

bot_code = dedent('''\
# TL Event Reminder Discord Bot
# Author: ChatGPT for Jonas
# Python 3.11+ recommended
#
# Features:
# - Slash commands to configure event reminders per server
# - Per-event target channel override (choose the channel when creating the event)
# - Optional description text included in reminder messages
# - Supports recurring weekly events or a one-time date
# - Reminds when an event starts and can also ping X minutes before start
# - Timezone-aware (Europe/Berlin by default)
# - Simple JSON persistence (no external DB)
#
# Commands (server admin only):
# /set_announce_channel <#channel>
# /add_event name:<text> weekdays:<text> start_time:<text> duration_min:<int> pre_reminders:<text> mention_role:<role optional> post_channel:<#channel optional> description:<text optional> date:<YYYY-MM-DD optional>
#    - weekdays: comma list, e.g. "Mon,Wed,Sat" or "0,3,5" (0=Mon...6=Sun) — ignored if 'date' is set
#    - start_time: "HH:MM" 24h, server local (Europe/Berlin)
#    - pre_reminders: comma minutes list like "30,10,5" (optional, can be empty)
#    - post_channel: channel to post this event's reminders in (fallback = announce channel)
#    - description: extra text appended to messages
#    - date: if provided, this is a one-time event on that date (YYYY-MM-DD), ignoring weekdays
# /list_events
# /remove_event name:<text>
# /test_event_ping name:<text>
#
# How it works:
# A background task runs every 30s, checks the schedule and posts to the configured channel.
# Duplicate prevention ensures one post per event occurrence and reminder moment.

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
    if not s:
        return []
    days = []
    for part in [p.strip().lower() for p in s.split(",") if p.strip()]:
        if part not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{part}'. Use Mon..Sun or 0..6 (0=Mon).")
        days.append(DOW_MAP[part])
    days = sorted(set(days))
    return days

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
    mins = sorted(set([m for m in mins if m > 0]))
    return mins

def parse_date_yyyy_mm_dd(s: str) -> date_cls:
    try:
        y, m, d = s.strip().split("-")
        return date_cls(int(y), int(m), int(d))
    except Exception:
        raise ValueError("date must be 'YYYY-MM-DD' if provided.")

@dataclass
class Event:
    name: str
    weekdays: List[int]  # 0=Mon..6=Sun
    start_hhmm: str      # "HH:MM"
    duration_min: int
    pre_reminders: List[int]  # minutes before start
    mention_role_id: Optional[int] = None
    channel_id: Optional[int] = None
    description: str = ""
    one_time_date: Optional[str] = None  # "YYYY-MM-DD"

    def next_occurrence_start(self, ref_dt: datetime) -> Optional[datetime]:
        """Return the next start datetime >= ref_dt in TZ, or None if one-time date is in the past."""
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            dt_date = parse_date_yyyy_mm_dd(self.one_time_date)
            dt = datetime.combine(dt_date, start_t, tzinfo=TZ)
            return dt if dt >= ref_dt else None
        # recurring
        today = ref_dt.date()
        for add_days in range(0, 8):
            d = today + timedelta(days=add_days)
            dow = d.weekday()
            if dow in self.weekdays:
                dt = datetime.combine(d, start_t, tzinfo=TZ)
                if dt >= ref_dt:
                    return dt
        # fallback next week
        d = today + timedelta(days=7)
        return datetime.combine(d, start_t, tzinfo=TZ)

    def occurrence_start_on_date(self, date_) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            # only trigger on that date
            try:
                d = parse_date_yyyy_mm_dd(self.one_time_date)
            except ValueError:
                return None
            if d != date_:
                return None
            return datetime.combine(d, start_t, tzinfo=TZ)
        # recurring
        if date_.weekday() not in self.weekdays:
            return None
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

def load_post_log()) -> Set[str]:
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
    if
