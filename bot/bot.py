# TL Event Reminder Discord Bot
# Author: ChatGPT for Jonas
# Python 3.11+ recommended
#
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
        # wiederkehrend
        today = ref_dt.date()
        for add_days in range(0, 8):
            d = today + timedelta(days=add_days)
            if d.weekday() in self.weekdays:
                dt = datetime.combine(d, start_t, tzinfo=TZ)
                if dt >= ref_dt:
                    return dt
        return datetime.combine(today + timedelta(days=7), start_t, tzinfo=TZ)

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
