from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

try:
    from bot.json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore
except Exception:
    from json_store import load_json_file, save_json_atomic, warn_json_store  # type: ignore

import discord

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FILE = DATA_DIR / "raid_stats.json"

def _default() -> dict:
    return {
        "users": {},
        "events": {}
    }

def _load() -> dict:
    data = load_json_file(FILE, _default(), context=__name__)

    if not isinstance(data, dict):
        return _default()

    users = data.setdefault("users", {})
    for _gid, guild_users in users.items():
        if not isinstance(guild_users, dict):
            continue

        for _uid, bucket in guild_users.items():
            if isinstance(bucket, dict):
                bucket.setdefault("yes", 0)
                bucket.setdefault("bank", 0)
                bucket.setdefault("maybe", 0)
                bucket.setdefault("no", 0)

    data.setdefault("events", {})
    return data

def _save(data: dict) -> None:
    save_json_atomic(FILE, data, context=__name__)

stats = _load()

def _user_bucket(guild_id: int, user_id: int) -> dict:
    users = stats.setdefault("users", {})
    guild_users = users.setdefault(str(guild_id), {})
    user_stats = guild_users.setdefault(str(user_id), {"yes": 0, "bank": 0, "maybe": 0, "no": 0})

    user_stats.setdefault("yes", 0)
    user_stats.setdefault("bank", 0)
    user_stats.setdefault("maybe", 0)
    user_stats.setdefault("no", 0)

    return user_stats

def _event_bucket(guild_id: int, event_id: str) -> dict:
    events = stats.setdefault("events", {})
    guild_events = events.setdefault(str(guild_id), {})
    event_stats = guild_events.setdefault(str(event_id), {})
    return event_stats

def _dec_if_possible(bucket: dict, key: str) -> None:
    if key in bucket and isinstance(bucket[key], int) and bucket[key] > 0:
        bucket[key] -= 1

def record_response(guild_id: int, user_id: int, event_id: str, response: str) -> None:
    """
    Bleibt erhalten, damit bestehende RSVP-Logik weiter funktioniert.
    response = "yes" | "bank" | "maybe" | "no"
    """
    response = str(response).strip().lower()

    if response not in {"yes", "bank", "maybe", "no"}:
        return

    user_bucket = _user_bucket(guild_id, user_id)
    event_bucket = _event_bucket(guild_id, str(event_id))

    user_key = str(user_id)
    old_response = event_bucket.get(user_key)

    if old_response == response:
        return

    if old_response in {"yes", "bank", "maybe", "no"}:
        _dec_if_possible(user_bucket, old_response)

    user_bucket[response] = int(user_bucket.get(response, 0)) + 1
    event_bucket[user_key] = response

    _save(stats)

def get_user_stats(guild_id: int, user_id: int) -> Optional[dict]:
    users = stats.get("users", {})
    guild_users = users.get(str(guild_id), {})
    data = guild_users.get(str(user_id))

    if isinstance(data, dict):
        data.setdefault("yes", 0)
        data.setdefault("bank", 0)
        data.setdefault("maybe", 0)
        data.setdefault("no", 0)

    return data

def get_top_yes_stats(guild_id: int, limit: int = 10) -> List[Tuple[int, int]]:
    users = stats.get("users", {})
    guild_users = users.get(str(guild_id), {})

    ranking: List[Tuple[int, int]] = []

    for uid, data in guild_users.items():
        try:
            ranking.append((int(uid), int(data.get("yes", 0))))
        except Exception:
            continue

    ranking.sort(key=lambda x: x[1], reverse=True)
    return ranking[:limit]

def _entry_user_id(entry: Any) -> int:
    try:
        if isinstance(entry, dict):
            return int(entry.get("id", 0) or 0)
        return int(entry)
    except Exception:
        return 0

def _voters_set(obj: dict) -> set[int]:
    voted: set[int] = set()

    yes = obj.get("yes") or {}
    for key in ("TANK", "HEAL", "DPS", "BANK"):
        for entry in yes.get(key, []) or []:
            uid = _entry_user_id(entry)
            if uid:
                voted.add(uid)

    for entry in obj.get("no", []) or []:
        uid = _entry_user_id(entry)
        if uid:
            voted.add(uid)

    maybe = obj.get("maybe") or {}
    for uid_str, entry in maybe.items():
        try:
            uid = int(uid_str)
        except Exception:
            uid = _entry_user_id(entry)
        if uid:
            voted.add(uid)

    return voted

def _eligible_members(guild: discord.Guild, obj: dict) -> List[discord.Member]:
    tr_id = int(obj.get("target_role_id", 0) or 0)

    if not tr_id:
        return [m for m in guild.members if not m.bot]

    role = guild.get_role(tr_id)

    if not role:
        return [m for m in guild.members if not m.bot]

    return [m for m in role.members if not m.bot]

def get_non_response_stats(
    guild: discord.Guild,
    event_store: dict,
    only_started: bool = True,
) -> List[dict]:
    """
    Zeigt aktuelle Gildenmitglieder, die bei Events nicht abgestimmt haben.

    Ergebnis:
    [
      {
        "user_id": int,
        "name": str,
        "missing": int,
        "events": [str, str, ...]
      }
    ]
    """
    now = datetime.now().astimezone()
    result: Dict[int, dict] = {}

    for _msg_id, obj in list((event_store or {}).items()):
        try:
            if int(obj.get("guild_id", 0) or 0) != guild.id:
                continue

            when = datetime.fromisoformat(obj.get("when_iso"))

            if only_started:
                check_now = datetime.now(when.tzinfo) if when.tzinfo else datetime.now()
                if when > check_now:
                    continue

            title = str(obj.get("title", "Event"))
            voted = _voters_set(obj)
            eligible = _eligible_members(guild, obj)

            for member in eligible:
                if member.id in voted:
                    continue

                bucket = result.setdefault(
                    member.id,
                    {
                        "user_id": member.id,
                        "name": member.display_name,
                        "missing": 0,
                        "events": []
                    }
                )

                bucket["missing"] += 1
                bucket["events"].append(title)

        except Exception:
            continue

    out = list(result.values())
    out.sort(key=lambda x: x["missing"], reverse=True)
    return out
