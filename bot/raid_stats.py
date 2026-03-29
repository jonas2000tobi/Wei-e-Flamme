from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FILE = DATA_DIR / "raid_stats.json"


def _default() -> dict:
    return {
        "users": {},   # users[guild_id][user_id] = {"yes":0,"maybe":0,"no":0}
        "events": {}   # events[guild_id][event_id][user_id] = "yes"|"maybe"|"no"
    }


def _load() -> dict:
    try:
        return json.loads(FILE.read_text(encoding="utf-8"))
    except Exception:
        return _default()


def _save(data: dict) -> None:
    FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


stats = _load()


def _user_bucket(guild_id: int, user_id: int) -> dict:
    users = stats.setdefault("users", {})
    guild_users = users.setdefault(str(guild_id), {})
    user_stats = guild_users.setdefault(str(user_id), {"yes": 0, "maybe": 0, "no": 0})
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
    response = "yes" | "maybe" | "no"
    Pro Event zählt immer nur der aktuelle Status eines Users.
    Wenn er von maybe -> yes wechselt, wird maybe runter und yes hochgezählt.
    """
    response = str(response).strip().lower()
    if response not in {"yes", "maybe", "no"}:
        return

    user_bucket = _user_bucket(guild_id, user_id)
    event_bucket = _event_bucket(guild_id, str(event_id))

    user_key = str(user_id)
    old_response = event_bucket.get(user_key)

    if old_response == response:
        return

    if old_response in {"yes", "maybe", "no"}:
        _dec_if_possible(user_bucket, old_response)

    user_bucket[response] = int(user_bucket.get(response, 0)) + 1
    event_bucket[user_key] = response

    _save(stats)


def get_user_stats(guild_id: int, user_id: int) -> Optional[dict]:
    users = stats.get("users", {})
    guild_users = users.get(str(guild_id), {})
    return guild_users.get(str(user_id))


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
