from __future__ import annotations
import json
from pathlib import Path
from typing import Dict

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PREF_FILE = DATA_DIR / "event_dm_prefs.json"


def _load() -> Dict[str, Dict[str, bool]]:
    try:
        return json.loads(PREF_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: Dict[str, Dict[str, bool]]) -> None:
    PREF_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


prefs: Dict[str, Dict[str, bool]] = _load()


def is_dm_enabled(guild_id: int, user_id: int) -> bool:
    guild_prefs = prefs.get(str(guild_id), {})
    return bool(guild_prefs.get(str(user_id), True))


def set_dm_pref(guild_id: int, user_id: int, enabled: bool) -> None:
    guild_key = str(guild_id)
    user_key = str(user_id)

    guild_prefs = prefs.setdefault(guild_key, {})
    guild_prefs[user_key] = bool(enabled)
    _save(prefs)
