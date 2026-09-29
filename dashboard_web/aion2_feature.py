from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Dashboard und Bot benutzen dieselbe Spieldefinition. Das verhindert, dass
# Klassen/Rollen zwischen den beiden Railway-Services auseinanderlaufen.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.aion2_game import (  # noqa: E402
    AION2_CLASS_META as _BOT_CLASS_META,
    AION2_FACTIONS as _BOT_FACTIONS,
    normalize_class,
    normalize_faction,
    role_for_class,
    role_label,
)


def _asset_name(english_name: str) -> str:
    return f"aion2/{str(english_name or '').strip().lower().replace(' ', '_')}.png"


AION2_CLASS_META: dict[str, dict[str, str]] = {
    german: {
        "en": english,
        "role": role,
        "emoji": emoji,
        "asset": _asset_name(english),
    }
    for german, (english, role, emoji) in _BOT_CLASS_META.items()
}

AION2_FACTION_META: dict[str, dict[str, str]] = {
    key: {
        "label": label,
        "emoji": emoji,
        "asset": f"aion2/{key.lower()}.png",
    }
    for key, (label, emoji) in _BOT_FACTIONS.items()
}

# Fextralife blockiert fremde iframes (X-Frame-Options/CSP). IMapp ist die
# aktuell verwendete, einbettbare Alternative; der externe Link bleibt separat.
AION2_MAP_DIRECT_URL = "https://interactivemap.app/aion2/maps/verteron"
AION2_MAP_EMBED_URL = AION2_MAP_DIRECT_URL + "?embed=light"


def class_asset(value: Any) -> str:
    name = normalize_class(value)
    return str((AION2_CLASS_META.get(name) or {}).get("asset") or "")


def faction_asset(value: Any) -> str:
    key = normalize_faction(value)
    return str((AION2_FACTION_META.get(key) or {}).get("asset") or "")
