from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Eine kanonische Rollenlogik für Bot + Dashboard. Legacy HEAL/HEALER wird nur
# noch beim Einlesen akzeptiert und sofort als SUPPORT behandelt.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.role_keys import (  # noqa: E402
    CANONICAL_EVENT_ROLES,
    normalize_role_key,
    role_label,
)


def normalize_event_role(value: Any) -> str:
    return normalize_role_key(value)


def event_role_label(value: Any) -> str:
    return role_label(value)


def role_bucket(value: Any) -> str:
    txt = str(value or "").strip().lower()
    if any(x in txt for x in ("tank", "wächter", "waechter")):
        return "Tank"
    if any(x in txt for x in ("support", "heal", "heiler")):
        return "Support"
    if any(x in txt for x in ("dps", "dd", "damage", "schaden")):
        return "DPS"
    if any(x in txt for x in ("reserve", "bank")):
        return "Reserve"
    return str(value or "Andere") or "Andere"


def role_order(value: Any) -> int:
    return {"TANK": 0, "SUPPORT": 1, "DPS": 2, "BANK": 3}.get(normalize_event_role(value), 20)
