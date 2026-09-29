from __future__ import annotations

from typing import Any

# Dashboard_Web wird auf Railway als eigener Root (/dashboard_web -> /app)
# deployed. Daher keine Runtime-Abhängigkeit auf ../bot/role_keys.py.
CANONICAL_EVENT_ROLES = ("TANK", "SUPPORT", "DPS", "BANK")


def _normalize_role_key(value: Any, *, allow_status: bool = True) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "HEAL": "SUPPORT",
        "HEALER": "SUPPORT",
        "HEILER": "SUPPORT",
        "SUP": "SUPPORT",
        "RESERVE": "BANK",
    }
    raw = aliases.get(raw, raw)
    allowed = set(CANONICAL_EVENT_ROLES)
    if allow_status:
        allowed.update({"MAYBE", "NO", "MANUAL", ""})
    return raw if raw in allowed else raw


def normalize_event_role(value: Any) -> str:
    return _normalize_role_key(value)


def event_role_label(value: Any) -> str:
    key = _normalize_role_key(value)
    return {
        "TANK": "Tank",
        "SUPPORT": "Support",
        "DPS": "DPS",
        "BANK": "Reserve",
        "MAYBE": "Vielleicht",
        "NO": "Abgemeldet",
    }.get(key, key or "—")


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
    return {"TANK": 0, "SUPPORT": 1, "DPS": 2, "BANK": 3}.get(
        normalize_event_role(value), 20
    )
