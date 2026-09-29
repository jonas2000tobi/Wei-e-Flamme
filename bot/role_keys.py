from __future__ import annotations

from typing import Any

CANONICAL_EVENT_ROLES = ("TANK", "SUPPORT", "DPS", "BANK")
CANONICAL_PRIMARY_ROLES = ("TANK", "SUPPORT", "DPS")


def normalize_role_key(value: Any, *, allow_status: bool = True) -> str:
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


def role_label(value: Any) -> str:
    key = normalize_role_key(value)
    return {
        "TANK": "Tank",
        "SUPPORT": "Support",
        "DPS": "DPS",
        "BANK": "Reserve",
        "MAYBE": "Vielleicht",
        "NO": "Abgemeldet",
    }.get(key, key or "—")


def normalize_yes_buckets(value: Any) -> dict[str, list[Any]]:
    source = value if isinstance(value, dict) else {}
    out: dict[str, list[Any]] = {key: [] for key in CANONICAL_EVENT_ROLES}
    for raw_key, entries in source.items():
        key = normalize_role_key(raw_key, allow_status=False)
        if key not in out or not isinstance(entries, list):
            continue
        out[key].extend(entries)
    return out
