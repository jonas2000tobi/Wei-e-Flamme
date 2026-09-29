from __future__ import annotations

import re
from typing import Any

# Eine Quelle für alle Aion-2-Klassen im Bot. Deutsch = UI/Profilname,
# Englisch = Discord-Emoji-/Assetname.
AION2_CLASS_META: dict[str, tuple[str, str, str]] = {
    "Templer": ("Templar", "TANK", "Templar"),
    "Gladiator": ("Gladiator", "DPS", "Gladiator"),
    "Assassine": ("Assassin", "DPS", "Assassin"),
    "Jäger": ("Ranger", "DPS", "Ranger"),
    "Zauberer": ("Sorcerer", "DPS", "Sorcerer"),
    "Geisterbeschwörer": ("Elementalist", "DPS", "Elementalist"),
    "Kleriker": ("Cleric", "SUPPORT", "Cleric"),
    "Kantor": ("Chanter", "SUPPORT", "Chanter"),
}

AION2_CLASS_ALIASES: dict[str, str] = {
    "Beschwörer": "Geisterbeschwörer",
    "Spiritmaster": "Geisterbeschwörer",
    "Spirit Master": "Geisterbeschwörer",
    "Elementalist": "Geisterbeschwörer",
}

AION2_CLASSES = tuple(AION2_CLASS_META.keys())

AION2_FACTIONS: dict[str, tuple[str, str]] = {
    "ELYOS": ("Elyos", "Elyos"),
    "ASMODIA": ("Asmodia", "Asmodia"),
}


def normalize_class(value: Any) -> str:
    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if raw in AION2_CLASS_META:
        return raw
    if raw in AION2_CLASS_ALIASES:
        return AION2_CLASS_ALIASES[raw]
    folded = raw.casefold()
    for name, (english, _role, _emoji) in AION2_CLASS_META.items():
        if folded in {name.casefold(), english.casefold()}:
            return name
    return ""


def role_for_class(value: Any) -> str:
    name = normalize_class(value)
    return str((AION2_CLASS_META.get(name) or ("", "", ""))[1])


def role_label(value: Any) -> str:
    role = str(value or "").strip().upper()
    if role in {"HEAL", "HEALER", "SUPPORT"}:
        role = "SUPPORT"
    return {"TANK": "Tank", "SUPPORT": "Support", "DPS": "DPS"}.get(role, role or "Nicht gesetzt")


def normalize_faction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "ASMODIAN": "ASMODIA",
        "ASMODIANS": "ASMODIA",
        "ASMODIER": "ASMODIA",
    }
    raw = aliases.get(raw, raw)
    return raw if raw in AION2_FACTIONS else ""
