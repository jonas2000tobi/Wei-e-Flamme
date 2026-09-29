from __future__ import annotations

import re
from typing import Any

# Dashboard_Web läuft bei Railway mit /dashboard_web als eigenem Root-Verzeichnis.
# Deshalb darf dieses Modul zur Laufzeit NICHT von ../bot abhängen.
# Die Definitionen hier spiegeln bot/aion2_game.py und halten den Dashboard-Service
# vollständig eigenständig deploybar.
_AION2_CLASS_META_RAW: dict[str, tuple[str, str, str]] = {
    "Templer": ("Templar", "TANK", "Templar"),
    "Gladiator": ("Gladiator", "DPS", "Gladiator"),
    "Assassine": ("Assassin", "DPS", "Assassin"),
    "Jäger": ("Ranger", "DPS", "Ranger"),
    "Zauberer": ("Sorcerer", "DPS", "Sorcerer"),
    "Geisterbeschwörer": ("Elementalist", "DPS", "Elementalist"),
    "Kleriker": ("Cleric", "SUPPORT", "Cleric"),
    "Kantor": ("Chanter", "SUPPORT", "Chanter"),
}

_AION2_CLASS_ALIASES: dict[str, str] = {
    "Beschwörer": "Geisterbeschwörer",
    "Spiritmaster": "Geisterbeschwörer",
    "Spirit Master": "Geisterbeschwörer",
    "Elementalist": "Geisterbeschwörer",
}

_AION2_FACTIONS_RAW: dict[str, tuple[str, str]] = {
    "ELYOS": ("Elyos", "Elyos"),
    "ASMODIA": ("Asmodia", "Asmodia"),
}


def normalize_class(value: Any) -> str:
    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if raw in _AION2_CLASS_META_RAW:
        return raw
    if raw in _AION2_CLASS_ALIASES:
        return _AION2_CLASS_ALIASES[raw]
    folded = raw.casefold()
    for name, (english, _role, _emoji) in _AION2_CLASS_META_RAW.items():
        if folded in {name.casefold(), english.casefold()}:
            return name
    return ""


def role_for_class(value: Any) -> str:
    name = normalize_class(value)
    return str((_AION2_CLASS_META_RAW.get(name) or ("", "", ""))[1])


def role_label(value: Any) -> str:
    role = str(value or "").strip().upper()
    if role in {"HEAL", "HEALER", "HEILER", "SUPPORT"}:
        role = "SUPPORT"
    return {"TANK": "Tank", "SUPPORT": "Support", "DPS": "DPS"}.get(
        role, role or "Nicht gesetzt"
    )


def normalize_faction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    aliases = {
        "ASMODIAN": "ASMODIA",
        "ASMODIANS": "ASMODIA",
        "ASMODIER": "ASMODIA",
    }
    raw = aliases.get(raw, raw)
    return raw if raw in _AION2_FACTIONS_RAW else ""


def _asset_name(english_name: str) -> str:
    return f"aion2/{str(english_name or '').strip().lower().replace(' ', '_')}.png"


AION2_CLASS_META: dict[str, dict[str, str]] = {
    german: {
        "en": english,
        "role": role,
        "emoji": emoji,
        "asset": _asset_name(english),
    }
    for german, (english, role, emoji) in _AION2_CLASS_META_RAW.items()
}

AION2_FACTION_META: dict[str, dict[str, str]] = {
    key: {
        "label": label,
        "emoji": emoji,
        "asset": f"aion2/{key.lower()}.png",
    }
    for key, (label, emoji) in _AION2_FACTIONS_RAW.items()
}

# Fextralife blockiert fremde iframes (X-Frame-Options/CSP). IMapp ist die
# einbettbare Alternative; der externe Link bleibt separat.
AION2_MAP_DIRECT_URL = "https://interactivemap.app/aion2/maps/verteron"
AION2_MAP_EMBED_URL = AION2_MAP_DIRECT_URL + "?embed=light"


def class_asset(value: Any) -> str:
    name = normalize_class(value)
    return str((AION2_CLASS_META.get(name) or {}).get("asset") or "")


def faction_asset(value: Any) -> str:
    key = normalize_faction(value)
    return str((AION2_FACTION_META.get(key) or {}).get("asset") or "")
