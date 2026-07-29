from __future__ import annotations

"""Dauerhafte Eventbilder für Bot und Dashboard.

Die fünf Standardbilder werden vom Dashboard selbst ausgeliefert. Dadurch
laufen keine Discord-Signaturen mehr ab. Eigene externe URLs bleiben weiterhin
möglich, wenn ausdrücklich ``image_type=custom`` gewählt wurde.
"""

import os
from typing import Any, Mapping
from urllib.parse import urlparse

DEFAULT_DASHBOARD_PUBLIC_BASE_URL = "https://dashboardweb-production-2933.up.railway.app"
EVENT_IMAGE_ASSET_VERSION = "2.3.6"

EVENT_IMAGE_ASSETS: dict[str, str] = {
    "normal_raid": "normal_raid.webp",
    "hard_raid": "hard_raid.webp",
    "trials": "trials.webp",
    "pvp": "pvp.webp",
    "guild_boss": "guild_boss.webp",
}

# Öffentliche Dashboard-URLs für Discord-Embeds.
EVENT_IMAGE_URLS: dict[str, str] = {
    "guild_boss": f"{DEFAULT_DASHBOARD_PUBLIC_BASE_URL}/static/event_images/guild_boss.webp?v={EVENT_IMAGE_ASSET_VERSION}",
    "normal_raid": f"{DEFAULT_DASHBOARD_PUBLIC_BASE_URL}/static/event_images/normal_raid.webp?v={EVENT_IMAGE_ASSET_VERSION}",
    "hard_raid": f"{DEFAULT_DASHBOARD_PUBLIC_BASE_URL}/static/event_images/hard_raid.webp?v={EVENT_IMAGE_ASSET_VERSION}",
    "trials": f"{DEFAULT_DASHBOARD_PUBLIC_BASE_URL}/static/event_images/trials.webp?v={EVENT_IMAGE_ASSET_VERSION}",
    "pvp": f"{DEFAULT_DASHBOARD_PUBLIC_BASE_URL}/static/event_images/pvp.webp?v={EVENT_IMAGE_ASSET_VERSION}",
}

DISPLAY_PRESET_KEYS: dict[str, str] = {
    "Normal Mode Raid": "normal_raid",
    "Hardmode Raid": "hard_raid",
    "Dimensionsprüfung": "trials",
    "Gildenbosse": "guild_boss",
    "Segensstein": "pvp",
}

_PRESET_ALIASES: dict[str, str] = {
    "normal": "normal_raid",
    "normal raid": "normal_raid",
    "normal mode": "normal_raid",
    "normal mode raid": "normal_raid",
    "normal_raid": "normal_raid",
    "nm raid": "normal_raid",
    "hard": "hard_raid",
    "hard raid": "hard_raid",
    "hard mode": "hard_raid",
    "hard mode raid": "hard_raid",
    "hardmode raid": "hard_raid",
    "hard_raid": "hard_raid",
    "hm raid": "hard_raid",
    "trial": "trials",
    "trials": "trials",
    "dimensionsprüfung": "trials",
    "dimensionspruefung": "trials",
    "pvp": "pvp",
    "segensstein": "pvp",
    "segensstein pvp": "pvp",
    "boss": "guild_boss",
    "gildenboss": "guild_boss",
    "gildenbosse": "guild_boss",
    "guild boss": "guild_boss",
    "guild_boss": "guild_boss",
}

_NO_IMAGE_KEYS = {"none", "kein bild", "no image", "off", "disabled"}
_CUSTOM_IMAGE_KEYS = {"custom", "eigene url", "external", "extern"}

# Alte, bereits gespeicherte Discord-Anhangslinks werden zusätzlich anhand
# stabiler URL-Bestandteile erkannt.
LEGACY_URL_MARKERS: dict[str, str] = {
    # Aktuelle Discord-Nachrichten / Dateinamen
    "1528469820738375882": "guild_boss",
    "Gildenbosse.png": "guild_boss",
    "1528469819865956352": "normal_raid",
    "0d9886d3-f6f5-4f3e-b926-1f86151dc84d": "normal_raid",
    "1529804865679786085": "hard_raid",
    "7225f274-cc4f-4eda-ba74-ca401f4e572b": "hard_raid",
    "1530963146020356246": "trials",
    "8b34c404-89eb-4259-9e6e-acbcc1def2b2": "trials",
    "1528469820310290493": "pvp",
    "4a00e277-c3b6-48b0-ac7e-ad3e3722aaf1": "pvp",
    # Ältere gespeicherte Links
    "1516086614957494312": "normal_raid",
    "282b2b20-5a8f-4251-b038-15fde2ac723d": "normal_raid",
    "1513816935832228033": "hard_raid",
    "1491660359952502825": "trials",
    "file_000000007dcc7246bb6e57ae41860769": "trials",
    "1513202292302811186": "pvp",
    "1780845919107": "pvp",
}


def dashboard_public_base_url() -> str:
    for name in ("DASHBOARD_PUBLIC_BASE_URL", "DASHBOARD_BASE_URL", "DASHBOARD_URL"):
        value = str(os.getenv(name) or "").strip().rstrip("/")
        if value.startswith(("https://", "http://")):
            return value
    return DEFAULT_DASHBOARD_PUBLIC_BASE_URL


def event_image_asset_path(preset_key: str) -> str:
    key = str(preset_key or "").strip().lower()
    if key not in EVENT_IMAGE_ASSETS:
        return ""
    return f"/event-image/{key}?v={EVENT_IMAGE_ASSET_VERSION}"


def event_image_asset_url(preset_key: str) -> str:
    key = str(preset_key or "").strip().lower()
    filename = EVENT_IMAGE_ASSETS.get(key)
    if not filename:
        return ""
    return f"{dashboard_public_base_url()}/static/event_images/{filename}?v={EVENT_IMAGE_ASSET_VERSION}"


def preset_urls_by_display_name() -> dict[str, str]:
    return {label: event_image_asset_url(key) for label, key in DISPLAY_PRESET_KEYS.items()}


def is_discord_attachment_url(url: str) -> bool:
    value = str(url or "").strip()
    try:
        host = (urlparse(value).hostname or "").lower()
        path = (urlparse(value).path or "").lower()
    except Exception:
        return False
    return host in {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
    } and ("/attachments/" in path or "/ephemeral-attachments/" in path)


def stable_external_image_url(url: str) -> str:
    """Behält eine gültige externe Bild-URL exakt bei.

    Discord-Anhangslinks können signierte Query-Parameter benötigen. Deshalb
    werden eigene und vorkonfigurierte URLs nicht mehr automatisch gekürzt.
    """
    value = str(url or "").strip()
    if not value.startswith(("https://", "http://")):
        return ""
    return value


def legacy_preset_key(url: str) -> str:
    value = str(url or "")
    for marker, preset_key in LEGACY_URL_MARKERS.items():
        if marker in value:
            return preset_key
    return ""


def _selected_image_mode(event: Mapping[str, Any]) -> str:
    return str(event.get("image_type") or event.get("image_preset") or "").strip().casefold()


def _selected_preset_key(event: Mapping[str, Any]) -> str:
    mode = _selected_image_mode(event)
    return _PRESET_ALIASES.get(mode, mode if mode in EVENT_IMAGE_ASSETS else "")


def infer_preset_key(event: Mapping[str, Any] | None) -> str:
    event = event or {}
    selected = _selected_preset_key(event)
    if selected:
        return selected

    values: list[str] = []
    for key in ("dkp_event_type", "event_type", "title", "name", "description"):
        value = event.get(key)
        if value is not None:
            values.append(str(value))
    text = " ".join(values).casefold().replace("-", " ").replace("_", " ")

    if any(term in text for term in ("segensstein", "boonstone", "pvp")):
        return "pvp"
    if any(term in text for term in ("trial", "dimensionsprüfung", "dimensionspruefung", "prüfung", "pruefung")):
        return "trials"
    if any(term in text for term in ("hardmode", "hard mode", "hard raid", "hm raid")):
        return "hard_raid"
    if any(term in text for term in ("gildenboss", "gildenbosse", "guild boss", "guildboss")):
        return "guild_boss"
    if any(term in text for term in ("normalmodus", "normal mode", "normal raid", "nm raid")):
        return "normal_raid"
    return ""


def normalize_event_image_url(
    event: Mapping[str, Any] | None,
    raw_url: str = "",
    *,
    absolute: bool = True,
    infer_when_missing: bool = False,
) -> str:
    """Gibt eine dauerhafte Bild-URL zurück.

    Für bekannte Eventtypen wird immer das feste lokale Bild verwendet. Damit
    werden auch bereits laufende Events repariert, deren alte Bild-URL zwar noch
    gespeichert ist, aber inzwischen nicht mehr geladen werden kann.
    """
    event = event or {}
    raw = str(raw_url or "").strip()
    if not raw:
        for key in ("image_url", "banner_url", "thumbnail_url", "media_url", "event_image_url"):
            candidate = str(event.get(key) or "").strip()
            if candidate:
                raw = candidate
                break

    mode = _selected_image_mode(event)
    if mode in _NO_IMAGE_KEYS:
        return ""

    legacy_key = legacy_preset_key(raw)
    # Alte Standardbilder wurden teilweise als "custom" gespeichert. Nur echte
    # fremde URLs bleiben custom; bekannte Standardlinks werden lokal repariert.
    if mode in _CUSTOM_IMAGE_KEYS and not legacy_key:
        return stable_external_image_url(raw)

    preset_key = infer_preset_key(event) or legacy_key
    if preset_key:
        return event_image_asset_url(preset_key) if absolute else event_image_asset_path(preset_key)

    # Nur benutzerdefinierte/unbekannte Alt-Events behalten ihre externe URL.
    if not mode and raw:
        return stable_external_image_url(raw)

    if raw.startswith("/event-image/"):
        path = raw.split("?", 1)[0]
        stable = f"{path}?v={EVENT_IMAGE_ASSET_VERSION}"
        return f"{dashboard_public_base_url()}{stable}" if absolute else stable

    if raw.startswith("/static/event_images/"):
        filename = raw.split("?", 1)[0].rsplit("/", 1)[-1]
        key = next((k for k, v in EVENT_IMAGE_ASSETS.items() if v == filename), "")
        if key:
            return event_image_asset_url(key) if absolute else event_image_asset_path(key)

    if raw.startswith(("https://", "http://")):
        try:
            parsed = urlparse(raw)
            if str(parsed.path or "").startswith("/event-image/"):
                path = str(parsed.path)
                stable = f"{path}?v={EVENT_IMAGE_ASSET_VERSION}"
                return f"{dashboard_public_base_url()}{stable}" if absolute else stable
            if str(parsed.path or "").startswith("/static/event_images/"):
                filename = str(parsed.path).rsplit("/", 1)[-1]
                key = next((k for k, v in EVENT_IMAGE_ASSETS.items() if v == filename), "")
                if key:
                    return event_image_asset_url(key) if absolute else event_image_asset_path(key)
        except Exception:
            pass
        return stable_external_image_url(raw)

    if infer_when_missing and preset_key:
        return event_image_asset_url(preset_key) if absolute else event_image_asset_path(preset_key)
    return ""
