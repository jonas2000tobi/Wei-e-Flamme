from __future__ import annotations

"""Stabile Eventbild-URLs für Discord-Bot und Dashboard.

Discord-Anhangslinks mit ``ex/is/hm`` sind signiert und laufen ab. Deshalb liegen
unsere Standardbilder im Dashboard unter ``/static/event_images``. Der Bot nutzt
absolute URLs auf dieselben Dateien, damit Discord-Embeds ebenfalls dauerhaft
funktionieren.
"""

import os
from typing import Any, Mapping
from urllib.parse import urlparse

DEFAULT_DASHBOARD_PUBLIC_BASE_URL = "https://dashboardweb-production-2933.up.railway.app"
EVENT_IMAGE_ASSET_VERSION = "2.2.2"

EVENT_IMAGE_ASSETS: dict[str, str] = {
    "normal_raid": "normal_raid.webp",
    "hard_raid": "hard_raid.webp",
    "trials": "trials.webp",
    "nightmare": "nightmare.webp",
    "pvp": "pvp.webp",
    "guild_boss": "guild_boss.webp",
}

DISPLAY_PRESET_KEYS: dict[str, str] = {
    "Normal Mode Raid": "normal_raid",
    "Hardmode Raid": "hard_raid",
    "Dimensionsprüfung": "trials",
    "Gildenbosse": "guild_boss",
    "Segensstein": "pvp",
}

# Alte, bereits gespeicherte Discord-Anhangslinks werden anhand stabiler Teile
# erkannt. Die Query-Parameter selbst dürfen nicht weiterverwendet werden.
LEGACY_URL_MARKERS: dict[str, str] = {
    "1516086614957494312": "normal_raid",
    "282b2b20-5a8f-4251-b038-15fde2ac723d": "normal_raid",
    "1513816935832228033": "hard_raid",
    "7225f274-cc4f-4eda-ba74-ca401f4e572b": "hard_raid",
    "1513816992358858842": "nightmare",
    "d6ee8bc1-432a-4d28-914d-31be80adf835": "nightmare",
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
    filename = EVENT_IMAGE_ASSETS.get(str(preset_key or "").strip().lower(), "")
    return f"/static/event_images/{filename}?v={EVENT_IMAGE_ASSET_VERSION}" if filename else ""


def event_image_asset_url(preset_key: str) -> str:
    path = event_image_asset_path(preset_key)
    return f"{dashboard_public_base_url()}{path}" if path else ""


def preset_urls_by_display_name() -> dict[str, str]:
    return {label: event_image_asset_url(key) for label, key in DISPLAY_PRESET_KEYS.items()}


def is_discord_attachment_url(url: str) -> bool:
    value = str(url or "").strip()
    try:
        host = (urlparse(value).hostname or "").lower()
    except Exception:
        return False
    return host in {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
    } and "/attachments/" in value


def legacy_preset_key(url: str) -> str:
    value = str(url or "")
    for marker, preset_key in LEGACY_URL_MARKERS.items():
        if marker in value:
            return preset_key
    return ""


def infer_preset_key(event: Mapping[str, Any] | None) -> str:
    event = event or {}
    values: list[str] = []
    for key in (
        "image_preset",
        "image_type",
        "dkp_event_type",
        "event_type",
        "title",
        "name",
        "description",
    ):
        value = event.get(key)
        if value is not None:
            values.append(str(value))
    text = " ".join(values).casefold().replace("-", " ").replace("_", " ")

    if any(term in text for term in ("segensstein", "boonstone", "pvp")):
        return "pvp"
    if any(term in text for term in ("nightmare", "albtraum")):
        return "nightmare"
    if any(term in text for term in ("trial", "prüfung", "pruefung")):
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
    """Gibt eine dauerhafte Eventbild-URL zurück.

    Eigene externe URLs bleiben unverändert. Alte/signierte Discord-Anhangslinks
    werden auf ein lokales Standardbild abgebildet. Bei leerer URL kann das
    Dashboard optional aus Eventtyp/Titel ein passendes Bild ableiten.
    """
    event = event or {}
    raw = str(raw_url or "").strip()
    if not raw:
        for key in ("image_url", "banner_url", "thumbnail_url", "media_url", "event_image_url"):
            candidate = str(event.get(key) or "").strip()
            if candidate:
                raw = candidate
                break

    if raw.startswith("/static/"):
        path, _, _query = raw.partition("?")
        if path.startswith("/static/event_images/"):
            raw = f"{path}?v={EVENT_IMAGE_ASSET_VERSION}"
        return f"{dashboard_public_base_url()}{raw}" if absolute else raw

    inferred = infer_preset_key(event)
    legacy = legacy_preset_key(raw)
    if raw and is_discord_attachment_url(raw):
        preset_key = inferred or legacy
        if preset_key:
            return event_image_asset_url(preset_key) if absolute else event_image_asset_path(preset_key)
        # Nicht bekannte Discord-Anhangslinks mit Signatur sind nicht dauerhaft.
        if any(token in raw for token in ("?ex=", "&ex=", "&hm=", "?hm=")):
            return ""
        return raw

    if raw.startswith(("https://", "http://")):
        try:
            parsed = urlparse(raw)
            if str(parsed.path or "").startswith("/static/event_images/"):
                stable_path = f"{parsed.path}?v={EVENT_IMAGE_ASSET_VERSION}"
                return f"{dashboard_public_base_url()}{stable_path}" if absolute else stable_path
        except Exception:
            pass
        return raw

    if infer_when_missing and inferred:
        return event_image_asset_url(inferred) if absolute else event_image_asset_path(inferred)
    return ""
