# -*- coding: utf-8 -*-
from __future__ import annotations

# Patch: stabile Bildlogik aus der funktionierenden Version + Schaden-Fix (▲-Werte nicht als Schaden).

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urljoin, urlencode, parse_qsl, urlunparse
import hashlib

from item_catalog_db import connect, ensure_item_catalog_schema, upsert_item

BASE = "https://questlog.gg"
GAME = "throne-and-liberty"
DEFAULT_LOCALE = "de"  # hart festgelegt: Questlog-Import fuer Ebolus immer Deutsch
WEAPON_CATEGORY_SLUGS = [
    "sword",
    "sword2h",
    "dagger",
    "bow",
    "crossbow",
    "wand",
    "staff",
    "spear",
    "orb",
    "gauntlet",
]

DEFAULT_START_URL = f"{BASE}/{GAME}/{DEFAULT_LOCALE}/db/items/weapons/sword?grade=41"
DEFAULT_WEAPON_CATEGORY_URLS = [
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/sword?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/sword2h?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/dagger?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/bow?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/crossbow?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/wand?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/staff?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/spear?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/orb?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/weapons/gauntlet?grade=41",
]
DEFAULT_ARMOR_CATEGORY_URLS = [
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/head?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/chest?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/cloak?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/hands?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/feet?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/armor/legs?grade=41",
]

DEFAULT_ACCESSORY_CATEGORY_URLS = [
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/necklace?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/bracelet?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/brooch?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/ring?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/belt?grade=41",
    "https://questlog.gg/throne-and-liberty/de/db/items/accessories/earring?grade=41",
]

# Diese bekannten Listen werden nur als Fallback genutzt. Der Importer versucht zuerst,
# Kategorien von /db/items automatisch zu entdecken.
FALLBACK_CATEGORY_PATHS = [
    "weapons",
    "armor",
    "armors",
    "equipment",
    "accessories",
    "materials",
    "material",
    "currencies",
    "currency",
    "consumables",
    "misc",
]

REQUEST_DELAY = float(os.getenv("QUESTLOG_IMPORT_DELAY", "1.2"))
MAX_PAGES = int(os.getenv("QUESTLOG_MAX_PAGES", "250"))
MAX_ITEMS = int(os.getenv("QUESTLOG_MAX_ITEMS", "0") or "0")
HEADLESS = os.getenv("QUESTLOG_HEADLESS", "1").lower() not in {"0", "false", "no", "off"}
NAV_TIMEOUT_MS = int(os.getenv("QUESTLOG_TIMEOUT_MS", "120000") or "120000")
PAGE_SETTLE_MS = int(os.getenv("QUESTLOG_PAGE_SETTLE_MS", "6000") or "6000")

# Standard: nur Rare und höher. Questlog nutzt in der URL grade=41 für diesen Filter.
DEFAULT_MIN_RARITY = os.getenv("QUESTLOG_MIN_RARITY", "Rare").strip() or "Rare"
RARITY_RANK = {"Common": 10, "Uncommon": 20, "Rare": 30, "Epic": 40, "Legendary": 50}


BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 EboQuestlogImporter/1.0"
)

WEAPON_ALIASES = [
    ("Schwert & Schild", ["sword and shield", "sword & shield", "schwert", "schild"]),
    ("Großschwert", ["greatsword", "great sword", "großschwert", "grossschwert"]),
    ("Dolche", ["daggers", "dagger", "dolche", "dolch"]),
    ("Armbrust", ["crossbow", "crossbows", "armbrust"]),
    ("Langbogen", ["longbow", "longbows", "bow", "bogen", "langbogen"]),
    ("Stab", ["staff", "stäbe", "stab"]),
    ("Zauberstab", ["wand and tome", "wand", "tome", "zauberstab"]),
    ("Speer", ["spear", "spears", "speer"]),
    ("Kugel", ["orb", "orbs", "kugel"]),
    ("Fäustlinge", ["gauntlets", "gauntlet", "fäustlinge", "faeustlinge"]),
]

ARMOR_ALIASES = [
    ("Helm", ["helmet", "helm", "headgear", "head armor"]),
    ("Brust", ["chest", "body armor", "chest armor", "armor", "rüstung", "ruestung"]),
    ("Hose", ["pants", "legs", "leg armor", "hose"]),
    ("Handschuhe", ["gloves", "handschuhe"]),
    ("Schuhe", ["boots", "shoes", "schuhe", "stiefel"]),
    ("Umhang", ["cloak", "cape", "umhang"]),
]

ACCESSORY_ALIASES = [
    ("Kette", ["necklace", "kette", "halskette"]),
    ("Armband", ["bracelet", "armband"]),
    ("Brosche", ["brooch", "brosche"]),
    ("Ring", ["ring"]),
    ("Gürtel", ["belt", "gürtel", "guertel"]),
    ("Ohrringe", ["earring", "earrings", "ohrring", "ohrringe"]),
]

RARITY_ALIASES = [
    ("Common", ["common", "gewöhnlich", "gewoehnlich"]),
    ("Uncommon", ["uncommon", "ungewöhnlich", "ungewoehnlich"]),
    ("Rare", ["rare", "selten"]),
    ("Epic", ["epic", "episch"]),
    ("Legendary", ["legendary", "legendär", "legendaer"]),
]

STAT_KEYWORDS = [
    "strength", "dexterity", "wisdom", "perception",
    "stärke", "geschicklichkeit", "weisheit", "wahrnehmung",
    "hit chance", "critical hit", "critical chance", "heavy attack",
    "max health", "max mana", "mana regen", "health regen",
    "cooldown", "attack speed", "skill damage", "boss damage",
    "max damage", "range", "wildkin", "mana", "hit chance",
    "abkling", "angriffsgeschwindigkeit", "schaden", "reichweite", "trefferchance", "krit",
    "melee", "ranged", "magic", "evasion", "endurance", "defense",
    "verteidigung", "ausweichen", "magieausweichen", "fernkampfausweichen", "nahkampfausweichen",
    "ausdauer", "robustheit", "schadensreduzierung", "heilung", "buff", "debuff",
]


# Begriffe, die Questlog als Navigation/Kategorie nutzt und nicht als Item importiert werden dürfen.
ITEM_NAME_BLACKLIST = {
    "items", "item", "weapons", "weapon", "armor", "armour", "equipment", "accessories",
    "materials", "material", "currencies", "currency", "consumables", "food", "misc",
    "sword", "sword and shield", "greatsword", "dagger", "daggers", "bow", "longbow",
    "crossbow", "wand", "staff", "spear", "orb", "gauntlet", "gauntlets",
    "head", "chest", "cloak", "hands", "feet", "legs", "necklace", "bracelet", "brooch",
    "ring", "belt", "earring", "all", "filter", "search", "database", "questlog",
}

# Questlog-Unterkategorie-Slugs. Damit wird z. B. /weapons/bow sicher als
# Langbogen erkannt, auch wenn der Itemname selbst nicht "Longbow" enthält.
WEAPON_SLUG_TO_SUBCATEGORY = {
    "sword": "Schwert & Schild",
    "sword2h": "Großschwert",
    "dagger": "Dolche",
    "bow": "Langbogen",
    "crossbow": "Armbrust",
    "wand": "Zauberstab",
    "staff": "Stab",
    "spear": "Speer",
    "orb": "Kugel",
    "gauntlet": "Fäustlinge",
}

ARMOR_SLUG_TO_SUBCATEGORY = {
    "head": "Helm",
    "chest": "Brust",
    "legs": "Hose",
    "hands": "Handschuhe",
    "feet": "Schuhe",
    "cloak": "Umhang",
}

ACCESSORY_SLUG_TO_SUBCATEGORY = {
    "necklace": "Kette",
    "bracelet": "Armband",
    "brooch": "Brosche",
    "ring": "Ring",
    "belt": "Gürtel",
    "earring": "Ohrringe",
}

BAD_NON_ITEM_NAME_PATTERNS = [
    r"\bguide\b", r"\bhow\s+to\b", r"\bunlock(?:ing)?\b", r"\bintroduction\b", r"\bintro\b",
    r"\bbreakdown\b", r"\bcontracts?\b", r"\bpuzzle\b", r"\bboss\b", r"\bevent\b",
    r"\bfirst\s+look\b", r"\bwhat\s+to\s+do\b", r"\bfishing\b", r"\bamitoi\b",
    r"\bhousing\b", r"\bplayer\s+homes\b", r"\bmystic\s+keys\b", r"\bdistribution\b",
    r"\bstat\s+points\b", r"\btraits\s+guide\b", r"\bskill\s+showcase\b", r"\bweapon\s+combos\b",
]

NON_ITEM_URL_HINTS = [
    "/db/guides", "/guides", "/db/skills", "/skills", "/db/npcs", "/npcs",
    "/db/quests", "/quests", "/db/events", "/events", "/db/guardians", "/guardians",
    "/news", "/article", "/articles", "/builds",
]

JSON_NAME_KEYS = [
    "name", "title", "itemName", "item_name", "displayName", "display_name",
    "localizedName", "localized_name", "label", "fullName", "full_name",
]
JSON_ID_KEYS = [
    "id", "itemId", "item_id", "itemID", "code", "hash", "key", "slug", "itemCode", "item_code", "uuid",
]
JSON_URL_KEYS = ["url", "href", "link", "path", "pathname", "detailUrl", "detail_url"]
JSON_IMAGE_KEYS = [
    "icon", "iconUrl", "icon_url", "image", "imageUrl", "image_url", "thumbnail", "thumbnailUrl",
    "smallIcon", "small_icon", "largeIcon", "large_icon", "asset", "assetUrl", "asset_url",
]


def to_abs_url(value: Any) -> str:
    src = str(value or "").strip()
    if not src:
        return ""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return urljoin(BASE, src)
    if src.startswith("http://") or src.startswith("https://"):
        return src
    return src




def force_de_locale_url(url: str) -> str:
    """Questlog-URL hart auf /de/ normalisieren.

    Wichtig: Questlog/Next.js kann in eingebetteten Links andere Locales wie /ja/
    liefern. Fuer den Ebolus-Katalog wollen wir ausschließlich deutsche Daten.
    """
    raw = str(url or "").strip()
    if not raw:
        return raw
    try:
        parsed = urlparse(raw)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2 and parts[0] == GAME:
            parts[1] = "de"
            new_path = "/" + "/".join(parts)
            return urlunparse((parsed.scheme or "https", parsed.netloc or "questlog.gg", new_path, parsed.params, parsed.query, parsed.fragment))
    except Exception:
        pass
    return raw


def is_de_questlog_url(url: str) -> bool:
    parts = path_parts(url)
    try:
        idx = parts.index(GAME)
        return len(parts) > idx + 1 and parts[idx + 1] == "de"
    except ValueError:
        return False

def url_with_query_param(url: str, key: str, value: str) -> str:
    try:
        parsed = urlparse(url)
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        q[key] = value
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(q), parsed.fragment))
    except Exception:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}{key}={value}"


def query_int_param(url: str, key: str, default: int = 1) -> int:
    try:
        q = dict(parse_qsl(urlparse(str(url or "")).query, keep_blank_values=True))
        raw = str(q.get(key) or "").strip()
        return int(raw) if raw else int(default)
    except Exception:
        return int(default)


def next_page_url(url: str) -> str:
    """Questlog-Kategorie-Seiten paginieren.

    Questlog zeigt auf vielen Listen nur 40 Items pro Seite. Scrollen allein reicht
    nicht; weitere Items liegen hinter ?page=2, ?page=3, ... .
    Wir generieren die naechste Seite nur, wenn auf der aktuellen Seite neue
    Detailseiten gefunden wurden. So stoppen wir automatisch, falls Questlog eine
    nicht vorhandene Seite auf Seite 1 zurueckfallen laesst.
    """
    return url_with_query_param(url, "page", str(query_int_param(url, "page", 1) + 1))


def force_weapon_grade_filter(url: str) -> str:
    url = force_de_locale_url(url)
    """Für Item-Listen ab Rare filtern.

    Detailseiten brauchen keinen grade-Query. Listen und Unterlisten dagegen schon,
    sonst importiert Questlog auch Common/Uncommon und teils unnötige Seitendaten.
    Historischer Funktionsname bleibt aus Kompatibilität erhalten.
    """
    if is_items_url(url) and classify_main_category(url) in {"weapon", "armor"}:
        return url_with_query_param(url, "grade", "41")
    return url


def rarity_allowed(rarity: str | None, min_rarity: str = DEFAULT_MIN_RARITY) -> bool:
    if not min_rarity:
        return True
    if not rarity:
        # Bei fehlender Seltenheit lieber nicht blind importieren.
        return False
    return RARITY_RANK.get(str(rarity), 0) >= RARITY_RANK.get(str(min_rarity), 0)


def is_skill_core_like(url: str, text: str, name: str = "") -> bool:
    hay = f"{url} {name} {text}".lower()
    needles = [
        "talistone", "skillcore", "skill-core", "skill core",
        "fähigkeitskern", "faehigkeitskern", "fähigkeitkern", "faehigkeitkern",
        "fähigkeits-kerne", "faehigkeits-kerne", "fähigkeitskern",
    ]
    return any(n in hay for n in needles)


SKIP_REASON_BY_URL: dict[str, str] = {}


def skip_item(url: str, reason: str) -> None:
    SKIP_REASON_BY_URL[str(url)] = str(reason)
    return None


def has_grade_filter(url: str) -> bool:
    try:
        q = dict(parse_qsl(urlparse(str(url or "")).query, keep_blank_values=True))
        return bool(q.get("grade"))
    except Exception:
        return "grade=" in str(url or "")


def stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:16]


def value_to_text(value: Any, locale: str = DEFAULT_LOCALE) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, (int, float)):
        return clean_text(str(value))
    if isinstance(value, dict):
        for key in (locale, "en", "de", "value", "text", "name", "title", "label"):
            if key in value:
                txt = value_to_text(value.get(key), locale)
                if txt:
                    return txt
    return ""


def pick_text(d: dict[str, Any], keys: list[str], locale: str = DEFAULT_LOCALE) -> str:
    for key in keys:
        if key in d:
            txt = value_to_text(d.get(key), locale)
            if txt:
                return txt
    # Fallback: case-insensitive keys
    lower_map = {str(k).lower(): k for k in d.keys()}
    for key in keys:
        real = lower_map.get(key.lower())
        if real is not None:
            txt = value_to_text(d.get(real), locale)
            if txt:
                return txt
    return ""


def compact_json_text(obj: Any, limit: int = 12000) -> str:
    try:
        raw = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        raw = str(obj)
    return raw[:limit]


def find_first_url_value(obj: Any, keys: list[str]) -> str:
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(needle.lower() == kl for needle in keys) or any(needle.lower() in kl for needle in keys):
                if isinstance(v, str) and v.strip():
                    return to_abs_url(v)
            found = find_first_url_value(v, keys)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = find_first_url_value(v, keys)
            if found:
                return found
    return ""


def extract_candidate_name(d: dict[str, Any], locale: str = DEFAULT_LOCALE) -> str:
    name = pick_text(d, JSON_NAME_KEYS, locale)
    if name:
        return name
    # Manchmal heißen Lokalisierungsfelder direkt en/de ohne Name-Key.
    for key in (locale, "en", "de"):
        if key in d:
            txt = value_to_text(d.get(key), locale)
            if 3 <= len(txt) <= 120:
                return txt
    return ""


def is_probably_item_object(d: dict[str, Any], source_url: str, main_category: str, locale: str = DEFAULT_LOCALE) -> bool:
    name = extract_candidate_name(d, locale)
    if not name or not (3 <= len(name) <= 140):
        return False
    low_name = name.strip().lower()
    if is_bad_non_item_name(name):
        return False
    if low_name.startswith(("http://", "https://")):
        return False
    if candidate_has_non_item_url(d):
        return False

    # Auf Hauptlisten wie /weapons importieren wir gar keine JSON-Objekte. Dort liegen
    # bei Questlog viele Guides/News/Navigationen im App-JSON. Importiert wird erst auf
    # Unterlisten wie /weapons/bow.
    if main_category in {"weapon", "armor", "accessory"} and not (is_subcategory_list_url(source_url) or is_detail_url(source_url)):
        return False

    keys = {str(k).lower() for k in d.keys()}
    serialized = compact_json_text(d, limit=8000).lower()

    item_specific_key_hints = {
        "itemid", "item_id", "itemcode", "item_code", "itemlevel", "item_level",
        "rarity", "grade", "quality", "icon", "iconurl", "icon_url", "image", "imageurl",
        "image_url", "thumbnail", "damage", "attack", "defense", "stats", "stat",
        "trait", "traits", "effect", "effects", "ability", "abilities", "equip", "slot",
        "weapon", "armor", "accessory", "accessories", "jewelry", "material", "currency",
    }
    has_hint_key = any(any(h in k for h in item_specific_key_hints) for k in keys)
    has_hint_text = any(h in serialized for h in [
        "rarity", "itemlevel", "item level", "damage", "weapon damage", "icon", "trait", "equipment",
        "critical hit", "strength", "dexterity", "wisdom", "perception",
    ])
    has_id = any(k.lower() in keys for k in [x.lower() for x in JSON_ID_KEYS])
    has_img = bool(find_first_url_value(d, JSON_IMAGE_KEYS))

    # Guides/Bosse/Skills haben oft id+name+thumbnail. Deshalb reicht id+image nicht.
    # Für Waffen/Rüstung verlangen wir auf Listen mindestens irgendeinen Item-Hinweis
    # wie Rarity/Stats/Damage/Itemlevel/Slot/Equipment.
    if main_category in {"weapon", "armor", "accessory"}:
        return bool(has_hint_key or has_hint_text) and not (has_id and has_img and not (has_hint_key or has_hint_text))

    return bool(has_hint_key or has_hint_text or has_id or has_img)


def source_url_from_candidate(d: dict[str, Any], current_url: str, source_id: str) -> str:
    raw_url = pick_text(d, JSON_URL_KEYS, DEFAULT_LOCALE)
    if raw_url:
        abs_url = to_abs_url(raw_url)
        if is_items_url(abs_url):
            return abs_url.rstrip("/")
        if raw_url.startswith("/"):
            return to_abs_url(raw_url).rstrip("/")
    return f"{current_url.rstrip('/')}#json-{source_id}"


def extract_source_id(d: dict[str, Any], fallback_seed: str, locale: str = DEFAULT_LOCALE) -> str:
    sid = pick_text(d, JSON_ID_KEYS, locale)
    if sid:
        return slugify(sid)[:120]
    return stable_hash(fallback_seed)


def extract_structured_stats_from_json(obj: Any, locale: str = DEFAULT_LOCALE) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    def add(key: Any, val: Any) -> None:
        k = value_to_text(key, locale)
        v = value_to_text(val, locale)
        if not k or not v:
            return
        if len(k) > 80 or len(v) > 120:
            return
        low = k.lower()
        if any(word in low for word in STAT_KEYWORDS) or re.search(r"[+\-]?\d", v):
            stats[k] = v

    for d in walk_json(obj):
        if not isinstance(d, dict):
            continue
        keys = {str(k).lower() for k in d.keys()}
        # Typische Formen: {name: Strength, value: 3}, {statName: ..., statValue: ...}
        name = pick_text(d, ["stat", "statName", "stat_name", "name", "label", "type"], locale)
        value = pick_text(d, ["value", "amount", "statValue", "stat_value", "bonus", "max", "min"], locale)
        if name and value:
            add(name, value)
        # Direkte Felder: {strength: 3, dexterity: 2}
        for k, v in d.items():
            kl = str(k).lower()
            if any(word in kl for word in STAT_KEYWORDS):
                if isinstance(v, (str, int, float)):
                    add(k, v)
    return stats


def extract_structured_abilities_from_json(obj: Any, locale: str = DEFAULT_LOCALE) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    interesting = ["ability", "abilities", "effect", "effects", "passive", "skill", "description", "desc", "tooltip"]
    for d in walk_json(obj):
        if not isinstance(d, dict):
            continue
        keys = {str(k).lower() for k in d.keys()}
        if not any(any(word in k for word in interesting) for k in keys):
            continue
        label = pick_text(d, ["name", "title", "label", "abilityName", "effectName"], locale)
        text = pick_text(d, ["description", "desc", "text", "tooltip", "effect", "effects", "content"], locale)
        if text and 15 <= len(text) <= 2000:
            out.append({"label": label[:120] if label else "Effect", "text": text[:1500]})
    # Dedupe
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for a in out:
        key = (a.get("label", "") + "|" + a.get("text", "")).lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(a)
    return result[:8]


def json_candidate_to_record(d: dict[str, Any], current_url: str, main_category_hint: str, locale: str) -> dict[str, Any] | None:
    if not is_probably_item_object(d, current_url, main_category_hint, locale):
        return None
    name = extract_candidate_name(d, locale)
    if is_bad_non_item_name(name) or candidate_has_non_item_url(d):
        return None
    serialized = compact_json_text(d, limit=25000)
    source_id = extract_source_id(d, name + current_url + serialized[:500], locale)
    source_url = source_url_from_candidate(d, current_url, source_id)
    main_category = main_category_hint or classify_main_category(source_url or current_url, serialized)
    sub_category, confidence = detect_sub_category(main_category, serialized, source_url or current_url)
    if confidence == "low" and main_category in {"weapon", "armor", "accessory"}:
        # Ohne sicheren Untertyp nehmen wir solche Kandidaten nicht mehr.
        # Das verhindert Guides/Bosse/Skills im Waffenimport.
        return None
    stats = extract_structured_stats_from_json(d, locale)
    # Text-Fallback ergänzen.
    for k, v in extract_stats_from_lines(serialized).items():
        stats.setdefault(k, v)
    abilities = extract_structured_abilities_from_json(d, locale)
    if not abilities:
        abilities = extract_abilities(serialized)
    damage_min, damage_max = extract_damage(serialized)
    image_url = find_first_url_value(d, JSON_IMAGE_KEYS)
    icon_url = image_url
    return {
        "source": "questlog",
        "source_url": source_url,
        "source_item_id": source_id,
        "locale": locale,
        "name": name,
        "slug": slugify(name),
        "main_category": main_category,
        "sub_category": sub_category,
        "rarity": detect_rarity(serialized),
        "item_level": extract_level(serialized, "Item Level", "itemLevel", "Gegenstandsstufe", "Level"),
        "required_level": extract_level(serialized, "Required Level", "requiredLevel", "Benötigte Stufe"),
        "damage_min": damage_min,
        "damage_max": damage_max,
        "defense": extract_defense(serialized),
        "stats": stats,
        "abilities": abilities,
        "traits": [],
        "image_url": image_url or None,
        "icon_url": icon_url or None,
        "classification_confidence": confidence,
        "raw_text": serialized[:30000],
        "raw_data": {
            "scraped_at": now_iso(),
            "url": current_url,
            "source": "json_payload_or_next_data",
            "parser": "questlog-json-v3",
            "raw": d,
        },
    }


def extract_records_from_json_payloads(payloads: list[Any], current_url: str, main_category: str, locale: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for payload in payloads:
        for d in walk_json(payload):
            if not isinstance(d, dict):
                continue
            rec = json_candidate_to_record(d, current_url, main_category, locale)
            if not rec:
                continue
            key = str(rec.get("source_url") or rec.get("name"))
            if key in seen:
                continue
            seen.add(key)
            records.append(rec)
    return records


def extract_dom_card_records(page, current_url: str, main_category: str, locale: str) -> list[dict[str, Any]]:
    """Fallback, wenn Questlog Itemkarten ohne <a>-Detail-Links rendert.

    Wir sammeln Elternbereiche von Bildern und nehmen daraus sichtbaren Text. Das ist
    bewusst defensiv gefiltert, damit nicht Navigation/Logos als Items landen.
    """
    try:
        cards = page.locator("img").evaluate_all(
            """
            imgs => imgs.map((img, idx) => {
              let node = img;
              let best = img.parentElement;
              for (let i = 0; i < 5 && node && node.parentElement; i++) {
                node = node.parentElement;
                const txt = (node.innerText || '').trim();
                if (txt.length >= 3 && txt.length <= 2000) best = node;
              }
              return {
                idx,
                src: img.currentSrc || img.src || img.getAttribute('data-src') || '',
                alt: img.alt || '',
                text: best ? (best.innerText || '').trim() : '',
                href: best ? ((best.closest('a') || {}).href || '') : ''
              };
            })
            """
        )
    except Exception:
        return []

    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for c in cards or []:
        text = normalize_raw_text(str(c.get("text") or ""))
        src = to_abs_url(c.get("src") or "")
        if not text or not src:
            continue
        lines = [clean_text(x) for x in re.split(r"[\n\r]+", text) if clean_text(x)]
        # Ersten plausiblen Namen nehmen.
        name = ""
        for line in lines[:6]:
            low = line.lower()
            if 3 <= len(line) <= 120 and low not in ITEM_NAME_BLACKLIST and not re.fullmatch(r"[+\-]?\d+(?:[.,]\d+)?%?", line):
                name = line
                break
        if not name or is_bad_non_item_name(name):
            continue
        # DOM-Karten nur auf echten Unterlisten importieren. Auf /weapons selbst stehen
        # auch Guides/Navigation/Empfehlungen.
        if main_category in {"weapon", "armor", "accessory"} and not is_subcategory_list_url(current_url):
            continue
        # Ohne itemartige Hinweise nicht importieren; sonst würden Logos/Kategorien reinlaufen.
        if not any(k in text.lower() for k in ["damage", "level", "rarity", "trait", "attack", "defense", "common", "uncommon", "rare", "epic", "legendary"]):
            continue
        source_id = stable_hash(current_url + name + src)
        source_url = str(c.get("href") or "").strip()
        if source_url and is_items_url(source_url):
            source_url = source_url.rstrip("/")
        else:
            source_url = f"{current_url.rstrip('/')}#dom-{source_id}"
        if source_url in seen:
            continue
        seen.add(source_url)
        sub_category, confidence = detect_sub_category(main_category, text, current_url)
        if confidence == "low" and main_category in {"weapon", "armor", "accessory"}:
            continue
        damage_min, damage_max = extract_damage(text)
        records.append({
            "source": "questlog",
            "source_url": source_url,
            "source_item_id": source_id,
            "locale": locale,
            "name": name,
            "slug": slugify(name),
            "main_category": main_category,
            "sub_category": sub_category,
            "rarity": detect_rarity(text),
            "item_level": extract_level(text, "Item Level", "Level"),
            "required_level": extract_level(text, "Required Level"),
            "damage_min": damage_min,
            "damage_max": damage_max,
            "defense": extract_defense(text),
            "stats": extract_stats_from_lines(text),
            "abilities": extract_abilities(text),
            "traits": [],
            "image_url": src,
            "icon_url": src,
            "classification_confidence": confidence,
            "raw_text": text[:30000],
            "raw_data": {"scraped_at": now_iso(), "url": current_url, "source": "dom_card", "parser": "questlog-dom-v3"},
        })
    return records


def attach_json_capture(page, bucket: list[Any]) -> None:
    def on_response(response):
        try:
            url = response.url or ""
            if "questlog" not in url and "/_next/" not in url and "/api/" not in url:
                return
            ct = (response.headers or {}).get("content-type", "").lower()
            if "json" not in ct and "/_next/data/" not in url and "/api/" not in url:
                return
            data = response.json()
            bucket.append(data)
        except Exception:
            return
    try:
        page.on("response", on_response)
    except Exception:
        pass


@dataclass
class CategorySeed:
    url: str
    main_category: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def slugify(value: str) -> str:
    value = clean_text(value).lower()
    value = value.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-") or "item"


def parse_number(value: str) -> float | None:
    m = re.search(r"-?\d+(?:[.,]\d+)?", value or "")
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except Exception:
        return None


def path_parts(url: str) -> list[str]:
    return [p for p in urlparse(url).path.split("/") if p]


def item_path_index(url: str) -> int:
    parts = path_parts(url)
    try:
        return parts.index("items")
    except ValueError:
        return -1


def is_items_url(url: str) -> bool:
    return f"/{GAME}/" in url and "/db/items" in url


def is_item_detail_url(url: str) -> bool:
    # Questlog nutzt fuer echte Detailseiten singular /db/item/<item_id>.
    # Die Listen/Filter liegen dagegen unter /db/items/....
    return f"/{GAME}/" in url and "/db/item/" in url


def item_detail_id(url: str) -> str:
    parts = path_parts(url)
    try:
        idx = parts.index("item")
        if len(parts) > idx + 1:
            return parts[idx + 1]
    except ValueError:
        pass
    return parts[-1] if parts else ""


def item_tail(url: str) -> list[str]:
    parts = path_parts(url)
    idx = item_path_index(url)
    if idx < 0:
        return []
    return parts[idx + 1:]


def subcategory_slug(url: str) -> str:
    tail = item_tail(url)
    if len(tail) >= 2:
        return tail[1].lower()
    return ""


def subcategory_from_url(main_category: str, url: str) -> str | None:
    slug = subcategory_slug(url)
    if main_category == "weapon":
        return WEAPON_SLUG_TO_SUBCATEGORY.get(slug)
    if main_category == "armor":
        return ARMOR_SLUG_TO_SUBCATEGORY.get(slug)
    if main_category == "accessory":
        return ACCESSORY_SLUG_TO_SUBCATEGORY.get(slug)
    return None


def is_bad_non_item_name(name: str) -> bool:
    low = clean_text(name).lower()
    if not low or low in ITEM_NAME_BLACKLIST:
        return True
    return any(re.search(pattern, low, flags=re.I) for pattern in BAD_NON_ITEM_NAME_PATTERNS)


def candidate_has_non_item_url(d: dict[str, Any]) -> bool:
    for key in JSON_URL_KEYS:
        raw = pick_text(d, [key], DEFAULT_LOCALE)
        if not raw:
            continue
        abs_url = to_abs_url(raw).lower()
        if any(hint in abs_url for hint in NON_ITEM_URL_HINTS):
            return True
        if "/db/" in abs_url and "/db/items" not in abs_url and "/db/item/" not in abs_url:
            return True
    return False


def is_category_list_url(url: str) -> bool:
    """Oberste Questlog-Itemliste, z. B. /db/items/weapons."""
    if not is_items_url(url):
        return False
    return len(item_tail(url)) == 1


def is_subcategory_list_url(url: str) -> bool:
    """Questlog-Unterliste, z. B. /db/items/weapons/sword.

    Der alte Importer hat solche URLs fälschlich als Item-Detailseiten behandelt.
    Questlog nutzt diese Ebene aber als Filter-/Unterkategorie-Seite.
    Echte Detailseiten liegen darunter, z. B. /db/items/weapons/sword/<item>.
    """
    if not is_items_url(url):
        return False
    return len(item_tail(url)) == 2


def is_list_url(url: str) -> bool:
    return is_category_list_url(url) or is_subcategory_list_url(url)


def is_detail_url(url: str) -> bool:
    if is_item_detail_url(url):
        return True
    if not is_items_url(url):
        return False
    # Alte/Fallback-Struktur: /db/items/weapons/sword/<slug>.
    return len(item_tail(url)) >= 3


def category_segment(url: str) -> str:
    parts = path_parts(url)
    idx = item_path_index(url)
    if idx >= 0 and len(parts) > idx + 1:
        return parts[idx + 1].lower()
    return ""


def normalize_main_category_arg(value: Any) -> str:
    low = clean_text(str(value or "")).lower()
    mapping = {
        "waffe": "weapon", "waffen": "weapon", "weapon": "weapon", "weapons": "weapon",
        "rüstung": "armor", "ruestung": "armor", "rüstungen": "armor", "ruestungen": "armor", "armor": "armor", "armors": "armor", "gear": "armor", "equipment": "armor",
        "zubehör": "accessory", "zubehoer": "accessory", "schmuck": "accessory", "accessory": "accessory", "accessories": "accessory", "accessoire": "accessory", "accessoires": "accessory",
        "material": "material", "materials": "material",
        "währung": "currency", "waehrung": "currency", "currency": "currency", "currencies": "currency",
    }
    return mapping.get(low, low)


def classify_main_category(url: str, text: str = "") -> str:
    seg = category_segment(url)
    hay = f"{url} {seg} {text}".lower()
    if "weapon" in hay or "waffe" in hay:
        return "weapon"
    if any(x in hay for x in ["/db/items/accessories", "accessories", "accessory", "jewelry", "jewellery", "zubehör", "zubehoer", "schmuck"]):
        return "accessory"
    if any(x in hay for x in ["armor", "armour", "equipment", "gear", "rüstung", "ruestung"]):
        return "armor"
    if "material" in hay or "crafting" in hay:
        return "material"
    if "currenc" in hay or "währung" in hay or "waehrung" in hay:
        return "currency"
    return "misc"


def detect_alias(text: str, aliases: list[tuple[str, list[str]]]) -> str | None:
    low = f" {text.lower()} "
    for label, keys in aliases:
        for key in keys:
            if re.search(rf"(?<![a-z0-9]){re.escape(key.lower())}(?![a-z0-9])", low):
                return label
    return None


def detect_sub_category(main_category: str, text: str, url: str = "") -> tuple[str | None, str]:
    from_url = subcategory_from_url(main_category, url)
    if from_url:
        return from_url, "high"
    hay = f"{url} {text}"
    if main_category == "weapon":
        label = detect_alias(hay, WEAPON_ALIASES)
        return label, "high" if label else "low"
    if main_category == "armor":
        label = detect_alias(hay, ARMOR_ALIASES)
        return label, "high" if label else "low"
    if main_category == "accessory":
        label = detect_alias(hay, ACCESSORY_ALIASES)
        return label, "high" if label else "low"
    if main_category == "material":
        return "Material", "medium"
    if main_category == "currency":
        return "Währung", "medium"
    return None, "medium"


def detect_rarity(text: str) -> str | None:
    return detect_alias(text, RARITY_ALIASES)


def extract_level(text: str, *labels: str) -> int | None:
    for label in labels:
        m = re.search(rf"{re.escape(label)}\s*:?\s*(\d+)", text, flags=re.I)
        if m:
            return int(m.group(1))
    return None


def extract_damage(text: str) -> tuple[float | None, float | None]:
    patterns = [
        r"(?:max\.?\s+)?(?:base\s+)?damage\s*:?\s*(\d+(?:[.,]\d+)?)\s*[-–~]\s*(\d+(?:[.,]\d+)?)",
        r"(?:max\.?\s+)?schaden\s*:?\s*(\d+(?:[.,]\d+)?)\s*[-–~]\s*(\d+(?:[.,]\d+)?)",
        r"weapon\s+damage\s*:?\s*(\d+(?:[.,]\d+)?)\s*[-–~]\s*(\d+(?:[.,]\d+)?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))
    return None, None


def extract_defense(text: str) -> float | None:
    for pattern in [r"defense\s*:?\s*(\d+(?:[.,]\d+)?)", r"verteidigung\s*:?\s*(\d+(?:[.,]\d+)?)"]:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return float(m.group(1).replace(",", "."))
    return None


def extract_stats_from_lines(text: str) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    # Erst echte Key/Value-Paare. Keine nackten Labels mehr speichern.
    stats.update(extract_stat_value_pairs_from_text(text))
    raw_lines = [clean_text(x) for x in re.split(r"[\n\r]+| {2,}", normalize_raw_text(text)) if clean_text(x)]
    for line in raw_lines:
        low = line.lower()
        if not any(k in low for k in STAT_KEYWORDS):
            continue
        # Zeilen ohne Wert nicht aufnehmen. Das war der Grund für "Reichweite:" ohne Zahl.
        if not re.search(r"\d", line):
            continue
        m = re.match(r"^([A-Za-zÄÖÜäöüß ./%'\-]+?)\s*:?\s*([+\-]?\d+(?:[.,]\d+)?%?(?:\s*[~\-–|/]\s*[+\-]?\d+(?:[.,]\d+)?%?)*)$", line)
        if m:
            key = clean_text(m.group(1))
            val = clean_text(m.group(2))
            if key and val and len(key) <= 80:
                stats.setdefault(key, val)
    return stats


def extract_abilities(text: str) -> list[dict[str, str]]:
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", text) if clean_text(x)]
    markers = ["effect", "ability", "special", "passive", "fähigkeit", "effekt"]
    out: list[dict[str, str]] = []
    for idx, line in enumerate(lines):
        low = line.lower()
        if any(m in low for m in markers):
            chunk = " ".join(lines[idx: idx + 5])
            if len(chunk) > 20:
                out.append({"label": line[:120], "text": chunk[:1500]})
    # Dedupe
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for item in out:
        key = item["text"].lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result[:8]




def extract_stat_value_pairs_from_text(text: str) -> dict[str, Any]:
    """Liest sichtbare Questlog-Statzeilen mit echten Werten.

    Beispiele von Detailseiten:
      Max. Schaden 61 ~ 195
      Reichweite: 19,2m
      Angriffsgeschwindigkeit: 0,64s
      Geschicklichkeit: 11
      Trefferchance: 180
    """
    stats: dict[str, Any] = {}
    text = normalize_raw_text(text)
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", text) if clean_text(x)]
    known_labels = [
        "Max. Schaden", "Max Damage", "Schaden", "Damage", "Reichweite", "Range",
        "Angriffsgeschwindigkeit", "Attack Speed", "Stärke", "Strength", "Geschicklichkeit", "Dexterity",
        "Weisheit", "Wisdom", "Wahrnehmung", "Perception", "Trefferchance", "Hit Chance",
        "Krit. Trefferchance", "Critical Hit Chance", "Schwere Angriffschance", "Heavy Attack Chance",
        "Max. Leben", "Max Health", "Max. Mana", "Max Mana", "Manaregeneration", "Mana Regen",
        "Lebensregeneration", "Health Regen", "Abklingzeit", "Cooldown Speed", "Untote-Zusatzschaden",
        "Wildkin-Bonusschaden", "Wildkin Bonus Damage", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
        "Mana-Kosteneffizienz", "Mana Cost Efficiency",
    ]
    label_pattern = "|".join(sorted((re.escape(x) for x in known_labels), key=len, reverse=True))

    for line in lines:
        # Deutsch/Englisch mit Doppelpunkt
        m = re.match(rf"^({label_pattern})\s*:?\s*([+\-]?\d+(?:[.,]\d+)?(?:\s*[~\-–]\s*[+\-]?\d+(?:[.,]\d+)?)?\s*(?:%|m|s)?)$", line, flags=re.I)
        if m:
            stats[clean_text(m.group(1))] = clean_text(m.group(2))
            continue
        # Allgemeiner Fallback: Label : Wert
        m = re.match(r"^([A-Za-zÄÖÜäöüß ./%'\-]+?)\s*:\s*([+\-]?\d+(?:[.,]\d+)?(?:\s*[|~\-–]\s*[+\-]?\d+(?:[.,]\d+)?)*\s*(?:%|m|s)?)$", line)
        if m:
            key = clean_text(m.group(1))
            val = clean_text(m.group(2))
            if any(k in key.lower() for k in STAT_KEYWORDS) or re.search(r"\d", val):
                stats[key] = val
            continue
        # Form ohne Doppelpunkt: "Geschicklichkeit 11"
        m = re.match(rf"^({label_pattern})\s+([+\-]?\d+(?:[.,]\d+)?(?:\s*[~\-–]\s*[+\-]?\d+(?:[.,]\d+)?)?\s*(?:%|m|s)?)$", line, flags=re.I)
        if m:
            stats[clean_text(m.group(1))] = clean_text(m.group(2))
    return stats


def extract_stat_pairs_from_dom(page) -> dict[str, Any]:
    """Liest Tabellen/Listen der Detailseite direkt aus dem DOM.

    Questlog zeigt rechts häufig eine Stat-Tabelle: Header-Zeile + Werte-Zeile.
    Diese Funktion macht daraus {Header: Wert}.
    """
    stats: dict[str, Any] = {}
    try:
        tables = page.locator("table").evaluate_all(
            """
            tables => tables.map(t => Array.from(t.querySelectorAll('tr')).map(tr =>
              Array.from(tr.querySelectorAll('th,td')).map(c => (c.innerText || '').trim()).filter(Boolean)
            ).filter(r => r.length))
            """
        )
        for table in tables or []:
            if not table or len(table) < 2:
                continue
            header = table[0]
            # Wertzeile meistens zweite Zeile; bei mehreren Zeilen jede verarbeiten.
            for row in table[1:]:
                if len(row) == len(header):
                    for k, v in zip(header, row):
                        k = clean_text(k)
                        v = clean_text(v)
                        if k and v and k.lower() not in {"level", "stufe"}:
                            stats[k] = v
                elif len(row) >= 2:
                    stats[clean_text(row[0])] = clean_text(row[1])
    except Exception:
        pass
    try:
        text = page.locator("body").inner_text(timeout=3000)
        for k, v in extract_stat_value_pairs_from_text(text).items():
            stats.setdefault(k, v)
    except Exception:
        pass
    return stats


def extract_traits_from_text(text: str) -> list[dict[str, Any]]:
    traits: list[dict[str, Any]] = []
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(text)) if clean_text(x)]
    for line in lines:
        # Beispiele: Mana Regen: 15 | 30 | 45 | 60
        m = re.match(r"^([A-Za-zÄÖÜäöüß ./%'\-]+?)\s*:\s*((?:\d+(?:[.,]\d+)?%?\s*(?:\||/|,)?\s*){2,})$", line)
        if not m:
            continue
        name = clean_text(m.group(1))
        values = [clean_text(x) for x in re.split(r"\s*[|/,]\s*", m.group(2)) if clean_text(x)]
        if name and len(values) >= 2:
            traits.append({"name": name, "values": values})
    # Dedupe
    seen=set(); out=[]
    for t in traits:
        key=(t.get('name','')+str(t.get('values',''))).lower()
        if key in seen: continue
        seen.add(key); out.append(t)
    return out[:12]


def _armor_expected_trait_count_from_level(level_value: Any) -> int:
    """Questlog-Rüstung: mögliche Eigenschaften nach Item-Level.

    Jonas-Regel:
    - Level 21 und 31: 6 Eigenschaften
    - Level 45, 50 und 80+: 8 Eigenschaften
    """
    try:
        lvl = int(str(level_value or "0"))
    except Exception:
        lvl = 0
    return 8 if lvl >= 45 else 6


ARMOR_TRAIT_LABELS_DE = [
    "Max. Gesundheit", "Max. Leben", "Max. Mana",
    "Gesundheitsregeneration", "Manaregeneration", "Mana-Regeneration",
    "Mana-Kosteneffizienz", "Manakosteneffizienz",
    "Trefferchance", "Krit. Trefferchance", "Kritische Trefferchance",
    "Chance auf starken Angriff", "Chance auf Fesseln", "Schwächungschance", "Schwaechungschance",
    "Nahkampfausweichen", "Fernkampfausweichen", "Magieausweichen",
    "Nahkampfausdauer", "Fernkampfausdauer", "Magieausdauer",
    "Buff-Dauer", "Debuff-Dauer", "Angriffstempo",
    "Untote-Zusatzschaden", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden", "Wildkin-Bonusschaden",
    "Manakosteneffizienz", "Manakosteneffizienz",
]


def _canon_armor_trait_label(label: str) -> str:
    low = clean_text(label).lower().strip(" :")
    aliases = {
        "max. leben": "Max. Gesundheit",
        "kritische trefferchance": "Krit. Trefferchance",
        "mana-regeneration": "Manaregeneration",
        "manakosteneffizienz": "Mana-Kosteneffizienz",
        "schwaechungschance": "Schwächungschance",
    }
    if low in aliases:
        return aliases[low]
    for cand in ARMOR_TRAIT_LABELS_DE:
        if low == cand.lower().strip(" :"):
            return cand
    return clean_text(label).rstrip(":")


def _parse_armor_traits_from_text_sequence(tokens: list[str], expected: int) -> list[dict[str, Any]]:
    """Liest Questlog-Eigenschaften aus einer echten Text-Reihenfolge.

    Der Punkt, der vorher kaputt war: wir dürfen nicht versuchen, aus dem kompletten
    Body einzelne Regex-Funde zu raten. Questlog rendert den linken Itemkasten aber
    immer in der gleichen Reihenfolge:

        Eigenschaften:
        Label
        150 | 300 | 450 | 600
        Label
        40 | 80 | 120 | 160

    Manchmal stehen Label und Werte in einer Zeile. Diese Funktion kann beides.
    """
    if expected <= 0:
        return []

    # Labels lang -> kurz, damit "Krit. Trefferchance" vor "Trefferchance" gewinnt.
    labels = sorted(set(ARMOR_TRAIT_LABELS_DE), key=len, reverse=True)
    label_lows = [(x.lower().strip(" :"), x) for x in labels]

    def find_label(text: str) -> tuple[str, str] | None:
        t = clean_text(text).strip()
        low = t.lower().strip(" :")
        if not t:
            return None
        for low_label, label in label_lows:
            if low == low_label:
                return _canon_armor_trait_label(label), ""
            if low.startswith(low_label + ":"):
                return _canon_armor_trait_label(label), t[len(label):].lstrip(" :")
            if low.startswith(low_label + " "):
                rest = t[len(label):].strip()
                # Nur als Inline werten, wenn danach wirklich Zahlen kommen.
                if re.search(r"\d", rest):
                    return _canon_armor_trait_label(label), rest
        return None

    def nums(text: str) -> list[str]:
        # Traits haben Werte wie 150, 300 oder -1,5 %, 3 %, 12 %.
        return [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", str(text or ""))]

    def is_stop(text: str) -> bool:
        low = clean_text(text).lower().strip(" :")
        return any(x in low for x in (
            "dieser gegenstand hat", "this item has", "ausrüstungseffekt", "ausruestungseffekt",
            "ausrüstungsset", "ausruestungsset", "set des", "verkaufspreis", "sales price",
            "kommentare", "comments", "von npcs erbeutet", "dropped from", "in lithographen",
            "lithograph", "remove ads", "auction house", "auktionshaus", "preisverlauf", "bestandsverlauf",
            "karte", "map", "stats", "enchanting", "teilen", "share",
        ))

    clean_tokens = [clean_text(t).strip() for t in tokens if clean_text(t).strip()]

    # Start exakt nach Eigenschaften suchen. Wenn nicht da, erster bekannter Trait.
    start = -1
    for i, t in enumerate(clean_tokens):
        if clean_text(t).lower().strip(" :") in {"eigenschaften", "traits"}:
            start = i + 1
            break
    if start < 0:
        for i, t in enumerate(clean_tokens):
            if find_label(t):
                start = i
                break
    if start < 0:
        return []

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    while i < len(clean_tokens) and len(out) < expected:
        t = clean_tokens[i]
        if is_stop(t):
            break
        found = find_label(t)
        if not found:
            i += 1
            continue

        label, inline = found
        key = label.lower().strip(" :")
        if key in seen:
            i += 1
            continue

        values: list[str] = []
        if inline:
            values.extend(nums(inline))

        j = i + 1
        while j < len(clean_tokens) and len(values) < 4:
            nxt = clean_tokens[j]
            if is_stop(nxt):
                break
            if find_label(nxt):
                break
            # Separators/empty dots ignorieren, Zahlen einsammeln.
            values.extend(nums(nxt))
            j += 1

        # Jede Questlog-Eigenschaft hat faktisch 4 Stufen. Wenn ein Layout nur 2 liefert,
        # nehmen wir sie trotzdem, aber nur aus dem echten Eigenschaften-Block.
        if len(values) >= 2:
            out.append({"name": label, "values": values[:4]})
            seen.add(key)
            i = max(j, i + 1)
            continue
        i += 1

    return out[:expected]




def _parse_armor_traits_by_label_windows(raw_text: str, expected: int) -> list[dict[str, Any]]:
    """Letzter robuster Fallback für Questlog-Rüstungs-Eigenschaften.

    Arbeitet wie der Waffenparser: erst den echten Abschnitt ab "Eigenschaften" isolieren,
    dann bekannte Trait-Labels nach ihrer tatsächlichen Reihenfolge im Abschnitt finden.
    Pro Label werden die Zahlen bis zum nächsten bekannten Label gelesen. Dadurch ist es egal,
    ob Questlog die Zeile als "Label: 1 | 2 | 3 | 4" oder als mehrere DOM-Textnodes rendert.
    """
    if expected <= 0:
        return []
    raw = normalize_raw_text(raw_text)
    low = raw.lower()
    start = low.find("eigenschaften")
    if start < 0:
        start = low.find("traits")
    if start < 0:
        return []

    segment = raw[start:]
    seg_low = segment.lower()
    stop_markers = [
        "dieser gegenstand hat", "this item has", "ausrüstungseffekte", "ausruestungseffekte",
        "ausrüstungseffekt", "ausruestungseffekt", "ausrüstungsset", "ausruestungsset",
        "verkaufspreis", "sales price", "kommentare", "comments", "von npcs erbeutet",
        "dropped from", "in lithographen", "lithograph", "auktion", "auction house",
        "preisverlauf", "bestandsverlauf", "remove ads",
    ]
    cut = len(segment)
    for marker in stop_markers:
        idx = seg_low.find(marker)
        if idx > 0:
            cut = min(cut, idx)
    segment = segment[:cut]

    # Varianten/Schreibweisen. Kanonische Namen bleiben deutsch fürs Dashboard.
    labels = [
        "Max. Gesundheit", "Max. Leben", "Max. Mana",
        "Gesundheitsregeneration", "Manaregeneration", "Mana-Regeneration",
        "Mana-Kosteneffizienz", "Manakosteneffizienz", "Manakosten-Effizienz",
        "Trefferchance", "Krit. Trefferchance", "Kritische Trefferchance",
        "Chance auf starken Angriff", "Chance auf Fesseln", "Schwächungschance", "Schwaechungschance",
        "Nahkampfausweichen", "Fernkampfausweichen", "Magieausweichen",
        "Nahkampfausdauer", "Fernkampfausdauer", "Magieausdauer",
        "Buff-Dauer", "Debuff-Dauer", "Angriffstempo",
        "Untote-Zusatzschaden", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
        "Wildkin-Bonusschaden", "Wildkin Bonus Damage",
    ]
    # längere Labels zuerst verhindert, dass "Trefferchance" in "Krit. Trefferchance" matcht.
    labels_sorted = sorted(set(labels), key=len, reverse=True)

    matches: list[tuple[int, int, str]] = []
    for label in labels_sorted:
        pattern = re.compile(r"(?<![A-Za-zÄÖÜäöüß])" + re.escape(label) + r"\s*:?")
        for m in pattern.finditer(segment):
            canon = _canon_armor_trait_label(label)
            matches.append((m.start(), m.end(), canon))

    if not matches:
        return []

    # Doppelte/überlappende Matches entfernen. Wenn zwei an gleicher Position starten,
    # gewinnt das längere Label, weil labels_sorted nach Länge sortiert ist.
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    cleaned: list[tuple[int, int, str]] = []
    occupied_until = -1
    for m in matches:
        if m[0] < occupied_until:
            continue
        cleaned.append(m)
        occupied_until = m[1]
    matches = cleaned

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, (start_pos, end_pos, label) in enumerate(matches):
        key = label.lower().strip(" :")
        if key in seen:
            continue
        next_pos = matches[idx + 1][0] if idx + 1 < len(matches) else len(segment)
        value_text = segment[end_pos:next_pos]
        nums = [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", value_text)]
        # Jede Eigenschaft hat 4 Stufen. Falls Questlog Prozentwerte mit Komma rendert, bleibt das als Text erhalten.
        if len(nums) >= 4:
            out.append({"name": label, "values": nums[:4]})
            seen.add(key)
        if len(out) >= expected:
            break
    return out[:expected]


def extract_armor_traits_from_dom_textnodes(page, *, sub_category: str | None, item_level: Any, item_name: str) -> list[dict[str, Any]]:
    """Extrahiert Armor-Eigenschaften direkt aus sichtbaren Textnodes des linken Itemkastens.

    Das ist absichtlich derselbe robuste Ansatz wie bei Waffen: erst der echte
    Questlog-Detailkasten, dann exakt der Eigenschaften-Abschnitt. Keine globale
    Body-Regex mehr.
    """
    armor_types = {"helm", "brust", "hose", "handschuhe", "schuhe", "umhang"}
    if str(sub_category or "").strip().lower() not in armor_types:
        return []
    expected = _armor_expected_trait_count_from_level(item_level)

    try:
        tokens = page.evaluate(
            """
            (itemName) => {
              const clean = (s) => (s || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (!st || st.display === 'none' || st.visibility === 'hidden' || Number(st.opacity || 1) === 0) return false;
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
              };
              const all = Array.from(document.querySelectorAll('body *'));
              const namePart = clean(itemName).split(/\s+/).slice(0, 2).join(' ');
              let candidates = all.map(el => ({el, text: clean(el.innerText || '')}))
                .filter(x => x.text && x.text.includes('Eigenschaften') && x.text.length < 7000)
                .filter(x => !/Preisverlauf|Bestandsverlauf|Durchschnittspreis|Auf Lager/.test(x.text));
              if (namePart) candidates = candidates.filter(x => x.text.includes(namePart) || candidates.length < 3);
              candidates.sort((a, b) => a.text.length - b.text.length);
              const root = (candidates[0] && candidates[0].el) || document.body;

              const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
                acceptNode(node) {
                  const txt = clean(node.nodeValue || '');
                  if (!txt) return NodeFilter.FILTER_REJECT;
                  const p = node.parentElement;
                  if (!visible(p)) return NodeFilter.FILTER_REJECT;
                  return NodeFilter.FILTER_ACCEPT;
                }
              });
              const out = [];
              let n;
              while ((n = walker.nextNode())) {
                const txt = clean(n.nodeValue || '');
                if (txt) out.push(txt);
              }
              return out;
            }
            """,
            item_name or "",
        )
    except Exception:
        tokens = []

    if not isinstance(tokens, list):
        return []
    traits = _parse_armor_traits_from_text_sequence([str(x) for x in tokens], expected)
    return traits[:expected]

def _line_values_after_label(lines: list[str], label_idx: int, max_lookahead: int = 8) -> list[str]:
    vals: list[str] = []
    stop_words = {
        "passiv", "passive", "eigenschaften", "traits", "kommentare", "comments",
        "karte", "map", "auktionshaus", "auction house", "stats", "enchanting",
    }
    for j in range(label_idx + 1, min(len(lines), label_idx + 1 + max_lookahead)):
        x = clean_text(lines[j]).strip(" :")
        if not x:
            continue
        low = x.lower()
        if low in stop_words:
            break
        # Ein neues Label beendet den aktuellen Wertblock.
        if re.match(r"^[A-Za-zÄÖÜäöüß ./%'\-]+:$", x) and not re.search(r"\d", x):
            break
        vals.append(x)
    return vals


def parse_number_token(value: str) -> str:
    m = re.search(r"[+\-]?\d+(?:[.,]\d+)?\s*(?:%|m|s|Sek\.)?", str(value or ""), flags=re.I)
    return clean_text(m.group(0)) if m else ""


def _looks_like_delta_token(value: str) -> bool:
    text = str(value or "").strip().lower()
    return "▲" in text or "△" in text or "arrow" in text or text.startswith("+")


def _number_entries_from_values(values: list[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for raw in values:
        num = parse_number_token(raw)
        if not num:
            continue
        entries.append({"value": num, "raw": raw, "is_delta": _looks_like_delta_token(raw)})
    return entries


def find_label_value_block(lines: list[str], labels: list[str], max_lookahead: int = 8) -> dict[str, Any] | None:
    label_lows = [x.lower().rstrip(":") for x in labels]
    for i, line in enumerate(lines):
        low = clean_text(line).lower().rstrip(":")
        if low not in label_lows:
            continue
        vals: list[str] = []
        stop_words = {
            "passiv", "passive", "eigenschaften", "traits", "kommentare", "comments",
            "karte", "map", "auktionshaus", "auction house", "stats", "enchanting",
        }
        for j in range(i + 1, min(len(lines), i + 1 + max_lookahead)):
            x = clean_text(lines[j]).strip(" :")
            if not x:
                continue
            low_x = x.lower().rstrip(":")
            if low_x in stop_words:
                break
            # Gleiches oder anderes Label beendet den aktuellen Wertblock.
            if low_x in label_lows:
                break
            if re.match(r"^[A-Za-zÄÖÜäöüß ./%'\-]+:$", x) and not re.search(r"\d", x):
                break
            vals.append(x)
        entries = _number_entries_from_values(vals)
        nums = [e["value"] for e in entries]
        if not nums:
            continue
        return {"label": clean_text(line).rstrip(":"), "values": nums, "entries": entries, "raw_values": vals}
    return None


def extract_questlog_detail_model(text: str, *, name: str, rarity: str | None, sub_category: str | None, image_url: str | None) -> dict[str, Any]:
    """Baut eine Questlog-nahe Detailstruktur aus der sichtbaren Itemseite.

    Ziel ist nicht nur eine flache Stats-Liste, sondern Daten wie auf Questlog:
    Item-Level, Max. Schaden mit Range, Reichweite, Angriffstempo, Passiv,
    Zusatzstats und Eigenschaften/Traits.
    """
    raw = normalize_raw_text(text)
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", raw) if clean_text(x)]
    detail: dict[str, Any] = {
        "name": name,
        "rarity": rarity,
        "type": sub_category,
        "image_url": image_url,
        "primary": [],
        "bonus_stats": [],
        "traits": [],
    }

    # Item Level 50 (21-50) / Item Level 21 (Fixed Level) / Gegenstandsstufe ...
    m = re.search(r"(?:Item\s*Level|Gegenstandsstufe)\s*(\d+)(?:\s*\(([^)]*)\))?", raw, flags=re.I)
    if m:
        detail["item_level"] = int(m.group(1))
        if m.group(2):
            detail["item_level_range"] = clean_text(m.group(2))

    # Rüstungs-Hauptwerte / Defense
    # Questlog zeigt bei Rüstung meist zwei getrennte Hauptwerte, z. B.
    # Nahkampfverteidigung 530 ▲152 und Fernkampfverteidigung 581 ▲167.
    # Der alte Parser hat daraus nur ein generisches "DEF" gemacht.
    armor_defense_labels = [
        ("Nahkampfverteidigung", ["Nahkampfverteidigung", "Melee Defense"]),
        ("Fernkampfverteidigung", ["Fernkampfverteidigung", "Ranged Defense"]),
        ("Magieverteidigung", ["Magieverteidigung", "Magic Defense"]),
    ]
    parsed_specific_defense = False
    for out_label, labels in armor_defense_labels:
        block = find_label_value_block(lines, labels, max_lookahead=5)
        if not block:
            mm = re.search(rf"{re.escape(out_label)}\s*:?[\s\n]+([+\-]?\d+(?:[.,]\d+)?)(?:\s*▲\s*([+\-]?\d+(?:[.,]\d+)?))?", raw, flags=re.I)
            if mm:
                val = clean_text(mm.group(1))
                delta = clean_text(mm.group(2) or "")
                detail.setdefault("defenses", {})[out_label] = {"value": val, "delta": delta}
                detail["primary"].append({"label": out_label, "value": val, "delta": delta})
                parsed_specific_defense = True
            continue
        entries = block.get("entries") or _number_entries_from_values(block.get("raw_values") or [])
        base_nums = [str(e.get("value")) for e in entries if not e.get("is_delta")]
        deltas = [str(e.get("value")) for e in entries if e.get("is_delta")]
        if base_nums:
            detail.setdefault("defenses", {})[out_label] = {"value": base_nums[0], "delta": deltas[0] if deltas else "", "raw": block.get("raw_values")}
            detail["primary"].append({"label": out_label, "value": base_nums[0], "delta": deltas[0] if deltas else "", "raw": block.get("raw_values")})
            parsed_specific_defense = True

    # Fallback für alte/andere Layouts: generische Verteidigung nur nutzen,
    # wenn keine spezifische Rüstungsverteidigung erkannt wurde.
    if not parsed_specific_defense:
        defense_block = find_label_value_block(lines, ["Verteidigung", "Defense", "Rüstung", "Ruestung", "Armor"], max_lookahead=8)
        if defense_block:
            entries = defense_block.get("entries") or _number_entries_from_values(defense_block.get("raw_values") or [])
            base_nums = [str(e.get("value")) for e in entries if not e.get("is_delta")]
            deltas = [str(e.get("value")) for e in entries if e.get("is_delta")]
            if base_nums:
                detail["defense"] = {"value": base_nums[0], "delta": deltas[0] if deltas else "", "raw": defense_block.get("raw_values")}
                detail["primary"].append({"label": "Verteidigung", "value": base_nums[0], "delta": deltas[0] if deltas else "", "raw": defense_block.get("raw_values")})

    # Hauptwerte
    dmg = find_label_value_block(lines, ["Max. Schaden", "Max Damage", "Schaden", "Damage"], max_lookahead=10)
    if dmg:
        entries = dmg.get("entries") or _number_entries_from_values(dmg.get("raw_values") or [])
        # Questlog zeigt oft:
        # Max. Schaden / 70 / ▲ 22 / ~ / 277 / ▲ 88
        # Wichtig: ▲-Werte sind Upgrade-Deltas und dürfen nicht als Max-Schaden gelesen werden.
        base_nums = [str(e.get("value")) for e in entries if not e.get("is_delta")]
        deltas = [str(e.get("value")) for e in entries if e.get("is_delta")]
        if len(base_nums) >= 2:
            detail["max_damage"] = {"min": base_nums[0], "max": base_nums[1], "deltas": deltas[:2], "raw": dmg.get("raw_values")}
            detail["primary"].append({"label": "Max. Schaden", "value": f"{base_nums[0]} ~ {base_nums[1]}", "deltas": deltas[:2], "raw": dmg.get("raw_values")})
        elif base_nums:
            detail["primary"].append({"label": "Max. Schaden", "value": " ~ ".join(base_nums), "deltas": deltas[:2], "raw": dmg.get("raw_values")})

    for labels, out_label in [
        (["Reichweite", "Range"], "Reichweite"),
        (["Angriffstempo", "Angriffsgeschwindigkeit", "Attack Speed"], "Angriffstempo"),
    ]:
        block = find_label_value_block(lines, labels, max_lookahead=4)
        if block:
            val = (block.get("values") or [""])[0]
            if val:
                detail["primary"].append({"label": out_label, "value": val, "raw": block.get("raw_values")})
                detail[out_label.lower().replace(".", "").replace(" ", "_")] = val

    # Passive/Fähigkeit: Marker -> Name -> Beschreibung bis zum nächsten bekannten Abschnitt.
    passive_markers = {"passiv", "passive"}
    stat_label_lows = {
        "stärke", "geschicklichkeit", "weisheit", "wahrnehmung", "trefferchance", "krit. trefferchance",
        "kritische trefferchance", "abklingtempo", "abklingzeit", "spezies-schadensbonus", "wildkin-bonusschaden",
        "untote-zusatzschaden", "humanoide-zusatzschaden", "konstrukt-zusatzschaden", "schildgesundheit", "shield health",
        "zusatzschaden", "bonusschaden", "chance auf zweitwaffenangriff", "off-hand weapon attack chance",
        "eigenschaften", "traits",
        "max. gesundheit", "max. leben", "max. mana", "manaregeneration", "trefferchance", "chance auf",
    }
    for i, line in enumerate(lines):
        low = line.lower().strip(" :")
        if low not in passive_markers:
            continue
        p_name = ""
        desc_parts: list[str] = []
        for j in range(i + 1, min(len(lines), i + 12)):
            x = clean_text(lines[j])
            if not x:
                continue
            xl = x.lower().strip(" :")
            if xl in stat_label_lows or xl in {"eigenschaften", "traits"}:
                break
            if not p_name and not re.fullmatch(r"[+\-]?\d+(?:[.,]\d+)?%?", x):
                p_name = x
                continue
            desc_parts.append(x)
        if p_name or desc_parts:
            detail["passive"] = {"name": p_name, "text": " ".join(desc_parts).strip()}
            break

    # Bonus-Stats: Label-Zeile, danach Wert und optional ▲-Delta.
    bonus_labels = [
        "Stärke", "Geschicklichkeit", "Weisheit", "Wahrnehmung", "Standhaftigkeit",
        "Trefferchance", "Nahkampftrefferchance", "Fernkampftrefferchance", "Magietrefferchance",
        "Krit. Trefferchance", "Kritische Trefferchance", "Krit. Nahkampftrefferchance",
        "Krit. Fernkampftrefferchance", "Krit. Magietrefferchance",
        "Schwerer Angriff Chance", "Schwerer-Angriff-Chance", "Chance auf schweren Angriff", "Heavy Attack Chance",
        "Abklingtempo", "Abklingzeit", "Abklingzeittempo", "Cooldown Speed",
        "Angriffstempo", "Attack Speed", "Reichweite", "Range", "Zusatzschaden", "Bonusschaden", "Bonus Damage",
        "Spezies-Schadensbonus", "Fähigkeitsschaden-Bonus", "Skill Damage Bonus",
        "Chance auf Zweitwaffenangriff", "Off-Hand Weapon Attack Chance",
        "Schildgesundheit", "Shield Health", "Schildblockchance", "Shield Block Chance",
        "Wildkin-Bonusschaden", "Untote-Zusatzschaden", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
        "Max. Gesundheit", "Max. Leben", "Max. Mana", "Manaregeneration", "Mana-Kosteneffizienz",
    ]
    for label in bonus_labels:
        block = find_label_value_block(lines, [label], max_lookahead=4)
        if not block:
            # Falls Label und Wert auf einer Zeile stehen.
            mm = re.search(rf"{re.escape(label)}\s*:?\s*([+\-]?\d+(?:[.,]\d+)?\s*(?:%|m|s)?)", raw, flags=re.I)
            if mm:
                val = clean_text(mm.group(1))
                detail["bonus_stats"].append({"label": label, "value": val})
            continue
        vals = block.get("values") or []
        if vals:
            entry = {"label": label, "value": vals[0]}
            if len(vals) >= 2:
                entry["delta"] = vals[1]
            detail["bonus_stats"].append(entry)

    # Eigenschaften / Traits: Abschnitt nach Eigenschaften: strikt nur bis zum nächsten Questlog-Bereich.
    # Bei Rüstung stand danach z. B. Set-Effekt, Verkaufspreis, Ads usw. und wurde fälschlich als Trait gelesen.
    trait_start = -1
    for i, line in enumerate(lines):
        if line.lower().strip(" :") in {"eigenschaften", "traits"}:
            trait_start = i + 1
            break

    def _is_trait_stop_line(value: str) -> bool:
        low = clean_text(value).lower().strip(" :[]{}")
        if not low:
            return False
        exact_stops = {
            "kommentare", "comments", "used in litographs", "in litographen verwendet",
            "dropped from npcs", "von npcs erbeutet", "dropped from resources", "von ressourcen erbeutet",
            "map", "karte", "auction house", "auktionshaus", "stats", "enchanting",
            "remove ads", "learn more", "share", "teilen",
        }
        if low in exact_stops:
            return True
        # Settexte/Ads/Questlog-Bereiche können mitten im Text stehen, nicht nur am Anfang.
        stop_contains = (
            "ausrüstungseffekt", "ausruestungseffekt", "equipment set effect", "set effect",
            "verkaufspreis", "sales price", "remove ads", "sponsored", "enjoying questlog",
            "kommentare", "comments", "erbeutet", "litograph", "lithograph", "verwendet",
        )
        if any(p in low for p in stop_contains):
            return True
        prefixes = (
            "dieser gegenstand hat", "this item has", "set:", "ausrüstungsset", "ausruestungsset",
        )
        return any(low.startswith(p) for p in prefixes)

    def _valid_trait_label(value: str) -> bool:
        low = clean_text(value).lower().strip(" :")
        if not low or _is_trait_stop_line(low):
            return False
        if len(low) > 45:
            return False
        bad_bits = ["ads", "questlog", "werbung", "sponsored", "set", "ausrüstungseffekt", "ausruestungseffekt"]
        if any(b in low for b in bad_bits):
            return False
        # Nur echte Questlog-Eigenschaftsnamen zulassen. Dadurch werden Set-/Lore-Zeilen nicht als Trait gespeichert.
        allowed_traits = {
            "max. gesundheit", "max. leben", "max. mana", "manaregeneration", "mana-kosteneffizienz",
            "trefferchance", "krit. trefferchance", "kritische trefferchance",
            "chance auf starken angriff", "chance auf fesseln", "schwächungschance", "schwaechungschance",
            "nahkampfausweichen", "fernkampfausweichen", "magieausweichen",
            "nahkampfausdauer", "fernkampfausdauer", "magieausdauer",
            "buff-dauer", "debuff-dauer", "angriffstempo",
            "wildkin-bonusschaden", "untote-zusatzschaden", "humanoide-zusatzschaden", "konstrukt-zusatzschaden",
            "mana-regeneration", "manakosteneffizienz",
        }
        if low not in allowed_traits and "-zusatzschaden" not in low and "bonusschaden" not in low:
            return False
        # Traits sind kurze Werte-Namen, keine ganzen Sätze.
        if re.search(r"[.!?]", low):
            return False
        return True

    if trait_start >= 0:
        i = trait_start
        while i < len(lines):
            label = clean_text(lines[i]).rstrip(":")
            low = label.lower()
            if _is_trait_stop_line(label):
                break
            if not label or re.search(r"^(\||▲|~)$", label):
                i += 1
                continue
            # Trait-Name hat meistens keine Zahl, Werte danach schon.
            if not re.search(r"\d", label) and _valid_trait_label(label):
                vals: list[str] = []
                j = i + 1
                while j < len(lines) and j < i + 10:
                    v = clean_text(lines[j]).strip("|")
                    vl = v.lower().rstrip(":")
                    if not v:
                        j += 1
                        continue
                    if _is_trait_stop_line(v):
                        j = len(lines)
                        break
                    # Neues Label ohne Zahl beendet den aktuellen Wertblock.
                    if not re.search(r"\d", v) and v != "|":
                        break
                    if v != "|":
                        num = parse_number_token(v)
                        if num:
                            vals.append(num)
                    j += 1
                # Normale Trait-Reihen bei Questlog haben meistens 4 Werte, manche 2/6.
                # Einzelne Fake-Paare aus Settext/Ads werden dadurch nicht mehr übernommen.
                if len(vals) >= 2 and len(vals) <= 8:
                    detail["traits"].append({"name": label, "values": vals[:8]})
                    i = j
                    continue
            i += 1


    # Rüstungs-Zusatzwerte nach Questlog-Regel begrenzen.
    # Muster aus Questlog-Rüstungsseiten:
    # - Item Level 21: 2 Zusatzwerte nach den DEF-Werten
    # - Item Level 31/45/50: 4 Zusatzwerte
    # - Item Level 80: 5 Zusatzwerte
    # Dadurch werden Settexte, Verkaufspreise, Ads oder Trait-Werte nicht mehr als Zusatzwerte gelesen.
    def _armor_expected_bonus_count(level_value: Any) -> int | None:
        try:
            lvl = int(str(level_value or "0"))
        except Exception:
            return None
        if lvl >= 80:
            return 5
        if lvl in {31, 45, 50}:
            return 4
        if lvl <= 21 and lvl > 0:
            return 2
        return 4 if lvl else None

    def _parse_armor_bonus_stats_by_level() -> list[dict[str, Any]]:
        armor_types = {"helm", "brust", "hose", "handschuhe", "schuhe", "umhang"}
        if str(sub_category or "").strip().lower() not in armor_types:
            return []
        expected = _armor_expected_bonus_count(detail.get("item_level"))
        if not expected:
            return []

        # Nur die echten Zusatzwert-Namen zulassen. Keine Eigenschaften, keine Settexte.
        allowed_labels = [
            "Stärke", "Geschicklichkeit", "Weisheit", "Wahrnehmung",
            "Standhaftigkeit", "Ausdauer", "Schadensverminderung",
            "Trefferchance", "Krit. Trefferchance", "Kritische Trefferchance",
            "Max. Gesundheit", "Max. Leben", "Max. Mana", "Manaregeneration",
            "Mana-Kosteneffizienz", "Abklingtempo", "Abklingzeit",
            "Nahkampfausweichen", "Fernkampfausweichen", "Magieausweichen",
            "Nahkampfausdauer", "Fernkampfausdauer", "Magieausdauer",
            "Buff-Dauer", "Debuff-Dauer",
            "Wildkin-Bonusschaden", "Untote-Zusatzschaden", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
        ]
        allowed_map = {x.lower().strip(" :"): x for x in allowed_labels}
        primary_lows = {
            "nahkampfverteidigung", "fernkampfverteidigung", "magieverteidigung",
            "verteidigung", "def", "wert", "max. schaden", "reichweite", "angriffstempo",
            "angriffsgeschwindigkeit", "passiv", "passive", "eigenschaften", "traits",
        }

        # Zusatzwerte stehen auf Questlog vor Eigenschaften. Alles danach ignorieren.
        end = len(lines)
        for idx, line in enumerate(lines):
            if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
                end = idx
                break

        # Start ungefähr nach Item-Level/DEF. Wir scannen trotzdem ab oben, aber akzeptieren nur allowed labels.
        out: list[dict[str, Any]] = []
        seen_labels: set[str] = set()
        i = 0
        while i < end and len(out) < expected:
            raw_label = clean_text(lines[i]).rstrip(":")
            low = raw_label.lower().strip(" :")
            canonical = allowed_map.get(low)
            if not canonical or low in primary_lows:
                i += 1
                continue

            vals: list[str] = []
            j = i + 1
            while j < end and j < i + 5:
                v = clean_text(lines[j]).strip()
                vl = v.lower().strip(" :")
                if not v:
                    j += 1
                    continue
                # Neues Label oder Abschnitt beendet den aktuellen Wert.
                if vl in allowed_map or vl in primary_lows:
                    break
                if vl.startswith(("ausrüstungseffekt", "ausruestungseffekt", "verkaufspreis", "remove ads", "kommentare")):
                    break
                num = parse_number_token(v)
                if num:
                    vals.append(v)
                # Ein Basiswert + optional ▲ reicht für einen Zusatzwert.
                if len(vals) >= 2:
                    break
                j += 1

            entries = _number_entries_from_values(vals)
            base_nums = [str(e.get("value")) for e in entries if not e.get("is_delta")]
            deltas = [str(e.get("value")) for e in entries if e.get("is_delta")]
            if base_nums and canonical.lower() not in seen_labels:
                entry: dict[str, Any] = {"label": canonical, "value": base_nums[0]}
                if deltas:
                    entry["delta"] = deltas[0]
                out.append(entry)
                seen_labels.add(canonical.lower())
                i = max(j, i + 1)
                continue
            i += 1

        return out[:expected]

    armor_level_bonus_stats = _parse_armor_bonus_stats_by_level()
    if armor_level_bonus_stats:
        detail["bonus_stats"] = armor_level_bonus_stats

    # Dedupe Bonusstats/Traits.
    seen = set(); b=[]
    for x in detail.get("bonus_stats") or []:
        key = (x.get("label"), x.get("value"), x.get("delta"))
        if key in seen: continue
        seen.add(key); b.append(x)
    detail["bonus_stats"] = b
    seen = set(); tr=[]
    for x in detail.get("traits") or []:
        key = (x.get("name"), tuple(x.get("values") or []))
        if key in seen: continue
        seen.add(key); tr.append(x)

    # Questlog-Rüstungen haben je nach Item-Level unterschiedlich viele mögliche
    # Eigenschafts-Reihen. Nicht mit Zusatzwerten verwechseln:
    # Zusatzwerte hängen ebenfalls vom Level ab, aber Traits/Eigenschaften haben
    # eine eigene Begrenzung. Dadurch schneiden wir Ads/Lore/Settext sauber ab.
    def _armor_expected_trait_count(level_value: Any) -> int:
        try:
            lvl = int(str(level_value or "0"))
        except Exception:
            lvl = 0
        if lvl in {45, 50} or lvl >= 80:
            return 8
        if lvl in {21, 31}:
            return 6
        # Fallback für seltene Zwischenstufen: lieber konservativ, aber nicht 5.
        return 8 if lvl >= 45 else 6

    trait_limit = _armor_expected_trait_count(detail.get("item_level"))

    def _reparse_armor_traits_by_expected_count() -> list[dict[str, Any]]:
        """Armor traits robust erfassen.

        Questlog streut auf manchen Detailseiten Buttons/Ads/Settexte zwischen die
        Eigenschaftszeilen. Deshalb darf "Remove Ads" nicht als harter Stop
        zählen, solange die erwartete Anzahl noch nicht erreicht ist.
        Die Anzahl kommt aus der Item-Level-Regel:
        Level 21/31 = 6, Level 45/50/80 = 8.
        """
        armor_types = {"helm", "brust", "hose", "handschuhe", "schuhe", "umhang"}
        if str(sub_category or "").strip().lower() not in armor_types:
            return []

        start = -1
        for idx, line in enumerate(lines):
            if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
                start = idx + 1
                break
        if start < 0:
            return []

        out: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        i = start
        hard_stop_contains = (
            "kommentare", "comments", "dropped from", "erbeutet",
            "used in litograph", "litographen", "verwendet",
        )

        while i < len(lines) and len(out) < trait_limit:
            label = clean_text(lines[i]).rstrip(":")
            low = label.lower().strip(" :")

            if not label:
                i += 1
                continue

            # Harte Seitenbereiche beenden erst, wenn wir die erwartete Menge schon haben.
            # Vorher überspringen wir Noise und suchen weiter.
            if any(x in low for x in hard_stop_contains):
                if len(out) >= trait_limit:
                    break
                i += 1
                continue

            # Ads/Buttons/Settexte nicht als Trait nehmen, aber auch nicht zu früh abbrechen.
            if _is_trait_stop_line(label):
                i += 1
                continue

            # Questlog rendert manche Eigenschaften als eine einzige Zeile:
            # "Max. Mana: 150 | 300 | 450 | 600". Der alte Parser erwartete
            # Label und Werte getrennt in Folgezeilen und hat diese Zeile dadurch
            # komplett verloren. Genau deshalb fehlte bei Level-21-Rüstung oft
            # die 6. Eigenschaft.
            inline_label = ""
            inline_values_text = ""
            m_inline = re.match(r"^(.+?)\s*:\s*(.+)$", label)
            if m_inline:
                inline_label = clean_text(m_inline.group(1)).rstrip(":")
                inline_values_text = clean_text(m_inline.group(2))
            else:
                # Fallback für Layouts ohne Doppelpunkt: "Max. Mana 150 | 300 | 450 | 600"
                known_inline_trait_labels = [
                    "Max. Gesundheit", "Max. Leben", "Max. Mana", "Manaregeneration",
                    "Mana-Kosteneffizienz", "Trefferchance", "Krit. Trefferchance",
                    "Kritische Trefferchance", "Chance auf starken Angriff", "Chance auf Fesseln",
                    "Schwächungschance", "Schwaechungschance", "Nahkampfausweichen",
                    "Fernkampfausweichen", "Magieausweichen", "Nahkampfausdauer",
                    "Fernkampfausdauer", "Magieausdauer", "Buff-Dauer", "Debuff-Dauer",
                    "Angriffstempo", "Wildkin-Bonusschaden", "Untote-Zusatzschaden",
                    "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
                ]
                for candidate in sorted(known_inline_trait_labels, key=len, reverse=True):
                    if low.startswith(candidate.lower() + " "):
                        inline_label = candidate
                        inline_values_text = label[len(candidate):].strip()
                        break

            if inline_label and _valid_trait_label(inline_label):
                inline_vals = [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", inline_values_text)]
                if len(inline_vals) >= 2 and inline_label.lower().strip(" :") not in seen_names:
                    out.append({"name": inline_label, "values": inline_vals[:8]})
                    seen_names.add(inline_label.lower().strip(" :"))
                    i += 1
                    continue

            if _valid_trait_label(label) and low not in seen_names:
                vals: list[str] = []
                j = i + 1
                while j < len(lines) and j < i + 14:
                    v = clean_text(lines[j]).strip("|")
                    vl = v.lower().strip(" :")
                    if not v:
                        j += 1
                        continue
                    if any(x in vl for x in hard_stop_contains):
                        break
                    if _is_trait_stop_line(v):
                        j += 1
                        continue
                    # Neues Trait-Label beendet diesen Werteblock.
                    if _valid_trait_label(v) and not re.search(r"\d", v):
                        break
                    num = parse_number_token(v)
                    if num:
                        vals.append(num)
                    if len(vals) >= 4:
                        break
                    j += 1

                if len(vals) >= 2:
                    out.append({"name": label, "values": vals[:8]})
                    seen_names.add(low)
                    i = max(j, i + 1)
                    continue

            i += 1

        return out[:trait_limit]

    def _parse_armor_traits_from_section_text() -> list[dict[str, Any]]:
        """Armor-Eigenschaften aus dem echten Itemkasten lesen.

        Diese Version arbeitet nicht mehr über frei schwebende Regex-Matches im
        kompletten Body. Sie isoliert den Eigenschaften-Abschnitt und scannt ihn
        zeilenweise gegen eine feste Liste echter Questlog-Traitnamen.
        Erwartete Menge: Level 21/31 = 6, Level 45/50/80 = 8.
        """
        armor_types = {"helm", "brust", "hose", "handschuhe", "schuhe", "umhang"}
        if str(sub_category or "").strip().lower() not in armor_types:
            return []

        trait_labels = [
            "Max. Gesundheit", "Max. Leben", "Max. Mana", "Manaregeneration", "Mana-Regeneration",
            "Mana-Kosteneffizienz", "Manakosteneffizienz", "Trefferchance", "Krit. Trefferchance",
            "Kritische Trefferchance", "Chance auf starken Angriff", "Chance auf Fesseln",
            "Schwächungschance", "Schwaechungschance", "Nahkampfausweichen", "Fernkampfausweichen",
            "Magieausweichen", "Nahkampfausdauer", "Fernkampfausdauer", "Magieausdauer",
            "Buff-Dauer", "Debuff-Dauer", "Angriffstempo", "Wildkin-Bonusschaden",
            "Untote-Zusatzschaden", "Humanoide-Zusatzschaden", "Konstrukt-Zusatzschaden",
        ]
        label_lows = [(x.lower().strip(" :"), x) for x in sorted(trait_labels, key=len, reverse=True)]

        def _is_known_trait_start(line: str) -> tuple[str, str] | None:
            txt = clean_text(line).strip()
            low = txt.lower().strip(" :")
            for low_label, canonical in label_lows:
                if low == low_label:
                    return canonical, ""
                # Inline: "Max. Mana: 150 | 300 | 450 | 600"
                if low.startswith(low_label + ":"):
                    return canonical, txt[len(canonical):].lstrip(" :")
                # Inline ohne Doppelpunkt: "Max. Mana 150 | 300 | 450 | 600"
                if low.startswith(low_label + " "):
                    return canonical, txt[len(canonical):].strip()
            return None

        # Abschnitt zwischen Eigenschaften und dem nächsten echten Questlog-Bereich.
        src_lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(raw)) if clean_text(x)]
        start_idx = -1
        for idx, line in enumerate(src_lines):
            if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
                start_idx = idx + 1
                break
        if start_idx < 0:
            # Fallback: wenn "Eigenschaften:" und erstes Trait in einer Zeile stehen.
            joined = "\n".join(src_lines)
            m = re.search(r"\bEigenschaften\b\s*:?(.*)$", joined, flags=re.I | re.S)
            if not m:
                return []
            src_lines = [clean_text(x) for x in re.split(r"[\n\r]+", m.group(1)) if clean_text(x)]
            start_idx = 0

        stop_words = (
            "dieser gegenstand hat", "this item has", "ausrüstungseffekte", "ausruestungseffekte",
            "ausrüstungsset", "ausruestungsset", "equipment set", "verkaufspreis", "sales price",
            "kommentare", "comments", "von npcs erbeutet", "dropped from", "in lithographen",
            "litograph", "remove ads", "auction house", "auktionshaus", "preisverlauf", "bestandsverlauf",
        )

        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        i = start_idx
        while i < len(src_lines) and len(out) < trait_limit:
            line = clean_text(src_lines[i])
            low = line.lower().strip(" :")
            if any(w in low for w in stop_words):
                break

            found = _is_known_trait_start(line)
            if not found:
                i += 1
                continue

            label, inline_values = found
            key = label.lower().strip(" :")
            if key in seen:
                i += 1
                continue

            vals: list[str] = []
            if inline_values:
                vals.extend([clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", inline_values)])

            j = i + 1
            while j < len(src_lines) and len(vals) < 4:
                nxt = clean_text(src_lines[j]).strip("|")
                nl = nxt.lower().strip(" :")
                if not nxt:
                    j += 1
                    continue
                if any(w in nl for w in stop_words):
                    break
                # nächster Trait beginnt -> aktueller Block fertig
                if _is_known_trait_start(nxt):
                    break
                nums = [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", nxt)]
                if nums:
                    vals.extend(nums)
                j += 1

            if len(vals) >= 2:
                out.append({"name": label, "values": vals[:4]})
                seen.add(key)
                i = max(j, i + 1)
                continue
            i += 1

        return out[:trait_limit]

    armor_traits_by_section = _parse_armor_traits_from_section_text()
    armor_traits_by_window = _parse_armor_traits_by_label_windows(raw, trait_limit)
    armor_traits_by_old = _reparse_armor_traits_by_expected_count()

    # Reihenfolge: exakter Abschnittsparser mit Label-Fenstern gewinnt.
    # Nur auf schwächere Parser zurückfallen, wenn sie mindestens die erwartete Menge liefern.
    if len(armor_traits_by_window) >= trait_limit:
        detail["traits"] = armor_traits_by_window[:trait_limit]
        detail["trait_count_source"] = "armor_label_window_section"
    elif len(armor_traits_by_section) >= trait_limit:
        detail["traits"] = armor_traits_by_section[:trait_limit]
        detail["trait_count_source"] = "armor_section_text"
    elif len(armor_traits_by_old) >= trait_limit:
        detail["traits"] = armor_traits_by_old[:trait_limit]
        detail["trait_count_source"] = "armor_old_reparse"
    else:
        # Wenn nicht genug gefunden wurde, trotzdem die beste Fundmenge nehmen,
        # aber nicht durch einen schlechteren Parser überschreiben.
        candidates = [armor_traits_by_window, armor_traits_by_section, armor_traits_by_old, tr]
        best = max(candidates, key=lambda arr: len(arr or []))
        detail["traits"] = (best or [])[:trait_limit]
        detail["trait_count_source"] = "armor_best_effort"

    detail["trait_count_rule"] = trait_limit
    detail["trait_count_observed"] = len(detail.get("traits") or [])
    return detail


def normalize_raw_text(text: str) -> str:
    text = str(text or "")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def collect_json_candidates(page) -> list[Any]:
    candidates: list[Any] = []
    try:
        raw = page.locator("script#__NEXT_DATA__").text_content(timeout=1000)
        if raw:
            candidates.append(json.loads(raw))
    except Exception:
        pass
    try:
        raw_list = page.locator('script[type="application/ld+json"]').evaluate_all("els => els.map(e => e.textContent || '')")
        for raw in raw_list or []:
            try:
                candidates.append(json.loads(raw))
            except Exception:
                pass
    except Exception:
        pass
    return candidates


def walk_json(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_json(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk_json(v)


def best_name_from_json(candidates: list[Any]) -> str:
    for obj in candidates:
        for d in walk_json(obj):
            for key in ("name", "title", "itemName"):
                value = d.get(key) if isinstance(d, dict) else None
                if isinstance(value, str) and 2 <= len(value) <= 120 and "Questlog" not in value:
                    return clean_text(value)
    return ""


def page_title(page) -> str:
    for selector in ["h1", "[data-testid*='title']", "main h2"]:
        try:
            val = page.locator(selector).first.inner_text(timeout=1500)
            val = clean_text(val)
            if val and len(val) <= 120:
                return val
        except Exception:
            pass
    try:
        og = page.locator('meta[property="og:title"]').get_attribute("content", timeout=1000)
        og = clean_text((og or "").split(" - ")[0])
        if og:
            return og
    except Exception:
        pass
    try:
        title = clean_text(page.title())
        title = re.sub(r"\s*[-|].*$", "", title).strip()
        return title
    except Exception:
        return ""


def collect_image_urls(page) -> tuple[str | None, str | None]:
    """Findet bevorzugt das echte Item-/Waffenbild auf Questlog-Detailseiten.

    Questlog hat auf der Detailseite mehrere kleine Bilder: Itembild oben rechts,
    Passiv-/Fähigkeitsicon, Typ-Icon, Ads, Map usw. Diese Funktion bevorzugt deshalb
    Kandidaten aus dem oberen Itemkarten-Bereich und bestraft lokale Passive-/Skill-Kontexte.
    """
    detail_id = ""
    try:
        detail_id = item_detail_id(page.url).lower()
    except Exception:
        detail_id = ""
    try:
        item_name = page_title(page).lower()
    except Exception:
        item_name = ""
    name_tokens = [t for t in re.split(r"[^a-zA-ZäöüÄÖÜß0-9]+", item_name) if len(t) >= 4][:8]
    candidates: list[dict[str, Any]] = []

    def add_candidate(
        src: Any,
        *,
        w: int = 0,
        h: int = 0,
        x: int = 0,
        y: int = 0,
        source: str = "",
        alt: str = "",
        local_context: str = "",
        broad_context: str = "",
    ) -> None:
        src = to_abs_url(str(src or "").strip())
        if not src or src.startswith("data:") or src.endswith(".svg"):
            return
        low = src.lower()
        hard_bad = [
            "doubleclick", "googleads", "googlesyndication", "adservice", "learning", "onechuman",
            "sponsored", "advertisement", "avatar", "profile", "logo", "favicon", "map", "worldmap",
            "youtube", "twitch", "discord", "premium", "removeads",
        ]
        if any(x in low for x in hard_bad):
            return

        local = f"{alt} {local_context}".lower()
        broad = f"{alt} {local_context} {broad_context}".lower()
        score = 0

        # Questlog-Assetmuster:
        # - echte Itembilder liegen fast immer unter .../Icon/Item_128/Equip/...
        # - Passiv-/Fähigkeitsbilder liegen oft unter .../Icon/Item_128/Misc/Perk_...
        #   und enthalten manchmal sogar die Item-ID. Deshalb muss Perk stärker verlieren
        #   als die ID gewinnen kann.
        is_equip_asset = "/icon/item_128/equip/" in low or "/equip/" in low
        is_weapon_asset = "/equip/weapon/" in low
        is_armor_asset = "/equip/armor/" in low or "/equip/accessory/" in low or "/equip/equipment/" in low
        is_perk_asset = "/misc/perk" in low or "perk_" in low

        if is_weapon_asset:
            score += 820
        elif is_armor_asset:
            score += 760
        elif is_equip_asset:
            score += 520

        if is_perk_asset:
            score -= 900

        # Bestes Signal nur noch, wenn es kein Perk-/Fähigkeitsasset ist.
        # Sonst gewinnt z.B. Perk_dagger_... fälschlich gegen IT_P_Dagger_....
        if detail_id and detail_id in low and not is_perk_asset:
            score += 220

        # Pfad-Hinweise. Skill-/Ability-Assets dürfen nicht gegen Itembilder gewinnen.
        if any(x in low for x in ["/item", "items", "equipment", "weapon", "armor"]):
            score += 70
        if "icon" in low:
            score += 10
        if any(x in low for x in ["skill", "ability", "passive", "buff", "spell", "trait"]):
            score -= 130
        if any(x in low for x in ["webp", "png", "jpg", "jpeg"]):
            score += 8

        # Quelle/Grafikgröße.
        if source == "css":
            score += 45
        elif source == "img":
            score += 20
        elif source == "srcset":
            score += 16
        elif source == "meta":
            score -= 30
        if w and h:
            area = w * h
            if 40 <= w <= 260 and 40 <= h <= 260:
                score += 35
            if area < 1800:
                score -= 25
            if area > 300000:
                score -= 80
            # Passiv-Icons sind meist kleine quadratische Bilder im unteren Bereich.
            if abs(w - h) <= 8 and 32 <= w <= 90 and y and y > 430:
                score -= 35

        # Position: Das Waffenbild sitzt oben in der Itemkarte; Passivicons deutlich darunter.
        if y:
            if 160 <= y <= 390:
                score += 45
            elif y > 430:
                score -= 25
        if x:
            # Ads links/rechts liegen oft weit außerhalb vom Content.
            if x < 250 or x > 1450:
                score -= 40

        # Lokaler Kontext ist entscheidend. Broad-Kontext kann die komplette Itemkarte enthalten
        # und darf deshalb nicht zu hart bestrafen.
        passive_words = ["passiv", "passive", "fähigkeit", "faehigkeit", "skill", "effekt", "effect"]
        trait_words = ["eigenschaften", "traits", "verkaufspreis", "kommentare", "erbeutet"]
        item_header_words = ["item level", "gegenstandsstufe", "max. schaden", "max damage", "reichweite", "angriffstempo"]
        if any(wd in local for wd in passive_words):
            score -= 160
        elif any(wd in broad for wd in passive_words):
            score -= 8
        if any(wd in local for wd in trait_words):
            score -= 80
        if any(wd in local for wd in item_header_words):
            score += 50
        elif any(wd in broad for wd in item_header_words):
            score += 22
        if item_name and item_name in broad:
            score += 25
        token_hits = sum(1 for t in name_tokens if t in broad)
        score += min(token_hits * 8, 32)

        candidates.append({
            "src": src,
            "w": int(w or 0),
            "h": int(h or 0),
            "x": int(x or 0),
            "y": int(y or 0),
            "source": source,
            "score": score,
        })

    try:
        imgs = page.locator("img").evaluate_all(
            """
            els => els.map(img => {
                let box = img.getBoundingClientRect();
                let p1 = img.parentElement;
                let p2 = p1 ? p1.parentElement : null;
                let p3 = p2 ? p2.parentElement : null;
                return {
                    src: img.currentSrc || img.src || img.getAttribute('data-src') || '',
                    srcset: img.srcset || img.getAttribute('data-srcset') || '',
                    alt: img.alt || '',
                    w: Math.round(img.naturalWidth || box.width || img.width || 0),
                    h: Math.round(img.naturalHeight || box.height || img.height || 0),
                    x: Math.round(box.left || 0),
                    y: Math.round(box.top || 0),
                    local: ((p1 && p1.innerText) || '') + ' ' + ((p2 && p2.innerText) || ''),
                    broad: ((p3 && p3.innerText) || '')
                };
            })
            """
        )
        for img in imgs or []:
            add_candidate(
                img.get("src"),
                w=int(img.get("w") or 0), h=int(img.get("h") or 0),
                x=int(img.get("x") or 0), y=int(img.get("y") or 0),
                source="img", alt=str(img.get("alt") or ""),
                local_context=str(img.get("local") or ""), broad_context=str(img.get("broad") or ""),
            )
            srcset = str(img.get("srcset") or "")
            for part in srcset.split(","):
                first = part.strip().split(" ")[0]
                if first:
                    add_candidate(
                        first,
                        w=int(img.get("w") or 0), h=int(img.get("h") or 0),
                        x=int(img.get("x") or 0), y=int(img.get("y") or 0),
                        source="srcset", alt=str(img.get("alt") or ""),
                        local_context=str(img.get("local") or ""), broad_context=str(img.get("broad") or ""),
                    )
    except Exception:
        pass

    try:
        backgrounds = page.locator("*").evaluate_all(
            """
            els => els.slice(0, 3500).map(e => {
              const st = getComputedStyle(e);
              const box = e.getBoundingClientRect();
              const p1 = e.parentElement;
              const p2 = p1 ? p1.parentElement : null;
              return {
                bg: st.backgroundImage || '',
                w: Math.round(box.width || 0),
                h: Math.round(box.height || 0),
                x: Math.round(box.left || 0),
                y: Math.round(box.top || 0),
                local: (e.innerText || '').trim().slice(0, 600),
                broad: (((p1 && p1.innerText) || '') + ' ' + ((p2 && p2.innerText) || '')).trim().slice(0, 1000)
              };
            }).filter(x => x.bg && x.bg !== 'none')
            """
        )
        for bg in backgrounds or []:
            for m in re.finditer(r"url\([\"']?([^\"')]+)[\"']?\)", str(bg.get("bg") or "")):
                add_candidate(
                    m.group(1),
                    w=int(bg.get("w") or 0), h=int(bg.get("h") or 0),
                    x=int(bg.get("x") or 0), y=int(bg.get("y") or 0),
                    source="css",
                    local_context=str(bg.get("local") or ""), broad_context=str(bg.get("broad") or ""),
                )
    except Exception:
        pass

    # Meta nur als letzter Fallback; oft ist das Preview/Share-Bild, nicht das Itembild.
    try:
        metas = page.locator("meta[property='og:image'], meta[name='twitter:image']").evaluate_all(
            "els => els.map(e => e.getAttribute('content') || '').filter(Boolean)"
        )
        for src in metas or []:
            add_candidate(src, source="meta")
    except Exception:
        pass

    seen: set[str] = set()
    valid: list[dict[str, Any]] = []
    for c in candidates:
        src = c.get("src")
        if not src or src in seen:
            continue
        seen.add(src)
        valid.append(c)
    if not valid:
        return None, None

    valid.sort(key=lambda x: (int(x.get("score") or 0), int(x.get("w") or 0) * int(x.get("h") or 0)), reverse=True)
    best = valid[0].get("src")
    return best, best



def hydrate_listing_page(page) -> None:
    """Laedt lazy/infinite Questlog-Listen komplett nach.

    Questlog zeigt auf Kategorie-Seiten initial oft nur ca. 40 Detail-Links.
    Weitere Items erscheinen erst nach Scrollen/Virtualisierung. Ohne diesen Schritt
    importieren Helm/Brust/etc. nur die ersten 40 Items.
    """
    try:
        last_count = -1
        stable_rounds = 0
        max_rounds = int(os.getenv("QUESTLOG_SCROLL_ROUNDS", "40") or "40")
        step_px = int(os.getenv("QUESTLOG_SCROLL_STEP", "900") or "900")
        wait_ms = int(os.getenv("QUESTLOG_SCROLL_WAIT_MS", "450") or "450")
    except Exception:
        max_rounds = 40
        step_px = 900
        wait_ms = 450

    for _ in range(max_rounds):
        try:
            current_count = len(collect_detail_links(page))
        except Exception:
            current_count = -1

        if current_count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = current_count

        # Zwei stabile Runden nach unten reichen meist. Nicht sofort abbrechen,
        # weil Questlog manchmal nach dem ersten Scroll noch nachrendert.
        if stable_rounds >= 3:
            break

        try:
            page.mouse.wheel(0, step_px)
        except Exception:
            try:
                page.evaluate("y => window.scrollBy(0, y)", step_px)
            except Exception:
                pass
        try:
            page.wait_for_timeout(wait_ms)
        except Exception:
            pass

    # Nochmal ganz nach unten und kurz warten, falls der letzte Block erst dann kommt.
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
    except Exception:
        pass

def collect_links(page) -> list[str]:
    try:
        links = page.locator("a[href]").evaluate_all("els => els.map(a => a.href).filter(Boolean)")
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for href in links or []:
        href = force_de_locale_url(str(href).split("#", 1)[0].rstrip("/"))
        if not is_items_url(href) or not is_de_questlog_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append(href)
    return out

def collect_detail_links(page) -> list[str]:
    """Sammelt echte Questlog-Item-Detailseiten.

    Wichtig: Questlog-Detailseiten liegen unter /db/item/<id> (singular).
    Die Unterseiten /db/items/weapons/bow sind nur Listen/Filter.
    """
    urls: list[str] = []
    seen: set[str] = set()

    def add(raw: Any) -> None:
        href = force_de_locale_url(to_abs_url(str(raw or "")).split("#", 1)[0].split("?", 1)[0].rstrip("/"))
        if not href or not is_item_detail_url(href) or not is_de_questlog_url(href):
            return
        low = href.lower()
        if any(hint in low for hint in NON_ITEM_URL_HINTS):
            return
        if href in seen:
            return
        seen.add(href)
        urls.append(href)

    try:
        links = page.locator("a[href]").evaluate_all("els => els.map(a => a.href).filter(Boolean)")
        for href in links or []:
            add(href)
    except Exception:
        pass

    # Fallback: Links stehen bei Next/React teils nur im HTML/Script.
    try:
        html_text = page.content()
    except Exception:
        html_text = ""
    for m in re.finditer(r"/(?:throne-and-liberty)/(?:[a-z]{2})/db/item/[A-Za-z0-9_.%\-]+", html_text):
        add(BASE + m.group(0))

    return urls


def find_next_url(page) -> str | None:
    """Findet echte Pagination-Links.

    Wichtig: Nicht mehr blind ?page=2 erfinden. Auf Questlog führte das auf
    Railway zu leeren/unnötigen Seiten und verwirrenden 0-Item-Imports.
    """
    try:
        entries = page.locator("a[href]").evaluate_all(
            """
            els => els.map(a => ({
              href: a.href,
              text: (a.innerText || '').trim().toLowerCase(),
              aria: (a.getAttribute('aria-label') || '').trim().toLowerCase(),
              rel: (a.getAttribute('rel') || '').trim().toLowerCase()
            }))
            """
        )
    except Exception:
        entries = []
    for e in entries or []:
        href = str(e.get("href") or "").split("#", 1)[0].rstrip("/")
        label = " ".join([str(e.get("text") or ""), str(e.get("aria") or ""), str(e.get("rel") or "")])
        if href and is_items_url(href) and any(x in label for x in ["next", "weiter", "nächste", "naechste"]):
            return href
    return None


def goto_page(page, url: str, timeout_ms: int | None = None) -> bool:
    """Robustes Laden für Questlog/Railway.

    Auf Railway bleibt Questlog teils lange vor DOMContentLoaded hängen. Für Scraping
    reicht oft schon der erste Response-Commit plus ein kurzer Warteblock, weil
    Playwright danach trotzdem DOM/JS ausführen kann. Deshalb nicht hart auf
    networkidle warten.
    """
    timeout_ms = int(timeout_ms or NAV_TIMEOUT_MS)
    try:
        page.set_default_navigation_timeout(timeout_ms)
    except Exception:
        pass

    last_exc: Exception | None = None
    for wait_until in ("commit", "domcontentloaded", "load"):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=min(15000, timeout_ms))
            except Exception:
                pass
            try:
                page.wait_for_timeout(PAGE_SETTLE_MS)
            except Exception:
                pass
            return True
        except Exception as exc:
            last_exc = exc
            try:
                page.wait_for_timeout(1000)
            except Exception:
                pass

    print(f"❌ Seite nicht erreichbar: {url} ({type(last_exc).__name__}: {last_exc})", flush=True)
    return False


def discover_category_seeds(page, locale: str, include_fallback: bool = True) -> list[CategorySeed]:
    roots = [
        f"{BASE}/{GAME}/{locale}/db/items",
        DEFAULT_START_URL,
    ]
    seeds: dict[str, CategorySeed] = {}
    for root in roots:
        if not goto_page(page, root, timeout_ms=NAV_TIMEOUT_MS):
            continue
        for href in collect_links(page):
            if is_category_list_url(href):
                main = classify_main_category(href)
                seeds[href] = CategorySeed(href, main)
    if include_fallback:
        for path in FALLBACK_CATEGORY_PATHS:
            url = force_weapon_grade_filter(f"{BASE}/{GAME}/{locale}/db/items/{path}")
            seeds.setdefault(url, CategorySeed(url, classify_main_category(url)))
    # Waffen zuerst, dann Rüstung, Material, Currency, Rest.
    order = {"weapon": 1, "armor": 2, "material": 3, "currency": 4, "misc": 9}
    return sorted(seeds.values(), key=lambda s: (order.get(s.main_category, 9), s.url))


def same_main_category(url: str, main_category: str) -> bool:
    return classify_main_category(url) == main_category


def is_locale_detail_url(url: str, locale: str = DEFAULT_LOCALE) -> bool:
    parts = path_parts(url)
    try:
        idx = parts.index(GAME)
        return len(parts) > idx + 3 and parts[idx + 1] == locale and parts[idx + 2] == "db" and parts[idx + 3] == "item"
    except ValueError:
        return False


def detail_matches_source_list(detail_url: str, list_url: str, main_category: str) -> bool:
    """Verhindert Cross-Imports zwischen Unterkategorien.

    Beispiel: /weapons/sword darf keine sword2h-Detailseiten speichern.
    Questlog verlinkt in React/HTML teils mehr als die sichtbare Liste. Für Waffen
    sind die Item-IDs aber stabil nach Typ geprefixt: bow_..., sword2h_..., usw.
    """
    if not is_locale_detail_url(detail_url, DEFAULT_LOCALE):
        return False
    slug = subcategory_slug(list_url)
    if main_category == "weapon" and slug in WEAPON_SLUG_TO_SUBCATEGORY:
        item_id = item_detail_id(detail_url).lower()
        return item_id.startswith(f"{slug.lower()}_")
    return True


def expand_seed_url(raw_url: str) -> list[str]:
    """Hauptlisten auf feste deutsche Unterlisten expandieren.

    Fuer Waffen gelten ausschließlich die 10 deutschen Links aus DEFAULT_WEAPON_CATEGORY_URLS.
    Fremdsprachen-URLs wie /ja/ werden niemals gecrawlt.
    """
    url = force_weapon_grade_filter(force_de_locale_url(raw_url.rstrip("/")))
    if classify_main_category(url) == "weapon" and is_category_list_url(url):
        return [force_weapon_grade_filter(u.rstrip("/")) for u in DEFAULT_WEAPON_CATEGORY_URLS]
    if classify_main_category(url) == "armor" and is_category_list_url(url):
        return [force_weapon_grade_filter(u.rstrip("/")) for u in DEFAULT_ARMOR_CATEGORY_URLS]
    if classify_main_category(url) == "accessory" and is_category_list_url(url):
        return [force_weapon_grade_filter(u.rstrip("/")) for u in DEFAULT_ACCESSORY_CATEGORY_URLS]
    return [url]


def collect_next_data_urls(page) -> list[str]:
    """Fallback für Next.js-Datenlinks, falls Items nicht als normale <a> auftauchen.

    Questlog ist eine JS-App. Manchmal stehen Slugs/Links nur in eingebetteten JSON-Daten.
    Diese Funktion sammelt alle passenden /db/items/... Pfade aus HTML und Scripts.
    """
    try:
        html_text = page.content()
    except Exception:
        html_text = ""
    urls: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r"/(?:throne-and-liberty)/(?:[a-z]{2})/db/items/[A-Za-z0-9_./%\-]+", html_text):
        href = force_de_locale_url(BASE + m.group(0))
        href = href.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        if href not in seen and is_items_url(href):
            seen.add(href)
            urls.append(href)
    return urls




def get_questlog_item_card_text(page, item_name: str = "") -> str:
    """Extrahiert bevorzugt nur den linken Questlog-Itemkasten.

    Der Body-Text enthält bei Questlog auch Auction-House, Footer, Ads, Tabs und
    andere globale Blöcke. Für Rüstungstraits ist das tödlich. Diese Funktion
    sucht den kleinsten sichtbaren DOM-Block, der Item Level + Eigenschaften bzw.
    die typischen Itemwerte enthält. Daraus werden Traits/Stats wesentlich stabiler
    gelesen als aus document.body.innerText.
    """
    try:
        return str(page.evaluate(
            """
            (itemName) => {
              const wanted = (itemName || '').toLowerCase().trim();
              const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toLowerCase();
              const rawText = (el) => (el.innerText || '').replace(/\r/g, '\n').trim();
              const els = Array.from(document.querySelectorAll('div, section, article, main, aside'));
              const candidates = [];
              for (const el of els) {
                const rect = el.getBoundingClientRect();
                if (!rect || rect.width < 180 || rect.height < 180) continue;
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) continue;
                const text = rawText(el);
                if (!text || text.length < 120 || text.length > 9000) continue;
                const low = norm(text);
                let score = 0;
                if (wanted && low.includes(wanted)) score += 80;
                if (low.includes('item level')) score += 45;
                if (low.includes('gegenstandsstufe')) score += 45;
                if (low.includes('eigenschaften')) score += 70;
                if (low.includes('traits')) score += 60;
                if (low.includes('nahkampfverteidigung') || low.includes('fernkampfverteidigung')) score += 45;
                if (low.includes('max. schaden') || low.includes('reichweite') || low.includes('angriffstempo')) score += 35;
                if (low.includes('passiv') || low.includes('passive')) score += 12;
                if (low.includes('auction house') || low.includes('auktionshaus') || low.includes('preisverlauf') || low.includes('bestandsverlauf')) score -= 120;
                if (low.includes('kommentare') || low.includes('comments')) score -= 35;
                if (low.includes('remove ads') || low.includes('advertisement') || low.includes('sponsored')) score -= 50;
                // der Itemkasten ist typischerweise links/oben und relativ schmal
                if (rect.width < 620) score += 20;
                if (rect.x < 900) score += 8;
                const area = rect.width * rect.height;
                candidates.push({score, area, len: text.length, text});
              }
              candidates.sort((a,b) => (b.score - a.score) || (a.area - b.area) || (a.len - b.len));
              const best = candidates.find(c => c.score >= 80);
              return best ? best.text : '';
            }
            """,
            item_name or "",
        ) or "")
    except Exception:
        return ""

def parse_detail_page(page, url: str, main_category_hint: str, locale: str, source_list_url: str = "") -> dict[str, Any] | None:
    url = force_de_locale_url(url)
    if not is_de_questlog_url(url):
        return skip_item(url, "nicht-deutsche Questlog-URL blockiert")
    if not goto_page(page, url):
        return skip_item(url, "Seite nicht erreichbar/Timeout")
    try:
        body_text = page.locator("body").inner_text(timeout=6000)
    except Exception:
        body_text = page.evaluate("() => document.body ? document.body.innerText : ''")
    json_candidates = collect_json_candidates(page)
    name = page_title(page) or best_name_from_json(json_candidates)
    card_text = get_questlog_item_card_text(page, name)
    raw_text = normalize_raw_text(card_text or body_text)
    if not name or name.lower() in {"items", "weapons", "questlog"}:
        return skip_item(url, "kein echter Itemname erkannt")
    main_category = main_category_hint or classify_main_category(source_list_url or url, raw_text)
    # Unterkategorie kommt bei Questlog am sichersten aus der Listen-URL, z. B. /weapons/bow.
    sub_category, confidence = detect_sub_category(main_category, raw_text, source_list_url or url)
    if confidence == "low" and main_category in {"weapon", "armor", "accessory"}:
        # Kategorie aus Listen-URL ist sicher, Untertyp aber nicht.
        confidence = "medium"
    rarity = detect_rarity(raw_text)
    # Wenn der Link aus einer gefilterten Questlog-Liste kommt (?grade=41), ist Rare+
    # bereits über die Liste abgesichert. Falls Questlog die Seltenheit erst spät/kaputt
    # rendert, importieren wir trotzdem statt echte Waffen zu verlieren.
    if not rarity and has_grade_filter(source_list_url):
        rarity = "Rare"
    if not rarity_allowed(rarity):
        return skip_item(url, f"Seltenheit unter Filter oder nicht erkannt: {rarity or '-'}")
    # Fähigkeitskerne werden hier nicht mehr pauschal gefiltert. Bei den Waffen-
    # Detailseiten von Questlog kommen sie laut aktueller Struktur nicht mehr vor.

    damage_min, damage_max = extract_damage(raw_text)
    item_level = extract_level(raw_text, "Item Level", "Gegenstandsstufe", "Level")
    required_level = extract_level(raw_text, "Required Level", "Benötigte Stufe")
    image_url, icon_url = collect_image_urls(page)
    dom_stats = extract_stat_pairs_from_dom(page)
    text_stats = extract_stats_from_lines(raw_text)
    stats = dict(text_stats)
    for k, v in dom_stats.items():
        stats[k] = v

    detail_model = extract_questlog_detail_model(
        raw_text,
        name=name,
        rarity=rarity,
        sub_category=sub_category,
        image_url=image_url or icon_url,
    )

    # Waffen: Zusatzwerte generisch aus der Itemkarte nachlesen, damit seltene Werte
    # wie Schildgesundheit nicht fehlen.
    if main_category == "weapon":
        detail_model = _sanitize_weapon_detail_model(detail_model, raw_text)

    # Zubehör/Schmuck hat ein anderes Questlog-Layout als Armor:
    # - Zusatzwerte stehen nur vor "Eigenschaften"
    # - Eigenschaften dürfen nicht als Bonusstats oder Passive/Effekt landen
    if main_category == "accessory":
        detail_model = _sanitize_accessory_detail_model(detail_model, raw_text)

    # Armor-Eigenschaften final aus echten DOM-Textnodes des linken Questlog-Itemkastens.
    # Das überschreibt die alten Body/Text-Fallbacks, wenn die erwartete Menge gefunden wird.
    armor_dom_traits = extract_armor_traits_from_dom_textnodes(
        page,
        sub_category=sub_category,
        item_level=detail_model.get("item_level") or item_level,
        item_name=name,
    )
    expected_traits = detail_model.get("trait_count_rule") or _armor_expected_trait_count_from_level(detail_model.get("item_level") or item_level)
    if armor_dom_traits and len(armor_dom_traits) >= int(expected_traits or 0):
        detail_model["traits"] = armor_dom_traits[:int(expected_traits)]
        detail_model["trait_count_observed"] = len(detail_model["traits"])
        detail_model["trait_count_source"] = "dom_textnodes_left_item_card"
    # Detailmodell zusätzlich in flache Felder spiegeln, damit Dashboard/API ohne
    # Sonderlogik brauchbare Werte bekommt.
    if detail_model.get("max_damage") and (damage_min is None or damage_max is None):
        try:
            damage_min = float(str(detail_model["max_damage"].get("min", "")).replace(",", "."))
            damage_max = float(str(detail_model["max_damage"].get("max", "")).replace(",", "."))
        except Exception:
            pass
    detail_defense = detail_model.get("defense") if isinstance(detail_model, dict) else None
    if isinstance(detail_defense, dict) and detail_defense.get("value"):
        # Für Rüstungsitems den Verteidigungswert zusätzlich in die feste Spalte spiegeln.
        try:
            defense_value = float(str(detail_defense.get("value", "")).replace(",", "."))
        except Exception:
            defense_value = None
    else:
        defense_value = None

    for row in detail_model.get("primary") or []:
        if isinstance(row, dict) and row.get("label") and row.get("value"):
            stats.setdefault(str(row.get("label")), str(row.get("value")))
    for row in detail_model.get("bonus_stats") or []:
        if isinstance(row, dict) and row.get("label") and row.get("value"):
            val = str(row.get("value"))
            if row.get("delta"):
                val += f" ▲ {row.get('delta')}"
            stats.setdefault(str(row.get("label")), val)

    source_item_id = item_detail_id(url)
    return {
        "source": "questlog",
        "source_url": url,
        "source_item_id": source_item_id,
        "locale": locale,
        "name": name,
        "slug": slugify(name),
        "main_category": main_category,
        "sub_category": sub_category,
        "rarity": rarity,
        "item_level": item_level,
        "required_level": required_level,
        "damage_min": damage_min,
        "damage_max": damage_max,
        "defense": defense_value if defense_value is not None else extract_defense(raw_text),
        "stats": stats,
        "abilities": ([] if main_category == "accessory" else ([{"label": (detail_model.get("passive") or {}).get("name") or "Passiv", "text": (detail_model.get("passive") or {}).get("text") or ""}] if detail_model.get("passive") else extract_abilities(raw_text))),
        "traits": detail_model.get("traits") or extract_traits_from_text(raw_text),
        "image_url": image_url,
        "icon_url": icon_url,
        "classification_confidence": confidence,
        "raw_text": raw_text[:30000],
        "raw_data": {
            "scraped_at": now_iso(),
            "url": url,
            "detail": detail_model,
            "main_category_hint": main_category_hint,
            "json_candidate_count": len(json_candidates),
            "source_list_url": source_list_url,
            "parser": "playwright-detail-page-v16-armor-dom-textnode-traits",
        },
    }


def record_identity_key(item: dict[str, Any]) -> str:
    name = slugify(str(item.get("name") or ""))
    sub = str(item.get("sub_category") or "").strip().lower()
    cat = str(item.get("main_category") or "").strip().lower()
    return f"{cat}|{sub}|{name}"


def record_quality(item: dict[str, Any]) -> int:
    score = 0
    if item.get("rarity"):
        score += 10
    if item.get("image_url") or item.get("icon_url"):
        score += 8
    if item.get("damage_min") is not None or item.get("damage_max") is not None:
        score += 8
    if item.get("stats"):
        score += 6
    if item.get("abilities"):
        score += 6
    if item.get("source_url") and "#" not in str(item.get("source_url")):
        score += 5
    score += min(len(str(item.get("raw_text") or "")) // 500, 5)
    return score


def dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for rec in records:
        key = record_identity_key(rec)
        if not key or key.endswith('|'):
            continue
        if key not in best or record_quality(rec) > record_quality(best[key]):
            best[key] = rec
    return list(best.values())


def reset_imported_category(conn, categories: set[str]) -> int:
    if not categories:
        return 0
    placeholders = ",".join(["%s"] * len(categories))
    sql = f"DELETE FROM item_catalog WHERE source = 'questlog' AND main_category IN ({placeholders})"
    cur = conn.execute(sql, tuple(sorted(categories)))
    try:
        deleted = int(cur.rowcount or 0)
    except Exception:
        deleted = 0
    conn.commit()
    return deleted



def armor_source_url_for_subcategory(sub_category: Any) -> str:
    mapping = {
        "helm": "head",
        "brust": "chest",
        "umhang": "cloak",
        "handschuhe": "hands",
        "schuhe": "feet",
        "hose": "legs",
    }
    key = clean_text(sub_category).lower()
    slug = mapping.get(key, "")
    if not slug:
        return "https://questlog.gg/throne-and-liberty/de/db/items/armor/head?grade=41"
    return f"https://questlog.gg/throne-and-liberty/de/db/items/armor/{slug}?grade=41"


def armor_expected_bonus_count_from_level(level_value: Any) -> int:
    """Questlog-Rüstungs-Zusatzwerte direkt unter den DEF-Werten.

    Nicht mit Eigenschaften/Traits verwechseln:
    Level 21 = 2 Zusatzwerte
    Level 31 = 4 Zusatzwerte
    Level 45 = 4 Zusatzwerte
    Level 50 = 4 Zusatzwerte
    Level 80 = 5 Zusatzwerte
    """
    try:
        lvl = int(str(level_value or "0"))
    except Exception:
        lvl = 0
    if lvl >= 80:
        return 5
    if lvl in {31, 45, 50}:
        return 4
    if lvl in {21} or (0 < lvl <= 21):
        return 2
    return 4 if lvl else 0


def armor_expected_trait_count_from_level_public(level_value: Any) -> int:
    """Questlog-Rüstungs-Eigenschaften/Traits, getrennt von Zusatzwerten."""
    try:
        lvl = int(str(level_value or "0"))
    except Exception:
        lvl = 0
    if lvl >= 45:
        return 8
    if lvl in {21, 31} or (0 < lvl < 45):
        return 6
    return 0


def _extract_bonus_stats_from_parsed(parsed: dict[str, Any], row: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    detail = ((parsed.get("raw_data") or {}).get("detail") or {}) if isinstance(parsed.get("raw_data"), dict) else {}
    level = parsed.get("item_level") or detail.get("item_level") or row.get("item_level")
    expected = armor_expected_bonus_count_from_level(level)
    bonus = detail.get("bonus_stats") or []
    if not isinstance(bonus, list):
        bonus = []
    clean_bonus: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in bonus:
        if not isinstance(item, dict):
            continue
        label = clean_text(item.get("label") or item.get("name") or "").rstrip(":")
        value = clean_text(item.get("value") or "")
        delta = clean_text(item.get("delta") or "")
        if not label or not value:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        out: dict[str, Any] = {"label": label, "value": value}
        if delta:
            out["delta"] = delta
        clean_bonus.append(out)
        if expected and len(clean_bonus) >= expected:
            break
    return clean_bonus, expected


def update_armor_stats_only(conn, row: dict[str, Any], parsed: dict[str, Any]) -> bool:
    """Aktualisiert bewusst nur DEF/Item-Level/Zusatzwerte von bestehenden Armor-Items.

    Kein Delete, kein Reinsert, keine Bild-/Name-/Kategorie-Änderung.
    Wenn nicht genug Zusatzwerte gefunden werden, bleibt der alte Datensatz unangetastet.
    """
    source_url = str(row.get("source_url") or "").strip()
    if not source_url:
        return False

    raw_data = row.get("raw_data") or {}
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data or "{}")
        except Exception:
            raw_data = {}
    if not isinstance(raw_data, dict):
        raw_data = {}

    parsed_raw = parsed.get("raw_data") if isinstance(parsed.get("raw_data"), dict) else {}
    detail = (parsed_raw.get("detail") or {}) if isinstance(parsed_raw, dict) else {}
    bonus_stats, expected = _extract_bonus_stats_from_parsed(parsed, row)

    if expected > 0 and len(bonus_stats) < expected:
        print(f"⚠️ Zusatzwerte nicht überschrieben: {row.get('name') or source_url} ({len(bonus_stats)}/{expected})", flush=True)
        return False

    old_detail = raw_data.get("detail") if isinstance(raw_data.get("detail"), dict) else {}
    merged_detail = dict(old_detail)
    for key in ("primary", "defenses", "defense", "item_level", "item_level_range"):
        if key in detail:
            merged_detail[key] = detail.get(key)
    merged_detail["bonus_stats"] = bonus_stats[:expected] if expected else bonus_stats
    merged_detail["bonus_count_rule"] = expected
    merged_detail["bonus_count_observed"] = len(merged_detail.get("bonus_stats") or [])
    merged_detail["bonus_count_source"] = "armor_stats_only_parser"

    raw_data["detail"] = merged_detail
    raw_data["armor_stats_only_updated_at"] = now_iso()

    stats_payload: dict[str, Any] = {}
    for block in (merged_detail.get("primary") or []):
        if isinstance(block, dict) and block.get("label") and block.get("value"):
            val = str(block.get("value"))
            if block.get("delta"):
                val += f" ▲ {block.get('delta')}"
            stats_payload[str(block.get("label"))] = val
    for block in (merged_detail.get("bonus_stats") or []):
        if isinstance(block, dict) and block.get("label") and block.get("value"):
            val = str(block.get("value"))
            if block.get("delta"):
                val += f" ▲ {block.get('delta')}"
            stats_payload[str(block.get("label"))] = val

    conn.execute(
        """
        UPDATE item_catalog
        SET stats = %(stats)s::jsonb,
            raw_data = %(raw_data)s::jsonb,
            item_level = COALESCE(%(item_level)s, item_level),
            defense = COALESCE(%(defense)s, defense),
            updated_at = now()
        WHERE source_url = %(source_url)s
        """,
        {
            "stats": json.dumps(stats_payload, ensure_ascii=False),
            "raw_data": json.dumps(raw_data, ensure_ascii=False),
            "item_level": parsed.get("item_level") or detail.get("item_level"),
            "defense": parsed.get("defense"),
            "source_url": source_url,
        },
    )
    return True




def _armor_row_raw_data(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_data") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "{}")
        except Exception:
            raw = {}
    return raw if isinstance(raw, dict) else {}


def _armor_row_detail(row: dict[str, Any]) -> dict[str, Any]:
    raw = _armor_row_raw_data(row)
    detail = raw.get("detail") if isinstance(raw.get("detail"), dict) else {}
    return detail if isinstance(detail, dict) else {}


def _armor_row_item_level(row: dict[str, Any]) -> int:
    detail = _armor_row_detail(row)
    for value in (row.get("item_level"), detail.get("item_level"), detail.get("level")):
        try:
            if value not in (None, ""):
                return int(str(value).strip())
        except Exception:
            pass
    return 0


def _armor_row_needs_stats_update(row: dict[str, Any]) -> bool:
    level = _armor_row_item_level(row)
    expected = armor_expected_bonus_count_from_level(level)
    if expected <= 0:
        return False
    detail = _armor_row_detail(row)
    bonus = detail.get("bonus_stats") or []
    if not isinstance(bonus, list):
        bonus = []
    try:
        observed = int(detail.get("bonus_count_observed") or len(bonus))
    except Exception:
        observed = len(bonus)
    try:
        rule = int(detail.get("bonus_count_rule") or expected)
    except Exception:
        rule = expected
    return observed < rule or len(bonus) < expected


def _armor_row_needs_traits_update(row: dict[str, Any]) -> bool:
    level = _armor_row_item_level(row)
    expected = armor_expected_trait_count_from_level_public(level)
    if expected <= 0:
        return False
    detail = _armor_row_detail(row)
    traits = detail.get("traits") or row.get("traits") or []
    if isinstance(traits, str):
        try:
            traits = json.loads(traits or "[]")
        except Exception:
            traits = []
    if not isinstance(traits, list):
        traits = []
    try:
        observed = int(detail.get("trait_count_observed") or len(traits))
    except Exception:
        observed = len(traits)
    try:
        rule = int(detail.get("trait_count_rule") or expected)
    except Exception:
        rule = expected
    return observed < rule or len(traits) < expected

def run_armor_stats_only(args: argparse.Namespace) -> int:
    """Korrigiert nur Rüstungs-Hauptwerte und Zusatzwerte vorhandener Items."""
    if not os.getenv("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL fehlt. Setze die Postgres-Variable im Importer-Service.")
    from playwright.sync_api import sync_playwright

    conn = connect()
    ensure_item_catalog_schema(conn)
    cur = conn.execute(
        """
        SELECT source_url, name, sub_category, item_level, raw_data
        FROM item_catalog
        WHERE source = 'questlog'
          AND main_category = 'armor'
          AND is_active = TRUE
        ORDER BY sub_category ASC, name ASC
        """
    )
    rows = [dict(r) for r in (cur.fetchall() or [])]

    total_rows = len(rows)
    if getattr(args, "failed_only", False):
        rows = [r for r in rows if _armor_row_needs_stats_update(r)]
        print(f"🔧 Armor-Stats-only Update: nur fehlerhafte Items {len(rows)}/{total_rows}", flush=True)
    else:
        print(f"🔧 Armor-Stats-only Update: {len(rows)} vorhandene Armor-Items", flush=True)
    if not rows:
        conn.close()
        print("✅ Keine fehlerhaften Armor-Zusatzwerte gefunden.", flush=True)
        return 0

    updated = 0
    skipped = 0
    with sync_playwright() as pw:
        launch_kwargs: dict[str, Any] = {
            "headless": HEADLESS,
            "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-extensions", "--disable-background-networking"],
        }
        exe = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
        if exe:
            launch_kwargs["executable_path"] = exe
        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1200},
            user_agent=BROWSER_UA,
            locale="de-DE",
            timezone_id="Europe/Berlin",
            extra_http_headers={"Accept-Language": "de-DE,de;q=0.9,en;q=0.7"},
        )
        try:
            context.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in {"font", "media"}
                else route.continue_(),
            )
        except Exception:
            pass
        page = context.new_page()
        try:
            for row in rows:
                url = force_de_locale_url(str(row.get("source_url") or ""))
                if not url:
                    skipped += 1
                    continue
                source_list_url = armor_source_url_for_subcategory(row.get("sub_category"))
                parsed = parse_detail_page(page, url, "armor", "de", source_list_url=source_list_url)
                if not parsed:
                    skipped += 1
                    print(f"⚠️ Stats skip: {row.get('name') or url} ({SKIP_REASON_BY_URL.get(url, 'parse failed')})", flush=True)
                    continue
                bonus_stats, expected = _extract_bonus_stats_from_parsed(parsed, row)
                if update_armor_stats_only(conn, row, parsed):
                    conn.commit()
                    print(f"✅ Zusatzwerte aktualisiert: {row.get('name')} ({len(bonus_stats)}/{expected})", flush=True)
                    updated += 1
                else:
                    conn.rollback()
                    skipped += 1
                time.sleep(REQUEST_DELAY)
        finally:
            try:
                page.close()
            except Exception:
                pass
            browser.close()
            conn.close()

    print(f"✅ Armor-Stats-only fertig. Aktualisiert: {updated}, übersprungen: {skipped}", flush=True)
    return updated


def update_armor_traits_only(conn, row: dict[str, Any], parsed: dict[str, Any]) -> bool:
    """Aktualisiert bewusst nur Eigenschaften/Traits von bestehenden Armor-Items.

    Kein Delete, kein Reinsert, keine Bild-/Name-/Kategorie-Änderung. Wenn der Parser
    nicht genug Eigenschaften findet, bleibt der alte Datensatz unangetastet.
    """
    source_url = str(row.get("source_url") or "").strip()
    if not source_url:
        return False

    raw_data = row.get("raw_data") or {}
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data or "{}")
        except Exception:
            raw_data = {}
    if not isinstance(raw_data, dict):
        raw_data = {}

    detail = ((parsed.get("raw_data") or {}).get("detail") or {}) if isinstance(parsed.get("raw_data"), dict) else {}
    traits = parsed.get("traits") or detail.get("traits") or []
    if not isinstance(traits, list):
        traits = []
    expected = int(detail.get("trait_count_rule") or _armor_expected_trait_count_from_level(parsed.get("item_level") or row.get("item_level")))

    if expected > 0 and len(traits) < expected:
        print(f"⚠️ Traits nicht überschrieben: {row.get('name') or source_url} ({len(traits)}/{expected})", flush=True)
        return False

    old_detail = raw_data.get("detail") if isinstance(raw_data.get("detail"), dict) else {}
    merged_detail = dict(old_detail)
    for key in ("traits", "trait_count_rule", "trait_count_observed", "trait_count_source"):
        if key in detail:
            merged_detail[key] = detail.get(key)
    merged_detail["trait_count_observed"] = len(traits)
    merged_detail.setdefault("trait_count_rule", expected)
    merged_detail["trait_count_source"] = detail.get("trait_count_source") or "traits_only_parser"

    raw_data["detail"] = merged_detail
    raw_data["traits_only_updated_at"] = now_iso()

    conn.execute(
        """
        UPDATE item_catalog
        SET traits = %(traits)s::jsonb,
            raw_data = %(raw_data)s::jsonb,
            updated_at = now()
        WHERE source_url = %(source_url)s
        """,
        {
            "traits": json.dumps(traits, ensure_ascii=False),
            "raw_data": json.dumps(raw_data, ensure_ascii=False),
            "source_url": source_url,
        },
    )
    return True


def run_traits_only(args: argparse.Namespace) -> int:
    """Korrigiert nur Rüstungseigenschaften vorhandener Items.

    Wichtig für Jonas: kein Reset, kein Löschen, kein Überschreiben der Bilder.
    """
    if not os.getenv("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL fehlt. Setze die Postgres-Variable im Importer-Service.")
    from playwright.sync_api import sync_playwright

    conn = connect()
    ensure_item_catalog_schema(conn)
    rows = []
    try:
        cur = conn.execute(
            """
            SELECT source_url, name, sub_category, item_level, raw_data, traits
            FROM item_catalog
            WHERE source = 'questlog'
              AND main_category = 'armor'
              AND is_active = TRUE
            ORDER BY sub_category ASC, name ASC
            """
        )
        rows = [dict(r) for r in (cur.fetchall() or [])]
    except Exception:
        conn.close()
        raise

    total_rows = len(rows)
    if getattr(args, "failed_only", False):
        rows = [r for r in rows if _armor_row_needs_traits_update(r)]
        print(f"🔧 Traits-only Armor Update: nur fehlerhafte Items {len(rows)}/{total_rows}", flush=True)
    else:
        print(f"🔧 Traits-only Armor Update: {len(rows)} vorhandene Armor-Items", flush=True)
    if not rows:
        conn.close()
        print("✅ Keine fehlerhaften Armor-Traits gefunden.", flush=True)
        return 0

    updated = 0
    skipped = 0
    with sync_playwright() as pw:
        launch_kwargs: dict[str, Any] = {
            "headless": HEADLESS,
            "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-extensions", "--disable-background-networking"],
        }
        exe = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
        if exe:
            launch_kwargs["executable_path"] = exe
        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1200},
            user_agent=BROWSER_UA,
            locale="de-DE",
            timezone_id="Europe/Berlin",
            extra_http_headers={"Accept-Language": "de-DE,de;q=0.9,en;q=0.7"},
        )
        try:
            context.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in {"font", "media"}
                else route.continue_(),
            )
        except Exception:
            pass
        page = context.new_page()
        try:
            for row in rows:
                url = force_de_locale_url(str(row.get("source_url") or ""))
                if not url:
                    skipped += 1
                    continue
                source_list_url = armor_source_url_for_subcategory(row.get("sub_category"))
                parsed = parse_detail_page(page, url, "armor", "de", source_list_url=source_list_url)
                if not parsed:
                    skipped += 1
                    print(f"⚠️ Traits skip: {row.get('name') or url} ({SKIP_REASON_BY_URL.get(url, 'parse failed')})", flush=True)
                    continue
                if update_armor_traits_only(conn, row, parsed):
                    conn.commit()
                    detail = (parsed.get("raw_data") or {}).get("detail") or {}
                    obs = int(detail.get("trait_count_observed") or len(parsed.get("traits") or []))
                    rule = int(detail.get("trait_count_rule") or _armor_expected_trait_count_from_level(parsed.get("item_level") or row.get("item_level")))
                    print(f"✅ Traits aktualisiert: {row.get('name')} ({obs}/{rule})", flush=True)
                    updated += 1
                else:
                    conn.rollback()
                    skipped += 1
                time.sleep(REQUEST_DELAY)
        finally:
            try:
                page.close()
            except Exception:
                pass
            browser.close()
            conn.close()

    print(f"✅ Traits-only fertig. Aktualisiert: {updated}, übersprungen: {skipped}", flush=True)
    return updated

def crawl_category(context, conn, seed: CategorySeed, limit_left: int | None = None, dry_run: bool = False) -> int:
    """Crawlt eine Questlog-Hauptkategorie.

    Questlog-Struktur bei Items:
      /db/items/weapons              = Hauptliste
      /db/items/weapons/sword        = Unterliste / Waffentyp
      /db/item/<item_id>            = echte Item-Detailseite

    Der alte Stand hat /weapons/sword als Detailseite genommen und deshalb alles
    übersprungen. Diese Version geht erst in die Unterlisten und sammelt dort die
    echten Detailseiten.
    """
    list_page = context.new_page()
    detail_page = context.new_page()
    network_payloads: list[Any] = []
    attach_json_capture(list_page, network_payloads)
    imported = 0

    list_queue: list[str] = [seed.url.rstrip("/")]
    visited_lists: set[str] = set()
    seen_detail: set[str] = set()

    while list_queue and len(visited_lists) < MAX_PAGES:
        current_url = list_queue.pop(0).rstrip("/")
        if current_url in visited_lists:
            continue
        visited_lists.add(current_url)

        print(f"📄 {seed.main_category}: {current_url}", flush=True)
        network_payloads.clear()
        if not goto_page(list_page, current_url):
            continue

        # Questlog rendert Kategorie-Listen lazy/virtuell. Ohne Scrollen sieht der
        # DOM oft nur die ersten ca. 40 Items, obwohl es mehr gibt.
        hydrate_listing_page(list_page)

        # Keine JSON/DOM-Listenobjekte mehr direkt importieren.
        # Die hatten Guides, Bosse, Streamtitel und Übersetzungen als Waffen gespeichert.
        # Ab jetzt werden nur echte Detailseiten /db/item/<id> geöffnet und daraus Werte gelesen.
        page_imported = 0

        links = collect_links(list_page)
        # Fallback: Links aus HTML/Next.js-Daten ergänzen.
        for href in collect_next_data_urls(list_page):
            if href not in links:
                links.append(href)

        # Nur innerhalb der gewünschten Hauptkategorie bleiben. Sonst nimmt die Waffen-Seite
        # auch Armor/Material-Navigation mit.
        links = [u for u in links if same_main_category(u, seed.main_category)]

        # Wenn der Seed bereits eine konkrete Unterkategorie ist, bleiben wir hart
        # auf dieser einen Waffenart. Sonst würden Links aus Questlog-Nav/JSON wieder
        # andere Kategorien mit reinziehen.
        if is_subcategory_list_url(seed.url):
            sub_lists = []
        else:
            sub_lists = sorted({
                force_weapon_grade_filter(u)
                for u in links
                if is_subcategory_list_url(u) and force_weapon_grade_filter(u) not in visited_lists
            })

        all_detail_links = sorted({
            u
            for u in collect_detail_links(list_page)
            if detail_matches_source_list(u, current_url, seed.main_category)
        })
        detail_links = [u for u in all_detail_links if u not in seen_detail]

        for sub in sub_lists:
            if sub not in list_queue:
                list_queue.append(sub)

        # Questlog paginiert viele Itemlisten mit 40 Items pro Seite. Bei festen
        # Unterkategorien wie /armor/head?grade=41 oder /weapons/bow?grade=41
        # duerfen wir nicht nur scrollen, sondern muessen ?page=2, ?page=3 usw.
        # versuchen. Sobald eine generierte Seite keine neuen Detailseiten mehr
        # liefert, wird nicht weiter paginiert.
        if is_subcategory_list_url(current_url) and detail_links:
            generated_next = next_page_url(force_weapon_grade_filter(current_url))
            if generated_next not in visited_lists and generated_next not in list_queue:
                list_queue.append(generated_next)

        next_url = find_next_url(list_page)
        if next_url and same_main_category(next_url, seed.main_category) and is_list_url(next_url):
            next_url = force_weapon_grade_filter(next_url)
            if next_url not in visited_lists and next_url not in list_queue:
                list_queue.append(next_url)

        print(f"   ↳ Detailseiten gefunden: {len(all_detail_links)} · neu: {len(detail_links)} · Listen gefunden: {len(sub_lists)} · importiert: {page_imported}", flush=True)

        if not detail_links:
            continue

        for href in detail_links:
            if limit_left is not None and imported >= limit_left:
                list_page.close()
                detail_page.close()
                return imported
            seen_detail.add(href)
            try:
                item = parse_detail_page(detail_page, href, seed.main_category, DEFAULT_LOCALE, source_list_url=current_url)
                if not item:
                    reason = SKIP_REASON_BY_URL.get(href, "kein verwertbares Detailmodell")
                    print(f"⚠️ Übersprungen: {href} ({reason})", flush=True)
                    continue
                if dry_run:
                    print(f"DRY ✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}", flush=True)
                else:
                    upsert_item(conn, item)
                    conn.commit()
                    print(f"✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}", flush=True)
                imported += 1
                page_imported += 1
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"❌ Fehler bei {href}: {type(exc).__name__}: {exc}", flush=True)
            time.sleep(REQUEST_DELAY)

    list_page.close()
    detail_page.close()
    return imported




def debug_dump_item_card(context, url: str) -> None:
    """Debuggt exakt, was Playwright auf einer Questlog-Detailseite sieht.

    Dieser Modus schreibt nichts in die Datenbank. Er dumpft sehr roh: Body-Text,
    DOM-Kandidaten, Links und Bilder. Damit bauen wir den Armor-Trait-Parser auf
    echte Questlog-Ausgabe statt weiter zu raten.
    """
    url = force_de_locale_url(str(url or "").strip())
    if not url:
        print("❌ DEBUG: leere URL", flush=True)
        return

    page = context.new_page()
    try:
        print("\n================ QUESTLOG DEBUG START ================", flush=True)
        print(f"URL: {url}", flush=True)
        ok = goto_page(page, url, timeout_ms=NAV_TIMEOUT_MS)
        print(f"PAGE_OK: {ok}", flush=True)
        if not ok:
            return

        try:
            page.wait_for_timeout(2500)
            page.evaluate("() => window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
        except Exception:
            pass

        try:
            body_text = page.locator("body").inner_text(timeout=10000)
        except Exception:
            body_text = page.evaluate("() => document.body ? document.body.innerText : ''")

        json_candidates = collect_json_candidates(page)
        name = page_title(page) or best_name_from_json(json_candidates)
        print(f"NAME_GUESSED: {name}", flush=True)
        print(f"JSON_CANDIDATES: {len(json_candidates)}", flush=True)

        body_lines = [clean_text(x) for x in str(body_text or "").splitlines() if clean_text(x)]
        print("\n--- BODY TEXT LINES ---", flush=True)
        print(f"BODY_LINE_COUNT: {len(body_lines)}", flush=True)
        for idx, line in enumerate(body_lines[:420]):
            print(f"{idx:03d}: {line}", flush=True)
        if len(body_lines) > 420:
            print(f"... ({len(body_lines) - 420} weitere Body-Zeilen)", flush=True)

        try:
            candidates = page.evaluate(
                r"""
                (itemName) => {
                  const wanted = String(itemName || '').toLowerCase().trim();
                  const norm = (s) => String(s || '').replace(/\s+/g, ' ').trim().toLowerCase();
                  const rawText = (el) => String(el.innerText || '').replace(/\r/g, '\n').trim();
                  const els = Array.from(document.querySelectorAll('div, section, article, main, aside'));
                  const out = [];
                  for (const el of els) {
                    const rect = el.getBoundingClientRect();
                    if (!rect || rect.width < 120 || rect.height < 60) continue;
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) continue;
                    const text = rawText(el);
                    if (!text || text.length < 25 || text.length > 20000) continue;
                    const low = norm(text);
                    let score = 0;
                    if (wanted && wanted !== 'episch' && wanted !== 'selten' && wanted !== 'legendär' && low.includes(wanted)) score += 90;
                    if (low.includes('item level')) score += 70;
                    if (low.includes('gegenstandsstufe')) score += 70;
                    if (low.includes('eigenschaften')) score += 100;
                    if (low.includes('nahkampfverteidigung') || low.includes('fernkampfverteidigung')) score += 80;
                    if (low.includes('max. schaden') || low.includes('reichweite') || low.includes('angriffstempo')) score += 65;
                    if (low.includes('passiv') || low.includes('passive')) score += 15;
                    if (low.includes('auction house') || low.includes('auktionshaus') || low.includes('preisverlauf') || low.includes('bestandsverlauf')) score -= 220;
                    if (low.includes('kommentare') || low.includes('comments')) score -= 60;
                    if (low.includes('remove ads') || low.includes('advertisement') || low.includes('sponsored')) score -= 80;
                    if (rect.x < 900) score += 20;
                    if (rect.width < 850) score += 10;
                    const area = Math.round(rect.width * rect.height);
                    out.push({
                      score,
                      x: Math.round(rect.x),
                      y: Math.round(rect.y),
                      w: Math.round(rect.width),
                      h: Math.round(rect.height),
                      area,
                      len: text.length,
                      tag: el.tagName,
                      cls: String(el.className || '').slice(0, 180),
                      text
                    });
                  }
                  out.sort((a,b) => (b.score - a.score) || (a.area - b.area) || (a.len - b.len));
                  return out.slice(0, 12);
                }
                """,
                name or "",
            ) or []
        except Exception as exc:
            print(f"DEBUG_CANDIDATES_ERROR: {type(exc).__name__}: {exc}", flush=True)
            candidates = []

        print("\n--- TOP DOM-KANDIDATEN ---", flush=True)
        for i, c in enumerate(candidates, 1):
            print(f"\n### CANDIDATE {i} score={c.get('score')} rect={c.get('x')},{c.get('y')} {c.get('w')}x{c.get('h')} len={c.get('len')} tag={c.get('tag')}", flush=True)
            cls = str(c.get('cls') or '')
            if cls:
                print(f"CLASS: {cls}", flush=True)
            lines = [clean_text(x) for x in str(c.get('text') or '').splitlines() if clean_text(x)]
            for idx, line in enumerate(lines[:220]):
                print(f"{idx:03d}: {line}", flush=True)
            if len(lines) > 220:
                print(f"... ({len(lines) - 220} weitere Zeilen)", flush=True)

        card_text = get_questlog_item_card_text(page, name)
        print("\n--- GET_QUESTLOG_ITEM_CARD_TEXT ---", flush=True)
        card_lines = [clean_text(x) for x in str(card_text or '').splitlines() if clean_text(x)]
        print(f"CARD_LINE_COUNT: {len(card_lines)}", flush=True)
        for idx, line in enumerate(card_lines[:220]):
            print(f"{idx:03d}: {line}", flush=True)
        if len(card_lines) > 220:
            print(f"... ({len(card_lines) - 220} weitere Zeilen)", flush=True)

        try:
            print("\n--- LINKS DE ITEM ---", flush=True)
            links = page.evaluate(
                r"""
                () => Array.from(document.querySelectorAll('a[href]'))
                  .map(a => ({href: a.href, text: String(a.innerText || '').replace(/\s+/g, ' ').trim()}))
                  .filter(x => x.href.includes('/throne-and-liberty/de/db/item'))
                  .slice(0, 80)
                """
            ) or []
            for idx, row in enumerate(links):
                print(f"{idx:03d}: {row.get('text') or '-'} -> {row.get('href')}", flush=True)
        except Exception as exc:
            print(f"LINK_DEBUG_ERROR: {type(exc).__name__}: {exc}", flush=True)

        try:
            print("\n--- ALL IMAGES MATCHING QUESTLOG ITEM_128 ---", flush=True)
            imgs = page.evaluate(
                r"""
                () => {
                  const out = [];
                  for (const img of Array.from(document.querySelectorAll('img'))) {
                    const rect = img.getBoundingClientRect();
                    const srcs = [img.currentSrc, img.src, img.getAttribute('src'), img.getAttribute('data-src')].filter(Boolean);
                    for (const src of srcs) {
                      if (String(src).includes('Item_128') || String(src).includes('/Icon/')) {
                        out.push({src, alt: img.alt || '', x: Math.round(rect.x), y: Math.round(rect.y), w: Math.round(rect.width), h: Math.round(rect.height)});
                      }
                    }
                  }
                  return out.slice(0, 120);
                }
                """
            ) or []
            for idx, img in enumerate(imgs):
                print(f"{idx:03d}: {img.get('w')}x{img.get('h')} at {img.get('x')},{img.get('y')} alt={img.get('alt') or '-'} src={img.get('src')}", flush=True)
        except Exception as exc:
            print(f"IMAGE_LIST_DEBUG_ERROR: {type(exc).__name__}: {exc}", flush=True)

        try:
            image_url, icon_url = collect_image_urls(page)
            print("\n--- IMAGE PICK ---", flush=True)
            print(f"image_url: {image_url}", flush=True)
            print(f"icon_url:  {icon_url}", flush=True)
        except Exception as exc:
            print(f"IMAGE_DEBUG_ERROR: {type(exc).__name__}: {exc}", flush=True)

        try:
            item = parse_detail_page(page, url, classify_main_category(url), DEFAULT_LOCALE, source_list_url=url)
        except Exception as exc:
            item = None
            print(f"PARSE_ERROR: {type(exc).__name__}: {exc}", flush=True)

        print("\n--- PARSER RESULT ---", flush=True)
        if not item:
            print("PARSED_ITEM: None", flush=True)
        else:
            detail = ((item.get('raw_data') or {}).get('detail') or {}) if isinstance(item, dict) else {}
            print(json.dumps({
                "name": item.get("name"),
                "main_category": item.get("main_category"),
                "sub_category": item.get("sub_category"),
                "rarity": item.get("rarity"),
                "item_level": item.get("item_level"),
                "primary": detail.get("primary"),
                "bonus_stats": detail.get("bonus_stats"),
                "trait_count_rule": detail.get("trait_count_rule"),
                "trait_count_observed": detail.get("trait_count_observed"),
                "trait_count_source": detail.get("trait_count_source"),
                "traits": detail.get("traits"),
            }, ensure_ascii=False, indent=2), flush=True)
        print("================ QUESTLOG DEBUG END ================\n", flush=True)
    finally:
        try:
            page.close()
        except Exception:
            pass


def run_debug(args: argparse.Namespace) -> int:
    from playwright.sync_api import sync_playwright

    urls = [x.strip() for x in re.split(r"[,;\n]+", str(getattr(args, "debug_url", "") or "")) if x.strip()]
    if not urls:
        print("❌ Keine --debug-url angegeben.", flush=True)
        return 2

    with sync_playwright() as pw:
        launch_kwargs: dict[str, Any] = {
            "headless": HEADLESS,
            "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-extensions", "--disable-background-networking"],
        }
        exe = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
        if exe:
            launch_kwargs["executable_path"] = exe
        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1200},
            user_agent=BROWSER_UA,
            locale="de-DE",
            timezone_id="Europe/Berlin",
            extra_http_headers={"Accept-Language": "de-DE,de;q=0.9,en;q=0.7"},
        )
        try:
            context.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in {"font", "media"}
                else route.continue_(),
            )
        except Exception:
            pass
        try:
            for url in urls:
                debug_dump_item_card(context, url)
        finally:
            browser.close()
    return 0

def run_import(args: argparse.Namespace) -> int:
    if getattr(args, "debug_url", ""):
        return run_debug(args)

    # Kombi-Modus: vorhandene Armor-Items korrigieren, ohne Reset und ohne Bilder/Namen/Kategorien anzufassen.
    # Reihenfolge: erst DEF/Zusatzwerte, dann Eigenschaften. So sind beide Datenbereiche aktuell.
    if getattr(args, "armor_stats_only", False) and getattr(args, "traits_only", False):
        print("🔧 Kombi-Update: Armor DEF/Zusatzwerte + Eigenschaften. Kein Reset, keine Bild-/Item-Änderung.")
        rc1 = run_armor_stats_only(args)
        rc2 = run_traits_only(args)
        return rc1 or rc2

    if getattr(args, "armor_stats_only", False):
        return run_armor_stats_only(args)
    if getattr(args, "traits_only", False):
        return run_traits_only(args)
    if not os.getenv("DATABASE_URL") and not args.dry_run:
        raise RuntimeError("DATABASE_URL fehlt. Setze die Postgres-Variable im Importer-Service.")
    from playwright.sync_api import sync_playwright

    total = 0
    with sync_playwright() as pw:
        launch_kwargs: dict[str, Any] = {
            "headless": HEADLESS,
            "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-extensions", "--disable-background-networking"],
        }
        exe = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
        if exe:
            launch_kwargs["executable_path"] = exe
        browser = pw.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1200},
            user_agent=BROWSER_UA,
            locale="en-US" if DEFAULT_LOCALE.startswith("en") else "de-DE",
            timezone_id="Europe/Berlin",
            extra_http_headers={
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
            },
        )
        try:
            # Fonts/Video/Tracking kosten auf Railway viel Zeit, für den Import sind sie egal.
            context.route(
                "**/*",
                lambda route, request: route.abort()
                if request.resource_type in {"font", "media"}
                else route.continue_(),
            )
        except Exception:
            pass
        page = context.new_page()
        preset = str(getattr(args, "preset", "") or "").strip().lower()
        if preset in {"weapon", "weapons", "waffen", "waffe"}:
            seeds = [CategorySeed(force_weapon_grade_filter(u.rstrip("/")), "weapon") for u in DEFAULT_WEAPON_CATEGORY_URLS]
        elif preset in {"armor", "armors", "rüstung", "ruestung", "rüstungen", "ruestungen", "gear", "equipment"}:
            # Rüstung nutzt ausschließlich die festen deutschen Rare+-Unterseiten.
            # Keine Sammelseiten crawlen, damit keine fremden Sprachen/Unterkategorien reinlaufen.
            seeds = [CategorySeed(force_weapon_grade_filter(u.rstrip("/")), "armor") for u in DEFAULT_ARMOR_CATEGORY_URLS]
        elif preset in {"accessory", "accessories", "zubehör", "zubehoer", "schmuck", "accessoire", "accessoires"}:
            # Zubehör/Schmuck getrennt von Armor speichern: main_category=accessory.
            seeds = [CategorySeed(force_weapon_grade_filter(u.rstrip("/")), "accessory") for u in DEFAULT_ACCESSORY_CATEGORY_URLS]
        elif args.category_url:
            raw_urls = [x.strip() for x in re.split(r"[,;\n]+", str(args.category_url or "")) if x.strip()]
            expanded_urls: list[str] = []
            for raw in raw_urls:
                expanded_urls.extend(expand_seed_url(raw))
            # Reihenfolge erhalten, Duplikate entfernen.
            seen_seed_urls: set[str] = set()
            clean_urls: list[str] = []
            for u in expanded_urls:
                if u not in seen_seed_urls:
                    seen_seed_urls.add(u)
                    clean_urls.append(u)
            seeds = [CategorySeed(force_weapon_grade_filter(u.rstrip("/")), classify_main_category(u)) for u in clean_urls]
        else:
            seeds = discover_category_seeds(page, DEFAULT_LOCALE, include_fallback=True)
        page.close()
        if args.only:
            wanted = {normalize_main_category_arg(x.strip()) for x in args.only.split(",") if x.strip()}
            seeds = [s for s in seeds if s.main_category in wanted]
        print("🔎 Kategorien:")
        for s in seeds:
            print(f"  - {s.main_category}: {s.url}")
        if args.discover_only:
            browser.close()
            return 0
        conn = None
        try:
            if args.dry_run:
                class DummyConn:
                    def commit(self): pass
                    def rollback(self): pass
                conn = DummyConn()
            else:
                conn = connect()
                ensure_item_catalog_schema(conn)
                reset_requested = bool(args.reset_category) or os.getenv("QUESTLOG_RESET_CATEGORY", "0").lower() in {"1", "true", "yes", "ja"}
                if reset_requested:
                    cats = {s.main_category for s in seeds}
                    deleted = reset_imported_category(conn, cats)
                    print(f"🧹 Alte Questlog-Items gelöscht: {deleted} ({', '.join(sorted(cats))})", flush=True)
            for seed in seeds:
                remaining = None
                if MAX_ITEMS > 0:
                    remaining = max(0, MAX_ITEMS - total)
                    if remaining <= 0:
                        break
                total += crawl_category(context, conn, seed, limit_left=remaining, dry_run=args.dry_run)
        finally:
            try:
                if conn and not args.dry_run:
                    conn.close()
            finally:
                browser.close()
    print(f"✅ Import fertig. Items importiert/aktualisiert: {total}")
    return total



# ---------------------------------------------------------------------------
# Finaler Armor-Detail-Updater-Fix
# ---------------------------------------------------------------------------
# Der vorherige Armor-Parser war zu abhängig von festen Trait-Namen und hat bei
# Rüstung/Handschuhen oft nur 5-7 Eigenschaften gefunden. Questlog rendert den
# Eigenschaften-Block aber sehr regelmäßig: Label: + vier Werte. Deshalb lesen
# diese Overrides generisch aus dem Abschnitt ab "Eigenschaften" und nicht mehr
# über eine unvollständige Whitelist.

_old_extract_bonus_stats_from_parsed = _extract_bonus_stats_from_parsed


def _ql_num_tokens_keep(text: Any) -> list[str]:
    raw = str(text or "")
    return [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", raw)]


def _armor_noise_line(text: Any) -> bool:
    low = clean_text(text).lower().strip(" :")
    if not low:
        return True
    noise_parts = (
        "remove ads", "werbung", "anzeige", "questlog", "datenbank", "upgrade",
        "anmelden", "zurück", "zurueck", "teilen", "share", "hot", "new",
        "charakter-builds", "skill-builds", "auktionshaus", "kampfprotokolle",
        "preisverlauf", "bestandsverlauf", "durchschnittspreis", "auf lager",
        "kommentare", "comments", "karte", "map", "enchanting", "stats",
        "verkaufspreis", "sales price", "von npcs erbeutet", "dropped from",
        "in lithographen", "lithograph", "litograph", "ausrüstungseffekt",
        "ausruestungseffekt", "ausrüstungseffekte", "ausruestungseffekte",
        "ausrüstungsset", "ausruestungsset", "dieser gegenstand hat",
        "this item has", "regen in", "nacht in", "discord", "strg",
    )
    return any(x in low for x in noise_parts)


def _armor_generic_label(text: Any) -> str:
    t = clean_text(text).strip()
    if not t:
        return ""
    # Inline-Label abtrennen: "Max. Mana: 150 | 300 ..."
    if ":" in t:
        left = clean_text(t.split(":", 1)[0]).rstrip(":")
    else:
        left = t.rstrip(":")
    low = left.lower().strip(" :")
    if not left or _armor_noise_line(left):
        return ""
    if re.search(r"\d", left):
        return ""
    if left in {"|", "~", "▲", "Episch", "Selten", "Rare", "Epic", "Level", "Item Level", "Eigenschaften", "Traits"}:
        return ""
    if len(left) > 80:
        return ""
    return left


def _armor_is_likely_next_label(token: Any) -> bool:
    t = clean_text(token).strip()
    if not t or _armor_noise_line(t):
        return False
    if _ql_num_tokens_keep(t):
        return False
    return bool(_armor_generic_label(t))


def _parse_armor_traits_from_text_sequence(tokens: list[str], expected: int) -> list[dict[str, Any]]:  # type: ignore[override]
    """Generischer Questlog-Traitparser für Armor.

    Quelle ist eine echte Text-Reihenfolge aus Itemkasten oder Body. Er startet
    exakt nach "Eigenschaften" und nimmt dann jedes Label mit vier Wertstufen.
    Damit funktionieren auch Trait-Namen, die nicht in einer alten Whitelist
    standen, z. B. seltene Resistenz-/Chance-Zeilen.
    """
    if expected <= 0:
        return []
    clean_tokens = [clean_text(t).strip() for t in tokens if clean_text(t).strip()]
    start = -1
    for i, t in enumerate(clean_tokens):
        if clean_text(t).lower().strip(" :") in {"eigenschaften", "traits"}:
            start = i + 1
            break
    if start < 0:
        return []

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    max_i = min(len(clean_tokens), start + 260)
    while i < max_i and len(out) < expected:
        tok = clean_tokens[i]
        if _armor_noise_line(tok):
            i += 1
            continue

        label = ""
        inline_values = ""
        if ":" in tok:
            left, right = tok.split(":", 1)
            label = _armor_generic_label(left)
            inline_values = right
        else:
            label = _armor_generic_label(tok)

        if not label:
            i += 1
            continue
        key = label.lower().strip(" :")
        if key in seen:
            i += 1
            continue

        vals: list[str] = []
        if inline_values:
            vals.extend(_ql_num_tokens_keep(inline_values))

        j = i + 1
        while j < max_i and len(vals) < 4:
            nxt = clean_tokens[j]
            if _armor_noise_line(nxt):
                j += 1
                continue
            # Nächstes Label beginnt. Wenn wir noch nicht 4 Werte haben, ist das
            # aktuelle Label offenbar kein echter Trait.
            if _armor_is_likely_next_label(nxt):
                break
            if clean_text(nxt).strip() != "|":
                vals.extend(_ql_num_tokens_keep(nxt))
            j += 1

        if len(vals) >= 4:
            out.append({"name": label, "values": vals[:4]})
            seen.add(key)
            i = max(j, i + 1)
            continue
        i += 1

    return out[:expected]


def _parse_armor_traits_by_label_windows(raw_text: str, expected: int) -> list[dict[str, Any]]:  # type: ignore[override]
    raw = normalize_raw_text(raw_text)
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", raw) if clean_text(x)]
    return _parse_armor_traits_from_text_sequence(lines, expected)


def _parse_armor_bonus_stats_generic(raw_text: str, expected: int) -> list[dict[str, Any]]:
    if expected <= 0:
        return []
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(raw_text)) if clean_text(x)]
    if not lines:
        return []

    # Ende ist der Eigenschaften-Block. Bonuswerte liegen davor.
    end = len(lines)
    for idx, line in enumerate(lines):
        if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
            end = idx
            break

    # Start nach den beiden Verteidigungswerten suchen. Wenn das nicht klappt,
    # nach Item Level starten.
    defense_labels = {"nahkampfverteidigung", "fernkampfverteidigung", "magieverteidigung", "verteidigung", "def"}
    start = 0
    defense_hits = 0
    for idx, line in enumerate(lines[:end]):
        low = clean_text(line).lower().strip(" :")
        if low in defense_labels:
            defense_hits += 1
            # Nach dem letzten DEF-Label starten wir direkt hinter dessen Basiswert.
            # Questlog rendert Fixed-Level-Items ohne ▲-Delta:
            #   Fernkampfverteidigung / 246 / Weisheit / 7 ...
            # Der alte idx+3 sprang dabei über den ersten echten Zusatzwert.
            # Bei Items mit ▲-Delta landet idx+2 auf der Delta-Zeile; die wird
            # unten als reine Zahl/kein Label sauber übersprungen.
            start = max(start, idx + 2)
    if defense_hits < 1:
        for idx, line in enumerate(lines[:end]):
            if "item level" in clean_text(line).lower() or clean_text(line).lower().strip(" :") == "level":
                start = idx + 1
                break

    primary_labels = defense_labels | {"item level", "level", "episch", "selten", "rare", "epic"}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    while i < end and len(out) < expected:
        tok = lines[i]
        if _armor_noise_line(tok):
            i += 1
            continue
        label = ""
        inline = ""
        if ":" in tok:
            left, right = tok.split(":", 1)
            label = _armor_generic_label(left)
            inline = right
        else:
            label = _armor_generic_label(tok)
        if not label or label.lower().strip(" :") in primary_labels:
            i += 1
            continue
        key = label.lower().strip(" :")
        if key in seen:
            i += 1
            continue
        vals: list[str] = []
        if inline:
            vals.extend(_ql_num_tokens_keep(inline))
        j = i + 1
        while j < end and len(vals) < 2:
            nxt = lines[j]
            if _armor_noise_line(nxt):
                j += 1
                continue
            if _armor_is_likely_next_label(nxt):
                break
            if clean_text(nxt).strip() not in {"|", "~"}:
                vals.extend(_ql_num_tokens_keep(nxt))
            j += 1
        if vals:
            entry: dict[str, Any] = {"label": label, "value": vals[0]}
            # Questlog zeigt Verbesserung als ▲-Zeile. In den Tokens ist das meist der zweite Wert.
            if len(vals) >= 2:
                entry["delta"] = vals[1]
            out.append(entry)
            seen.add(key)
            i = max(j, i + 1)
            continue
        i += 1
    return out[:expected]


def _extract_bonus_stats_from_parsed(parsed: dict[str, Any], row: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:  # type: ignore[override]
    detail = ((parsed.get("raw_data") or {}).get("detail") or {}) if isinstance(parsed.get("raw_data"), dict) else {}
    level = parsed.get("item_level") or detail.get("item_level") or row.get("item_level")
    expected = armor_expected_bonus_count_from_level(level)

    raw_text = str(parsed.get("raw_text") or "")
    generic = _parse_armor_bonus_stats_generic(raw_text, expected)
    if expected > 0 and len(generic) >= expected:
        return generic[:expected], expected

    # Fallback auf den bisherigen Detailparser, falls generisch bei Sonderseiten
    # nichts findet. Aber wenn generisch mehr findet, nehmen wir generisch.
    try:
        old_bonus, _ = _old_extract_bonus_stats_from_parsed(parsed, row)
    except Exception:
        old_bonus = []
    if len(generic) >= len(old_bonus or []):
        return generic[:expected] if expected else generic, expected
    return (old_bonus or [])[:expected] if expected else (old_bonus or []), expected



# Patch: scalable Questlog armor pieces like "Item Level 50 (21-50)" often expose
# only 3 Zusatzwerte on the rendered German detail page. They are not missing;
# the fixed rule "Level 50 = 4" only fits fixed/newer armor blocks.
def _armor_is_scaling_21_50_item(raw_text: str) -> bool:
    raw = normalize_raw_text(raw_text or "")
    return bool(re.search(r"Item\s*Level\s*50\s*\(\s*21\s*[-–]\s*50\s*\)", raw, re.I)) or bool(re.search(r"\(\s*21\s*[-–]\s*50\s*\)", raw))


def _armor_expected_bonus_count_from_context(level_value: Any, raw_text: str, found_count: int = 0) -> int:
    base = armor_expected_bonus_count_from_level(level_value)
    if base == 4 and found_count == 3 and _armor_is_scaling_21_50_item(raw_text):
        return 3
    return base


def _extract_bonus_stats_from_parsed(parsed: dict[str, Any], row: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:  # type: ignore[override]
    detail = ((parsed.get("raw_data") or {}).get("detail") or {}) if isinstance(parsed.get("raw_data"), dict) else {}
    level = parsed.get("item_level") or detail.get("item_level") or row.get("item_level")
    raw_text = str(parsed.get("raw_text") or "")

    initial_expected = armor_expected_bonus_count_from_level(level)
    generic = _parse_armor_bonus_stats_generic(raw_text, initial_expected)
    expected = _armor_expected_bonus_count_from_context(level, raw_text, len(generic))

    if expected > 0 and len(generic) >= expected:
        return generic[:expected], expected

    try:
        old_bonus, _ = _old_extract_bonus_stats_from_parsed(parsed, row)
    except Exception:
        old_bonus = []

    best = generic if len(generic) >= len(old_bonus or []) else (old_bonus or [])
    if expected > 0:
        return best[:expected], expected
    return best, expected


# Patch: Zubehör/Schmuck-Detailparser getrennt von Armor.
# Questlog zeigt bei Accessories Zusatzwerte und Eigenschaften sehr ähnlich,
# aber die Eigenschaften stehen NACH dem Marker "Eigenschaften". Der alte
# generische Parser hat dadurch Trait-Zeilen wie Max. Mana oder
# Fähigkeitsschaden-Bonus teilweise als Zusatzwert/Passiv übernommen.
def _accessory_is_stop_line(value: Any) -> bool:
    low = clean_text(str(value or "")).lower().strip(" :[]{}")
    if not low:
        return False
    if low in {
        "auction house", "auktionshaus", "stats", "enchanting", "kommentare", "comments",
        "remove ads", "teilen", "share", "preisverlauf", "bestandsverlauf",
    }:
        return True
    return any(x in low for x in (
        "verkaufspreis", "sales price", "dieser gegenstand hat", "this item has",
        "resonanzeigenschaften", "ausrüstungseffekte", "ausruestungseffekte",
        "von npcs erbeutet", "dropped from", "in rezepten", "in kisten", "remove ads",
        "sponsored", "questlog.gg",
    ))


def _accessory_label(text: Any) -> str:
    t = clean_text(str(text or "")).strip().rstrip(":")
    if not t or _accessory_is_stop_line(t):
        return ""
    if t in {"|", "~", "▲", "Episch", "Selten", "Rare", "Epic", "Level", "Item Level", "Eigenschaften", "Traits"}:
        return ""
    if re.search(r"\d", t):
        return ""
    if len(t) > 80:
        return ""
    return t


def _accessory_number_tokens(text: Any) -> list[str]:
    s = clean_text(str(text or ""))
    if not s or s == "|":
        return []
    # ▲ bleibt im Delta-Feld nicht erhalten, aber die Zahl wird sauber extrahiert.
    return [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*%?", s)]


def _accessory_is_likely_label(text: Any) -> bool:
    return bool(_accessory_label(text)) and not _accessory_number_tokens(text)


def _accessory_primary_labels() -> set[str]:
    return {
        "magieverteidigung", "nahkampfverteidigung", "fernkampfverteidigung",
        "verteidigung", "def", "magic defense", "melee defense", "ranged defense",
    }


def _parse_accessory_bonus_stats(raw_text: str) -> list[dict[str, Any]]:
    """Parse Zubehör-Zusatzwerte strikt nach Questlog-Aufbau.

    Regel laut Questlog-Karte:
    Primärwert (z. B. Magieverteidigung) -> Zusatzwerte -> Eigenschaften:

    Alles zwischen dem Primärwert-Block und "Eigenschaften:" sind Zusatzwerte.
    Keine Whitelist, damit Werte wie Nahkampftrefferchance, Fernkampftrefferchance,
    Angriffstempo usw. nicht verloren gehen.
    """
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(raw_text)) if clean_text(x)]
    if not lines:
        return []

    primary_labels = _accessory_primary_labels()

    # Ende der Zusatzwerte: ab Eigenschaften beginnen Traits.
    end = len(lines)
    for idx, line in enumerate(lines):
        if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
            end = idx
            break

    # Start: nach dem letzten Primärwert-Block. Bei Zubehör ist das meistens
    # Magieverteidigung + Basiswert + optional ▲ Delta.
    start = 0
    for idx, line in enumerate(lines[:end]):
        low = clean_text(line).lower().strip(" :")
        if low in primary_labels or any(low.startswith(pl + " ") for pl in primary_labels):
            j = idx + 1
            # Zahlen, Pfeile und Trenner gehören noch zum Primärwert.
            while j < end:
                nxt = clean_text(lines[j]).strip()
                nl = nxt.lower().strip(" :")
                if not nxt or nxt == "|" or nxt.startswith("▲") or _accessory_number_tokens(nxt):
                    j += 1
                    continue
                break
            start = max(start, j)

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    while i < end:
        tok = clean_text(lines[i]).strip()
        if not tok or tok == "|" or _accessory_is_stop_line(tok):
            i += 1
            continue

        label = ""
        inline = ""
        if ":" in tok:
            left, right = tok.split(":", 1)
            label = _accessory_label(left)
            inline = right
        else:
            label = _accessory_label(tok)

        if not label:
            i += 1
            continue

        low_label = clean_text(label).lower().strip(" :")
        if low_label in primary_labels or low_label in {"item level", "level"}:
            i += 1
            continue
        if low_label in seen:
            i += 1
            continue

        vals: list[str] = []
        if inline:
            vals.extend(_accessory_number_tokens(inline))

        j = i + 1
        # Zusatzwerte haben einen Basiswert und optional ein grünes ▲-Delta.
        # Wir lesen höchstens bis zum nächsten Label oder Eigenschaften.
        while j < end and len(vals) < 2:
            nxt = clean_text(lines[j]).strip()
            nl = nxt.lower().strip(" :")
            if not nxt or nxt == "|":
                j += 1
                continue
            if nl in {"eigenschaften", "traits"} or nl in primary_labels or _accessory_is_stop_line(nxt):
                break
            if _accessory_is_likely_label(nxt) or (":" in nxt and not _accessory_number_tokens(nxt.split(":", 1)[0])):
                break
            vals.extend(_accessory_number_tokens(nxt))
            j += 1

        if vals:
            entry: dict[str, Any] = {"label": label, "value": vals[0]}
            if len(vals) >= 2:
                entry["delta"] = vals[1]
            out.append(entry)
            seen.add(low_label)
            i = max(j, i + 1)
            continue
        i += 1

    return out

def _parse_accessory_traits(raw_text: str) -> list[dict[str, Any]]:
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(raw_text)) if clean_text(x)]
    start = -1
    for idx, line in enumerate(lines):
        if clean_text(line).lower().strip(" :") in {"eigenschaften", "traits"}:
            start = idx + 1
            break
    if start < 0:
        return []

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    while i < len(lines) and len(out) < 12:
        tok = clean_text(lines[i]).strip()
        if not tok or tok == "|":
            i += 1
            continue
        if _accessory_is_stop_line(tok):
            break

        label = ""
        inline = ""
        if ":" in tok:
            left, right = tok.split(":", 1)
            label = _accessory_label(left)
            inline = right
        else:
            label = _accessory_label(tok)
        if not label:
            i += 1
            continue
        key = label.lower().strip(" :")
        if key in seen:
            i += 1
            continue

        vals: list[str] = []
        if inline:
            vals.extend(_accessory_number_tokens(inline))
        j = i + 1
        while j < len(lines) and len(vals) < 4:
            nxt = clean_text(lines[j]).strip()
            if not nxt or nxt == "|":
                j += 1
                continue
            if _accessory_is_stop_line(nxt):
                break
            if _accessory_is_likely_label(nxt):
                break
            vals.extend(_accessory_number_tokens(nxt))
            j += 1

        # Questlog-Traits haben vier Stufen. Weniger nicht übernehmen.
        if len(vals) >= 4:
            out.append({"name": label, "values": vals[:4]})
            seen.add(key)
            i = max(j, i + 1)
            continue
        i += 1

    return out


def _sanitize_accessory_detail_model(detail: dict[str, Any], raw_text: str) -> dict[str, Any]:
    if not isinstance(detail, dict):
        detail = {}
    cleaned = dict(detail)

    bonus = _parse_accessory_bonus_stats(raw_text)
    if bonus:
        cleaned["bonus_stats"] = bonus

    traits = _parse_accessory_traits(raw_text)
    if traits:
        cleaned["traits"] = traits
        cleaned["trait_count_observed"] = len(traits)
        cleaned["trait_count_source"] = "accessory_section_parser"

    # Zubehör hat in Questlog keine Waffen-Passive. Alte Fallbacks haben hier
    # Trait-Zeilen wie Fähigkeitsschaden-Bonus als Passive/Effekt missverstanden.
    cleaned.pop("passive", None)
    cleaned["accessory_parser"] = "section_bonus_traits_v1"
    return cleaned



# ---------------------------------------------------------------------------
# Waffen-Fix: generische Zusatzwerte aus Questlog-Detailkarte lesen
# ---------------------------------------------------------------------------
def _weapon_num_tokens(text: str) -> list[str]:
    s = clean_text(str(text or ""))
    if not s or s == "|":
        return []
    # Entfernt Questlog-Delta-Pfeil, behält Prozent, Sekunden, Meter.
    s = s.replace("▲", " ")
    return [clean_text(x) for x in re.findall(r"[+\-]?\d+(?:[.,]\d+)?\s*(?:%|m|s|Sek\.)?", s, flags=re.I)]


def _weapon_clean_label(text: str) -> str:
    label = clean_text(str(text or "")).strip(" :")
    if not label or len(label) > 80:
        return ""
    low = label.lower()
    bad_exact = {
        "episch", "heroisch", "selten", "ungewöhnlich", "gewöhnlich", "uncommon", "common", "rare", "epic", "heroic",
        "item level", "level", "passiv", "passive", "eigenschaften", "traits", "runes", "runen",
        "skill core", "not equipped", "auction house", "auktionshaus", "stats", "enchanting", "lucent-wert", "kampfkraft",
    }
    if low.strip(" :") in bad_exact:
        return ""
    if re.fullmatch(r"[+\-]?\d+(?:[.,]\d+)?\s*(?:%|m|s|Sek\.)?", label, flags=re.I):
        return ""
    if label == "|" or label.startswith("▲"):
        return ""
    # Keine offensichtlichen Beschreibungs-/Fließtextzeilen als Label.
    if len(label.split()) > 5 and not label.endswith(":"):
        return ""
    return label


def _weapon_stop_line(text: str) -> bool:
    low = clean_text(str(text or "")).lower().strip(" :[]{}")
    if not low:
        return False
    exact = {
        "eigenschaften", "traits", "runes", "runen", "skill core", "auction house", "auktionshaus",
        "stats", "enchanting", "kommentare", "comments", "remove ads", "learn more", "teilen", "share",
        "verkaufspreis", "lucent-wert", "preisverlauf", "bestandsverlauf",
    }
    if low in exact:
        return True
    return low.startswith(("dieser gegenstand hat resonanzeigenschaften", "ausrüstungseffekte", "heroische effekte"))


def _weapon_primary_labels() -> set[str]:
    return {
        "max. schaden", "max damage", "schaden", "damage",
        "reichweite", "range", "angriffstempo", "attack speed", "angriffsgeschwindigkeit",
    }


def _parse_weapon_bonus_stats_generic(raw_text: str) -> list[dict[str, Any]]:
    """Liest Waffen-Zusatzwerte generisch statt per starrer Whitelist.

    Questlog zeigt bei Waffen nach den Hauptwerten weitere Extra-Stats, z. B.
    Stärke, Wahrnehmung, Chance auf Zweitwaffenangriff, Schildgesundheit usw.
    Dieser Parser nimmt jedes Label mit direkt folgendem Zahlenwert vor dem
    Eigenschaften-Abschnitt auf und verliert dadurch neue/seltene Statnamen nicht.
    """
    lines = [clean_text(x) for x in re.split(r"[\n\r]+", normalize_raw_text(raw_text)) if clean_text(x)]
    if not lines:
        return []

    primary = _weapon_primary_labels()
    end = len(lines)
    for idx, line in enumerate(lines):
        low = clean_text(line).lower().strip(" :")
        if low in {"eigenschaften", "traits"}:
            end = idx
            break

    # Erst nach der letzten Hauptwert-Zeile beginnen, damit Max. Schaden/Reichweite/
    # Grund-Angriffstempo nicht doppelt als Zusatzwert erscheinen.
    start = 0
    for idx, line in enumerate(lines[:end]):
        low = clean_text(line).lower().strip(" :")
        if low in primary or any(low.startswith(p + " ") for p in primary):
            j = idx + 1
            while j < end:
                nxt = clean_text(lines[j]).strip()
                nl = nxt.lower().strip(" :")
                if not nxt or nxt == "|" or nxt.startswith("▲") or _weapon_num_tokens(nxt):
                    j += 1
                    continue
                break
            start = max(start, j)

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    i = start
    while i < end:
        tok = clean_text(lines[i]).strip()
        if not tok or tok == "|" or _weapon_stop_line(tok):
            i += 1
            continue
        low = tok.lower().strip(" :")
        if low in primary:
            i += 1
            continue
        if low in {"passiv", "passive"}:
            # Passive-Name/Beschreibung nicht als Stat lesen; Zahlen innerhalb der Beschreibung ignorieren.
            i += 1
            # bis zum nächsten klaren Label mit numerischem Wert oder Eigenschaften weiterlaufen
            continue

        label = ""
        inline = ""
        if ":" in tok:
            left, right = tok.split(":", 1)
            label = _weapon_clean_label(left)
            inline = right
        else:
            label = _weapon_clean_label(tok)
        if not label:
            i += 1
            continue
        key = label.lower().strip(" :")
        if key in seen or key in primary:
            i += 1
            continue

        vals: list[str] = []
        if inline:
            vals.extend(_weapon_num_tokens(inline))
        j = i + 1
        while j < end and len(vals) < 2:
            nxt = clean_text(lines[j]).strip()
            nl = nxt.lower().strip(" :")
            if not nxt or nxt == "|":
                j += 1
                continue
            if _weapon_stop_line(nxt) or nl in primary:
                break
            # Wenn die nächste Zeile selbst ein Label ist und keine Zahl enthält, endet dieser Stat.
            if not _weapon_num_tokens(nxt) and _weapon_clean_label(nxt):
                break
            vals.extend(_weapon_num_tokens(nxt))
            j += 1

        if vals:
            entry: dict[str, Any] = {"label": label, "value": vals[0]}
            if len(vals) >= 2:
                entry["delta"] = vals[1]
            out.append(entry)
            seen.add(key)
            i = max(j, i + 1)
            continue
        i += 1

    return out


def _sanitize_weapon_detail_model(detail: dict[str, Any], raw_text: str) -> dict[str, Any]:
    if not isinstance(detail, dict):
        detail = {}
    cleaned = dict(detail)
    old_bonus = cleaned.get("bonus_stats") if isinstance(cleaned.get("bonus_stats"), list) else []
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in old_bonus:
        if not isinstance(row, dict):
            continue
        label = clean_text(row.get("label") or row.get("name") or "")
        if not label:
            continue
        key = label.lower().strip(" :")
        if key in seen:
            continue
        merged.append(row)
        seen.add(key)
    for row in _parse_weapon_bonus_stats_generic(raw_text):
        label = clean_text(row.get("label") or "")
        key = label.lower().strip(" :")
        if not label or key in seen:
            continue
        merged.append(row)
        seen.add(key)
    if merged:
        cleaned["bonus_stats"] = merged
    cleaned["weapon_parser"] = "generic_bonus_stats_v2"
    return cleaned

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Questlog.gg Item-Importer für Ebo Dashboard/Postgres")
    p.add_argument("--category-url", default="", help="Eine oder mehrere Questlog-Kategorien crawlen, getrennt mit Komma/Semikolon, z. B. /db/items/armor,/db/items/accessories")
    p.add_argument("--preset", default="", help="Vordefinierter Import: weapon, armor oder accessories. accessories = Zubehör/Schmuck ab Rare")
    p.add_argument("--only", default="", help="Nur Hauptkategorien, z. B. weapon,armor,accessory,material,currency,misc")
    p.add_argument("--discover-only", action="store_true", help="Nur Kategorie-Links anzeigen, nichts importieren")
    p.add_argument("--dry-run", action="store_true", help="Nichts in Postgres schreiben")
    p.add_argument("--reset-category", action="store_true", help="Vor dem Import questlog-Items der gewählten Hauptkategorie löschen")
    p.add_argument("--debug-url", default="", help="Eine oder mehrere Questlog-Detailseiten debuggen. Schreibt nichts in Postgres.")
    p.add_argument("--traits-only", action="store_true", help="Nur vorhandene Armor-Eigenschaften neu lesen und aktualisieren. Kein Reset, keine Bild-/Item-Änderung.")
    p.add_argument("--armor-stats-only", action="store_true", help="Nur vorhandene Armor-DEF/Zusatzwerte neu lesen und aktualisieren. Kein Reset, keine Bild-/Item-Änderung. Kann mit --traits-only kombiniert werden.")
    p.add_argument("--failed-only", action="store_true", help="Nur Armor-Items erneut prüfen, deren bisherige Zusatzwerte/Traits laut gespeicherten Counts unvollständig sind.")
    return p


if __name__ == "__main__":
    parser = build_arg_parser()
    run_import(parser.parse_args())
