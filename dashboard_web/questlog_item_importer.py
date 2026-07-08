from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urljoin
import hashlib

from item_catalog_db import connect, ensure_item_catalog_schema, upsert_item

BASE = "https://questlog.gg"
GAME = "throne-and-liberty"
DEFAULT_LOCALE = os.getenv("QUESTLOG_LOCALE", "en").strip() or "en"
DEFAULT_START_URL = f"{BASE}/{GAME}/{DEFAULT_LOCALE}/db/items/weapons"

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
    ("Brosche", ["brooch", "brosche"]),
    ("Ohrringe", ["earring", "earrings", "ohrring", "ohrringe"]),
    ("Kette", ["necklace", "kette", "halskette"]),
    ("Armband", ["bracelet", "armband"]),
    ("Ring", ["ring"]),
    ("Gürtel", ["belt", "gürtel", "guertel"]),
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
    "melee", "ranged", "magic", "evasion", "endurance", "defense",
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
    if main_category in {"weapon", "armor"} and not (is_subcategory_list_url(source_url) or is_detail_url(source_url)):
        return False

    keys = {str(k).lower() for k in d.keys()}
    serialized = compact_json_text(d, limit=8000).lower()

    item_specific_key_hints = {
        "itemid", "item_id", "itemcode", "item_code", "itemlevel", "item_level",
        "rarity", "grade", "quality", "icon", "iconurl", "icon_url", "image", "imageurl",
        "image_url", "thumbnail", "damage", "attack", "defense", "stats", "stat",
        "trait", "traits", "effect", "effects", "ability", "abilities", "equip", "slot",
        "weapon", "armor", "material", "currency",
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
    if main_category in {"weapon", "armor"}:
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
    if confidence == "low" and main_category in {"weapon", "armor"}:
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
        if main_category in {"weapon", "armor"} and not is_subcategory_list_url(current_url):
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
        if confidence == "low" and main_category in {"weapon", "armor"}:
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
        if "/db/" in abs_url and "/db/items" not in abs_url:
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
    if not is_items_url(url):
        return False
    # /db/items/weapons/sword ist nur eine Unterliste.
    # Echte Items haben mindestens drei Segmente nach /items/.
    return len(item_tail(url)) >= 3


def category_segment(url: str) -> str:
    parts = path_parts(url)
    idx = item_path_index(url)
    if idx >= 0 and len(parts) > idx + 1:
        return parts[idx + 1].lower()
    return ""


def classify_main_category(url: str, text: str = "") -> str:
    seg = category_segment(url)
    hay = f"{url} {seg} {text}".lower()
    if "weapon" in hay or "waffe" in hay:
        return "weapon"
    if any(x in hay for x in ["armor", "armour", "equipment", "gear", "accessor", "jewelry", "jewellery", "rüstung", "ruestung"]):
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
        r"(?:base\s+)?damage\s*:?\s*(\d+(?:[.,]\d+)?)\s*[-–~]\s*(\d+(?:[.,]\d+)?)",
        r"schaden\s*:?\s*(\d+(?:[.,]\d+)?)\s*[-–~]\s*(\d+(?:[.,]\d+)?)",
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
    raw_lines = [clean_text(x) for x in re.split(r"[\n\r]+| {2,}", text) if clean_text(x)]
    for line in raw_lines:
        low = line.lower()
        if not any(k in low for k in STAT_KEYWORDS):
            continue
        # Typische Questlog-Zeilen: "Strength +3", "Critical Hit Chance 120"
        m = re.match(r"^([A-Za-zÄÖÜäöüß ./%'\-]+?)\s*([+\-]?\d+(?:[.,]\d+)?%?)$", line)
        if m:
            key = clean_text(m.group(1))
            val = clean_text(m.group(2))
            if key and val:
                stats[key] = val
                continue
        # Fallback: komplette Zeile behalten, aber nicht ausufern lassen.
        if 4 <= len(line) <= 90:
            stats.setdefault(line, True)
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
    """Findet Item-Bild/Icon möglichst robust.

    Questlog kann Bilder als normale <img>, lazy-loaded data-src/currentSrc,
    Meta-Preview (og:image) oder CSS background-image ausliefern. Wir speichern
    nur die URL in Postgres; heruntergeladen wird das Bild im MVP nicht.
    """
    candidates: list[dict[str, Any]] = []

    def add_candidate(src: str, *, w: int = 0, h: int = 0, source: str = "") -> None:
        src = str(src or "").strip()
        if not src or src.startswith("data:") or src.endswith(".svg"):
            return
        low = src.lower()
        if not any(x in low for x in ["icon", "item", "cdn", "assets", "questlog", "amazonaws", "cloudfront", "webp", "png", "jpg", "jpeg"]):
            return
        candidates.append({"src": src, "w": int(w or 0), "h": int(h or 0), "source": source})

    try:
        imgs = page.locator("img").evaluate_all(
            """
            els => els.map(img => ({
                src: img.currentSrc || img.src || img.getAttribute('data-src') || img.getAttribute('data-nimg') || '',
                srcset: img.srcset || img.getAttribute('data-srcset') || '',
                alt: img.alt || '',
                w: img.naturalWidth || img.width || 0,
                h: img.naturalHeight || img.height || 0
            }))
            """
        )
        for img in imgs or []:
            add_candidate(str(img.get("src") or ""), w=int(img.get("w") or 0), h=int(img.get("h") or 0), source="img")
            srcset = str(img.get("srcset") or "")
            if srcset:
                # Letzten srcset-Eintrag nehmen, meistens größte Variante.
                last = srcset.split(",")[-1].strip().split(" ")[0]
                add_candidate(last, w=int(img.get("w") or 0), h=int(img.get("h") or 0), source="srcset")
    except Exception:
        pass

    try:
        metas = page.locator("meta[property='og:image'], meta[name='twitter:image']").evaluate_all(
            "els => els.map(e => e.getAttribute('content') || '').filter(Boolean)"
        )
        for src in metas or []:
            add_candidate(str(src), source="meta")
    except Exception:
        pass

    try:
        backgrounds = page.locator("*").evaluate_all(
            """
            els => els.slice(0, 2000).map(e => getComputedStyle(e).backgroundImage || '')
              .filter(v => v && v !== 'none')
            """
        )
        for bg in backgrounds or []:
            for m in re.finditer(r"url\\([\"']?([^\"')]+)[\"']?\\)", str(bg)):
                add_candidate(m.group(1), source="css")
    except Exception:
        pass

    # Dedupe
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

    # Größtes Bild als image_url; quadratisches/kleines Bild als icon_url.
    valid.sort(key=lambda x: int(x.get("w") or 0) * int(x.get("h") or 0), reverse=True)
    image_url = valid[0].get("src")
    square = sorted(valid, key=lambda x: (abs(int(x.get("w") or 0) - int(x.get("h") or 0)), -int(x.get("w") or 0) * int(x.get("h") or 0)))
    icon_url = square[0].get("src") if square else image_url
    return image_url, icon_url

def collect_links(page) -> list[str]:
    try:
        links = page.locator("a[href]").evaluate_all("els => els.map(a => a.href).filter(Boolean)")
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for href in links or []:
        href = str(href).split("#", 1)[0].rstrip("/")
        if not is_items_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append(href)
    return out


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
            url = f"{BASE}/{GAME}/{locale}/db/items/{path}"
            seeds.setdefault(url, CategorySeed(url, classify_main_category(url)))
    # Waffen zuerst, dann Rüstung, Material, Currency, Rest.
    order = {"weapon": 1, "armor": 2, "material": 3, "currency": 4, "misc": 9}
    return sorted(seeds.values(), key=lambda s: (order.get(s.main_category, 9), s.url))


def same_main_category(url: str, main_category: str) -> bool:
    return classify_main_category(url) == main_category


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
    for m in re.finditer(r"/(?:throne-and-liberty)/(?:en|de)/db/items/[A-Za-z0-9_./%\-]+", html_text):
        href = BASE + m.group(0)
        href = href.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        if href not in seen and is_items_url(href):
            seen.add(href)
            urls.append(href)
    return urls


def parse_detail_page(page, url: str, main_category_hint: str, locale: str) -> dict[str, Any] | None:
    if not goto_page(page, url):
        return None
    try:
        body_text = page.locator("body").inner_text(timeout=6000)
    except Exception:
        body_text = page.evaluate("() => document.body ? document.body.innerText : ''")
    raw_text = normalize_raw_text(body_text)
    json_candidates = collect_json_candidates(page)
    name = page_title(page) or best_name_from_json(json_candidates)
    if not name or name.lower() in {"items", "weapons", "questlog"}:
        return None
    main_category = main_category_hint or classify_main_category(url, raw_text)
    sub_category, confidence = detect_sub_category(main_category, raw_text, url)
    if confidence == "low" and main_category in {"weapon", "armor"}:
        # Kategorie aus Listen-URL ist sicher, Untertyp aber nicht.
        confidence = "medium"
    damage_min, damage_max = extract_damage(raw_text)
    item_level = extract_level(raw_text, "Item Level", "Gegenstandsstufe", "Level")
    required_level = extract_level(raw_text, "Required Level", "Benötigte Stufe")
    image_url, icon_url = collect_image_urls(page)
    source_item_id = ""
    parts = path_parts(url)
    if parts:
        source_item_id = parts[-1]
    return {
        "source": "questlog",
        "source_url": url,
        "source_item_id": source_item_id,
        "locale": locale,
        "name": name,
        "slug": slugify(name),
        "main_category": main_category,
        "sub_category": sub_category,
        "rarity": detect_rarity(raw_text),
        "item_level": item_level,
        "required_level": required_level,
        "damage_min": damage_min,
        "damage_max": damage_max,
        "defense": extract_defense(raw_text),
        "stats": extract_stats_from_lines(raw_text),
        "abilities": extract_abilities(raw_text),
        "traits": [],
        "image_url": image_url,
        "icon_url": icon_url,
        "classification_confidence": confidence,
        "raw_text": raw_text[:30000],
        "raw_data": {
            "scraped_at": now_iso(),
            "url": url,
            "main_category_hint": main_category_hint,
            "json_candidate_count": len(json_candidates),
            "parser": "playwright-visible-text-v2-timeout-fix",
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


def crawl_category(context, conn, seed: CategorySeed, limit_left: int | None = None, dry_run: bool = False) -> int:
    """Crawlt eine Questlog-Hauptkategorie.

    Questlog-Struktur bei Items:
      /db/items/weapons              = Hauptliste
      /db/items/weapons/sword        = Unterliste / Waffentyp
      /db/items/weapons/sword/<slug> = echte Item-Detailseite

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

        # 1) Questlog rendert die Itemkarten teils ohne echte Detail-Links.
        # Wichtig: Auf der Hauptseite /weapons liegen im JSON auch Guides/News.
        # Deshalb importieren wir JSON/DOM-Records nur auf Unterseiten wie /weapons/bow.
        page_records: list[dict[str, Any]] = []
        if is_subcategory_list_url(current_url):
            payloads = list(network_payloads)
            payloads.extend(collect_json_candidates(list_page))
            page_records = extract_records_from_json_payloads(payloads, current_url, seed.main_category, DEFAULT_LOCALE)
            dom_records = extract_dom_card_records(list_page, current_url, seed.main_category, DEFAULT_LOCALE)
            existing_record_keys = {record_identity_key(r) for r in page_records}
            for rec in dom_records:
                key = record_identity_key(rec)
                if key not in existing_record_keys:
                    existing_record_keys.add(key)
                    page_records.append(rec)
            page_records = dedupe_records(page_records)

        page_imported = 0
        for item in page_records:
            rec_key = record_identity_key(item)
            if rec_key in seen_detail:
                continue
            seen_detail.add(rec_key)
            try:
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
                print(f"❌ Fehler bei JSON/DOM-Item {item.get('name')}: {type(exc).__name__}: {exc}", flush=True)

        links = collect_links(list_page)
        # Fallback: Links aus HTML/Next.js-Daten ergänzen.
        for href in collect_next_data_urls(list_page):
            if href not in links:
                links.append(href)

        # Nur innerhalb der gewünschten Hauptkategorie bleiben. Sonst nimmt die Waffen-Seite
        # auch Armor/Material-Navigation mit.
        links = [u for u in links if same_main_category(u, seed.main_category)]

        sub_lists = sorted({u for u in links if is_subcategory_list_url(u) and u not in visited_lists})
        detail_links = sorted({u for u in links if is_detail_url(u) and u not in seen_detail})

        for sub in sub_lists:
            if sub not in list_queue:
                list_queue.append(sub)

        next_url = find_next_url(list_page)
        if next_url and same_main_category(next_url, seed.main_category) and is_list_url(next_url):
            if next_url not in visited_lists and next_url not in list_queue:
                list_queue.append(next_url)

        print(f"   ↳ JSON/DOM-Items: {page_imported} · Listen gefunden: {len(sub_lists)} · Detailseiten gefunden: {len(detail_links)}", flush=True)

        if not detail_links:
            continue

        for href in detail_links:
            if limit_left is not None and imported >= limit_left:
                list_page.close()
                detail_page.close()
                return imported
            seen_detail.add(href)
            try:
                item = parse_detail_page(detail_page, href, seed.main_category, DEFAULT_LOCALE)
                if not item:
                    print(f"⚠️ Übersprungen: {href}", flush=True)
                    continue
                if dry_run:
                    print(f"DRY ✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}", flush=True)
                else:
                    upsert_item(conn, item)
                    conn.commit()
                    print(f"✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}", flush=True)
                imported += 1
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


def run_import(args: argparse.Namespace) -> int:
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
                "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
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
        if args.category_url:
            seeds = [CategorySeed(args.category_url.rstrip("/"), classify_main_category(args.category_url))]
        else:
            seeds = discover_category_seeds(page, DEFAULT_LOCALE, include_fallback=True)
        page.close()
        if args.only:
            wanted = {x.strip() for x in args.only.split(",") if x.strip()}
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


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Questlog.gg Item-Importer für Ebo Dashboard/Postgres")
    p.add_argument("--category-url", default="", help="Nur eine konkrete Questlog-Kategorie crawlen, z. B. /db/items/weapons")
    p.add_argument("--only", default="", help="Nur Hauptkategorien, z. B. weapon,armor,material,currency,misc")
    p.add_argument("--discover-only", action="store_true", help="Nur Kategorie-Links anzeigen, nichts importieren")
    p.add_argument("--dry-run", action="store_true", help="Nichts in Postgres schreiben")
    p.add_argument("--reset-category", action="store_true", help="Vor dem Import questlog-Items der gewählten Hauptkategorie löschen")
    return p


if __name__ == "__main__":
    parser = build_arg_parser()
    run_import(parser.parse_args())
