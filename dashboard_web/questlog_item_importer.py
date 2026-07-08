from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

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


def is_category_list_url(url: str) -> bool:
    if not is_items_url(url):
        return False
    parts = path_parts(url)
    idx = item_path_index(url)
    return idx >= 0 and len(parts) == idx + 2


def is_detail_url(url: str) -> bool:
    if not is_items_url(url):
        return False
    parts = path_parts(url)
    idx = item_path_index(url)
    if idx < 0:
        return False
    # /db/items/weapons/some-item ist Detail. /db/items/weapons?page=2 ist Liste.
    return len(parts) >= idx + 3


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
        href = str(e.get("href") or "")
        label = " ".join([str(e.get("text") or ""), str(e.get("aria") or ""), str(e.get("rel") or "")])
        if href and is_items_url(href) and any(x in label for x in ["next", "weiter"]):
            return href.rstrip("/")
    current = page.url
    m = re.search(r"([?&]page=)(\d+)", current)
    if m:
        return re.sub(r"([?&]page=)\d+", lambda mm: f"{mm.group(1)}{int(m.group(2)) + 1}", current)
    joiner = "&" if "?" in current else "?"
    return f"{current}{joiner}page=2"


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


def crawl_category(context, conn, seed: CategorySeed, limit_left: int | None = None, dry_run: bool = False) -> int:
    list_page = context.new_page()
    detail_page = context.new_page()
    imported = 0
    seen_detail: set[str] = set()
    current_url = seed.url
    for page_no in range(1, MAX_PAGES + 1):
        print(f"📄 {seed.main_category}: {current_url}")
        if not goto_page(list_page, current_url):
            break
        detail_links = [u for u in collect_links(list_page) if is_detail_url(u)]
        # Falls Questlog die erste Seite unter /weapons ohne ?page rendert und Page 2 durch Fallback erfunden wurde,
        # stoppen wir, sobald keine Detail-Links mehr kommen.
        new_links = [u for u in detail_links if u not in seen_detail]
        if not new_links:
            print("ℹ️ Keine neuen Detailseiten mehr gefunden.")
            break
        for href in new_links:
            if limit_left is not None and imported >= limit_left:
                list_page.close()
                detail_page.close()
                return imported
            seen_detail.add(href)
            try:
                item = parse_detail_page(detail_page, href, seed.main_category, DEFAULT_LOCALE)
                if not item:
                    print(f"⚠️ Übersprungen: {href}")
                    continue
                if dry_run:
                    print(f"DRY ✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}")
                else:
                    upsert_item(conn, item)
                    conn.commit()
                    print(f"✅ {item['main_category']} / {item.get('sub_category') or '-'} / {item['name']}")
                imported += 1
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"❌ Fehler bei {href}: {type(exc).__name__}: {exc}")
            time.sleep(REQUEST_DELAY)
        next_url = find_next_url(list_page)
        if not next_url or next_url == current_url:
            break
        current_url = next_url
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
    return p


if __name__ == "__main__":
    parser = build_arg_parser()
    run_import(parser.parse_args())
