from __future__ import annotations

import html as html_lib
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")

EVENTS: tuple[dict[str, str], ...] = (
    {"key": "watcher_kaira", "name": "Watcher Kaira", "type": "Boss", "icon": "👹"},
    {"key": "beritra_air_raid", "name": "Beritra-Luftangriff", "type": "Boss", "icon": "🐉"},
    {"key": "shugofesta", "name": "Shugofesta", "type": "Event", "icon": "🎪"},
    {"key": "space_time_rift", "name": "Raum-Zeit-Riss", "type": "PvP", "icon": "🌀"},
    {"key": "abyss_event", "name": "Abyss-Event", "type": "Event", "icon": "⚔️"},
    {"key": "reshanta_world_bosses", "name": "Reshanta-Weltbosse", "type": "Boss", "icon": "👹"},
    {"key": "abyss_siege_nahma", "name": "Abyss-Belagerungsboss & Nahma", "type": "Boss", "icon": "🐲"},
    {"key": "abyss_rift", "name": "Abyss-Riss", "type": "PvP", "icon": "🌀"},
)
EVENT_BY_KEY = {row["key"]: row for row in EVENTS}
WEEKDAYS = {"mo": 0, "di": 1, "mi": 2, "do": 3, "fr": 4, "sa": 5, "so": 6}


def visible_text(raw_html: str) -> str:
    text = str(raw_html or "")
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(?:div|p|li|section|article|h1|h2|h3|h4|tr|td|button)\s*>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html_lib.unescape(text).replace("\xa0", " ")
    lines: list[str] = []
    for line in text.splitlines():
        clean = re.sub(r"\s+", " ", line).strip()
        if clean:
            lines.append(clean)
    return "\n".join(lines)


def resolve_display_time(text: str, now_local: datetime) -> datetime | None:
    s = str(text or "")
    m = re.search(
        r"(?i)\b(heute|morgen|Mo|Di|Mi|Do|Fr|Sa|So)?\s*(\d{1,2}):(\d{2})\s*Uhr\b",
        s,
    )
    if not m:
        return None
    prefix = (m.group(1) or "").casefold()
    hour = int(m.group(2))
    minute = int(m.group(3))
    base = now_local
    if prefix == "morgen":
        base = base + timedelta(days=1)
    elif prefix in WEEKDAYS:
        target = WEEKDAYS[prefix]
        days = (target - base.weekday()) % 7
        candidate = base.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days)
        if days == 0 and candidate < base - timedelta(minutes=5):
            candidate += timedelta(days=7)
        return candidate
    candidate = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if not prefix and candidate < base - timedelta(hours=12):
        candidate += timedelta(days=1)
    return candidate


def _event_chunks(text: str) -> list[tuple[dict[str, str], str]]:
    matches: list[tuple[int, int, dict[str, str]]] = []
    for event in EVENTS:
        for m in re.finditer(re.escape(event["name"]), text, flags=re.IGNORECASE):
            matches.append((m.start(), m.end(), event))
    matches.sort(key=lambda item: item[0])
    out: list[tuple[dict[str, str], str]] = []
    seen: set[str] = set()
    for idx, (start, _end, event) in enumerate(matches):
        if event["key"] in seen:
            continue
        seen.add(event["key"])
        next_start = len(text)
        for other_start, _, other_event in matches[idx + 1 :]:
            if other_event["key"] not in seen:
                next_start = other_start
                break
        out.append((event, text[start:next_start].strip()))
    return out


def parse_wakayashi_events(raw_html: str, *, now: datetime | None = None, main_page: bool = False) -> list[dict[str, Any]]:
    now_local = (now or datetime.now(timezone.utc)).astimezone(BERLIN)
    text = visible_text(raw_html)
    if main_page:
        start = text.casefold().find("event-timer")
        if start >= 0:
            text = text[start:]
        end = text.casefold().find("interaktive karte")
        if end > 0:
            text = text[:end]
    events: list[dict[str, Any]] = []
    for event, chunk in _event_chunks(text):
        low = chunk.casefold()
        active = bool(re.search(r"\blive\b|endet\s+in", low))
        scheduled = resolve_display_time(chunk, now_local)
        events.append(
            {
                "key": event["key"],
                "name": event["name"],
                "type": event["type"],
                "icon": event["icon"],
                "status": "ACTIVE" if active else "UPCOMING",
                "scheduled_at": scheduled.astimezone(timezone.utc).isoformat() if scheduled else "",
                "source_text": re.sub(r"\s+", " ", chunk).strip()[:800],
            }
        )
    return events


def merge_events(primary: list[dict[str, Any]], extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for event in extra:
        if event.get("key"):
            merged[str(event["key"])] = dict(event)
    for event in primary:
        if event.get("key"):
            merged[str(event["key"])] = dict(event)
    return list(merged.values())
