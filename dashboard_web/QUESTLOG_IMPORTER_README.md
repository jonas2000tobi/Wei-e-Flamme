# Questlog Item-Importer für Ebo Dashboard

Ziel: Questlog.gg wird per Playwright gelesen, Items werden lokal in Postgres unter `item_catalog` gespeichert. Bot und Dashboard lesen danach nur aus Postgres.

## Railway-Service

Empfohlen: eigener Railway-Service mit Root `dashboard_web`, weil dort Playwright/Chromium bereits eingerichtet ist.

Start Command:

```bash
python questlog_item_importer.py --only weapon,armor,material,currency,misc
```

Für den ersten Test nur Waffen:

```bash
python questlog_item_importer.py --category-url https://questlog.gg/throne-and-liberty/en/db/items/weapons --only weapon
```

## ENV

```text
DATABASE_URL=${{Postgres.DATABASE_URL}}
QUESTLOG_LOCALE=en
QUESTLOG_IMPORT_DELAY=1.2
QUESTLOG_MAX_PAGES=250
QUESTLOG_MAX_ITEMS=0
QUESTLOG_HEADLESS=1
```

`QUESTLOG_MAX_ITEMS=0` bedeutet unbegrenzt.

## Sicherheit

Der Importer überschreibt keine produktiven JSON-Dateien. Er schreibt nur in die neue Postgres-Tabelle `item_catalog`.

## Dashboard

Nach dem Import:

```text
/items
/api/items
/api/items?category=weapon
/api/items?category=weapon&sub_category=Langbogen
```
