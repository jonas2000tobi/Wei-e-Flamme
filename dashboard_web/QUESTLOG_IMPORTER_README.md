# Questlog Item-Importer für Beer and Buffs

Der Importer liest Questlog.gg mit Playwright und speichert die Gegenstände im bestehenden Postgres-Katalog `item_catalog`. Bot und Dashboard verwenden danach dieselbe feste `item_catalog.id`.

## Fähigkeitskerne importieren

Questlog führt die Fähigkeitskerne unter:

```text
https://questlog.gg/throne-and-liberty/de/db/items/misc/perk?page=1
```

Der Importer beginnt bei Seite 1 und ruft automatisch Seite 2, 3 usw. auf, bis keine neuen `perk_*`-Detailseiten mehr vorhanden sind. Die aktuell sichtbaren sieben Seiten müssen deshalb nicht einzeln angegeben werden.

Railway Start Command:

```bash
python questlog_item_importer.py --preset skillcores
```

Erster Test ohne Datenbankänderung:

```bash
python questlog_item_importer.py --preset skillcores --dry-run
```

Nur die bereits importierten Fähigkeitskerne vorher löschen und anschließend sauber neu importieren:

```bash
python questlog_item_importer.py --preset skillcores --reset-category
```

`--reset-category` löscht bei diesem Preset ausdrücklich nur Questlog-Fähigkeitskerne. Andere Einträge der Hauptkategorie `misc` bleiben erhalten.

## Speicherung

Fähigkeitskerne werden so gespeichert:

```text
main_category = misc
sub_category  = Fähigkeitskern
source_item_id = perk_...
```

Zusätzlich werden – soweit Questlog sie liefert – übernommen:

- deutscher Name
- Seltenheit beziehungsweise Tier
- echtes Fähigkeitskern-Bild
- Name und Beschreibung des passiven Effekts
- Questlog-Detail-URL
- stabile `item_catalog.id`

Die Needliste erkennt diese Einträge anschließend für beide Slots:

```text
Fähigkeitskern 1
Fähigkeitskern 2
```

## Weitere Presets

```bash
python questlog_item_importer.py --preset weapon
python questlog_item_importer.py --preset armor
python questlog_item_importer.py --preset accessories
```

## ENV

```text
DATABASE_URL=${{Postgres.DATABASE_URL}}
QUESTLOG_IMPORT_DELAY=1.2
QUESTLOG_MAX_PAGES=250
QUESTLOG_MAX_ITEMS=0
QUESTLOG_HEADLESS=1
QUESTLOG_TIMEOUT_MS=120000
```

`QUESTLOG_MAX_ITEMS=0` bedeutet unbegrenzt.

## Sicherheit

Der Importer überschreibt keine produktiven JSON-Dateien. Er schreibt ausschließlich in `item_catalog` und aktualisiert vorhandene Datensätze über die eindeutige Questlog-Detail-URL.
