BEER AND BUFFS – ZENTRALE ITEMDATENBANK FÜR NEEDS UND LOOT

Ersetzt:
- bot/runtime_db.py
- bot/loot_needs.py
- bot/loot_auction.py
- dashboard_web/main.py

Funktion:
1. Discord-Needliste
   - Slot auswählen.
   - Bei Waffen zuerst Waffentyp auswählen.
   - Danach Items direkt aus der Postgres-Tabelle item_catalog durchsuchen.
   - 24 Items pro Seite, mit Suche und Vor-/Zurückblättern.
   - Das ausgewählte Item wird über die feste item_catalog.id gespeichert.

2. Discord „Loot gedroppt“
   - Slot und gegebenenfalls Waffentyp auswählen.
   - Item direkt aus item_catalog suchen und auswählen.
   - Der Bot erkennt passende Main- und Secondary-Needs anhand derselben
     item_catalog.id und startet die richtige Auktionsstufe.
   - Die spätere Erhalten-Markierung trifft ebenfalls den korrekten Need-Slot.

3. Dashboard-Needliste
   - Lädt alle aktiven item_catalog-Einträge, nicht nur die ersten 500/10.000.
   - Waffen, Rüstung, Schmuck und Fähigkeitskerne werden nach Slot gefiltert.
   - Speichert dieselbe feste Katalog-ID wie der Discord-Bot.

4. Dashboard „Loot gedroppt“
   - Auswahl aus allen aktiven Items der zentralen Datenbank.
   - Übergibt Name und item_catalog.id an den Bot.
   - Manuelle Texteingabe bleibt als Fallback möglich.

Wichtig:
- Die Tabelle item_catalog muss vom vorhandenen ItemImporter befüllt sein.
- Keine neue Railway-Variable erforderlich.
- Keine JSON-Daten, Tokens oder .env-Dateien enthalten.
- Keine neuen Slash-Commands; das Discord-Command-Limit bleibt unverändert.

Deployment:
1. ZIP entpacken.
2. Die vier Dateien im Repository ersetzen.
3. Commit/Push auf main.
4. Bot-Service und Dashboard-Web-Service neu deployen.
5. ItemImporter muss nicht neu gebaut werden, sofern er bereits dieselbe
   DATABASE_URL nutzt und item_catalog gefüllt ist.

Tests:
- 26 Python-Dateien des Gesamtprojekts erfolgreich kompiliert.
- Dashboard-FastAPI-Import: 150 Routen.
- Dashboard-Picker mit stabiler catalog_item_id getestet.
- Alte und neue lokale Item-IDs matchen über dieselbe item_catalog.id.
