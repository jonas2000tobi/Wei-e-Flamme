BEER AND BUFFS – LOGO- UND GILDENMENÜ-FIX

Ersetzt:
- bot/member_portal.py
- dashboard_web/main.py
- dashboard_web/static/beer_and_buffs_logo.png
- dashboard_web/static/beer_and_buffs_favicon.png
- dashboard_web/static/logo.png
- dashboard_web/static/logo_128.png
- dashboard_web/static/logo_512.png
- dashboard_web/static/beer_and_buffs_opengraph.webp

Behoben:
1. Dashboard-Logo
   - Logo liegt jetzt im tatsächlich gemounteten Ordner dashboard_web/static.
   - Transparente PNG-Version erstellt.
   - Header, Statusbereich, Favicon und Browser-Vorschau nutzen das neue Logo.
   - Asset-Version erhöht, damit Browser das kaputte alte Bild nicht aus dem Cache laden.

2. Discord-Gildenzentrale
   - Mitglieder-Button reagiert sofort und zeigt nicht mehr scheinbar gar nichts.
   - Kalender, Abwesenheiten, Mitglieder und Zurück bestätigen Discord direkt.
   - Fehler werden sichtbar gemeldet und zusätzlich in Railway geloggt.
   - Persistente Custom-IDs geprüft: 101 IDs, keine Duplikate.
   - Mitgliederliste zählt die Vereinigungsmenge aus member, leader, advisor und guardian.
   - Embed-Länge wird sicher begrenzt.
   - DM-Serverauflösung wird 5 Minuten im RAM gecacht.
   - Identische Buttonklicks schreiben nicht mehr jedes Mal synchron auf das Railway-Volume.

Tests:
- 26 Python-Dateien erfolgreich kompiliert.
- Alle 101 Portal-Custom-IDs eindeutig.
- Logo/Favicon als RGBA mit Alpha-Kanal geprüft.

Deployment:
1. ZIP entpacken.
2. Dateien an denselben Pfaden ersetzen.
3. Commit und Push.
4. Bot-Service UND Dashboard-Web-Service neu deployen.
5. Browser hart neu laden oder Cache leeren.
6. Discord-DM: Gilde -> Mitglieder testen.

Hinweis:
Das hochgeladene Bild kam technisch als JPEG ohne Alpha-Kanal an.
Der randverbundene schwarze Hintergrund wurde deshalb in eine echte
transparente PNG-Version umgewandelt, ohne die schwarzen Innenkonturen
des Logos pauschal zu löschen.
