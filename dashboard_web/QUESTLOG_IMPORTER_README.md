# Status Live Truthy Hotfix

Geändert: `dashboard_web/main.py`

## Fix
- Serverstatus wird nicht mehr aus Legenden/alten Maintenance-Historien geraten.
- Fearless/Europe wird zuerst über Throney geprüft, das den offiziellen T&L-Status alle 15 Minuten spiegelt.
- Questlog/Official bleiben Fallbacks, aber nur bei explizitem Treffer direkt am Servernamen.
- TLDB wurde als Statusquelle entfernt, weil die Seite aktuell globale/alte Maintenance-Texte liefern kann und dadurch falsch-positive Wartung erzeugt.
- Wetter und Spielzeit werden nicht mehr geraten. Wenn Questlog per einfachem HTTP nicht lesbar ist, bleibt es ehrlich "nicht ermittelbar".

## Hintergrund
Questlog Rain/Day-Night laden die echten Inhalte clientseitig per JavaScript. Ohne gefundenen internen JSON-Endpunkt oder Browser-Renderer/Playwright kann das Dashboard diese Werte nicht zuverlässig live auslesen.
