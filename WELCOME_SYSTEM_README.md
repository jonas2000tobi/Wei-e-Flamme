# Welcome & Onboarding System – v2.5.0

Das Welcome-System ist Teil des optionalen Moduls `Onboarding & Recruitment`.

## Ablauf

1. Ein neues Mitglied betritt den Discord-Server.
2. Der Bot erstellt im konfigurierten Welcome-Kanal eine Embed-Nachricht mit Discord-Avatar, Anzeigename und einem zufälligen Spruch.
3. Status: `🟡 Onboarding läuft`.
4. Das vorhandene DM-Onboarding wird gestartet.
5. Bei aktiviertem Staff-Review wird dieselbe Welcome-Nachricht zu `🟠 Wartet auf Freigabe` geändert.
6. Nach erfolgreichem Onboarding wird dieselbe Nachricht zu `🟢 Onboarding abgeschlossen` geändert und zeigt Kategorie, Rolle und Erfahrungsstatus.
7. Bei Ablehnung wird sie rot markiert.
8. Verlässt das Mitglied den Server, kann dieselbe Nachricht optional zu `⚫ Server verlassen` geändert werden.

## Dashboard

`Einstellungen -> Module -> Onboarding & Recruitment aktivieren`

Danach erscheint unter den Modulen der Bereich `Welcome Card` mit:

- Welcome Card an/aus
- Welcome-Kanal
- Update bei Serveraustritt an/aus
- bis zu 50 frei editierbaren Zufallssprüchen

Unterstützte Platzhalter:

- `{user}` – Discord-Anzeigename
- `{guild}` – Gildenname
- `{member_count}` – aktuelle Mitgliederzahl

Der Welcome-Kanal verwendet dieselbe zentrale Einstellung wie `Gilde & Discord -> Willkommen`.

## Persistenz

Die Welcome-Konfiguration liegt in `module_settings` / `guild_settings` und wird zwischen Dashboard und Bot geteilt. Die Message-IDs bereits geposteter Welcome Cards werden botseitig in `bot/data/onboarding_welcome_messages.json` gespeichert, damit Nachrichten auch nach einem Bot-Neustart weiter bearbeitet werden können.
