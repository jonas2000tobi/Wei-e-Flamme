
# TL Event Reminder Discord Bot

Ein einfacher Discord-Bot (Python) für **Throne & Liberty**-Event-Erinnerungen.

## Features
- Slash-Commands zum Konfigurieren: Kanal setzen, Events hinzufügen/auflisten/entfernen
- Mehrere **Pre-Reminder** (z. B. 30/10/5 Minuten vor Start)
- **Europe/Berlin**-Zeitzone
- Persistenz als JSON (kein externer DB-Server nötig)

## Schnellstart
1. **Python 3.11+** installieren.
2. `pip install -r requirements.txt`
3. Umgebungsvariable setzen (PowerShell / Bash):
   - Windows (PowerShell): `$env:DISCORD_BOT_TOKEN = "DEIN_TOKEN"`
   - Linux/macOS (bash): `export DISCORD_BOT_TOKEN="DEIN_TOKEN"`
4. `python bot.py` starten.

## Discord-Bot anlegen (Kurz)
- https://discord.com/developers/applications → New Application → Bot hinzufügen.
- **Privileged Gateway Intents** sind nicht nötig (für Slash Commands reicht Standard).
- Unter **OAuth2 → URL Generator**: Scopes `bot`, `applications.commands` anhaken.
- Berechtigungen: `Administrator` (oder granular, aber dann aufpassen).
- Invite-Link erzeugen → Bot auf deinen Server einladen.

## Nutzung der Commands
- `/set_announce_channel #dein-channel`
- `/add_event name:"Siege" weekdays:"Mon,Thu" start_time:"20:00" duration_min:60 pre_reminders:"30,10" mention_role:@TL-Events`
- `/list_events`
- `/remove_event name:"Siege"`
- `/test_event_ping name:"Siege"`

**Hinweise:**
- Wochentage: `Mon..Sun` oder `0..6` (0=Mon).
- Zeiten im 24h-Format der **Europe/Berlin**-Zeitzone.
- Der Bot postet **genau zur Minute** (Task läuft im 30s-Takt).
- Doppelte Posts werden über eine Post-Log-Datei verhindert.

## Persistenz
- Daten liegen im Ordner `data/` (wird beim ersten Start angelegt).
- `guild_configs.json` enthält die Events je Server.
- `post_log.json` verhindert doppelte Erinnerungen.

## Produktionstipps
- In `systemd` oder als Docker-Container laufen lassen.
- Regelmäßig Backups vom `data/`-Ordner machen.
- Für Multi-Server-Betrieb reicht das JSON-Format; bei großem Umfang ggf. SQLite nutzen.

Viel Spaß!
