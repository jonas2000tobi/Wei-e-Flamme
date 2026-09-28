# Beer and Buffs Bot

Discord-Bot für Events/RSVP, EC/DKP, Loot-Auktionen, Needlisten, Onboarding,
Voice-Anwesenheit und die private Gildenzentrale.

## Start

```bash
python -m pip install -r requirements.txt
python bot/bot.py
```

Erforderlich ist mindestens `DISCORD_TOKEN`. PostgreSQL-Funktionen verwenden
`DATABASE_URL`; ohne PostgreSQL bleiben die lokalen JSON-Dateien die produktive
Fallback-Quelle.

## Dashboard

Das FastAPI-Dashboard liegt in `dashboard_web/` und wird als eigener Railway-
Service betrieben. Sicherheitsrelevant sind insbesondere:

- `DASHBOARD_SESSION_SECRET`: zufälliger Wert mit mindestens 32 Zeichen
- Discord OAuth-Variablen oder bewusst aktivierte Basic-Auth
- `DATABASE_URL`

## Daten

Produktive Laufzeitdaten liegen unter `bot/data/`. Diese Dateien nicht durch
leere Beispieldateien ersetzen. Vor Deployments immer ein Backup behalten.
