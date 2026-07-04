# Ebo Dashboard Web-Service

Separater Railway-Service für das read-only Web-Dashboard.

## Railway Variablen

Im Web-Service setzen:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
DASHBOARD_USERNAME=admin
DASHBOARD_PASSWORD=<sicheres-passwort>
```

Optional:

```txt
DASHBOARD_GUILD_ID=1457385148730576987
```

## Start

Railway nutzt den Procfile:

```txt
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

Der Bot veröffentlicht alle 5 Minuten einen Snapshot in Postgres. Sofort aktualisieren kannst du im Discord mit `/dashboard_status`.
