DISCORD INTERACTION TIMEOUT FIX

Ersetzt:
- bot/guild_config.py
- bot/dkp_system.py
- bot/member_portal.py

Behoben:
- /guild setup, set_role, set_channel, clear_role, clear_channel, set_rule,
  branding, show und rehome bestätigen Discord sofort.
- Postgres-/Dateizugriffe laufen außerhalb des Discord-Event-Loops.
- /dkp phase3_ec_status bestätigt vor Rollen- und Datenbankprüfung.
- Gildenzentrale: Kalender, Abwesenheiten, Mitglieder und Zurück reagieren
  auch bei langsamer Postgres-Verbindung rechtzeitig.

Keine JSON-Dateien und keine Zugangsdaten enthalten.
