# Beer and Buffs Plattform 2.1.0

## Sicherheits- und Stabilitätsupdate

Dieses Release fasst die komplette technische Härtung in einem Update zusammen. Vorhandene produktive Dateien unter `bot/data/` wurden nicht verändert, geleert oder ersetzt. Die zuvor leere Datei `bot/guild_configs.json` wurde lediglich zu gültigem JSON (`{}`) normalisiert.

## Vor dem Deployment zwingend setzen

### Dashboard-Session

`DASHBOARD_SESSION_SECRET` ist jetzt Pflicht und muss mindestens 32 Zeichen lang sein. Ohne sicheren Wert startet das Dashboard absichtlich nicht.

Beispiel zum Erzeugen eines Secrets:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

### Empfohlener Loginmodus

```text
DASHBOARD_AUTH_MODE=discord
DASHBOARD_SESSION_SECRET=<zufälliger Wert>
DASHBOARD_DISCORD_CLIENT_SECRET=<Discord OAuth Secret>
```

Die Client-ID kann weiterhin aus dem aktuellen Bot-Snapshot aufgelöst werden. Eine feste `DASHBOARD_DISCORD_CLIENT_ID` kann zusätzlich gesetzt werden.

### Basic Auth nur als Notfallzugang

Ein gesetztes Passwort öffnet keinen zweiten Loginweg mehr. Basic Auth muss ausdrücklich aktiviert werden:

```text
DASHBOARD_ALLOW_BASIC_AUTH=1
DASHBOARD_USERNAME=<Benutzername>
DASHBOARD_PASSWORD=<starkes Passwort>
```

Alternativ kann `DASHBOARD_AUTH_MODE=basic` genutzt werden. Für den Normalbetrieb bleibt Discord OAuth die Empfehlung.

## Änderungen

### Dashboard-Sicherheit

- Vorhersagbarer Session-Fallback entfernt.
- Dashboard startet ohne sicheren Session-Secret nicht mehr.
- Discord-Mitgliedschaft und Adminstatus werden bei jedem Request gegen den aktuellen Bot-Snapshot geprüft.
- Entzogene Rollen wirken unmittelbar; ein alter Session-Cookie behält keine Adminrechte.
- `/api/snapshot` liefert Mitgliedern nur noch das eigene Profil, eigene EC-Daten, eigene Needs, freigegebene Events und bereinigte Auktionsdaten.
- Vollständiger Snapshot liegt unter `/api/admin/snapshot` und ist admin-only.
- System-, Audit-, Datenbank-, Attendance-, Export- und EC-Verwaltungsrouten sind admin-only.
- Zustandsändernde Datenbankaktionen wurden von GET auf POST umgestellt.
- Same-Origin-/CSRF-Prüfung für schreibende Requests ergänzt.
- Externe `next`-Weiterleitungen und Open Redirects blockiert.
- Basic Auth ist nur noch ein ausdrücklich aktivierter Notfallmodus.
- Drei rein synchrone Adminrouten laufen nicht mehr fälschlich als blockierende Async-Routen.
- Veralteten FastAPI-Startup-Hook durch Lifespan-Handler ersetzt.

### Auktionen und Sofortkauf

- Pro Auktion gibt es einen eigenen `asyncio.Lock`.
- Gebot, Sofortkauf, Dashboard-Aktion und automatischer Abschluss verwenden denselben Lock.
- Sofortkäufe erhalten einen reservierten `processing`-Status.
- Doppelklicks und parallele Kaufversuche werden idempotent abgefangen.
- EC-Abbuchung wird bei fehlgeschlagener Auslieferung zurückgerollt beziehungsweise erstattet.
- Fehlgeschlagene Erstposts bleiben nicht mehr als scheinbar aktive Auktion gespeichert.
- Einzelne Auktionsänderungen werden gezielt nach PostgreSQL geschrieben statt alles komplett neu zu spiegeln.

### EC/DKP

- Prozessweiter Transaktions-Lock ergänzt.
- Eindeutige Request-IDs verhindern doppelte Dashboard-Buchungen.
- Pending-Journal repariert unterbrochene JSON-Buchungen beim Neustart.
- Saldo und EC-Journal werden im PostgreSQL-Mirror in derselben Transaktion geschrieben.
- Startguthaben und spätere Buchungen nutzen denselben abgesicherten Ablauf.

### Events und RSVP

- Pro Event gibt es einen eigenen RSVP-Lock.
- Discord-Interaktionen werden früh bestätigt, bevor Datei-, DB- oder Discord-Folgearbeit beginnt.
- Einzelne RSVP- und Eventänderungen verwenden gezielte PostgreSQL-Upserts.
- Vollspiegelung bleibt als Reparaturfunktion verfügbar.

### Needs, Profile und Abwesenheiten

- Einzelne Need-Änderungen spiegeln nur noch den betroffenen Spieler.
- Kataloglöschung, Cleanup und kompletter Reset spiegeln nur noch die betroffene Gilde.
- Mehrere schnelle Need-Änderungen werden gebündelt, ohne den gesamten Gildenbestand neu zu schreiben.
- Profiländerungen und Abwesenheitsmeldungen spiegeln nur noch das betroffene Mitglied.
- Dashboard-Profilqueue schreibt nur die tatsächlich geänderten Profile.
- Profil- und Abwesenheitswrites laufen außerhalb des Discord-Event-Loops.

### Persistente Interaktionen und Startverhalten

- Offene Onboarding-Schritte werden in `onboarding_sessions.json` gespeichert.
- Onboarding-Buttons werden nach einem Botneustart erneut registriert.
- Fehler beim Senden einer Onboarding-DM werden nicht mehr als Erfolg gemeldet.
- Schlägt ein Modulsetup oder der Join-Hook fehl, beendet sich der Bot kontrolliert statt teilweise aktiv weiterzulaufen.

### Slash-Commands

Funktionen wurden nicht entfernt. Einzelne Top-Level-Befehle wurden unter Gruppen zusammengefasst.

Aktuelle Top-Level-Struktur:

```text
/alliance
/attendance
/auction
/audit
/dashboard
/dkp
/event
/guild
/leader
/loot
/onboarding
/ping
/portal
/raid
/report
/template
/voice_panel
```

Damit sind es **17 Top-Level-Commands** statt ungefähr 98. Keine Gruppe überschreitet 25 Unterbefehle.

Beispiele:

```text
/raid_create_dm              -> /event create
/raid_delete                 -> /event delete
/raid_resend_missing         -> /event resend_missing
/raid_template_create        -> /template create
/loot_need_all               -> /loot need_all
/loot_item_add               -> /loot item_add
/portal_force_new_all        -> /portal force_all
/onboarding_status           -> /onboarding status
/weekly_report_now           -> /report now
/voice_attendance_status     -> /attendance status
```

Beim Botstart werden die Commands serverbezogen synchronisiert und alte globale Commands entfernt. Die neuen Befehle sind dadurch pro Server direkt verfügbar.

### Deployment und Repository

- `bot/requirements.txt` enthält wieder eine echte Paketliste.
- Root-`requirements.txt` bereinigt.
- Gültige `dashboard_web/railpack.json` ergänzt.
- Chromium wird im Dashboard-Docker-Build installiert, nicht bei jedem Start.
- Alte Botkopien, eingebettetes Alt-ZIP, Bytecode und 19 exakte Bildduplikate entfernt.
- Echte Bot-Dokumentation, `.gitignore`, Testkonfiguration und Release-Validator ergänzt.

### Build-Builder

Die bisher frei berechnete „Kampfkraft“ heißt jetzt **Build-Score** und ist sichtbar als grober Vergleichswert gekennzeichnet. Grundwerte und Breakpoints werden ausdrücklich als Vorschau behandelt. Es wird nicht mehr der Eindruck erweckt, dies sei eine offizielle oder exakte Throne-and-Liberty-Formel.

## Validierung

- 28 Python-Dateien vollständig per AST geparst.
- 4 JSON-Dateien validiert.
- 6 automatisierte Stabilitätstests bestanden.
- Alle 15 modularen Setup-Funktionen konnten auf einem isolierten Projektklon registriert werden.
- 17 Top-Level-Commands registriert; größte Gruppe: 21 Unterbefehle.
- Dashboard-Sicherheits-, EC-, Auktions-, RSVP-, Need-, Profil-, Deployment- und Repository-Prüfungen bestanden.
- Shellsyntax von `dashboard_web/start.sh` und JSON-Syntax von `railpack.json` geprüft.
- Keine Änderungen unter `bot/data/` gegenüber dem hochgeladenen Projekt.

## Bewusste Grenze der Prüfung

Das Release wurde statisch, mit Unit-Tests und durch lokale Discord-Command-Registrierung geprüft. Es wurde nicht mit deinem echten Discord-Token, deiner produktiven Railway-Umgebung oder deiner produktiven PostgreSQL-Datenbank ausgeführt. Vor dem Austausch des laufenden Services deshalb das aktuelle Railway-Deployment und Volume sichern.

## Empfohlene Deployment-Reihenfolge

1. Aktuelles Railway-Deployment und Volume sichern.
2. `DASHBOARD_SESSION_SECRET` und Loginvariablen setzen.
3. Bot-Service mit diesem Release deployen und den erfolgreichen Modul-/Command-Sync im Log prüfen.
4. Danach Dashboard-Service deployen.
5. Discord-Login, Memberbereich und Adminbereich getrennt testen.
6. Testauktion mit zwei parallelen Klicks sowie eine Test-EC-Buchung durchführen.
7. Erst danach reguläre Gildenauktionen wieder freigeben.
