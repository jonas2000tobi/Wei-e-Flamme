# v2.9.0 – Raid-Aufstellung & Leader-Ticket-Chats

## Raid-/Gruppenaufstellung im Dashboard
- Eventdetailseite für Dashboard-Admins um eine interaktive Aufstellung erweitert.
- Angemeldete Spieler lassen sich per Drag & Drop in frei konfigurierbare Gruppen schieben.
- Gruppengröße und Anzahl der Gruppen sind pro Event frei einstellbar (Standard: 6 Spieler pro Gruppe).
- 12 Zusagen ergeben beim ersten Öffnen automatisch zwei 6er-Gruppen.
- Reserve/Bank ist eine eigene Drop-Zone.
- Automatische Verteilung als Option: Tanks, Heals und DPS werden möglichst gleichmäßig über die vorhandenen Gruppen verteilt; Überhang landet auf Reserve.
- Aufstellung wird in Postgres in `dashboard_event_lineups` gespeichert.
- Mit „Auf Discord veröffentlichen“ erstellt der Bot einen separaten Aufstellungspost im Eventkanal.
- Nach der ersten Veröffentlichung sind weitere Drag-&-Drop-Änderungen Live-Sync: Dashboard speichert, Bot-Queue verarbeitet und editiert dieselbe Discord-Nachricht.
- RSVP-Änderungen auf Discord oder im Dashboard ziehen eine bereits veröffentlichte Aufstellung automatisch nach; abgemeldete Spieler werden beim Sync aus Gruppen entfernt.
- Noch nicht eingeteilte Zusagen werden im Discord-Aufstellungspost separat angezeigt.

## Leader Contact – privater Ticket-Chat
- Normale (nicht anonyme) Leader-Contact-Anfragen erhalten im internen Ticket einen neuen Button `💬 Ticket öffnen`.
- Der Button erstellt einen privaten Textkanal für genau das betreffende Mitglied, die konfigurierte Leader-Rolle und den Bot.
- Der Kanal wird in derselben Discord-Kategorie wie der interne Leader-Contact-Kanal erstellt.
- Der ursprüngliche Tickettext wird im privaten Kanal zusammengefasst und mit dem internen Ticket verlinkt.
- Der interne Ticket-Embed erhält einen Link/Verweis auf den privaten Ticket-Chat.
- Mehrfaches Klicken erzeugt keinen zweiten Kanal, sondern verwendet den vorhandenen Ticket-Chat.
- Anonyme Meldungen zeigen den Button nicht.
- Wird das Ticket als erledigt markiert, bekommt der private Ticket-Chat ebenfalls eine Abschlussmeldung; die bestehende Archivlogik bleibt erhalten.

## Geänderte Runtime-Dateien
- `dashboard_web/main.py`
- `bot/event_rsvp_dm.py`
- `bot/leader_contact.py`
