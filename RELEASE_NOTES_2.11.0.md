# Guild Platform v2.11.0

## Mitgliederaktivität & Rückmeldungen
- Neues optionales Modul `member_activity`.
- Admin-Mitgliederprofil: mögliche Events, Zusagen, Reserve, Vielleicht, direkte Ablehnungen, Abwesend (bestätigt -> Abmelden) und keine Rückmeldung.
- Aktivität für 7/30/90 Tage sowie seit Erfassung: aktive Tage, Nachrichten, Reaktionen vergeben/erhalten, Voice-Gesamtzeit, Voice-Sessions, Durchschnitt und letzte Aktivität.
- Eigene Admin-Übersicht `/member-activity`.
- Normale Nachrichteninhalte werden nicht gespeichert; nur Zähler/Tagesaggregate.
- Beginn der Gildenmitgliedschaft wird bei neu vergebenen zentralen Member-/Lead-Rollen erfasst; für bereits bestehende Mitglieder dient der Discord-Serverbeitritt als Fallback.

## Leader Contact / Tickets
- Neuer Admin-Bereich `/tickets` mit offen/übernommen/Archiv, Suche nach Ersteller und Ticketdetail.
- Ticket-Chatverläufe aus explizit eröffneten privaten Ticket-Channels werden dauerhaft zum Ticket gespeichert, inklusive Attachment-Links.
- Interne Leitungsnotizen pro Ticket.
- Ticket-Aktionen aus dem Dashboard: Übernehmen, Ticket-Chat öffnen (nicht anonym), Erledigt.
- Dashboard-Aktionen laufen über eine Bot-Queue und aktualisieren dieselben Discord-Tickets.
- Abschluss erfolgt erst nach erfolgreicher Kopie in den konfigurierten Discord-Archivkanal.
- Privater Ticket-Channel wird nach Abschluss schreibgeschützt und nach 7 Tagen automatisch gelöscht; Dashboard-Archiv bleibt erhalten.

## Onboarding + Aion 2
- Das vorhandene Onboarding bleibt generisch.
- Optionale Aion-2-Komponente in den Onboarding-Einstellungen.
- Wenn aktiviert: Charaktername und Aion-2-Klasse werden zusätzlich abgefragt; die vorhandene Tank/Heal/DPS-Rolle wird ins Aion-Profil übernommen.
- Aion-2-Profil im Mitgliederprofil mit Charaktername, Klasse, Rolle, Level und Gearscore.
- Level und Gearscore sind nachträglich durch Admins im Mitgliederprofil pflegbar.
