# v2.10.1 – Rollenpicker & Leader-Archiv-Fix

- `/guild set_role` akzeptiert nun ein leeres `role`-Feld und öffnet dann einen nativen Discord-Rollenpicker.
- `/dashboard set_member_role` besitzt denselben Rollenpicker-Fallback und aktualisiert den Dashboard-Snapshot direkt nach Auswahl.
- Leader-Ticket-Archivierung löst den Archivkanal bei Bedarf per Discord API nach, statt nur den lokalen Cache zu verwenden.
- Vor dem Archivieren werden Senderechte geprüft; bei Fehler bleibt das aktive Ticket erhalten und zeigt die konkrete Ursache.
- Das aktive Leader-Ticket wird erst gelöscht, nachdem die Archivkopie erfolgreich erstellt wurde.
