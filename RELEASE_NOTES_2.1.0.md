Beer and Buffs – Gesamtpatch V3
Stand: 26.07.2026

Enthalten:
1. Dashboard-Itemdatenbank
   - Standardmäßig Item-Level absteigend: 80 vor 50 vor 20.
   - Kategorieabhängige Typfilter, z. B. bei Waffe keine Hose.
   - Beliebteste und zuletzt angesehene Items.
   - Mobiler Needlisten-Picker.
   - Needlisten-Picker zeigt passende Items sofort und sortiert sie nach Level.
   - Aktuelle Desktop-/Mobile-Kastleton-Header.
   - Mitgliederseite inklusive Abwesenheitsstatus.

2. Discord-Bot Item-/Needlistenlogik
   - Passende Items werden sofort angezeigt; Suche ist nur noch optional.
   - Discord-Selects sind seitenweise mit je 25 Items aufgebaut.
   - Sortierung überall nach Level absteigend, danach alphabetisch.
   - Item-Level wird in der Auswahlbeschreibung angezeigt.

3. Voice-Anwesenheit
   - Join/Leave-Listener bleibt erhalten.
   - Zusätzlicher Abgleich alle 30 Sekunden repariert verpasste Voice-Zustände.
   - Bereits beim Botstart verbundene Mitglieder werden erfasst.
   - Live-Dauer offener Sessions wird im Dashboard berechnet.

4. Rollen-DKP
   - /dkp role role:@Rolle amount:10 reason:Grund
   - Negative Beträge ziehen DKP ab.
   - Bots und @everyone werden übersprungen/gesperrt.

5. Server-Emojis in Eventposts
   - Kalender/Zeit/Abgestimmt/Zielgruppe werden anhand der Server-Emoji-Namen geladen.
   - RSVP-Buttons Tank/Heal/DPS/Reserve/Vielleicht/Abmelden verwenden Server-Emojis.
   - Bestehende aktuelle und kommende Eventposts werden beim Botstart neu editiert.

6. EC-Anwesenheitsprüfung
   - Neuer Button „Alle offenen → War da“.
   - Bereits gesetzte Status werden bei der Sammelaktion nicht überschrieben.
   - Einzelbearbeitung und Spieler nachtragen bleiben erhalten.
   - Zähler, Vorschau und EC-Vergabe zählen jede Discord-Nutzer-ID nur einmal.
   - Historische doppelte Teilnehmerzeilen werden automatisch bereinigt.
   - Gesetzte und manuell nachgetragene Spieler stehen in der Auswahl zuerst.
   - Auswahl zeigt Seite X/Y und besitzt einen Aktualisieren-Button.
   - Manuelles Nachtragen eines bereits angemeldeten Spielers überschreibt dessen RSVP-Rolle nicht mehr.
   - Reserve-Status kann korrekt gespeichert werden.
   - Offene Check-Nachrichten werden nach einem Deploy automatisch aktualisiert.

Nicht verändert:
- voice_creator.py
- Event-Voice-Erstellung
- Event-Voice-Zeitsteuerung

Installation:
- Ordner bot/ und dashboard_web/ über das Projekt kopieren.
- Danach Bot-Service und Dashboard-Web-Service neu deployen.
- Für ausschließlich diesen Anwesenheitsfix reichen bot/dkp_system.py und bot/event_rsvp_dm.py sowie ein Bot-Redeploy.


V4 Anwesenheits-Synchronisierung:
- Discord-Bot-Nachträge und Status werden sofort ins Dashboard-Snapshot veröffentlicht.
- Dashboard-Reviews werden über Postgres an die echte Bot-Anwesenheit übertragen.
- Spieler können im Dashboard zur Anwesenheit hinzugefügt werden.
- Bot-Button "Alle offenen → War da" plus /dkp attendance_all_present als sichere Alternative.
- Zähler basieren auf eindeutigen Discord-Nutzer-IDs.
