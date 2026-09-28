# Guild Platform 2.6.0 – Bewerbungen, Voice Setup und Event-Standardkanal

## Neu
- Onboarding-Review nennt beim Akzeptieren/Ablehnen jetzt den betroffenen Member mit Anzeigename und Mention.
- Kategorie **Bewerber** erhält nach abgeschlossenem Onboarding sofort die konfigurierte Bewerberrolle.
- Für Bewerber kann automatisch ein privater Textkanal `bewerbung-<name>` erstellt werden.
- Bewerbungs-Chat ist nur für Bewerber, konfigurierte Lead-Rolle und Bot sichtbar (Discord-Administratoren umgehen Kanal-Overwrites technisch immer).
- Bewerbungs-Chat wird im Review verlinkt und erhält Statusmeldungen bei Annahme/Ablehnung.
- Bewerbungs-Kategorie und Lead-Rolle sind im Dashboard unter Onboarding konfigurierbar.
- Neuer Discord-Command `/onboarding application_chat` zur schnellen Konfiguration ohne Dashboard.
- Neuer `/voice_panel status` zeigt Ziel-Kategorie, Return-Voice und Rollenrechte.

## Verbessert
- `/event create` nutzt jetzt automatisch `/guild set_channel kind:events`, wenn kein expliziter Channel angegeben wird.
- `/event create` hat zusätzlich den optionalen Parameter `channel` zum Überschreiben des Standardkanals.
- Voice-Panel erklärt nun korrekt, ob eine zentrale Voice-Kategorie oder die Kategorie des Panel-Textkanals verwendet wird.
