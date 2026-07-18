BEER AND BUFFS – KOMBI-UPDATE

Enthalten:
1. Komplettes Dashboard-Branding
   - sichtbare alte Namen auf Beer and Buffs umgestellt
   - neues Gildenlogo als Standardlogo
   - neues Favicon
   - neue Browser-/Vorschau-Assets
   - Branding bleibt weiterhin über die Guild-Konfiguration überschreibbar

2. Neuer Discord-Befehl
   /guild cleanup_dms
   Parameter:
   - bestaetigen: Ja/Nein
   - neue_gildenzentrale: Ja/Nein
   - verlauf_limit: 1–1000

   Der Befehl löscht bei allen aktuellen Menschen des Discord-Servers
   ausschließlich Nachrichten dieses Bots. Dazu gehören ausdrücklich auch:
   - alte Gildenzentralen
   - alte Gildenmenüs
   - Admin-Menüs
   - Loot-/Event-Menüs
   - sonstige Bot-DMs

   Nutzer-Nachrichten werden nicht gelöscht.
   Optional können anschließend neue Gildenzentralen gesendet werden.

3. Zentrale Itemdatenbank
   - Items aus item_catalog in Discord-Needlisten
   - Items aus item_catalog im Dashboard-Needbuilder
   - Items aus item_catalog bei "Loot gedroppt"
   - stabile catalog_item_id für Need-/Loot-Matching

4. Voice-Sichtbarkeit
   - @MEMBER/voice_allowed sieht neu erzeugte Voice-Kanäle
   - Leitung/Advisor/Guardian werden berücksichtigt
   - voice_blocked gewinnt
   - konfigurierte voice_category wird benutzt

Deployment:
1. ZIP entpacken.
2. Alle enthaltenen Dateien im Repository ersetzen.
3. Commit/Push auf main.
4. Bot-Service UND Dashboard-Web-Service neu deployen.
5. Discord komplett neu öffnen.

DM-Löschung:
 /guild cleanup_dms
 bestaetigen: Ja
 neue_gildenzentrale: Nein
 verlauf_limit: 1000

Für Löschen und direkt frische Menüs:
 /guild cleanup_dms
 bestaetigen: Ja
 neue_gildenzentrale: Ja
 verlauf_limit: 1000

Wichtig:
- Gelöscht werden DMs bei aktuellen Servermitgliedern.
- Nachrichten von Nutzern bleiben bestehen.
- Die Löschung ist nicht rückgängig zu machen.
