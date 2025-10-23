# bot/bot.py
# Einstieg für den Bot – nur DM-RSVP aktiv, kein altes öffentliches RSVP.

from __future__ import annotations
import os
import discord
from discord import app_commands

# Robust import (funktioniert als Paket "bot." und auch flach)
try:
    from bot.event_rsvp_dm import setup_rsvp_dm
except ModuleNotFoundError:
    from event_rsvp_dm import setup_rsvp_dm


# ---------------- Discord Setup ----------------
INTENTS = discord.Intents.default()
INTENTS.members = True           # wichtig für Rollen-Mitglieder (DM-Zielgruppe)
INTENTS.guilds = True
INTENTS.message_content = False  # nicht nötig

client = discord.Client(intents=INTENTS)
tree = app_commands.CommandTree(client)


# ---------------- Admin: Hard-Sync ----------------
@tree.command(name="wf_admin_sync_hard", description="Hard-Sync: Guild-Scope leeren & global syncen (Admin)")
async def wf_admin_sync_hard(inter: discord.Interaction):
    perms = getattr(inter.user, "guild_permissions", None)
    if not perms or not (perms.administrator or perms.manage_guild):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
        return
    await inter.response.defer(ephemeral=True, thinking=True)
    try:
        # 1) global sync
        await tree.sync()
        # 2) Guild-Scope leeren (entfernt alte Leichen)
        guild_obj = discord.Object(id=inter.guild_id)
        tree.clear_commands(guild=guild_obj)
        await tree.sync(guild=guild_obj)
        await inter.followup.send("✅ Hard-Sync: Guild-Scope geleert & global synchronisiert.", ephemeral=True)
    except Exception as e:
        await inter.followup.send(f"❌ Fehler: {e}", ephemeral=True)


# ---------------- Kurzhilfe ----------------
@tree.command(name="wf", description="Weiße Flamme – Hilfe (DM-RSVP aktiv)")
async def wf(inter: discord.Interaction):
    txt = (
        "🔥 **DM-RSVP aktiv** – Anmeldungen laufen per **DM**.\n"
        "• `/raid_dm_set_role` – Standard-Zielrolle speichern\n"
        "• `/raid_dm_create` – Raid erstellen (optional `target_role` direkt angeben)\n"
        "• `/raid_dm_show` – Übersicht neu zeichnen\n"
        "• `/raid_dm_close` – Übersicht sperren (DMs bleiben nutzbar)\n\n"
        "Die Übersicht im Kanal hat **keine Buttons** und aktualisiert sich automatisch."
    )
    await inter.response.send_message(txt, ephemeral=True)


# ---------------- Lifecycle ----------------
@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    # DM-basiertes RSVP registrieren (Slash-Commands + persistente DM-Views)
    await setup_rsvp_dm(client, tree)

    # Global sync + pro Guild Guild-Scope bereinigen (keine alten Kommandos)
    await tree.sync()
    for g in client.guilds:
        guild_obj = discord.Object(id=g.id)
        tree.clear_commands(guild=guild_obj)
        await tree.sync(guild=guild_obj)

    print("Slash-Commands synchronisiert. DM-RSVP ist bereit.")


# ---------------- Start ----------------
if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Setze DISCORD_BOT_TOKEN in der Umgebung (DISCORD_BOT_TOKEN).")
    # Empfehlung: als Modul starten, damit 'bot.' Imports sauber funktionieren:
    #   python -m bot.bot
    client.run(token)
