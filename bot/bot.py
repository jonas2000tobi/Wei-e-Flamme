# bot/bot.py
# -----------------------------------------------------------
# Hauptdatei: verbindet alle Module (RSVP + Onboarding)
# -----------------------------------------------------------

import discord
from discord.ext import commands
from discord import app_commands

from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member
from onboarding_dm import send_onboarding_dm, setup_onboarding

import asyncio
import logging

# -----------------------------------------------------------
# Grundkonfiguration
# -----------------------------------------------------------

logging.basicConfig(level=logging.INFO)

# Intents: Mitglieder, Nachrichten, DMs usw.
intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.messages = True
intents.message_content = False  # nur für DMs nicht nötig
intents.dm_messages = True

client = commands.Bot(command_prefix="!", intents=intents)
tree = client.tree

# -----------------------------------------------------------
# Startup
# -----------------------------------------------------------

@client.event
async def on_ready():
    print(f"✅ Eingeloggt als {client.user} (ID: {client.user.id})")
    try:
        await setup_rsvp_dm(client, tree)
        await setup_onboarding(tree)
        await tree.sync()
        print("🌐 Slash-Commands synchronisiert")
    except Exception as e:
        print(f"Fehler bei Setup/Sync: {e}")
    print("Bot ist bereit.")

# -----------------------------------------------------------
# Neue Mitglieder
# -----------------------------------------------------------

@client.event
async def on_member_join(member: discord.Member):
    """Wird aufgerufen, wenn ein neues Mitglied beitritt."""
    try:
        # DM mit Rollenmenü
        await send_onboarding_dm(member)

        # Falls aktive Raids existieren → DMs nachsenden
        await asyncio.sleep(3)  # kleine Verzögerung für bessere Stabilität
        await auto_resend_for_new_member(member)
    except Exception as e:
        print(f"Fehler on_member_join: {e}")

# -----------------------------------------------------------
# Globaler Error-Handler
# -----------------------------------------------------------

@client.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("❌ Unbekannter Befehl.")
    else:
        await ctx.send(f"❌ Fehler: {error}")
        raise error

# -----------------------------------------------------------
# Bot starten
# -----------------------------------------------------------

if __name__ == "__main__":
    import os
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        print("❌ Kein Token in DISCORD_TOKEN gefunden!")
    else:
        client.run(TOKEN)
