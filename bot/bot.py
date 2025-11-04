# bot.py — Minimal, stabil, nur das Nötige
from __future__ import annotations
import os
import threading
import time
import requests

import discord
from discord import app_commands

# ===== Keepalive (optional) =====
def keep_alive():
    url = os.getenv("KEEPALIVE_URL", "").strip()
    if not url:
        return
    while True:
        try:
            requests.get(url, timeout=10)
        except Exception:
            pass
        time.sleep(300)
threading.Thread(target=keep_alive, daemon=True).start()

# ===== Discord Setup =====
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")

intents = discord.Intents.default()
intents.guilds = True
intents.members = True           # WICHTIG: für on_member_join
intents.dm_messages = True
intents.message_content = False  # nicht nötig

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# RSVP-DM Modul
from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")
    # Slash-Commands global syncen
    await tree.sync()
    # RSVP/DM-Features registrieren
    await setup_rsvp_dm(client, tree)
    # Nachladen ist safe; nochmal syncen
    await tree.sync()
    print("Slash-Commands synchronisiert.")

@client.event
async def on_member_join(member: discord.Member):
    # Neue Mitglieder erhalten automatisch DM-Einladungen für laufende Raids
    await auto_resend_for_new_member(member)

# Zum Testen: Bot lebt?
@tree.command(name="ping", description="Lebenszeichen")
async def ping(inter: discord.Interaction):
    await inter.response.send_message("pong", ephemeral=True)

if __name__ == "__main__":
    client.run(TOKEN)
