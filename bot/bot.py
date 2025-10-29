# bot/bot.py
# ------------------------------------------------------
# Hauptbot mit Onboarding-DM + Raid/Event RSVP-System
# ------------------------------------------------------

import discord
from discord.ext import tasks, commands
from discord import app_commands
import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- robustes Import-Setup ---
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
if str(HERE.parent) not in sys.path:
    sys.path.insert(0, str(HERE.parent))

# Event- & Onboarding-Module sicher importieren
try:
    from bot.event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member, store, save_store
except ModuleNotFoundError:
    from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member, store, save_store

try:
    from bot.onboarding_dm import setup_onboarding, send_onboarding_dm
except ModuleNotFoundError:
    from onboarding_dm import setup_onboarding, send_onboarding_dm


# ------------------------------------------------------
# Grundkonfiguration
# ------------------------------------------------------

intents = discord.Intents.all()
client = commands.Bot(command_prefix="!", intents=intents)
tree = client.tree


# ------------------------------------------------------
# On Ready – Botstart
# ------------------------------------------------------

@client.event
async def on_ready():
    print(f"✅ Eingeloggt als {client.user}")
    try:
        await tree.sync()
        print("✅ Slash-Commands synchronisiert.")
    except Exception as e:
        print(f"⚠️ Slash-Command Sync Fehler: {e}")
    cleanup_expired_events.start()


# ------------------------------------------------------
# Member Join – schickt Onboarding + Event-DMs
# ------------------------------------------------------

@client.event
async def on_member_join(member: discord.Member):
    try:
        await send_onboarding_dm(member)
        await auto_resend_for_new_member(member)
    except Exception as e:
        print(f"[on_member_join] Fehler: {e}")


# ------------------------------------------------------
# Hintergrund-Task: Löscht alte Events 2h nach Start
# ------------------------------------------------------

@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        now = datetime.now(timezone.utc)
        remove_ids = []
        for msg_id, obj in list(store.items()):
            try:
                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    guild = client.get_guild(int(obj["guild_id"]))
                    if guild:
                        ch = guild.get_channel(int(obj["channel_id"]))
                        if ch:
                            try:
                                msg = await ch.fetch_message(int(msg_id))
                                await msg.delete()
                            except Exception:
                                pass
                    remove_ids.append(msg_id)
            except Exception:
                continue
        # alte Events löschen
        for mid in remove_ids:
            store.pop(mid, None)
        if remove_ids:
            save_store()
            print(f"🧹 Alte Events entfernt: {len(remove_ids)}")
    except Exception as e:
        print(f"[cleanup_expired_events] Fehler: {e}")


# ------------------------------------------------------
# Setup aller Slash Commands
# ------------------------------------------------------

async def setup_bot():
    await setup_rsvp_dm(client, tree)
    await setup_onboarding(client, tree)
    print("✅ Setup abgeschlossen.")


@client.event
async def on_connect():
    client.loop.create_task(setup_bot())


# ------------------------------------------------------
# Botstart
# ------------------------------------------------------

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    print("❌ Kein Token gefunden! Bitte Umgebungsvariable DISCORD_TOKEN setzen.")
else:
    client.run(TOKEN)
