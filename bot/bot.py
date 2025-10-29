# /bot/bot.py
# Einstiegsdatei: schlank, lädt Module, synced Commands, Cleanup-Task.

from __future__ import annotations
import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks

# Module
from bot.event_rsvp_dm import store as RSVP_STORE, save_store as save_rsvp_store, TZ as RSVP_TZ
from bot.event_rsvp_dm import setup_rsvp_dm
from bot.onboarding import setup_onboarding
from bot.join_hook import register_join_hook

# --------------------------- Grundsetup ---------------------------
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

TZ = RSVP_TZ  # Einheitlich Europe/Berlin

# --------------------------- Events ---------------------------
@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")
    try:
        await tree.sync()
        print("✅ Slash-Commands synchronisiert.")
    except Exception as e:
        print(f"⚠️ Slash-Command Sync Fehler: {e}")

    # Module initialisieren
    try:
        await setup_rsvp_dm(bot, tree)
        await setup_onboarding(bot, tree)
        register_join_hook(bot)  # on_member_join-Hook (Onboarding + Auto-Resend)
        print("✅ Module bereit.")
    except Exception as e:
        print(f"⚠️ Modul-Setup Fehler: {e}")

    # Cleanup-Task starten
    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")

# --------------------------- Cleanup Task ---------------------------
@tasks.loop(minutes=5)
async def cleanup_expired_events():
    """Entfernt Event-Posts 2h nach Start und säubert den Store."""
    try:
        now = datetime.now(TZ)
        remove_ids = []
        for msg_id, obj in list(RSVP_STORE.items()):
            try:
                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    guild = bot.get_guild(int(obj["guild_id"]))
                    if guild:
                        ch = guild.get_channel(int(obj["channel_id"]))
                        if isinstance(ch, (discord.TextChannel, discord.Thread)):
                            try:
                                msg = await ch.fetch_message(int(msg_id))
                                await msg.delete()
                            except Exception:
                                pass
                    remove_ids.append(msg_id)
            except Exception:
                continue

        for mid in remove_ids:
            RSVP_STORE.pop(mid, None)
        if remove_ids:
            save_rsvp_store()
            print(f"🧹 Alte Events entfernt: {len(remove_ids)}")
    except Exception as e:
        print(f"[cleanup_expired_events] Fehler: {e}")

# --------------------------- Start ---------------------------
def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("❌ Kein Token gefunden! Bitte Umgebungsvariable DISCORD_TOKEN setzen.")
        return
    bot.run(token)

if __name__ == "__main__":
    main()
