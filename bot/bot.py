# /bot/bot.py
from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List

import discord
from discord.ext import commands, tasks

# Lokale Module (ohne Paketpräfix!)
from event_rsvp_dm import setup_rsvp_dm, store, TZ  # store & TZ wiederverwenden
from onboarding import setup_onboarding
from join_hook import register_join_hook

INTENTS = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=INTENTS)
tree = bot.tree


@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")

    try:
        synced = await tree.sync()
        print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
    except Exception as e:
        print(f"⚠️ Slash-Command Sync Fehler: {e}")

    try:
        await setup_rsvp_dm(bot, tree)
        await setup_onboarding(bot, tree)
        print("✅ Module geladen.")
    except Exception as e:
        print(f"⚠️ Modul-Setup Fehler: {e}")

    try:
        register_join_hook(bot)
        print("✅ Join-Hook registriert.")
    except Exception as e:
        print(f"⚠️ Join-Hook Fehler: {e}")

    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")


@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        now = datetime.now(TZ)
        to_remove: List[str] = []

        for msg_id, obj in list(store.items()):
            try:
                when = datetime.fromisoformat(obj.get("when_iso"))
            except Exception:
                to_remove.append(msg_id)
                continue

            if now <= when + timedelta(hours=2):
                continue

            try:
                guild = bot.get_guild(int(obj["guild_id"]))
                if guild:
                    ch = guild.get_channel(int(obj["channel_id"]))
                    if isinstance(ch, (discord.TextChannel, discord.Thread)):
                        try:
                            msg = await ch.fetch_message(int(msg_id))
                            await msg.delete()
                        except Exception:
                            pass
            finally:
                to_remove.append(msg_id)

        if to_remove:
            from event_rsvp_dm import save_store  # lazy import
            for mid in to_remove:
                store.pop(mid, None)
            save_store()
            print(f"🧹 Alte Events entfernt: {len(to_remove)}")

    except Exception as e:
        print(f"[cleanup_expired_events] Fehler: {e}")


def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("❌ Kein Token gefunden! Umgebungsvariable DISCORD_TOKEN setzen.")
        return
    bot.run(token)


if __name__ == "__main__":
    main()
