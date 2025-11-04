# bot/bot.py
from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List

import discord
from discord.ext import commands, tasks
from discord import app_commands

# -------- Intents ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True          # nötig für DMs/Rollen
intents.dm_messages = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree


# -------- Robuste Imports (Root- oder /bot-Start) ----------
def _import_modules():
    global setup_rsvp_dm, auto_resend_for_new_member
    try:
        from bot.event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member  # type: ignore
    except ModuleNotFoundError:
        from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member      # type: ignore

    # Onboarding und Join-Hook bleiben deine Originaldateien:
    global setup_onboarding, send_onboarding_dm
    try:
        from bot.onboarding import setup_onboarding, send_onboarding_dm  # type: ignore
    except ModuleNotFoundError:
        from onboarding import setup_onboarding, send_onboarding_dm      # type: ignore

    global register_join_hook
    try:
        from bot.join_hook import register_join_hook  # type: ignore
    except ModuleNotFoundError:
        from join_hook import register_join_hook      # type: ignore


# -------- Token ----------
def _get_token() -> str | None:
    for key in ("DISCORD_TOKEN", "DISCORD_BOT_TOKEN", "TOKEN"):
        val = os.getenv(key)
        if val and val.strip():
            print(f"✅ Token aus {key}")
            return val.strip()
    print("❌ Kein Token gefunden (DISCORD_TOKEN / DISCORD_BOT_TOKEN / TOKEN).")
    return None


# -------- Ready ----------
@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")

    try:
        _import_modules()
        await setup_rsvp_dm(bot, tree)          # RSVP/DM Commands
        await setup_onboarding(bot, tree)       # deine Onboarding-Slash-Commands
        register_join_hook(bot, send_onboarding_dm, auto_resend_for_new_member)
        print("✅ Module geladen (RSVP-DM, Onboarding, Join-Hook).")
    except Exception as e:
        print(f"⚠️ Modul-Setup Fehler: {e}")

    # Slash-Commands syncen
    try:
        synced = await tree.sync()
        print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
    except Exception as e:
        print(f"⚠️ Sync-Fehler: {e}")

    # Cleanup-Task starten
    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")


# -------- AppCommand-Fehler global ----------
@tree.error
async def on_app_command_error(inter: discord.Interaction, error: app_commands.AppCommandError):
    try:
        if not inter.response.is_done():
            await inter.response.send_message(f"❌ {error}", ephemeral=True)
        else:
            await inter.followup.send(f"❌ {error}", ephemeral=True)
    except Exception:
        pass
    print(f"[AppCmdError] {getattr(inter.command, 'name', '?')}: {error!r}")


# -------- Cleanup: löscht Server-Übersichts-Posts > 2h nach Start ----------
@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        try:
            from bot.event_rsvp_dm import store, save_store, TZ  # type: ignore
        except ModuleNotFoundError:
            from event_rsvp_dm import store, save_store, TZ      # type: ignore

        now = datetime.now(TZ)
        remove: List[str] = []
        for msg_id, obj in list(store.items()):
            try:
                when = datetime.fromisoformat(obj.get("when_iso"))
            except Exception:
                remove.append(msg_id)
                continue

            if now > when + timedelta(hours=2):
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
                    remove.append(msg_id)

        if remove:
            for mid in remove:
                store.pop(mid, None)
            save_store()
            print(f"🧹 Alte Events entfernt: {len(remove)}")
    except Exception as e:
        print(f"[cleanup_expired_events] Fehler: {e}")


# -------- Minimaler Test ----------
@tree.command(name="ping", description="Lebenszeichen.")
async def ping(inter: discord.Interaction):
    await inter.response.send_message("🏓 Pong!", ephemeral=True)


# -------- Main ----------
def main():
    print("🚀 Starte Bot ...")
    token = _get_token()
    if not token:
        return
    bot.run(token)


if __name__ == "__main__":
    main()
