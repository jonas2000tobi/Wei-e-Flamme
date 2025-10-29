# /bot/bot.py
from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List

import discord
from discord.ext import commands, tasks
from discord import app_commands

# --------- Intents ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True       # für Rollen/DMs notwendig
intents.dm_messages = True
intents.message_content = False  # brauchst du nicht für Slash-Commands

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# --------- Robuste Modul-Imports (Root- oder /bot-Start möglich) ----------
def _import_modules():
    global setup_rsvp_dm, setup_onboarding, register_join_hook
    try:
        from bot.event_rsvp_dm import setup_rsvp_dm  # type: ignore
    except ModuleNotFoundError:
        from event_rsvp_dm import setup_rsvp_dm      # type: ignore

    try:
        from bot.onboarding import setup_onboarding  # type: ignore
    except ModuleNotFoundError:
        from onboarding import setup_onboarding      # type: ignore

    try:
        from bot.join_hook import register_join_hook # type: ignore
    except ModuleNotFoundError:
        from join_hook import register_join_hook     # type: ignore

# --------- Token-Handling ----------
def _get_token() -> str | None:
    for key in ["DISCORD_TOKEN", "DISCORD_BOT_TOKEN", "TOKEN"]:
        val = os.getenv(key)
        if val and val.strip():
            print(f"✅ Token gefunden unter Variable: {key}")
            return val.strip()
    print("❌ Kein gültiges Token in Environment-Variablen gefunden!")
    return None

# --------- Optional: Cogs laden (wenn du später welche in /bot/cogs/ ablegst) ----------
async def _load_cogs_if_any():
    cogs_dir = os.path.join(os.path.dirname(__file__), "cogs")
    if not os.path.isdir(cogs_dir):
        return
    for filename in os.listdir(cogs_dir):
        if filename.endswith(".py") and not filename.startswith("__"):
            ext_name = f"bot.cogs.{filename[:-3]}"
            try:
                await bot.load_extension(ext_name)
                print(f"🧩 Modul geladen: {ext_name}")
            except Exception as e:
                print(f"❌ Fehler beim Laden von {ext_name}: {e}")

# --------- Events ----------
@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")

    # 1) Module importieren und registrieren (RSVP-DM, Onboarding, Join-Hook)
    try:
        _import_modules()
        await setup_rsvp_dm(bot, tree)
        await setup_onboarding(bot, tree)
        register_join_hook(bot)  # registriert on_member_join Hook
        print("✅ Module (RSVP-DM, Onboarding, Join-Hook) geladen.")
    except Exception as e:
        print(f"⚠️ Modul-Setup Fehler: {e}")

    # 2) Optionale Cogs (wenn vorhanden)
    print("✅ Lade optionale Cogs (falls vorhanden) ...")
    await _load_cogs_if_any()

    # 3) Danach syncen
    try:
        synced = await tree.sync()
        print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
    except Exception as e:
        print(f"⚠️ Fehler beim Synchronisieren der Slash-Commands: {e}")

    # 4) Cleanup-Task starten
    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")

# Globaler Fehler-Logger für App-Commands
@tree.error
async def on_app_command_error(inter: discord.Interaction, error: app_commands.AppCommandError):
    try:
        if not inter.response.is_done():
            await inter.response.send_message(f"❌ Command-Fehler: {error}", ephemeral=True)
        else:
            await inter.followup.send(f"❌ Command-Fehler: {error}", ephemeral=True)
    except Exception:
        pass
    print(f"[AppCmdError] {getattr(inter.command, 'name', '?')}: {error!r}")

# --------- Cleanup Task (nutzt lazy import vom Store) ----------
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

# --------- Minimaler Test-Command ----------
@tree.command(name="ping", description="Testet, ob der Bot reagiert.")
async def ping(inter: discord.Interaction):
    await inter.response.send_message("🏓 Pong! Bot läuft einwandfrei.", ephemeral=True)

# --------- Main ----------
def main():
    print("🚀 Starte Weiße Flamme Discord-Bot ...")
    debug_keys = [k for k in os.environ.keys() if "DISCORD" in k or "TOKEN" in k]
    print(f"ENV DEBUG – gefundene Keys: {debug_keys}")

    token = _get_token()
    if not token:
        print("❌ Kein Token gefunden! Bitte setze DISCORD_TOKEN (oder DISCORD_BOT_TOKEN/TOKEN).")
        return
    bot.run(token)

if __name__ == "__main__":
    main()
