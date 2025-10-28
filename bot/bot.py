# bot/bot.py
from __future__ import annotations
import os
import asyncio
import logging

import discord
from discord import app_commands

# ---- Logging so we see why "Die Anwendung reagiert nicht" passiert ----
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("wf-bot")

# ---- Intents: Members sind nötig für on_member_join & Rollenvergabe ----
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # in Dev-Portal aktivieren!

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ---- Module laden (die Dateien hast du bereits) ------------------------
# event_rsvp_dm: /raid_create_dm + Auto-Resend
# onboarding_dm: Willkommens-DM + Review
from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member
from onboarding_dm import setup_onboarding, send_onboarding_dm

# ---- First-run Guard (damit setup & sync nur 1x laufen) ---------------
_ready_once = False

@client.event
async def on_ready():
    global _ready_once
    log.info(f"Logged in as {client.user} (id={client.user.id})")
    if _ready_once:
        return
    _ready_once = True

    # Commands registrieren
    try:
        await setup_onboarding(client, tree)
        await setup_rsvp_dm(client, tree)
    except Exception as e:
        log.exception("Fehler beim setup_*: %r", e)

    # Erstes Sync (global). Wenn du schneller testen willst, setze GUILD_ID.
    try:
        guild_id = os.getenv("GUILD_ID")
        if guild_id:
            gobj = discord.Object(id=int(guild_id))
            await tree.sync(guild=gobj)
            log.info("Slash-Commands GUILD-scope gesynct für %s", guild_id)
        else:
            await tree.sync()
            log.info("Slash-Commands GLOBAL gesynct")
    except Exception as e:
        log.exception("Fehler beim tree.sync(): %r", e)

# ---- Member Join: Onboarding-DM & Auto-Resend für Events --------------
@client.event
async def on_member_join(member: discord.Member):
    # Schicke die Onboarding-DM
    try:
        await send_onboarding_dm(member)
    except Exception:
        log.exception("send_onboarding_dm crashte")

    # RSVP-DMs für noch relevante Events erneut senden
    try:
        await auto_resend_for_new_member(member)
    except Exception:
        log.exception("auto_resend_for_new_member crashte")

# ---- Nützliche Admin-/Debug-Commands ----------------------------------

@tree.command(name="ping", description="Lebenszeichen des Bots (Debug).")
async def ping_cmd(inter: discord.Interaction):
    await inter.response.send_message("Pong 🏓", ephemeral=True)

@tree.command(name="wf_admin_sync_hard", description="(Admin) Slash-Commands neu synchronisieren.")
async def wf_admin_sync_hard(inter: discord.Interaction):
    perms = getattr(inter.user, "guild_permissions", None)
    if not perms or not (perms.administrator or perms.manage_guild):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
        return
    await inter.response.defer(ephemeral=True, thinking=True)
    try:
        # bevorzuge GUILD-Scoped Sync (sofort sichtbar)
        await tree.sync(guild=discord.Object(id=inter.guild_id))
        await inter.followup.send("✅ Slash-Commands (guild) neu gesynct.", ephemeral=True)
    except Exception as e:
        log.exception("wf_admin_sync_hard Fehler: %r", e)
        await inter.followup.send(f"❌ Sync-Fehler: `{e}`", ephemeral=True)

# ---- Globaler Fehlerhaken für app_commands ----------------------------

@tree.error
async def on_app_command_error(inter: discord.Interaction, error: app_commands.AppCommandError):
    # Das hilft enorm, um „Die Anwendung reagiert nicht“ zu beseitigen.
    log.exception("AppCmd Error bei %s: %r", getattr(inter.command, 'name', '?'), error)
    try:
        if inter.response.is_done():
            await inter.followup.send("❌ Unerwarteter Fehler. (Log checken)", ephemeral=True)
        else:
            await inter.response.send_message("❌ Unerwarteter Fehler. (Log checken)", ephemeral=True)
    except Exception:
        pass

# ---- Start -------------------------------------------------------------

def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN ist nicht gesetzt.")
    client.run(token)

if __name__ == "__main__":
    main()
