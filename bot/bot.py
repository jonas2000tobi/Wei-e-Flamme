# bot/bot.py
from __future__ import annotations
import os
import discord
from discord import app_commands

# Robust import (Paket/flat)
try:
    from bot.event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member
    from bot.onboarding import setup_onboarding, handle_member_join_onboarding, handle_member_update_onboarding
except ModuleNotFoundError:
    from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member
    from onboarding import setup_onboarding, handle_member_join_onboarding, handle_member_update_onboarding

INTENTS = discord.Intents.default()
INTENTS.guilds = True
INTENTS.members = True          # wichtig: neue Member + Rollenänderungen
INTENTS.message_content = False

client = discord.Client(intents=INTENTS)
tree = app_commands.CommandTree(client)

# ---------- Admin: Hard Sync ----------
@tree.command(name="wf_admin_sync_hard", description="Hard-Sync: Guild-Scope leeren & global syncen (Admin)")
async def wf_admin_sync_hard(inter: discord.Interaction):
    perms = getattr(inter.user, "guild_permissions", None)
    if not perms or not (perms.administrator or perms.manage_guild):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    await inter.response.defer(ephemeral=True, thinking=True)
    try:
        await tree.sync()
        guild_obj = discord.Object(id=inter.guild_id)
        tree.clear_commands(guild=guild_obj)
        await tree.sync(guild=guild_obj)
        await inter.followup.send("✅ Hard-Sync: Guild-Scope geleert & global synchronisiert.", ephemeral=True)
    except Exception as e:
        await inter.followup.send(f"❌ Fehler: {e}", ephemeral=True)

# ---------- Kurzhilfe ----------
@tree.command(name="wf", description="Weiße Flamme – Hilfe")
async def wf(inter: discord.Interaction):
    txt = (
        "🔥 **DM-RSVP aktiv** (Einladungen per DM, Übersicht im Kanal ohne Buttons)\n"
        "• `/raid_dm_set_role` – Standard-Zielrolle speichern\n"
        "• `/raid_dm_create` – Raid erstellen (optional `target_role` + `channel`)\n"
        "• `/raid_dm_show` – Übersicht neu zeichnen\n"
        "• `/raid_dm_close` – Übersicht sperren (DMs bleiben nutzbar)\n"
        "• `/raid_dm_delete` – Übersicht + Eintrag löschen\n"
        "• `/raid_dm_resend` – Neue Rollenmitglieder nachträglich einladen\n\n"
        "🧭 **Onboarding** (DM an neue Mitglieder, Review in #gildenleitung)\n"
        "• `/onb_set_staff_channel` – Staff-/Gildenleitungskanal setzen\n"
        "• `/onb_set_roles` – Rollen zuweisen (WeißeFlamme/Allianz/Freund[/Newbie])\n"
        "• `/onb_test_dm` – Eigene Onboarding-DM testen\n"
    )
    await inter.response.send_message(txt, ephemeral=True)

# ---------- Lifecycle ----------
@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    # Module registrieren
    await setup_rsvp_dm(client, tree)
    await setup_onboarding(client, tree)

    # Global sync + Guild-Scope bereinigen
    await tree.sync()
    for g in client.guilds:
        guild_obj = discord.Object(id=g.id)
        tree.clear_commands(guild=guild_obj)
        await tree.sync(guild=guild_obj)

    print("Slash-Commands synchronisiert. DM-RSVP & Onboarding aktiv.")

# ---------- Member Hooks ----------
@client.event
async def on_member_join(member: discord.Member):
    if member.bot: return
    # Onboarding-DM anstoßen
    await handle_member_join_onboarding(member)
    # Auto-RSVP: nur aktive Events (bis +2h nach Start)
    await auto_resend_for_new_member(member)

@client.event
async def on_member_update(before: discord.Member, after: discord.Member):
    # Onboarding evtl. fortführen (falls nötig) – hier nur Rollenänderungen relevant
    await handle_member_update_onboarding(before, after)
    # Wenn Member Zielrolle bekommen hat: aktive Events nachsenden
    await auto_resend_for_new_member(after)

# ---------- Start ----------
if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Setze DISCORD_BOT_TOKEN in der Umgebung.")
    client.run(token)
