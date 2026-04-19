from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands

# -------- Intents ----------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.dm_messages = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# -------- Platzhalter / Imports ----------
setup_rsvp_dm = None
auto_resend_for_new_member = None
setup_onboarding = None
send_onboarding_dm = None
register_join_hook = None
set_dm_pref = None
is_dm_enabled = None
get_user_stats = None
get_top_yes_stats = None
setup_leader_contact = None
store = {}


def _import_modules():
    global setup_rsvp_dm, auto_resend_for_new_member, store
    try:
        from bot.event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member, store  # type: ignore
        print("✅ Import: bot.event_rsvp_dm")
    except ModuleNotFoundError:
        from event_rsvp_dm import setup_rsvp_dm, auto_resend_for_new_member, store  # type: ignore
        print("✅ Import: event_rsvp_dm (root)")

    global setup_onboarding, send_onboarding_dm
    try:
        from bot.onboarding_dm import setup_onboarding, send_onboarding_dm  # type: ignore
        print("✅ Import: bot.onboarding_dm")
    except ModuleNotFoundError:
        try:
            from onboarding_dm import setup_onboarding, send_onboarding_dm  # type: ignore
            print("✅ Import: onboarding_dm (root)")
        except ModuleNotFoundError:
            try:
                from bot.onboarding import setup_onboarding, send_onboarding_dm  # type: ignore
                print("⚠️ Fallback-Import: bot.onboarding")
            except ModuleNotFoundError:
                from onboarding import setup_onboarding, send_onboarding_dm  # type: ignore
                print("⚠️ Fallback-Import: onboarding (root)")

    global register_join_hook
    try:
        from bot.join_hook import register_join_hook  # type: ignore
        print("✅ Import: bot.join_hook")
    except ModuleNotFoundError:
        from join_hook import register_join_hook  # type: ignore
        print("✅ Import: join_hook (root)")

    global set_dm_pref, is_dm_enabled
    try:
        from bot.event_dm_prefs import set_dm_pref, is_dm_enabled  # type: ignore
        print("✅ Import: bot.event_dm_prefs")
    except ModuleNotFoundError:
        from event_dm_prefs import set_dm_pref, is_dm_enabled  # type: ignore
        print("✅ Import: event_dm_prefs (root)")

    global get_user_stats, get_top_yes_stats
    try:
        from bot.raid_stats import get_user_stats, get_top_yes_stats  # type: ignore
        print("✅ Import: bot.raid_stats")
    except ModuleNotFoundError:
        from raid_stats import get_user_stats, get_top_yes_stats  # type: ignore
        print("✅ Import: raid_stats (root)")

    global setup_leader_contact
    try:
        from bot.leader_contact import setup_leader_contact  # type: ignore
        print("✅ Import: bot.leader_contact")
    except ModuleNotFoundError:
        from leader_contact import setup_leader_contact  # type: ignore
        print("✅ Import: leader_contact (root)")


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
        await setup_rsvp_dm(bot, tree)
        await setup_onboarding(bot, tree)
        await setup_leader_contact(bot, tree)
        register_join_hook(bot, send_onboarding_dm, auto_resend_for_new_member)
        print("✅ Module geladen (RSVP-DM, Onboarding, Join-Hook, DM-Prefs, Stats, Leader-Contact).")
    except Exception as e:
        print(f"⚠️ Modul-Setup Fehler: {e}")

    try:
        synced = await tree.sync()
        print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
    except Exception as e:
        print(f"⚠️ Sync-Fehler: {e}")

    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")


# -------- Globaler AppCommand-Error ----------
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


# -------- Cleanup ----------
@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        try:
            from bot.event_rsvp_dm import store, save_store, TZ, delete_pending_dm_messages_for_started_events  # type: ignore
        except ModuleNotFoundError:
            from event_rsvp_dm import store, save_store, TZ, delete_pending_dm_messages_for_started_events  # type: ignore

        now = datetime.now(TZ)
        remove: List[str] = []

        # 1) Sobald Event begonnen hat -> offene DMs von Nicht-Abstimmern löschen
        try:
            changed = await delete_pending_dm_messages_for_started_events(bot)
            if changed:
                save_store()
                print(f"🧹 Offene Event-DMs entfernt: {changed}")
        except Exception as e:
            print(f"[cleanup_expired_events] delete_pending_dm_messages_for_started_events Fehler: {e}")

        # 2) 2h nach Eventstart -> Server-Post + Store entfernen
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


# -------- Basis ----------
@tree.command(name="ping", description="Lebenszeichen.")
async def ping(inter: discord.Interaction):
    await inter.response.send_message("🏓 Pong!", ephemeral=True)


# -------- DM Opt-Out ----------
@tree.command(name="raid_dm", description="Eigene Raid-DM Einstellungen")
@app_commands.describe(mode="on / off / status")
async def raid_dm(inter: discord.Interaction, mode: str):
    if inter.guild_id is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if set_dm_pref is None or is_dm_enabled is None:
        await inter.response.send_message("❌ DM-System nicht geladen.", ephemeral=True)
        return

    m = (mode or "").strip().lower()

    if m == "on":
        set_dm_pref(inter.guild_id, inter.user.id, True)
        await inter.response.send_message("✅ Raid-/Event-DMs aktiviert.", ephemeral=True)
        return

    if m == "off":
        set_dm_pref(inter.guild_id, inter.user.id, False)
        await inter.response.send_message(
            "✅ Raid-/Event-DMs deaktiviert. Du kannst trotzdem direkt unter der Raid-Ankündigung per Button abstimmen.",
            ephemeral=True
        )
        return

    if m == "status":
        enabled = is_dm_enabled(inter.guild_id, inter.user.id)
        await inter.response.send_message(
            f"📬 Raid-/Event-DMs sind aktuell: **{'AN' if enabled else 'AUS'}**",
            ephemeral=True
        )
        return

    await inter.response.send_message("Nutze: `on`, `off` oder `status`.", ephemeral=True)


# -------- Kalender ----------
@tree.command(name="raid_calendar", description="Zeigt kommende Raid-/Event-Termine")
async def raid_calendar(inter: discord.Interaction):
    if inter.guild_id is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if not isinstance(store, dict):
        await inter.response.send_message("❌ Event-Store nicht geladen.", ephemeral=True)
        return

    events: List[Tuple[datetime, str]] = []
    for obj in store.values():
        try:
            if int(obj.get("guild_id", 0) or 0) != inter.guild_id:
                continue
            when = datetime.fromisoformat(obj["when_iso"])
            if when < datetime.now(when.tzinfo):
                continue
            title = str(obj.get("title", "Event"))
            events.append((when, title))
        except Exception:
            continue

    events.sort(key=lambda x: x[0])

    if not events:
        await inter.response.send_message("📅 Keine kommenden Events gefunden.", ephemeral=True)
        return

    lines = []
    for when, title in events[:10]:
        lines.append(f"• {when.strftime('%d.%m.%Y %H:%M')} — {title}")

    emb = discord.Embed(
        title="📅 Kommende Events",
        description="\n".join(lines),
        color=discord.Color.blurple()
    )
    await inter.response.send_message(embed=emb, ephemeral=True)


# -------- Stats ----------
@tree.command(name="raid_stats", description="Zeigt deine Raid-Statistik")
async def raid_stats_cmd(inter: discord.Interaction):
    if inter.guild_id is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if get_user_stats is None:
        await inter.response.send_message("❌ Statistiksystem nicht geladen.", ephemeral=True)
        return

    s = get_user_stats(inter.guild_id, inter.user.id)
    if not s:
        await inter.response.send_message("📊 Noch keine Statistik vorhanden.", ephemeral=True)
        return

    total = (
        int(s.get("yes", 0))
        + int(s.get("bank", 0))
        + int(s.get("maybe", 0))
        + int(s.get("no", 0))
    )

    emb = discord.Embed(
        title=f"📊 Raid-Statistik: {inter.user.display_name}",
        color=discord.Color.orange()
    )
    emb.add_field(name="✅ Zusagen", value=str(s.get("yes", 0)), inline=True)
    emb.add_field(name="🏦 Bank", value=str(s.get("bank", 0)), inline=True)
    emb.add_field(name="❔ Vielleicht", value=str(s.get("maybe", 0)), inline=True)
    emb.add_field(name="❌ Abmeldungen", value=str(s.get("no", 0)), inline=True)
    emb.add_field(name="📦 Gesamt", value=str(total), inline=False)

    await inter.response.send_message(embed=emb, ephemeral=True)


@tree.command(name="raid_stats_top", description="Top 10 nach Zusagen")
async def raid_stats_top_cmd(inter: discord.Interaction):
    if inter.guild_id is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if get_top_yes_stats is None:
        await inter.response.send_message("❌ Statistiksystem nicht geladen.", ephemeral=True)
        return

    ranking = get_top_yes_stats(inter.guild_id, limit=10)
    if not ranking:
        await inter.response.send_message("📊 Noch keine Statistik vorhanden.", ephemeral=True)
        return

    lines = []
    for i, (uid, yes_count) in enumerate(ranking, start=1):
        lines.append(f"{i}. <@{uid}> — {yes_count}")

    emb = discord.Embed(
        title="🏆 Top Raid-Zusagen",
        description="\n".join(lines),
        color=discord.Color.gold()
    )
    await inter.response.send_message(embed=emb)


# -------- Main ----------
def main():
    print("🚀 Starte Bot ...")
    token = _get_token()
    if not token:
        return
    bot.run(token)


if __name__ == "__main__":
    main()
