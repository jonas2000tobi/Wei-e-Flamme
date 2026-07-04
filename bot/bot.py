from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple

# Railway/Script-Start-Hotfix:
# Wenn der Bot als `python bot/bot.py` gestartet wird, liegt /app/bot vor /app im Importpfad.
# Dann wird `bot.py` als Modul `bot` gefunden und `from bot.xyz` bricht mit
# "bot is not a package". Deshalb immer den Projekt-Root zuerst in sys.path setzen.
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import discord
from discord.ext import commands, tasks
from discord import app_commands

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.dm_messages = True
intents.message_content = False
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

setup_rsvp_dm = None
auto_resend_for_new_member = None
setup_onboarding = None
send_onboarding_dm = None
register_join_hook = None
set_dm_pref = None
is_dm_enabled = None
get_user_stats = None
get_top_yes_stats = None
get_non_response_stats = None
setup_leader_contact = None
setup_raid_templates = None
setup_weekly_report = None
setup_member_portal = None
setup_loot_needs = None
setup_alliance_config = None
setup_dkp_system = None
setup_loot_auction = None
setup_voice_creator = None
setup_audit_system = None
setup_voice_attendance = None
setup_dashboard_data = None
store = {}
_modules_initialized = False


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

    global get_user_stats, get_top_yes_stats, get_non_response_stats

    try:
        from bot.raid_stats import get_user_stats, get_top_yes_stats, get_non_response_stats  # type: ignore
        print("✅ Import: bot.raid_stats")
    except ModuleNotFoundError:
        from raid_stats import get_user_stats, get_top_yes_stats, get_non_response_stats  # type: ignore
        print("✅ Import: raid_stats (root)")

    global setup_leader_contact

    try:
        from bot.leader_contact import setup_leader_contact  # type: ignore
        print("✅ Import: bot.leader_contact")
    except ModuleNotFoundError:
        from leader_contact import setup_leader_contact  # type: ignore
        print("✅ Import: leader_contact (root)")

    global setup_raid_templates

    try:
        from bot.raid_templates import setup_raid_templates  # type: ignore
        print("✅ Import: bot.raid_templates")
    except ModuleNotFoundError:
        from raid_templates import setup_raid_templates  # type: ignore
        print("✅ Import: raid_templates (root)")

    global setup_weekly_report

    try:
        from bot.weekly_report import setup_weekly_report  # type: ignore
        print("✅ Import: bot.weekly_report")
    except ModuleNotFoundError:
        from weekly_report import setup_weekly_report  # type: ignore
        print("✅ Import: weekly_report (root)")

    global setup_member_portal

    try:
        from bot.member_portal import setup_member_portal  # type: ignore
        print("✅ Import: bot.member_portal")
    except ModuleNotFoundError:
        from member_portal import setup_member_portal  # type: ignore
        print("✅ Import: member_portal (root)")

    global setup_loot_needs

    try:
        from bot.loot_needs import setup_loot_needs  # type: ignore
        print("✅ Import: bot.loot_needs")
    except ModuleNotFoundError:
        from loot_needs import setup_loot_needs  # type: ignore
        print("✅ Import: loot_needs (root)")

    global setup_alliance_config

    try:
        from bot.alliance_config import setup_alliance_config  # type: ignore
        print("✅ Import: bot.alliance_config")
    except ModuleNotFoundError:
        from alliance_config import setup_alliance_config  # type: ignore
        print("✅ Import: alliance_config (root)")

    global setup_dkp_system

    try:
        from bot.dkp_system import setup_dkp_system  # type: ignore
        print("✅ Import: bot.dkp_system")
    except ModuleNotFoundError:
        from dkp_system import setup_dkp_system  # type: ignore
        print("✅ Import: dkp_system (root)")

    global setup_loot_auction

    try:
        from bot.loot_auction import setup_loot_auction  # type: ignore
        print("✅ Import: bot.loot_auction")
    except ModuleNotFoundError:
        from loot_auction import setup_loot_auction  # type: ignore
        print("✅ Import: loot_auction (root)")

    global setup_voice_creator

    try:
        from bot.voice_creator import setup_voice_creator  # type: ignore
        print("✅ Import: bot.voice_creator")
    except ModuleNotFoundError:
        try:
            from voice_creator import setup_voice_creator  # type: ignore
            print("✅ Import: voice_creator (root)")
        except ModuleNotFoundError:
            setup_voice_creator = None
            print("⚠️ Voice-Creator nicht gefunden")

    global setup_audit_system

    try:
        from bot.audit_system import setup_audit_system  # type: ignore
        print("✅ Import: bot.audit_system")
    except ModuleNotFoundError:
        from audit_system import setup_audit_system  # type: ignore
        print("✅ Import: audit_system (root)")

    global setup_voice_attendance

    try:
        from bot.voice_attendance import setup_voice_attendance  # type: ignore
        print("✅ Import: bot.voice_attendance")
    except ModuleNotFoundError:
        try:
            from voice_attendance import setup_voice_attendance  # type: ignore
            print("✅ Import: voice_attendance (root)")
        except Exception as e:
            setup_voice_attendance = None
            print(f"❌ Voice-Attendance Import deaktiviert: {e!r}")
    except Exception as e:
        # Wichtig: ein kaputter optionaler Runtime-/Voice-Import darf nicht verhindern,
        # dass andere Module wie Dashboard, DKP, Loot und Slash-Command-Sync starten.
        setup_voice_attendance = None
        print(f"❌ Voice-Attendance Import deaktiviert: {e!r}")

    global setup_dashboard_data

    try:
        from bot.dashboard_data import setup_dashboard_data  # type: ignore
        print("✅ Import: bot.dashboard_data")
    except ModuleNotFoundError:
        try:
            from dashboard_data import setup_dashboard_data  # type: ignore
            print("✅ Import: dashboard_data (root)")
        except Exception as e:
            setup_dashboard_data = None
            print(f"❌ Dashboard-Datenlayer Import deaktiviert: {e!r}")
    except Exception as e:
        setup_dashboard_data = None
        print(f"❌ Dashboard-Datenlayer Import deaktiviert: {e!r}")


def _get_token() -> str | None:
    for key in ("DISCORD_TOKEN", "DISCORD_BOT_TOKEN", "TOKEN"):
        val = os.getenv(key)

        if val and val.strip():
            print(f"✅ Token aus {key}")
            return val.strip()

    print("❌ Kein Token gefunden (DISCORD_TOKEN / DISCORD_BOT_TOKEN / TOKEN).")
    return None


@bot.event
async def on_ready():
    global _modules_initialized
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")

    # on_ready can fire again after a gateway reconnect. Registering commands,
    # listeners and persistent views more than once causes duplicate handlers.
    if not _modules_initialized:
        _import_modules()

        async def _safe_setup(name: str, setup_func):
            if setup_func is None:
                print(f"⚠️ Setup übersprungen: {name} ist nicht geladen")
                return False
            try:
                await setup_func(bot, tree)
                print(f"✅ Setup: {name}")
                return True
            except Exception as e:
                print(f"❌ Setup-Fehler {name}: {e!r}")
                return False

        # Alliance/Home-Server first, then the portal and its child modules.
        setup_steps = [
            ("audit_system", setup_audit_system),
            ("voice_attendance", setup_voice_attendance),
            ("dashboard_data", setup_dashboard_data),
            ("alliance_config", setup_alliance_config),
            ("event_rsvp_dm", setup_rsvp_dm),
            ("onboarding", setup_onboarding),
            ("leader_contact", setup_leader_contact),
            ("raid_templates", setup_raid_templates),
            ("weekly_report", setup_weekly_report),
            ("dkp_system", setup_dkp_system),
            ("loot_needs", setup_loot_needs),
            ("loot_auction", setup_loot_auction),
            ("voice_creator", setup_voice_creator),
            ("member_portal", setup_member_portal),
        ]
        results = []
        for module_name, setup_func in setup_steps:
            results.append(await _safe_setup(module_name, setup_func))

        try:
            register_join_hook(bot, send_onboarding_dm, auto_resend_for_new_member)
            print("✅ Join-Hook registriert.")
        except Exception as e:
            print(f"❌ Join-Hook Setup-Fehler: {e!r}")

        try:
            synced = await tree.sync()
            print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
        except Exception as e:
            print(f"⚠️ Sync-Fehler: {e!r}")

        _modules_initialized = True
        print(f"✅ Module einmalig initialisiert: {sum(results)}/{len(results)}")
    else:
        print("ℹ️ Gateway-Reconnect: Module werden nicht doppelt registriert.")

    if not cleanup_expired_events.is_running():
        cleanup_expired_events.start()
        print("🧹 Cleanup-Task gestartet.")


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


@tasks.loop(minutes=5)
async def cleanup_expired_events():
    try:
        try:
            from bot.event_rsvp_dm import (  # type: ignore
                store,
                save_store,
                TZ,
                delete_pending_dm_messages_for_started_events,
                _is_alliance_event,
                _init_event_shape,
            )
        except ModuleNotFoundError:
            from event_rsvp_dm import (  # type: ignore
                store,
                save_store,
                TZ,
                delete_pending_dm_messages_for_started_events,
                _is_alliance_event,
                _init_event_shape,
            )

        now = datetime.now(TZ)
        remove: List[str] = []

        try:
            changed = await delete_pending_dm_messages_for_started_events(bot)

            if changed:
                save_store()
                print(f"🧹 Offene Event-DMs entfernt: {changed}")

        except Exception as e:
            print(f"[cleanup_expired_events] delete_pending_dm_messages_for_started_events Fehler: {e}")

        for msg_id, obj in list(store.items()):
            try:
                _init_event_shape(obj)
                when = datetime.fromisoformat(obj.get("when_iso"))
            except Exception:
                remove.append(msg_id)
                continue

            # Discord-Posts bleiben bis 2 Stunden nach Eventstart bestehen.
            if now <= when + timedelta(hours=2):
                continue

            try:
                if _is_alliance_event(obj) and obj.get("mirrors"):
                    for mirror in list(obj.get("mirrors") or []):
                        try:
                            guild = bot.get_guild(int(mirror.get("guild_id", 0) or 0))

                            if not guild:
                                continue

                            ch = guild.get_channel(int(mirror.get("channel_id", 0) or 0))

                            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                                try:
                                    msg = await ch.fetch_message(int(mirror.get("message_id", 0) or 0))
                                    await msg.delete()
                                except Exception:
                                    pass

                        except Exception:
                            pass

                else:
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


@tree.command(name="ping", description="Lebenszeichen.")
async def ping(inter: discord.Interaction):
    await inter.response.send_message("🏓 Pong!", ephemeral=True)


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


@tree.command(name="raid_stats", description="Zeigt Mitglieder, die bei Raid-/Events nicht abgestimmt haben")
async def raid_stats_cmd(inter: discord.Interaction):
    if inter.guild_id is None or inter.guild is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if get_non_response_stats is None:
        await inter.response.send_message("❌ Statistiksystem nicht geladen.", ephemeral=True)
        return

    data = get_non_response_stats(inter.guild, store, only_started=True)

    if not data:
        await inter.response.send_message("📊 Aktuell keine Nicht-Abstimmer gefunden.", ephemeral=True)
        return

    lines = []

    for i, entry in enumerate(data[:20], start=1):
        uid = int(entry["user_id"])
        missing = int(entry["missing"])
        lines.append(f"{i}. <@{uid}> — **{missing}x** nicht abgestimmt")

    emb = discord.Embed(
        title="📊 Nicht abgestimmt – Raid/Event-Auswertung",
        description="\n".join(lines),
        color=discord.Color.orange()
    )

    emb.set_footer(text="Gezählt werden gestartete Events, bei denen das Mitglied aktuell noch in der Gilde/Zielgruppe ist.")

    await inter.response.send_message(embed=emb, ephemeral=True)


@tree.command(name="raid_stats_top", description="Top 10 Mitglieder, die am häufigsten nicht abgestimmt haben")
async def raid_stats_top_cmd(inter: discord.Interaction):
    if inter.guild_id is None or inter.guild is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    if get_non_response_stats is None:
        await inter.response.send_message("❌ Statistiksystem nicht geladen.", ephemeral=True)
        return

    data = get_non_response_stats(inter.guild, store, only_started=True)

    if not data:
        await inter.response.send_message("📊 Aktuell keine Nicht-Abstimmer gefunden.", ephemeral=True)
        return

    lines = []

    for i, entry in enumerate(data[:10], start=1):
        uid = int(entry["user_id"])
        missing = int(entry["missing"])
        lines.append(f"{i}. <@{uid}> — **{missing}x** nicht abgestimmt")

    emb = discord.Embed(
        title="🏆 Top Nicht-Abstimmer",
        description="\n".join(lines),
        color=discord.Color.red()
    )

    await inter.response.send_message(embed=emb)


def main():
    print("🚀 Starte Bot ...")
    token = _get_token()

    if not token:
        return

    bot.run(token)


if __name__ == "__main__":
    main()
