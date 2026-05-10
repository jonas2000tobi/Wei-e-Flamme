from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import tasks

try:
    from bot.event_rsvp_dm import store, TZ, _eligible_members  # type: ignore
except ModuleNotFoundError:
    from event_rsvp_dm import store, TZ, _eligible_members  # type: ignore

try:
    from bot.raid_stats import get_non_response_stats  # type: ignore
except ModuleNotFoundError:
    from raid_stats import get_non_response_stats  # type: ignore


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CFG_FILE = DATA_DIR / "weekly_report_cfg.json"
POST_LOG_FILE = DATA_DIR / "weekly_report_post_log.json"
ONBOARDING_CFG_FILE = DATA_DIR / "onboarding_cfg.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"
MEMBER_PROFILE_FILE = DATA_DIR / "member_profiles.json"


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


cfg: dict = _load_json(CFG_FILE, {})
post_log: dict = _load_json(POST_LOG_FILE, {})


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _gcfg(guild_id: int) -> dict:
    c = cfg.get(str(guild_id)) or {}
    c.setdefault("enabled", False)
    c.setdefault("channel_id", 0)
    c.setdefault("weekday", 6)
    c.setdefault("hour", 18)
    c.setdefault("minute", 0)
    cfg[str(guild_id)] = c
    return c


def _save_cfg() -> None:
    _save_json(CFG_FILE, cfg)


def _save_post_log() -> None:
    _save_json(POST_LOG_FILE, post_log)


def _week_start_end() -> tuple[datetime, datetime]:
    now = datetime.now(TZ)
    start = now - timedelta(days=now.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end


def _event_voters(obj: dict) -> set[int]:
    voted: set[int] = set()

    def uid_from(entry):
        try:
            if isinstance(entry, dict):
                return int(entry.get("id", 0) or 0)
            return int(entry)
        except Exception:
            return 0

    yes = obj.get("yes") or {}

    for key in ("TANK", "HEAL", "DPS", "BANK"):
        for e in yes.get(key, []) or []:
            uid = uid_from(e)
            if uid:
                voted.add(uid)

    for e in obj.get("no", []) or []:
        uid = uid_from(e)
        if uid:
            voted.add(uid)

    maybe = obj.get("maybe") or {}

    for uid_str, entry in maybe.items():
        try:
            uid = int(uid_str)
        except Exception:
            uid = uid_from(entry)

        if uid:
            voted.add(uid)

    return voted


def _events_this_week(guild: discord.Guild) -> List[dict]:
    start, end = _week_start_end()
    out = []

    for msg_id, obj in list(store.items()):
        try:
            if int(obj.get("guild_id", 0) or 0) != guild.id:
                continue

            when = datetime.fromisoformat(obj.get("when_iso"))

            if when.tzinfo is None:
                when = when.replace(tzinfo=TZ)

            if not (start <= when < end):
                continue

            eligible = _eligible_members(guild, obj)
            voted = _event_voters(obj)

            out.append({
                "message_id": str(msg_id),
                "title": str(obj.get("title", "Event")),
                "when": when,
                "voted": len(voted),
                "eligible": len(eligible),
            })

        except Exception:
            continue

    out.sort(key=lambda x: x["when"])
    return out


def _new_members_this_week(guild: discord.Guild) -> List[discord.Member]:
    start, end = _week_start_end()
    out = []

    for m in guild.members:
        if m.bot:
            continue

        if not m.joined_at:
            continue

        joined = m.joined_at.astimezone(TZ)

        if start <= joined < end:
            out.append(m)

    out.sort(key=lambda m: m.joined_at or datetime.now(TZ))
    return out


def _load_onboarding_cfg() -> dict:
    return _load_json(ONBOARDING_CFG_FILE, {})


def _open_applicants(guild: discord.Guild) -> List[discord.Member]:
    ob_cfg = _load_onboarding_cfg()
    gcfg = ob_cfg.get(str(guild.id)) or {}
    cat = gcfg.get("category_roles") or {}
    applicant_role_id = int(cat.get("applicant", 0) or 0)

    if not applicant_role_id:
        return []

    role = guild.get_role(applicant_role_id)

    if not role:
        return []

    return [m for m in role.members if not m.bot]


def _load_leader_contact_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _load_member_profiles() -> dict:
    return _load_json(MEMBER_PROFILE_FILE, {})


def _active_absences(guild: discord.Guild) -> List[str]:
    data = _load_member_profiles()
    g = data.get(str(guild.id)) or {}
    absences = g.get("absences") or {}
    users = g.get("users") or {}

    out = []

    today = datetime.now(TZ).date()

    for uid_str, a in absences.items():
        try:
            uid = int(uid_str)
            member = guild.get_member(uid)

            if not member or member.bot:
                continue

            to_s = str(a.get("to", "") or "")
            if "-" not in to_s:
                continue

            dd, mm = [int(x) for x in to_s.split("-")]
            to_d = datetime(today.year, mm, dd, tzinfo=TZ).date()

            if to_d < today:
                continue

            profile = users.get(uid_str) or {}
            name = profile.get("ingame_name") or member.display_name
            from_s = str(a.get("from", "—"))
            reason = str(a.get("reason", "—"))

            out.append(f"• **{name}**: {from_s} bis {to_s} – {reason[:80]}")

        except Exception:
            continue

    return out[:15]


async def _leader_contact_status_counts(guild: discord.Guild) -> dict:
    out = {
        "open": 0,
        "claimed": 0,
        "done": 0,
    }

    lc_cfg = _load_leader_contact_cfg()
    gcfg = lc_cfg.get(str(guild.id)) or {}
    internal_channel_id = int(gcfg.get("internal_channel_id", 0) or 0)

    if not internal_channel_id:
        return out

    ch = guild.get_channel(internal_channel_id)

    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return out

    try:
        async for msg in ch.history(limit=200):
            if not msg.embeds:
                continue

            emb = msg.embeds[0]

            status_text = ""

            for field in emb.fields:
                if field.name == "Status":
                    status_text = str(field.value or "")
                    break

            if "Offen" in status_text:
                out["open"] += 1
            elif "Übernommen" in status_text:
                out["claimed"] += 1
            elif "Erledigt" in status_text:
                out["done"] += 1

    except Exception:
        pass

    return out


async def build_weekly_report_embed(guild: discord.Guild) -> discord.Embed:
    start, end = _week_start_end()

    new_members = _new_members_this_week(guild)
    applicants = _open_applicants(guild)
    events = _events_this_week(guild)
    non_response = get_non_response_stats(guild, store, only_started=True)
    leader_counts = await _leader_contact_status_counts(guild)
    absences = _active_absences(guild)

    emb = discord.Embed(
        title="📋 Wochenbericht – ebolus",
        description=(
            f"Zeitraum: **{start.strftime('%d.%m.%Y')} – {(end - timedelta(days=1)).strftime('%d.%m.%Y')}**"
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(TZ),
    )

    member_lines = [
        f"Neue Mitglieder diese Woche: **{len(new_members)}**",
        f"Offene Bewerber: **{len(applicants)}**",
    ]

    if new_members:
        member_lines.append("")
        member_lines.extend([f"• {m.mention}" for m in new_members[:10]])

    emb.add_field(
        name="👥 Mitglieder",
        value="\n".join(member_lines) or "—",
        inline=False,
    )

    if events:
        event_lines = []

        for e in events[:15]:
            event_lines.append(
                f"• **{e['title']}** — {e['when'].strftime('%d.%m. %H:%M')} "
                f"— **{e['voted']} / {e['eligible']}** abgestimmt"
            )
    else:
        event_lines = ["Keine Events diese Woche gefunden."]

    emb.add_field(
        name="📅 Events diese Woche",
        value="\n".join(event_lines),
        inline=False,
    )

    if non_response:
        nr_lines = []

        for i, entry in enumerate(non_response[:10], start=1):
            nr_lines.append(
                f"{i}. <@{int(entry['user_id'])}> — **{int(entry['missing'])}x** nicht abgestimmt"
            )
    else:
        nr_lines = ["Keine Nicht-Abstimmer gefunden."]

    emb.add_field(
        name="⚠️ Nicht-Abstimmer",
        value="\n".join(nr_lines),
        inline=False,
    )

    emb.add_field(
        name="📨 Leader-Anfragen",
        value=(
            f"Offen: **{leader_counts['open']}**\n"
            f"Übernommen: **{leader_counts['claimed']}**\n"
            f"Erledigt: **{leader_counts['done']}**"
        ),
        inline=False,
    )

    emb.add_field(
        name="🏖️ Abwesenheiten",
        value="\n".join(absences) if absences else "Keine aktiven/geplanten Abwesenheiten gefunden.",
        inline=False,
    )

    emb.add_field(
        name="🎁 Loot / Needliste",
        value="Noch kein Loot-Modul aktiv.",
        inline=False,
    )

    emb.set_footer(text="Automatischer Leader-Wochenbericht")

    return emb


async def send_weekly_report(client: discord.Client, guild: discord.Guild, channel: discord.TextChannel) -> Optional[discord.Message]:
    emb = await build_weekly_report_embed(guild)
    return await channel.send(embed=emb)


def _should_post_now(guild_id: int, c: dict) -> bool:
    now = datetime.now(TZ)

    if not bool(c.get("enabled", False)):
        return False

    if now.weekday() != int(c.get("weekday", 6)):
        return False

    if now.hour != int(c.get("hour", 18)):
        return False

    if now.minute != int(c.get("minute", 0)):
        return False

    today_key = now.strftime("%Y-%m-%d")
    last_key = str(post_log.get(str(guild_id), ""))

    return last_key != today_key


def _mark_posted(guild_id: int) -> None:
    post_log[str(guild_id)] = datetime.now(TZ).strftime("%Y-%m-%d")
    _save_json(POST_LOG_FILE, post_log)


_report_task_started = False


async def setup_weekly_report(client: discord.Client, tree: app_commands.CommandTree):
    global _report_task_started

    @tree.command(name="weekly_report_set_channel", description="(Admin) Channel für Wochenbericht setzen")
    async def weekly_report_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg()

        await inter.response.send_message(f"✅ Wochenbericht-Channel gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="weekly_report_toggle", description="(Admin) Wochenbericht aktivieren/deaktivieren")
    async def weekly_report_toggle(inter: discord.Interaction, enabled: bool):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["enabled"] = bool(enabled)
        cfg[str(inter.guild_id)] = c
        _save_cfg()

        await inter.response.send_message(
            f"✅ Wochenbericht {'aktiviert' if enabled else 'deaktiviert'}.",
            ephemeral=True
        )

    @tree.command(name="weekly_report_time", description="(Admin) Zeit für Wochenbericht setzen")
    @app_commands.describe(
        weekday="0=Montag, 1=Dienstag, ..., 6=Sonntag",
        hour="Stunde 0-23",
        minute="Minute 0-59"
    )
    async def weekly_report_time(inter: discord.Interaction, weekday: int, hour: int, minute: int):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        if weekday < 0 or weekday > 6 or hour < 0 or hour > 23 or minute < 0 or minute > 59:
            await inter.response.send_message("❌ Ungültige Zeit.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["weekday"] = int(weekday)
        c["hour"] = int(hour)
        c["minute"] = int(minute)
        cfg[str(inter.guild_id)] = c
        _save_cfg()

        await inter.response.send_message(
            f"✅ Wochenbericht-Zeit gesetzt: Wochentag `{weekday}`, `{hour:02d}:{minute:02d}`.",
            ephemeral=True
        )

    @tree.command(name="weekly_report_status", description="(Admin) Zeigt Wochenbericht-Konfiguration")
    async def weekly_report_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        ch_id = int(c.get("channel_id", 0) or 0)

        text = (
            f"**Wochenbericht Status**\n"
            f"• Aktiv: **{'Ja' if c.get('enabled') else 'Nein'}**\n"
            f"• Channel: {f'<#{ch_id}>' if ch_id else '—'}\n"
            f"• Wochentag: `{c.get('weekday')}`\n"
            f"• Uhrzeit: `{int(c.get('hour', 18)):02d}:{int(c.get('minute', 0)):02d}`"
        )

        await inter.response.send_message(text, ephemeral=True)

    @tree.command(name="weekly_report_now", description="(Admin) Erstellt den Wochenbericht sofort")
    async def weekly_report_now(inter: discord.Interaction):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        ch_id = int(c.get("channel_id", 0) or 0)
        ch = inter.guild.get_channel(ch_id) if ch_id else inter.channel

        if not isinstance(ch, discord.TextChannel):
            await inter.followup.send("❌ Kein gültiger Report-Channel gesetzt.", ephemeral=True)
            return

        try:
            msg = await send_weekly_report(client, inter.guild, ch)
        except Exception as e:
            await inter.followup.send(f"❌ Bericht konnte nicht erstellt werden: {e}", ephemeral=True)
            return

        await inter.followup.send(f"✅ Wochenbericht erstellt: {msg.jump_url}", ephemeral=True)

    if not _report_task_started:
        _report_task_started = True

        @tasks.loop(minutes=1)
        async def weekly_report_loop():
            try:
                for guild in client.guilds:
                    c = _gcfg(guild.id)

                    if not _should_post_now(guild.id, c):
                        continue

                    ch_id = int(c.get("channel_id", 0) or 0)

                    if not ch_id:
                        continue

                    ch = guild.get_channel(ch_id)

                    if not isinstance(ch, discord.TextChannel):
                        continue

                    try:
                        await send_weekly_report(client, guild, ch)
                        _mark_posted(guild.id)
                    except Exception as e:
                        print(f"[weekly_report] Fehler bei Guild {guild.id}: {e!r}")

            except Exception as e:
                print(f"[weekly_report_loop] Fehler: {e!r}")

        weekly_report_loop.start()
        print("📋 Weekly-Report-Task gestartet.")
