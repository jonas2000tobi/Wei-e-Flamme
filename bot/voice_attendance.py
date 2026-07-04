from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import discord
from discord import app_commands

try:
    from bot.runtime_db import (  # type: ignore
        db_status,
        start_voice_session,
        close_open_voice_sessions_for_user,
        fetch_voice_sessions,
        count_voice_sessions,
        aggregate_voice_seconds,
    )
except Exception:
    from runtime_db import (  # type: ignore
        db_status,
        start_voice_session,
        close_open_voice_sessions_for_user,
        fetch_voice_sessions,
        count_voice_sessions,
        aggregate_voice_seconds,
    )

try:
    from bot.audit_system import audit_log  # type: ignore
except Exception:
    try:
        from audit_system import audit_log  # type: ignore
    except Exception:
        audit_log = None  # type: ignore

TZ = ZoneInfo("Europe/Berlin")


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(value or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(timezone.utc).isoformat()


def _fmt_minutes(seconds: int) -> str:
    minutes = max(0, int(round(int(seconds or 0) / 60)))
    if minutes < 60:
        return f"{minutes} Min"
    h = minutes // 60
    m = minutes % 60
    return f"{h}h {m:02d}m"


def _short_name(guild: Optional[discord.Guild], user_id: int, fallback: str = "") -> str:
    if guild:
        member = guild.get_member(int(user_id))
        if member:
            return member.display_name
    fallback = str(fallback or "").strip()
    return fallback or f"User {user_id}"


def _event_modules():
    try:
        from bot import event_rsvp_dm as rsvp  # type: ignore
        return rsvp
    except Exception:
        try:
            import event_rsvp_dm as rsvp  # type: ignore
            return rsvp
        except Exception:
            return None


def _entry_user_id(entry: Any) -> int:
    try:
        if isinstance(entry, dict):
            return int(entry.get("id", 0) or 0)
        return int(entry or 0)
    except Exception:
        return 0


def _entry_name(entry: Any, uid: int) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name") or entry.get("display_name") or f"User {uid}")
    return f"User {uid}"


def _participants_from_event(event: dict) -> list[dict[str, Any]]:
    # Attendance-Snapshot-Form
    participants = event.get("participants")
    if isinstance(participants, list) and participants:
        out = []
        seen: set[int] = set()
        for p in participants:
            try:
                uid = int(p.get("id", 0) or 0)
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                out.append({
                    "id": uid,
                    "name": str(p.get("name") or f"User {uid}"),
                    "signup": str(p.get("signup") or ""),
                })
            except Exception:
                continue
        return out

    # Live-RSVP-Store-Form
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    yes = event.get("yes") if isinstance(event.get("yes"), dict) else {}
    for role_key in ("TANK", "HEAL", "DPS", "BANK"):
        for entry in yes.get(role_key, []) or []:
            uid = _entry_user_id(entry)
            if not uid or uid in seen:
                continue
            seen.add(uid)
            out.append({"id": uid, "name": _entry_name(entry, uid), "signup": role_key})
    return out


def _event_voice_channel_id(event: dict) -> int:
    for key in ("voice_channel_id", "voice_last_channel_id", "event_voice_channel_id"):
        try:
            value = int(event.get(key, 0) or 0)
            if value:
                return value
        except Exception:
            continue
    return 0


def _event_record_id(event: dict, fallback: str = "") -> str:
    for key in ("event_id", "message_id", "id"):
        value = str(event.get(key, "") or "").strip()
        if value:
            return value
    return str(fallback or "")


def _get_event(guild_id: int, event_id: str = "") -> tuple[Optional[str], Optional[dict]]:
    rsvp = _event_modules()
    if not rsvp:
        return None, None

    event_id = str(event_id or "").strip()

    # Erst Live-Store, weil dort evtl. noch voice_channel_id und Mirrors hängen.
    try:
        store = getattr(rsvp, "store", {}) or {}
        if event_id and event_id in store and isinstance(store[event_id], dict):
            return event_id, store[event_id]
        if event_id:
            for mid, obj in store.items():
                if not isinstance(obj, dict):
                    continue
                if str(obj.get("message_id", "") or "") == event_id or str(mid) == event_id:
                    return str(mid), obj
    except Exception:
        pass

    try:
        if event_id and hasattr(rsvp, "get_attendance_event"):
            ev = rsvp.get_attendance_event(int(guild_id), event_id)
            if isinstance(ev, dict):
                return event_id, ev
    except Exception:
        pass

    return None, None


def _list_events(guild_id: int, limit: int = 10) -> list[tuple[str, dict]]:
    rsvp = _event_modules()
    if not rsvp:
        return []
    found: dict[str, dict] = {}

    try:
        if hasattr(rsvp, "get_attendance_events_for_guild"):
            for ev in rsvp.get_attendance_events_for_guild(int(guild_id)) or []:
                if not isinstance(ev, dict):
                    continue
                eid = _event_record_id(ev)
                if eid:
                    found[eid] = ev
    except Exception:
        pass

    try:
        store = getattr(rsvp, "store", {}) or {}
        for mid, obj in store.items():
            if not isinstance(obj, dict):
                continue
            try:
                if int(obj.get("guild_id", 0) or 0) != int(guild_id):
                    continue
            except Exception:
                continue
            found[str(mid)] = obj
    except Exception:
        pass

    def _key(item: tuple[str, dict]):
        dt = _parse_dt(str(item[1].get("when_iso", "") or ""))
        return dt or datetime.min.replace(tzinfo=TZ)

    items = list(found.items())
    items.sort(key=_key, reverse=True)
    return items[: max(1, min(limit, 25))]


def _pick_default_event(guild_id: int) -> tuple[Optional[str], Optional[dict]]:
    events = _list_events(guild_id, limit=25)
    if not events:
        return None, None
    now = datetime.now(TZ)

    # Erst Events, die gerade/recent gelaufen sind.
    recent = []
    for eid, ev in events:
        when = _parse_dt(str(ev.get("when_iso", "") or ""))
        if not when:
            continue
        if now - timedelta(days=7) <= when <= now + timedelta(hours=1):
            recent.append((eid, ev, abs((now - when).total_seconds())))
    if recent:
        recent.sort(key=lambda x: x[2])
        return recent[0][0], recent[0][1]

    return events[0]


async def _bootstrap_current_voice_members(client: discord.Client) -> int:
    count = 0
    now_meta = {"bootstrap": True}
    for guild in list(client.guilds):
        try:
            for ch in guild.voice_channels:
                for member in ch.members:
                    if member.bot:
                        continue
                    start_voice_session(
                        guild_id=int(guild.id),
                        user_id=int(member.id),
                        channel_id=int(ch.id),
                        member_name=member.display_name,
                        channel_name=ch.name,
                        metadata=now_meta,
                    )
                    count += 1
        except Exception as e:
            print(f"[voice_attendance] Bootstrap Fehler in Guild {getattr(guild, 'id', '?')}: {e!r}")
    return count


async def setup_voice_attendance(client: discord.Client, tree: app_commands.CommandTree):
    if not getattr(client, "_ebolus_voice_attendance_listener_added", False):
        async def _voice_attendance_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
            try:
                if member.bot or member.guild is None:
                    return
                before_ch = before.channel
                after_ch = after.channel
                if before_ch is not None and after_ch is not None and before_ch.id == after_ch.id:
                    return

                if before_ch is not None:
                    close_open_voice_sessions_for_user(
                        int(member.guild.id),
                        int(member.id),
                        channel_id=int(before_ch.id),
                    )

                if after_ch is not None:
                    start_voice_session(
                        guild_id=int(member.guild.id),
                        user_id=int(member.id),
                        channel_id=int(after_ch.id),
                        member_name=member.display_name,
                        channel_name=getattr(after_ch, "name", ""),
                    )
            except Exception as e:
                print(f"[voice_attendance] Voice-State-Fehler: {e!r}")

        client.add_listener(_voice_attendance_state_update, "on_voice_state_update")
        setattr(client, "_ebolus_voice_attendance_listener_added", True)
        print("🎙️ Voice-Attendance Listener gestartet.")

    try:
        boot_count = await _bootstrap_current_voice_members(client)
        print(f"🎙️ Voice-Attendance Bootstrap: {boot_count} laufende Voice-Mitglieder erfasst.")
    except Exception as e:
        print(f"[voice_attendance] Bootstrap Startfehler: {e!r}")

    @tree.command(name="voice_attendance_status", description="(Admin) Status vom Voice-Attendance-Tracking")
    async def voice_attendance_status(inter: discord.Interaction):
        if inter.guild_id is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        status = db_status()
        server_sessions = count_voice_sessions(inter.guild_id)
        open_server = count_voice_sessions(inter.guild_id, open_only=True)
        total_sessions = count_voice_sessions(None)

        emb = discord.Embed(
            title="🎙️ Voice-Attendance Status",
            color=discord.Color.green() if status.get("backend") == "postgres" else discord.Color.orange(),
        )
        emb.add_field(name="Tracking", value="Aktiv", inline=True)
        emb.add_field(name="Backend", value=str(status.get("backend") or "?"), inline=True)
        emb.add_field(name="Speicher", value=str(status.get("path") or "?"), inline=False)
        emb.add_field(name="Sessions Server", value=str(server_sessions), inline=True)
        emb.add_field(name="Offene Sessions", value=str(open_server), inline=True)
        emb.add_field(name="Sessions Gesamt", value=str(total_sessions), inline=True)
        emb.set_footer(text="Es wird nur gemessen. EC wird nicht automatisch vergeben.")
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="voice_attendance_events", description="(Admin) Zeigt Event-IDs für Voice-Anwesenheitsvorschläge")
    async def voice_attendance_events(inter: discord.Interaction):
        if inter.guild_id is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        events = _list_events(inter.guild_id, limit=12)
        if not events:
            await inter.response.send_message("❌ Keine Events gefunden.", ephemeral=True)
            return

        lines = []
        for eid, ev in events:
            when = _parse_dt(str(ev.get("when_iso", "") or ""))
            when_txt = when.strftime("%d.%m.%Y %H:%M") if when else "?"
            title = str(ev.get("title", "Event") or "Event")[:45]
            voice_id = _event_voice_channel_id(ev)
            voice_txt = f" · Voice <#{voice_id}>" if voice_id else ""
            lines.append(f"• `{eid}` — **{title}** — {when_txt}{voice_txt}")

        emb = discord.Embed(
            title="🎙️ Events für Voice-Anwesenheit",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        emb.set_footer(text="Nutze die ID mit /voice_attendance_suggest event_id:...")
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="voice_attendance_recent", description="(Admin) Letzte gemessene Voice-Sessions")
    @app_commands.describe(member="Optional: nur ein Mitglied", limit="Anzahl 1-15")
    async def voice_attendance_recent(inter: discord.Interaction, member: Optional[discord.Member] = None, limit: Optional[int] = 8):
        if inter.guild_id is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        lim = max(1, min(int(limit or 8), 15))
        since = _iso_utc(_now_utc() - timedelta(days=14))
        rows = fetch_voice_sessions(
            inter.guild_id,
            since_iso=since,
            until_iso=_iso_utc(_now_utc() + timedelta(minutes=1)),
            user_ids=[int(member.id)] if member else None,
            limit=50,
        )[:lim]

        if not rows:
            await inter.response.send_message("🎙️ Noch keine Voice-Sessions gefunden.", ephemeral=True)
            return

        lines = []
        for r in rows:
            uid = int(r.get("user_id") or 0)
            ch_id = int(r.get("channel_id") or 0)
            start = _parse_dt(str(r.get("joined_at") or ""))
            start_txt = start.strftime("%d.%m. %H:%M") if start else "?"
            if r.get("left_at"):
                sec = int(r.get("duration_seconds") or 0)
                dur = _fmt_minutes(sec)
            else:
                dur = "läuft noch"
            lines.append(f"• {start_txt} — {_short_name(inter.guild, uid)} — <#{ch_id}> — **{dur}**")

        emb = discord.Embed(
            title="🎙️ Letzte Voice-Sessions",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="voice_attendance_suggest", description="(Admin) Voice-Zeiten als Anwesenheitsvorschlag für ein Event")
    @app_commands.describe(
        event_id="Optional: Event-ID aus /voice_attendance_events. Leer = passendstes aktuelles Event",
        dauer_minuten="Geplante Eventdauer. Standard 120",
        vorlauf_minuten="Voice-Zeit vor Start mitzählen. Standard 15",
        voll_ab_prozent="Ab wie viel % Eventdauer: voll dabei. Standard 70",
        teil_ab_prozent="Ab wie viel % Eventdauer: teilweise. Standard 20",
        nur_event_voice="Wenn ja: nur gespeicherten Event-Voice zählen. Wenn nein: alle Voicekanäle im Zeitfenster"
    )
    async def voice_attendance_suggest(
        inter: discord.Interaction,
        event_id: Optional[str] = None,
        dauer_minuten: Optional[int] = 120,
        vorlauf_minuten: Optional[int] = 15,
        voll_ab_prozent: Optional[int] = 70,
        teil_ab_prozent: Optional[int] = 20,
        nur_event_voice: Optional[bool] = True,
    ):
        if inter.guild_id is None or inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)

        eid, ev = _get_event(inter.guild_id, event_id or "") if event_id else _pick_default_event(inter.guild_id)
        if not eid or not ev:
            await inter.followup.send("❌ Kein Event gefunden. Nutze erst `/voice_attendance_events` und kopiere die Event-ID.", ephemeral=True)
            return

        when = _parse_dt(str(ev.get("when_iso", "") or ""))
        if not when:
            await inter.followup.send("❌ Event hat keine gültige Startzeit.", ephemeral=True)
            return

        duration_min = max(15, min(int(dauer_minuten or 120), 600))
        pre_min = max(0, min(int(vorlauf_minuten or 15), 180))
        full_pct = max(1, min(int(voll_ab_prozent or 70), 100))
        partial_pct = max(0, min(int(teil_ab_prozent or 20), full_pct))

        window_start = when - timedelta(minutes=pre_min)
        window_end = when + timedelta(minutes=duration_min)
        event_seconds = duration_min * 60

        voice_id = _event_voice_channel_id(ev)
        channel_ids = [voice_id] if (bool(nur_event_voice) and voice_id) else None
        participants = _participants_from_event(ev)
        participant_ids = [int(p["id"]) for p in participants if int(p.get("id", 0) or 0)]

        totals = aggregate_voice_seconds(
            inter.guild_id,
            since_iso=_iso_utc(window_start),
            until_iso=_iso_utc(window_end),
            channel_ids=channel_ids,
            user_ids=participant_ids or None,
        )

        if not participants:
            # Fallback: Wenn kein RSVP-Snapshot vorhanden ist, zumindest die gemessenen Voice-User zeigen.
            participants = [{"id": uid, "name": _short_name(inter.guild, uid), "signup": ""} for uid in totals.keys()]

        full: list[str] = []
        partial: list[str] = []
        none: list[str] = []

        for p in participants:
            uid = int(p.get("id") or 0)
            name = _short_name(inter.guild, uid, str(p.get("name") or ""))
            signup = str(p.get("signup") or "").strip()
            seconds = int(totals.get(uid, 0))
            pct = int(round((seconds / event_seconds) * 100)) if event_seconds else 0
            role_suffix = f" · {signup}" if signup else ""
            line = f"{name}{role_suffix} — **{_fmt_minutes(seconds)}** ({pct}%)"
            if pct >= full_pct:
                full.append("✅ " + line)
            elif pct >= partial_pct:
                partial.append("🟡 " + line)
            else:
                none.append("❌ " + line)

        def _field_text(items: list[str], max_items: int = 15) -> str:
            if not items:
                return "—"
            shown = items[:max_items]
            rest = len(items) - len(shown)
            text = "\n".join(shown)
            if rest > 0:
                text += f"\n… +{rest} weitere"
            return text[:1024]

        title = str(ev.get("title", "Event") or "Event")
        voice_text = f"nur Event-Voice <#{voice_id}>" if channel_ids else "alle Voicekanäle im Zeitfenster"
        emb = discord.Embed(
            title="🎙️ Voice-Anwesenheitsvorschlag",
            description=(
                f"**Event:** {title}\n"
                f"**Event-ID:** `{eid}`\n"
                f"**Fenster:** {window_start.strftime('%d.%m.%Y %H:%M')} – {window_end.strftime('%H:%M')}\n"
                f"**Zählung:** {voice_text}\n\n"
                "Das ist nur ein Vorschlag. **Es wird keine EC automatisch vergeben.**"
            ),
            color=discord.Color.green(),
        )
        emb.add_field(name=f"✅ Voll dabei ≥ {full_pct}%", value=_field_text(full), inline=False)
        emb.add_field(name=f"🟡 Teilweise ≥ {partial_pct}%", value=_field_text(partial), inline=False)
        emb.add_field(name="❌ Nicht/kaum erkannt", value=_field_text(none), inline=False)
        emb.set_footer(text="Optional: Im EC-Anwesenheitscheck per Button übernehmen. EC wird trotzdem erst nach Bestätigung vergeben.")

        if audit_log:
            try:
                audit_log(
                    guild_id=inter.guild_id,
                    actor_id=inter.user.id,
                    action="voice_attendance_suggest",
                    target_type="event",
                    target_id=str(eid),
                    summary=f"Voice-Anwesenheitsvorschlag erstellt: {title}",
                    metadata={
                        "duration_minutes": duration_min,
                        "pre_minutes": pre_min,
                        "full_percent": full_pct,
                        "partial_percent": partial_pct,
                        "voice_channel_id": voice_id,
                    },
                )
            except Exception:
                pass

        await inter.followup.send(embed=emb, ephemeral=True)
