from __future__ import annotations

import json
import re
import asyncio
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Any, Tuple

import discord
from discord import app_commands
from discord.ui import View, button, Modal, TextInput, Select
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

try:
    from bot.event_dm_prefs import set_dm_pref, is_dm_enabled  # type: ignore
except ModuleNotFoundError:
    try:
        from event_dm_prefs import set_dm_pref, is_dm_enabled  # type: ignore
    except ModuleNotFoundError:
        set_dm_pref = None  # type: ignore
        is_dm_enabled = None  # type: ignore


TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CFG_FILE = DATA_DIR / "member_portal_cfg.json"
PROFILE_FILE = DATA_DIR / "member_profiles.json"
SENT_FILE = DATA_DIR / "member_portal_sent.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


cfg: dict = _load_json(CFG_FILE, {})
profiles: dict = _load_json(PROFILE_FILE, {})
sent_state: dict = _load_json(SENT_FILE, {})


def save_cfg() -> None:
    _save_json(CFG_FILE, cfg)


def save_profiles() -> None:
    _save_json(PROFILE_FILE, profiles)


def save_sent() -> None:
    _save_json(SENT_FILE, sent_state)


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _is_portal_admin(guild: Optional[discord.Guild], member: Optional[discord.Member]) -> bool:
    if guild is None or member is None:
        return False

    perms = getattr(member, "guild_permissions", None)
    if perms and (perms.administrator or perms.manage_guild):
        return True

    try:
        c = _gcfg(guild.id)
        roles = c.get("position_roles") or {}
        for key in ("leader", "advisor", "guardian"):
            role_id = int(roles.get(key, 0) or 0)
            if not role_id:
                continue
            role = guild.get_role(role_id)
            if role and role in member.roles:
                return True
    except Exception:
        pass

    try:
        leader_cfg = _get_leader_cfg(guild.id)
        role_id = int(leader_cfg.get("leader_role_id", 0) or 0)
        role = guild.get_role(role_id) if role_id else None
        if role and role in member.roles:
            return True
    except Exception:
        pass

    return False


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _gcfg(guild_id: int) -> dict:
    c = cfg.get(str(guild_id)) or {}
    c.setdefault("absence_channel_id", 0)
    c.setdefault("portal_post_channel_id", 0)
    c.setdefault("portal_post_message_id", 0)
    c.setdefault("member_role_id", 0)
    c.setdefault("position_roles", {
        "leader": 0,
        "advisor": 0,
        "guardian": 0,
    })
    c.setdefault("events", [])
    cfg[str(guild_id)] = c
    return c


def _gdata(guild_id: int) -> dict:
    g = profiles.get(str(guild_id)) or {}
    g.setdefault("users", {})
    g.setdefault("absences", {})
    profiles[str(guild_id)] = g
    return g


def _user_profile(guild_id: int, user_id: int) -> dict:
    g = _gdata(guild_id)
    u = g["users"].get(str(user_id)) or {}
    u.setdefault("ingame_name", "")
    u.setdefault("main_role", "")
    u.setdefault("gearscore", "")
    u.setdefault("created_at", datetime.now(TZ).isoformat())
    g["users"][str(user_id)] = u
    return u


def _sent_guild(guild_id: int) -> dict:
    g = sent_state.get(str(guild_id)) or {}
    g.setdefault("sent_users", [])
    g.setdefault("users", {})
    sent_state[str(guild_id)] = g
    return g


def _portal_user_state(guild_id: int, user_id: int) -> dict:
    g = _sent_guild(guild_id)
    users = g.setdefault("users", {})
    u = users.get(str(user_id)) or {}
    u.setdefault("menu_message_id", 0)
    u.setdefault("sent_at", "")
    users[str(user_id)] = u
    return u


def _mark_portal_sent(guild_id: int, user_id: int, message_id: int | None = None) -> None:
    g = _sent_guild(guild_id)

    arr = set(str(x) for x in g.get("sent_users", []))
    arr.add(str(user_id))
    g["sent_users"] = sorted(arr)

    u = _portal_user_state(guild_id, user_id)

    if message_id:
        u["menu_message_id"] = int(message_id)

    u["sent_at"] = datetime.now(TZ).isoformat()

    save_sent()


def _portal_was_sent(guild_id: int, user_id: int) -> bool:
    g = _sent_guild(guild_id)
    arr = set(str(x) for x in g.get("sent_users", []))

    if str(user_id) in arr:
        return True

    u = _portal_user_state(guild_id, user_id)
    return bool(int(u.get("menu_message_id", 0) or 0))


def _portal_message_id(guild_id: int, user_id: int) -> int:
    u = _portal_user_state(guild_id, user_id)

    try:
        return int(u.get("menu_message_id", 0) or 0)
    except Exception:
        return 0


def _clear_portal_sent(guild_id: int, user_id: int) -> None:
    g = _sent_guild(guild_id)

    arr = set(str(x) for x in g.get("sent_users", []))
    arr.discard(str(user_id))
    g["sent_users"] = sorted(arr)

    users = g.setdefault("users", {})
    users.pop(str(user_id), None)

    save_sent()


def _resolve_guild_for_user(client: discord.Client, user_id: int) -> Optional[discord.Guild]:
    """
    Wichtig bei Allianz-/Multi-Server-Betrieb:
    Der Bot ist auf mehreren Discords. Für das private Gildenmenü müssen wir
    den echten Ebolus/Home-Server nehmen, nicht irgendeinen Partner-Server,
    auf dem der User zufällig auch ist.
    """

    # 1. Bevorzugt: Server, auf dem der User die gesetzte Gildenmitglied-Rolle hat.
    for guild in client.guilds:
        try:
            c = _gcfg(guild.id)
            member_role_id = int(c.get("member_role_id", 0) or 0)

            if not member_role_id:
                continue

            role = guild.get_role(member_role_id)

            if not role:
                continue

            member = guild.get_member(user_id)

            if member and not member.bot and role in member.roles:
                return guild

        except Exception:
            continue

    # 2. Bevorzugt: Server, für den bereits ein Portal-Menü für diesen User gespeichert ist.
    for guild_id_str, g in list(sent_state.items()):
        try:
            guild_id = int(guild_id_str)
            guild = client.get_guild(guild_id)

            if not guild:
                continue

            sent_users = {str(x) for x in g.get("sent_users", [])}
            users = g.get("users", {}) or {}

            if str(user_id) in sent_users or str(user_id) in users:
                member = guild.get_member(user_id)

                if member and not member.bot:
                    return guild

        except Exception:
            continue

    # 3. Falls alliance_config vorhanden ist: Home-/Ebolus-Server bevorzugen.
    try:
        try:
            from bot.alliance_config import _home_guild_id  # type: ignore
        except ModuleNotFoundError:
            from alliance_config import _home_guild_id  # type: ignore

        home_id = int(_home_guild_id() or 0)

        if home_id:
            guild = client.get_guild(home_id)

            if guild:
                member = guild.get_member(user_id)

                if member and not member.bot:
                    return guild

    except Exception:
        pass

    # 4. Fallback: altes Verhalten.
    for guild in client.guilds:
        member = guild.get_member(user_id)

        if member and not member.bot:
            return guild

    return None


def _member_position(guild: discord.Guild, member: discord.Member) -> str:
    c = _gcfg(guild.id)
    roles = c.get("position_roles") or {}

    leader = guild.get_role(int(roles.get("leader", 0) or 0))
    advisor = guild.get_role(int(roles.get("advisor", 0) or 0))
    guardian = guild.get_role(int(roles.get("guardian", 0) or 0))

    if leader and leader in member.roles:
        return "Anführer"
    if advisor and advisor in member.roles:
        return "Gildenberater"
    if guardian and guardian in member.roles:
        return "Wächter"

    return "Mitglied"


def _position_rank(position: str) -> int:
    order = {
        "Anführer": 0,
        "Gildenberater": 1,
        "Berater": 1,
        "Wächter": 2,
        "Mitglied": 3,
    }
    return order.get(position, 99)


def _parse_gearscore(value: Any) -> int:
    try:
        s = str(value or "").strip()
        s = re.sub(r"[^0-9]", "", s)

        if not s:
            return 0

        return int(s)
    except Exception:
        return 0


def _display_name(member: discord.Member) -> str:
    return member.display_name


def _guild_join_date(member: discord.Member) -> str:
    if not member.joined_at:
        return "Unbekannt"

    return member.joined_at.astimezone(TZ).strftime("%d.%m.%Y")


def _guild_days(member: discord.Member) -> int:
    if not member.joined_at:
        return 0

    joined = member.joined_at.astimezone(TZ)
    now = datetime.now(TZ)

    return max(0, (now.date() - joined.date()).days)


_DATE_RE = re.compile(r"^\d{2}-\d{2}$")


def _valid_ddmm(value: str) -> bool:
    value = (value or "").strip()

    if not _DATE_RE.match(value):
        return False

    try:
        dd, mm = [int(x) for x in value.split("-")]
        date(datetime.now(TZ).year, mm, dd)
        return True
    except Exception:
        return False


def _ddmm_to_date(value: str, year: int) -> date:
    dd, mm = [int(x) for x in value.split("-")]
    return date(year, mm, dd)


def _absence_for_user(guild_id: int, user_id: int) -> Optional[dict]:
    g = _gdata(guild_id)
    raw = g.get("absences", {}).get(str(user_id))
    return raw if isinstance(raw, dict) else None


def _absence_dates(absence: dict) -> Optional[tuple[date, date]]:
    try:
        from_s = str(absence.get("from", "")).strip()
        to_s = str(absence.get("to", "")).strip()

        if not _valid_ddmm(from_s) or not _valid_ddmm(to_s):
            return None

        today = datetime.now(TZ).date()
        from_d = _ddmm_to_date(from_s, today.year)
        to_d = _ddmm_to_date(to_s, today.year)

        # Nur echter Jahreswechsel, z. B. 28-12 bis 03-01.
        if to_d < from_d:
            to_d = date(today.year + 1, to_d.month, to_d.day)

        return from_d, to_d

    except Exception:
        return None

def _is_absent_now(absence: dict) -> bool:
    try:
        dates = _absence_dates(absence)

        if not dates:
            return False

        from_d, to_d = dates
        today = datetime.now(TZ).date()

        return from_d <= today <= to_d

    except Exception:
        return False


def _status_for_user(guild_id: int, user_id: int) -> str:
    absence = _absence_for_user(guild_id, user_id)

    if absence and _is_absent_now(absence):
        from_s = str(absence.get("from", "—"))
        to_s = str(absence.get("to", "—"))
        return f"Abwesend von {from_s} bis {to_s}"

    return "Aktiv"


def _rsvp_entry_user_id(entry: Any) -> int:
    try:
        if isinstance(entry, dict):
            return int(entry.get("id", 0) or 0)
        return int(entry)
    except Exception:
        return 0


def _rsvp_voted(obj: dict, user_id: int) -> bool:
    yes = obj.get("yes") or {}

    for key in ("TANK", "HEAL", "DPS", "BANK"):
        for entry in yes.get(key, []) or []:
            if _rsvp_entry_user_id(entry) == int(user_id):
                return True

    for entry in obj.get("no", []) or []:
        if _rsvp_entry_user_id(entry) == int(user_id):
            return True

    maybe = obj.get("maybe") or {}

    if str(user_id) in maybe:
        return True

    for entry in maybe.values():
        if _rsvp_entry_user_id(entry) == int(user_id):
            return True

    return False


def _rsvp_user_status(obj: dict, user_id: int) -> str:
    yes = obj.get("yes") or {}

    labels = {
        "TANK": "Tank",
        "HEAL": "Heal",
        "DPS": "DPS",
        "BANK": "Bank",
    }

    for key, label in labels.items():
        for entry in yes.get(key, []) or []:
            if _rsvp_entry_user_id(entry) == int(user_id):
                return label

    maybe = obj.get("maybe") or {}

    if str(user_id) in maybe:
        return "Vielleicht"

    for entry in maybe.values():
        if _rsvp_entry_user_id(entry) == int(user_id):
            return "Vielleicht"

    for entry in obj.get("no", []) or []:
        if _rsvp_entry_user_id(entry) == int(user_id):
            return "Abgemeldet"

    return ""


def _event_status_block(guild: discord.Guild, member: discord.Member) -> str:
    try:
        try:
            from bot.event_rsvp_dm import store as event_store  # type: ignore
        except ModuleNotFoundError:
            from event_rsvp_dm import store as event_store  # type: ignore

        now = datetime.now(TZ)
        answered = []
        open_votes = []

        for _msg_id, obj in list(event_store.items()):
            try:
                event_guild_id = int(obj.get("guild_id", 0) or 0)
                scope = str(obj.get("scope", "") or "").lower()

                if scope == "alliance":
                    mirrors = obj.get("mirrors") or []
                    mirror_guild_ids = set()

                    for mirror in mirrors:
                        try:
                            mirror_guild_ids.add(int(mirror.get("guild_id", 0) or 0))
                        except Exception:
                            pass

                    if int(guild.id) not in mirror_guild_ids and event_guild_id != int(guild.id):
                        continue
                else:
                    if event_guild_id != int(guild.id):
                        continue

                when = datetime.fromisoformat(obj.get("when_iso", ""))

                if now >= when:
                    continue

                title = str(obj.get("title", "Event"))
                line_base = f"{when.strftime('%d.%m. %H:%M')} – {title}"

                status = _rsvp_user_status(obj, member.id)

                if status:
                    answered.append((when, f"• {line_base} – {status}"))
                else:
                    open_votes.append((when, f"• {line_base}"))

            except Exception:
                continue

        answered.sort(key=lambda x: x[0])
        open_votes.sort(key=lambda x: x[0])

        parts = []

        if answered:
            lines = [x[1] for x in answered[:3]]
            parts.append("📌 **Deine kommenden Events:**\n" + "\n".join(lines))

        if open_votes:
            lines = [x[1] for x in open_votes[:3]]
            parts.append("⚠️ **Offene Abstimmungen:**\n" + "\n".join(lines))

        if not parts:
            return "📌 **Deine kommenden Events:**\nKeine aktiven Anmeldungen oder offenen Abstimmungen."

        return "\n\n".join(parts)

    except Exception:
        return "📌 **Deine kommenden Events:**\nKeine Übersicht verfügbar."


def _main_menu_embed(guild: discord.Guild, member: Optional[discord.Member] = None) -> discord.Embed:
    event_block = ""

    if member:
        event_block = _event_status_block(guild, member) + "\n\n"

    emb = discord.Embed(
        title="⚜️ Ebolus Kommandozentrale",
        description=(
            event_block +
            "Willkommen im privaten Gildenmenü.\n\n"
            "Wähle unten im Menü einen Bereich aus.\n"
            "Die Beschreibung steht direkt in der Auswahl."
        ),
        color=discord.Color.gold()
    )

    emb.set_footer(text=f"Server: {guild.name} • Ebolus Gildenbot")

    return emb


def _profile_embed(guild: discord.Guild, member: discord.Member) -> discord.Embed:
    p = _user_profile(guild.id, member.id)

    ingame = p.get("ingame_name") or _display_name(member)
    main_role = p.get("main_role") or "Nicht gesetzt"
    gearscore = p.get("gearscore") or "Nicht gesetzt"

    emb = discord.Embed(
        title="👤 Dein Gildenprofil",
        description=f"Profil von **{ingame}**",
        color=discord.Color.gold()
    )

    emb.add_field(name="🎮 Ingame-Name", value=str(ingame), inline=False)
    emb.add_field(name="⚔️ Main-Rolle", value=str(main_role), inline=True)
    emb.add_field(name="💠 Gearscore", value=str(gearscore), inline=True)
    emb.add_field(name="🏰 Rang", value=_member_position(guild, member), inline=True)
    emb.add_field(
        name="📆 In der Gilde seit",
        value=f"{_guild_join_date(member)}\n{_guild_days(member)} Tage",
        inline=True
    )
    emb.add_field(name="🟢 Status", value=_status_for_user(guild.id, member.id), inline=False)

    emb.set_footer(text="Bearbeitbar: Ingame-Name, Main-Rolle, Gearscore")

    return emb


def _events_embed(guild_id: int) -> discord.Embed:
    c = _gcfg(guild_id)
    events = c.get("events") or []

    emb = discord.Embed(
        title="📅 Ebolus Gildenkalender",
        color=discord.Color.gold()
    )

    if not events:
        emb.description = "Aktuell sind keine festen Gilden-Events hinterlegt."
        return emb

    lines = []

    for e in events:
        weekday = str(e.get("weekday", "—"))
        time = str(e.get("time", "—"))
        title = str(e.get("title", "Event"))

        icon = "📌"
        title_l = title.lower()

        if "gildenboss" in title_l or "boss" in title_l:
            icon = "🔥"
        elif "raid" in title_l:
            icon = "⚔️"
        elif "fenrir" in title_l:
            icon = "🐺"

        lines.append(f"{icon} **{weekday}**\n{time} Uhr – {title}")

    emb.description = "\n\n".join(lines)
    emb.set_footer(text="Reine Übersicht. Keine Anmeldung / keine RSVP-Funktion.")

    return emb


def _member_sort_key(guild: discord.Guild, member: discord.Member) -> Tuple[int, int, str]:
    p = _user_profile(guild.id, member.id)
    position = _member_position(guild, member)
    rank = _position_rank(position)
    gs = _parse_gearscore(p.get("gearscore"))
    return (rank, -gs, _display_name(member).lower())


def _members_list_embed(guild: discord.Guild) -> discord.Embed:
    emb = discord.Embed(
        title="👥 Ebolus Mitglieder",
        color=discord.Color.gold()
    )

    c = _gcfg(guild.id)
    member_role_id = int(c.get("member_role_id", 0) or 0)

    if not member_role_id:
        emb.description = "❌ Keine Ebolus-/Gildenmitglied-Rolle gesetzt. Nutze zuerst `/portal_set_member_role`."
        return emb

    member_role = guild.get_role(member_role_id)

    if not member_role:
        emb.description = "❌ Die gespeicherte Ebolus-/Gildenmitglied-Rolle wurde nicht gefunden."
        return emb

    members = [m for m in member_role.members if not m.bot]
    members.sort(key=lambda m: _member_sort_key(guild, m))

    lines = []

    for i, m in enumerate(members[:40], start=1):
        p = _user_profile(guild.id, m.id)

        name = p.get("ingame_name") or _display_name(m)
        pos = _member_position(guild, m)
        gs = p.get("gearscore") or "—"

        lines.append(f"**{i}. {name}** — {pos} — GS {gs}")

    if not lines:
        emb.description = "Keine Mitglieder mit der Ebolus-/Gildenmitglied-Rolle gefunden."
    else:
        emb.description = "\n".join(lines)

    if len(members) > 40:
        emb.set_footer(text=f"Anzeige begrenzt auf 40 von {len(members)} Mitgliedern. Sortierung: Rang, dann Gearscore.")
    else:
        emb.set_footer(text="Angezeigt werden nur Mitglieder mit der Ebolus-/Gildenmitglied-Rolle.")

    return emb


def _absence_calendar_embed(guild: discord.Guild) -> discord.Embed:
    g = _gdata(guild.id)
    absences = g.get("absences") or {}
    users = g.get("users") or {}

    emb = discord.Embed(
        title="🏖️ Abwesenheitskalender",
        color=discord.Color.gold()
    )

    today = datetime.now(TZ).date()
    running_rows = []
    upcoming_rows = []

    c = _gcfg(guild.id)
    member_role_id = int(c.get("member_role_id", 0) or 0)
    member_role = guild.get_role(member_role_id) if member_role_id else None

    for uid_str, absence in absences.items():
        try:
            uid = int(uid_str)
        except Exception:
            continue

        member = guild.get_member(uid)

        if not member or member.bot:
            continue

        if member_role and member_role not in member.roles:
            continue

        if not isinstance(absence, dict):
            continue

        dates = _absence_dates(absence)

        if not dates:
            continue

        from_d, to_d = dates

        # Abgelaufene Abwesenheiten komplett ausblenden.
        if to_d < today:
            continue

        p = users.get(str(uid)) or {}
        name = p.get("ingame_name") or _display_name(member)

        from_s = str(absence.get("from", "—"))
        to_s = str(absence.get("to", "—"))
        reason = str(absence.get("reason", "—")).strip() or "—"

        if from_d <= today <= to_d:
            line = f"🟠 **{name}** — {from_s} bis {to_s}\nGrund: {reason}"
            running_rows.append((to_d, from_d, line))
        else:
            line = f"⚪ **{name}** — {from_s} bis {to_s}\nGrund: {reason}"
            upcoming_rows.append((from_d, to_d, line))

    # Laufende zuerst nach Enddatum: wer zuerst wieder da ist, steht oben.
    running_rows.sort(key=lambda x: (x[0], x[1]))

    # Bevorstehende nach Startdatum: wer zuerst weg ist, steht oben.
    upcoming_rows.sort(key=lambda x: (x[0], x[1]))

    parts = []

    if running_rows:
        parts.append(
            "**🟠 Laufende Abwesenheiten**\n" +
            "\n\n".join(row[2] for row in running_rows[:15])
        )

    if upcoming_rows:
        parts.append(
            "**⚪ Bevorstehende Abwesenheiten**\n" +
            "\n\n".join(row[2] for row in upcoming_rows[:15])
        )

    if not parts:
        emb.description = "Aktuell sind keine laufenden oder kommenden Abwesenheiten eingetragen."
    else:
        emb.description = "\n\n".join(parts)

    total = len(running_rows) + len(upcoming_rows)

    if total > 30:
        emb.set_footer(text=f"Anzeige begrenzt auf 30 von {total} Abwesenheiten. 🟠 läuft aktuell • ⚪ kommt noch")
    else:
        emb.set_footer(text="🟠 läuft aktuell • ⚪ kommt noch")

    return emb

def _dm_settings_embed(guild: discord.Guild, member: discord.Member) -> discord.Embed:
    enabled = True

    if is_dm_enabled is not None:
        try:
            enabled = is_dm_enabled(guild.id, member.id)
        except Exception:
            enabled = True

    emb = discord.Embed(
        title="📬 Raid-/Event-DMs",
        description=(
            f"Aktueller Status: **{'AN' if enabled else 'AUS'}**\n\n"
            "Wenn DMs aktiviert sind, bekommst du bei neuen Raid-/Event-Anmeldungen eine Privatnachricht.\n\n"
            "Wenn DMs deaktiviert sind, kannst du trotzdem direkt unter der Raid-Ankündigung im Server abstimmen."
        ),
        color=discord.Color.gold()
    )

    emb.set_footer(text="Diese Einstellung betrifft nur Raid-/Event-DMs, nicht das Gildenmenü.")

    return emb


def _rules_loot_embed() -> discord.Embed:
    emb = discord.Embed(
        title="📜 Regeln & Lootsystem",
        color=discord.Color.gold()
    )

    emb.add_field(
        name="📌 Gildenregeln",
        value=(
            "• Bei Events im Discord an- oder abmelden.\n"
            "• Voice ist bei wichtigen Gildenterminen erwünscht.\n"
            "• Bei längerer Abwesenheit bitte im Bot abmelden.\n"
            "• Nach 9 Tagen ohne Abmeldung/Reaktion kann ein Ausschluss aus der Gilde erfolgen.\n"
            "• Lootsystem beachten."
        ),
        inline=False
    )

    emb.add_field(
        name="🎁 ERZ & Weltbosse",
        value=(
            "• Wenn der Empfänger den Drop für seine Hauptwaffe braucht: Item behalten.\n"
            "• Wenn nicht: Gildenverkauf für 25% des Auktionshauspreises.\n"
            "• Erlös geht an die Dropgruppe.\n"
            "• Wenn keiner aus der Gilde kaufen will: nach 10 Tagen ins Auktionshaus.\n"
            "• Erlös geht ebenfalls an die Dropgruppe.\n"
            "• Wichtig: Needliste vor dem Drop eintragen."
        ),
        inline=False
    )

    emb.add_field(
        name="🧱 Erzboss-Materialien",
        value=(
            "• Gildenauktion für 50% des Auktionshauspreises.\n"
            "• Keine Needliste notwendig.\n"
            "• Wenn nach 10 Tagen kein Kauf erfolgt: Verkauf im Auktionshaus.\n"
            "• Erlös geht an die Dropgruppe."
        ),
        inline=False
    )

    emb.set_footer(text="Kurzfassung. Im Zweifel entscheidet die Gildenleitung.")

    return emb


async def _fetch_portal_message(client: discord.Client, guild_id: int, user_id: int) -> Optional[discord.Message]:
    mid = _portal_message_id(guild_id, user_id)

    if not mid:
        return None

    user = client.get_user(user_id)

    if user is None:
        try:
            user = await client.fetch_user(user_id)
        except Exception:
            return None

    try:
        dm = user.dm_channel or await user.create_dm()
        msg = await dm.fetch_message(mid)
        return msg
    except Exception:
        return None


async def _send_new_portal_menu(user: discord.abc.User, guild: discord.Guild) -> Optional[discord.Message]:
    member = guild.get_member(user.id)

    try:
        msg = await user.send(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())
        _mark_portal_sent(guild.id, user.id, msg.id)
        return msg
    except Exception:
        return None


async def ensure_portal_menu_for_user(
    client: discord.Client,
    guild_id: int,
    user_id: int,
    force_view: str = "main"
) -> bool:
    guild = client.get_guild(guild_id)

    if not guild:
        return False

    member = guild.get_member(user_id)

    if not member:
        try:
            member = await guild.fetch_member(user_id)
        except Exception:
            member = None

    if not member or member.bot:
        return False

    msg = await _fetch_portal_message(client, guild_id, user_id)

    try:
        if msg:
            if force_view == "profile":
                await msg.edit(embed=_profile_embed(guild, member), view=ProfileView())
            elif force_view == "events":
                await msg.edit(embed=_events_embed(guild_id), view=EventsInfoView())
            elif force_view == "members":
                await msg.edit(embed=_members_list_embed(guild), view=BackOnlyView())
            elif force_view == "absences":
                await msg.edit(embed=_absence_calendar_embed(guild), view=AbsenceCalendarView())
            elif force_view == "dm_settings":
                await msg.edit(embed=_dm_settings_embed(guild, member), view=DmSettingsView())
            else:
                await msg.edit(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())

            _mark_portal_sent(guild_id, user_id, msg.id)
            return True

        sent = await _send_new_portal_menu(member, guild)
        return sent is not None

    except Exception:
        sent = await _send_new_portal_menu(member, guild)
        return sent is not None


async def _send_main_menu_to_member(member: discord.Member, force: bool = False) -> bool:
    if member.bot:
        return False

    if not force and _portal_was_sent(member.guild.id, member.id):
        ok = await ensure_portal_menu_for_user(member._state._get_client(), member.guild.id, member.id)
        return ok

    return await ensure_portal_menu_for_user(member._state._get_client(), member.guild.id, member.id)


def _member_has_member_role(member: discord.Member) -> bool:
    c = _gcfg(member.guild.id)
    role_id = int(c.get("member_role_id", 0) or 0)

    if not role_id:
        return False

    role = member.guild.get_role(role_id)
    return bool(role and role in member.roles)


async def _delete_old_bot_dms_for_member(
    client: discord.Client,
    member: discord.Member,
    limit: int = 300
) -> int:
    if member.bot:
        return 0

    if client.user is None:
        return 0

    active_menu_id = _portal_message_id(member.guild.id, member.id)

    protected_titles = {
        "⚜️ Ebolus Kommandozentrale",
        "🏰 ebolus – Gildenmenü",
        "👤 Dein Gildenprofil",
        "📅 Ebolus Gildenkalender",
        "📅 Gildenkalender – ebolus",
        "📅 Gilden-Events",
        "🏖️ Abwesenheitskalender",
        "📬 Raid-/Event-DMs",
        "❓ Hilfe – Ebolus Gildenbot",
        "❓ Hilfe – ebolus Gildenbot",
        "👥 Ebolus Mitglieder",
        "👥 Mitgliederliste – ebolus",
        "📜 Regeln & Lootsystem",
        "📜 Regeln & Lootsystem – ebolus",
        "🎁 Needliste – ebolus",
        "👤 Persönlich",
        "🎁 Loot & Bedarf",
        "📅 Gilde",
        "🛡️ Kontakt & Hilfe",
        "🛡️ Admin",
        "📅 Admin – Event",
        "🎁 Admin – Loot",
    }

    deleted = 0

    try:
        dm = member.dm_channel or await member.create_dm()

        async for msg in dm.history(limit=limit):
            try:
                if msg.author.id != client.user.id:
                    continue

                if active_menu_id and msg.id == active_menu_id:
                    continue

                title = ""
                if msg.embeds:
                    title = msg.embeds[0].title or ""

                if title in protected_titles:
                    if not active_menu_id:
                        _mark_portal_sent(member.guild.id, member.id, msg.id)
                        active_menu_id = msg.id
                        continue

                    await msg.delete()
                    deleted += 1
                    await asyncio.sleep(0.08)
                    continue

                await msg.delete()
                deleted += 1
                await asyncio.sleep(0.08)

            except Exception:
                pass

    except Exception:
        pass

    return deleted


def _get_leader_cfg(guild_id: int) -> dict:
    data = _load_json(LEADER_CONTACT_CFG_FILE, {})
    c = data.get(str(guild_id)) or {}
    c.setdefault("internal_channel_id", 0)
    c.setdefault("leader_role_id", 0)
    return c


def _leader_status_view():
    try:
        from bot.leader_contact import LeaderStatusView  # type: ignore
        return LeaderStatusView()
    except ModuleNotFoundError:
        try:
            from leader_contact import LeaderStatusView  # type: ignore
            return LeaderStatusView()
        except Exception:
            return None
    except Exception:
        return None


async def _resolve_guild_member_from_inter(inter: discord.Interaction) -> tuple[Optional[discord.Guild], Optional[discord.Member]]:
    guild = _resolve_guild_for_user(inter.client, inter.user.id)

    if not guild:
        return None, None

    member = guild.get_member(inter.user.id)

    if not member:
        try:
            member = await guild.fetch_member(inter.user.id)
        except Exception:
            member = None

    return guild, member


class ProfileEditModal(Modal):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(title="Profil bearbeiten", timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id

        p = _user_profile(guild_id, user_id)

        self.ingame_name = TextInput(
            label="Ingame-Name",
            placeholder="z. B. Firetube",
            required=True,
            max_length=50,
            default=str(p.get("ingame_name") or "")
        )

        self.main_role = TextInput(
            label="Main-Rolle",
            placeholder="z. B. Heiler, Tank, DPS",
            required=True,
            max_length=50,
            default=str(p.get("main_role") or "")
        )

        self.gearscore = TextInput(
            label="Gearscore",
            placeholder="z. B. 4200",
            required=True,
            max_length=10,
            default=str(p.get("gearscore") or "")
        )

        self.add_item(self.ingame_name)
        self.add_item(self.main_role)
        self.add_item(self.gearscore)

    async def on_submit(self, inter: discord.Interaction):
        gs_raw = str(self.gearscore.value).strip()
        gs = _parse_gearscore(gs_raw)

        if gs <= 0:
            await inter.response.send_message("❌ Gearscore ungültig. Bitte nur Zahlen eintragen, z. B. `4200`.")
            return

        p = _user_profile(self.guild_id, self.user_id)
        p["ingame_name"] = str(self.ingame_name.value).strip()
        p["main_role"] = str(self.main_role.value).strip()
        p["gearscore"] = str(gs)

        save_profiles()

        try:
            await inter.response.defer()
        except Exception:
            pass

        await ensure_portal_menu_for_user(inter.client, self.guild_id, self.user_id, force_view="profile")


class AbsenceModal(Modal):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(title="Abwesenheit melden", timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id

        self.from_date = TextInput(
            label="Abwesend von (TT-MM)",
            placeholder="z. B. 15-05",
            required=True,
            max_length=5
        )

        self.to_date = TextInput(
            label="Abwesend bis (TT-MM)",
            placeholder="z. B. 18-05",
            required=True,
            max_length=5
        )

        self.reason = TextInput(
            label="Grund / Hinweis",
            placeholder="z. B. Spätschicht / Urlaub / privat",
            required=True,
            max_length=800,
            style=discord.TextStyle.paragraph
        )

        self.add_item(self.from_date)
        self.add_item(self.to_date)
        self.add_item(self.reason)

    async def on_submit(self, inter: discord.Interaction):
        from_s = str(self.from_date.value).strip()
        to_s = str(self.to_date.value).strip()
        reason_s = str(self.reason.value).strip()

        if not _valid_ddmm(from_s) or not _valid_ddmm(to_s):
            await inter.response.send_message("❌ Datum ungültig. Bitte im Format `TT-MM` eintragen, z. B. `15-05`.")
            return

        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        member = guild.get_member(self.user_id)

        if not member:
            try:
                member = await guild.fetch_member(self.user_id)
            except Exception:
                member = None

        if not member:
            await inter.response.send_message("❌ Mitglied nicht gefunden.")
            return

        g = _gdata(self.guild_id)
        g["absences"][str(self.user_id)] = {
            "from": from_s,
            "to": to_s,
            "reason": reason_s,
            "created_at": datetime.now(TZ).isoformat()
        }

        save_profiles()

        p = _user_profile(self.guild_id, self.user_id)
        ingame = p.get("ingame_name") or _display_name(member)

        c = _gcfg(self.guild_id)
        ch_id = int(c.get("absence_channel_id", 0) or 0)
        ch = guild.get_channel(ch_id) if ch_id else None

        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            emb = discord.Embed(
                title="🏖️ Abwesenheit gemeldet",
                description=(
                    f"**{ingame}** ist abwesend von **{from_s}** bis **{to_s}**.\n\n"
                    f"**Grund:**\n{reason_s}"
                ),
                color=discord.Color.gold(),
                timestamp=datetime.now(TZ)
            )

            emb.set_footer(text=f"Gemeldet von {_display_name(member)}")

            try:
                await ch.send(embed=emb)
            except Exception:
                pass

        try:
            await inter.response.defer()
        except Exception:
            pass

        await ensure_portal_menu_for_user(inter.client, self.guild_id, self.user_id, force_view="main")


class PortalLeaderContactModal(Modal):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(title="Leader kontaktieren", timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id

        self.topic = TextInput(
            label="Thema",
            placeholder="z. B. Frage, Beschwerde, Hilfe, Problem",
            required=True,
            max_length=120
        )

        self.message = TextInput(
            label="Nachricht",
            placeholder="Schreib hier dein Anliegen rein.",
            required=True,
            max_length=1500,
            style=discord.TextStyle.paragraph
        )

        self.add_item(self.topic)
        self.add_item(self.message)

    async def on_submit(self, inter: discord.Interaction):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        member = guild.get_member(self.user_id)

        if not member:
            try:
                member = await guild.fetch_member(self.user_id)
            except Exception:
                member = None

        if not member:
            await inter.response.send_message("❌ Mitglied nicht gefunden.")
            return

        leader_cfg = _get_leader_cfg(self.guild_id)
        internal_channel_id = int(leader_cfg.get("internal_channel_id", 0) or 0)
        leader_role_id = int(leader_cfg.get("leader_role_id", 0) or 0)

        internal_ch = guild.get_channel(internal_channel_id) if internal_channel_id else None
        leader_role = guild.get_role(leader_role_id) if leader_role_id else None

        if not isinstance(internal_ch, (discord.TextChannel, discord.Thread)):
            await inter.response.send_message("❌ Leader-Kontakt ist noch nicht eingerichtet. Es fehlt der interne Leader-Kanal.")
            return

        topic = _safe_text(str(self.topic.value))
        msg = _safe_text(str(self.message.value))

        p = _user_profile(self.guild_id, self.user_id)
        ingame = p.get("ingame_name") or _display_name(member)

        emb = discord.Embed(
            title="📨 Neue Leader-Anfrage",
            color=discord.Color.gold(),
            timestamp=datetime.now(TZ)
        )

        emb.add_field(name="Von", value=f"{ingame} ({_display_name(member)})", inline=False)
        emb.add_field(name="Thema", value=topic or "—", inline=False)
        emb.add_field(name="Nachricht", value=msg or "—", inline=False)
        emb.add_field(name="Status", value="🆕 Offen", inline=False)
        emb.set_footer(text=f"{datetime.now(TZ).strftime('%d.%m.%Y %H:%M')} (Europe/Berlin)")

        try:
            await internal_ch.send(
                content=leader_role.mention if leader_role else None,
                embed=emb,
                view=_leader_status_view()
            )
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Anfrage nicht senden: {e}")
            return

        try:
            await inter.response.defer()
        except Exception:
            pass

        await ensure_portal_menu_for_user(inter.client, self.guild_id, self.user_id, force_view="main")


class PortalOpenView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="⚜️ Gildenmenü im Privatchat öffnen", style=ButtonStyle.secondary, custom_id="portal_open_dm")
    async def btn_open_dm(self, inter: discord.Interaction, _):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        ok = await ensure_portal_menu_for_user(inter.client, inter.guild.id, inter.user.id, force_view="main")

        if ok:
            await inter.response.send_message("✅ Ich habe dein Gildenmenü im Privatchat geöffnet oder aktualisiert.", ephemeral=True)
        else:
            await inter.response.send_message(
                "❌ Konnte dir keine Privatnachricht schicken. Prüfe deine Discord-DM-Einstellungen.",
                ephemeral=True
            )


class PortalMainSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label="Persönlich",
                value="personal",
                description="Profil, Gearscore, Abwesenheit und Raid-DMs",
                emoji="👤"
            ),
            discord.SelectOption(
                label="Loot & Bedarf",
                value="loot",
                description="Needliste für Main/Secondary und Lootregeln",
                emoji="🎁"
            ),
            discord.SelectOption(
                label="Gilde",
                value="guild",
                description="Kalender, Abwesenheiten und Mitgliederübersicht",
                emoji="📅"
            ),
            discord.SelectOption(
                label="Kontakt & Hilfe",
                value="support",
                description="Leader kontaktieren oder Hilfe zum Bot öffnen",
                emoji="🛡️"
            ),
            discord.SelectOption(
                label="Admin",
                value="admin",
                description="Event- und Loot-Verwaltung für Leitung",
                emoji="⚙️"
            ),
        ]

        super().__init__(
            placeholder="Bereich auswählen …",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="portal_main_select"
        )

    async def callback(self, inter: discord.Interaction):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        choice = self.values[0]

        if choice == "personal":
            enabled = True

            if is_dm_enabled is not None:
                try:
                    enabled = is_dm_enabled(guild.id, member.id)
                except Exception:
                    enabled = True

            emb = discord.Embed(
                title="👤 Persönlich",
                description=(
                    "Hier findest du alles, was dein eigenes Profil und deine Anwesenheit betrifft.\n\n"
                    "**Profil**\n"
                    "Ingame-Name, Main-Rolle und Gearscore ansehen oder bearbeiten.\n\n"
                    "**Abwesenheit**\n"
                    "Urlaub, Schicht, Pause oder längere Inaktivität melden.\n\n"
                    f"**Raid-/Event-DMs**\n"
                    f"Aktueller Status: **{'AN' if enabled else 'AUS'}**"
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(embed=emb, view=PersonalMenuView())
            return

        if choice == "loot":
            emb = discord.Embed(
                title="🎁 Loot & Bedarf",
                description=(
                    "Hier verwaltest du alles rund um Items und Loot.\n\n"
                    "**Needliste**\n"
                    "Trage ein, welche Items du für Main und Secondary brauchst.\n\n"
                    "**Regeln & Loot**\n"
                    "Zeigt die wichtigsten Regeln zum Lootsystem."
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(embed=emb, view=LootMenuView())
            return

        if choice == "guild":
            emb = discord.Embed(
                title="📅 Gilde",
                description=(
                    "Hier findest du die wichtigsten Gildenübersichten.\n\n"
                    "**Kalender**\n"
                    "Feste Gildentermine und regelmäßige Events.\n\n"
                    "**Abwesenheiten**\n"
                    "Übersicht aktueller und kommender Abwesenheiten.\n\n"
                    "**Mitglieder**\n"
                    "Übersicht der Ebolus-Mitglieder mit Rang und Gearscore."
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(embed=emb, view=GuildMenuView())
            return

        if choice == "support":
            emb = discord.Embed(
                title="🛡️ Kontakt & Hilfe",
                description=(
                    "Hier kannst du die Gildenleitung erreichen oder Hilfe zum Bot lesen.\n\n"
                    "**Leaderkontakt**\n"
                    "Sende eine Anfrage direkt an die Gildenleitung.\n\n"
                    "**Hilfe**\n"
                    "Kurze Erklärung der Bot-Funktionen."
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(embed=emb, view=SupportMenuView())
            return


        if choice == "admin":
            if not _is_portal_admin(guild, member):
                await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
                return

            emb = discord.Embed(
                title="🛡️ Admin",
                description=(
                    "Interner Verwaltungsbereich für die Gildenleitung.\n\n"
                    "**Event**\n"
                    "Raids erstellen, Allianz-Raids erstellen, Events löschen und fehlende Abstimmungen erneut senden.\n\n"
                    "**Loot**\n"
                    "Items hinzufügen, Loot-Drops melden, Items als erhalten markieren und Katalog anzeigen."
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(embed=emb, view=AdminMenuView())
            return


class MemberPortalMainView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(PortalMainSelect())


class PersonalMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="👤 Profil", style=ButtonStyle.secondary, custom_id="portal_personal_profile")
    async def btn_profile(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_profile_embed(guild, member), view=ProfileView())

    @button(label="🏖️ Abwesenheit melden", style=ButtonStyle.secondary, custom_id="portal_personal_absence")
    async def btn_absence(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(AbsenceModal(guild.id, inter.user.id))

    @button(label="📬 Raid-DMs", style=ButtonStyle.secondary, custom_id="portal_personal_dm_settings")
    async def btn_dm_settings(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_dm_settings_embed(guild, member), view=DmSettingsView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_personal_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class DmSettingsView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="📬 DMs an/aus schalten", style=ButtonStyle.secondary, custom_id="portal_dm_toggle")
    async def btn_toggle(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if set_dm_pref is None or is_dm_enabled is None:
            await inter.response.send_message("❌ DM-Einstellungssystem ist nicht geladen.")
            return

        current = is_dm_enabled(guild.id, member.id)
        set_dm_pref(guild.id, member.id, not current)

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_dm_settings_embed(guild, member), view=DmSettingsView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_dm_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        enabled = True

        if is_dm_enabled is not None and guild and member:
            try:
                enabled = is_dm_enabled(guild.id, member.id)
            except Exception:
                enabled = True

        emb = discord.Embed(
            title="👤 Persönlich",
            description=(
                "Hier findest du alles, was dein eigenes Profil und deine Anwesenheit betrifft.\n\n"
                "**Profil**\n"
                "Ingame-Name, Main-Rolle und Gearscore ansehen oder bearbeiten.\n\n"
                "**Abwesenheit**\n"
                "Urlaub, Schicht, Pause oder längere Inaktivität melden.\n\n"
                f"**Raid-/Event-DMs**\n"
                f"Aktueller Status: **{'AN' if enabled else 'AUS'}**"
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=PersonalMenuView())


class LootMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🎁 Needliste", style=ButtonStyle.secondary, custom_id="portal_loot_needlist")
    async def btn_needlist(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        try:
            try:
                from bot.loot_needs import open_need_menu  # type: ignore
            except ModuleNotFoundError:
                from loot_needs import open_need_menu  # type: ignore

            await open_need_menu(inter, guild.id, member.id)

        except Exception:
            emb = discord.Embed(
                title="🎁 Needliste – ebolus",
                description="Die Needliste ist noch nicht aktiv. Das Modul `loot_needs.py` muss noch eingebaut werden.",
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=emb, view=BackOnlyView())

    @button(label="📜 Regeln & Loot", style=ButtonStyle.secondary, custom_id="portal_loot_rules")
    async def btn_rules(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_rules_loot_embed(), view=RulesLootView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_loot_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class GuildMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="📅 Kalender", style=ButtonStyle.secondary, custom_id="portal_guild_calendar")
    async def btn_calendar(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_events_embed(guild.id), view=EventsInfoView())

    @button(label="🏖️ Abwesenheiten", style=ButtonStyle.secondary, custom_id="portal_guild_absences")
    async def btn_absences(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_absence_calendar_embed(guild), view=AbsenceCalendarView())

    @button(label="👥 Mitglieder", style=ButtonStyle.secondary, custom_id="portal_guild_members")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_members_list_embed(guild), view=BackOnlyView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_guild_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class AbsenceCalendarView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🔄 Aktualisieren", style=ButtonStyle.secondary, custom_id="portal_absence_calendar_refresh")
    async def btn_refresh(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_absence_calendar_embed(guild), view=AbsenceCalendarView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_absence_calendar_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title="📅 Gilde",
            description=(
                "Hier findest du die wichtigsten Gildenübersichten.\n\n"
                "**Kalender**\n"
                "Feste Gildentermine und regelmäßige Events.\n\n"
                "**Abwesenheiten**\n"
                "Übersicht aktueller und kommender Abwesenheiten.\n\n"
                "**Mitglieder**\n"
                "Übersicht der Ebolus-Mitglieder mit Rang und Gearscore."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=GuildMenuView())




def _admin_parse_id(value: str) -> int:
    raw = str(value or "").strip()
    digits = re.sub(r"[^0-9]", "", raw)
    try:
        return int(digits) if digits else 0
    except Exception:
        return 0


def _admin_parse_event_date(value: str) -> tuple[int, int, int]:
    raw = str(value or "").strip()
    if "." in raw:
        dd, mm, yyyy = [int(x) for x in raw.split(".")]
        return yyyy, mm, dd
    yyyy, mm, dd = [int(x) for x in raw.split("-")]
    return yyyy, mm, dd


def _admin_event_module():
    try:
        import bot.event_rsvp_dm as rsvp_mod  # type: ignore
    except ModuleNotFoundError:
        import event_rsvp_dm as rsvp_mod  # type: ignore
    return rsvp_mod


EVENT_IMAGE_PRESETS = {
    "Normal Raid": "https://media.discordapp.net/attachments/1488142284812714085/1516086614957494312/282b2b20-5a8f-4251-b038-15fde2ac723d.png?ex=6a315d30&is=6a300bb0&hm=767b9ad51564019a71be77906c480350e29137f24e08b6abd99f67a9c9edad33&=&format=webp&quality=lossless",
    "Hard Raid": "https://media.discordapp.net/attachments/1488142284812714085/1513816935832228033/7225f274-cc4f-4eda-ba74-ca401f4e572b.png?ex=6a310462&is=6a2fb2e2&hm=9aa88c9c5b45f6eea14ec33541344421b7d467b3b5969f2c8d7faeebb3b30df2&=&format=webp&quality=lossless",
    "Nightmare": "https://media.discordapp.net/attachments/1488142284812714085/1513816992358858842/d6ee8bc1-432a-4d28-914d-31be80adf835.png?ex=6a310470&is=6a2fb2f0&hm=77fbec16dae3b00858a4dd20000eec86150d99de8823aab5acc7a3189f39092c&=&format=webp&quality=lossless",
    "Trials": "https://media.discordapp.net/attachments/1488142284812714085/1491660359952502825/file_000000007dcc7246bb6e57ae41860769.png?ex=6a30d4f7&is=6a2f8377&hm=40ae17883015fa630db3155e0d922cdfbf8fea9ca88a43a0b20a51d6852a9e64&=&format=webp&quality=lossless&width=1440&height=960",
    "PvP": "https://media.discordapp.net/attachments/1488142284812714085/1513202292302811186/1780845919107.png?ex=6a30c234&is=6a2f70b4&hm=eb19a0dbc88e29a962ba726adc39f397f6240652dfd5b377a87c74b311f680b5&=&format=webp&quality=lossless",
}


def _admin_active_rsvp_events(guild: Optional[discord.Guild]) -> list[tuple[str, dict]]:
    if guild is None:
        return []

    try:
        rsvp = _admin_event_module()
        now = datetime.now(rsvp.TZ)
        out = []
        for msg_id, obj in list((getattr(rsvp, "store", {}) or {}).items()):
            try:
                if int(obj.get("guild_id", 0) or 0) != int(guild.id):
                    continue
                when = datetime.fromisoformat(str(obj.get("when_iso", "")))
                if when < now:
                    continue
                out.append((str(msg_id), obj))
            except Exception:
                continue
        out.sort(key=lambda pair: datetime.fromisoformat(str(pair[1].get("when_iso", ""))))
        return out[:25]
    except Exception:
        return []


async def _admin_create_regular_raid_from_menu(
    inter: discord.Interaction,
    guild_id: int,
    title: str,
    date_text: str,
    time_text: str,
    channel_text: str,
    description: str,
    image_url: str | None = None,
):
    rsvp = _admin_event_module()
    guild = inter.client.get_guild(int(guild_id))
    if not guild:
        await inter.followup.send("❌ Server nicht gefunden.", ephemeral=True)
        return

    if not _is_portal_admin(guild, guild.get_member(inter.user.id)):
        await inter.followup.send("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
        return

    try:
        yyyy, mm, dd = _admin_parse_event_date(date_text)
        hh, mi = [int(x) for x in str(time_text).strip().split(":")]
        when = datetime(yyyy, mm, dd, hh, mi, tzinfo=rsvp.TZ)
    except Exception:
        await inter.followup.send("❌ Datum/Zeit ungültig. Nutze z. B. `2026-06-20` oder `20.06.2026` und `20:30`.", ephemeral=True)
        return

    channel_id = _admin_parse_id(channel_text)
    ch = guild.get_channel(channel_id) if channel_id else None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        await inter.followup.send("❌ Zielkanal nicht gefunden. Kopiere die Channel-ID oder nutze eine Channel-Erwähnung wie `<#123>`.", ephemeral=True)
        return

    obj = {
        "guild_id": int(guild.id),
        "channel_id": int(ch.id),
        "title": str(title).strip(),
        "description": str(description or "").strip(),
        "when_iso": when.isoformat(),
        "image_url": str(image_url or "").strip() or None,
        "yes": {"TANK": [], "HEAL": [], "DPS": [], "BANK": []},
        "maybe": {},
        "no": [],
        "target_role_id": 0,
        "dm_messages": {},
    }

    emb = rsvp.build_embed(guild, obj)
    msg = await ch.send(embed=emb)
    rsvp.store[str(msg.id)] = obj
    rsvp.save_store()

    try:
        await msg.edit(view=rsvp.ServerRaidView(int(msg.id)))
    except Exception:
        pass

    sent = 0
    skipped_opt_out = 0
    for target in rsvp._eligible_members(guild, obj):
        try:
            if not rsvp.is_dm_enabled(guild.id, target.id):
                skipped_opt_out += 1
                continue
            dm_text = rsvp._format_dm_text(
                title=str(title).strip(),
                when=when,
                channel_name_or_ref=f"Übersicht im Server: #{getattr(ch, 'name', 'Event')}",
                description=str(description or "").strip(),
                intro_line="Wähle unten deine Teilnahme:",
            )
            dm_msg = await target.send(dm_text, view=rsvp.RaidView(int(msg.id)))
            obj["dm_messages"][str(target.id)] = int(dm_msg.id)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    rsvp.save_store()

    try:
        rsvp._schedule_portal_refresh_for_event(inter.client, guild, obj)
    except Exception:
        pass

    await inter.followup.send(
        f"✅ Event erstellt: {msg.jump_url}\n"
        f"✉️ DMs versendet: **{sent}**\n"
        f"🔕 Opt-out übersprungen: **{skipped_opt_out}**",
        ephemeral=True
    )


async def _admin_resend_missing_from_menu(inter: discord.Interaction, guild_id: int, message_id: str):
    rsvp = _admin_event_module()
    guild = inter.client.get_guild(int(guild_id))
    if not guild:
        await inter.followup.send("❌ Server nicht gefunden.", ephemeral=True)
        return

    obj = rsvp.store.get(str(message_id))
    if not obj or int(obj.get("guild_id", 0) or 0) != int(guild.id):
        await inter.followup.send("❌ Event nicht gefunden.", ephemeral=True)
        return

    rsvp._init_event_shape(obj)
    when = datetime.fromisoformat(obj["when_iso"])
    already = rsvp._voters_set(obj)
    targets = [m for m in rsvp._eligible_members(guild, obj) if m.id not in already]

    sent = 0
    skipped_opt_out = 0
    for target in targets:
        try:
            if not rsvp.is_dm_enabled(guild.id, target.id):
                skipped_opt_out += 1
                continue
            dm_text = rsvp._format_dm_text(
                title=str(obj.get("title", "Event")),
                when=when,
                channel_name_or_ref=f"Übersicht: <#{obj.get('channel_id')}>",
                description=obj.get("description"),
                intro_line="Du hast noch nicht abgestimmt:",
            )
            dm_msg = await target.send(dm_text, view=rsvp.RaidView(int(message_id)))
            obj.setdefault("dm_messages", {})[str(target.id)] = int(dm_msg.id)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    rsvp.save_store()
    await inter.followup.send(f"✅ Resend an **{sent}** Nutzer.\n🔕 Opt-out übersprungen: **{skipped_opt_out}**", ephemeral=True)


async def _admin_delete_event_from_menu(inter: discord.Interaction, guild_id: int, message_id: str):
    rsvp = _admin_event_module()
    guild = inter.client.get_guild(int(guild_id))
    if not guild:
        await inter.followup.send("❌ Server nicht gefunden.", ephemeral=True)
        return

    obj = rsvp.store.get(str(message_id))
    if not obj or int(obj.get("guild_id", 0) or 0) != int(guild.id):
        await inter.followup.send("❌ Event nicht gefunden.", ephemeral=True)
        return

    rsvp._init_event_shape(obj)
    refresh_members = []
    try:
        refresh_members = list(rsvp._eligible_members(guild, obj))
    except Exception:
        refresh_members = []

    deleted_posts = 0
    failed_posts = []
    deleted_dms = 0

    if rsvp._is_alliance_event(obj) and obj.get("mirrors"):
        mirrors = list(obj.get("mirrors") or [])
    else:
        mirrors = [{"guild_id": guild.id, "channel_id": obj.get("channel_id"), "message_id": message_id, "label": guild.name}]

    for mirror in mirrors:
        try:
            mguild = inter.client.get_guild(int(mirror.get("guild_id", 0) or 0))
            if not mguild:
                failed_posts.append(f"{mirror.get('label', 'Unbekannt')} — Server nicht gefunden")
                continue
            ch = mguild.get_channel(int(mirror.get("channel_id", 0) or 0))
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                failed_posts.append(f"{mirror.get('label', mguild.name)} — Channel nicht gefunden")
                continue
            try:
                msg = await ch.fetch_message(int(mirror.get("message_id", message_id) or message_id))
                await msg.delete()
                deleted_posts += 1
            except Exception:
                failed_posts.append(f"{mirror.get('label', mguild.name)} — Post nicht gefunden oder keine Rechte")
            await asyncio.sleep(0.05)
        except Exception as e:
            failed_posts.append(str(e))

    for uid_str in list((obj.get("dm_messages") or {}).keys()):
        try:
            ok = await rsvp._delete_dm_message_for_user(inter.client, obj, int(uid_str))
            if ok:
                deleted_dms += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    rsvp.store.pop(str(message_id), None)
    rsvp.save_store()

    try:
        if refresh_members:
            asyncio.create_task(rsvp._refresh_existing_portals_for_members(inter.client, guild, refresh_members))
    except Exception:
        pass

    text = f"✅ Event gelöscht.\n🧾 Serverposts gelöscht: **{deleted_posts}**\n✉️ DMs gelöscht: **{deleted_dms}**"
    if failed_posts:
        text += "\n\n⚠️ Nicht gelöscht:\n" + "\n".join(f"• {x}" for x in failed_posts[:10])
    await inter.followup.send(text[:1900], ephemeral=True)


class AdminEventCreateModal(Modal):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(title="Event erstellen", timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.title_input = TextInput(label="Titel", placeholder="z. B. Gildenbosse", required=True, max_length=100)
        self.date_input = TextInput(label="Datum", placeholder="YYYY-MM-DD oder TT.MM.JJJJ", required=True, max_length=20)
        self.time_input = TextInput(label="Uhrzeit", placeholder="HH:MM", required=True, max_length=10)
        self.channel_input = TextInput(label="Zielkanal", placeholder="#raid-anmeldung oder Channel-ID", required=True, max_length=80)
        self.description_input = TextInput(label="Beschreibung", placeholder="Optional", required=False, style=discord.TextStyle.paragraph, max_length=800)
        self.add_item(self.title_input)
        self.add_item(self.date_input)
        self.add_item(self.time_input)
        self.add_item(self.channel_input)
        self.add_item(self.description_input)

    async def on_submit(self, inter: discord.Interaction):
        data = {
            "guild_id": self.guild_id,
            "user_id": self.user_id,
            "title": str(self.title_input.value),
            "date_text": str(self.date_input.value),
            "time_text": str(self.time_input.value),
            "channel_text": str(self.channel_input.value),
            "description": str(self.description_input.value or ""),
        }

        emb = discord.Embed(
            title="🖼️ Event-Bild wählen",
            description=(
                "Wähle, welches Bild für dieses Event verwendet werden soll.\n\n"
                "**Kein Bild** erstellt den Raid ohne Bild.\n"
                "**Eigene URL** öffnet danach ein Eingabefeld für deinen Bildlink."
            ),
            color=discord.Color.gold()
        )

        await inter.response.send_message(embed=emb, view=AdminEventImageSelectView(data), ephemeral=True)


class AdminEventImageSelectView(View):
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminEventImageSelect(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_image_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Das Event wurde nicht erstellt.",
                color=discord.Color.orange()
            ),
            view=None
        )


class AdminEventImageSelect(Select):
    def __init__(self, data: dict):
        self.data = dict(data)
        options = [
            discord.SelectOption(label="Kein Bild", value="none", description="Event ohne Bild erstellen"),
            discord.SelectOption(label="Eigene URL", value="custom", description="Eigenen Bildlink eingeben"),
            discord.SelectOption(label="Normal Raid", value="Normal Raid"),
            discord.SelectOption(label="Hard Raid", value="Hard Raid"),
            discord.SelectOption(label="Trials", value="Trials"),
            discord.SelectOption(label="Nightmare", value="Nightmare"),
            discord.SelectOption(label="PvP", value="PvP"),
        ]
        super().__init__(
            placeholder="Bildtyp wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="admin_event_image_select"
        )

    async def callback(self, inter: discord.Interaction):
        value = self.values[0]

        if value == "custom":
            await inter.response.send_modal(AdminEventCustomImageModal(self.data))
            return

        image_url = None if value == "none" else EVENT_IMAGE_PRESETS.get(value)
        await inter.response.defer(ephemeral=True, thinking=True)
        await _admin_create_regular_raid_from_menu(
            inter,
            int(self.data["guild_id"]),
            str(self.data["title"]),
            str(self.data["date_text"]),
            str(self.data["time_text"]),
            str(self.data["channel_text"]),
            str(self.data.get("description", "")),
            image_url=image_url,
        )


class AdminEventCustomImageModal(Modal):
    def __init__(self, data: dict):
        super().__init__(title="Eigene Event-Bild-URL", timeout=None)
        self.data = dict(data)
        self.url_input = TextInput(
            label="Bild-URL",
            placeholder="https://...",
            required=True,
            max_length=500
        )
        self.add_item(self.url_input)

    async def on_submit(self, inter: discord.Interaction):
        image_url = str(self.url_input.value or "").strip()

        if not (image_url.startswith("http://") or image_url.startswith("https://")):
            await inter.response.send_message("❌ Bitte eine gültige Bild-URL mit http:// oder https:// eingeben.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True, thinking=True)
        await _admin_create_regular_raid_from_menu(
            inter,
            int(self.data["guild_id"]),
            str(self.data["title"]),
            str(self.data["date_text"]),
            str(self.data["time_text"]),
            str(self.data["channel_text"]),
            str(self.data.get("description", "")),
            image_url=image_url,
        )


class AdminEventSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, events: list[tuple[str, dict]]):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.add_item(AdminEventSelect(guild_id, user_id, action, events))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_event_select_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="📅 Admin – Event", description="Wähle eine Event-Aktion.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventMenuView())


class AdminEventSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, events: list[tuple[str, dict]]):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        options = []
        for msg_id, obj in events[:25]:
            try:
                when = datetime.fromisoformat(str(obj.get("when_iso", "")))
                label = f"{when.strftime('%d.%m. %H:%M')} – {str(obj.get('title', 'Event'))}"[:100]
            except Exception:
                label = str(obj.get("title", "Event"))[:100]
            options.append(discord.SelectOption(label=label, value=str(msg_id), description=f"ID {msg_id}"[:100]))
        super().__init__(placeholder="Event wählen", min_values=1, max_values=1, options=options, custom_id=f"admin_event_select_{action}")

    async def callback(self, inter: discord.Interaction):
        msg_id = str(self.values[0])
        if self.action == "resend":
            await inter.response.defer(ephemeral=True, thinking=True)
            await _admin_resend_missing_from_menu(inter, self.guild_id, msg_id)
            return

        if self.action == "delete":
            emb = discord.Embed(
                title="🗑️ Event löschen – Bestätigung",
                description=f"Soll dieses Event wirklich gelöscht werden?\n\nMessage-ID: `{msg_id}`",
                color=discord.Color.orange()
            )
            await inter.response.edit_message(embed=emb, view=AdminEventDeleteConfirmView(self.guild_id, self.user_id, msg_id))
            return


class AdminEventDeleteConfirmView(View):
    def __init__(self, guild_id: int, user_id: int, message_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.message_id = str(message_id)

    @button(label="✅ Löschen", style=ButtonStyle.danger, custom_id="admin_event_delete_confirm")
    async def btn_confirm(self, inter: discord.Interaction, _):
        await inter.response.defer(ephemeral=True, thinking=True)
        await _admin_delete_event_from_menu(inter, self.guild_id, self.message_id)

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_delete_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="Abgebrochen", description="Das Event wurde nicht gelöscht.", color=discord.Color.orange())
        await inter.response.edit_message(embed=emb, view=AdminEventMenuView())


class AdminMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="📅 Event", style=ButtonStyle.secondary, custom_id="portal_admin_event")
    async def btn_event(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        emb = discord.Embed(
            title="📅 Admin – Event",
            description=(
                "Event-Verwaltung im Menü.\n\n"
                "Aktuell sind die bestehenden Slash-Commands weiterhin die sicherste Eingabeform:\n"
                "• `/raid_create_dm` – normalen Raid erstellen\n"
                "• `/alliance_raid_create` – Allianz-Raid erstellen\n"
                "• `/raid_delete` – Event löschen\n"
                "• `/alliance_raid_delete` – Allianz-Event löschen\n"
                "• `/raid_resend_missing` – fehlende Abstimmungen erneut senden\n\n"
                "Die vollständige Formular-Version bauen wir als nächsten Schritt, ohne die bestehenden Eventdaten anzufassen."
            ),
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminEventMenuView())

    @button(label="🎁 Loot", style=ButtonStyle.secondary, custom_id="portal_admin_loot")
    async def btn_loot(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        emb = discord.Embed(
            title="🎁 Admin – Loot",
            description="Wähle eine Loot-Aktion.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminLootMenuView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_admin_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class AdminEventMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="📅 Event erstellen", style=ButtonStyle.secondary, custom_id="portal_admin_event_create", row=0)
    async def btn_create(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        await inter.response.send_modal(AdminEventCreateModal(guild.id, member.id))

    @button(label="🌐 Allianz-Event", style=ButtonStyle.secondary, custom_id="portal_admin_event_alliance", row=0)
    async def btn_alliance(self, inter: discord.Interaction, _):
        await inter.response.send_message(
            "🌐 Allianz-Events bleiben vorerst über `/alliance_raid_create`, weil dort Allianz-Gruppe, Eventtyp und Partner-Channel-Konfigurationen sauber geprüft werden müssen. Normale Raids kannst du jetzt direkt hier im Menü erstellen.",
            ephemeral=True
        )

    @button(label="🗑️ Event löschen", style=ButtonStyle.secondary, custom_id="portal_admin_event_delete", row=1)
    async def btn_delete(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        events = _admin_active_rsvp_events(guild)
        if not events:
            await inter.response.send_message("📅 Keine aktiven Events gefunden.", ephemeral=True)
            return
        emb = discord.Embed(title="🗑️ Event löschen", description="Wähle das Event, das gelöscht werden soll.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventSelectView(guild.id, member.id, "delete", events))

    @button(label="📨 Resend Missing", style=ButtonStyle.secondary, custom_id="portal_admin_event_resend", row=1)
    async def btn_resend(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        events = _admin_active_rsvp_events(guild)
        if not events:
            await inter.response.send_message("📅 Keine aktiven Events gefunden.", ephemeral=True)
            return
        emb = discord.Embed(title="📨 Resend Missing", description="Wähle das Event, für das fehlende Abstimmungen erneut gesendet werden sollen.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventSelectView(guild.id, member.id, "resend", events))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_admin_event_back", row=2)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🛡️ Admin", description="Wähle einen Bereich.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminMenuView())


class AdminLootMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _open(self, inter: discord.Interaction, fn_name: str):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.", ephemeral=True)
            return
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        try:
            try:
                import bot.loot_needs as loot_mod  # type: ignore
            except ModuleNotFoundError:
                import loot_needs as loot_mod  # type: ignore
            fn = getattr(loot_mod, fn_name)
            await fn(inter, guild.id, member.id)
        except Exception as e:
            await inter.response.send_message(f"❌ Loot-Menü konnte nicht geöffnet werden: `{e}`", ephemeral=True)

    @button(label="➕ Item hinzufügen", style=ButtonStyle.secondary, custom_id="portal_admin_loot_add", row=0)
    async def btn_add(self, inter: discord.Interaction, _):
        await self._open(inter, "open_admin_item_add_menu")

    @button(label="📦 Loot gedroppt", style=ButtonStyle.secondary, custom_id="portal_admin_loot_drop", row=0)
    async def btn_drop(self, inter: discord.Interaction, _):
        await self._open(inter, "open_admin_loot_drop_menu")

    @button(label="✅ Item erhalten", style=ButtonStyle.secondary, custom_id="portal_admin_loot_mark", row=1)
    async def btn_mark(self, inter: discord.Interaction, _):
        await self._open(inter, "open_admin_mark_received_menu")

    @button(label="❌ Erhalten freigeben", style=ButtonStyle.secondary, custom_id="portal_admin_loot_unmark", row=1)
    async def btn_unmark(self, inter: discord.Interaction, _):
        await self._open(inter, "open_admin_unmark_received_menu")

    @button(label="📋 Katalog", style=ButtonStyle.secondary, custom_id="portal_admin_loot_catalog", row=2)
    async def btn_catalog(self, inter: discord.Interaction, _):
        await self._open(inter, "open_admin_item_catalog_menu")

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_admin_loot_back", row=3)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🛡️ Admin", description="Wähle einen Bereich.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminMenuView())


class SupportMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🛡️ Leaderkontakt", style=ButtonStyle.secondary, custom_id="portal_support_leader")
    async def btn_leader(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(PortalLeaderContactModal(guild.id, inter.user.id))

    @button(label="❓ Hilfe", style=ButtonStyle.secondary, custom_id="portal_support_help")
    async def btn_help(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title="❓ Hilfe – Ebolus Gildenbot",
            description=(
                "**👤 Profil**\n"
                "Hier pflegst du Ingame-Name, Main-Rolle und Gearscore.\n\n"
                "**📬 Raid-/Event-DMs**\n"
                "Im Bereich Persönlich kannst du Raid-DMs aktivieren oder deaktivieren.\n\n"
                "**🎁 Needliste**\n"
                "Hier trägst du ein, welche Items du brauchst. Die Gildenleitung nutzt das für Bossplanung und Lootübersicht.\n\n"
                "**🏖️ Abwesenheit**\n"
                "Hier meldest du Urlaub, Schicht oder längere Inaktivität.\n\n"
                "**📅 Kalender & Abwesenheiten**\n"
                "Zeigt feste Gildentermine und aktuelle/kommende Abwesenheiten.\n\n"
                "**📜 Regeln & Loot**\n"
                "Zeigt die wichtigsten Gildenregeln und das Lootsystem.\n\n"
                "**🛡️ Leaderkontakt**\n"
                "Schickt eine Anfrage direkt an die Gildenleitung."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=HelpView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_support_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class ProfileView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="✏️ Profil bearbeiten", style=ButtonStyle.secondary, custom_id="portal_profile_edit")
    async def btn_edit(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(ProfileEditModal(guild.id, inter.user.id))

    @button(label="👥 Mitglieder", style=ButtonStyle.secondary, custom_id="portal_member_list")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_members_list_embed(guild), view=BackOnlyView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_profile_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class EventsInfoView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🔄 Aktualisieren", style=ButtonStyle.secondary, custom_id="portal_events_refresh")
    async def btn_refresh(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_events_embed(guild.id), view=EventsInfoView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_events_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title="📅 Gilde",
            description=(
                "Hier findest du die wichtigsten Gildenübersichten.\n\n"
                "**Kalender**\n"
                "Feste Gildentermine und regelmäßige Events.\n\n"
                "**Abwesenheiten**\n"
                "Übersicht aktueller und kommender Abwesenheiten.\n\n"
                "**Mitglieder**\n"
                "Übersicht der Ebolus-Mitglieder mit Rang und Gearscore."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=GuildMenuView())


class RulesLootView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🎁 Needliste öffnen", style=ButtonStyle.secondary, custom_id="rules_open_need")
    async def btn_need(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        try:
            try:
                from bot.loot_needs import open_need_menu  # type: ignore
            except ModuleNotFoundError:
                from loot_needs import open_need_menu  # type: ignore

            await open_need_menu(inter, guild.id, member.id)
        except Exception:
            await inter.response.edit_message(embed=_rules_loot_embed(), view=RulesLootView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="rules_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class HelpView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🛡️ Leaderkontakt", style=ButtonStyle.secondary, custom_id="help_leader_contact")
    async def btn_leader(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(PortalLeaderContactModal(guild.id, inter.user.id))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="help_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class BackOnlyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


async def setup_member_portal(client: discord.Client, tree: app_commands.CommandTree):
    try:
        client.add_view(PortalOpenView())
        client.add_view(MemberPortalMainView())
        client.add_view(PersonalMenuView())
        client.add_view(DmSettingsView())
        client.add_view(LootMenuView())
        client.add_view(GuildMenuView())
        client.add_view(AbsenceCalendarView())
        client.add_view(SupportMenuView())
        client.add_view(ProfileView())
        client.add_view(EventsInfoView())
        client.add_view(RulesLootView())
        client.add_view(HelpView())
        client.add_view(BackOnlyView())
    except Exception:
        pass

    if hasattr(client, "add_listener"):
        async def _portal_on_member_update(before: discord.Member, after: discord.Member):
            try:
                if after.bot:
                    return

                c = _gcfg(after.guild.id)
                member_role_id = int(c.get("member_role_id", 0) or 0)

                if not member_role_id:
                    return

                before_ids = {r.id for r in before.roles}
                after_ids = {r.id for r in after.roles}

                got_member_role = member_role_id not in before_ids and member_role_id in after_ids

                if got_member_role:
                    await ensure_portal_menu_for_user(client, after.guild.id, after.id)

            except Exception as e:
                print(f"[member_portal] on_member_update Fehler: {e!r}")

        async def _portal_on_member_join(member: discord.Member):
            try:
                if member.bot:
                    return

                if _member_has_member_role(member):
                    await ensure_portal_menu_for_user(client, member.guild.id, member.id)

            except Exception as e:
                print(f"[member_portal] on_member_join Fehler: {e!r}")

        async def _portal_on_member_remove(member: discord.Member):
            try:
                _clear_portal_sent(member.guild.id, member.id)
            except Exception:
                pass

        try:
            client.add_listener(_portal_on_member_update, "on_member_update")
            client.add_listener(_portal_on_member_join, "on_member_join")
            client.add_listener(_portal_on_member_remove, "on_member_remove")
            print("✅ Member-Portal Listener aktiv.")
        except Exception as e:
            print(f"[member_portal] Listener-Setup Fehler: {e!r}")

    @tree.command(name="portal_set_absence_channel", description="(Admin) Abwesenheitskanal setzen")
    async def portal_set_absence_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["absence_channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(f"✅ Abwesenheitskanal gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="portal_set_member_role", description="(Admin) Rolle setzen, deren Mitglieder automatisch das Gildenmenü bekommen")
    async def portal_set_member_role(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["member_role_id"] = int(role.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Ebolus-/Gildenmitglied-Rolle gesetzt: {role.mention}\n"
            f"Neue Mitglieder mit dieser Rolle bekommen automatisch das Gildenmenü per DM.",
            ephemeral=True
        )

    @tree.command(name="portal_send_all", description="(Admin) Öffnet/aktualisiert das Gildenmenü per DM bei allen mit Gildenmitglied-Rolle")
    async def portal_send_all(inter: discord.Interaction, force: bool = False):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        member_role_id = int(c.get("member_role_id", 0) or 0)

        if not member_role_id:
            await inter.followup.send("❌ Keine Ebolus-/Gildenmitglied-Rolle gesetzt. Nutze zuerst `/portal_set_member_role`.", ephemeral=True)
            return

        role = inter.guild.get_role(member_role_id)

        if not role:
            await inter.followup.send("❌ Ebolus-/Gildenmitglied-Rolle nicht gefunden.", ephemeral=True)
            return

        sent_or_updated = 0
        failed = 0

        for member in role.members:
            if member.bot:
                continue

            ok = await ensure_portal_menu_for_user(client, inter.guild_id, member.id)

            if ok:
                sent_or_updated += 1
            else:
                failed += 1

            await asyncio.sleep(0.15)

        await inter.followup.send(
            f"✅ Portal-DM abgeschlossen.\n"
            f"✉️ Geöffnet/aktualisiert: **{sent_or_updated}**\n"
            f"❌ Fehlgeschlagen/DMs zu: **{failed}**",
            ephemeral=True
        )

    @tree.command(name="portal_resend_user", description="(Admin) Öffnet/aktualisiert bei einem Spieler das Gildenmenü")
    async def portal_resend_user(inter: discord.Interaction, member: discord.Member):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        ok = await ensure_portal_menu_for_user(client, inter.guild_id, member.id)

        if ok:
            await inter.followup.send(f"✅ Gildenmenü bei **{member.display_name}** geöffnet/aktualisiert.", ephemeral=True)
        else:
            await inter.followup.send(f"❌ Konnte **{member.display_name}** keine DM senden.", ephemeral=True)

    @tree.command(name="portal_force_new_user", description="(Admin) Erzwingt ein komplett neues Gildenmenü per DM")
    async def portal_force_new_user(inter: discord.Interaction, member: discord.Member):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        if member.bot:
            await inter.followup.send("❌ Bots bekommen kein Gildenmenü.", ephemeral=True)
            return

        # Alte gespeicherte Portal-ID vergessen.
        # Dadurch wird NICHT die Needliste, das Profil oder sonstige Daten gelöscht.
        _clear_portal_sent(inter.guild_id, member.id)

        # Wirklich neue DM senden, nicht alte Nachricht editieren.
        msg = await _send_new_portal_menu(member, inter.guild)

        if msg:
            await inter.followup.send(
                f"✅ Neues Gildenmenü bei **{member.display_name}** gesendet.\n"
                f"Neue Menü-ID: `{msg.id}`",
                ephemeral=True
            )
        else:
            await inter.followup.send(
                f"❌ Konnte **{member.display_name}** keine neue DM senden. "
                f"Prüfe, ob DMs vom Server erlaubt sind.",
                ephemeral=True
            )

    @tree.command(name="portal_force_new_all", description="(Admin) Erzwingt ein komplett neues Gildenmenü bei allen Gildenmitgliedern")
    async def portal_force_new_all(inter: discord.Interaction):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        member_role_id = int(c.get("member_role_id", 0) or 0)

        if not member_role_id:
            await inter.followup.send(
                "❌ Keine Ebolus-/Gildenmitglied-Rolle gesetzt. Nutze zuerst `/portal_set_member_role`.",
                ephemeral=True
            )
            return

        role = inter.guild.get_role(member_role_id)

        if not role:
            await inter.followup.send("❌ Ebolus-/Gildenmitglied-Rolle nicht gefunden.", ephemeral=True)
            return

        sent_count = 0
        failed_count = 0
        checked_count = 0

        for member in role.members:
            if member.bot:
                continue

            checked_count += 1

            try:
                # Alte gespeicherte Portal-ID vergessen.
                # Löscht keine Needlisten, Profile, Abwesenheiten oder sonstige Nutzerdaten.
                _clear_portal_sent(inter.guild_id, member.id)

                # Wirklich neue DM senden, nicht alte Nachricht editieren.
                msg = await _send_new_portal_menu(member, inter.guild)

                if msg:
                    sent_count += 1
                else:
                    failed_count += 1

                await asyncio.sleep(0.25)

            except Exception:
                failed_count += 1

        await inter.followup.send(
            f"✅ Force-New für alle abgeschlossen.\n"
            f"👥 Geprüft: **{checked_count}**\n"
            f"✉️ Neu gesendet: **{sent_count}**\n"
            f"❌ Fehlgeschlagen/DMs zu: **{failed_count}**",
            ephemeral=True
        )

    @tree.command(name="portal_dm_cleanup_user", description="(Admin) Löscht alte Bot-DMs bei einem Mitglied, schützt aktives Gildenmenü")
    async def portal_dm_cleanup_user(
        inter: discord.Interaction,
        member: discord.Member,
        limit: int = 300
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        limit = max(1, min(limit, 1000))

        await ensure_portal_menu_for_user(client, inter.guild_id, member.id)
        deleted = await _delete_old_bot_dms_for_member(inter.client, member, limit=limit)
        await ensure_portal_menu_for_user(client, inter.guild_id, member.id)

        await inter.followup.send(
            f"✅ DM-Cleanup bei **{member.display_name}** abgeschlossen.\n"
            f"🧹 Gelöschte Bot-Nachrichten: **{deleted}**\n"
            f"⚜️ Aktives Gildenmenü wurde geschützt.",
            ephemeral=True
        )

    @tree.command(name="portal_dm_cleanup_all", description="(Admin) Löscht alte Bot-DMs bei allen Gildenmitgliedern, schützt aktives Gildenmenü")
    async def portal_dm_cleanup_all(
        inter: discord.Interaction,
        limit: int = 300
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        limit = max(1, min(limit, 1000))

        c = _gcfg(inter.guild_id)
        member_role_id = int(c.get("member_role_id", 0) or 0)

        if not member_role_id:
            await inter.followup.send(
                "❌ Keine Ebolus-/Gildenmitglied-Rolle gesetzt. Nutze zuerst `/portal_set_member_role`.",
                ephemeral=True
            )
            return

        role = inter.guild.get_role(member_role_id)

        if not role:
            await inter.followup.send("❌ Ebolus-/Gildenmitglied-Rolle nicht gefunden.", ephemeral=True)
            return

        total_deleted = 0
        checked = 0
        failed = 0

        for member in role.members:
            if member.bot:
                continue

            checked += 1

            try:
                await ensure_portal_menu_for_user(client, inter.guild_id, member.id)
                deleted = await _delete_old_bot_dms_for_member(inter.client, member, limit=limit)
                total_deleted += deleted
                await ensure_portal_menu_for_user(client, inter.guild_id, member.id)
                await asyncio.sleep(0.2)
            except Exception:
                failed += 1

        await inter.followup.send(
            f"✅ DM-Cleanup für Ebolus-/Gildenmitglieder abgeschlossen.\n"
            f"👥 Geprüft: **{checked}**\n"
            f"🧹 Gelöschte Bot-Nachrichten: **{total_deleted}**\n"
            f"❌ Fehlgeschlagen: **{failed}**\n"
            f"⚜️ Aktive Gildenmenüs wurden geschützt.",
            ephemeral=True
        )

    @tree.command(name="portal_set_position_roles", description="(Admin) Rollen für Anführer/Gildenberater/Wächter setzen")
    async def portal_set_position_roles(
        inter: discord.Interaction,
        anfuehrer: discord.Role,
        gildenberater: discord.Role,
        waechter: discord.Role,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["position_roles"] = {
            "leader": int(anfuehrer.id),
            "advisor": int(gildenberater.id),
            "guardian": int(waechter.id),
        }

        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Positionsrollen gesetzt:\n"
            f"• Anführer: {anfuehrer.mention}\n"
            f"• Gildenberater: {gildenberater.mention}\n"
            f"• Wächter: {waechter.mention}",
            ephemeral=True
        )

    @tree.command(name="portal_event_add", description="(Admin) Festes Event für das Gildenmenü hinzufügen")
    async def portal_event_add(
        inter: discord.Interaction,
        weekday: str,
        time: str,
        title: str,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        events = c.get("events") or []

        events.append({
            "weekday": weekday.strip(),
            "time": time.strip(),
            "title": title.strip(),
        })

        c["events"] = events
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Event hinzugefügt:\n**{weekday}, {time} Uhr** – {title}",
            ephemeral=True
        )

    @tree.command(name="portal_events_clear", description="(Admin) Alle festen Gildenmenü-Events löschen")
    async def portal_events_clear(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["events"] = []
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message("✅ Alle Gildenmenü-Events gelöscht.", ephemeral=True)

    @tree.command(name="portal_events_list", description="(Admin) Zeigt gespeicherte Gildenmenü-Events")
    async def portal_events_list(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        events = c.get("events") or []

        if not events:
            await inter.response.send_message("📅 Keine Events gespeichert.", ephemeral=True)
            return

        lines = []

        for i, e in enumerate(events, start=1):
            lines.append(f"{i}. **{e.get('weekday')}**, {e.get('time')} Uhr – {e.get('title')}")

        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @tree.command(name="portal_post", description="(Admin) Postet den Button zum Öffnen des privaten Gildenmenüs")
    async def portal_post(inter: discord.Interaction, channel: Optional[discord.TextChannel] = None):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        ch = channel or inter.channel

        if not isinstance(ch, discord.TextChannel):
            await inter.response.send_message("❌ Ungültiger Channel.", ephemeral=True)
            return

        emb = discord.Embed(
            title="⚜️ Ebolus Gildenbot",
            description=(
                "Öffne hier dein persönliches Gildenmenü im Privatchat.\n\n"
                "Dort findest du:\n"
                "• dein Profil\n"
                "• deine Needliste\n"
                "• Raid-DM Einstellungen\n"
                "• feste Gilden-Events\n"
                "• Abwesenheit melden\n"
                "• Abwesenheitskalender\n"
                "• Leader kontaktieren\n"
                "• Regeln & Loot\n"
                "• Mitgliederübersicht\n"
                "• Hilfe"
            ),
            color=discord.Color.gold()
        )

        try:
            msg = await ch.send(embed=emb, view=PortalOpenView())
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Portal-Post nicht senden: {e}", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["portal_post_channel_id"] = int(ch.id)
        c["portal_post_message_id"] = int(msg.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(f"✅ Portal-Post erstellt: {msg.jump_url}", ephemeral=True)

    @tree.command(name="portal_status", description="(Admin) Zeigt Portal-Konfiguration")
    async def portal_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        roles = c.get("position_roles") or {}

        absence_ch_id = int(c.get("absence_channel_id", 0) or 0)
        portal_ch_id = int(c.get("portal_post_channel_id", 0) or 0)
        member_role_id = int(c.get("member_role_id", 0) or 0)

        leader_role_id = int(roles.get("leader", 0) or 0)
        advisor_role_id = int(roles.get("advisor", 0) or 0)
        guardian_role_id = int(roles.get("guardian", 0) or 0)

        text = (
            f"**Member-Portal Status**\n"
            f"• Ebolus-/Gildenmitglied-Rolle: {f'<@&{member_role_id}>' if member_role_id else '—'}\n"
            f"• Abwesenheitskanal: {f'<#{absence_ch_id}>' if absence_ch_id else '—'}\n"
            f"• Portal-Post-Channel: {f'<#{portal_ch_id}>' if portal_ch_id else '—'}\n"
            f"• Portal-Message-ID: `{c.get('portal_post_message_id', 0)}`\n\n"
            f"**Positionsrollen**\n"
            f"• Anführer: {f'<@&{leader_role_id}>' if leader_role_id else '—'}\n"
            f"• Gildenberater: {f'<@&{advisor_role_id}>' if advisor_role_id else '—'}\n"
            f"• Wächter: {f'<@&{guardian_role_id}>' if guardian_role_id else '—'}\n\n"
            f"**Feste Events:** {len(c.get('events') or [])}"
        )

        await inter.response.send_message(text, ephemeral=True)
