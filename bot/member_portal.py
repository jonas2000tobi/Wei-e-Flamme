from __future__ import annotations

import json
import re
import asyncio
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Any, Tuple

import discord
from discord import app_commands
from discord.ui import View, button, Modal, TextInput, Select, ChannelSelect, RoleSelect
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


def _get_ec_balance_safe(guild_id: int, user_id: int) -> int:
    try:
        try:
            from bot.dkp_system import get_balance  # type: ignore
        except ModuleNotFoundError:
            from dkp_system import get_balance  # type: ignore
        return int(get_balance(int(guild_id), int(user_id)) or 0)
    except Exception:
        return 0


# Custom Discord-Emojis für Rollen/RSVP
EMOJI_TANK = "<:tank:1516465336054972456>"
EMOJI_HEAL = "<:heal:1516478246001049690>"
EMOJI_DPS = "<:dps:1516476505918668940>"
EMOJI_BANK = "<:reserve:1516465611243520201>"
EMOJI_MAYBE = "<:maybe:1516465379445047497>"
EMOJI_NO = "<:no:1516465299359273070>"

# Custom Discord-Emojis für das Gildenmenü
EMOJI_EBOLUS = "<:ebolus:1516448234355163208>"
EMOJI_PERSONAL = "<:persoenlich:1516459694997372949>"
EMOJI_LOOT = "<:loot:1516459736659136672>"
EMOJI_GUILD = "<:ebolus:1516448234355163208>"
EMOJI_CONTACT = "<:kontakt:1516459812999921775>"
EMOJI_ADMIN = "<:admin:1516459630572601487>"
EMOJI_TIME = "<:time:1516461870146523379>"
EMOJI_VOTED = "<:voted:1516461766761119936>"
EMOJI_TARGET = "<:target:1516461644471865365>"
EMOJI_ABSENCE = "<:nichtda:1516463499872833616>"
EMOJI_CALENDAR = "<:Kalender:1516462026468098181>"
EMOJI_BACK = "<:zurueck:1516470839120498778>"
EMOJI_HELP = "<:hilfe:1516470888818802900>"
EMOJI_MEMBER = "<:member:1516474249492168734>"


def _menu_emoji(value: str):
    try:
        if isinstance(value, str) and (value.startswith("<:") or value.startswith("<a:")):
            return discord.PartialEmoji.from_str(value)
    except Exception:
        pass
    return value

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
    c.setdefault("event_voice_category_id", 0)
    c.setdefault("event_voice_return_channel_id", 0)
    c.setdefault("guild_info_text", "")
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
        "TANK": f"{EMOJI_TANK} Tank",
        "HEAL": f"{EMOJI_HEAL} Heal",
        "DPS": f"{EMOJI_DPS} DPS",
        "BANK": f"{EMOJI_BANK} Reserve",
    }

    for key, label in labels.items():
        for entry in yes.get(key, []) or []:
            if _rsvp_entry_user_id(entry) == int(user_id):
                return label

    maybe = obj.get("maybe") or {}

    if str(user_id) in maybe:
        return f"{EMOJI_MAYBE} Vielleicht"

    for entry in maybe.values():
        if _rsvp_entry_user_id(entry) == int(user_id):
            return f"{EMOJI_MAYBE} Vielleicht"

    for entry in obj.get("no", []) or []:
        if _rsvp_entry_user_id(entry) == int(user_id):
            return f"{EMOJI_NO} Abgemeldet"

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
            parts.append(f"{EMOJI_CALENDAR} **Deine kommenden Events:**\n" + "\n".join(lines))

        if open_votes:
            lines = [x[1] for x in open_votes[:3]]
            parts.append(f"{EMOJI_VOTED} **Offene Abstimmungen:**\n" + "\n".join(lines))

        if not parts:
            return f"{EMOJI_CALENDAR} **Deine kommenden Events:**\nKeine aktiven Anmeldungen oder offenen Abstimmungen."

        return "\n\n".join(parts)

    except Exception:
        return f"{EMOJI_CALENDAR} **Deine kommenden Events:**\nKeine Übersicht verfügbar."


def _main_menu_embed(guild: discord.Guild, member: Optional[discord.Member] = None) -> discord.Embed:
    event_block = ""

    if member:
        event_block = _event_status_block(guild, member) + "\n\n"

    c = _gcfg(guild.id)
    guild_info = str(c.get("guild_info_text", "") or "").strip()
    if not guild_info:
        guild_info = "Keine aktuelle Mitteilung."

    ec_block = ""
    if member:
        ec_balance = _get_ec_balance_safe(guild.id, member.id)
        ec_block = f"\n\n🪙 **Ebolus Coins (EC)**\nDein Konto: **{ec_balance} EC**"

    emb = discord.Embed(
        title=f"{EMOJI_EBOLUS} Ebolus Kommandozentrale",
        description=(
            event_block +
            "Willkommen im privaten Gildenmenü.\n\n"
            f"📢 **Gildeninfo**\n{guild_info}" +
            ec_block
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
        title=f"{EMOJI_PERSONAL} Dein Gildenprofil",
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
        title=f"{EMOJI_CALENDAR} Ebolus Gildenkalender",
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
        title=f"{EMOJI_ABSENCE} Abwesenheitskalender",
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
    except Exception as e:
        print(f"[member_portal] DM-Send Fehler für user={getattr(user, 'id', '?')} guild={guild.id}: {e!r}")
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

    except Exception as e:
        print(f"[member_portal] Portal-Menü edit/send Fehler für guild={guild_id} user={user_id}: {e!r}")
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
        f"{EMOJI_EBOLUS} Ebolus Kommandozentrale",
        "🏰 ebolus – Gildenmenü",
        "👤 Dein Gildenprofil",
        f"{EMOJI_PERSONAL} Dein Gildenprofil",
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
        f"{EMOJI_PERSONAL} Persönlich",
        "🎁 Loot & Bedarf",
        f"{EMOJI_LOOT} Loot & Bedarf",
        "📅 Gilde",
        f"{EMOJI_GUILD} Gilde",
        "🛡️ Kontakt & Hilfe",
        f"{EMOJI_CONTACT} Kontakt & Hilfe",
        "🛡️ Admin",
        f"{EMOJI_ADMIN} Admin",
        "<:gilde:1516444419040215050> Gilde",
        "<:gilde:1516444419040215050> Admin – Event",
        "📅 Admin – Event",
        f"{EMOJI_GUILD} Admin – Event",
        "🎁 Admin – Loot",
        f"{EMOJI_LOOT} Admin – Loot",
        "✅ Admin – Anwesenheit",
        "✅ Anwesenheit prüfen",
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
                title=f"{EMOJI_ABSENCE} Abwesenheit gemeldet",
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
                emoji=_menu_emoji(EMOJI_PERSONAL)
            ),
            discord.SelectOption(
                label="Loot & Bedarf",
                value="loot",
                description="Needliste für Main/Secondary und Lootregeln",
                emoji=_menu_emoji(EMOJI_LOOT)
            ),
            discord.SelectOption(
                label="Gilde",
                value="guild",
                description="Kalender, Abwesenheiten und Mitgliederübersicht",
                emoji=_menu_emoji(EMOJI_EBOLUS)
            ),
            discord.SelectOption(
                label="Kontakt & Hilfe",
                value="support",
                description="Leader kontaktieren oder Hilfe zum Bot öffnen",
                emoji=_menu_emoji(EMOJI_CONTACT)
            ),
            discord.SelectOption(
                label="Admin",
                value="admin",
                description="Event- und Loot-Verwaltung für Leitung",
                emoji=_menu_emoji(EMOJI_ADMIN)
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
                title=f"{EMOJI_PERSONAL} Persönlich",
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
                title=f"{EMOJI_LOOT} Loot & Bedarf",
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
                title=f"{EMOJI_GUILD} Gilde",
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
                title=f"{EMOJI_CONTACT} Kontakt & Hilfe",
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
                title=f"{EMOJI_ADMIN} Admin",
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

    @button(label="Profil", emoji=_menu_emoji(EMOJI_PERSONAL), style=ButtonStyle.secondary, custom_id="portal_personal_profile")
    async def btn_profile(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_profile_embed(guild, member), view=ProfileView())

    @button(label="Abwesenheit melden", emoji=_menu_emoji(EMOJI_ABSENCE), style=ButtonStyle.secondary, custom_id="portal_personal_absence")
    async def btn_absence(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(AbsenceModal(guild.id, inter.user.id))

    @button(label="Raid-DMs", emoji=_menu_emoji(EMOJI_VOTED), style=ButtonStyle.secondary, custom_id="portal_personal_dm_settings")
    async def btn_dm_settings(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_dm_settings_embed(guild, member), view=DmSettingsView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_personal_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class DmSettingsView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="DMs an/aus schalten", emoji=_menu_emoji(EMOJI_VOTED), style=ButtonStyle.secondary, custom_id="portal_dm_toggle")
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

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_dm_back")
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
            title=f"{EMOJI_PERSONAL} Persönlich",
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

    @button(label="Needliste", emoji=_menu_emoji(EMOJI_LOOT), style=ButtonStyle.secondary, custom_id="portal_loot_needlist")
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

    @button(label="Regeln & Loot", emoji=_menu_emoji(EMOJI_LOOT), style=ButtonStyle.secondary, custom_id="portal_loot_rules")
    async def btn_rules(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_rules_loot_embed(), view=RulesLootView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_loot_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class GuildMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Kalender", emoji=_menu_emoji(EMOJI_CALENDAR), style=ButtonStyle.secondary, custom_id="portal_guild_calendar")
    async def btn_calendar(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_events_embed(guild.id), view=EventsInfoView())

    @button(label="Abwesenheiten", emoji=_menu_emoji(EMOJI_ABSENCE), style=ButtonStyle.secondary, custom_id="portal_guild_absences")
    async def btn_absences(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_absence_calendar_embed(guild), view=AbsenceCalendarView())

    @button(label="Mitglieder", emoji=_menu_emoji(EMOJI_MEMBER), style=ButtonStyle.secondary, custom_id="portal_guild_members")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_members_list_embed(guild), view=BackOnlyView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_guild_back")
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

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_absence_calendar_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title=f"{EMOJI_GUILD} Gilde",
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


def _admin_clean_voice_name(title: str) -> str:
    raw = str(title or "Event").strip() or "Event"
    raw = re.sub(r"[#@`*_~|<>\n\r]+", "", raw).strip()
    if len(raw) > 60:
        raw = raw[:60].strip()
    return f"🔊 {raw}"


async def _admin_create_event_voice(
    guild: discord.Guild,
    title: str,
    category_id: int = 0,
) -> Optional[discord.VoiceChannel]:
    category = guild.get_channel(int(category_id or 0)) if category_id else None
    if category is not None and not isinstance(category, discord.CategoryChannel):
        category = None

    try:
        return await guild.create_voice_channel(
            name=_admin_clean_voice_name(title),
            category=category,
            reason="Event-Voice automatisch über Gildenmenü erstellt",
        )
    except Exception as e:
        print(f"[member_portal] Event-Voice konnte nicht erstellt werden: {e!r}")
        return None


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
    channel_id: int,
    target_role_id: int,
    description: str,
    image_url: str | None = None,
    reminders: list[dict] | None = None,
    voice_enabled: bool = False,
    voice_category_id: int = 0,
    voice_return_channel_id: int = 0,
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

    ch = guild.get_channel(int(channel_id or 0))
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        await inter.followup.send("❌ Zielkanal nicht gefunden.", ephemeral=True)
        return

    target_role_id = int(target_role_id or 0)
    if target_role_id and guild.get_role(target_role_id) is None:
        await inter.followup.send("❌ Zielrolle nicht gefunden.", ephemeral=True)
        return

    voice_channel_id = 0
    if voice_enabled:
        voice = await _admin_create_event_voice(guild, str(title).strip(), int(voice_category_id or 0))
        if voice is None:
            await inter.followup.send(
                "⚠️ Event-Voice konnte nicht erstellt werden. Das Event wird trotzdem ohne Voice erstellt.",
                ephemeral=True,
            )
        else:
            voice_channel_id = int(voice.id)

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
        "target_role_id": int(target_role_id),
        "reminders": reminders or [],
        "reminder_sent": {},
        "voice_enabled": bool(voice_channel_id),
        "voice_channel_id": int(voice_channel_id),
        "voice_return_channel_id": int(voice_return_channel_id or 0),
        "voice_cleanup_done": False,
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
        f"🔕 Opt-out übersprungen: **{skipped_opt_out}**\n"
        f"⏰ Reminder: **{len(reminders or [])}**\n"
        f"🔊 Voice: **{'erstellt' if voice_channel_id else 'nein'}**",
        ephemeral=True
    )


async def _admin_create_alliance_raid_from_menu(
    inter: discord.Interaction,
    guild_id: int,
    group: str,
    event_type: str,
    title: str,
    date_text: str,
    time_text: str,
    target_role_id: int,
    description: str,
    image_url: str | None = None,
):
    """Allianz-Event über das Gildenmenü erstellen.

    Nutzt bewusst das vorhandene Allianz-System aus alliance_config.py:
    - Allianz-Gruppe
    - Home-/Partner-Server
    - Eventtyp-Channels
    - Mirror-Posts
    - Home-DMs
    """
    rsvp = _admin_event_module()
    guild = inter.client.get_guild(int(guild_id))
    if not guild:
        await inter.followup.send("❌ Server nicht gefunden.", ephemeral=True)
        return

    if not _is_portal_admin(guild, guild.get_member(inter.user.id)):
        await inter.followup.send("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
        return

    # Das Allianz-Menü läuft im privaten Gildenportal per DM.
    # rsvp._require_alliance_home_leader(inter) kann hier nicht benutzt werden,
    # weil inter.guild in DMs None ist und dann immer "Nur im Server nutzbar" liefert.
    # Stattdessen prüfen wir gegen den bereits aufgelösten Home-/Ebolus-Server.
    try:
        home_id = int(rsvp._alliance_home_guild_id(default=int(guild.id)) or int(guild.id))
    except Exception:
        home_id = int(guild.id)

    if int(guild.id) != int(home_id):
        await inter.followup.send("❌ Allianz-Events können nur über den Home-/Ebolus-Server erstellt werden.", ephemeral=True)
        return

    normalized_event_type = rsvp._normalize_alliance_event_type(str(event_type))
    if not normalized_event_type:
        await inter.followup.send(
            f"❌ Ungültiger Allianz-Eventtyp. Erlaubt: {rsvp._alliance_event_type_text()}",
            ephemeral=True,
        )
        return

    try:
        yyyy, mm, dd = _admin_parse_event_date(date_text)
        hh, mi = [int(x) for x in str(time_text).strip().split(":")]
        when = datetime(yyyy, mm, dd, hh, mi, tzinfo=rsvp.TZ)
    except Exception:
        await inter.followup.send("❌ Datum/Zeit ungültig. Nutze z. B. `2026-06-20` oder `20.06.2026` und `20:30`.", ephemeral=True)
        return

    try:
        get_alliance_group = rsvp._import_alliance_config()
        group_obj = get_alliance_group(str(group))
    except Exception as e:
        await inter.followup.send(f"❌ Allianz-Konfiguration konnte nicht geladen werden: `{e}`", ephemeral=True)
        return

    if not group_obj:
        await inter.followup.send("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
        return

    servers = group_obj.get("servers") or {}
    if not servers:
        await inter.followup.send("❌ In dieser Allianz-Gruppe sind keine Server/Channels hinterlegt.", ephemeral=True)
        return

    home_server = servers.get(str(guild.id))
    if not home_server:
        await inter.followup.send(
            "❌ Der Home-/Ebolus-Server ist in dieser Allianz-Gruppe nicht hinterlegt.\n"
            "Nutze zuerst `/alliance_server_add_home`.",
            ephemeral=True,
        )
        return

    home_channel_id = rsvp._server_channel_id_for_event(home_server, normalized_event_type)
    home_channel = guild.get_channel(home_channel_id)
    if not isinstance(home_channel, (discord.TextChannel, discord.Thread)):
        await inter.followup.send(
            f"❌ Home-Zielchannel für **{normalized_event_type}** wurde nicht gefunden.\n"
            f"Setze ihn mit `/alliance_event_channel_set group:{group} event_type:{normalized_event_type} channel:#channel`.",
            ephemeral=True,
        )
        return

    target_role = guild.get_role(int(target_role_id or 0)) if int(target_role_id or 0) else None
    if int(target_role_id or 0) and target_role is None:
        await inter.followup.send("❌ Zielrolle nicht gefunden.", ephemeral=True)
        return

    obj = {
        "scope": "alliance",
        "alliance_group": str(group_obj.get("name", group)),
        "event_type": normalized_event_type,
        "guild_id": int(guild.id),
        "channel_id": int(home_channel.id),
        "title": str(title).strip(),
        "description": str(description or "").strip(),
        "when_iso": when.isoformat(),
        "image_url": str(image_url or "").strip() or None,
        "yes": {"TANK": [], "HEAL": [], "DPS": [], "BANK": []},
        "maybe": {},
        "no": [],
        "target_role_id": int(target_role.id) if target_role else 0,
        "dm_messages": {},
        "mirrors": [],
    }

    home_emb = rsvp.build_embed(guild, obj)
    home_msg = await home_channel.send(embed=home_emb)
    master_id = int(home_msg.id)
    obj["message_id"] = master_id

    obj["mirrors"].append({
        "guild_id": int(guild.id),
        "discord_name": guild.name,
        "label": str(home_server.get("label", guild.name)),
        "short_label": str(home_server.get("short_label", home_server.get("label", guild.name))),
        "channel_id": int(home_channel.id),
        "channel_name": getattr(home_channel, "name", ""),
        "message_id": master_id,
        "send_dm": bool(home_server.get("send_dm", True)),
        "home": True,
    })

    rsvp.store[str(master_id)] = obj
    rsvp.save_store()

    try:
        await home_msg.edit(view=rsvp.ServerRaidView(master_id))
    except Exception:
        pass

    posted = [f"✅ **{guild.name}** → {home_channel.mention}"]
    failed = []

    for guild_id_str, server_cfg in servers.items():
        try:
            gid = int(guild_id_str)
            if gid == guild.id:
                continue

            partner_guild = inter.client.get_guild(gid)
            if not partner_guild:
                failed.append(f"❌ `{server_cfg.get('label', guild_id_str)}` — Bot sieht den Server nicht")
                continue

            target_channel_id = rsvp._server_channel_id_for_event(server_cfg, normalized_event_type)
            ch = partner_guild.get_channel(target_channel_id)
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                failed.append(f"❌ `{server_cfg.get('label', partner_guild.name)}` — Channel für {normalized_event_type} nicht gefunden")
                continue

            emb = rsvp.build_embed(partner_guild, obj)
            msg = await ch.send(embed=emb, view=rsvp.ServerRaidView(master_id))

            obj["mirrors"].append({
                "guild_id": int(partner_guild.id),
                "discord_name": partner_guild.name,
                "label": str(server_cfg.get("label", partner_guild.name)),
                "short_label": str(server_cfg.get("short_label", server_cfg.get("label", partner_guild.name))),
                "channel_id": int(ch.id),
                "channel_name": getattr(ch, "name", ""),
                "message_id": int(msg.id),
                "send_dm": False,
                "home": False,
            })

            posted.append(f"✅ **{server_cfg.get('label', partner_guild.name)}** → <#{ch.id}>")
            await asyncio.sleep(0.15)

        except Exception as e:
            failed.append(f"❌ `{server_cfg.get('label', guild_id_str)}` — {e}")

    # Allianz-Events aus dem Gildenmenü sollen immer Home-/Ebolus-DMs senden.
    # Die Partner-Server bekommen weiterhin nur Mirror-Posts mit Buttons, keine DMs.
    sent = 0
    skipped_opt_out = 0
    sent, skipped_opt_out = await rsvp._send_home_dms_for_alliance_event(
        guild,
        obj,
        master_id,
        f"Allianz-Übersicht im Server: #{getattr(home_channel, 'name', 'raid')}",
    )

    rsvp.store[str(master_id)] = obj
    rsvp.save_store()

    try:
        await rsvp._push_overview(inter.client, str(master_id), obj)
    except Exception:
        pass

    try:
        rsvp._schedule_portal_refresh_for_event(inter.client, guild, obj)
    except Exception:
        pass

    result = (
        f"✅ Allianz-Event erstellt.\n"
        f"Gruppe: **{group_obj.get('name', group)}**\n"
        f"Eventtyp: **{normalized_event_type}**\n"
        f"Master-Message-ID: `{master_id}`\n"
        f"✉️ Home-DMs versendet: **{sent}**\n"
        f"🔕 Home-Opt-out übersprungen: **{skipped_opt_out}**\n\n"
        f"**Gepostet:**\n" + "\n".join(posted)
    )
    if failed:
        result += "\n\n**Fehler:**\n" + "\n".join(failed)
    if len(result) > 1900:
        result = result[:1850] + "\n… gekürzt"

    await inter.followup.send(result, ephemeral=True)


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
        self.description_input = TextInput(label="Beschreibung", placeholder="Optional", required=False, style=discord.TextStyle.paragraph, max_length=800)
        self.add_item(self.title_input)
        self.add_item(self.date_input)
        self.add_item(self.time_input)
        self.add_item(self.description_input)

    async def on_submit(self, inter: discord.Interaction):
        data = {
            "guild_id": self.guild_id,
            "user_id": self.user_id,
            "title": str(self.title_input.value),
            "date_text": str(self.date_input.value),
            "time_text": str(self.time_input.value),
            "description": str(self.description_input.value or ""),
        }

        emb = discord.Embed(
            title="📍 Zielkanal wählen",
            description="Wähle den Discord-Kanal, in dem der Raid/Event-Post erscheinen soll.",
            color=discord.Color.gold()
        )

        await inter.response.send_message(embed=emb, view=AdminEventChannelSelectView(data, inter.client), ephemeral=True)


class AdminAllianceEventCreateModal(Modal):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(title="Allianz-Event erstellen", timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.title_input = TextInput(label="Titel", placeholder="z. B. Allianz HM Raid", required=True, max_length=100)
        self.date_input = TextInput(label="Datum", placeholder="YYYY-MM-DD oder TT.MM.JJJJ", required=True, max_length=20)
        self.time_input = TextInput(label="Uhrzeit", placeholder="HH:MM", required=True, max_length=10)
        self.description_input = TextInput(label="Beschreibung", placeholder="Optional", required=False, style=discord.TextStyle.paragraph, max_length=800)
        self.add_item(self.title_input)
        self.add_item(self.date_input)
        self.add_item(self.time_input)
        self.add_item(self.description_input)

    async def on_submit(self, inter: discord.Interaction):
        data = {
            "scope": "alliance",
            "guild_id": self.guild_id,
            "user_id": self.user_id,
            "title": str(self.title_input.value),
            "date_text": str(self.date_input.value),
            "time_text": str(self.time_input.value),
            "description": str(self.description_input.value or ""),
        }

        emb = discord.Embed(
            title="🌐 Allianz-Gruppe wählen",
            description=(
                "Wähle die vorhandene Allianz-Gruppe aus.\n\n"
                "Die Zielkanäle werden aus dem bestehenden `alliance_config`-System genommen."
            ),
            color=discord.Color.gold(),
        )
        await inter.response.send_message(embed=emb, view=AdminAllianceGroupSelectView(data), ephemeral=True)


class AdminAllianceGroupSelectView(View):
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminAllianceGroupSelect(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_alliance_group_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(title="Abgebrochen", description="Das Allianz-Event wurde nicht erstellt.", color=discord.Color.orange()),
            view=None,
        )


class AdminAllianceGroupSelect(Select):
    def __init__(self, data: dict):
        self.data = dict(data)
        options: list[discord.SelectOption] = []
        try:
            try:
                from bot.alliance_config import list_alliance_groups  # type: ignore
            except ModuleNotFoundError:
                from alliance_config import list_alliance_groups  # type: ignore
            groups = list_alliance_groups() or {}
            for key, obj in list(groups.items())[:25]:
                name = str((obj or {}).get("name", key) or key)
                servers = (obj or {}).get("servers") or {}
                options.append(discord.SelectOption(
                    label=name[:100],
                    value=str(key)[:100],
                    description=f"{len(servers)} Server hinterlegt"[:100],
                ))
        except Exception:
            options = []

        if not options:
            options.append(discord.SelectOption(label="Keine Allianz-Gruppe gefunden", value="__none__", description="Erst alliance_config einrichten"))

        super().__init__(placeholder="Allianz-Gruppe wählen", min_values=1, max_values=1, options=options, custom_id="admin_alliance_group_select")

    async def callback(self, inter: discord.Interaction):
        group = str(self.values[0])
        if group == "__none__":
            await inter.response.send_message("❌ Keine Allianz-Gruppe gefunden. Richte zuerst das Allianz-System ein.", ephemeral=True)
            return
        self.data["alliance_group"] = group
        emb = discord.Embed(
            title="🌐 Allianz-Eventtyp wählen",
            description="Der Eventtyp entscheidet, in welche konfigurierten Allianz-Channels gepostet wird.",
            color=discord.Color.gold(),
        )
        await inter.response.edit_message(embed=emb, view=AdminAllianceEventTypeSelectView(self.data))


class AdminAllianceEventTypeSelectView(View):
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminAllianceEventTypeSelect(self.data))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_alliance_type_back", row=1)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🌐 Allianz-Gruppe wählen", description="Wähle die vorhandene Allianz-Gruppe aus.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminAllianceGroupSelectView(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_alliance_type_cancel", row=1)
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(embed=discord.Embed(title="Abgebrochen", description="Das Allianz-Event wurde nicht erstellt.", color=discord.Color.orange()), view=None)


class AdminAllianceEventTypeSelect(Select):
    def __init__(self, data: dict):
        self.data = dict(data)
        rsvp = _admin_event_module()
        event_types = list(getattr(rsvp, "ALLIANCE_EVENT_TYPES", ["NM Raid", "HM Raid", "PvP Schlacht", "Dimensionsprüfung"]))
        options = [discord.SelectOption(label=x, value=x) for x in event_types[:25]]
        super().__init__(placeholder="Allianz-Eventtyp wählen", min_values=1, max_values=1, options=options, custom_id="admin_alliance_event_type_select")

    async def callback(self, inter: discord.Interaction):
        self.data["event_type"] = str(self.values[0])
        emb = discord.Embed(
            title=f"{EMOJI_TARGET} Home-Zielrolle wählen",
            description=(
                "Optional: Wähle eine Home-/Ebolus-Rolle, die DMs erhalten soll.\n\n"
                "Oder wähle **Alle / keine Zielrolle**, wenn alle Ebolus-Mitglieder zählen sollen."
            ),
            color=discord.Color.gold(),
        )
        await inter.response.edit_message(embed=emb, view=AdminAllianceRoleSelectView(self.data, inter.client))


class AdminAllianceRoleSelectView(View):
    def __init__(self, data: dict, client: discord.Client | None = None):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminAllianceRoleSelect(self.data, client))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_alliance_role_back", row=1)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🌐 Allianz-Eventtyp wählen", description="Wähle den Eventtyp.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminAllianceEventTypeSelectView(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_alliance_role_cancel", row=1)
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(embed=discord.Embed(title="Abgebrochen", description="Das Allianz-Event wurde nicht erstellt.", color=discord.Color.orange()), view=None)


class AdminAllianceRoleSelect(Select):
    def __init__(self, data: dict, client: discord.Client | None = None):
        self.data = dict(data)
        guild_id = int(self.data.get("guild_id", 0) or 0)
        guild = client.get_guild(guild_id) if client is not None else None
        options = [discord.SelectOption(label="Alle / keine Zielrolle", value="0", description="Alle Ebolus-Mitglieder, die DMs aktiviert haben")]

        if guild is not None:
            roles = [r for r in guild.roles if not r.is_default()]
            roles.sort(key=lambda r: r.position, reverse=True)
            for role in roles[:24]:
                options.append(discord.SelectOption(label=role.name[:100], value=str(role.id), description=f"{len(role.members)} Mitglieder"[:100]))

        super().__init__(placeholder="Home-Zielrolle wählen", min_values=1, max_values=1, options=options, custom_id="admin_alliance_role_select")

    async def callback(self, inter: discord.Interaction):
        try:
            role_id = int(self.values[0])
        except Exception:
            role_id = 0
        self.data["target_role_id"] = int(role_id or 0)
        emb = discord.Embed(
            title="🖼️ Allianz-Event-Bild wählen",
            description=(
                "Wähle, welches Bild für dieses Allianz-Event verwendet werden soll.\n\n"
                "Danach wird das Allianz-Event über das vorhandene Allianz-System erstellt und gespiegelt."
            ),
            color=discord.Color.gold(),
        )
        await inter.response.edit_message(embed=emb, view=AdminEventImageSelectView(self.data))


class AdminEventChannelSelectView(View):
    def __init__(self, data: dict, client: discord.Client | None = None):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminEventChannelSelect(self.data, client))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_channel_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Das Event wurde nicht erstellt.",
                color=discord.Color.orange()
            ),
            view=None
        )


class AdminEventChannelSelect(Select):
    def __init__(self, data: dict, client: discord.Client | None = None):
        self.data = dict(data)
        guild_id = int(self.data.get("guild_id", 0) or 0)
        guild = client.get_guild(guild_id) if client is not None else None

        options: list[discord.SelectOption] = []

        if guild is not None:
            channels = []

            for ch in guild.text_channels:
                try:
                    me = guild.me
                    perms = ch.permissions_for(me) if me else None

                    if perms and not (perms.view_channel and perms.send_messages):
                        continue

                    channels.append(ch)
                except Exception:
                    channels.append(ch)

            channels.sort(key=lambda c: (c.category.name.lower() if c.category else "", c.position, c.name.lower()))

            for ch in channels[:25]:
                category = ch.category.name if ch.category else "Ohne Kategorie"
                options.append(
                    discord.SelectOption(
                        label=f"#{ch.name}"[:100],
                        value=str(ch.id),
                        description=category[:100]
                    )
                )

        if not options:
            options.append(
                discord.SelectOption(
                    label="Keine Kanäle gefunden",
                    value="0",
                    description="Bot findet keinen beschreibbaren Textkanal."
                )
            )

        super().__init__(
            placeholder="Zielkanal wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="admin_event_channel_select"
        )

    async def callback(self, inter: discord.Interaction):
        try:
            channel_id = int(self.values[0])
        except Exception:
            channel_id = 0

        if not channel_id:
            await inter.response.send_message("❌ Kein gültiger Kanal gefunden. Prüfe, ob der Bot Zugriff auf Server-Textkanäle hat.", ephemeral=True)
            return

        self.data["channel_id"] = channel_id

        emb = discord.Embed(
            title=f"{EMOJI_TARGET} Zielrolle wählen",
            description=(
                "Wähle die Rolle, die für dieses Event angeschrieben werden soll.\n\n"
                "Oder wähle **Alle / keine Zielrolle**, wenn alle Servermitglieder zählen sollen."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=AdminEventRoleSelectView(self.data, inter.client))


class AdminEventRoleSelectView(View):
    def __init__(self, data: dict, client: discord.Client | None = None):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminEventRoleSelect(self.data, client))

    async def _go_image_select(self, inter: discord.Interaction, target_role_id: int):
        self.data["target_role_id"] = int(target_role_id or 0)
        emb = discord.Embed(
            title="🖼️ Event-Bild wählen",
            description=(
                "Wähle, welches Bild für dieses Event verwendet werden soll.\n\n"
                "**Kein Bild** erstellt den Raid ohne Bild.\n"
                "**Eigene URL** öffnet danach ein Eingabefeld für deinen Bildlink."
            ),
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminEventImageSelectView(self.data))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_event_role_back", row=1)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="📍 Zielkanal wählen",
            description="Wähle den Discord-Kanal, in dem der Raid/Event-Post erscheinen soll.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminEventChannelSelectView(self.data, inter.client))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_role_cancel", row=2)
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Das Event wurde nicht erstellt.",
                color=discord.Color.orange()
            ),
            view=None
        )


class AdminEventRoleSelect(Select):
    def __init__(self, data: dict, client: discord.Client | None = None):
        self.data = dict(data)
        guild_id = int(self.data.get("guild_id", 0) or 0)
        guild = client.get_guild(guild_id) if client is not None else None

        options: list[discord.SelectOption] = [
            discord.SelectOption(
                label="Alle / keine Zielrolle",
                value="0",
                description="Alle Servermitglieder zählen als Zielgruppe."
            )
        ]

        if guild is not None:
            roles = [r for r in guild.roles if not r.is_default() and not r.managed]
            roles.sort(key=lambda r: (-r.position, r.name.lower()))

            for role in roles[:24]:
                options.append(
                    discord.SelectOption(
                        label=f"@{role.name}"[:100],
                        value=str(role.id),
                        description=f"Mitglieder: {len(role.members)}"[:100]
                    )
                )

        super().__init__(
            placeholder="Zielrolle wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="admin_event_role_select"
        )

    async def callback(self, inter: discord.Interaction):
        try:
            role_id = int(self.values[0])
        except Exception:
            role_id = 0

        self.data["target_role_id"] = int(role_id or 0)

        guild = inter.client.get_guild(int(self.data.get("guild_id", 0) or 0))
        role_text = "Alle / keine Zielrolle"

        if guild and role_id:
            role = guild.get_role(role_id)
            if role:
                role_text = role.mention

        emb = discord.Embed(
            title="🖼️ Event-Bild wählen",
            description=(
                f"Zielrolle: {role_text}\n\n"
                "Wähle, welches Bild für dieses Event verwendet werden soll.\n\n"
                "**Kein Bild** erstellt den Raid ohne Bild.\n"
                "**Eigene URL** öffnet danach ein Eingabefeld für deinen Bildlink."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=AdminEventImageSelectView(self.data))


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
        self.data["image_url"] = image_url or ""

        if str(self.data.get("scope", "")) == "alliance":
            await inter.response.defer(ephemeral=True, thinking=True)
            await _admin_create_alliance_raid_from_menu(
                inter,
                int(self.data["guild_id"]),
                str(self.data.get("alliance_group", "")),
                str(self.data.get("event_type", "")),
                str(self.data["title"]),
                str(self.data["date_text"]),
                str(self.data["time_text"]),
                int(self.data.get("target_role_id", 0) or 0),
                str(self.data.get("description", "")),
                image_url=(str(self.data.get("image_url", "") or "").strip() or None),
            )
            return

        await _admin_show_reminder_select(inter, self.data)


def _admin_reminder_options(value: str) -> list[dict]:
    if value == "none":
        return []
    if value == "24":
        return [{"minutes": 1440, "target": "missing"}]
    if value == "2":
        return [{"minutes": 120, "target": "missing"}]
    if value == "30":
        return [{"minutes": 30, "target": "yes"}]
    if value == "24_2":
        return [{"minutes": 1440, "target": "missing"}, {"minutes": 120, "target": "missing"}]
    if value == "all":
        return [
            {"minutes": 1440, "target": "missing"},
            {"minutes": 120, "target": "missing"},
            {"minutes": 30, "target": "yes"},
        ]
    return []


async def _admin_show_reminder_select(inter: discord.Interaction, data: dict, send_new: bool = False):
    emb = discord.Embed(
        title="⏰ Reminder wählen",
        description=(
            "Wähle, welche automatischen Erinnerungen für dieses Event aktiv sein sollen.\n\n"
            "24h/2h gehen an Leute, die noch nicht abgestimmt haben.\n"
            "30min geht an angemeldete Teilnehmer."
        ),
        color=discord.Color.gold(),
    )
    view = AdminEventReminderSelectView(data)
    if send_new:
        await inter.response.send_message(embed=emb, view=view, ephemeral=True)
    else:
        await inter.response.edit_message(embed=emb, view=view)


class AdminEventReminderSelectView(View):
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminEventReminderSelect(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_reminder_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(
                title="Abgebrochen",
                description="Das Event wurde nicht erstellt.",
                color=discord.Color.orange(),
            ),
            view=None,
        )


class AdminEventReminderSelect(Select):
    def __init__(self, data: dict):
        self.data = dict(data)
        options = [
            discord.SelectOption(label="Kein Reminder", value="none", description="Keine automatische Erinnerung"),
            discord.SelectOption(label="24h vorher", value="24", description="An alle ohne Antwort"),
            discord.SelectOption(label="2h vorher", value="2", description="An alle ohne Antwort"),
            discord.SelectOption(label="30min vorher", value="30", description="An angemeldete Teilnehmer"),
            discord.SelectOption(label="24h + 2h vorher", value="24_2", description="An alle ohne Antwort"),
            discord.SelectOption(label="24h + 2h + 30min", value="all", description="Fehlende + Teilnehmer kurz vorher"),
        ]
        super().__init__(
            placeholder="Reminder wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="admin_event_reminder_select",
        )

    async def callback(self, inter: discord.Interaction):
        reminders = _admin_reminder_options(self.values[0])
        self.data["reminders"] = reminders
        await _admin_show_voice_select(inter, self.data)


async def _admin_show_voice_select(inter: discord.Interaction, data: dict):
    guild = inter.client.get_guild(int(data.get("guild_id", 0) or 0))
    c = _gcfg(guild.id) if guild else {}
    category_id = int(c.get("event_voice_category_id", 0) or 0)
    return_id = int(c.get("event_voice_return_channel_id", 0) or 0)

    category_txt = "Nicht gesetzt"
    return_txt = "Nicht gesetzt"

    if guild and category_id:
        category = guild.get_channel(category_id)
        if isinstance(category, discord.CategoryChannel):
            category_txt = category.name

    if guild and return_id:
        channel = guild.get_channel(return_id)
        if isinstance(channel, discord.VoiceChannel):
            return_txt = channel.name

    emb = discord.Embed(
        title="🔊 Event-Voice wählen",
        description=(
            "Soll für dieses Event automatisch ein Voice-Channel erstellt werden?\n\n"
            f"Kategorie: **{category_txt}**\n"
            f"Sammel-Voice nach Event: **{return_txt}**\n\n"
            "Der Event-Voice wird nach Eventende geschlossen. Wenn ein Sammel-Voice gesetzt ist, werden Mitglieder vorher dorthin verschoben."
        ),
        color=discord.Color.gold(),
    )
    await inter.response.edit_message(embed=emb, view=AdminEventVoiceSelectView(data))


class AdminEventVoiceSelectView(View):
    def __init__(self, data: dict):
        super().__init__(timeout=None)
        self.data = dict(data)
        self.add_item(AdminEventVoiceSelect(self.data))

    @button(label="❌ Abbrechen", style=ButtonStyle.secondary, custom_id="admin_event_voice_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        await inter.response.edit_message(
            embed=discord.Embed(title="Abgebrochen", description="Das Event wurde nicht erstellt.", color=discord.Color.orange()),
            view=None,
        )


class AdminEventVoiceSelect(Select):
    def __init__(self, data: dict):
        self.data = dict(data)
        options = [
            discord.SelectOption(label="Kein Voice-Channel", value="no", description="Event ohne eigenen Voice erstellen"),
            discord.SelectOption(label="Voice-Channel erstellen", value="yes", description="Bot erstellt einen temporären Event-Voice"),
        ]
        super().__init__(
            placeholder="Voice-Option wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="admin_event_voice_select",
        )

    async def callback(self, inter: discord.Interaction):
        voice_enabled = self.values[0] == "yes"
        guild = inter.client.get_guild(int(self.data.get("guild_id", 0) or 0))
        c = _gcfg(guild.id) if guild else {}

        await inter.response.defer(ephemeral=True, thinking=True)
        await _admin_create_regular_raid_from_menu(
            inter,
            int(self.data["guild_id"]),
            str(self.data["title"]),
            str(self.data["date_text"]),
            str(self.data["time_text"]),
            int(self.data.get("channel_id", 0) or 0),
            int(self.data.get("target_role_id", 0) or 0),
            str(self.data.get("description", "")),
            image_url=(str(self.data.get("image_url", "") or "").strip() or None),
            reminders=list(self.data.get("reminders") or []),
            voice_enabled=voice_enabled,
            voice_category_id=int(c.get("event_voice_category_id", 0) or 0),
            voice_return_channel_id=int(c.get("event_voice_return_channel_id", 0) or 0),
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

        self.data["image_url"] = image_url

        if str(self.data.get("scope", "")) == "alliance":
            await inter.response.defer(ephemeral=True, thinking=True)
            await _admin_create_alliance_raid_from_menu(
                inter,
                int(self.data["guild_id"]),
                str(self.data.get("alliance_group", "")),
                str(self.data.get("event_type", "")),
                str(self.data["title"]),
                str(self.data["date_text"]),
                str(self.data["time_text"]),
                int(self.data.get("target_role_id", 0) or 0),
                str(self.data.get("description", "")),
                image_url=image_url,
            )
            return

        await _admin_show_reminder_select(inter, self.data, send_new=True)




def _attendance_status_label(status: str) -> str:
    status = str(status or "").strip().lower()
    if status == "present":
        return "✅ War da"
    if status == "absent":
        return "❌ Nicht da"
    if status == "excused":
        return "🟡 Entschuldigt"
    return "⚪ Offen"


async def _admin_attendance_events(guild: Optional[discord.Guild]) -> list[dict]:
    if guild is None:
        return []

    try:
        rsvp = _admin_event_module()
        now = datetime.now(rsvp.TZ)

        # Wenn ein Event bereits gestartet ist, aber noch im RSVP-Store liegt,
        # legen wir sofort einen Attendance-Snapshot an.
        for msg_id, obj in list((getattr(rsvp, "store", {}) or {}).items()):
            try:
                if int(obj.get("guild_id", 0) or 0) != int(guild.id):
                    continue
                when = datetime.fromisoformat(str(obj.get("when_iso", "")))
                if when <= now:
                    rsvp.ensure_attendance_snapshot(guild._state._get_client(), str(msg_id), obj)
            except Exception:
                continue

        events = []
        for ev in rsvp.get_attendance_events_for_guild(int(guild.id)):
            try:
                when = datetime.fromisoformat(str(ev.get("when_iso", "")))
                # Anzeige begrenzen: letzte 30 Tage und gestartete Events.
                if when > now:
                    continue
                if when < now - timedelta(days=30):
                    continue
                events.append(ev)
            except Exception:
                continue

        events.sort(key=lambda ev: datetime.fromisoformat(str(ev.get("when_iso", ""))), reverse=True)
        return events[:25]

    except Exception:
        return []


def _admin_attendance_embed(guild: discord.Guild, event: dict) -> discord.Embed:
    participants = event.get("participants") or []
    attendance = event.get("attendance") or {}

    counts = {"present": 0, "absent": 0, "excused": 0, "open": 0}
    lines = []

    for p in participants:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            continue

        status = str((attendance.get(str(uid)) or {}).get("status", "") or "")
        if status in ("present", "absent", "excused"):
            counts[status] += 1
        else:
            counts["open"] += 1

        name = _profile_name(guild, uid, str(p.get("name", "Unbekannt") or "Unbekannt"))
        signup = str(p.get("signup", "") or "")
        suffix = f" — {signup}" if signup else ""
        lines.append(f"• **{name}**{suffix}: {_attendance_status_label(status)}")

    title = str(event.get("title", "Event") or "Event")
    try:
        when = datetime.fromisoformat(str(event.get("when_iso", ""))).strftime("%d.%m.%Y %H:%M")
    except Exception:
        when = "Unbekannt"

    desc = (
        f"**{title}**\n"
        f"{EMOJI_TIME} {when}\n\n"
        f"✅ War da: **{counts['present']}**\n"
        f"❌ Nicht da: **{counts['absent']}**\n"
        f"🟡 Entschuldigt: **{counts['excused']}**\n"
        f"⚪ Offen: **{counts['open']}**\n"
    )

    if lines:
        shown = lines[:20]
        desc += "\n" + "\n".join(shown)
        if len(lines) > 20:
            desc += f"\n… {len(lines) - 20} weitere Teilnehmer"
    else:
        desc += "\nKeine angemeldeten Teilnehmer gefunden."

    emb = discord.Embed(
        title="✅ Anwesenheit prüfen",
        description=desc[:3900],
        color=discord.Color.gold(),
    )
    emb.set_footer(text="Markiert werden nur Spieler, die beim Event angemeldet waren.")
    return emb


class AdminEventSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, events: list[tuple[str, dict]]):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.add_item(AdminEventSelect(guild_id, user_id, action, events))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_event_select_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title=f"{EMOJI_GUILD} Admin – Event", description="Wähle eine Event-Aktion.", color=discord.Color.gold())
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



class AdminAttendanceEventSelectView(View):
    def __init__(self, guild_id: int, user_id: int, events: list[dict]):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.add_item(AdminAttendanceEventSelect(guild_id, user_id, events))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_attendance_event_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title=f"{EMOJI_GUILD} Admin – Event", description="Wähle eine Event-Aktion.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventMenuView())


class AdminAttendanceEventSelect(Select):
    def __init__(self, guild_id: int, user_id: int, events: list[dict]):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        options = []
        for ev in events[:25]:
            event_id = str(ev.get("event_id", "") or ev.get("message_id", ""))
            try:
                when = datetime.fromisoformat(str(ev.get("when_iso", "")))
                label = f"{when.strftime('%d.%m. %H:%M')} – {str(ev.get('title', 'Event'))}"[:100]
            except Exception:
                label = str(ev.get("title", "Event"))[:100]
            count = len(ev.get("participants") or [])
            options.append(discord.SelectOption(label=label, value=event_id, description=f"Teilnehmer: {count}"[:100]))
        if not options:
            options = [discord.SelectOption(label="Keine Events gefunden", value="0")]
        super().__init__(placeholder="Event für Anwesenheit wählen", min_values=1, max_values=1, options=options, custom_id="admin_attendance_event_select")

    async def callback(self, inter: discord.Interaction):
        event_id = str(self.values[0])
        if event_id == "0":
            await inter.response.send_message(f"{EMOJI_CALENDAR} Keine Events gefunden.", ephemeral=True)
            return
        rsvp = _admin_event_module()
        event = rsvp.get_attendance_event(self.guild_id, event_id)
        guild = inter.client.get_guild(self.guild_id)
        if not guild or not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_admin_attendance_embed(guild, event), view=AdminAttendanceMemberSelectView(self.guild_id, self.user_id, event_id, event))


class AdminAttendanceMemberSelectView(View):
    def __init__(self, guild_id: int, user_id: int, event_id: str, event: dict, page: int = 0):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.event_id = str(event_id)
        self.page = max(0, int(page))
        participants = event.get("participants") or []
        max_page = max(0, (len(participants) - 1) // 25)
        if self.page > max_page:
            self.page = max_page
        self.add_item(AdminAttendanceMemberSelect(guild_id, user_id, event_id, event, self.page))

    @button(label="⬅️ Eventliste", style=ButtonStyle.secondary, custom_id="admin_attendance_member_back", row=1)
    async def btn_back(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)
        events = await _admin_attendance_events(guild)
        emb = discord.Embed(title="✅ Admin – Anwesenheit", description="Wähle ein gestartetes Event aus den letzten 30 Tagen.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminAttendanceEventSelectView(self.guild_id, self.user_id, events))

    @button(label="◀️", style=ButtonStyle.secondary, custom_id="admin_attendance_page_prev", row=1)
    async def btn_prev(self, inter: discord.Interaction, _):
        rsvp = _admin_event_module()
        guild = inter.client.get_guild(self.guild_id)
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not guild or not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_admin_attendance_embed(guild, event), view=AdminAttendanceMemberSelectView(self.guild_id, self.user_id, self.event_id, event, max(0, self.page - 1)))

    @button(label="▶️", style=ButtonStyle.secondary, custom_id="admin_attendance_page_next", row=1)
    async def btn_next(self, inter: discord.Interaction, _):
        rsvp = _admin_event_module()
        guild = inter.client.get_guild(self.guild_id)
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not guild or not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        participants = event.get("participants") or []
        max_page = max(0, (len(participants) - 1) // 25)
        await inter.response.edit_message(embed=_admin_attendance_embed(guild, event), view=AdminAttendanceMemberSelectView(self.guild_id, self.user_id, self.event_id, event, min(max_page, self.page + 1)))

class AdminAttendanceMemberSelect(Select):
    def __init__(self, guild_id: int, user_id: int, event_id: str, event: dict, page: int = 0):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.event_id = str(event_id)
        self.page = max(0, int(page))
        participants = event.get("participants") or []
        attendance = event.get("attendance") or {}
        start_i = self.page * 25
        page_participants = participants[start_i:start_i + 25]
        options = []
        # guild ist hier nicht zuverlässig verfügbar, darum nutzen wir gespeicherte Namen.
        for p in page_participants:
            try:
                uid = int(p.get("id", 0) or 0)
            except Exception:
                continue
            name = str(p.get("name", f"User {uid}") or f"User {uid}")
            signup = str(p.get("signup", "") or "")
            status = str((attendance.get(str(uid)) or {}).get("status", "") or "")
            desc = f"{signup} • {_attendance_status_label(status)}" if signup else _attendance_status_label(status)
            options.append(discord.SelectOption(label=name[:100], value=str(uid), description=desc[:100]))
        if not options:
            options = [discord.SelectOption(label="Keine Teilnehmer gefunden", value="0")]
        super().__init__(placeholder=f"Spieler markieren – Seite {self.page + 1}", min_values=1, max_values=1, options=options, custom_id="admin_attendance_member_select")

    async def callback(self, inter: discord.Interaction):
        uid = int(self.values[0])
        if uid == 0:
            await inter.response.send_message("Keine Teilnehmer gefunden.", ephemeral=True)
            return
        rsvp = _admin_event_module()
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        guild = inter.client.get_guild(self.guild_id)
        if not guild or not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return

        name = _profile_name(guild, uid, f"User {uid}")
        emb = discord.Embed(
            title="✅ Anwesenheit markieren",
            description=f"Spieler: **{name}**\nEvent: **{event.get('title', 'Event')}**\n\nWähle den Status.",
            color=discord.Color.gold(),
        )
        await inter.response.edit_message(embed=emb, view=AdminAttendanceMarkView(self.guild_id, self.user_id, self.event_id, uid, self.page))


class AdminAttendanceMarkView(View):
    def __init__(self, guild_id: int, user_id: int, event_id: str, target_user_id: int, page: int = 0):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.event_id = str(event_id)
        self.target_user_id = int(target_user_id)
        self.page = max(0, int(page))

    async def _mark(self, inter: discord.Interaction, status: str):
        rsvp = _admin_event_module()
        ok = rsvp.set_attendance_status(self.guild_id, self.event_id, self.target_user_id, status, inter.user.id)
        guild = inter.client.get_guild(self.guild_id)
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not ok or not guild or not event:
            await inter.response.send_message("❌ Anwesenheit konnte nicht gespeichert werden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_admin_attendance_embed(guild, event), view=AdminAttendanceMemberSelectView(self.guild_id, self.user_id, self.event_id, event, self.page))

    @button(label="✅ War da", style=ButtonStyle.success, custom_id="admin_attendance_mark_present", row=0)
    async def btn_present(self, inter: discord.Interaction, _):
        await self._mark(inter, "present")

    @button(label="❌ Nicht da", style=ButtonStyle.danger, custom_id="admin_attendance_mark_absent", row=0)
    async def btn_absent(self, inter: discord.Interaction, _):
        await self._mark(inter, "absent")

    @button(label="🟡 Entschuldigt", style=ButtonStyle.secondary, custom_id="admin_attendance_mark_excused", row=1)
    async def btn_excused(self, inter: discord.Interaction, _):
        await self._mark(inter, "excused")

    @button(label="⚪ Offen", style=ButtonStyle.secondary, custom_id="admin_attendance_mark_clear", row=1)
    async def btn_clear(self, inter: discord.Interaction, _):
        await self._mark(inter, "clear")

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_attendance_mark_back", row=2)
    async def btn_back(self, inter: discord.Interaction, _):
        rsvp = _admin_event_module()
        guild = inter.client.get_guild(self.guild_id)
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not guild or not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_admin_attendance_embed(guild, event), view=AdminAttendanceMemberSelectView(self.guild_id, self.user_id, self.event_id, event, self.page))


class AdminVoiceSettingsView(View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)

    @button(label="📁 Kategorie setzen", style=ButtonStyle.secondary, custom_id="admin_voice_set_category", row=0)
    async def btn_category(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(
            embed=discord.Embed(title="📁 Event-Voice-Kategorie", description="Wähle die Kategorie, in der Event-Voices erstellt werden sollen.", color=discord.Color.gold()),
            view=AdminVoiceCategorySelectView(self.guild_id, guild),
        )

    @button(label="🔁 Sammel-Voice setzen", style=ButtonStyle.secondary, custom_id="admin_voice_set_return", row=0)
    async def btn_return(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(
            embed=discord.Embed(title="🔁 Sammel-Voice", description="Wähle den Voice-Channel, in den Mitglieder nach Eventende verschoben werden sollen.", color=discord.Color.gold()),
            view=AdminVoiceReturnSelectView(self.guild_id, guild),
        )

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_voice_back", row=1)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title=f"{EMOJI_GUILD} Admin – Event", description="Wähle eine Event-Aktion.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventMenuView())


class AdminVoiceCategorySelectView(View):
    def __init__(self, guild_id: int, guild: discord.Guild):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.add_item(AdminVoiceCategorySelect(guild_id, guild))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_voice_category_back")
    async def btn_back(self, inter: discord.Interaction, _):
        await _admin_show_voice_settings(inter, self.guild_id)


class AdminVoiceCategorySelect(Select):
    def __init__(self, guild_id: int, guild: discord.Guild):
        self.guild_id = int(guild_id)
        categories = sorted(guild.categories, key=lambda c: c.position)[:25]
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in categories]
        if not options:
            options = [discord.SelectOption(label="Keine Kategorie gefunden", value="0")]
        super().__init__(placeholder="Kategorie wählen", min_values=1, max_values=1, options=options, custom_id="admin_voice_category_select")

    async def callback(self, inter: discord.Interaction):
        c = _gcfg(self.guild_id)
        c["event_voice_category_id"] = int(self.values[0])
        cfg[str(self.guild_id)] = c
        save_cfg()
        await inter.response.send_message("✅ Event-Voice-Kategorie gespeichert.", ephemeral=True)


class AdminVoiceReturnSelectView(View):
    def __init__(self, guild_id: int, guild: discord.Guild):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.add_item(AdminVoiceReturnSelect(guild_id, guild))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="admin_voice_return_back")
    async def btn_back(self, inter: discord.Interaction, _):
        await _admin_show_voice_settings(inter, self.guild_id)


class AdminVoiceReturnSelect(Select):
    def __init__(self, guild_id: int, guild: discord.Guild):
        self.guild_id = int(guild_id)
        channels = sorted(guild.voice_channels, key=lambda c: (c.category.position if c.category else 999, c.position, c.name.lower()))[:25]
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id), description=(c.category.name[:100] if c.category else "Keine Kategorie")) for c in channels]
        if not options:
            options = [discord.SelectOption(label="Kein Voice-Channel gefunden", value="0")]
        super().__init__(placeholder="Sammel-Voice wählen", min_values=1, max_values=1, options=options, custom_id="admin_voice_return_select")

    async def callback(self, inter: discord.Interaction):
        c = _gcfg(self.guild_id)
        c["event_voice_return_channel_id"] = int(self.values[0])
        cfg[str(self.guild_id)] = c
        save_cfg()
        await inter.response.send_message("✅ Sammel-Voice gespeichert.", ephemeral=True)


async def _admin_show_voice_settings(inter: discord.Interaction, guild_id: int):
    guild = inter.client.get_guild(int(guild_id))
    c = _gcfg(int(guild_id))
    category_id = int(c.get("event_voice_category_id", 0) or 0)
    return_id = int(c.get("event_voice_return_channel_id", 0) or 0)

    category_txt = "Nicht gesetzt"
    return_txt = "Nicht gesetzt"

    if guild and category_id:
        category = guild.get_channel(category_id)
        if isinstance(category, discord.CategoryChannel):
            category_txt = category.name

    if guild and return_id:
        channel = guild.get_channel(return_id)
        if isinstance(channel, discord.VoiceChannel):
            return_txt = channel.name

    emb = discord.Embed(
        title="🔊 Voice-Einstellungen",
        description=(
            f"Event-Voice-Kategorie: **{category_txt}**\n"
            f"Sammel-Voice nach Event: **{return_txt}**\n\n"
            "Diese Einstellungen werden verwendet, wenn beim Event-Erstellen `Voice-Channel erstellen` gewählt wird."
        ),
        color=discord.Color.gold(),
    )
    await inter.response.edit_message(embed=emb, view=AdminVoiceSettingsView(int(guild_id)))


class AdminMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Event", emoji=_menu_emoji(EMOJI_EBOLUS), style=ButtonStyle.secondary, custom_id="portal_admin_event")
    async def btn_event(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        emb = discord.Embed(
            title=f"{EMOJI_GUILD} Admin – Event",
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

    @button(label="Loot", emoji=_menu_emoji(EMOJI_LOOT), style=ButtonStyle.secondary, custom_id="portal_admin_loot")
    async def btn_loot(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        emb = discord.Embed(
            title=f"{EMOJI_LOOT} Admin – Loot",
            description="Wähle eine Loot-Aktion.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminLootMenuView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_admin_back")
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
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)
        await inter.response.send_modal(AdminAllianceEventCreateModal(guild.id, member.id))

    @button(label="🗑️ Event löschen", style=ButtonStyle.secondary, custom_id="portal_admin_event_delete", row=1)
    async def btn_delete(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        events = _admin_active_rsvp_events(guild)
        if not events:
            await inter.response.send_message(f"{EMOJI_CALENDAR} Keine aktiven Events gefunden.", ephemeral=True)
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
            await inter.response.send_message(f"{EMOJI_CALENDAR} Keine aktiven Events gefunden.", ephemeral=True)
            return
        emb = discord.Embed(title="📨 Resend Missing", description="Wähle das Event, für das fehlende Abstimmungen erneut gesendet werden sollen.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminEventSelectView(guild.id, member.id, "resend", events))

    @button(label="✅ Anwesenheit", style=ButtonStyle.secondary, custom_id="portal_admin_event_attendance", row=2)
    async def btn_attendance(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        events = await _admin_attendance_events(guild)
        if not events:
            await inter.response.send_message(f"{EMOJI_CALENDAR} Keine gestarteten Events für Anwesenheit gefunden.", ephemeral=True)
            return
        emb = discord.Embed(title="✅ Admin – Anwesenheit", description="Wähle ein gestartetes Event aus den letzten 30 Tagen.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminAttendanceEventSelectView(guild.id, member.id, events))

    @button(label="🔊 Voice-Einstellungen", style=ButtonStyle.secondary, custom_id="portal_admin_event_voice_settings", row=3)
    async def btn_voice_settings(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)
        if not _is_portal_admin(guild, member):
            await inter.response.send_message("❌ Dieser Bereich ist nur für Gildenleitung, Berater oder Wächter.", ephemeral=True)
            return
        await _admin_show_voice_settings(inter, guild.id)

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_admin_event_back", row=4)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title=f"{EMOJI_ADMIN} Admin", description="Wähle einen Bereich.", color=discord.Color.gold())
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

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_admin_loot_back", row=3)
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title=f"{EMOJI_ADMIN} Admin", description="Wähle einen Bereich.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminMenuView())


class SupportMenuView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Leaderkontakt", emoji=_menu_emoji(EMOJI_CONTACT), style=ButtonStyle.secondary, custom_id="portal_support_leader")
    async def btn_leader(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(PortalLeaderContactModal(guild.id, inter.user.id))

    @button(label="Hilfe", emoji=_menu_emoji(EMOJI_HELP), style=ButtonStyle.secondary, custom_id="portal_support_help")
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
                f"**{EMOJI_ABSENCE} Abwesenheit**\n"
                "Hier meldest du Urlaub, Schicht oder längere Inaktivität.\n\n"
                f"**{EMOJI_CALENDAR} Kalender & Abwesenheiten**\n"
                "Zeigt feste Gildentermine und aktuelle/kommende Abwesenheiten.\n\n"
                "**📜 Regeln & Loot**\n"
                "Zeigt die wichtigsten Gildenregeln und das Lootsystem.\n\n"
                "**🛡️ Leaderkontakt**\n"
                "Schickt eine Anfrage direkt an die Gildenleitung."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(embed=emb, view=HelpView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_support_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class ProfileView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Profil bearbeiten", emoji=_menu_emoji(EMOJI_PERSONAL), style=ButtonStyle.secondary, custom_id="portal_profile_edit")
    async def btn_edit(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(ProfileEditModal(guild.id, inter.user.id))

    @button(label="Mitglieder", emoji=_menu_emoji(EMOJI_MEMBER), style=ButtonStyle.secondary, custom_id="portal_member_list")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_members_list_embed(guild), view=BackOnlyView())

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_profile_back")
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

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_events_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title=f"{EMOJI_GUILD} Gilde",
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

    @button(label="Needliste öffnen", emoji=_menu_emoji(EMOJI_LOOT), style=ButtonStyle.secondary, custom_id="rules_open_need")
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

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="rules_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class HelpView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Leaderkontakt", emoji=_menu_emoji(EMOJI_CONTACT), style=ButtonStyle.secondary, custom_id="help_leader_contact")
    async def btn_leader(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(PortalLeaderContactModal(guild.id, inter.user.id))

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="help_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())


class BackOnlyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Zurück", emoji=_menu_emoji(EMOJI_BACK), style=ButtonStyle.secondary, custom_id="portal_back_main")
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

    @tree.command(name="portal_set_guild_info", description="(Admin) Mitteilung im Gildenmenü setzen oder leeren")
    async def portal_set_guild_info(inter: discord.Interaction, text: str = ""):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins/Manage Guild.", ephemeral=True)
            return

        if not inter.guild_id:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        cleaned = _safe_text(str(text or "").strip())

        if len(cleaned) > 900:
            await inter.response.send_message("❌ Die Gildeninfo ist zu lang. Bitte maximal ca. 900 Zeichen nutzen.", ephemeral=True)
            return

        c["guild_info_text"] = cleaned
        save_cfg()

        if cleaned:
            await inter.response.send_message("✅ Gildeninfo für das Gildenmenü gesetzt. Nutze `/portal_send_all force:false`, um bestehende Menüs zu aktualisieren.", ephemeral=True)
        else:
            await inter.response.send_message("✅ Gildeninfo geleert. Nutze `/portal_send_all force:false`, um bestehende Menüs zu aktualisieren.", ephemeral=True)

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
            await inter.response.send_message(f"{EMOJI_CALENDAR} Keine Events gespeichert.", ephemeral=True)
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
