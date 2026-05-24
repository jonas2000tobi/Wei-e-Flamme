from __future__ import annotations

import json
import re
import asyncio
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Any, Tuple

import discord
from discord import app_commands
from discord.ui import View, button, Modal, TextInput
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

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


def _is_absent_now(absence: dict) -> bool:
    try:
        from_s = str(absence.get("from", "")).strip()
        to_s = str(absence.get("to", "")).strip()

        if not _valid_ddmm(from_s) or not _valid_ddmm(to_s):
            return False

        today = datetime.now(TZ).date()
        from_d = _ddmm_to_date(from_s, today.year)
        to_d = _ddmm_to_date(to_s, today.year)

        if to_d < from_d:
            to_d = date(today.year + 1, to_d.month, to_d.day)

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


def _main_menu_embed(guild: discord.Guild) -> discord.Embed:
    emb = discord.Embed(
        title="🏰 ebolus – Gildenmenü",
        description="Wähle unten aus, was du öffnen möchtest.",
        color=discord.Color.blurple()
    )
    emb.set_footer(text=f"Server: {guild.name}")
    return emb


def _profile_embed(guild: discord.Guild, member: discord.Member) -> discord.Embed:
    p = _user_profile(guild.id, member.id)

    ingame = p.get("ingame_name") or _display_name(member)
    main_role = p.get("main_role") or "Nicht gesetzt"
    gearscore = p.get("gearscore") or "Nicht gesetzt"

    emb = discord.Embed(
        title="👤 Dein Gildenprofil",
        color=discord.Color.blurple()
    )

    emb.add_field(name="Ingame-Name", value=str(ingame), inline=False)
    emb.add_field(name="Main-Rolle", value=str(main_role), inline=True)
    emb.add_field(name="Gearscore", value=str(gearscore), inline=True)
    emb.add_field(
        name="Seit",
        value=f"{_guild_days(member)} Tage in der Gilde\nBeigetreten am: {_guild_join_date(member)}",
        inline=False
    )
    emb.add_field(name="Position", value=_member_position(guild, member), inline=True)
    emb.add_field(name="Status", value=_status_for_user(guild.id, member.id), inline=True)

    emb.set_footer(text="Bearbeitbar: Ingame-Name, Main-Rolle, Gearscore")

    return emb


def _events_embed(guild_id: int) -> discord.Embed:
    c = _gcfg(guild_id)
    events = c.get("events") or []

    emb = discord.Embed(
        title="📅 Gildenkalender – ebolus",
        color=discord.Color.green()
    )

    if not events:
        emb.description = "Aktuell sind keine festen Gilden-Events hinterlegt."
        return emb

    lines = []

    for e in events:
        weekday = str(e.get("weekday", "—"))
        time = str(e.get("time", "—"))
        title = str(e.get("title", "Event"))
        lines.append(f"**{weekday}, {time} Uhr**\n{title}")

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
        title="👥 Mitgliederliste – ebolus",
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


def _rules_loot_embed() -> discord.Embed:
    emb = discord.Embed(
        title="📜 Regeln & Lootsystem – ebolus",
        color=discord.Color.orange()
    )

    emb.add_field(
        name="📌 Grundregeln",
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
    try:
        msg = await user.send(embed=_main_menu_embed(guild), view=MemberPortalMainView())
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
            else:
                await msg.edit(embed=_main_menu_embed(guild), view=MemberPortalMainView())

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
        "🏰 ebolus – Gildenmenü",
        "👤 Dein Gildenprofil",
        "📅 Gildenkalender – ebolus",
        "📅 Gilden-Events",
        "❓ Hilfe – ebolus Gildenbot",
        "👥 Mitgliederliste – ebolus",
        "📜 Regeln & Lootsystem – ebolus",
        "🎁 Needliste – ebolus",
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
                color=discord.Color.orange(),
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
            color=discord.Color.blurple(),
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

    @button(label="📬 Gildenmenü im Privatchat öffnen", style=ButtonStyle.primary, custom_id="portal_open_dm")
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


class MemberPortalMainView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _guild_member(self, inter: discord.Interaction) -> tuple[Optional[discord.Guild], Optional[discord.Member]]:
        return await _resolve_guild_member_from_inter(inter)

    @button(label="👤 Mein Profil", style=ButtonStyle.primary, custom_id="portal_profile", row=0)
    async def btn_profile(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        _mark_portal_sent(guild.id, member.id, inter.message.id if inter.message else None)

        await inter.response.edit_message(
            embed=_profile_embed(guild, member),
            view=ProfileView()
        )

    @button(label="📅 Gilden-Events", style=ButtonStyle.secondary, custom_id="portal_events", row=0)
    async def btn_events(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(
            embed=_events_embed(guild.id),
            view=EventsInfoView()
        )

    @button(label="🏖️ Abwesenheit melden", style=ButtonStyle.secondary, custom_id="portal_absence", row=0)
    async def btn_absence(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(AbsenceModal(guild.id, inter.user.id))

    @button(label="📨 Leader kontaktieren", style=ButtonStyle.primary, custom_id="portal_leader_contact", row=1)
    async def btn_leader_contact(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(PortalLeaderContactModal(guild.id, inter.user.id))

    @button(label="📜 Regeln & Loot", style=ButtonStyle.secondary, custom_id="portal_rules_loot", row=1)
    async def btn_rules_loot(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(embed=_rules_loot_embed(), view=BackOnlyView())

    @button(label="🎁 Needliste", style=ButtonStyle.secondary, custom_id="portal_needlist", row=1)
    async def btn_needlist(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

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
                color=discord.Color.orange()
            )
            await inter.response.edit_message(embed=emb, view=BackOnlyView())

    @button(label="❓ Hilfe", style=ButtonStyle.secondary, custom_id="portal_help", row=2)
    async def btn_help(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = discord.Embed(
            title="❓ Hilfe – ebolus Gildenbot",
            description=(
                "**Mein Profil**\n"
                "Zeigt dein Gildenprofil. Du kannst Ingame-Name, Main-Rolle und Gearscore bearbeiten.\n\n"
                "**Gilden-Events**\n"
                "Zeigt feste Gildentermine. Das ist nur eine Übersicht, keine Anmeldung.\n\n"
                "**Abwesenheit melden**\n"
                "Meldet deine Abwesenheit an die Gildenleitung und speichert sie.\n\n"
                "**Leader kontaktieren**\n"
                "Schickt eine Anfrage direkt an die Gildenleitung.\n\n"
                "**Regeln & Loot**\n"
                "Zeigt die wichtigsten Gildenregeln und das Lootsystem.\n\n"
                "**Needliste**\n"
                "Hier trägst du später deine gewünschten Items ein.\n\n"
                "**Mitgliederliste**\n"
                "Sortiert nach Rang und Gearscore. Angezeigt werden nur Mitglieder mit der Ebolus-/Gildenmitglied-Rolle."
            ),
            color=discord.Color.blurple()
        )

        await inter.response.edit_message(embed=emb, view=BackOnlyView())


class ProfileView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _guild_member(self, inter: discord.Interaction) -> tuple[Optional[discord.Guild], Optional[discord.Member]]:
        return await _resolve_guild_member_from_inter(inter)

    @button(label="✏️ Profil bearbeiten", style=ButtonStyle.primary, custom_id="portal_profile_edit")
    async def btn_edit(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.send_modal(ProfileEditModal(guild.id, inter.user.id))

    @button(label="👥 Mitgliederliste", style=ButtonStyle.secondary, custom_id="portal_member_list")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        if member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        await inter.response.edit_message(
            embed=_members_list_embed(guild),
            view=BackOnlyView()
        )

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_profile_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = _main_menu_embed(guild) if guild else discord.Embed(
            title="🏰 ebolus – Gildenmenü",
            description="Wähle unten aus, was du öffnen möchtest.",
            color=discord.Color.blurple()
        )

        await inter.response.edit_message(embed=emb, view=MemberPortalMainView())


class EventsInfoView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="🔄 Aktualisieren", style=ButtonStyle.primary, custom_id="portal_events_refresh")
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

        emb = _main_menu_embed(guild) if guild else discord.Embed(
            title="🏰 ebolus – Gildenmenü",
            description="Wähle unten aus, was du öffnen möchtest.",
            color=discord.Color.blurple()
        )

        await inter.response.edit_message(embed=emb, view=MemberPortalMainView())


class BackOnlyView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_back_main")
    async def btn_back(self, inter: discord.Interaction, _):
        guild, member = await _resolve_guild_member_from_inter(inter)

        if guild and member and inter.message:
            _mark_portal_sent(guild.id, member.id, inter.message.id)

        emb = _main_menu_embed(guild) if guild else discord.Embed(
            title="🏰 ebolus – Gildenmenü",
            description="Wähle unten aus, was du öffnen möchtest.",
            color=discord.Color.blurple()
        )

        await inter.response.edit_message(embed=emb, view=MemberPortalMainView())


async def setup_member_portal(client: discord.Client, tree: app_commands.CommandTree):
    try:
        client.add_view(PortalOpenView())
        client.add_view(MemberPortalMainView())
        client.add_view(ProfileView())
        client.add_view(EventsInfoView())
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
            f"🏰 Aktives Gildenmenü wurde geschützt.",
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
            f"🏰 Aktive Gildenmenüs wurden geschützt.",
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
            title="🏰 ebolus – Gildenbot",
            description=(
                "Öffne hier dein persönliches Gildenmenü im Privatchat.\n\n"
                "Dort findest du:\n"
                "• dein Profil\n"
                "• feste Gilden-Events\n"
                "• Abwesenheit melden\n"
                "• Leader kontaktieren\n"
                "• Regeln & Loot\n"
                "• Needliste\n"
                "• Hilfe"
            ),
            color=discord.Color.blurple()
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
