from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Any, List

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


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


cfg: dict = _load_json(CFG_FILE, {})
profiles: dict = _load_json(PROFILE_FILE, {})


def save_cfg() -> None:
    _save_json(CFG_FILE, cfg)


def save_profiles() -> None:
    _save_json(PROFILE_FILE, profiles)


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _gcfg(guild_id: int) -> dict:
    c = cfg.get(str(guild_id)) or {}
    c.setdefault("absence_channel_id", 0)
    c.setdefault("portal_post_channel_id", 0)
    c.setdefault("portal_post_message_id", 0)
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
        return "Berater"
    if guardian and guardian in member.roles:
        return "Wächter"

    return "Mitglied"


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


def _absence_not_expired(absence: dict) -> bool:
    try:
        to_s = str(absence.get("to", "")).strip()
        if not _valid_ddmm(to_s):
            return False

        today = datetime.now(TZ).date()
        to_d = _ddmm_to_date(to_s, today.year)

        if to_d < today:
            return False

        return True
    except Exception:
        return False


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
        return f"Abwesend bis {absence.get('to', '—')}"
    return "Aktiv"


def _absence_label(guild_id: int, user_id: int) -> str:
    absence = _absence_for_user(guild_id, user_id)
    if absence and _absence_not_expired(absence):
        return f"abwesend bis {absence.get('to', '—')}"
    return "aktiv"


def _profile_embed(guild: discord.Guild, member: discord.Member) -> discord.Embed:
    p = _user_profile(guild.id, member.id)

    ingame = p.get("ingame_name") or member.display_name
    main_role = p.get("main_role") or "Nicht gesetzt"
    gearscore = p.get("gearscore") or "Nicht gesetzt"

    emb = discord.Embed(
        title="👤 Dein Gildenprofil",
        color=discord.Color.blurple()
    )

    emb.add_field(name="Ingame-Name", value=str(ingame), inline=False)
    emb.add_field(name="Main-Rolle", value=str(main_role), inline=True)
    emb.add_field(name="Gearscore", value=str(gearscore), inline=True)
    emb.add_field(name="Seit", value=f"{_guild_days(member)} Tagen in der Gilde", inline=False)
    emb.add_field(name="Position", value=_member_position(guild, member), inline=True)
    emb.add_field(name="Status", value=_status_for_user(guild.id, member.id), inline=True)

    emb.set_footer(text="Bearbeitbar: Ingame-Name, Main-Rolle, Gearscore")

    return emb


def _events_embed(guild_id: int) -> discord.Embed:
    c = _gcfg(guild_id)
    events = c.get("events") or []

    emb = discord.Embed(
        title="📅 Gilden-Events",
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
        lines.append(f"**{weekday}, {time} Uhr** – {title}")

    emb.description = "\n".join(lines)
    emb.set_footer(text="Reine Übersicht. Keine Anmeldung / keine RSVP-Funktion.")

    return emb


def _members_list_embed(guild: discord.Guild) -> discord.Embed:
    emb = discord.Embed(
        title="👥 Mitgliederliste – ebolus",
        color=discord.Color.gold()
    )

    lines = []

    members = [m for m in guild.members if not m.bot]
    members.sort(key=lambda m: m.display_name.lower())

    for m in members[:40]:
        p = _user_profile(guild.id, m.id)
        ingame = p.get("ingame_name") or m.display_name
        main_role = p.get("main_role") or "—"
        gs = p.get("gearscore") or "—"
        pos = _member_position(guild, m)
        status = _absence_label(guild.id, m.id)

        lines.append(f"• **{ingame}** – {main_role} – GS {gs} – {pos} – {status}")

    if not lines:
        emb.description = "Keine Mitglieder gefunden."
    else:
        emb.description = "\n".join(lines)

    if len(members) > 40:
        emb.set_footer(text=f"Anzeige begrenzt auf 40 von {len(members)} Mitgliedern.")

    return emb


async def _send_main_menu(user: discord.abc.User, guild: discord.Guild) -> None:
    emb = discord.Embed(
        title="🏰 ebolus – Gildenmenü",
        description=(
            "Wähle unten aus, was du öffnen möchtest.\n\n"
            "Dieses Menü funktioniert direkt im Privatchat mit dem Bot."
        ),
        color=discord.Color.blurple()
    )

    emb.set_footer(text=f"Server: {guild.name}")

    await user.send(embed=emb, view=MemberPortalMainView())


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
        p = _user_profile(self.guild_id, self.user_id)
        p["ingame_name"] = str(self.ingame_name.value).strip()
        p["main_role"] = str(self.main_role.value).strip()
        p["gearscore"] = str(self.gearscore.value).strip()

        save_profiles()

        guild = inter.client.get_guild(self.guild_id)
        member = guild.get_member(self.user_id) if guild else None

        if guild and member:
            await inter.response.send_message(
                embed=_profile_embed(guild, member),
                view=ProfileView(),
            )
        else:
            await inter.response.send_message("✅ Profil gespeichert.", view=MemberPortalMainView())


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
            await inter.response.send_message(
                "❌ Datum ungültig. Bitte im Format `TT-MM` eintragen, z. B. `15-05`.",
                view=MemberPortalMainView()
            )
            return

        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", view=MemberPortalMainView())
            return

        member = guild.get_member(self.user_id)
        if not member:
            try:
                member = await guild.fetch_member(self.user_id)
            except Exception:
                member = None

        if not member:
            await inter.response.send_message("❌ Mitglied nicht gefunden.", view=MemberPortalMainView())
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
        ingame = p.get("ingame_name") or member.display_name

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

            emb.set_footer(text=f"Gemeldet von {member.display_name}")

            try:
                await ch.send(embed=emb)
            except Exception:
                pass

        await inter.response.send_message(
            f"✅ Abwesenheit gespeichert: **{from_s} bis {to_s}**.",
            view=MemberPortalMainView()
        )


class PortalOpenView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="📬 Gildenmenü im Privatchat öffnen", style=ButtonStyle.primary, custom_id="portal_open_dm")
    async def btn_open_dm(self, inter: discord.Interaction, _):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        try:
            await _send_main_menu(inter.user, inter.guild)
            await inter.response.send_message("✅ Ich habe dir das Gildenmenü per Privatnachricht geschickt.", ephemeral=True)
        except Exception:
            await inter.response.send_message(
                "❌ Konnte dir keine Privatnachricht schicken. Prüfe deine Discord-DM-Einstellungen.",
                ephemeral=True
            )


class MemberPortalMainView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _guild_member(self, inter: discord.Interaction) -> tuple[Optional[discord.Guild], Optional[discord.Member]]:
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

    @button(label="👤 Mein Profil", style=ButtonStyle.primary, custom_id="portal_profile")
    async def btn_profile(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.edit_message(
            embed=_profile_embed(guild, member),
            view=ProfileView()
        )

    @button(label="📅 Gilden-Events", style=ButtonStyle.secondary, custom_id="portal_events")
    async def btn_events(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.edit_message(
            embed=_events_embed(guild.id),
            view=EventsInfoView()
        )

    @button(label="🏖️ Abwesenheit melden", style=ButtonStyle.secondary, custom_id="portal_absence")
    async def btn_absence(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.send_modal(AbsenceModal(guild.id, inter.user.id))

    @button(label="❓ Hilfe", style=ButtonStyle.secondary, custom_id="portal_help")
    async def btn_help(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="❓ Hilfe – ebolus Gildenbot",
            description=(
                "**Mein Profil**\n"
                "Zeigt dein Gildenprofil. Du kannst Ingame-Name, Main-Rolle und Gearscore bearbeiten.\n\n"
                "**Gilden-Events**\n"
                "Zeigt feste Gildentermine. Das ist nur eine Übersicht, keine Anmeldung.\n\n"
                "**Abwesenheit melden**\n"
                "Meldet deine Abwesenheit an die Gildenleitung und speichert sie für die Mitgliederliste.\n\n"
                "**Mitgliederliste**\n"
                "Findest du im Profilbereich."
            ),
            color=discord.Color.blurple()
        )

        await inter.response.edit_message(embed=emb, view=BackOnlyView())


class ProfileView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _guild_member(self, inter: discord.Interaction) -> tuple[Optional[discord.Guild], Optional[discord.Member]]:
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

    @button(label="✏️ Profil bearbeiten", style=ButtonStyle.primary, custom_id="portal_profile_edit")
    async def btn_edit(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild or not member:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.send_modal(ProfileEditModal(guild.id, inter.user.id))

    @button(label="👥 Mitgliederliste", style=ButtonStyle.secondary, custom_id="portal_member_list")
    async def btn_members(self, inter: discord.Interaction, _):
        guild, member = await self._guild_member(inter)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.edit_message(
            embed=_members_list_embed(guild),
            view=BackOnlyView()
        )

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_profile_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(
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
        guild = _resolve_guild_for_user(inter.client, inter.user.id)

        if not guild:
            await inter.response.send_message("❌ Ich konnte deinen Server nicht zuordnen.")
            return

        await inter.response.edit_message(embed=_events_embed(guild.id), view=EventsInfoView())

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="portal_events_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(
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
        emb = discord.Embed(
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

    @tree.command(name="portal_set_position_roles", description="(Admin) Rollen für Anführer/Berater/Wächter setzen")
    async def portal_set_position_roles(
        inter: discord.Interaction,
        anfuehrer: discord.Role,
        berater: discord.Role,
        waechter: discord.Role,
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["position_roles"] = {
            "leader": int(anfuehrer.id),
            "advisor": int(berater.id),
            "guardian": int(waechter.id),
        }

        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Positionsrollen gesetzt:\n"
            f"• Anführer: {anfuehrer.mention}\n"
            f"• Berater: {berater.mention}\n"
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

        text = (
            f"**Member-Portal Status**\n"
            f"• Abwesenheitskanal: {f'<#{absence_ch_id}>' if absence_ch_id else '—'}\n"
            f"• Portal-Post-Channel: {f'<#{portal_ch_id}>' if portal_ch_id else '—'}\n"
            f"• Portal-Message-ID: `{c.get('portal_post_message_id', 0)}`\n\n"
            f"**Positionsrollen**\n"
            f"• Anführer: {f'<@&{roles.get('leader')}>' if int(roles.get('leader', 0) or 0) else '—'}\n"
            f"• Berater: {f'<@&{roles.get('advisor')}>' if int(roles.get('advisor', 0) or 0) else '—'}\n"
            f"• Wächter: {f'<@&{roles.get('guardian')}>' if int(roles.get('guardian', 0) or 0) else '—'}\n\n"
            f"**Feste Events:** {len(c.get('events') or [])}"
        )

        await inter.response.send_message(text, ephemeral=True)
