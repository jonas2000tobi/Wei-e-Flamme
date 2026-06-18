from __future__ import annotations

import json
import math
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button
from discord.enums import ButtonStyle

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DKP_CFG_FILE = DATA_DIR / "dkp_cfg.json"
DKP_BALANCES_FILE = DATA_DIR / "dkp_balances.json"
DKP_TX_FILE = DATA_DIR / "dkp_transactions.json"
DKP_CHECK_FILE = DATA_DIR / "dkp_event_checks.json"
MEMBER_PORTAL_CFG_FILE = DATA_DIR / "member_portal_cfg.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"

EVENT_TYPE_CHOICES = [
    "Gildenboss",
    "HM Raid",
    "NM Raid",
    "Normal Raid",
    "Übungsrun HM Raid",
    "Übungsrun Trials",
    "Segensstein PvP",
]

DEFAULT_EVENT_POINTS = {
    "Gildenboss": 20,
    "HM Raid": 12,
    "NM Raid": 12,
    "Normal Raid": 12,
    "Übungsrun HM Raid": 15,
    "Übungsrun Trials": 15,
    "Segensstein PvP": 5,
}

DEFAULT_DECAY_PERCENT = 15.0
DEFAULT_RESERVE_FACTOR = 0.5


def _load_json(path: Path, default):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


dkp_cfg: dict = _load_json(DKP_CFG_FILE, {})
dkp_balances: dict = _load_json(DKP_BALANCES_FILE, {})
dkp_transactions: dict = _load_json(DKP_TX_FILE, {})
dkp_event_checks: dict = _load_json(DKP_CHECK_FILE, {})


def save_cfg() -> None:
    _save_json(DKP_CFG_FILE, dkp_cfg)


def save_balances() -> None:
    _save_json(DKP_BALANCES_FILE, dkp_balances)


def save_transactions() -> None:
    _save_json(DKP_TX_FILE, dkp_transactions)


def save_event_checks() -> None:
    _save_json(DKP_CHECK_FILE, dkp_event_checks)


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _load_portal_cfg() -> dict:
    return _load_json(MEMBER_PORTAL_CFG_FILE, {})


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True
    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False
    leader_cfg = _load_leader_cfg()
    c = leader_cfg.get(str(inter.guild.id)) or {}
    role_id = int(c.get("leader_role_id", 0) or 0)
    role = inter.guild.get_role(role_id) if role_id else None
    return bool(role and role in inter.user.roles)


def _home_guild_id(default: int = 0) -> int:
    try:
        try:
            from bot.alliance_config import _home_guild_id as _alli_home  # type: ignore
        except ModuleNotFoundError:
            from alliance_config import _home_guild_id as _alli_home  # type: ignore
        return int(_alli_home(default=default) or default or 0)
    except Exception:
        return int(default or 0)


def _gcfg(guild_id: int) -> dict:
    gid = str(int(guild_id))
    c = dkp_cfg.get(gid) or {}
    c.setdefault("log_channel_id", 0)
    c.setdefault("decay_percent", DEFAULT_DECAY_PERCENT)
    c.setdefault("reserve_factor", DEFAULT_RESERVE_FACTOR)
    pts = c.get("event_points") if isinstance(c.get("event_points"), dict) else {}
    for k, v in DEFAULT_EVENT_POINTS.items():
        pts.setdefault(k, v)
    c["event_points"] = pts
    dkp_cfg[gid] = c
    return c


def _gbal(guild_id: int) -> dict:
    gid = str(int(guild_id))
    g = dkp_balances.get(gid) or {}
    g.setdefault("users", {})
    dkp_balances[gid] = g
    return g


def _gtx(guild_id: int) -> list:
    gid = str(int(guild_id))
    arr = dkp_transactions.get(gid)
    if not isinstance(arr, list):
        arr = []
        dkp_transactions[gid] = arr
    return arr


def get_balance(guild_id: int, user_id: int) -> int:
    users = _gbal(guild_id).setdefault("users", {})
    try:
        return int(users.get(str(int(user_id)), 0) or 0)
    except Exception:
        return 0


def set_balance(guild_id: int, user_id: int, value: int) -> None:
    users = _gbal(guild_id).setdefault("users", {})
    users[str(int(user_id))] = int(value)
    save_balances()


def _add_transaction(
    guild_id: int,
    user_id: int,
    amount: int,
    reason: str,
    actor_id: int,
    tx_type: str,
    event_id: str = "",
    meta: Optional[dict] = None,
) -> dict:
    before = get_balance(guild_id, user_id)
    after = before + int(amount)
    set_balance(guild_id, user_id, after)

    tx = {
        "id": f"{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')}-{int(user_id)}",
        "created_at": _now_iso(),
        "guild_id": int(guild_id),
        "user_id": int(user_id),
        "amount": int(amount),
        "balance_before": int(before),
        "balance_after": int(after),
        "reason": _safe_text(reason),
        "actor_id": int(actor_id),
        "type": str(tx_type),
        "event_id": str(event_id or ""),
        "meta": meta or {},
    }
    _gtx(guild_id).append(tx)
    save_transactions()
    return tx


def _format_amount(amount: int) -> str:
    amount = int(amount)
    return f"+{amount}" if amount >= 0 else str(amount)


def _event_type_choices():
    return [app_commands.Choice(name=x, value=x) for x in EVENT_TYPE_CHOICES]


def _event_points(guild_id: int, event_type: str) -> int:
    c = _gcfg(guild_id)
    pts = c.get("event_points") or {}
    return int(pts.get(event_type, DEFAULT_EVENT_POINTS.get(event_type, 0)) or 0)


def _reserve_points(guild_id: int, event_type: str) -> int:
    base = _event_points(guild_id, event_type)
    factor = float(_gcfg(guild_id).get("reserve_factor", DEFAULT_RESERVE_FACTOR) or DEFAULT_RESERVE_FACTOR)
    return int(math.ceil(base * factor))


def _member_role_id(home_guild_id: int) -> int:
    portal_cfg = _load_portal_cfg()
    c = portal_cfg.get(str(int(home_guild_id))) or {}
    return int(c.get("member_role_id", 0) or 0)


def _is_ebolus_member(client: discord.Client, home_guild_id: int, user_id: int) -> bool:
    guild = client.get_guild(int(home_guild_id))
    if not guild:
        return False
    member = guild.get_member(int(user_id))
    if not member or member.bot:
        return False
    role_id = _member_role_id(int(home_guild_id))
    if not role_id:
        return True
    role = guild.get_role(role_id)
    return bool(role and role in member.roles)


def _display_member(client: discord.Client, home_guild_id: int, user_id: int) -> str:
    guild = client.get_guild(int(home_guild_id))
    if guild:
        member = guild.get_member(int(user_id))
        if member:
            return member.display_name
    return f"User {user_id}"


async def _log_to_channel(client: discord.Client, guild_id: int, embed: discord.Embed) -> None:
    c = _gcfg(guild_id)
    ch_id = int(c.get("log_channel_id", 0) or 0)
    if not ch_id:
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    ch = guild.get_channel(ch_id)
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(embed=embed)
        except Exception as e:
            print(f"[dkp_system] Log-Post Fehler: {e!r}")





def _gchecks(guild_id: int) -> dict:
    gid = str(int(guild_id))
    g = dkp_event_checks.get(gid) or {}
    g.setdefault("events", {})
    dkp_event_checks[gid] = g
    return g


def _event_check_state(guild_id: int, event_id: str) -> dict:
    g = _gchecks(guild_id)
    events = g.setdefault("events", {})
    st = events.get(str(event_id)) or {}
    st.setdefault("posted", False)
    st.setdefault("posted_at", "")
    st.setdefault("message_id", 0)
    st.setdefault("channel_id", 0)
    st.setdefault("awarded", False)
    st.setdefault("awarded_at", "")
    events[str(event_id)] = st
    return st


def _parse_when(value: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(value or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None


def _dkp_log_channel(client: discord.Client, guild_id: int) -> Optional[discord.TextChannel | discord.Thread]:
    c = _gcfg(guild_id)
    ch_id = int(c.get("log_channel_id", 0) or 0)
    if not ch_id:
        return None
    guild = client.get_guild(int(guild_id))
    if not guild:
        return None
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


def _event_title_and_time(event: dict) -> tuple[str, str]:
    title = str(event.get("title", "Event") or "Event")
    dt = _parse_when(str(event.get("when_iso", "") or ""))
    when = dt.strftime("%d.%m.%Y %H:%M") if dt else "Unbekannt"
    return title, when


def _attendance_check_embed(client: discord.Client, home_guild_id: int, event: dict, event_id: str, event_type: str) -> discord.Embed:
    present, reserve, skipped_partner, skipped_open = _attendance_summary_for_award(client, home_guild_id, event, event_type)
    title, when = _event_title_and_time(event)
    base = _event_points(home_guild_id, event_type)
    res = _reserve_points(home_guild_id, event_type)
    emb = discord.Embed(title="📋 EC-Anwesenheit bestätigen", color=discord.Color.blurple())
    emb.description = (
        f"**{title}**\n"
        f"Zeit: **{when}**\n"
        f"Event-ID: `{event_id}`\n"
        f"EC-Typ: **{event_type}**\n"
        f"Wert: **{base} EC** • Reserve: **{res} EC**\n\n"
        "Die Anmeldung ist nur ein Vorschlag. Bitte bestätige die echte Anwesenheit, bevor EC vergeben werden."
    )

    participants = event.get("participants") or []
    lines = []
    for p in participants[:25]:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            uid = 0
        signup = str(p.get("signup", "") or "")
        role = {"TANK": "Tank", "HEAL": "Heal", "DPS": "DPS", "BANK": "Reserve"}.get(signup, signup or "?")
        if uid:
            marker = "✅ EC" if _is_ebolus_member(client, home_guild_id, uid) else "🤝 Allianz"
            lines.append(f"• {marker} <@{uid}> – {role}")
    if len(participants) > 25:
        lines.append(f"… {len(participants) - 25} weitere")
    emb.add_field(name="Anmeldungen", value="\n".join(lines)[:1000] if lines else "—", inline=False)

    emb.add_field(
        name="Aktueller Status",
        value=(
            f"✅ War da: **{len(present) + len(reserve)}** Ebolus\n"
            f"🤝 Partner ohne EC: **{len(skipped_partner)}**\n"
            f"⚪ Noch offen / nicht bestätigt: **{len(skipped_open)}**"
        ),
        inline=False,
    )
    emb.set_footer(text="Buttons: erst Anwesenheit bestätigen, dann EC vergeben. Einzelkorrektur: Admin → Event → Anwesenheit")
    return emb


async def _award_event_now(
    client: discord.Client,
    guild_id: int,
    event_id: str,
    actor: discord.abc.User,
) -> tuple[bool, str, Optional[discord.Embed]]:
    rsvp = _import_rsvp()
    if not rsvp:
        return False, "RSVP-/Anwesenheitssystem nicht geladen.", None
    event = rsvp.get_attendance_event(int(guild_id), str(event_id))
    if not event:
        return False, "Event nicht gefunden.", None
    event_type = _dkp_type_from_event(event, str(event_id))
    if not event_type:
        return False, "Dieses Event ist nicht EC-relevant oder hat keinen gespeicherten EC-Typ.", None
    if _event_has_dkp_already(int(guild_id), str(event_id), event_type):
        return False, "Für dieses Event wurden bereits EC vergeben. Nutze bei Fehlern `/dkp adjust`.", None

    present, reserve, skipped_partner, skipped_open = _attendance_summary_for_award(client, int(guild_id), event, event_type)
    awarded = present + reserve
    if not awarded:
        return False, "Keine bestätigten Ebolus-Teilnehmer mit Status 'War da' gefunden.", None

    for row in awarded:
        _add_transaction(
            int(guild_id),
            int(row["user_id"]),
            int(row["points"]),
            f"Event-Teilnahme: {event.get('title', 'Event')} ({event_type})",
            int(getattr(actor, "id", 0) or 0),
            "event_award",
            event_id=str(event_id),
            meta={"event_type": event_type, "signup": row.get("signup", ""), "event_title": str(event.get("title", "Event") or "Event")},
        )

    st = _event_check_state(int(guild_id), str(event_id))
    st["awarded"] = True
    st["awarded_at"] = _now_iso()
    save_event_checks()

    emb = _award_log_embed(event, event_type, present, reserve, skipped_partner, skipped_open, actor)
    await _log_to_channel(client, int(guild_id), emb)
    return True, f"EC vergeben: {len(awarded)} Ebolus-Spieler.", emb


class ECEventCheckView(View):
    def __init__(self, guild_id: int, event_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)

    async def _guard(self, inter: discord.Interaction) -> bool:
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return False
        if int(inter.guild.id) != int(self.guild_id):
            await inter.response.send_message("❌ Falscher Server für dieses Event.", ephemeral=True)
            return False
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return False
        return True

    @button(label="Alle Anmeldungen bestätigen", style=ButtonStyle.success, custom_id="dkp_check_confirm_all")
    async def confirm_all(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.response.send_message("❌ RSVP-System nicht geladen.", ephemeral=True)
            return
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not event:
            # Snapshot aus Store nachziehen, falls möglich.
            obj = (getattr(rsvp, "store", {}) or {}).get(str(self.event_id))
            if isinstance(obj, dict):
                event = rsvp.ensure_attendance_snapshot(inter.client, self.event_id, obj)
        if not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        count = 0
        for p in event.get("participants", []) or []:
            try:
                uid = int(p.get("id", 0) or 0)
            except Exception:
                uid = 0
            if not uid:
                continue
            if rsvp.set_attendance_status(self.guild_id, self.event_id, uid, "present", int(inter.user.id)):
                count += 1
        event = rsvp.get_attendance_event(self.guild_id, self.event_id) or event
        event_type = _dkp_type_from_event(event, self.event_id)
        emb = _attendance_check_embed(inter.client, self.guild_id, event, self.event_id, event_type or "Unbekannt")
        await inter.response.edit_message(embed=emb, view=self)
        await inter.followup.send(f"✅ {count} angemeldete Spieler wurden als 'War da' bestätigt. Prüfe bei Bedarf Einzelkorrekturen über Admin → Event → Anwesenheit.", ephemeral=True)

    @button(label="EC-Vorschau", style=ButtonStyle.secondary, custom_id="dkp_check_preview")
    async def preview(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        rsvp = _import_rsvp()
        event = rsvp.get_attendance_event(self.guild_id, self.event_id) if rsvp else None
        if not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        event_type = _dkp_type_from_event(event, self.event_id)
        if not event_type:
            await inter.response.send_message("❌ Dieses Event hat keinen EC-Typ.", ephemeral=True)
            return
        present, reserve, skipped_partner, skipped_open = _attendance_summary_for_award(inter.client, self.guild_id, event, event_type)
        emb = _award_preview_embed(event, event_type, present, reserve, skipped_partner, skipped_open, duplicate=_event_has_dkp_already(self.guild_id, self.event_id, event_type))
        await inter.response.send_message(embed=emb, ephemeral=True)

    @button(label="EC vergeben", style=ButtonStyle.primary, custom_id="dkp_check_award")
    async def award(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        ok, msg, _emb = await _award_event_now(inter.client, self.guild_id, self.event_id, inter.user)
        if ok:
            # Hauptnachricht aktualisieren/entschärfen.
            try:
                rsvp = _import_rsvp()
                event = rsvp.get_attendance_event(self.guild_id, self.event_id) if rsvp else None
                if event:
                    event_type = _dkp_type_from_event(event, self.event_id)
                    emb = _attendance_check_embed(inter.client, self.guild_id, event, self.event_id, event_type or "Unbekannt")
                    emb.title = "✅ EC-Vergabe abgeschlossen"
                    emb.color = discord.Color.green()
                    await inter.message.edit(embed=emb, view=None)
            except Exception:
                pass
            await inter.followup.send(f"✅ {msg}", ephemeral=True)
        else:
            await inter.followup.send(f"❌ {msg}", ephemeral=True)

    @button(label="Später / ignorieren", style=ButtonStyle.danger, custom_id="dkp_check_ignore")
    async def ignore(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        st = _event_check_state(self.guild_id, self.event_id)
        st["ignored"] = True
        st["ignored_at"] = _now_iso()
        save_event_checks()
        emb = inter.message.embeds[0] if inter.message and inter.message.embeds else discord.Embed(title="EC-Check ignoriert")
        emb.color = discord.Color.dark_grey()
        emb.set_footer(text=f"Ignoriert von {inter.user} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
        await inter.response.edit_message(embed=emb, view=None)


async def _post_event_check(client: discord.Client, guild_id: int, event_id: str, obj: dict) -> bool:
    rsvp = _import_rsvp()
    if not rsvp:
        return False
    event = rsvp.ensure_attendance_snapshot(client, str(event_id), obj)
    if not event:
        return False
    event_type = _dkp_type_from_event(event, str(event_id))
    if not event_type:
        return False
    ch = _dkp_log_channel(client, int(guild_id))
    if not ch:
        return False
    emb = _attendance_check_embed(client, int(guild_id), event, str(event_id), event_type)
    msg = await ch.send(embed=emb, view=ECEventCheckView(int(guild_id), str(event_id)))
    st = _event_check_state(int(guild_id), str(event_id))
    st["posted"] = True
    st["posted_at"] = _now_iso()
    st["message_id"] = int(msg.id)
    st["channel_id"] = int(ch.id)
    save_event_checks()
    return True


@tasks.loop(minutes=5)
async def dkp_event_check_loop():
    client = getattr(dkp_event_check_loop, "_client", None)
    if client is None:
        return
    rsvp = _import_rsvp()
    if not rsvp:
        return
    store = getattr(rsvp, "store", {}) or {}
    now = datetime.now(TZ)
    for event_id, obj in list(store.items()):
        if not isinstance(obj, dict):
            continue
        if not bool(obj.get("dkp_enabled", False)) and not str(obj.get("dkp_event_type", "") or "").strip():
            continue
        guild_id = int(obj.get("guild_id", 0) or 0)
        if not guild_id:
            continue
        home_id = _home_guild_id(default=guild_id)
        if int(guild_id) != int(home_id):
            # EC ist Ebolus-intern. Allianz-Master liegt auf dem Home-Server.
            continue
        st = _event_check_state(int(home_id), str(event_id))
        if st.get("posted") or st.get("ignored") or st.get("awarded"):
            continue
        if _event_has_dkp_already(int(home_id), str(event_id), _dkp_type_from_event(obj, str(event_id))):
            st["awarded"] = True
            save_event_checks()
            continue
        when = _parse_when(str(obj.get("when_iso", "") or ""))
        if not when:
            continue
        # Da Events keine feste Dauer speichern und der normale Cleanup nach ca. 2h greift,
        # wird der Leader-Check ca. 90 Minuten nach Eventbeginn gepostet.
        if now >= when + timedelta(minutes=90):
            try:
                await _post_event_check(client, int(home_id), str(event_id), obj)
            except Exception as e:
                print(f"[dkp_system] EC-Eventcheck konnte nicht gepostet werden ({event_id}): {e!r}")
def _import_rsvp():
    try:
        from bot import event_rsvp_dm as rsvp  # type: ignore
    except Exception:
        try:
            import event_rsvp_dm as rsvp  # type: ignore
        except Exception:
            return None
    return rsvp


def _event_has_dkp_already(guild_id: int, event_id: str, event_type: str) -> bool:
    for tx in _gtx(guild_id):
        if str(tx.get("event_id", "")) == str(event_id) and str((tx.get("meta") or {}).get("event_type", "")) == str(event_type) and str(tx.get("type")) == "event_award":
            return True
    return False


def _dkp_type_from_event(event: dict | None, event_id: str = "") -> str:
    if isinstance(event, dict):
        if bool(event.get("dkp_enabled", False)):
            value = str(event.get("dkp_event_type", "") or "").strip()
            if value:
                return value
        value = str(event.get("dkp_event_type", "") or "").strip()
        if value and value != "Nicht DKP-relevant":
            return value

    if event_id:
        rsvp = _import_rsvp()
        try:
            obj = (getattr(rsvp, "store", {}) or {}).get(str(event_id)) if rsvp else None
            if isinstance(obj, dict):
                if bool(obj.get("dkp_enabled", False)):
                    value = str(obj.get("dkp_event_type", "") or "").strip()
                    if value:
                        return value
                value = str(obj.get("dkp_event_type", "") or "").strip()
                if value and value != "Nicht DKP-relevant":
                    return value
        except Exception:
            pass

    return ""


def _resolve_event_type(choice: Optional[app_commands.Choice[str]], event: dict | None, event_id: str) -> str:
    if choice is not None:
        value = str(choice.value or "").strip()
        if value and value != "Nicht DKP-relevant":
            return value
    return _dkp_type_from_event(event, event_id)


def _attendance_summary_for_award(client: discord.Client, home_guild_id: int, event: dict, event_type: str) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    participants = event.get("participants") or []
    attendance = event.get("attendance") or {}
    present: list[dict] = []
    reserve: list[dict] = []
    skipped_not_ebolus: list[dict] = []
    skipped_not_present: list[dict] = []

    base_points = _event_points(home_guild_id, event_type)
    reserve_points = _reserve_points(home_guild_id, event_type)

    for p in participants:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            continue
        if not uid:
            continue
        status = str((attendance.get(str(uid)) or {}).get("status", "") or "")
        signup = str(p.get("signup", "") or "")
        row = {
            "user_id": uid,
            "name": str(p.get("name", "") or _display_member(client, home_guild_id, uid)),
            "signup": signup,
            "status": status or "open",
        }
        if status != "present":
            skipped_not_present.append(row)
            continue
        if not _is_ebolus_member(client, home_guild_id, uid):
            skipped_not_ebolus.append(row)
            continue
        if signup == "BANK":
            row["points"] = reserve_points
            reserve.append(row)
        else:
            row["points"] = base_points
            present.append(row)

    return present, reserve, skipped_not_ebolus, skipped_not_present


async def setup_dkp_system(client: discord.Client, tree: app_commands.CommandTree):
    # Initialisiert Defaults für alle aktuell bekannten Guilds.
    for guild in getattr(client, "guilds", []) or []:
        _gcfg(int(guild.id))
    save_cfg()

    # Wichtig: Discord erlaubt global maximal 100 Top-Level Slash-Commands.
    # Darum laufen die DKP-Funktionen als eine Command-Gruppe `/dkp ...`,
    # statt als viele einzelne `/dkp_*` Top-Level-Commands.
    for old_name in (
        "dkp",
        "dkp_set_log_channel",
        "dkp_balance",
        "dkp_adjust",
        "dkp_config_show",
        "dkp_set_event_points",
        "dkp_set_decay",
        "dkp_event_preview",
        "dkp_award_event",
        "dkp_decay_run",
    ):
        try:
            tree.remove_command(old_name)
        except Exception:
            pass

    dkp = app_commands.Group(name="dkp", description="Ebolus Coins / DKP verwalten")

    @dkp.command(name="set_log_channel", description="Leader: DKP-/Loot-Log-Kanal setzen")
    async def dkp_set_log_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        home_id = _home_guild_id(default=inter.guild.id)
        if int(inter.guild.id) != int(home_id):
            await inter.response.send_message("❌ DKP wird nur auf dem Ebolus/Home-Server verwaltet.", ephemeral=True)
            return
        c = _gcfg(inter.guild.id)
        c["log_channel_id"] = int(channel.id)
        dkp_cfg[str(inter.guild.id)] = c
        save_cfg()
        await inter.response.send_message(f"✅ DKP-/Loot-Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @dkp.command(name="balance", description="Zeigt deinen EC-/DKP-Stand privat")
    async def dkp_balance(inter: discord.Interaction, user: Optional[discord.Member] = None):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        # Privacy-Regel:
        # - Normale Member sehen nur den eigenen Stand.
        # - Leader/Admins dürfen Spielerstände privat prüfen, aber niemals öffentlich posten.
        target = user or inter.user
        if user is not None and int(user.id) != int(inter.user.id) and not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Du kannst nur deinen eigenen EC-Stand ansehen.", ephemeral=True)
            return

        bal = get_balance(inter.guild.id, target.id)
        if int(target.id) == int(inter.user.id):
            await inter.response.send_message(f"🪙 Dein aktueller EC-Stand: **{bal} EC**.", ephemeral=True)
        else:
            await inter.response.send_message(f"🔒 Privater Leader-Check: **{target.display_name}** hat aktuell **{bal} EC**.", ephemeral=True)

    @dkp.command(name="adjust", description="Leader: EC/DKP manuell geben oder abziehen")
    async def dkp_adjust(inter: discord.Interaction, user: discord.Member, amount: int, reason: str):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if not reason.strip():
            await inter.response.send_message("❌ Grund ist Pflicht.", ephemeral=True)
            return
        if amount == 0:
            await inter.response.send_message("❌ Betrag darf nicht 0 sein.", ephemeral=True)
            return
        home_id = _home_guild_id(default=inter.guild.id)
        if int(inter.guild.id) != int(home_id):
            await inter.response.send_message("❌ DKP wird nur auf dem Ebolus/Home-Server verwaltet.", ephemeral=True)
            return

        tx = _add_transaction(inter.guild.id, user.id, int(amount), reason, inter.user.id, "manual_adjust")
        emb = discord.Embed(title="🪙 Manuelle EC-Korrektur", color=discord.Color.gold())
        emb.add_field(name="Spieler", value=user.mention, inline=True)
        emb.add_field(name="Änderung", value=f"**{_format_amount(amount)} EC**", inline=True)
        emb.add_field(name="Stand", value="🔒 Nicht öffentlich angezeigt", inline=True)
        emb.add_field(name="Grund", value=_safe_text(reason)[:1000], inline=False)
        emb.set_footer(text=f"Ausgeführt von {inter.user} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
        await _log_to_channel(client, inter.guild.id, emb)
        await inter.response.send_message(f"✅ EC angepasst: {user.mention} { _format_amount(amount) } EC. Neuer Stand: **{tx['balance_after']}**", ephemeral=True)

    @dkp.command(name="config", description="Zeigt die EC-/DKP-Konfiguration")
    async def dkp_config_show(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        c = _gcfg(inter.guild.id)
        pts = c.get("event_points") or {}
        lines = [f"• **{k}**: {int(pts.get(k, 0) or 0)} EC" for k in EVENT_TYPE_CHOICES]
        emb = discord.Embed(
            title="⚙️ EC-/DKP-Konfiguration",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        log_id = int(c.get("log_channel_id", 0) or 0)
        emb.add_field(name="Reserve", value=f"{float(c.get('reserve_factor', DEFAULT_RESERVE_FACTOR)) * 100:.0f}% des Eventwertes, aufgerundet", inline=False)
        emb.add_field(name="Wöchentlicher Verfall", value=f"{float(c.get('decay_percent', DEFAULT_DECAY_PERCENT)):.1f}%", inline=True)
        emb.add_field(name="Log-Kanal", value=(f"<#{log_id}>" if log_id else "Nicht gesetzt"), inline=True)
        await inter.response.send_message(embed=emb, ephemeral=True)

    @dkp.command(name="set_points", description="Leader: EC-Wert für Eventtyp setzen")
    @app_commands.choices(event_type=_event_type_choices())
    async def dkp_set_event_points(inter: discord.Interaction, event_type: app_commands.Choice[str], points: int):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if points < 0 or points > 10000:
            await inter.response.send_message("❌ Punktewert ungültig.", ephemeral=True)
            return
        c = _gcfg(inter.guild.id)
        c.setdefault("event_points", {})[event_type.value] = int(points)
        dkp_cfg[str(inter.guild.id)] = c
        save_cfg()
        await inter.response.send_message(f"✅ **{event_type.value}** gibt jetzt **{points} EC**.", ephemeral=True)

    @dkp.command(name="set_decay", description="Leader: Wöchentlichen EC-Verfall konfigurieren")
    async def dkp_set_decay(inter: discord.Interaction, percent: float):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if percent < 0 or percent > 100:
            await inter.response.send_message("❌ Prozentwert muss zwischen 0 und 100 liegen.", ephemeral=True)
            return
        c = _gcfg(inter.guild.id)
        c["decay_percent"] = float(percent)
        dkp_cfg[str(inter.guild.id)] = c
        save_cfg()
        await inter.response.send_message(f"✅ Wöchentlicher EC-Verfall gesetzt: **{percent:.1f}%**", ephemeral=True)

    @dkp.command(name="event_preview", description="Leader: EC-Vorschau; Eventtyp wird automatisch erkannt")
    @app_commands.choices(event_type=_event_type_choices())
    async def dkp_event_preview(inter: discord.Interaction, event_id: str, event_type: Optional[app_commands.Choice[str]] = None):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.response.send_message("❌ RSVP-/Anwesenheitssystem nicht geladen.", ephemeral=True)
            return
        home_id = _home_guild_id(default=inter.guild.id)
        event = rsvp.get_attendance_event(int(home_id), str(event_id))
        if not event:
            await inter.response.send_message("❌ Event nicht gefunden. Öffne ggf. zuerst Admin → Event → Anwesenheit, damit ein Snapshot erstellt wird.", ephemeral=True)
            return
        resolved_event_type = _resolve_event_type(event_type, event, str(event_id))
        if not resolved_event_type:
            await inter.response.send_message("❌ Dieses Event ist nicht EC/DKP-relevant oder hat keinen gespeicherten EC-/DKP-Typ. Gib optional `event_type` an oder erstelle neue Events mit DKP-Typ.", ephemeral=True)
            return
        present, reserve, skipped_partner, skipped_open = _attendance_summary_for_award(client, int(home_id), event, resolved_event_type)
        emb = _award_preview_embed(event, resolved_event_type, present, reserve, skipped_partner, skipped_open, duplicate=_event_has_dkp_already(int(home_id), str(event_id), resolved_event_type))
        await inter.response.send_message(embed=emb, ephemeral=True)

    @dkp.command(name="award_event", description="Leader: EC vergeben; Eventtyp wird automatisch erkannt")
    @app_commands.choices(event_type=_event_type_choices())
    async def dkp_award_event(inter: discord.Interaction, event_id: str, confirm: bool = False, event_type: Optional[app_commands.Choice[str]] = None):
        await inter.response.defer(ephemeral=True, thinking=True)
        if inter.guild is None:
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.followup.send("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if not confirm:
            await inter.followup.send("❌ Sicherheitsabfrage: Setze `confirm:true`, wenn du die EC wirklich vergeben willst.", ephemeral=True)
            return
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.followup.send("❌ RSVP-/Anwesenheitssystem nicht geladen.", ephemeral=True)
            return
        home_id = _home_guild_id(default=inter.guild.id)
        if int(inter.guild.id) != int(home_id):
            await inter.followup.send("❌ EC/DKP wird nur auf dem Ebolus/Home-Server vergeben.", ephemeral=True)
            return
        event = rsvp.get_attendance_event(int(home_id), str(event_id))
        if not event:
            await inter.followup.send("❌ Event nicht gefunden. Öffne ggf. zuerst Admin → Event → Anwesenheit, damit ein Snapshot erstellt wird.", ephemeral=True)
            return
        resolved_event_type = _resolve_event_type(event_type, event, str(event_id))
        if not resolved_event_type:
            await inter.followup.send("❌ Dieses Event ist nicht EC/DKP-relevant oder hat keinen gespeicherten EC-/DKP-Typ. Gib optional `event_type` an oder erstelle neue Events mit DKP-Typ.", ephemeral=True)
            return
        if _event_has_dkp_already(int(home_id), str(event_id), resolved_event_type):
            await inter.followup.send("❌ Für dieses Event und diesen Eventtyp wurden bereits EC vergeben. Nutze bei Fehlern `/dkp adjust`.", ephemeral=True)
            return

        present, reserve, skipped_partner, skipped_open = _attendance_summary_for_award(client, int(home_id), event, resolved_event_type)
        awarded = present + reserve
        if not awarded:
            await inter.followup.send("❌ Keine bestätigten Ebolus-Teilnehmer mit Status 'War da' gefunden.", ephemeral=True)
            return

        for row in awarded:
            _add_transaction(
                int(home_id),
                int(row["user_id"]),
                int(row["points"]),
                f"Event-Teilnahme: {event.get('title', 'Event')} ({resolved_event_type})",
                inter.user.id,
                "event_award",
                event_id=str(event_id),
                meta={"event_type": resolved_event_type, "signup": row.get("signup", ""), "event_title": str(event.get("title", "Event") or "Event")},
            )

        emb = _award_log_embed(event, resolved_event_type, present, reserve, skipped_partner, skipped_open, inter.user)
        await _log_to_channel(client, int(home_id), emb)
        await inter.followup.send(f"✅ EC vergeben: **{len(awarded)}** Ebolus-Spieler. Log wurde gepostet.", ephemeral=True)

    @dkp.command(name="post_event_check", description="Leader: EC-Anwesenheitscheck sofort in den Log-Kanal posten")
    async def dkp_post_event_check(inter: discord.Interaction, event_id: str):
        await inter.response.defer(ephemeral=True, thinking=True)

        if inter.guild is None:
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.followup.send("❌ Nur Leader/Admins.", ephemeral=True)
            return

        home_id = _home_guild_id(default=inter.guild.id)
        if int(inter.guild.id) != int(home_id):
            await inter.followup.send("❌ EC/DKP wird nur auf dem Ebolus/Home-Server verwaltet.", ephemeral=True)
            return

        log_ch = _log_channel(client, int(home_id))
        if not log_ch:
            await inter.followup.send("❌ DKP-/Loot-Log-Kanal ist nicht gesetzt. Nutze zuerst `/dkp set_log_channel`.", ephemeral=True)
            return

        obj = None
        try:
            rsvp = _import_rsvp()
            if rsvp and hasattr(rsvp, "store"):
                obj = rsvp.store.get(str(event_id))
        except Exception:
            obj = None

        if not isinstance(obj, dict):
            await inter.followup.send("❌ Event nicht gefunden. Nutze die Message-/Event-ID aus der Raid-Ankündigung.", ephemeral=True)
            return

        if not _event_is_dkp_enabled(obj):
            await inter.followup.send("❌ Dieses Event ist nicht EC/DKP-relevant oder hat keinen gespeicherten EC-/DKP-Typ.", ephemeral=True)
            return

        ok = await _post_event_check(client, int(home_id), str(event_id), obj)
        if ok:
            await inter.followup.send("✅ EC-Anwesenheitscheck wurde sofort in den DKP-/Loot-Log-Kanal gepostet.", ephemeral=True)
        else:
            await inter.followup.send("❌ EC-Anwesenheitscheck konnte nicht gepostet werden. Prüfe Log-Kanal, Event-ID und Bot-Rechte.", ephemeral=True)

    @dkp.command(name="decay_run", description="Leader: Wöchentlichen EC-Verfall manuell ausführen")
    async def dkp_decay_run(inter: discord.Interaction, confirm: bool = False):
        await inter.response.defer(ephemeral=True, thinking=True)
        if inter.guild is None:
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.followup.send("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if not confirm:
            await inter.followup.send("❌ Sicherheitsabfrage: Setze `confirm:true`, wenn du den Verfall ausführen willst.", ephemeral=True)
            return
        percent = float(_gcfg(inter.guild.id).get("decay_percent", DEFAULT_DECAY_PERCENT) or 0)
        users = _gbal(inter.guild.id).setdefault("users", {})
        if not users:
            await inter.followup.send("Keine EC-Konten vorhanden.", ephemeral=True)
            return
        changed = []
        for uid_s, old_v in list(users.items()):
            try:
                uid = int(uid_s)
                old = int(old_v or 0)
            except Exception:
                continue
            new = int(math.floor(old * (1.0 - percent / 100.0)))
            diff = new - old
            if diff == 0:
                continue
            _add_transaction(inter.guild.id, uid, diff, f"Wöchentlicher EC-Verfall ({percent:.1f}%)", inter.user.id, "weekly_decay")
            changed.append((uid, diff))
        emb = discord.Embed(title="📉 EC-Verfall ausgeführt", color=discord.Color.orange())
        lines = []
        for uid, diff in changed[:30]:
            lines.append(f"• <@{uid}>: **{_format_amount(diff)} EC**")
        if not lines:
            lines = ["Keine Änderungen."]
        emb.description = f"Regel: **-{percent:.1f}%**\nÖffentliche Gesamtstände werden nicht angezeigt.\n\n" + "\n".join(lines)
        if len(changed) > 30:
            emb.description += f"\n… {len(changed) - 30} weitere"
        emb.set_footer(text=f"Ausgeführt von {inter.user} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
        await _log_to_channel(client, inter.guild.id, emb)
        await inter.followup.send(f"✅ EC-Verfall ausgeführt. Betroffene Konten: **{len(changed)}**", ephemeral=True)

    if not dkp_event_check_loop.is_running():
        dkp_event_check_loop._client = client  # type: ignore[attr-defined]
        dkp_event_check_loop.start()
        print("💰 EC-Eventcheck-Task gestartet.")

    tree.add_command(dkp)
    print("💰 DKP-System geladen: /dkp Command-Gruppe aktiv.")
def _award_preview_embed(event: dict, event_type: str, present: list[dict], reserve: list[dict], skipped_partner: list[dict], skipped_open: list[dict], duplicate: bool = False) -> discord.Embed:
    title = str(event.get("title", "Event") or "Event")
    try:
        when = datetime.fromisoformat(str(event.get("when_iso", ""))).strftime("%d.%m.%Y %H:%M")
    except Exception:
        when = "Unbekannt"
    emb = discord.Embed(title="💰 DKP-Vorschau", color=discord.Color.gold())
    emb.description = f"**{title}**\nZeit: {when}\nTyp: **{event_type}**"
    if duplicate:
        emb.add_field(name="⚠️ Hinweis", value="Für dieses Event und diesen Typ wurden bereits DKP vergeben.", inline=False)
    lines = [f"• <@{x['user_id']}>: **+{x['points']} DKP**" for x in present]
    emb.add_field(name="✅ Ebolus – Teilnahme", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    lines = [f"• <@{x['user_id']}>: **+{x['points']} DKP**" for x in reserve]
    emb.add_field(name="🏦 Ebolus – Reserve", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    lines = [f"• {x.get('name') or ('User ' + str(x['user_id']))}" for x in skipped_partner]
    emb.add_field(name="🤝 Allianz/Partner – keine DKP", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    open_count = len(skipped_open)
    emb.add_field(name="⚪ Nicht als 'War da' bestätigt", value=str(open_count), inline=True)
    return emb


def _award_log_embed(event: dict, event_type: str, present: list[dict], reserve: list[dict], skipped_partner: list[dict], skipped_open: list[dict], actor: discord.abc.User) -> discord.Embed:
    emb = _award_preview_embed(event, event_type, present, reserve, skipped_partner, skipped_open, duplicate=False)
    emb.title = "💰 DKP vergeben"
    emb.color = discord.Color.green()
    total = sum(int(x.get("points", 0) or 0) for x in present + reserve)
    emb.add_field(name="Summe", value=f"**{total} DKP** an **{len(present) + len(reserve)}** Ebolus-Spieler", inline=False)
    emb.set_footer(text=f"Bestätigt von {actor} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
    return emb
