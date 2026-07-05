from __future__ import annotations

import json
import math
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button, Select, UserSelect
from discord.enums import ButtonStyle

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore

try:
    from bot.runtime_db import aggregate_voice_seconds  # type: ignore
except Exception:
    try:
        from runtime_db import aggregate_voice_seconds  # type: ignore
    except Exception:
        aggregate_voice_seconds = None  # type: ignore

try:
    from bot.audit_system import audit_log  # type: ignore
except Exception:
    try:
        from audit_system import audit_log  # type: ignore
    except Exception:
        audit_log = None  # type: ignore

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
DEFAULT_RESERVE_FACTOR = 0.5  # Alt-Konfig bleibt lesbar, Reserve ist aber fix 5 EC.
DEFAULT_RESERVE_POINTS = 5
DEFAULT_WEEKLY_EVENT_LIMIT = 40
DEFAULT_START_BALANCE = 0
DEFAULT_DECAY_PROTECTED_BALANCE = 50
WEEKLY_RESET_WEEKDAY = 3  # Donnerstag
WEEKLY_RESET_HOUR = 10


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
    try:
        _phase3_mirror_ec_balances_to_pg()
    except Exception as e:
        print(f"[phase3-ec] Balance-Spiegelung übersprungen: {e!r}", flush=True)


def save_transactions() -> None:
    _save_json(DKP_TX_FILE, dkp_transactions)
    try:
        _phase3_mirror_ec_transactions_to_pg()
    except Exception as e:
        print(f"[phase3-ec] Transaktions-Spiegelung übersprungen: {e!r}", flush=True)


def save_event_checks() -> None:
    _save_json(DKP_CHECK_FILE, dkp_event_checks)
    try:
        _phase3_mirror_ec_event_checks_to_pg()
    except Exception as e:
        print(f"[phase3-ec] Eventcheck-Spiegelung übersprungen: {e!r}", flush=True)


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _load_portal_cfg() -> dict:
    return _load_json(MEMBER_PORTAL_CFG_FILE, {})


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _save_portal_cfg(cfg: dict) -> None:
    _save_json(MEMBER_PORTAL_CFG_FILE, cfg or {})


def _save_leader_cfg(cfg: dict) -> None:
    _save_json(LEADER_CONTACT_CFG_FILE, cfg or {})


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
    c.setdefault("weekly_event_limit", DEFAULT_WEEKLY_EVENT_LIMIT)
    c.setdefault("start_balance", DEFAULT_START_BALANCE)
    c.setdefault("decay_protected_balance", DEFAULT_DECAY_PROTECTED_BALANCE)
    c.setdefault("weekly_reset_weekday", WEEKLY_RESET_WEEKDAY)
    c.setdefault("weekly_reset_hour", WEEKLY_RESET_HOUR)
    c.setdefault("last_decay_period", "")
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


def _weekly_period_start(now: Optional[datetime] = None) -> datetime:
    """Aktuelle EC-Woche: Donnerstag 10:00 bis Donnerstag 09:59:59."""
    now = (now or datetime.now(TZ)).astimezone(TZ)
    days_since_reset = (now.weekday() - WEEKLY_RESET_WEEKDAY) % 7
    start_date = (now - timedelta(days=days_since_reset)).date()
    start = datetime(
        start_date.year,
        start_date.month,
        start_date.day,
        WEEKLY_RESET_HOUR,
        0,
        0,
        tzinfo=TZ,
    )
    if now < start:
        start -= timedelta(days=7)
    return start


def _weekly_period_key(now: Optional[datetime] = None) -> str:
    return _weekly_period_start(now).strftime("%Y-%m-%dT%H:%M%z")


def _tx_datetime(tx: dict) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(tx.get("created_at", "") or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None


def weekly_event_earned(guild_id: int, user_id: int, now: Optional[datetime] = None) -> int:
    start = _weekly_period_start(now)
    total = 0
    for tx in _gtx(guild_id):
        if int(tx.get("user_id", 0) or 0) != int(user_id):
            continue
        if str(tx.get("type", "") or "") != "event_award":
            continue
        amount = int(tx.get("amount", 0) or 0)
        if amount <= 0:
            continue
        created = _tx_datetime(tx)
        if created and created >= start:
            total += amount
    return total


def weekly_event_remaining(guild_id: int, user_id: int, now: Optional[datetime] = None) -> int:
    limit = int(_gcfg(guild_id).get("weekly_event_limit", DEFAULT_WEEKLY_EVENT_LIMIT) or 0)
    return max(0, limit - weekly_event_earned(guild_id, user_id, now))


def _railway_log_transaction(tx: dict) -> None:
    """Einzeiliger Railway-Log für jede einzelne EC-Buchung/Korrektur."""
    try:
        meta = tx.get("meta") if isinstance(tx.get("meta"), dict) else {}
        print(
            "[EC-VERGABE] "
            f"time={tx.get('created_at')} "
            f"guild={tx.get('guild_id')} "
            f"user={tx.get('user_id')} "
            f"name={meta.get('target_name') or meta.get('name') or ''!r} "
            f"amount={tx.get('amount')} "
            f"before={tx.get('balance_before')} "
            f"after={tx.get('balance_after')} "
            f"type={tx.get('type')} "
            f"event_id={tx.get('event_id')} "
            f"event_type={meta.get('event_type', '')!r} "
            f"event_title={meta.get('event_title', '')!r} "
            f"requested={meta.get('requested_amount', '')} "
            f"limited={meta.get('limited', '')} "
            f"reason={tx.get('reason')!r} "
            f"actor={tx.get('actor_id')}",
            flush=True,
        )
    except Exception as e:
        print(f"[dkp_system] EC-Railway-Log Fehler: {e!r}", flush=True)


def _append_starting_balance_transaction(guild_id: int, user_id: int, amount: int) -> None:
    tx = {
        "id": f"{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')}-{int(user_id)}-start",
        "created_at": _now_iso(),
        "guild_id": int(guild_id),
        "user_id": int(user_id),
        "amount": int(amount),
        "balance_before": 0,
        "balance_after": int(amount),
        "reason": "Startguthaben für neues Gildenmitglied",
        "actor_id": 0,
        "type": "starting_balance",
        "event_id": "",
        "meta": {"automatic": True},
    }
    _gtx(guild_id).append(tx)
    save_transactions()


def _ensure_start_balance(guild_id: int, user_id: int) -> int:
    users = _gbal(guild_id).setdefault("users", {})
    key = str(int(user_id))
    if key in users:
        try:
            return int(users.get(key, 0) or 0)
        except Exception:
            users[key] = 0
            save_balances()
            return 0

    amount = int(_gcfg(guild_id).get("start_balance", DEFAULT_START_BALANCE) or 0)
    users[key] = amount
    save_balances()
    if amount:
        _append_starting_balance_transaction(guild_id, user_id, amount)
    return amount


def get_balance(guild_id: int, user_id: int) -> int:
    return _ensure_start_balance(guild_id, user_id)


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
    requested_amount = int(amount)
    actual_amount = requested_amount
    tx_meta = dict(meta or {})

    # Nur echte Eventgutschriften zählen in das Wochenlimit.
    # Leader-/Admin-Gutschriften (manual_adjust) bleiben vollständig außerhalb.
    if str(tx_type) == "event_award" and requested_amount > 0:
        remaining = weekly_event_remaining(guild_id, user_id)
        actual_amount = min(requested_amount, remaining)
        tx_meta.update({
            "requested_amount": requested_amount,
            "weekly_limit": int(_gcfg(guild_id).get("weekly_event_limit", DEFAULT_WEEKLY_EVENT_LIMIT) or 0),
            "weekly_earned_before": weekly_event_earned(guild_id, user_id),
            "weekly_remaining_before": remaining,
            "weekly_period_start": _weekly_period_start().isoformat(),
            "limited": actual_amount < requested_amount,
        })

    before = get_balance(guild_id, user_id)
    after = before + actual_amount
    set_balance(guild_id, user_id, after)

    tx = {
        "id": f"{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')}-{int(user_id)}",
        "created_at": _now_iso(),
        "guild_id": int(guild_id),
        "user_id": int(user_id),
        "amount": int(actual_amount),
        "balance_before": int(before),
        "balance_after": int(after),
        "reason": _safe_text(reason),
        "actor_id": int(actor_id),
        "type": str(tx_type),
        "event_id": str(event_id or ""),
        "meta": tx_meta,
    }
    _gtx(guild_id).append(tx)
    save_transactions()

    # Railway-Konsole: jede relevante EC-Buchung einzeln sichtbar machen.
    if str(tx_type) in {"event_award", "manual_adjust", "starting_balance", "loot_auction", "loot_sale", "weekly_decay"} or int(actual_amount) != 0:
        _railway_log_transaction(tx)

    return tx


def _apply_weekly_decay(guild_id: int, actor_id: int = 0) -> list[tuple[int, int]]:
    c = _gcfg(guild_id)
    percent = float(c.get("decay_percent", DEFAULT_DECAY_PERCENT) or 0)
    protected = int(c.get("decay_protected_balance", DEFAULT_DECAY_PROTECTED_BALANCE) or 0)
    users = _gbal(guild_id).setdefault("users", {})
    changed: list[tuple[int, int]] = []

    for uid_s, old_v in list(users.items()):
        try:
            uid = int(uid_s)
            old = int(old_v or 0)
        except Exception:
            continue

        if old <= protected or percent <= 0:
            continue

        taxable = old - protected
        kept_taxable = int(math.floor(taxable * (1.0 - percent / 100.0)))
        new = protected + kept_taxable
        diff = new - old
        if diff == 0:
            continue

        _add_transaction(
            guild_id,
            uid,
            diff,
            f"Wöchentlicher EC-Verfall ({percent:.1f}% nur über {protected} EC)",
            actor_id,
            "weekly_decay",
            meta={
                "protected_balance": protected,
                "decay_percent": percent,
                "weekly_period": _weekly_period_key(),
                "automatic": actor_id == 0,
            },
        )
        changed.append((uid, diff))

    return changed


async def _run_scheduled_weekly_reset(client: discord.Client) -> None:
    """Führt den Wochenreset einmal pro Donnerstag-10-Uhr-Periode aus.

    Falls der Bot um 10:00 offline ist, wird der Reset beim nächsten Lauf nachgeholt.
    Beim ersten Start wird nur die aktuelle Periode vermerkt, ohne sofortigen Verfall.
    """
    now = datetime.now(TZ)
    current_key = _weekly_period_key(now)

    for guild in getattr(client, "guilds", []) or []:
        home_id = _home_guild_id(default=guild.id)
        if int(guild.id) != int(home_id):
            continue

        c = _gcfg(guild.id)
        last_key = str(c.get("last_decay_period", "") or "")

        if not last_key:
            c["last_decay_period"] = current_key
            dkp_cfg[str(guild.id)] = c
            save_cfg()
            continue

        if last_key == current_key:
            continue

        changed = _apply_weekly_decay(guild.id, actor_id=0)
        c["last_decay_period"] = current_key
        c["last_decay_at"] = now.isoformat()
        dkp_cfg[str(guild.id)] = c
        save_cfg()

        emb = discord.Embed(
            title="📉 Automatischer EC-Wochenreset",
            description=(
                f"Reset: **Donnerstag 10:00 Uhr**\n"
                f"Event-Wochenlimit: **{int(c.get('weekly_event_limit', DEFAULT_WEEKLY_EVENT_LIMIT))} EC**\n"
                f"Verfall: **{float(c.get('decay_percent', DEFAULT_DECAY_PERCENT)):.1f}%** "
                f"nur auf den Anteil über **{int(c.get('decay_protected_balance', DEFAULT_DECAY_PROTECTED_BALANCE))} EC**\n"
                f"Betroffene Konten: **{len(changed)}**"
            ),
            color=discord.Color.orange(),
            timestamp=now,
        )
        await _log_to_channel(client, guild.id, emb)


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
    # Ebolus-Regel: Reserve bekommt immer fix 5 EC, unabhängig vom Eventwert.
    return int(DEFAULT_RESERVE_POINTS)


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


def _signup_label(signup: str) -> str:
    signup = str(signup or "")
    return {
        "TANK": "Tank",
        "HEAL": "Heal",
        "DPS": "DPS",
        "BANK": "Reserve",
        "MANUAL": "nachgetragen",
    }.get(signup, signup or "?")


def _attendance_status_label(status: str) -> str:
    status = str(status or "")
    return {
        "present": "✅ War da",
        "reserve": "🪑 Reserve",
        "maybe": "❔ Vielleicht",
        "absent": "❌ Nicht da",
        "excused": "🟡 Entschuldigt",
        "open": "⚪ Offen",
        "": "⚪ Offen",
    }.get(status, "⚪ Offen")


def _participant_display_name(client: discord.Client, guild_id: int, participant: dict) -> str:
    try:
        uid = int(participant.get("id", 0) or 0)
    except Exception:
        uid = 0
    if uid:
        return _display_member(client, guild_id, uid)
    return str(participant.get("name", "Spieler") or "Spieler")


def _attendance_status_counts(event: dict) -> dict[str, int]:
    participants = event.get("participants") or []
    attendance = event.get("attendance") or {}
    counts = {"present": 0, "reserve": 0, "maybe": 0, "absent": 0, "excused": 0, "open": 0}
    for p in participants:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            uid = 0
        if not uid:
            continue
        status = str((attendance.get(str(uid)) or {}).get("status", "") or "")
        if not status:
            status = "open"
        if status not in counts:
            status = "open"
        counts[status] += 1
    return counts


async def _refresh_attendance_check_message(
    client: discord.Client,
    guild_id: int,
    event_id: str,
    channel_id: int,
    message_id: int,
) -> None:
    if not channel_id or not message_id:
        return
    rsvp = _import_rsvp()
    event = rsvp.get_attendance_event(int(guild_id), str(event_id)) if rsvp else None
    if not event:
        return
    event_type = _dkp_type_from_event(event, str(event_id)) or "Unbekannt"
    emb = _attendance_check_embed(client, int(guild_id), event, str(event_id), event_type)
    ch = client.get_channel(int(channel_id))
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            fetched = await client.fetch_channel(int(channel_id))
            ch = fetched if isinstance(fetched, (discord.TextChannel, discord.Thread)) else None
        except Exception:
            ch = None
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        print(f"[dkp_system] EC-Anwesenheitscheck Update: Kanal nicht gefunden channel_id={channel_id}")
        return
    try:
        msg = await ch.fetch_message(int(message_id))
        await msg.edit(embed=emb, view=ECEventCheckView(int(guild_id), str(event_id)))
    except Exception as e:
        print(f"[dkp_system] EC-Anwesenheitscheck konnte nicht aktualisiert werden: {e!r}")


def _attendance_check_embed(client: discord.Client, home_guild_id: int, event: dict, event_id: str, event_type: str) -> discord.Embed:
    present, reserve, skipped_partner, _skipped_open = _attendance_summary_for_award(client, home_guild_id, event, event_type)
    counts = _attendance_status_counts(event)
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
        "Die Anmeldung ist nur ein Vorschlag. Bitte bestätige die echte Anwesenheit, bevor EC vergeben werden.\n"
        "Nachgetragene Spieler werden **nur für diese EC-Anwesenheit** gespeichert und ändern keine Event-Anmeldung."
    )

    participants = event.get("participants") or []
    attendance = event.get("attendance") or {}
    lines = []
    for p in participants[:25]:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            uid = 0
        if not uid:
            continue
        signup = str(p.get("signup", "") or "")
        status = str((attendance.get(str(uid)) or {}).get("status", "") or "")
        status_label = _attendance_status_label(status)
        role = _signup_label(signup)
        manual = " *(nachgetragen)*" if bool(p.get("manual")) or str(p.get("source", "") or "") == "manual" or signup == "MANUAL" else ""
        marker = "EC" if _is_ebolus_member(client, home_guild_id, uid) else "Allianz"
        lines.append(f"• {status_label} {marker} <@{uid}> – {role}{manual}")
    if len(participants) > 25:
        lines.append(f"… {len(participants) - 25} weitere")
    emb.add_field(name="Anmeldungen / EC-Vorschlag", value="\n".join(lines)[:1000] if lines else "—", inline=False)

    emb.add_field(
        name="Aktueller Status",
        value=(
            f"✅ War da: **{counts.get('present', 0)}**\n"
            f"🪑 Reserve: **{counts.get('reserve', 0)}**\n"
            f"❔ Vielleicht: **{counts.get('maybe', 0)}**\n"
            f"❌ Nicht da / 🟡 Entschuldigt: **{counts.get('absent', 0) + counts.get('excused', 0)}**\n"
            f"🤝 Partner ohne EC: **{len(skipped_partner)}**\n"
            f"⚪ Noch offen / nicht bestätigt: **{counts.get('open', 0)}**\n\n"
            f"EC bei Vergabe: **{len(present)}** volle Wertung, **{len(reserve)}** Reserve"
        ),
        inline=False,
    )

    voice_id = _event_voice_channel_id(event)
    if voice_id:
        st = _event_check_state(int(home_guild_id), str(event_id))
        if st.get("voice_suggest_applied_at"):
            counts_voice = st.get("voice_suggest_counts") if isinstance(st.get("voice_suggest_counts"), dict) else {}
            actor_id = int(st.get("voice_suggest_applied_by", 0) or 0)
            actor_txt = f"<@{actor_id}>" if actor_id else "unbekannt"
            emb.add_field(
                name="🎙️ Voice-Vorschlag",
                value=(
                    f"Optional angewendet von {actor_txt}.\n"
                    f"✅ {counts_voice.get('present', 0)} · 🟡 {counts_voice.get('partial', 0)} · ❌ {counts_voice.get('absent', 0)} · ↪️ {counts_voice.get('skipped_existing', 0)} behalten"
                ),
                inline=False,
            )
        else:
            emb.add_field(
                name="🎙️ Voice-Vorschlag",
                value=f"Optional verfügbar über Button. Zählt nur den Event-Voice <#{voice_id}>.",
                inline=False,
            )

    emb.set_footer(text="Buttons: Anwesenheit bestätigen/ändern → optional Voice-Vorschlag → EC-Vorschau → EC vergeben")
    return emb


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(timezone.utc).isoformat()


def _fmt_voice_minutes(seconds: int) -> str:
    minutes = max(0, int(round(int(seconds or 0) / 60)))
    if minutes < 60:
        return f"{minutes} Min"
    h = minutes // 60
    m = minutes % 60
    return f"{h}h {m:02d}m"


def _event_voice_channel_id(event: dict) -> int:
    # voice_last_channel_id bleibt erhalten, auch wenn der temporäre Event-Voice nach dem Event gelöscht wurde.
    for key in ("voice_channel_id", "voice_last_channel_id", "event_voice_channel_id"):
        try:
            value = int(event.get(key, 0) or 0)
            if value:
                return value
        except Exception:
            continue
    return 0


def _voice_suggestion_settings(guild_id: int) -> dict[str, int]:
    # Noch bewusst einfache Defaults. Später wandert das ins Dashboard / guild_settings.
    c = _gcfg(int(guild_id))
    voice_cfg = c.get("voice_attendance") if isinstance(c.get("voice_attendance"), dict) else {}
    return {
        "duration_min": max(15, min(int(voice_cfg.get("duration_min", 120) or 120), 600)),
        "pre_min": max(0, min(int(voice_cfg.get("pre_min", 15) or 15), 180)),
        "full_pct": max(1, min(int(voice_cfg.get("full_pct", 70) or 70), 100)),
        "partial_pct": max(0, min(int(voice_cfg.get("partial_pct", 20) or 20), 100)),
    }


async def _apply_voice_attendance_suggestion(
    client: discord.Client,
    guild_id: int,
    event_id: str,
    actor: discord.abc.User,
) -> tuple[bool, str, Optional[discord.Embed]]:
    """Übernimmt Voice-Zeiten optional in den EC-Anwesenheitscheck.

    Wichtig: Das läuft nur auf Button-Klick. Es wird nicht automatisch bei Events aktiviert.
    Bestehende manuelle Status bleiben erhalten und werden nicht überschrieben.
    """
    if aggregate_voice_seconds is None:
        return False, "Voice-Attendance-Datenbankfunktion ist nicht geladen.", None

    rsvp = _import_rsvp()
    if not rsvp:
        return False, "RSVP-/Anwesenheitssystem nicht geladen.", None

    event = rsvp.get_attendance_event(int(guild_id), str(event_id))
    if not event:
        return False, "Event nicht gefunden.", None

    voice_id = _event_voice_channel_id(event)
    if not voice_id:
        return False, "Dieses Event hat keinen gespeicherten Event-Voice. Voice-Vorschlag bleibt optional und ist hier nicht aktiv.", None

    when = _parse_when(str(event.get("when_iso", "") or ""))
    if not when:
        return False, "Event hat keine gültige Startzeit.", None

    participants = list(event.get("participants") or [])
    if not participants:
        return False, "Dieses Event hat keine EC-Anwesenheitsliste.", None

    cfg = _voice_suggestion_settings(int(guild_id))
    # partial darf nicht größer als full sein.
    if cfg["partial_pct"] > cfg["full_pct"]:
        cfg["partial_pct"] = cfg["full_pct"]

    window_start = when - timedelta(minutes=cfg["pre_min"])
    window_end = when + timedelta(minutes=cfg["duration_min"])
    event_seconds = max(1, cfg["duration_min"] * 60)

    participant_ids: list[int] = []
    for p in participants:
        try:
            uid = int(p.get("id", 0) or 0)
            if uid:
                participant_ids.append(uid)
        except Exception:
            continue

    totals = aggregate_voice_seconds(
        int(guild_id),
        since_iso=_iso_utc(window_start),
        until_iso=_iso_utc(window_end),
        channel_ids=[int(voice_id)],
        user_ids=participant_ids or None,
    )

    attendance = event.get("attendance") if isinstance(event.get("attendance"), dict) else {}
    changed_present = 0
    changed_partial = 0
    changed_absent = 0
    skipped_existing = 0
    failed = 0
    preview_lines: list[str] = []

    for p in participants:
        try:
            uid = int(p.get("id", 0) or 0)
        except Exception:
            uid = 0
        if not uid:
            continue

        current = str((attendance.get(str(uid)) or {}).get("status", "") or "")
        seconds = int(totals.get(uid, 0) or 0)
        pct = int(round((seconds / event_seconds) * 100)) if event_seconds else 0
        name = _participant_display_name(client, int(guild_id), p)
        signup = _signup_label(str(p.get("signup", "") or ""))

        if pct >= cfg["full_pct"]:
            target_status = "present"
            bucket = "✅"
        elif pct >= cfg["partial_pct"]:
            target_status = "maybe"
            bucket = "🟡"
        else:
            target_status = "absent"
            bucket = "❌"

        if current:
            skipped_existing += 1
            if len(preview_lines) < 12:
                preview_lines.append(f"↪️ {name} – bereits gesetzt: {_attendance_status_label(current)}")
            continue

        ok = False
        try:
            ok = bool(rsvp.set_attendance_status(int(guild_id), str(event_id), uid, target_status, int(getattr(actor, "id", 0) or 0)))
        except Exception:
            ok = False

        if not ok:
            failed += 1
            continue

        if target_status == "present":
            changed_present += 1
        elif target_status == "maybe":
            changed_partial += 1
        elif target_status == "absent":
            changed_absent += 1

        if len(preview_lines) < 12:
            preview_lines.append(f"{bucket} {name} – {signup} – {_fmt_voice_minutes(seconds)} ({pct}%) → {_attendance_status_label(target_status)}")

    st = _event_check_state(int(guild_id), str(event_id))
    st["voice_suggest_applied_at"] = _now_iso()
    st["voice_suggest_applied_by"] = int(getattr(actor, "id", 0) or 0)
    st["voice_suggest_voice_channel_id"] = int(voice_id)
    st["voice_suggest_counts"] = {
        "present": changed_present,
        "partial": changed_partial,
        "absent": changed_absent,
        "skipped_existing": skipped_existing,
        "failed": failed,
    }
    save_event_checks()

    title, _when_txt = _event_title_and_time(event)
    emb = discord.Embed(
        title="🎙️ Voice-Vorschlag übernommen",
        description=(
            f"**Event:** {title}\n"
            f"**Event-ID:** `{event_id}`\n"
            f"**Voice:** <#{voice_id}>\n"
            f"**Fenster:** {window_start.strftime('%d.%m.%Y %H:%M')} – {window_end.strftime('%H:%M')}\n\n"
            "Das war **optional** und wurde nur durch diesen Button-Klick angewendet. "
            "Bestehende manuelle Status wurden nicht überschrieben."
        ),
        color=discord.Color.green(),
    )
    emb.add_field(
        name="Übernommen",
        value=(
            f"✅ War da: **{changed_present}**\n"
            f"🟡 Teilweise / prüfen: **{changed_partial}**\n"
            f"❌ Nicht erkannt: **{changed_absent}**\n"
            f"↪️ Schon manuell gesetzt: **{skipped_existing}**\n"
            f"⚠️ Fehlgeschlagen: **{failed}**"
        ),
        inline=False,
    )
    if preview_lines:
        emb.add_field(name="Auszug", value="\n".join(preview_lines)[:1024], inline=False)
    emb.set_footer(text=f"Schwellen: voll ab {cfg['full_pct']}%, teilweise ab {cfg['partial_pct']}%, Dauer {cfg['duration_min']} Min, Vorlauf {cfg['pre_min']} Min")

    if audit_log:
        try:
            audit_log(
                guild_id=int(guild_id),
                actor_id=int(getattr(actor, "id", 0) or 0),
                action="ec_attendance_voice_suggestion_apply",
                target_type="event",
                target_id=str(event_id),
                summary=f"Voice-Vorschlag in EC-Anwesenheit übernommen: {title}",
                metadata={
                    "voice_channel_id": int(voice_id),
                    "counts": st["voice_suggest_counts"],
                    "duration_min": cfg["duration_min"],
                    "pre_min": cfg["pre_min"],
                    "full_pct": cfg["full_pct"],
                    "partial_pct": cfg["partial_pct"],
                },
            )
        except Exception:
            pass

    msg = f"✅ Voice-Vorschlag übernommen: {changed_present} war da, {changed_partial} teilweise, {changed_absent} nicht erkannt."
    if skipped_existing:
        msg += f" {skipped_existing} vorhandene manuelle Status wurden nicht überschrieben."
    return True, msg, emb


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
        tx = _add_transaction(
            int(guild_id),
            int(row["user_id"]),
            int(row["requested_points"]),
            f"Event-Teilnahme: {event.get('title', 'Event')} ({event_type})",
            int(getattr(actor, "id", 0) or 0),
            "event_award",
            event_id=str(event_id),
            meta={"event_type": event_type, "signup": row.get("signup", ""), "event_title": str(event.get("title", "Event") or "Event"), "target_name": str(row.get("name", "") or "")},
        )
        row["points"] = int(tx.get("amount", 0) or 0)
        row["weekly_limited"] = row["points"] < int(row.get("requested_points", row["points"]) or 0)

    st = _event_check_state(int(guild_id), str(event_id))
    st["awarded"] = True
    st["awarded_at"] = _now_iso()
    save_event_checks()

    emb = _award_log_embed(event, event_type, present, reserve, skipped_partner, skipped_open, actor)
    await _log_to_channel(client, int(guild_id), emb)
    return True, f"EC vergeben: {len(awarded)} Ebolus-Spieler.", emb


class ECAttendanceParticipantSelect(Select):
    def __init__(self, guild_id: int, event_id: str, participants: list[dict], page: int, source_channel_id: int, source_message_id: int):
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)
        self.page = int(page)
        self.source_channel_id = int(source_channel_id)
        self.source_message_id = int(source_message_id)
        start = self.page * 25
        chunk = participants[start:start + 25]
        options: list[discord.SelectOption] = []
        for p in chunk:
            try:
                uid = int(p.get("id", 0) or 0)
            except Exception:
                uid = 0
            if not uid:
                continue
            raw_name = str(p.get("name", "") or f"User {uid}")
            label = raw_name[:90]
            signup = _signup_label(str(p.get("signup", "") or ""))
            manual = " • nachgetragen" if bool(p.get("manual")) or str(p.get("source", "") or "") == "manual" or str(p.get("signup", "") or "") == "MANUAL" else ""
            options.append(discord.SelectOption(label=label, value=str(uid), description=(signup + manual)[:100]))
        if not options:
            options = [discord.SelectOption(label="Keine Spieler gefunden", value="0", description="Dieses Event hat keine Anwesenheitsliste")]
        super().__init__(placeholder=f"Spieler wählen – Seite {self.page + 1}", min_values=1, max_values=1, options=options, custom_id="dkp_attendance_participant_select")

    async def callback(self, inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        try:
            uid = int(self.values[0])
        except Exception:
            uid = 0
        if not uid:
            await inter.response.send_message("❌ Kein Spieler gewählt.", ephemeral=True)
            return
        await inter.response.edit_message(
            content=f"Anwesenheit für <@{uid}> setzen:",
            embed=None,
            view=ECAttendanceStatusView(self.guild_id, self.event_id, uid, self.page, self.source_channel_id, self.source_message_id),
        )


class ECAttendanceParticipantView(View):
    def __init__(self, guild_id: int, event_id: str, event: dict, page: int, source_channel_id: int, source_message_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)
        self.event = event
        self.page = int(page)
        self.source_channel_id = int(source_channel_id)
        self.source_message_id = int(source_message_id)
        self.participants = list(event.get("participants") or [])
        self.add_item(ECAttendanceParticipantSelect(self.guild_id, self.event_id, self.participants, self.page, self.source_channel_id, self.source_message_id))

    async def _show_page(self, inter: discord.Interaction, page: int):
        rsvp = _import_rsvp()
        event = rsvp.get_attendance_event(self.guild_id, self.event_id) if rsvp else None
        if not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(
            content="Wähle einen Spieler aus der EC-Anwesenheitsliste. Das ändert keine Event-Anmeldung.",
            embed=None,
            view=ECAttendanceParticipantView(self.guild_id, self.event_id, event, page, self.source_channel_id, self.source_message_id),
        )

    @button(label="◀️", style=ButtonStyle.secondary, custom_id="dkp_attendance_page_prev", row=1)
    async def prev_page(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        await self._show_page(inter, max(0, self.page - 1))

    @button(label="▶️", style=ButtonStyle.secondary, custom_id="dkp_attendance_page_next", row=1)
    async def next_page(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        max_page = max(0, (len(self.participants) - 1) // 25)
        await self._show_page(inter, min(max_page, self.page + 1))


class ECAttendanceStatusView(View):
    def __init__(self, guild_id: int, event_id: str, target_user_id: int, page: int, source_channel_id: int, source_message_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)
        self.target_user_id = int(target_user_id)
        self.page = int(page)
        self.source_channel_id = int(source_channel_id)
        self.source_message_id = int(source_message_id)

    async def _set_status(self, inter: discord.Interaction, status: str):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.edit_original_response(content="❌ RSVP-/Anwesenheitssystem nicht geladen.", embed=None, view=None)
            return
        ok = rsvp.set_attendance_status(self.guild_id, self.event_id, self.target_user_id, status, int(inter.user.id))
        if not ok:
            await inter.edit_original_response(content="❌ Anwesenheit konnte nicht gespeichert werden.", embed=None, view=None)
            return
        await _refresh_attendance_check_message(inter.client, self.guild_id, self.event_id, self.source_channel_id, self.source_message_id)
        label = _attendance_status_label(status)
        await inter.edit_original_response(content=f"✅ <@{self.target_user_id}> wurde gesetzt auf: **{label}**", embed=None, view=None)

    @button(label="✅ War da", style=ButtonStyle.success, custom_id="dkp_attendance_status_present", row=0)
    async def present(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await self._set_status(inter, "present")

    @button(label="🪑 Reserve", style=ButtonStyle.secondary, custom_id="dkp_attendance_status_reserve", row=0)
    async def reserve(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await self._set_status(inter, "reserve")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dkp_attendance_status_maybe", row=0)
    async def maybe(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await self._set_status(inter, "maybe")

    @button(label="❌ Nicht da", style=ButtonStyle.danger, custom_id="dkp_attendance_status_absent", row=1)
    async def absent(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await self._set_status(inter, "absent")

    @button(label="⚪ Offen", style=ButtonStyle.secondary, custom_id="dkp_attendance_status_clear", row=1)
    async def clear(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await self._set_status(inter, "clear")

    @button(label="🗑️ Aus EC-Liste entfernen", style=ButtonStyle.danger, custom_id="dkp_attendance_status_remove", row=2)
    async def remove(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.edit_original_response(content="❌ RSVP-/Anwesenheitssystem nicht geladen.", embed=None, view=None)
            return
        ok = rsvp.remove_attendance_participant(self.guild_id, self.event_id, self.target_user_id, int(inter.user.id))
        if not ok:
            await inter.edit_original_response(content="❌ Spieler konnte nicht aus der EC-Anwesenheitsliste entfernt werden.", embed=None, view=None)
            return
        await _refresh_attendance_check_message(inter.client, self.guild_id, self.event_id, self.source_channel_id, self.source_message_id)
        await inter.edit_original_response(content=f"✅ <@{self.target_user_id}> wurde aus der EC-Anwesenheitsliste entfernt.", embed=None, view=None)


class ECAttendanceAddUserSelect(UserSelect):
    def __init__(self, guild_id: int, event_id: str, source_channel_id: int, source_message_id: int):
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)
        self.source_channel_id = int(source_channel_id)
        self.source_message_id = int(source_message_id)
        super().__init__(placeholder="Spieler nachtragen – nur für EC-Anwesenheit", min_values=1, max_values=1, custom_id="dkp_attendance_add_user_select")

    async def callback(self, inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.edit_original_response(content="❌ RSVP-/Anwesenheitssystem nicht geladen.", embed=None, view=None)
            return

        selected = self.values[0] if self.values else None
        uid = int(getattr(selected, "id", 0) or 0)
        if not uid:
            await inter.edit_original_response(content="❌ Bitte ein Servermitglied auswählen.", embed=None, view=None)
            return

        member = selected if isinstance(selected, discord.Member) else inter.guild.get_member(uid)
        if member is None:
            try:
                member = await inter.guild.fetch_member(uid)
            except Exception:
                member = None

        display_name = (
            str(getattr(member, "display_name", "") or "")
            or str(getattr(selected, "display_name", "") or "")
            or str(getattr(selected, "name", "") or f"User {uid}")
        )

        ok = rsvp.add_attendance_participant(
            self.guild_id,
            self.event_id,
            uid,
            display_name,
            signup="DPS",
            status="present",
            marked_by=int(inter.user.id),
        )
        if not ok:
            await inter.edit_original_response(content="❌ Spieler konnte nicht zur EC-Anwesenheit hinzugefügt werden. Event-Snapshot fehlt oder Event-ID ist ungültig.", embed=None, view=None)
            return
        await _refresh_attendance_check_message(inter.client, self.guild_id, self.event_id, self.source_channel_id, self.source_message_id)
        await inter.edit_original_response(
            content=(
                f"✅ <@{uid}> wurde **nur zur EC-Anwesenheit** nachgetragen und erstmal auf **War da** gesetzt.\n"
                "Die normale Event-Anmeldung wurde nicht verändert."
            ),
            embed=None,
            view=ECAttendanceStatusView(self.guild_id, self.event_id, uid, 0, self.source_channel_id, self.source_message_id),
        )


class ECAttendanceAddUserView(View):
    def __init__(self, guild_id: int, event_id: str, source_channel_id: int, source_message_id: int):
        super().__init__(timeout=None)
        self.add_item(ECAttendanceAddUserSelect(guild_id, event_id, source_channel_id, source_message_id))

    async def on_error(self, inter: discord.Interaction, error: Exception, item: discord.ui.Item[Any]) -> None:
        print(f"[dkp_system] ECAttendanceAddUserView Fehler event={getattr(item, 'event_id', '?')}: {error!r}")
        try:
            if inter.response.is_done():
                await inter.followup.send("❌ Spieler konnte nicht nachgetragen werden. Bitte erneut versuchen.", ephemeral=True)
            else:
                await inter.response.send_message("❌ Spieler konnte nicht nachgetragen werden. Bitte erneut versuchen.", ephemeral=True)
        except Exception:
            pass


class ECEventCheckView(View):
    def __init__(self, guild_id: int, event_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.event_id = str(event_id)

    async def on_error(self, inter: discord.Interaction, error: Exception, item: discord.ui.Item[Any]) -> None:
        print(f"[dkp_system] ECEventCheckView Fehler event={self.event_id}: {error!r}")
        try:
            if inter.response.is_done():
                await inter.followup.send("❌ EC-Anwesenheitscheck konnte nicht verarbeitet werden. Bitte kurz erneut versuchen oder den Check neu posten.", ephemeral=True)
            else:
                await inter.response.send_message("❌ EC-Anwesenheitscheck konnte nicht verarbeitet werden. Bitte kurz erneut versuchen oder den Check neu posten.", ephemeral=True)
        except Exception:
            pass

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

    @button(label="Alle Anmeldungen bestätigen", style=ButtonStyle.success, custom_id="dkp_check_confirm_all", row=0)
    async def confirm_all(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        rsvp = _import_rsvp()
        if not rsvp:
            await inter.edit_original_response(content="❌ RSVP-System nicht geladen.", embed=None, view=None)
            return
        event = rsvp.get_attendance_event(self.guild_id, self.event_id)
        if not event:
            # Snapshot aus Store nachziehen, falls möglich.
            obj = (getattr(rsvp, "store", {}) or {}).get(str(self.event_id))
            if isinstance(obj, dict):
                event = rsvp.ensure_attendance_snapshot(inter.client, self.event_id, obj)
        if not event:
            await inter.edit_original_response(content="❌ Event nicht gefunden.", embed=None, view=None)
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
        try:
            await inter.message.edit(embed=emb, view=self)
        except Exception as e:
            print(f"[dkp_system] EC-Check confirm_all Message-Update fehlgeschlagen: {e!r}")
        await inter.edit_original_response(content=f"✅ {count} angemeldete Spieler wurden als 'War da' bestätigt. Nutze bei Bedarf „Einzel bearbeiten“ oder „Spieler nachtragen“.", embed=None, view=None)

    @button(label="Einzel bearbeiten", style=ButtonStyle.secondary, custom_id="dkp_check_edit_one", row=0)
    async def edit_one(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        rsvp = _import_rsvp()
        event = rsvp.get_attendance_event(self.guild_id, self.event_id) if rsvp else None
        if not event:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
            return
        source_channel_id = int(getattr(inter.channel, "id", 0) or 0)
        source_message_id = int(getattr(inter.message, "id", 0) or 0) if inter.message else 0
        await inter.response.send_message(
            "Wähle einen Spieler aus der EC-Anwesenheitsliste. Das ändert keine Event-Anmeldung.",
            view=ECAttendanceParticipantView(self.guild_id, self.event_id, event, 0, source_channel_id, source_message_id),
            ephemeral=True,
        )

    @button(label="Spieler nachtragen", style=ButtonStyle.secondary, custom_id="dkp_check_add_user", row=0)
    async def add_user(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        source_channel_id = int(getattr(inter.channel, "id", 0) or 0)
        source_message_id = int(getattr(inter.message, "id", 0) or 0) if inter.message else 0
        await inter.response.send_message(
            "Wähle einen Spieler, der **nur für diese EC-Anwesenheit** nachgetragen werden soll. Die normale Event-Anmeldung bleibt unverändert.",
            view=ECAttendanceAddUserView(self.guild_id, self.event_id, source_channel_id, source_message_id),
            ephemeral=True,
        )

    @button(label="🎙️ Voice-Vorschlag übernehmen", style=ButtonStyle.secondary, custom_id="dkp_check_voice_suggest", row=1)
    async def voice_suggest(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        ok, msg, emb = await _apply_voice_attendance_suggestion(inter.client, self.guild_id, self.event_id, inter.user)
        if not ok:
            await inter.edit_original_response(content=f"❌ {msg}", embed=None, view=None)
            return
        try:
            source_channel_id = int(getattr(inter.channel, "id", 0) or 0)
            source_message_id = int(getattr(inter.message, "id", 0) or 0) if inter.message else 0
            await _refresh_attendance_check_message(inter.client, self.guild_id, self.event_id, source_channel_id, source_message_id)
        except Exception as e:
            print(f"[dkp_system] EC-Check Voice-Vorschlag Message-Update fehlgeschlagen: {e!r}")
        await inter.edit_original_response(content=msg, embed=emb, view=None)

    @button(label="EC-Vorschau", style=ButtonStyle.secondary, custom_id="dkp_check_preview", row=1)
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

    @button(label="EC vergeben", style=ButtonStyle.primary, custom_id="dkp_check_award", row=1)
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

    @button(label="Später / ignorieren", style=ButtonStyle.danger, custom_id="dkp_check_ignore", row=1)
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



# ---------------------------------------------------------------------------
# Dashboard → Bot EC-Buchungsqueue
# ---------------------------------------------------------------------------
# Dashboard-Web schreibt nur Postgres-Anfragen. Nur dieser Bot schreibt echte
# dkp_balances.json / dkp_transactions.json. Damit bleiben produktive JSON-Daten
# beim Bot und werden nicht vom Web-Service lokal kaputtgeschrieben.


def _dash_database_url() -> str:
    return str(os.getenv("DATABASE_URL") or "").strip()


def _dash_normalized_database_url() -> str:
    url = _dash_database_url()
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _dash_pg_connect():
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore
    return psycopg.connect(_dash_normalized_database_url(), row_factory=dict_row, connect_timeout=10)


def _dashboard_queue_enabled() -> bool:
    url = _dash_database_url().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")



def _phase3_jsonb(value: Any):
    from psycopg.types.json import Jsonb  # type: ignore
    return Jsonb(value if value is not None else {})


def _phase3_ec_enabled() -> bool:
    return _dashboard_queue_enabled()


def _ensure_phase3_ec_schema() -> None:
    """Phase 3.2: EC/DKP-Tabellen vorbereiten. JSON bleibt Hauptquelle."""
    if not _phase3_ec_enabled():
        return
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_balances (
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    balance INTEGER NOT NULL DEFAULT 0,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'bot_parallel',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_transactions (
                    guild_id TEXT NOT NULL,
                    transaction_id TEXT NOT NULL,
                    user_id TEXT,
                    amount INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    event_id TEXT,
                    created_at_text TEXT,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'bot_parallel',
                    mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, transaction_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_ec_event_checks (
                    guild_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    status TEXT,
                    awarded BOOLEAN NOT NULL DEFAULT FALSE,
                    posted BOOLEAN NOT NULL DEFAULT FALSE,
                    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source TEXT NOT NULL DEFAULT 'bot_parallel',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (guild_id, event_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phase3_migration_runs (
                    run_id TEXT PRIMARY KEY,
                    guild_id TEXT,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    counts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    notes TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_ec_tx_user ON phase3_ec_transactions (guild_id, user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_ec_tx_event ON phase3_ec_transactions (guild_id, event_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_phase3_ec_checks_status ON phase3_ec_event_checks (guild_id, awarded, posted)")
        conn.commit()
    finally:
        conn.close()


def _phase3_write_migration_run(mode: str, status: str, counts: dict, notes: str = "", guild_id: str = "") -> None:
    if not _phase3_ec_enabled():
        return
    _ensure_phase3_ec_schema()
    conn = _dash_pg_connect()
    try:
        run_id = f"phase3ec_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO phase3_migration_runs (run_id, guild_id, mode, status, counts_json, notes, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,now())
            """, (run_id, str(guild_id or ""), str(mode), str(status), _phase3_jsonb(counts or {}), str(notes or "")))
        conn.commit()
    finally:
        conn.close()


def _phase3_mirror_ec_balances_to_pg() -> dict:
    if not _phase3_ec_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt", "balances": 0}
    _ensure_phase3_ec_schema()
    count = 0
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            for gid, g in list((dkp_balances or {}).items()):
                users = g.get("users") if isinstance(g, dict) else {}
                if not isinstance(users, dict):
                    continue
                for uid, bal in list(users.items()):
                    try:
                        balance = int(bal or 0)
                    except Exception:
                        balance = 0
                    raw = {"balance": balance, "guild_id": str(gid), "user_id": str(uid)}
                    cur.execute("""
                        INSERT INTO phase3_ec_balances (guild_id, user_id, balance, raw_json, source, updated_at)
                        VALUES (%s,%s,%s,%s,'bot_parallel',now())
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET
                          balance=EXCLUDED.balance,
                          raw_json=EXCLUDED.raw_json,
                          source='bot_parallel',
                          updated_at=now()
                    """, (str(gid), str(uid), balance, _phase3_jsonb(raw)))
                    count += 1
        conn.commit()
        return {"ok": True, "balances": count}
    finally:
        conn.close()


def _phase3_mirror_ec_transactions_to_pg() -> dict:
    if not _phase3_ec_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt", "transactions": 0}
    _ensure_phase3_ec_schema()
    count = 0
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            for gid, arr in list((dkp_transactions or {}).items()):
                if not isinstance(arr, list):
                    continue
                for tx in arr:
                    if not isinstance(tx, dict):
                        continue
                    tx_id = str(tx.get("id") or tx.get("transaction_id") or "").strip()
                    if not tx_id:
                        # Fallback nur für alte Alt-Daten ohne ID.
                        tx_id = f"tx_{abs(hash(json.dumps(tx, sort_keys=True, ensure_ascii=False, default=str)))}"
                    cur.execute("""
                        INSERT INTO phase3_ec_transactions (guild_id, transaction_id, user_id, amount, reason, event_id, created_at_text, raw_json, source, mirrored_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'bot_parallel',now())
                        ON CONFLICT (guild_id, transaction_id) DO UPDATE SET
                          user_id=EXCLUDED.user_id,
                          amount=EXCLUDED.amount,
                          reason=EXCLUDED.reason,
                          event_id=EXCLUDED.event_id,
                          created_at_text=EXCLUDED.created_at_text,
                          raw_json=EXCLUDED.raw_json,
                          source='bot_parallel',
                          mirrored_at=now()
                    """, (
                        str(gid),
                        tx_id,
                        str(tx.get("user_id") or ""),
                        int(tx.get("amount", 0) or 0),
                        str(tx.get("reason") or tx.get("type") or ""),
                        str(tx.get("event_id") or ""),
                        str(tx.get("created_at") or tx.get("timestamp") or ""),
                        _phase3_jsonb(tx),
                    ))
                    count += 1
        conn.commit()
        return {"ok": True, "transactions": count}
    finally:
        conn.close()


def _phase3_mirror_ec_event_checks_to_pg() -> dict:
    if not _phase3_ec_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt", "checks": 0}
    _ensure_phase3_ec_schema()
    count = 0
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            for gid, g in list((dkp_event_checks or {}).items()):
                events = g.get("events") if isinstance(g, dict) else {}
                if not isinstance(events, dict):
                    continue
                for event_id, st in list(events.items()):
                    if not isinstance(st, dict):
                        continue
                    awarded = bool(st.get("awarded", False))
                    posted = bool(st.get("posted", False))
                    status = "awarded" if awarded else ("posted" if posted else str(st.get("status") or "open"))
                    cur.execute("""
                        INSERT INTO phase3_ec_event_checks (guild_id, event_id, status, awarded, posted, raw_json, source, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,'bot_parallel',now())
                        ON CONFLICT (guild_id, event_id) DO UPDATE SET
                          status=EXCLUDED.status,
                          awarded=EXCLUDED.awarded,
                          posted=EXCLUDED.posted,
                          raw_json=EXCLUDED.raw_json,
                          source='bot_parallel',
                          updated_at=now()
                    """, (str(gid), str(event_id), status, awarded, posted, _phase3_jsonb(st)))
                    count += 1
        conn.commit()
        return {"ok": True, "checks": count}
    finally:
        conn.close()


def _phase3_mirror_all_ec_to_pg() -> dict:
    if not _phase3_ec_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt oder ist keine Postgres-URL."}
    counts = {"balances": 0, "transactions": 0, "checks": 0}
    try:
        counts.update(_phase3_mirror_ec_balances_to_pg())
        counts.update(_phase3_mirror_ec_transactions_to_pg())
        counts.update(_phase3_mirror_ec_event_checks_to_pg())
        counts.pop("ok", None)
        _phase3_write_migration_run("ec_parallel_mirror", "done", counts, "Bot hat EC/DKP parallel nach Postgres gespiegelt.")
        return {"ok": True, "counts": counts}
    except Exception as e:
        _phase3_write_migration_run("ec_parallel_mirror", "failed", counts, repr(e))
        return {"ok": False, "error": repr(e), "counts": counts}


def _phase3_ec_status(guild_id: int = 0) -> dict:
    if not _phase3_ec_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt oder ist keine Postgres-URL."}
    _ensure_phase3_ec_schema()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            where = "WHERE guild_id = %s" if int(guild_id or 0) else ""
            args = (str(int(guild_id)),) if int(guild_id or 0) else ()
            out = {}
            for table in ("phase3_ec_balances", "phase3_ec_transactions", "phase3_ec_event_checks"):
                cur.execute(f"SELECT COUNT(*) AS c FROM {table} {where}", args)
                out[table] = int((cur.fetchone() or {}).get("c") or 0)
        json_counts = {"balances": 0, "transactions": 0, "checks": 0}
        for gid, g in list((dkp_balances or {}).items()):
            if int(guild_id or 0) and str(gid) != str(int(guild_id)):
                continue
            users = g.get("users") if isinstance(g, dict) else {}
            json_counts["balances"] += len(users) if isinstance(users, dict) else 0
        for gid, arr in list((dkp_transactions or {}).items()):
            if int(guild_id or 0) and str(gid) != str(int(guild_id)):
                continue
            json_counts["transactions"] += len(arr) if isinstance(arr, list) else 0
        for gid, g in list((dkp_event_checks or {}).items()):
            if int(guild_id or 0) and str(gid) != str(int(guild_id)):
                continue
            events = g.get("events") if isinstance(g, dict) else {}
            json_counts["checks"] += len(events) if isinstance(events, dict) else 0
        return {"ok": True, "postgres": out, "json": json_counts}
    finally:
        conn.close()


def _ensure_dashboard_ec_award_table() -> None:
    if not _dashboard_queue_enabled():
        return
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_ec_award_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    full_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    partial_ec DOUBLE PRECISION NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    actor_name TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    processed_at TIMESTAMPTZ,
                    result_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_ec_award_requests_lookup
                ON dashboard_ec_award_requests (guild_id, event_id, status, requested_at DESC)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _claim_dashboard_ec_award_requests(limit: int = 3) -> list[dict]:
    if not _dashboard_queue_enabled():
        return []
    _ensure_dashboard_ec_award_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dashboard_ec_award_requests
                SET status = 'processing', claimed_at = NOW()
                WHERE id IN (
                    SELECT id
                    FROM dashboard_ec_award_requests
                    WHERE status = 'pending'
                    ORDER BY requested_at ASC, id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
                """,
                (int(limit),),
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]
        conn.commit()
        return rows
    finally:
        conn.close()


def _finish_dashboard_ec_award_request(request_id: str, status: str, result: dict) -> None:
    if not _dashboard_queue_enabled() or not request_id:
        return
    _ensure_dashboard_ec_award_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dashboard_ec_award_requests
                SET status = %s, processed_at = NOW(), result_json = %s
                WHERE request_id = %s
                """,
                (str(status), json.dumps(result or {}, ensure_ascii=False, separators=(",", ":")), str(request_id)),
            )
        conn.commit()
    finally:
        conn.close()


def _dashboard_ec_award_queue_summary(guild_id: int, limit: int = 8) -> dict:
    """Kleine Diagnose für Leader-Commands: zeigt, ob die Dashboard-Queue lebt."""
    if not _dashboard_queue_enabled():
        return {"ok": False, "error": "DATABASE_URL fehlt oder ist keine Postgres-URL."}
    _ensure_dashboard_ec_award_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s
                GROUP BY status
                ORDER BY status
                """,
                (int(guild_id),),
            )
            counts = {str(r.get("status") or "unknown"): int(r.get("count") or 0) for r in (cur.fetchall() or [])}
            cur.execute(
                """
                SELECT request_id, guild_id, event_id, event_type, status, actor_name, requested_at, claimed_at, processed_at, result_json
                FROM dashboard_ec_award_requests
                WHERE guild_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), int(limit)),
            )
            recent = []
            for row in cur.fetchall() or []:
                item = dict(row)
                try:
                    item["result"] = json.loads(item.get("result_json") or "{}")
                except Exception:
                    item["result"] = {}
                recent.append(item)
        return {"ok": True, "counts": counts, "recent": recent}
    finally:
        conn.close()


def _dashboard_queue_status_text(status: str) -> str:
    s = str(status or "").lower()
    return {
        "pending": "⏳ offen",
        "processing": "🔄 verarbeitet",
        "done": "✅ erledigt",
        "failed": "❌ Fehler",
        "rejected": "⛔ blockiert",
        "cancelled": "⚪ abgebrochen",
    }.get(s, s or "—")


class _DashboardActor:
    def __init__(self, actor_id: int, name: str):
        self.id = int(actor_id or 0)
        self.name = str(name or "Dashboard")
        self.display_name = self.name

    def __str__(self) -> str:
        return self.name


def _dashboard_ec_award_embed(payload: dict, applied: list[dict], skipped: list[dict], actor_name: str) -> discord.Embed:
    event_title = str(payload.get("event_title") or payload.get("event_id") or "Event")
    event_type = str(payload.get("event_type") or "Dashboard Attendance")
    emb = discord.Embed(title="🌐 EC über Dashboard gebucht", color=discord.Color.green())
    emb.description = f"**{event_title}**\nTyp: **{event_type}**\nQuelle: Attendance Review im Web-Dashboard"
    total = sum(int(x.get("amount", 0) or 0) for x in applied)
    lines = []
    for row in applied[:25]:
        uid = int(row.get("user_id", 0) or 0)
        amount = int(row.get("amount", 0) or 0)
        requested = int(row.get("requested_amount", amount) or amount)
        suffix = f" (Limit: statt {requested} EC)" if amount < requested else ""
        lines.append(f"• <@{uid}>: **+{amount} EC**{suffix}")
    if len(applied) > 25:
        lines.append(f"… {len(applied) - 25} weitere")
    emb.add_field(name="Gebucht", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    if skipped:
        slines = []
        for row in skipped[:20]:
            name = str(row.get("display_name") or row.get("user_id") or "Unbekannt")
            reason = str(row.get("reason") or "übersprungen")
            slines.append(f"• {name}: {reason}")
        emb.add_field(name="Übersprungen", value="\n".join(slines)[:1000], inline=False)
    emb.add_field(name="Summe", value=f"**{total} EC** an **{len(applied)}** Spieler", inline=False)
    emb.set_footer(text=f"Ausgeführt von {actor_name or 'Dashboard'} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
    return emb


async def _process_dashboard_ec_award_request(client: discord.Client, row: dict) -> None:
    request_id = str(row.get("request_id") or "")
    try:
        guild_id = int(row.get("guild_id", 0) or 0)
        event_id = str(row.get("event_id") or "")
        event_type = str(row.get("event_type") or "Dashboard Attendance").strip() or "Dashboard Attendance"
        payload = json.loads(row.get("payload_json") or "{}")
        if not isinstance(payload, dict):
            raise RuntimeError("payload_json ist kein Objekt")
        rows = [x for x in (payload.get("rows") or []) if isinstance(x, dict)]
        actor_raw = payload.get("requested_by") if isinstance(payload.get("requested_by"), dict) else {}
        actor_id = int(actor_raw.get("id") or row.get("actor_id") or 0)
        actor_name = str(actor_raw.get("name") or row.get("actor_name") or "Dashboard")

        home_id = _home_guild_id(default=guild_id)
        if int(guild_id) != int(home_id):
            _finish_dashboard_ec_award_request(request_id, "rejected", {"ok": False, "error": "EC wird nur auf dem Ebolus/Home-Server gebucht.", "guild_id": guild_id, "home_id": home_id})
            return

        if _event_has_ec_award_for_event(int(home_id), event_id):
            _finish_dashboard_ec_award_request(request_id, "rejected", {"ok": False, "error": "Doppelbuchung blockiert: Für dieses Event gibt es bereits event_award-Transaktionen.", "event_id": event_id})
            return

        applied: list[dict] = []
        skipped: list[dict] = []
        for item in rows:
            try:
                uid = int(item.get("user_id", 0) or 0)
            except Exception:
                uid = 0
            requested_amount = int(round(float(item.get("ec_gain", 0) or 0)))
            if not uid:
                skipped.append({**item, "reason": "keine User-ID"})
                continue
            if requested_amount <= 0:
                skipped.append({**item, "reason": "0 EC"})
                continue
            if not _is_ebolus_member(client, int(home_id), uid):
                skipped.append({**item, "reason": "keine Ebolus-Gildenrolle / nicht im Home-Server"})
                continue
            tx = _add_transaction(
                int(home_id),
                uid,
                requested_amount,
                f"Dashboard Attendance Review: {payload.get('event_title', 'Event')} ({event_type})",
                actor_id,
                "event_award",
                event_id=event_id,
                meta={
                    "event_type": event_type,
                    "event_title": str(payload.get("event_title") or "Event"),
                    "target_name": str(item.get("display_name") or ""),
                    "signup": str(item.get("signup") or ""),
                    "review_status": str(item.get("status") or ""),
                    "voice_minutes": item.get("voice_minutes"),
                    "dashboard_request_id": request_id,
                    "source": "dashboard_attendance_review",
                },
            )
            actual = int(tx.get("amount", 0) or 0)
            applied.append({
                "user_id": uid,
                "display_name": str(item.get("display_name") or f"User {uid}"),
                "requested_amount": requested_amount,
                "amount": actual,
                "balance_before": int(tx.get("balance_before", 0) or 0),
                "balance_after": int(tx.get("balance_after", 0) or 0),
                "tx_id": str(tx.get("id") or ""),
                "limited": actual < requested_amount,
            })

        if not applied:
            _finish_dashboard_ec_award_request(request_id, "failed", {"ok": False, "error": "Keine EC-Buchung angewendet.", "skipped": skipped})
            return

        st = _event_check_state(int(home_id), event_id)
        st["awarded"] = True
        st["awarded_at"] = _now_iso()
        st["awarded_source"] = "dashboard"
        st["dashboard_request_id"] = request_id
        _gchecks(int(home_id)).setdefault("events", {})[str(event_id)] = st
        save_event_checks()

        try:
            emb = _dashboard_ec_award_embed(payload, applied, skipped, actor_name)
            await _log_to_channel(client, int(home_id), emb)
        except Exception as e:
            print(f"[dkp_system] Dashboard-EC Log-Embed Fehler: {e!r}", flush=True)

        _finish_dashboard_ec_award_request(request_id, "done", {
            "ok": True,
            "event_id": event_id,
            "event_type": event_type,
            "applied_count": len(applied),
            "skipped_count": len(skipped),
            "total_ec": sum(int(x.get("amount", 0) or 0) for x in applied),
            "applied": applied,
            "skipped": skipped,
        })
    except Exception as e:
        print(f"[dkp_system] Dashboard-EC Request Fehler {request_id}: {e!r}", flush=True)
        _finish_dashboard_ec_award_request(request_id, "failed", {"ok": False, "error": repr(e)})


@tasks.loop(minutes=1)
async def dashboard_ec_award_request_loop():
    client = getattr(dashboard_ec_award_request_loop, "_client", None)
    if client is None:
        return
    if not _dashboard_queue_enabled():
        return
    try:
        rows = _claim_dashboard_ec_award_requests(limit=3)
    except Exception as e:
        print(f"[dkp_system] Dashboard-EC Queue konnte nicht gelesen werden: {e!r}", flush=True)
        return
    for row in rows:
        await _process_dashboard_ec_award_request(client, row)


# ---------------------------------------------------------------------------
# Dashboard → Bot Einstellungsqueue
# ---------------------------------------------------------------------------
# Das Dashboard darf dkp_cfg.json nicht direkt schreiben, weil es in einem
# anderen Railway-Container läuft. Deshalb landen Regeländerungen zuerst in
# Postgres und werden hier vom Bot übernommen.


def _ensure_dashboard_settings_change_table() -> None:
    if not _dashboard_queue_enabled():
        return
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_settings_change_requests (
                    id BIGSERIAL PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    guild_id BIGINT NOT NULL,
                    scope TEXT NOT NULL DEFAULT 'dkp',
                    action_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    actor_name TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    processed_at TIMESTAMPTZ,
                    result_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_settings_change_requests_lookup
                ON dashboard_settings_change_requests (guild_id, scope, status, requested_at DESC)
                """
            )
        conn.commit()
    finally:
        conn.close()


def _claim_dashboard_settings_change_requests(limit: int = 5) -> list[dict]:
    if not _dashboard_queue_enabled():
        return []
    _ensure_dashboard_settings_change_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dashboard_settings_change_requests
                SET status = 'processing', claimed_at = NOW()
                WHERE id IN (
                    SELECT id
                    FROM dashboard_settings_change_requests
                    WHERE status = 'pending'
                    ORDER BY requested_at ASC, id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, request_id, guild_id, scope, action_type, status, payload_json, actor_id, actor_name, requested_at
                """,
                (int(limit),),
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]
        conn.commit()
        return rows
    finally:
        conn.close()


def _finish_dashboard_settings_change_request(request_id: str, status: str, result: dict) -> None:
    if not _dashboard_queue_enabled() or not request_id:
        return
    _ensure_dashboard_settings_change_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dashboard_settings_change_requests
                SET status = %s, processed_at = NOW(), result_json = %s
                WHERE request_id = %s
                """,
                (str(status), json.dumps(result, ensure_ascii=False, separators=(",", ":")), str(request_id)),
            )
        conn.commit()
    finally:
        conn.close()


def _dashboard_settings_change_summary(guild_id: int, limit: int = 8) -> dict:
    if not _dashboard_queue_enabled():
        return {"enabled": False, "counts": {}, "rows": []}
    _ensure_dashboard_settings_change_table()
    conn = _dash_pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM dashboard_settings_change_requests
                WHERE guild_id = %s
                GROUP BY status
                """,
                (int(guild_id),),
            )
            counts = {str(r["status"]): int(r["count"]) for r in (cur.fetchall() or [])}
            cur.execute(
                """
                SELECT request_id, action_type, status, requested_at, processed_at, result_json
                FROM dashboard_settings_change_requests
                WHERE guild_id = %s
                ORDER BY requested_at DESC, id DESC
                LIMIT %s
                """,
                (int(guild_id), int(limit)),
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]
        return {"enabled": True, "counts": counts, "rows": rows}
    finally:
        conn.close()


def _apply_dashboard_settings_change(row: dict) -> dict:
    request_id = str(row.get("request_id") or "")
    guild_id = int(row.get("guild_id") or 0)
    action = str(row.get("action_type") or "").strip().lower()
    try:
        payload = json.loads(row.get("payload_json") or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    if not guild_id:
        return {"ok": False, "error": "Guild-ID fehlt."}
    c = _gcfg(guild_id)
    before = json.loads(json.dumps(c, ensure_ascii=False))

    if action == "set_event_points":
        event_type = str(payload.get("event_type") or "").strip()
        if event_type not in EVENT_TYPE_CHOICES:
            return {"ok": False, "error": f"Unbekannter Eventtyp: {event_type}"}
        try:
            points = int(float(str(payload.get("points") or "0").replace(",", ".")))
        except Exception:
            return {"ok": False, "error": "Ungültiger EC-Wert."}
        if points < 0 or points > 500:
            return {"ok": False, "error": "EC-Wert muss zwischen 0 und 500 liegen."}
        pts = c.get("event_points") if isinstance(c.get("event_points"), dict) else {}
        old = int(pts.get(event_type, DEFAULT_EVENT_POINTS.get(event_type, 0)) or 0)
        pts[event_type] = points
        c["event_points"] = pts
        save_cfg()
        return {"ok": True, "message": f"{event_type}: {old} → {points} EC", "changed": {"event_type": event_type, "old": old, "new": points}}

    if action == "set_weekly_limit":
        try:
            limit = int(float(str(payload.get("weekly_event_limit") or "0").replace(",", ".")))
        except Exception:
            return {"ok": False, "error": "Ungültiges Wochenlimit."}
        if limit < 0 or limit > 1000:
            return {"ok": False, "error": "Wochenlimit muss zwischen 0 und 1000 liegen."}
        old = int(c.get("weekly_event_limit", DEFAULT_WEEKLY_EVENT_LIMIT) or 0)
        c["weekly_event_limit"] = limit
        save_cfg()
        return {"ok": True, "message": f"Wochenlimit: {old} → {limit} EC", "changed": {"old": old, "new": limit}}

    if action == "set_decay":
        try:
            percent = float(str(payload.get("decay_percent") or "0").replace(",", "."))
            protected = int(float(str(payload.get("decay_protected_balance") or "0").replace(",", ".")))
        except Exception:
            return {"ok": False, "error": "Ungültige Verfall-Regel."}
        if percent < 0 or percent > 100:
            return {"ok": False, "error": "Verfall muss zwischen 0 und 100 Prozent liegen."}
        if protected < 0 or protected > 100000:
            return {"ok": False, "error": "Schutzbetrag ist unplausibel."}
        old_percent = float(c.get("decay_percent", DEFAULT_DECAY_PERCENT) or 0)
        old_protected = int(c.get("decay_protected_balance", DEFAULT_DECAY_PROTECTED_BALANCE) or 0)
        c["decay_percent"] = percent
        c["decay_protected_balance"] = protected
        save_cfg()
        return {
            "ok": True,
            "message": f"Verfall: {old_percent:.1f}%/{old_protected} EC Schutz → {percent:.1f}%/{protected} EC Schutz",
            "changed": {"decay_percent_old": old_percent, "decay_percent_new": percent, "protected_old": old_protected, "protected_new": protected},
        }


    if action == "set_roles":
        def _parse_id(name: str) -> int:
            raw = str(payload.get(name) or "0").strip()
            if raw in {"", "—", "-"}:
                raw = "0"
            if not raw.isdigit():
                raise ValueError(f"{name} ist keine gültige Discord-ID.")
            value = int(raw or 0)
            if value and (value < 10**16 or value > 10**22):
                raise ValueError(f"{name} sieht nicht wie eine Discord-ID aus.")
            return value
        try:
            leader_role_id = _parse_id("leader_role_id")
            member_role_id = _parse_id("member_role_id")
            log_channel_id = _parse_id("log_channel_id")
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}

        old_log = int(c.get("log_channel_id", 0) or 0)
        if log_channel_id:
            c["log_channel_id"] = int(log_channel_id)
            dkp_cfg[str(guild_id)] = c
            save_cfg()

        leader_cfg = _load_leader_cfg()
        old_leader = int(((leader_cfg.get(str(guild_id)) or {}).get("leader_role_id", 0)) or 0)
        if leader_role_id:
            lc = leader_cfg.get(str(guild_id)) or {}
            lc["leader_role_id"] = int(leader_role_id)
            leader_cfg[str(guild_id)] = lc
            _save_leader_cfg(leader_cfg)

        portal_cfg = _load_portal_cfg()
        old_member = int(((portal_cfg.get(str(guild_id)) or {}).get("member_role_id", 0)) or 0)
        if member_role_id:
            pc = portal_cfg.get(str(guild_id)) or {}
            pc["member_role_id"] = int(member_role_id)
            portal_cfg[str(guild_id)] = pc
            _save_portal_cfg(portal_cfg)

        return {
            "ok": True,
            "message": "Rollen/Kanal übernommen. Danach /dashboard_status ausführen und neu einloggen, falls Dashboard-Zugriff betroffen ist.",
            "changed": {
                "log_channel_id_old": old_log,
                "log_channel_id_new": log_channel_id or old_log,
                "leader_role_id_old": old_leader,
                "leader_role_id_new": leader_role_id or old_leader,
                "member_role_id_old": old_member,
                "member_role_id_new": member_role_id or old_member,
            },
        }

    if action == "set_access_roles":
        def _parse_list(name: str) -> list[int]:
            raw = str(payload.get(name) or "")
            out: list[int] = []
            for part in raw.replace(";", ",").split(","):
                p = part.strip()
                if not p:
                    continue
                if not p.isdigit():
                    raise ValueError(f"{name} enthält keine gültige Discord-ID: {p}")
                value = int(p)
                if value < 10**16 or value > 10**22:
                    raise ValueError(f"{name} sieht nicht wie eine Discord-ID aus: {p}")
                if value not in out:
                    out.append(value)
            return out
        try:
            admin_roles = _parse_list("dashboard_admin_role_ids")
            allowed_roles = _parse_list("dashboard_allowed_role_ids")
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        old_admin = c.get("dashboard_admin_role_ids") or []
        old_allowed = c.get("dashboard_allowed_role_ids") or []
        c["dashboard_admin_role_ids"] = admin_roles
        c["dashboard_allowed_role_ids"] = allowed_roles
        dkp_cfg[str(guild_id)] = c
        save_cfg()
        return {
            "ok": True,
            "message": "Dashboard-Zugriffsrollen gespeichert. Aktiv wird es nach frischem Snapshot und erneutem Login, sofern dashboard_data diese Rollen exportiert.",
            "changed": {"admin_old": old_admin, "admin_new": admin_roles, "allowed_old": old_allowed, "allowed_new": allowed_roles},
        }

    return {"ok": False, "error": f"Unbekannte Einstellungsaktion: {action}"}


async def _process_dashboard_settings_change_request(client: discord.Client, row: dict) -> None:
    request_id = str(row.get("request_id") or "")
    guild_id = int(row.get("guild_id") or 0)
    actor_id = int(row.get("actor_id") or 0) if str(row.get("actor_id") or "").isdigit() else 0
    actor_name = str(row.get("actor_name") or "Dashboard")
    try:
        result = _apply_dashboard_settings_change(row)
        if result.get("ok"):
            result.update({"request_id": request_id, "actor_name": actor_name})
            _finish_dashboard_settings_change_request(request_id, "done", result)
            print(f"[dkp_system] Dashboard-Settings übernommen {request_id}: {result.get('message')}", flush=True)
            if audit_log:
                try:
                    audit_log(
                        guild_id=guild_id,
                        actor_id=actor_id,
                        action="dashboard_settings_change_apply",
                        target_type="settings",
                        target_id=str(row.get("action_type") or "dkp_cfg"),
                        summary=str(result.get("message") or "Dashboard-Einstellung übernommen"),
                        metadata={"request_id": request_id, "result": result},
                    )
                except Exception:
                    pass
        else:
            _finish_dashboard_settings_change_request(request_id, "rejected", {"ok": False, "error": result.get("error") or "abgelehnt", "request_id": request_id})
            print(f"[dkp_system] Dashboard-Settings abgelehnt {request_id}: {result.get('error')}", flush=True)
    except Exception as e:
        print(f"[dkp_system] Dashboard-Settings Fehler {request_id}: {e!r}", flush=True)
        _finish_dashboard_settings_change_request(request_id, "failed", {"ok": False, "error": repr(e), "request_id": request_id})


@tasks.loop(minutes=1)
async def dashboard_settings_change_request_loop():
    client = getattr(dashboard_settings_change_request_loop, "_client", None)
    if client is None:
        return
    if not _dashboard_queue_enabled():
        return
    try:
        rows = _claim_dashboard_settings_change_requests(limit=5)
    except Exception as e:
        print(f"[dkp_system] Dashboard-Settings Queue konnte nicht gelesen werden: {e!r}", flush=True)
        return
    for row in rows:
        await _process_dashboard_settings_change_request(client, row)


@tasks.loop(minutes=5)
async def dkp_event_check_loop():
    client = getattr(dkp_event_check_loop, "_client", None)
    if client is None:
        return
    try:
        await _run_scheduled_weekly_reset(client)
    except Exception as e:
        print(f"[dkp_system] Automatischer Wochenreset fehlgeschlagen: {e!r}")

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


def _event_has_ec_award_for_event(guild_id: int, event_id: str) -> bool:
    for tx in _gtx(guild_id):
        if str(tx.get("event_id", "")) == str(event_id) and str(tx.get("type")) == "event_award":
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



def _event_is_dkp_enabled(event: dict | None, event_id: str = "") -> bool:
    """True, wenn ein Event einen gespeicherten EC-/DKP-Typ hat.

    Wichtig für den Test-Command: Nicht nur dkp_enabled prüfen,
    weil ältere/Allianz-Events manchmal nur dkp_event_type gespeichert haben.
    """
    return bool(_dkp_type_from_event(event, event_id))


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
        is_reserve = status == "reserve" or (status == "present" and signup == "BANK")
        row = {
            "user_id": uid,
            "name": str(p.get("name", "") or _display_member(client, home_guild_id, uid)),
            "signup": signup,
            "status": status or "open",
        }
        if status not in {"present", "reserve"}:
            skipped_not_present.append(row)
            continue
        if not _is_ebolus_member(client, home_guild_id, uid):
            skipped_not_ebolus.append(row)
            continue
        requested = reserve_points if is_reserve else base_points
        remaining = weekly_event_remaining(home_guild_id, uid)
        row["requested_points"] = requested
        row["points"] = min(requested, remaining)
        row["weekly_remaining_before"] = remaining
        row["weekly_limited"] = row["points"] < requested
        if is_reserve:
            reserve.append(row)
        else:
            present.append(row)

    return present, reserve, skipped_not_ebolus, skipped_not_present

def _register_persistent_event_check_views(client: discord.Client) -> int:
    """Registriert offene EC-Anwesenheitscheck-Buttons nach Restart/Deploy neu.

    Ohne diese Registrierung sind alte Check-Nachrichten im Server zwar sichtbar,
    aber ihre Buttons laufen nach einem Deploy in „Interaktion fehlgeschlagen“.
    """
    count = 0
    for gid_str, g in list((dkp_event_checks or {}).items()):
        try:
            gid = int(gid_str)
        except Exception:
            continue
        events = g.get("events") if isinstance(g, dict) else {}
        if not isinstance(events, dict):
            continue
        for event_id, st in list(events.items()):
            if not isinstance(st, dict):
                continue
            if not bool(st.get("posted", False)):
                continue
            if bool(st.get("ignored", False)) or bool(st.get("awarded", False)):
                continue
            mid = int(st.get("message_id", 0) or 0)
            if not mid:
                continue
            try:
                client.add_view(ECEventCheckView(gid, str(event_id)), message_id=mid)
                count += 1
            except Exception as e:
                print(f"[dkp_system] EC-Check Persistent View konnte nicht registriert werden {gid}/{event_id}/{mid}: {e!r}")
    return count


async def setup_dkp_system(client: discord.Client, tree: app_commands.CommandTree):
    # Initialisiert Defaults für alle aktuell bekannten Guilds.
    for guild in getattr(client, "guilds", []) or []:
        c = _gcfg(int(guild.id))
        if not str(c.get("last_decay_period", "") or ""):
            c["last_decay_period"] = _weekly_period_key()
            dkp_cfg[str(int(guild.id))] = c
    save_cfg()

    try:
        registered_checks = _register_persistent_event_check_views(client)
        print(f"💰 EC-Anwesenheitscheck Persistent Views registriert: {registered_checks}")
    except Exception as e:
        print(f"[dkp_system] EC-Anwesenheitscheck Persistent Views Fehler: {e!r}")

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
    async def dkp_set_log_channel(inter: discord.Interaction):
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

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            c = _gcfg(pick_inter.guild.id)
            c["log_channel_id"] = int(channel.id)
            dkp_cfg[str(pick_inter.guild.id)] = c
            save_cfg()
            await pick_inter.response.edit_message(content=f"✅ DKP-/Loot-Log-Kanal gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "🧾 DKP-/Loot-Log-Kanal auswählen", _picked)

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
        emb.add_field(name="Reserve", value=f"Fix {int(DEFAULT_RESERVE_POINTS)} EC", inline=False)
        emb.add_field(name="Wochenlimit aus Events", value=f"{int(c.get('weekly_event_limit', DEFAULT_WEEKLY_EVENT_LIMIT))} EC", inline=True)
        emb.add_field(name="Reset", value="Donnerstag 10:00 Uhr", inline=True)
        emb.add_field(name="Startguthaben", value=f"{int(c.get('start_balance', DEFAULT_START_BALANCE))} EC", inline=True)
        emb.add_field(
            name="Wöchentlicher Verfall",
            value=(
                f"{float(c.get('decay_percent', DEFAULT_DECAY_PERCENT)):.1f}% "
                f"nur über {int(c.get('decay_protected_balance', DEFAULT_DECAY_PROTECTED_BALANCE))} EC"
            ),
            inline=True,
        )
        emb.add_field(name="Leader-Gutschriften", value="Nicht Teil des Wochenlimits", inline=True)
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
            tx = _add_transaction(
                int(home_id),
                int(row["user_id"]),
                int(row["requested_points"]),
                f"Event-Teilnahme: {event.get('title', 'Event')} ({resolved_event_type})",
                inter.user.id,
                "event_award",
                event_id=str(event_id),
                meta={"event_type": resolved_event_type, "signup": row.get("signup", ""), "event_title": str(event.get("title", "Event") or "Event"), "target_name": str(row.get("name", "") or "")},
            )
            row["points"] = int(tx.get("amount", 0) or 0)
            row["weekly_limited"] = row["points"] < int(row.get("requested_points", row["points"]) or 0)

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

        log_ch = _dkp_log_channel(client, int(home_id))
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
        c = _gcfg(inter.guild.id)
        percent = float(c.get("decay_percent", DEFAULT_DECAY_PERCENT) or 0)
        protected = int(c.get("decay_protected_balance", DEFAULT_DECAY_PROTECTED_BALANCE) or 0)
        users = _gbal(inter.guild.id).setdefault("users", {})
        if not users:
            await inter.followup.send("Keine EC-Konten vorhanden.", ephemeral=True)
            return
        changed = _apply_weekly_decay(inter.guild.id, actor_id=int(inter.user.id))
        emb = discord.Embed(title="📉 EC-Verfall ausgeführt", color=discord.Color.orange())
        lines = []
        for uid, diff in changed[:30]:
            lines.append(f"• <@{uid}>: **{_format_amount(diff)} EC**")
        if not lines:
            lines = ["Keine Änderungen."]
        emb.description = (
            f"Regel: **-{percent:.1f}% nur auf den Anteil über {protected} EC**\n"
            "Öffentliche Gesamtstände werden nicht angezeigt.\n\n" + "\n".join(lines)
        )
        if len(changed) > 30:
            emb.description += f"\n… {len(changed) - 30} weitere"
        emb.set_footer(text=f"Ausgeführt von {inter.user} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
        await _log_to_channel(client, inter.guild.id, emb)
        await inter.followup.send(f"✅ EC-Verfall ausgeführt. Betroffene Konten: **{len(changed)}**", ephemeral=True)

    @dkp.command(name="phase3_ec_status", description="Leader: Phase-3 EC/DKP Postgres-Status anzeigen")
    async def phase3_ec_status_cmd(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True)
        info = _phase3_ec_status(inter.guild.id)
        if not info.get("ok"):
            await inter.followup.send(f"❌ Phase-3 EC Status nicht verfügbar: `{info.get('error')}`", ephemeral=True)
            return
        js = info.get("json") or {}
        pg = info.get("postgres") or {}
        lines = [
            "🧱 **Phase 3.2 · EC/DKP Postgres**",
            f"EC-Konten: JSON **{js.get('balances', 0)}** · DB **{pg.get('phase3_ec_balances', 0)}**",
            f"EC-Verlauf: JSON **{js.get('transactions', 0)}** · DB **{pg.get('phase3_ec_transactions', 0)}**",
            f"Eventchecks: JSON **{js.get('checks', 0)}** · DB **{pg.get('phase3_ec_event_checks', 0)}**",
            "",
            "JSON bleibt aktuell Hauptquelle. Postgres ist Parallel-/Prüfschicht.",
        ]
        await inter.followup.send("\n".join(lines), ephemeral=True)

    @dkp.command(name="phase3_ec_mirror", description="Leader: EC/DKP jetzt manuell nach Postgres spiegeln")
    async def phase3_ec_mirror_cmd(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        res = _phase3_mirror_all_ec_to_pg()
        if not res.get("ok"):
            await inter.followup.send(f"❌ Spiegelung fehlgeschlagen: `{res.get('error')}`", ephemeral=True)
            return
        c = res.get("counts") or {}
        await inter.followup.send(
            "✅ Phase-3 EC/DKP gespiegelt.\n"
            f"Konten: **{c.get('balances', 0)}** · Transaktionen: **{c.get('transactions', 0)}** · Eventchecks: **{c.get('checks', 0)}**",
            ephemeral=True,
        )

    @dkp.command(name="dashboard_settings_status", description="Leader: Status der Dashboard-Einstellungsqueue")
    async def dashboard_settings_status(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True)
        try:
            info = _dashboard_settings_change_summary(inter.guild.id, limit=8)
        except Exception as e:
            await inter.followup.send(f"❌ Queue konnte nicht gelesen werden: `{e!r}`", ephemeral=True)
            return
        counts = info.get("counts") or {}
        rows = info.get("rows") or []
        lines = [f"pending: **{counts.get('pending', 0)}** · processing: **{counts.get('processing', 0)}** · done: **{counts.get('done', 0)}** · failed/rejected: **{counts.get('failed', 0) + counts.get('rejected', 0)}**"]
        for r in rows[:8]:
            result = ""
            try:
                result_obj = json.loads(r.get("result_json") or "{}")
                result = result_obj.get("message") or result_obj.get("error") or ""
            except Exception:
                result = ""
            lines.append(f"• `{r.get('status')}` · `{r.get('action_type')}` · {result or r.get('request_id')}")
        await inter.followup.send("⚙️ **Dashboard-Settings-Queue**\n" + "\n".join(lines), ephemeral=True)

    @dkp.command(name="dashboard_settings_run", description="Leader: Dashboard-Einstellungsqueue sofort verarbeiten")
    async def dashboard_settings_run(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True)
        try:
            rows = _claim_dashboard_settings_change_requests(limit=10)
        except Exception as e:
            await inter.followup.send(f"❌ Queue konnte nicht gelesen werden: `{e!r}`", ephemeral=True)
            return
        for row in rows:
            await _process_dashboard_settings_change_request(client, row)
        await inter.followup.send(f"✅ Settings-Queue verarbeitet. Anfragen: **{len(rows)}**", ephemeral=True)

    if not dkp_event_check_loop.is_running():
        dkp_event_check_loop._client = client  # type: ignore[attr-defined]
        dkp_event_check_loop.start()
        print("💰 EC-Eventcheck-Task gestartet.")

    if not dashboard_ec_award_request_loop.is_running():
        dashboard_ec_award_request_loop._client = client  # type: ignore[attr-defined]
        dashboard_ec_award_request_loop.start()
        print("🌐 Dashboard-EC-Buchungsqueue gestartet.")

    if not dashboard_settings_change_request_loop.is_running():
        dashboard_settings_change_request_loop._client = client  # type: ignore[attr-defined]
        dashboard_settings_change_request_loop.start()
        print("⚙️ Dashboard-Settings-Queue gestartet.")

    try:
        res = _phase3_mirror_all_ec_to_pg()
        if res.get("ok"):
            print(f"🧱 Phase 3.2 EC/DKP Startspiegelung: {res.get('counts')}", flush=True)
        else:
            print(f"🧱 Phase 3.2 EC/DKP noch nicht aktiv: {res.get('error')}", flush=True)
    except Exception as e:
        print(f"[phase3-ec] Startspiegelung Fehler: {e!r}", flush=True)

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
    lines = [
        (
            f"• <@{x['user_id']}>: **+{x['points']} EC**"
            + (f" (Limit: statt {x.get('requested_points')} EC)" if x.get("weekly_limited") else "")
        )
        for x in present
    ]
    emb.add_field(name="✅ Ebolus – Teilnahme", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    lines = [
        (
            f"• <@{x['user_id']}>: **+{x['points']} EC**"
            + (f" (Limit: statt {x.get('requested_points')} EC)" if x.get("weekly_limited") else "")
        )
        for x in reserve
    ]
    emb.add_field(name="🏦 Ebolus – Reserve", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    lines = [f"• {x.get('name') or ('User ' + str(x['user_id']))}" for x in skipped_partner]
    emb.add_field(name="🤝 Allianz/Partner – keine DKP", value="\n".join(lines)[:1000] if lines else "—", inline=False)
    open_count = len(skipped_open)
    emb.add_field(name="⚪ Nicht als 'War da' bestätigt", value=str(open_count), inline=True)
    return emb


def _award_log_embed(event: dict, event_type: str, present: list[dict], reserve: list[dict], skipped_partner: list[dict], skipped_open: list[dict], actor: discord.abc.User) -> discord.Embed:
    emb = _award_preview_embed(event, event_type, present, reserve, skipped_partner, skipped_open, duplicate=False)
    emb.title = "💰 EC vergeben"
    emb.color = discord.Color.green()
    total = sum(int(x.get("points", 0) or 0) for x in present + reserve)
    emb.add_field(name="Summe", value=f"**{total} EC** an **{len(present) + len(reserve)}** Ebolus-Spieler", inline=False)
    emb.set_footer(text=f"Bestätigt von {actor} • {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
    return emb
