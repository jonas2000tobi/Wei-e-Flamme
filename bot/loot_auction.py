from __future__ import annotations

import json
import math
import random
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button, Modal, TextInput
from discord.enums import ButtonStyle

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

AUCTION_FILE = DATA_DIR / "loot_auctions.json"
AUCTION_CFG_FILE = DATA_DIR / "loot_auction_cfg.json"
GUILD_CHEST_FILE = DATA_DIR / "guild_chest.json"
LOOT_ITEMS_FILE = DATA_DIR / "loot_items.json"
LOOT_NEEDS_FILE = DATA_DIR / "loot_needs.json"
MEMBER_PORTAL_CFG_FILE = DATA_DIR / "member_portal_cfg.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"

DEFAULT_DURATION_HOURS = 24
DEFAULT_MIN_INCREMENT = 5
DEFAULT_START_BID = 1
NEED_AUCTION_HOURS = 48
FREE_AUCTION_HOURS = 24
SALE_HOURS = 240
MAIN_NEED_START_BID = 30
SECOND_NEED_START_BID = 15
FREE_START_BID = 5
SALE_PRICE = 1
NEW_MEMBER_LOOT_LOCK_DAYS = 7
FREE_MIN_INCREMENT = 1
JUNK_ROLL_HOURS = 24
JUNK_ROLL_MAX_DEFAULT = 100
JUNK_ROLL_MAX_EXPANDED = 200
# Legacy alias, damit alte aktive Müll-Items nicht brechen.
JUNK_INTEREST_HOURS = JUNK_ROLL_HOURS

ELIGIBILITY_CHOICES = [
    app_commands.Choice(name="Automatisch: Main > Second > Frei", value="auto"),
    app_commands.Choice(name="Nur Main-Need-Spieler", value="main_need"),
    app_commands.Choice(name="Nur Second-Need-Spieler", value="secondary_need"),
    app_commands.Choice(name="Alle Ebolus-Mitglieder", value="all"),
]

_client_ref: Optional[discord.Client] = None


def _load_json(path: Path, default):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


auction_state: dict = _load_json(AUCTION_FILE, {})
auction_cfg: dict = _load_json(AUCTION_CFG_FILE, {})
guild_chest: dict = _load_json(GUILD_CHEST_FILE, {})


def save_auctions() -> None:
    _save_json(AUCTION_FILE, auction_state)


def save_cfg() -> None:
    _save_json(AUCTION_CFG_FILE, auction_cfg)


def save_chest() -> None:
    _save_json(GUILD_CHEST_FILE, guild_chest)


def _gchest(guild_id: int) -> dict:
    gid = str(int(guild_id))
    g = guild_chest.get(gid) or {}
    g.setdefault("items", {})
    guild_chest[gid] = g
    return g


def _now() -> datetime:
    return datetime.now(TZ)


def _now_iso() -> str:
    return _now().isoformat()


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _parse_dt(value: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(str(value or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _load_portal_cfg() -> dict:
    return _load_json(MEMBER_PORTAL_CFG_FILE, {})


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True
    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False
    cfg = _load_leader_cfg().get(str(inter.guild.id)) or {}
    role_id = int(cfg.get("leader_role_id", 0) or 0)
    role = inter.guild.get_role(role_id) if role_id else None
    return bool(role and role in inter.user.roles)


def _gcfg(guild_id: int) -> dict:
    gid = str(int(guild_id))
    c = auction_cfg.get(gid) or {}
    c.setdefault("auction_channel_id", 0)
    c.setdefault("log_channel_id", 0)
    c.setdefault("market_channel_id", 0)
    c.setdefault("active_channel_id", 0)
    auction_cfg[gid] = c
    return c


def _gauctions(guild_id: int) -> dict:
    gid = str(int(guild_id))
    g = auction_state.get(gid) or {}
    g.setdefault("auctions", {})
    auction_state[gid] = g
    return g


def _auction(guild_id: int, auction_id: str) -> Optional[dict]:
    return (_gauctions(guild_id).get("auctions") or {}).get(str(auction_id))


def _member_role_id(guild_id: int) -> int:
    c = _load_portal_cfg().get(str(int(guild_id))) or {}
    return int(c.get("member_role_id", 0) or 0)


def _is_ebolus_member(guild: discord.Guild, user_id: int) -> bool:
    member = guild.get_member(int(user_id))
    if not member or member.bot:
        return False
    role_id = _member_role_id(guild.id)
    if not role_id:
        return True
    role = guild.get_role(role_id)
    return bool(role and role in member.roles)


def _loot_lock_until_for_member(member: Optional[discord.Member]) -> Optional[datetime]:
    """Return the timestamp until which a member may not buy/bid on loot.

    Uses Discord server join time. This is reliable without an extra database and
    protects against brand-new accounts receiving guild loot immediately.
    """
    if not member or getattr(member, "bot", False):
        return None

    joined_at = getattr(member, "joined_at", None)
    if joined_at is None:
        return None

    if joined_at.tzinfo is None:
        joined_at = joined_at.replace(tzinfo=timezone.utc)

    until = joined_at.astimezone(TZ) + timedelta(days=NEW_MEMBER_LOOT_LOCK_DAYS)
    if _now() >= until:
        return None
    return until


def _format_timedelta_short(delta: timedelta) -> str:
    seconds = max(0, int(delta.total_seconds()))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = max(1, rem // 60) if days == 0 and hours == 0 else rem // 60

    parts: list[str] = []
    if days:
        parts.append(f"{days} Tag{'e' if days != 1 else ''}")
    if hours:
        parts.append(f"{hours} Std.")
    if not parts:
        parts.append(f"{minutes} Min.")
    return " ".join(parts[:2])


def _loot_lock_text_for_member(member: Optional[discord.Member]) -> str:
    until = _loot_lock_until_for_member(member)
    if not until:
        return ""
    remaining = _format_timedelta_short(until - _now())
    return f"Noch **{remaining}** bis **{until.strftime('%d.%m.%Y %H:%M')}**."


async def _require_loot_unlocked(inter: discord.Interaction, guild: discord.Guild, user_id: int) -> bool:
    member = guild.get_member(int(user_id))
    if member is None:
        try:
            member = await guild.fetch_member(int(user_id))
        except Exception:
            member = None

    until = _loot_lock_until_for_member(member)
    if not until:
        return True

    remaining = _format_timedelta_short(until - _now())
    await inter.response.send_message(
        "⏳ **Lootsperre für neue Mitglieder**\n"
        f"Du kannst erst nach **{NEW_MEMBER_LOOT_LOCK_DAYS} Tagen Gildenmitgliedschaft** auf Loot bieten oder Sale-Items kaufen.\n"
        f"Freischaltung: **{until.strftime('%d.%m.%Y %H:%M')}**\n"
        f"Restzeit: **{remaining}**",
        ephemeral=True,
    )
    return False


def _load_items() -> dict:
    return _load_json(LOOT_ITEMS_FILE, {})


def _load_needs() -> dict:
    return _load_json(LOOT_NEEDS_FILE, {})


def _all_items(guild_id: int) -> dict:
    g = _load_items().get(str(int(guild_id))) or {}
    return g.get("items") if isinstance(g.get("items"), dict) else {}


def _item_display(guild_id: int, item_id: str, fallback: str = "") -> str:
    item = _all_items(guild_id).get(str(item_id)) or {}
    if not item:
        return fallback or str(item_id or "Unbekanntes Item")
    name = str(item.get("name", item_id) or item_id)
    slot = str(item.get("slot", "") or "")
    wt = str(item.get("weapon_type", "") or "")
    if slot == "Waffe" and wt:
        return f"{name} ({wt})"
    return name


def _find_item(guild_id: int, query: str) -> tuple[str, str, list[tuple[str, str]]]:
    q = str(query or "").strip().lower()
    matches: list[tuple[str, str]] = []
    exact: list[tuple[str, str]] = []
    for item_id, item in _all_items(guild_id).items():
        name = str(item.get("name", "") or "")
        display = _item_display(guild_id, item_id, name)
        if str(item_id).lower() == q or name.lower() == q or display.lower() == q:
            exact.append((str(item_id), display))
        elif q and (q in name.lower() or q in display.lower() or q in str(item_id).lower()):
            matches.append((str(item_id), display))
    if exact:
        return exact[0][0], exact[0][1], exact + matches
    if len(matches) == 1:
        return matches[0][0], matches[0][1], matches
    return "", str(query or "Unbekanntes Item"), matches


def _slot_obj(value: Any) -> dict:
    if isinstance(value, dict):
        obj = dict(value)
        obj.setdefault("item_id", "")
        obj.setdefault("received", False)
        obj.setdefault("locked", bool(obj.get("received", False)))
        if bool(obj.get("locked", False)):
            obj["received"] = True
        if bool(obj.get("received", False)):
            obj["locked"] = True
        return obj
    if isinstance(value, str):
        return {"item_id": value, "received": False, "locked": False}
    return {"item_id": "", "received": False, "locked": False}


def _need_user_ids(guild: discord.Guild, item_id: str, tab: str) -> list[int]:
    if not item_id:
        return []
    tab = "Secondary" if str(tab).lower().startswith("sec") else "Main"
    needs = _load_needs().get(str(int(guild.id))) or {}
    users = needs.get("users") if isinstance(needs.get("users"), dict) else {}
    out: list[int] = []
    for uid_str, data in users.items():
        try:
            uid = int(uid_str)
        except Exception:
            continue
        if not _is_ebolus_member(guild, uid):
            continue
        bucket = data.get(tab) if isinstance(data, dict) else {}
        if not isinstance(bucket, dict):
            continue
        for slot_val in bucket.values():
            obj = _slot_obj(slot_val)
            if str(obj.get("item_id", "") or "") == str(item_id) and not bool(obj.get("received", False)):
                if uid not in out:
                    out.append(uid)
                break
    return out


def _main_need_user_ids(guild: discord.Guild, item_id: str) -> list[int]:
    return _need_user_ids(guild, item_id, "Main")


def _secondary_need_user_ids(guild: discord.Guild, item_id: str) -> list[int]:
    return _need_user_ids(guild, item_id, "Secondary")


def _need_mode_label(mode: str) -> str:
    mode = str(mode or "all")
    if mode == "main_need":
        return "Main-Need"
    if mode == "secondary_need":
        return "Second-Need"
    return "Freie Auktion"


def _eligible_user_ids(guild: discord.Guild, item_id: str, mode: str) -> tuple[str, list[int]]:
    mode = str(mode or "auto")
    main_users = _main_need_user_ids(guild, item_id)
    secondary_users = _secondary_need_user_ids(guild, item_id)
    if mode == "main_need":
        return "main_need", main_users
    if mode == "secondary_need":
        return "secondary_need", secondary_users
    if mode == "all":
        return "all", []
    # Automatik: Main hat Priorität. Secondary kommt nur dran, wenn es keinen Main-Need gibt.
    if main_users:
        return "main_need", main_users
    if secondary_users:
        return "secondary_need", secondary_users
    return "all", []


def _eligibility_text(auction: dict) -> str:
    mode = str(auction.get("eligibility_mode", "all") or "all")
    ids = [int(x) for x in auction.get("eligible_user_ids", []) or []]
    if mode in {"main_need", "secondary_need"}:
        label = "Main-Need" if mode == "main_need" else "Second-Need"
        if not ids:
            return f"Nur {label}, aber aktuell keine berechtigten Spieler gefunden."
        lines = ", ".join(f"<@{uid}>" for uid in ids[:12])
        if len(ids) > 12:
            lines += f" … +{len(ids)-12}"
        return f"Nur offene {label}-Spieler: {lines}"
    return "Alle Ebolus-Mitglieder dürfen bieten."


def _highest_bid(auction: dict) -> Optional[dict]:
    bids = auction.get("bids") if isinstance(auction.get("bids"), list) else []
    if not bids:
        return None
    return max(bids, key=lambda b: int(b.get("amount", 0) or 0))


def _current_price(auction: dict) -> int:
    top = _highest_bid(auction)
    if top:
        return int(top.get("amount", 0) or 0)
    return int(auction.get("start_bid", DEFAULT_START_BID) or DEFAULT_START_BID) - int(auction.get("min_increment", DEFAULT_MIN_INCREMENT) or DEFAULT_MIN_INCREMENT)


def _min_next_bid(auction: dict) -> int:
    top = _highest_bid(auction)
    if top:
        return int(top.get("amount", 0) or 0) + int(auction.get("min_increment", DEFAULT_MIN_INCREMENT) or DEFAULT_MIN_INCREMENT)
    return int(auction.get("start_bid", DEFAULT_START_BID) or DEFAULT_START_BID)


def _import_dkp():
    try:
        from bot import dkp_system as dkp  # type: ignore
        return dkp
    except Exception:
        try:
            import dkp_system as dkp  # type: ignore
            return dkp
        except Exception:
            return None


def _ec_balance(guild_id: int, user_id: int) -> int:
    dkp = _import_dkp()
    if dkp and hasattr(dkp, "get_balance"):
        try:
            return int(dkp.get_balance(int(guild_id), int(user_id)))
        except Exception:
            return 0
    # Direct fallback: same file names used by dkp_system.py
    data = _load_json(DATA_DIR / "dkp_balances.json", {})
    g = data.get(str(int(guild_id))) or {}
    users = g.get("users") if isinstance(g.get("users"), dict) else {}
    return int(users.get(str(int(user_id)), 0) or 0)


def _add_ec_transaction(guild_id: int, user_id: int, amount: int, reason: str, actor_id: int, auction_id: str, meta: Optional[dict] = None) -> bool:
    dkp = _import_dkp()
    if dkp and hasattr(dkp, "_add_transaction"):
        dkp._add_transaction(  # type: ignore[attr-defined]
            int(guild_id), int(user_id), int(amount), reason, int(actor_id), "loot_auction", event_id=str(auction_id), meta=meta or {}
        )
        return True
    return False


def _auction_channel(client: discord.Client, guild_id: int, fallback: Optional[discord.abc.GuildChannel] = None):
    c = _gcfg(guild_id)
    ch_id = int(c.get("auction_channel_id", 0) or 0)
    guild = client.get_guild(int(guild_id))
    if guild and ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return ch
    if isinstance(fallback, (discord.TextChannel, discord.Thread)):
        return fallback
    return None


def _log_channel(client: discord.Client, guild_id: int):
    c = _gcfg(guild_id)
    ch_id = int(c.get("log_channel_id", 0) or 0)
    guild = client.get_guild(int(guild_id))
    if guild and ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return ch
    # fallback: DKP log channel
    try:
        dkp = _import_dkp()
        if dkp and hasattr(dkp, "_dkp_log_channel"):
            return dkp._dkp_log_channel(client, int(guild_id))  # type: ignore[attr-defined]
    except Exception:
        pass
    return _auction_channel(client, guild_id)


def _market_channel(client: discord.Client, guild_id: int):
    """Öffentlicher Marktplatz-Kanal für Freie Auktionen und Sale-Käufe."""
    c = _gcfg(guild_id)
    ch_id = int(c.get("market_channel_id", 0) or 0)
    guild = client.get_guild(int(guild_id))
    if guild and ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return ch
    return None


def _active_auction_channel(client: discord.Client, guild_id: int):
    """Separater Kanal mit nur den aktuell verfügbaren Auktionen/Sales.

    Dieser Kanal ist als saubere Übersicht gedacht: eine kurze Karte pro aktives
    Item, keine Gebotsdetails, keine Abschlussmeldungen. Sobald das Item nicht
    mehr verfügbar ist, wird die Karte gelöscht.
    """
    c = _gcfg(guild_id)
    ch_id = int(c.get("active_channel_id", 0) or 0)
    guild = client.get_guild(int(guild_id))
    if guild and ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return ch
    return None


def _parse_channel_id(raw: str) -> int:
    text = str(raw or "").strip()
    # erlaubt: 123456789, <#123456789>, discord://... grob rausfiltern
    digits = "".join(ch for ch in text if ch.isdigit())
    try:
        return int(digits)
    except Exception:
        return 0


async def _resolve_text_channel_by_id(client: discord.Client, guild: discord.Guild, raw_channel_id: str):
    """Löst eine Kanal-ID robust auf, auch wenn Discord den Kanal im Slash-Dropdown nicht vorschlägt."""
    channel_id = _parse_channel_id(raw_channel_id)
    if not channel_id:
        return None, "Ungültige Kanal-ID. Rechtsklick auf Kanal → ID kopieren."

    ch = guild.get_channel(channel_id) or client.get_channel(channel_id)
    if ch is None:
        try:
            ch = await client.fetch_channel(channel_id)
        except Exception:
            ch = None

    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return None, "Die ID gehört nicht zu einem normalen Textkanal oder Thread. Forum/Kategorie/Voice geht dafür nicht direkt."

    me = guild.me
    if me is not None:
        try:
            perms = ch.permissions_for(me)
            missing: list[str] = []
            if not getattr(perms, "view_channel", False):
                missing.append("Kanal anzeigen")
            if isinstance(ch, discord.Thread):
                if not getattr(perms, "send_messages_in_threads", False):
                    missing.append("Nachrichten in Threads senden")
            elif not getattr(perms, "send_messages", False):
                missing.append("Nachrichten senden")
            if not getattr(perms, "embed_links", False):
                missing.append("Embed-Links")
            if not getattr(perms, "read_message_history", False):
                missing.append("Nachrichtenverlauf lesen")
            if missing:
                return None, "Bot hat dort nicht genug Rechte: " + ", ".join(missing)
        except Exception:
            pass

    return ch, ""


async def _set_auction_cfg_channel_by_id(
    inter: discord.Interaction,
    cfg_key: str,
    channel_id: str,
    success_label: str,
    *,
    sync_active: bool = False,
) -> None:
    if inter.guild is None or not _is_leader_or_admin(inter):
        await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
        return
    ch, err = await _resolve_text_channel_by_id(inter.client, inter.guild, channel_id)
    if ch is None:
        await inter.response.send_message(f"❌ {err}", ephemeral=True)
        return
    _gcfg(inter.guild.id)[cfg_key] = int(ch.id)
    save_cfg()
    if sync_active:
        await inter.response.defer(ephemeral=True, thinking=True)
        await _sync_active_auction_messages(inter.client, inter.guild.id)
        await inter.followup.send(f"✅ {success_label} gesetzt: {ch.mention}\nAktive Items wurden synchronisiert.", ephemeral=True)
    else:
        await inter.response.send_message(f"✅ {success_label} gesetzt: {ch.mention}", ephemeral=True)


def _status_label(status: str) -> str:
    return {
        "active": "🟢 Aktiv",
        "closed": "🔒 Beendet",
        "delivered": "✅ Übergeben / abgebucht",
        "cancelled": "❌ Abgebrochen",
        "expired": "⌛ Abgelaufen",
    }.get(str(status or ""), str(status or "?"))


def _auction_embed(guild: discord.Guild, auction: dict, compact: bool = False) -> discord.Embed:
    status = str(auction.get("status", "active") or "active")
    item_name = str(auction.get("item_name", "Item") or "Item")
    auction_id = str(auction.get("id", "") or "")
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    top = _highest_bid(auction)
    current = int(top.get("amount", 0) or 0) if top else 0
    min_next = _min_next_bid(auction)
    color = discord.Color.gold() if status == "active" else discord.Color.dark_gold()
    kind = str(auction.get("kind", "auction") or "auction")
    phase = _auction_phase(auction)
    mode = str(auction.get("eligibility_mode", "all") or "all")
    if phase == "sale":
        title_prefix = "🛒 Sale-Kauf"
    elif phase == "need":
        title_prefix = "🎯 Main-Need-Auktion" if mode == "main_need" else "🔁 Second-Need-Auktion"
    else:
        title_prefix = "⚖️ Freie Auktion"
    emb = discord.Embed(title=f"{title_prefix}: {item_name}", color=color, timestamp=_now())
    # Nutzeransicht bewusst schlank halten:
    # Status, Auktions-ID und Berechtigung sind intern/logisch relevant,
    # aber in der Gildenzentrale und in der Auktionskarte unnötig unübersichtlich.
    desc = []
    if end_dt:
        desc.append(f"Ende: **{end_dt.strftime('%d.%m.%Y %H:%M')}**")
    desc.append(f"Startgebot: **{int(auction.get('start_bid', DEFAULT_START_BID) or DEFAULT_START_BID)} EC**")
    desc.append(f"Mindestschritt: **{int(auction.get('min_increment', DEFAULT_MIN_INCREMENT) or DEFAULT_MIN_INCREMENT)} EC**")
    if top:
        desc.append(f"Höchstgebot: **{current} EC** von <@{int(top.get('user_id', 0) or 0)}>")
        desc.append(f"Nächstes Mindestgebot: **{min_next} EC**")
    else:
        desc.append("Höchstgebot: **noch keines**")
        desc.append(f"Nächstes Mindestgebot: **{min_next} EC**")
    emb.description = "\n".join(desc)

    if not compact:
        bids = auction.get("bids") if isinstance(auction.get("bids"), list) else []
        if bids:
            # Übersicht: nur die 3 letzten Gebote anzeigen.
            last = sorted(bids, key=lambda b: str(b.get("created_at", "")), reverse=True)[:3]
            lines = [f"• <@{int(b.get('user_id',0) or 0)}> – **{int(b.get('amount',0) or 0)} EC**" for b in last]
            emb.add_field(name="Letzte Gebote", value="\n".join(lines), inline=False)
    emb.set_footer(text="Gebote prüfen beim Bieten deinen aktuellen EC-Kontostand. Abgebucht wird erst bei Übergabe-Bestätigung.")
    return emb


async def _refresh_auction_message(client: discord.Client, guild_id: int, auction: dict) -> None:
    ch_id = int(auction.get("channel_id", 0) or 0)
    msg_id = int(auction.get("message_id", 0) or 0)
    if not ch_id or not msg_id:
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    ch = guild.get_channel(ch_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    try:
        msg = await ch.fetch_message(msg_id)
        phase = _auction_phase(auction)
        view = (SaleBuyView(int(guild_id), str(auction.get("id", ""))) if phase == "sale" else AuctionBidView(int(guild_id), str(auction.get("id", "")))) if auction.get("status") == "active" else None
        embed = _sale_embed(guild, auction) if phase == "sale" else _auction_embed(guild, auction)
        await msg.edit(embed=embed, view=view)
    except Exception as e:
        print(f"[loot_auction] refresh failed: {e!r}")


def _active_auction_label(auction: dict) -> str:
    phase = _auction_phase(auction)
    mode = str(auction.get("eligibility_mode", "all") or "all")
    if phase == "sale":
        return "🛒 Sale"
    if phase == "free":
        return "⚖️ Freie Auktion"
    if mode == "main_need":
        return "🎯 Main-Need-Auktion"
    if mode == "secondary_need":
        return "🔁 Second-Need-Auktion"
    return "🏷️ Need-Auktion"


def _active_auction_info_embed(guild: discord.Guild, auction: dict) -> discord.Embed:
    """Kurze Übersichtskarte für den Kanal der aktuellen Auktionen."""
    phase = _auction_phase(auction)
    item = str(auction.get("item_name", "Item") or "Item")
    auction_id = str(auction.get("id", "") or "")
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    label = _active_auction_label(auction)
    lines = [
        f"**Item:** {item}",
        f"**Bereich:** {label}",
        f"**Auktions-ID:** `{auction_id}`",
    ]
    if phase == "sale":
        price = int(auction.get("fixed_price", auction.get("start_bid", 0)) or 0)
        lines.append(f"**Preis:** {'Gratis' if price <= 0 else f'{price} EC'}")
        if _is_junk_interest_sale(auction):
            lines.append(_junk_sale_line(auction))
            lines.append("Aktion über **Gildenzentrale → Auktion → Sale-Kauf**.")
        else:
            lines.append("Kaufen über **Gildenzentrale → Auktion → Sale-Kauf**.")
    elif phase == "free":
        lines.append("Bieten über **Gildenzentrale → Auktion → Freie Auktion**.")
    else:
        mode = str(auction.get("eligibility_mode", "all") or "all")
        if mode == "main_need":
            lines.append("Bieten dürfen aktuell nur offene **Main-Need-Spieler**.")
        elif mode == "secondary_need":
            lines.append("Bieten dürfen aktuell nur offene **Second-Need-Spieler**.")
        else:
            lines.append("Bieten über **Gildenzentrale → Auktion → Need-Auktion**.")
    if end_dt:
        lines.append(f"**Läuft bis:** {end_dt.strftime('%d.%m.%Y %H:%M')}")
    lines.append("\nDetails und Gebote stehen im DKP-/Auktionskanal.")

    color = discord.Color.green() if phase == "sale" else discord.Color.gold()
    emb = discord.Embed(title=f"{label}: {item}", description="\n".join(lines), color=color, timestamp=_now())
    emb.set_footer(text="Diese Übersicht wird automatisch gelöscht, sobald das Item nicht mehr verfügbar ist.")
    return emb


async def _delete_active_auction_message(client: discord.Client, auction: dict) -> None:
    cid = int(auction.get("active_channel_id", 0) or 0)
    mid = int(auction.get("active_message_id", 0) or 0)
    if not cid or not mid:
        return
    try:
        ch = client.get_channel(cid)
        if ch is None:
            ch = await client.fetch_channel(cid)
        if isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            msg = await ch.fetch_message(mid)
            await msg.delete()
    except Exception as e:
        print(f"[loot_auction] active auction message delete failed: {e!r}")
    auction["active_channel_id"] = 0
    auction["active_message_id"] = 0
    auction["active_message_deleted_at"] = _now_iso()


async def _post_or_refresh_active_auction_message(client: discord.Client, guild_id: int, auction: dict) -> None:
    """Postet/aktualisiert eine kurze Karte im separaten Aktuelle-Auktionen-Kanal."""
    if str(auction.get("status", "")) != "active":
        await _delete_active_auction_message(client, auction)
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    ch = _active_auction_channel(client, guild_id)
    if not ch:
        return

    embed = _active_auction_info_embed(guild, auction)
    content = "📌 **Aktuell verfügbar**"
    old_cid = int(auction.get("active_channel_id", 0) or 0)
    old_mid = int(auction.get("active_message_id", 0) or 0)
    current_cid = int(getattr(ch, "id", 0) or 0)

    if old_mid and old_cid == current_cid:
        try:
            msg = await ch.fetch_message(old_mid)
            await msg.edit(content=content, embed=embed, view=None)
            return
        except Exception as e:
            print(f"[loot_auction] active auction message refresh failed: {e!r}")

    if old_mid:
        await _delete_active_auction_message(client, auction)
    try:
        msg = await ch.send(content=content, embed=embed, view=None)
        auction["active_channel_id"] = current_cid
        auction["active_message_id"] = int(msg.id)
        auction["active_message_posted_at"] = _now_iso()
        save_auctions()
    except Exception as e:
        print(f"[loot_auction] active auction message post failed: {e!r}")


async def _sync_active_auction_messages(client: discord.Client, guild_id: int | None = None) -> None:
    """Erstellt/aktualisiert nach Start oder Kanalwechsel die Übersicht aktiver Items."""
    gids = [str(int(guild_id))] if guild_id is not None else list(auction_state.keys())
    for gid_str in gids:
        try:
            gid = int(gid_str)
        except Exception:
            continue
        auctions = (_gauctions(gid).get("auctions") or {})
        for auc in list(auctions.values()):
            try:
                if str(auc.get("status", "")) == "active":
                    await _post_or_refresh_active_auction_message(client, gid, auc)
                else:
                    await _delete_active_auction_message(client, auc)
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"[loot_auction] active auction sync failed {gid}: {e!r}")


async def _sync_active_auction_messages_after_ready(client: discord.Client) -> None:
    try:
        await client.wait_until_ready()
        await asyncio.sleep(3)
        await _sync_active_auction_messages(client)
    except Exception as e:
        print(f"[loot_auction] active auction startup sync failed: {e!r}")


async def _delete_market_message(client: discord.Client, auction: dict) -> None:
    """Löscht die öffentliche Marktplatz-Nachricht, sobald ein Item nicht mehr verfügbar ist."""
    cid = int(auction.get("market_channel_id", 0) or 0)
    mid = int(auction.get("market_message_id", 0) or 0)
    if not cid or not mid:
        return
    try:
        ch = client.get_channel(cid)
        if ch is None:
            ch = await client.fetch_channel(cid)
        if isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            msg = await ch.fetch_message(mid)
            await msg.delete()
    except Exception as e:
        print(f"[loot_auction] market message delete failed: {e!r}")
    auction["market_channel_id"] = 0
    auction["market_message_id"] = 0
    auction["market_message_deleted_at"] = _now_iso()


def _market_embed(guild: discord.Guild, auction: dict, *, final: str = "") -> discord.Embed:
    """Schlanke öffentliche Marktplatzkarte für den Allgemein-Chat."""
    phase = _auction_phase(auction)
    item = str(auction.get("item_name", "Item") or "Item")
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    price = int(auction.get("fixed_price", auction.get("start_bid", 0)) or 0)
    price_text = "Gratis" if price <= 0 else f"{price} EC"

    if final == "sold":
        buyer = int(auction.get("sold_to", 0) or 0)
        if bool(auction.get("junk_drop", False)) and (auction.get("junk_roll_winner_id") or auction.get("junk_lottery_winner_id")):
            winner_roll = int(auction.get("junk_roll_winner_roll", 0) or 0)
            desc = (
                f"**Item:** {item}\n\n"
                f"**Gewinner:**\n🏆 <@{buyer}> mit **{winner_roll}**\n\n"
                "**Preis:** Gratis"
            )
            emb = discord.Embed(title="✅ Müll-Item Roll abgeschlossen", description=desc, color=discord.Color.green(), timestamp=_now())
            return _add_junk_roll_fields(emb, auction)
        return discord.Embed(
            title="✅ Sale-Kauf abgeschlossen",
            description=f"**Item:** {item}\n**Empfänger:** <@{buyer}>\n**Preis:** {price_text}",
            color=discord.Color.green(),
            timestamp=_now(),
        )
    if final == "expired":
        return discord.Embed(
            title="⌛ Sale-Kauf abgelaufen",
            description=f"**Item:** {item}\nDas Item ist nicht mehr verfügbar.",
            color=discord.Color.dark_grey(),
            timestamp=_now(),
        )
    if final == "cancelled":
        return discord.Embed(
            title="❌ Angebot beendet",
            description=f"**Item:** {item}\nDieses Angebot ist nicht mehr verfügbar.",
            color=discord.Color.red(),
            timestamp=_now(),
        )
    if final == "auction_closed":
        top = _highest_bid(auction)
        winner = int(top.get("user_id", 0) or 0) if top else 0
        amount = int(top.get("amount", 0) or 0) if top else 0
        desc = f"**Item:** {item}\nDie freie Auktion ist beendet."
        if winner:
            desc += f"\n**Gewinner:** <@{winner}>\n**Gebot:** {amount} EC"
        return discord.Embed(title="🏁 Freie Auktion beendet", description=desc, color=discord.Color.gold(), timestamp=_now())

    if phase == "sale":
        if _is_junk_interest_sale(auction):
            until = _junk_roll_until(auction)
            if _junk_roll_open(auction) and until:
                desc = (
                    f"**Item:** {item}\n"
                    f"**Ende:** {until.strftime('%d.%m.%Y %H:%M')}\n\n"
                    "**Status:** Würfelphase\n\n"
                    f"**Aktuell vorne:**\n{_junk_roll_summary(auction)}"
                )
                emb = discord.Embed(title="🧹 Müll-Item im Gratis-Roll", description=desc, color=discord.Color.green(), timestamp=_now())
                return _add_junk_roll_fields(emb, auction)
            desc = (
                f"**Item:** {item}\n\n"
                "**Status:** Gratis-Sofortkauf\n\n"
                "Es hat niemand gewürfelt.\n"
                "Der erste Spieler, der es nimmt, bekommt es direkt."
            )
            return discord.Embed(title="🛒 Müll-Item jetzt im Gratis-Sofortkauf", description=desc, color=discord.Color.green(), timestamp=_now())
        title = "🛒 Neues Sale-Item verfügbar"
        desc = f"**Item:** {item}\n**Preis:** {price_text}\nKaufen über **Gildenzentrale → Auktion → Sale-Kauf**."
        if end_dt:
            desc += f"\nVerfügbar bis: **{end_dt.strftime('%d.%m.%Y %H:%M')}**"
        return discord.Embed(title=title, description=desc, color=discord.Color.green(), timestamp=_now())

    title = "⚖️ Neues Item in der freien Auktion"
    desc = f"**Item:** {item}\nBieten über **Gildenzentrale → Auktion → Freie Auktion**."
    if end_dt:
        desc += f"\nEnde: **{end_dt.strftime('%d.%m.%Y %H:%M')}**"
    return discord.Embed(title=title, description=desc, color=discord.Color.gold(), timestamp=_now())


async def _edit_market_message_final(client: discord.Client, guild_id: int, auction: dict, *, final: str) -> None:
    """Bearbeitet die eine öffentliche Allgemein-/Marktplatz-Nachricht statt neue Posts zu erzeugen."""
    cid = int(auction.get("market_channel_id", 0) or 0)
    mid = int(auction.get("market_message_id", 0) or 0)
    if not cid or not mid:
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    try:
        ch = client.get_channel(cid)
        if ch is None:
            ch = await client.fetch_channel(cid)
        if isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            msg = await ch.fetch_message(mid)
            sold_text = "✅ **Sale-Item wurde genommen.**"
            if final == "sold" and bool(auction.get("junk_drop", False)) and (auction.get("junk_roll_winner_id") or auction.get("junk_lottery_winner_id")):
                sold_text = "✅ **Müll-Item Roll abgeschlossen.**"
            content = {
                "sold": sold_text,
                "expired": "⌛ **Sale-Item ist abgelaufen.**",
                "cancelled": "❌ **Angebot beendet.**",
                "auction_closed": "🏁 **Freie Auktion beendet.**",
            }.get(final, "ℹ️ **Angebot aktualisiert.**")
            await msg.edit(content=content, embed=_market_embed(guild, auction, final=final), view=None)
            auction["market_message_final_state"] = final
            auction["market_message_final_at"] = _now_iso()
            save_auctions()
    except Exception as e:
        print(f"[loot_auction] market final edit failed: {e!r}")


async def _post_or_refresh_market_message(client: discord.Client, guild_id: int, auction: dict) -> None:
    """Postet/aktualisiert die kurze öffentliche Nachricht für Freie Auktion oder Sale-Kauf.

    Wichtig: Der Allgemein-Chat bekommt bewusst nur eine schlanke Info ohne
    Gebotsdetails und ohne extra Abschluss-Post. Details bleiben im DKP-/Auktionskanal.
    """
    phase = _auction_phase(auction)
    if phase not in {"free", "sale"} or str(auction.get("status", "")) != "active":
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    ch = _market_channel(client, guild_id)
    if not ch:
        return

    embed = _market_embed(guild, auction)
    market_view = SaleBuyView(int(guild_id), str(auction.get("id", ""))) if phase == "sale" and _is_junk_interest_sale(auction) else None
    if phase == "sale" and _is_junk_interest_sale(auction):
        content = "🧹 **Müll-Item verfügbar!**"
    else:
        content = "🛒 **Neues Sale-Item verfügbar!**" if phase == "sale" else "⚖️ **Item ist jetzt in der freien Auktion verfügbar!**"

    old_cid = int(auction.get("market_channel_id", 0) or 0)
    old_mid = int(auction.get("market_message_id", 0) or 0)
    if old_mid and old_cid == int(getattr(ch, "id", 0) or 0):
        try:
            msg = await ch.fetch_message(old_mid)
            await msg.edit(content=content, embed=embed, view=market_view)
            return
        except Exception as e:
            print(f"[loot_auction] market message refresh failed: {e!r}")

    # Falls noch eine alte Marktplatz-Nachricht aus einem anderen Kanal existiert, erst löschen.
    if old_mid:
        await _delete_market_message(client, auction)
    try:
        msg = await ch.send(content=content, embed=embed, view=market_view)
        auction["market_channel_id"] = int(getattr(ch, "id", 0) or 0)
        auction["market_message_id"] = int(msg.id)
        auction["market_message_posted_at"] = _now_iso()
        auction.pop("market_message_final_state", None)
        auction.pop("market_message_final_at", None)
        save_auctions()
    except Exception as e:
        print(f"[loot_auction] market message post failed: {e!r}")


async def start_junk_sale_drop(
    inter: discord.Interaction,
    guild_id: int,
    item_title: str,
    actor_id: int | None = None,
    duration_hours: int = SALE_HOURS,
) -> dict:
    """Startet ein kostenloses Sale-Item für Müll-/Restdrops aus dem Adminbereich der Gildenzentrale.

    Das Item muss nicht im Loot-Katalog existieren. Es wird als normales aktives
    Sale-Item gespeichert und erscheint dadurch im Sale-Kauf der Gildenzentrale.
    """
    client = inter.client
    guild = client.get_guild(int(guild_id)) or inter.guild
    if guild is None:
        return {"ok": False, "error": "Server konnte nicht zugeordnet werden."}

    title = _safe_text(str(item_title or "").strip())[:120]
    if not title:
        return {"ok": False, "error": "Kein Item-Titel angegeben."}

    # Müll-/Restitems laufen anders als normale Sales:
    # 0-24 Stunden live würfeln, danach gewinnt der höchste eindeutige Wurf.
    # Wenn bis dahin niemand würfelt, bleibt das Item ohne Ablauf als Gratis-Sofortkauf offen.
    aid = _new_auction_id()
    interest_until = _now() + timedelta(hours=JUNK_INTEREST_HOURS)
    auc = {
        "id": aid,
        "guild_id": int(guild.id),
        "kind": "sale",
        "phase": "sale",
        "item_id": f"junk:{aid}",
        "item_name": title,
        "created_at": _now_iso(),
        "created_by": int(actor_id or getattr(inter.user, "id", 0) or 0),
        "ends_at": "",
        "junk_roll_until": interest_until.isoformat(),
        "junk_rolls": {},
        "junk_roll_max": JUNK_ROLL_MAX_DEFAULT,
        # Legacy-Felder bleiben bewusst drin, damit alte Helfer/alte Daten nicht brechen.
        "junk_interest_until": interest_until.isoformat(),
        "junk_interest_user_ids": [],
        "junk_interest_requests": {},
        "fixed_price": 0,
        "start_bid": 0,
        "min_increment": 0,
        "eligibility_mode": "all",
        "eligible_user_ids": [],
        "bids": [],
        "status": "active",
        "message_id": 0,
        "channel_id": 0,
        "source": "junk_drop_menu",
        "junk_drop": True,
        "created_note": "Kostenloses Müll-/Restitem",
    }
    _gauctions(guild.id).setdefault("auctions", {})[aid] = auc
    save_auctions()

    auction_channel_id = 0
    try:
        ch = _auction_channel(client, guild.id, None)
        if ch:
            msg = await ch.send(content="🧹 **Müll-Item kostenlos im Sale**", embed=_sale_embed(guild, auc), view=SaleBuyView(guild.id, aid))
            auc["message_id"] = int(msg.id)
            auc["channel_id"] = int(getattr(ch, "id", 0) or 0)
            auction_channel_id = int(getattr(ch, "id", 0) or 0)
            save_auctions()
    except Exception as e:
        print(f"[loot_auction] junk sale auction-channel post failed: {e!r}")

    try:
        await _post_or_refresh_market_message(client, guild.id, auc)
    except Exception as e:
        print(f"[loot_auction] junk sale market post failed: {e!r}")

    try:
        await _post_or_refresh_active_auction_message(client, guild.id, auc)
    except Exception as e:
        print(f"[loot_auction] junk sale active-channel post failed: {e!r}")

    save_auctions()
    return {
        "ok": True,
        "auction_id": aid,
        "auction": auc,
        "auction_channel_id": auction_channel_id,
        "market_channel_id": int(auc.get("market_channel_id", 0) or 0),
        "market_message_id": int(auc.get("market_message_id", 0) or 0),
    }


async def _place_bid(inter: discord.Interaction, guild_id: int, auction_id: str, amount: int, portal_user_id: int | None = None) -> None:
    guild = inter.guild or inter.client.get_guild(int(guild_id))
    if guild is None:
        await inter.response.send_message("❌ Server konnte nicht zugeordnet werden.", ephemeral=True)
        return
    auction = _auction(guild_id, auction_id)
    if not auction:
        await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
        return
    if str(auction.get("status", "")) != "active":
        await inter.response.send_message("❌ Diese Auktion ist nicht mehr aktiv.", ephemeral=True)
        return
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    if end_dt and _now() >= end_dt:
        await inter.response.send_message("❌ Diese Auktion ist bereits abgelaufen. Die Abschlussrunde verarbeitet sie gleich.", ephemeral=True)
        return
    user_id = int(inter.user.id)
    if not _is_ebolus_member(guild, user_id):
        await inter.response.send_message("❌ Nur Ebolus-Mitglieder dürfen mit EC bieten.", ephemeral=True)
        return
    if not await _require_loot_unlocked(inter, guild, user_id):
        return
    mode = str(auction.get("eligibility_mode", "all") or "all")
    eligible = [int(x) for x in auction.get("eligible_user_ids", []) or []]
    if mode in {"main_need", "secondary_need"} and user_id not in eligible:
        label = "Main-Need-Spieler" if mode == "main_need" else "Second-Need-Spieler"
        await inter.response.send_message(f"❌ Diese Auktion ist aktuell nur für berechtigte {label} freigegeben.", ephemeral=True)
        return
    min_bid = _min_next_bid(auction)
    if int(amount) < min_bid:
        await inter.response.send_message(f"❌ Mindestgebot ist aktuell **{min_bid} EC**.", ephemeral=True)
        return
    balance = _ec_balance(guild_id, user_id)
    if int(amount) > balance:
        await inter.response.send_message(f"❌ Du hast aktuell nur **{balance} EC**.", ephemeral=True)
        return

    bids = auction.setdefault("bids", [])
    bids.append({"user_id": user_id, "amount": int(amount), "created_at": _now_iso(), "name": getattr(inter.user, "display_name", str(user_id))})
    auction["updated_at"] = _now_iso()
    save_auctions()

    await _refresh_auction_message(inter.client, guild_id, auction)
    await _post_or_refresh_market_message(inter.client, guild_id, auction)
    # Die ursprünglichen Need-DMs werden im Hintergrund aktualisiert, damit die Button-Reaktion nicht timeoutet.
    # Falls diese Auktion noch aus einer älteren Version stammt und keine DM-Message-IDs gespeichert hat,
    # wird einmalig eine neue Live-Tracking-DM gesendet und ab dann aktualisiert/bei Übergabe gelöscht.
    async def _repair_and_refresh_tracking_dm():
        await _ensure_auction_tracking_dms(inter.client, guild_id, auction)
        await _refresh_auction_tracking_dms(inter.client, guild_id, auction)
    asyncio.create_task(_repair_and_refresh_tracking_dm())

    # Wenn das Gebot aus der Gildenzentrale/DM kommt, muss auch diese aktuelle DM-Nachricht
    # aktualisiert werden. Die normale Refresh-Funktion aktualisiert nur die öffentliche
    # Auktionsnachricht im Auktionskanal.
    if portal_user_id is not None:
        try:
            if inter.message:
                await inter.message.edit(embed=_auction_embed(guild, auction), view=PortalAuctionBidView(int(guild_id), int(portal_user_id), str(auction_id)))
        except Exception as e:
            print(f"[loot_auction] portal bid message refresh failed: {e!r}")

    await inter.response.send_message(f"✅ Gebot gesetzt: **{int(amount)} EC** für **{auction.get('item_name','Item')}**.", ephemeral=True)


class CustomBidModal(Modal, title="Eigenes EC-Gebot"):
    amount = TextInput(label="Gebot in EC", placeholder="z. B. 50", required=True, max_length=8)

    def __init__(self, guild_id: int, auction_id: str, portal_user_id: int | None = None):
        super().__init__(timeout=180)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)
        self.portal_user_id = int(portal_user_id) if portal_user_id is not None else None

    async def on_submit(self, inter: discord.Interaction):
        try:
            val = int(str(self.amount.value).strip())
        except Exception:
            await inter.response.send_message("❌ Bitte gib eine ganze Zahl ein.", ephemeral=True)
            return
        await _place_bid(inter, self.guild_id, self.auction_id, val, self.portal_user_id)


class AuctionBidView(View):
    def __init__(self, guild_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"lootauc:{auction_id}:{child.custom_id or child.label}"

    async def _quick(self, inter: discord.Interaction, add: int) -> None:
        auction = _auction(self.guild_id, self.auction_id)
        if not auction:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        amount = max(_min_next_bid(auction), _current_price(auction) + int(add))
        await _place_bid(inter, self.guild_id, self.auction_id, amount)

    @button(label="+5 EC", style=ButtonStyle.primary, custom_id="bid5")
    async def bid5(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 5)

    @button(label="+10 EC", style=ButtonStyle.primary, custom_id="bid10")
    async def bid10(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 10)

    @button(label="+25 EC", style=ButtonStyle.primary, custom_id="bid25")
    async def bid25(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 25)

    @button(label="Eigenes Gebot", style=ButtonStyle.secondary, custom_id="custom")
    async def custom(self, inter: discord.Interaction, btn: discord.ui.Button):
        await inter.response.send_modal(CustomBidModal(self.guild_id, self.auction_id))

    @button(label="Mein EC", style=ButtonStyle.secondary, custom_id="balance")
    async def balance(self, inter: discord.Interaction, btn: discord.ui.Button):
        bal = _ec_balance(self.guild_id, int(inter.user.id))
        await inter.response.send_message(f"🪙 Dein aktueller Kontostand: **{bal} EC**", ephemeral=True)



class PortalAuctionBidView(View):
    """Bietansicht in der privaten Gildenzentrale mit Zurück-Buttons."""
    def __init__(self, guild_id: int, user_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.auction_id = str(auction_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"portalauc:{self.guild_id}:{self.user_id}:{auction_id}:{child.custom_id or child.label}"

    async def _quick(self, inter: discord.Interaction, add: int) -> None:
        auction = _auction(self.guild_id, self.auction_id)
        if not auction:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        amount = max(_min_next_bid(auction), _current_price(auction) + int(add))
        await _place_bid(inter, self.guild_id, self.auction_id, amount, self.user_id)

    @button(label="+5 EC", style=ButtonStyle.primary, custom_id="bid5")
    async def bid5(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 5)

    @button(label="+10 EC", style=ButtonStyle.primary, custom_id="bid10")
    async def bid10(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 10)

    @button(label="+25 EC", style=ButtonStyle.primary, custom_id="bid25")
    async def bid25(self, inter: discord.Interaction, btn: discord.ui.Button):
        await self._quick(inter, 25)

    @button(label="Eigenes Gebot", style=ButtonStyle.secondary, custom_id="custom")
    async def custom(self, inter: discord.Interaction, btn: discord.ui.Button):
        await inter.response.send_modal(CustomBidModal(self.guild_id, self.auction_id, self.user_id))

    @button(label="Mein EC", style=ButtonStyle.secondary, custom_id="balance")
    async def balance(self, inter: discord.Interaction, btn: discord.ui.Button):
        bal = _ec_balance(self.guild_id, int(inter.user.id))
        await inter.response.send_message(f"🪙 Dein aktueller Kontostand: **{bal} EC**", ephemeral=True)

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary, custom_id="back_auction")
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))

    @button(label="Gildenzentrale", style=ButtonStyle.secondary, custom_id="back_main")
    async def back_main(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _portal_back_to_main(inter, self.guild_id, self.user_id)


class AuctionDeliveryView(View):
    def __init__(self, guild_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"lootdel:{auction_id}:{child.custom_id or child.label}"

    async def _guard(self, inter: discord.Interaction) -> bool:
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins dürfen das bestätigen.", ephemeral=True)
            return False
        return True

    @button(label="Übergabe erledigt / EC abbuchen", style=ButtonStyle.success, custom_id="confirm")
    async def confirm(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        auction = _auction(self.guild_id, self.auction_id)
        if not auction:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        if str(auction.get("status", "")) == "delivered":
            await inter.response.send_message("ℹ️ Diese Auktion wurde bereits abgerechnet.", ephemeral=True)
            return
        top = _highest_bid(auction)
        if not top:
            await inter.response.send_message("❌ Kein Gewinner vorhanden.", ephemeral=True)
            return
        winner = int(top.get("user_id", 0) or 0)
        amount = int(top.get("amount", 0) or 0)
        bal = _ec_balance(self.guild_id, winner)
        if bal < amount:
            await inter.response.send_message(f"❌ Gewinner hat aktuell nur **{bal} EC**, benötigt aber **{amount} EC**. Bitte manuell klären.", ephemeral=True)
            return
        ok = _add_ec_transaction(
            self.guild_id,
            winner,
            -amount,
            f"Loot-Auktion gewonnen: {auction.get('item_name','Item')}",
            int(inter.user.id),
            self.auction_id,
            meta={"auction_id": self.auction_id, "item_id": auction.get("item_id", ""), "item_name": auction.get("item_name", "")},
        )
        if not ok:
            await inter.response.send_message("❌ DKP/EC-System konnte nicht geladen werden. Keine EC abgebucht.", ephemeral=True)
            return
        auction["status"] = "delivered"
        auction["delivered_at"] = _now_iso()
        auction["delivered_by"] = int(inter.user.id)
        auction["charged_amount"] = amount
        _mark_chest_item_status(self.guild_id, auction, "delivered", {"delivered_to": winner, "delivered_by": int(inter.user.id), "delivered_at": auction["delivered_at"]})
        locked_need = _mark_need_received_for_winner(
            self.guild_id,
            winner,
            str(auction.get("item_id", "") or ""),
            eligibility_mode=str(auction.get("eligibility_mode", "all") or "all"),
            auction_id=self.auction_id,
        )
        if locked_need:
            auction["locked_need_slot"] = locked_need
        await _delete_auction_tracking_dms(inter.client, auction)
        if _auction_phase(auction) == "free":
            await _edit_market_message_final(inter.client, self.guild_id, auction, final="auction_closed")
        save_auctions()

        emb = discord.Embed(
            title="✅ Loot übergeben und EC abgebucht",
            description=(
                f"**Item:** {auction.get('item_name','Item')}\n"
                f"**Gewinner:** <@{winner}>\n"
                f"**Abgebucht:** **{amount} EC**\n"
                f"**Auktion:** `{self.auction_id}`"
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        ch = _auction_channel(inter.client, self.guild_id, None) or _log_channel(inter.client, self.guild_id)
        if ch:
            try:
                await ch.send(embed=emb)
            except Exception:
                pass
        try:
            await inter.message.edit(view=None)
        except Exception:
            pass
        await inter.response.send_message(embed=emb, ephemeral=True)

    @button(label="Keine Übergabe / offen lassen", style=ButtonStyle.secondary, custom_id="skip")
    async def skip(self, inter: discord.Interaction, btn: discord.ui.Button):
        if not await self._guard(inter):
            return
        await inter.response.send_message("ℹ️ Okay, die Auktion bleibt ohne EC-Abbuchung offen. Du kannst später erneut bestätigen.", ephemeral=True)


async def _announce_log(client: discord.Client, guild_id: int, title: str, description: str, color: discord.Color = discord.Color.gold()) -> None:
    # Detail-/Verwaltungsinfos gehören in den DKP-/Auktionskanal.
    # Der öffentliche Marktplatz/Allgemein-Chat bekommt nur _post_or_refresh_market_message().
    ch = _auction_channel(client, guild_id, None) or _log_channel(client, guild_id)
    if not ch:
        return
    try:
        await ch.send(embed=discord.Embed(title=title, description=description, color=color, timestamp=_now()))
    except Exception:
        pass


async def _dm_user(guild: discord.Guild, user_id: int, text: str) -> bool:
    member = guild.get_member(int(user_id))
    if not member or member.bot:
        return False
    try:
        await member.send(text)
        return True
    except Exception:
        return False


def _auction_dm_content(auction: dict, *, ended: bool = False, winner_id: int = 0) -> str:
    item_name = str(auction.get("item_name", "Item") or "Item")
    phase = _auction_phase(auction)
    mode = str(auction.get("eligibility_mode", "all") or "all")
    if phase == "need":
        phase_name = "Main-Need-Auktion" if mode == "main_need" else "Second-Need-Auktion"
    else:
        phase_name = "Freie Auktion" if phase == "free" else "Sale-Kauf"
    if ended:
        if winner_id:
            return (
                "🏁 **Auktion beendet**\n\n"
                f"**Item:** {item_name}\n"
                f"**Auktion:** {phase_name}\n"
                f"**Gewinner:** <@{winner_id}>\n"
            )
        return (
            "🏁 **Auktion beendet**\n\n"
            f"**Item:** {item_name}\n"
            f"**Auktion:** {phase_name}\n"
        )
    return (
        f"🎁 **Dein {phase_name}-Item ist gedroppt!**\n\n"
        "Diese Nachricht wird bei jedem Gebot aktualisiert, damit du den Stand direkt hier sehen kannst.\n"
        "Bieten kannst du über **Gildenzentrale → Auktion → Need-Auktion**."
    )


async def _send_auction_tracking_dm(guild: discord.Guild, user_id: int, auction: dict) -> Optional[dict]:
    """Send a persistent Need-DM that can be edited after every bid."""
    member = guild.get_member(int(user_id))
    if not member or member.bot:
        return None
    try:
        msg = await member.send(content=_auction_dm_content(auction), embed=_auction_embed(guild, auction))
        return {
            "user_id": int(user_id),
            "channel_id": int(getattr(msg.channel, "id", 0) or 0),
            "message_id": int(msg.id),
        }
    except Exception:
        return None


async def _refresh_auction_tracking_dms(client: discord.Client, guild_id: int, auction: dict, *, ended: bool = False, winner_id: int = 0) -> None:
    """Update the original Need-DM messages so users can follow the auction without opening the menu."""
    refs = auction.get("notify_message_refs")
    if not isinstance(refs, list) or not refs:
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    content = _auction_dm_content(auction, ended=ended, winner_id=int(winner_id or 0))
    embed = _auction_embed(guild, auction)
    for ref in list(refs):
        try:
            uid = int(ref.get("user_id", 0) or 0)
            cid = int(ref.get("channel_id", 0) or 0)
            mid = int(ref.get("message_id", 0) or 0)
            if not uid or not mid:
                continue
            ch = client.get_channel(cid) if cid else None
            if ch is None:
                user = client.get_user(uid) or await client.fetch_user(uid)
                ch = await user.create_dm()
            msg = await ch.fetch_message(mid)
            await msg.edit(content=content, embed=embed, view=None)
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"[loot_auction] tracking dm refresh failed: {e!r}")




async def _ensure_auction_tracking_dms(client: discord.Client, guild_id: int, auction: dict) -> None:
    """Ensure active Need auctions have stored DM message refs.

    Older auctions created before the live-DM feature do not have refs, so their old
    DM cannot be edited/deleted. For those, send a new live tracking DM once and
    store its message id for all future updates/deletion.
    """
    if _auction_phase(auction) != "need" or str(auction.get("status", "")) != "active":
        return
    refs = auction.get("notify_message_refs")
    if isinstance(refs, list) and refs:
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    eligible = [int(x) for x in auction.get("eligible_user_ids", []) or []]
    if not eligible:
        return
    new_refs: list[dict] = []
    for uid in eligible:
        ref = await _send_auction_tracking_dm(guild, uid, auction)
        if ref:
            new_refs.append(ref)
        await asyncio.sleep(0.08)
    if new_refs:
        auction["notify_message_refs"] = new_refs
        auction["tracking_dm_repaired_at"] = _now_iso()
        save_auctions()


async def _delete_auction_tracking_dms(client: discord.Client, auction: dict) -> None:
    """Delete the persistent Need-Tracking-DMs after the item is actually distributed."""
    refs = auction.get("notify_message_refs")
    if not isinstance(refs, list) or not refs:
        return
    for ref in list(refs):
        try:
            uid = int(ref.get("user_id", 0) or 0)
            cid = int(ref.get("channel_id", 0) or 0)
            mid = int(ref.get("message_id", 0) or 0)
            if not uid or not mid:
                continue
            ch = client.get_channel(cid) if cid else None
            if ch is None:
                user = client.get_user(uid) or await client.fetch_user(uid)
                ch = await user.create_dm()
            msg = await ch.fetch_message(mid)
            await msg.delete()
            await asyncio.sleep(0.05)
        except Exception as e:
            # The member may have deleted the DM manually or blocked DMs. That should not break delivery.
            print(f"[loot_auction] tracking dm delete failed: {e!r}")
    auction["notify_message_refs"] = []
    auction["tracking_dms_deleted_at"] = _now_iso()


def _mark_chest_item_status(guild_id: int, auction: dict, status: str, extra: Optional[dict] = None) -> None:
    chest_id = str(auction.get("chest_item_id", "") or "")
    if not chest_id:
        return
    items = _gchest(guild_id).setdefault("items", {})
    item = items.get(chest_id)
    if isinstance(item, dict):
        item["status"] = status
        item["updated_at"] = _now_iso()
        if extra:
            item.update(extra)
        save_chest()


def _mark_need_received_for_winner(
    guild_id: int,
    user_id: int,
    item_id: str,
    eligibility_mode: str = "all",
    auction_id: str = "",
) -> Optional[dict]:
    """Markiert genau EINEN passenden Need-Slot als erhalten und sperrt ihn.

    Main-Need-Auktionen sperren zuerst einen Main-Slot, Second-Need-Auktionen
    zuerst einen Secondary-Slot. Bei freien Auktionen/Sale wird Main vor
    Secondary geprüft. Doppelte Einträge desselben Items werden nicht alle
    gleichzeitig verbraucht.
    """
    if not item_id:
        return None

    data = _load_json(LOOT_NEEDS_FILE, {})
    g = data.get(str(int(guild_id))) or {}
    users = g.get("users") if isinstance(g.get("users"), dict) else {}
    u = users.get(str(int(user_id)))
    if not isinstance(u, dict):
        return None

    mode = str(eligibility_mode or "all")
    if mode == "main_need":
        tab_order = ("Main",)
    elif mode == "secondary_need":
        tab_order = ("Secondary",)
    else:
        tab_order = ("Main", "Secondary")

    for tab in tab_order:
        bucket = u.get(tab) if isinstance(u.get(tab), dict) else {}
        for slot, val in list(bucket.items()):
            obj = _slot_obj(val)
            if str(obj.get("item_id", "") or "") != str(item_id):
                continue
            if bool(obj.get("received", False)) or bool(obj.get("locked", False)):
                continue

            obj["received"] = True
            obj["locked"] = True
            obj["received_at"] = _now_iso()
            obj["received_by"] = int(user_id)
            obj["received_source"] = "loot_auction"
            obj["received_auction_id"] = str(auction_id or "")
            bucket[slot] = obj
            u[tab] = bucket
            users[str(int(user_id))] = u
            g["users"] = users
            data[str(int(guild_id))] = g
            _save_json(LOOT_NEEDS_FILE, data)
            return {"tab": tab, "slot": slot, "item_id": str(item_id)}

    return None


async def _transition_to_free_auction(client: discord.Client, guild_id: int, auction_id: str, auction: dict) -> bool:
    guild = client.get_guild(int(guild_id))
    if not guild:
        return False
    auction.update({
        "kind": "auction",
        "phase": "free",
        "eligibility_mode": "all",
        "eligible_user_ids": [],
        "start_bid": FREE_START_BID,
        "min_increment": DEFAULT_MIN_INCREMENT,
        "bids": [],
        "status": "active",
        "ends_at": (_now() + timedelta(hours=FREE_AUCTION_HOURS)).isoformat(),
        "transitioned_to_free_at": _now_iso(),
        "updated_at": _now_iso(),
    })
    _mark_chest_item_status(guild_id, auction, "free_auction")
    save_auctions()
    await _refresh_auction_message(client, guild_id, auction)
    await _refresh_auction_tracking_dms(client, guild_id, auction)
    await _post_or_refresh_market_message(client, guild_id, auction)
    await _post_or_refresh_active_auction_message(client, guild_id, auction)
    await _announce_log(
        client, guild_id,
        "⚖️ Item jetzt in freier Auktion",
        f"**Item:** {auction.get('item_name','Item')}\n"
        f"Die Need-Auktion hatte keine Gebote. Das Item ist jetzt für alle Ebolus-Mitglieder in der **Freien Auktion** verfügbar.\n"
        f"Startgebot: **{FREE_START_BID} EC**\nDauer: **24 Stunden**\nAuktions-ID: `{auction_id}`",
        discord.Color.gold(),
    )
    return True


async def _transition_to_sale(client: discord.Client, guild_id: int, auction_id: str, auction: dict) -> bool:
    guild = client.get_guild(int(guild_id))
    if not guild:
        return False
    auction.update({
        "kind": "sale",
        "phase": "sale",
        "eligibility_mode": "all",
        "eligible_user_ids": [],
        "fixed_price": SALE_PRICE,
        "start_bid": SALE_PRICE,
        "min_increment": 0,
        "bids": [],
        "status": "active",
        "ends_at": (_now() + timedelta(hours=SALE_HOURS)).isoformat(),
        "transitioned_to_sale_at": _now_iso(),
        "updated_at": _now_iso(),
    })
    _mark_chest_item_status(guild_id, auction, "sale")
    save_auctions()
    await _refresh_auction_message(client, guild_id, auction)
    await _refresh_auction_tracking_dms(client, guild_id, auction)
    await _post_or_refresh_market_message(client, guild_id, auction)
    await _post_or_refresh_active_auction_message(client, guild_id, auction)
    await _announce_log(
        client, guild_id,
        "🛒 Item jetzt im Sale-Kauf",
        f"**Item:** {auction.get('item_name','Item')}\n"
        f"Die freie Auktion hatte keine Gebote. Das Item ist jetzt im **Sale-Kauf** verfügbar.\n"
        f"Sofortkauf: **{SALE_PRICE} EC**\nAuktions-ID: `{auction_id}`",
        discord.Color.green(),
    )
    return True


async def _close_auction(client: discord.Client, guild_id: int, auction_id: str, reason: str = "time") -> bool:
    guild = client.get_guild(int(guild_id))
    auction = _auction(guild_id, auction_id)
    if not guild or not auction or str(auction.get("status", "")) != "active":
        return False

    phase = _auction_phase(auction)
    top = _highest_bid(auction)

    # No bid: automatic chain Need -> Free -> Sale -> Expired.
    if not top:
        if phase == "need":
            return await _transition_to_free_auction(client, guild_id, auction_id, auction)
        if phase == "free":
            return await _transition_to_sale(client, guild_id, auction_id, auction)
        if phase == "sale":
            auction["status"] = "expired"
            auction["expired_at"] = _now_iso()
            auction["close_reason"] = reason
            _mark_chest_item_status(guild_id, auction, "expired")
            await _edit_market_message_final(client, guild_id, auction, final="expired")
            await _delete_active_auction_message(client, auction)
            save_auctions()
            await _refresh_auction_message(client, guild_id, auction)
            await _announce_log(
                client, guild_id,
                "⌛ Sale-Kauf abgelaufen",
                f"**Item:** {auction.get('item_name','Item')}\nDas Item wurde nicht gekauft und ist jetzt abgelaufen.\nAuktions-ID: `{auction_id}`",
                discord.Color.dark_grey(),
            )
            return True

    # Bid exists: close and ask leaders to deliver.
    auction["status"] = "closed"
    auction["closed_at"] = _now_iso()
    auction["close_reason"] = reason
    _mark_chest_item_status(guild_id, auction, "waiting_delivery")
    save_auctions()
    await _refresh_auction_message(client, guild_id, auction)
    await _delete_active_auction_message(client, auction)
    if phase == "free":
        await _edit_market_message_final(client, guild_id, auction, final="auction_closed")

    winner = int(top.get("user_id", 0) or 0)
    await _refresh_auction_tracking_dms(client, guild_id, auction, ended=True, winner_id=winner)
    amount = int(top.get("amount", 0) or 0)
    try:
        await _dm_user(
            guild,
            winner,
            "🏁 **Du hast eine Loot-Auktion gewonnen!**\n\n"
            f"**Item:** {auction.get('item_name','Item')}\n"
            f"**Gebot:** {amount} EC\n\n"
            "Die Gildenleitung wird dir das Item übergeben. EC werden erst nach bestätigter Übergabe abgebucht."
        )
    except Exception:
        pass

    ch = _auction_channel(client, guild_id, None) or _log_channel(client, guild_id)
    if ch:
        emb = discord.Embed(
            title="🏁 Loot-Auktion beendet",
            description=(
                f"**Item:** {auction.get('item_name','Item')}\n"
                f"**Gewinner:** <@{winner}>\n"
                f"**Gebot:** **{amount} EC**\n"
                f"**Phase:** {_need_mode_label(str(auction.get('eligibility_mode','all'))) if phase == 'need' else 'Freie Auktion'}\n"
                f"**Auktions-ID:** `{auction_id}`\n\n"
                "Bitte Item übergeben und danach bestätigen, damit EC abgebucht werden und das Item aus der virtuellen Gildentruhe entfernt wird."
            ),
            color=discord.Color.gold(),
            timestamp=_now(),
        )
        msg = await ch.send(embed=emb, view=AuctionDeliveryView(guild_id, auction_id))
        auction["delivery_message_id"] = int(msg.id)
        auction["delivery_channel_id"] = int(getattr(ch, "id", 0) or 0)
        save_auctions()
    return True


@tasks.loop(minutes=1)
async def auction_close_loop():
    client = _client_ref
    if not client:
        return
    now = _now()
    for gid_str, g in list(auction_state.items()):
        try:
            gid = int(gid_str)
        except Exception:
            continue
        auctions = g.get("auctions") if isinstance(g.get("auctions"), dict) else {}
        for aid, auc in list(auctions.items()):
            if str(auc.get("status", "")) != "active":
                continue
            if _is_junk_interest_sale(auc):
                until = _junk_interest_until(auc)
                if until and now >= until and not auc.get("junk_interest_processed_at"):
                    try:
                        await _process_junk_interest_window(client, gid, str(aid), auc, reason="time")
                    except Exception as e:
                        print(f"[loot_auction] junk interest process failed {gid}/{aid}: {e!r}")
                    continue
            end_dt = _parse_dt(str(auc.get("ends_at", "") or ""))
            if end_dt and now >= end_dt:
                try:
                    await _close_auction(client, gid, str(aid), reason="time")
                except Exception as e:
                    print(f"[loot_auction] close failed {gid}/{aid}: {e!r}")


@auction_close_loop.before_loop
async def before_auction_close_loop():
    if _client_ref:
        await _client_ref.wait_until_ready()


def _new_auction_id() -> str:
    return datetime.now(TZ).strftime("A%Y%m%d%H%M%S%f")[:-3]


def _active_auctions(guild_id: int) -> list[dict]:
    auctions = (_gauctions(guild_id).get("auctions") or {}).values()
    out = [a for a in auctions if str(a.get("status", "")) == "active"]
    out.sort(key=lambda a: str(a.get("ends_at", "")))
    return out




# ---------------------------------------------------------------------------
# Gildenzentrale / Portal integration
# ---------------------------------------------------------------------------

def _auction_kind(auction: dict) -> str:
    return str(auction.get("kind", "auction") or "auction")


def _auction_phase(auction: dict) -> str:
    phase = str(auction.get("phase", "") or "")
    if phase:
        return phase
    kind = str(auction.get("kind", "") or "")
    if kind == "sale" or str(auction.get("status", "")) == "sale":
        return "sale"
    if str(auction.get("eligibility_mode", "")) in {"main_need", "secondary_need"}:
        return "need"
    return "free"


def _is_junk_interest_sale(auction: dict) -> bool:
    return bool(auction.get("junk_drop", False)) and _auction_phase(auction) == "sale" and int(auction.get("fixed_price", auction.get("start_bid", 0)) or 0) <= 0


def _junk_interest_until(auction: dict) -> Optional[datetime]:
    return _parse_dt(str(auction.get("junk_interest_until", "") or ""))


def _junk_roll_until(auction: dict) -> Optional[datetime]:
    return _parse_dt(str(auction.get("junk_roll_until", auction.get("junk_interest_until", "")) or ""))


def _junk_interest_until(auction: dict) -> Optional[datetime]:
    # Legacy-Name: neue Logik ist Würfelphase.
    return _junk_roll_until(auction)


def _junk_rolls(auction: dict) -> dict[str, dict]:
    raw = auction.get("junk_rolls")
    if not isinstance(raw, dict):
        raw = {}
    cleaned: dict[str, dict] = {}
    used: set[int] = set()
    changed = False
    max_seen = JUNK_ROLL_MAX_DEFAULT

    for uid_str, entry in list(raw.items()):
        try:
            uid = int(uid_str)
        except Exception:
            changed = True
            continue
        if not isinstance(entry, dict):
            entry = {"roll": entry, "created_at": _now_iso()}
            changed = True
        try:
            roll = int(entry.get("roll", 0) or 0)
        except Exception:
            changed = True
            continue
        if roll <= 0:
            changed = True
            continue
        if roll in used:
            # Alte/kaputte Daten mit doppelten Würfen werden nicht angezeigt.
            # Neue Würfe sind durch _next_unique_junk_roll eindeutig.
            changed = True
            continue
        max_seen = max(max_seen, roll)
        used.add(roll)
        cleaned[str(uid)] = {
            "user_id": uid,
            "roll": roll,
            "created_at": str(entry.get("created_at", "") or _now_iso()),
        }

    auction["junk_rolls"] = cleaned
    if max_seen > int(auction.get("junk_roll_max", JUNK_ROLL_MAX_DEFAULT) or JUNK_ROLL_MAX_DEFAULT):
        auction["junk_roll_max"] = min(max_seen, JUNK_ROLL_MAX_EXPANDED)
        changed = True
    return cleaned


def _junk_interest_user_ids(auction: dict) -> list[int]:
    # Legacy-Name: gibt jetzt die Spieler zurück, die bereits gewürfelt haben.
    out: list[int] = []
    for uid_str in _junk_rolls(auction).keys():
        try:
            out.append(int(uid_str))
        except Exception:
            pass
    return out


def _junk_roll_open(auction: dict) -> bool:
    until = _junk_roll_until(auction)
    return bool(until and _now() < until and not auction.get("junk_roll_processed_at") and not auction.get("junk_interest_processed_at"))


def _junk_interest_open(auction: dict) -> bool:
    # Legacy-Name: neue Logik ist Würfelphase.
    return _junk_roll_open(auction)


def _junk_roll_range(auction: dict) -> int:
    rolls = _junk_rolls(auction)
    configured = int(auction.get("junk_roll_max", JUNK_ROLL_MAX_DEFAULT) or JUNK_ROLL_MAX_DEFAULT)
    max_val = max(JUNK_ROLL_MAX_DEFAULT, configured)
    if len(rolls) >= JUNK_ROLL_MAX_DEFAULT:
        max_val = JUNK_ROLL_MAX_EXPANDED
    max_val = min(max_val, JUNK_ROLL_MAX_EXPANDED)
    auction["junk_roll_max"] = max_val
    return max_val


def _next_unique_junk_roll(auction: dict) -> tuple[int, int]:
    rolls = _junk_rolls(auction)
    max_val = _junk_roll_range(auction)
    used = {int(v.get("roll", 0) or 0) for v in rolls.values()}
    available = [n for n in range(1, max_val + 1) if n not in used]
    if not available and max_val < JUNK_ROLL_MAX_EXPANDED:
        max_val = JUNK_ROLL_MAX_EXPANDED
        auction["junk_roll_max"] = max_val
        available = [n for n in range(1, max_val + 1) if n not in used]
    if not available:
        raise RuntimeError("Keine freien Müll-Roll-Werte mehr verfügbar.")
    return int(random.choice(available)), int(max_val)


def _junk_roll_entries(auction: dict) -> list[tuple[int, int, str]]:
    entries: list[tuple[int, int, str]] = []
    for uid_str, entry in _junk_rolls(auction).items():
        try:
            uid = int(uid_str)
            roll = int(entry.get("roll", 0) or 0)
            created = str(entry.get("created_at", "") or "")
        except Exception:
            continue
        entries.append((uid, roll, created))
    entries.sort(key=lambda x: x[2] or "")
    return entries


def _junk_roll_leader(auction: dict) -> Optional[tuple[int, int]]:
    entries = _junk_roll_entries(auction)
    if not entries:
        return None
    uid, roll, _created = max(entries, key=lambda x: x[1])
    return int(uid), int(roll)


def _junk_roll_lines(auction: dict, *, limit: int = 20) -> str:
    entries = _junk_roll_entries(auction)
    if not entries:
        return "Noch niemand hat gewürfelt."
    lines = [f"{idx}. <@{uid}> – 🎲 **{roll}**" for idx, (uid, roll, _created) in enumerate(entries[:limit], start=1)]
    if len(entries) > limit:
        lines.append(f"… +{len(entries) - limit} weitere")
    return "\n".join(lines)


def _junk_roll_field_values(auction: dict, *, limit: int = 20) -> tuple[str, str, int]:
    """Bereitet die Würfe als zwei Discord-Embed-Spalten vor."""
    entries = _junk_roll_entries(auction)
    if not entries:
        return "Noch niemand hat gewürfelt.", "", 0

    shown = entries[:limit]
    lines = [f"{idx}. <@{uid}> – 🎲 **{roll}**" for idx, (uid, roll, _created) in enumerate(shown, start=1)]
    split_at = (len(lines) + 1) // 2
    left = "\n".join(lines[:split_at]) or "—"
    right = "\n".join(lines[split_at:])
    hidden = max(0, len(entries) - limit)
    return left, right, hidden


def _add_junk_roll_fields(emb: discord.Embed, auction: dict, *, limit: int = 20) -> discord.Embed:
    left, right, hidden = _junk_roll_field_values(auction, limit=limit)
    if right:
        emb.add_field(name="Würfe", value=left, inline=True)
        emb.add_field(name="\u200b", value=right, inline=True)
    else:
        emb.add_field(name="Würfe", value=left, inline=False)
    if hidden:
        emb.add_field(name="Weitere Würfe", value=f"… +{hidden} weitere", inline=False)
    return emb


def _junk_roll_summary(auction: dict) -> str:
    leader = _junk_roll_leader(auction)
    if not leader:
        return "—"
    uid, roll = leader
    return f"🏆 <@{uid}> mit **{roll}**"


def _junk_sale_line(auction: dict) -> str:
    if not _is_junk_interest_sale(auction):
        return ""
    until = _junk_roll_until(auction)
    count = len(_junk_roll_entries(auction))
    if _junk_roll_open(auction) and until:
        return (
            f"**Ende:** {until.strftime('%d.%m.%Y %H:%M')}\n"
            "**Status:** Würfelphase\n"
            f"**Würfe:** {count}\n"
            f"**Aktuell vorne:** {_junk_roll_summary(auction)}"
        )
    if auction.get("junk_roll_processed_at") or auction.get("junk_interest_processed_at"):
        if int(auction.get("junk_interest_count", count) or count) > 0:
            winner = int(auction.get("sold_to", 0) or auction.get("junk_roll_winner_id", 0) or auction.get("junk_lottery_winner_id", 0) or 0)
            roll = int(auction.get("junk_roll_winner_roll", 0) or 0)
            return f"Roll abgeschlossen. Gewinner: <@{winner}> mit **{roll}**."
        return "Keine Würfe. Das Item ist jetzt als **Gratis-Sofortkauf** verfügbar."
    return "Würfelphase vorbei. Das Item ist jetzt als **Gratis-Sofortkauf** verfügbar."


async def _finalize_sale_delivery(
    client: discord.Client,
    guild: discord.Guild,
    guild_id: int,
    auction: dict,
    auction_id: str,
    user_id: int,
    price: int,
    *,
    actor_id: int | None = None,
    source: str = "sale",
) -> discord.Embed:
    actor = int(actor_id or user_id)
    auction["status"] = "delivered"
    auction["sold_at"] = _now_iso()
    auction["sold_to"] = int(user_id)
    auction["charged_amount"] = int(price)
    auction["delivery_source"] = str(source)
    _mark_chest_item_status(int(guild_id), auction, "delivered", {"delivered_to": int(user_id), "delivered_by": actor, "delivered_at": auction["sold_at"]})
    locked_need = _mark_need_received_for_winner(
        int(guild_id),
        int(user_id),
        str(auction.get("item_id", "") or ""),
        eligibility_mode=str(auction.get("eligibility_mode", "all") or "all"),
        auction_id=str(auction_id),
    )
    if locked_need:
        auction["locked_need_slot"] = locked_need
    await _delete_auction_tracking_dms(client, auction)
    await _edit_market_message_final(client, int(guild_id), auction, final="sold")
    await _delete_active_auction_message(client, auction)
    save_auctions()
    try:
        await _refresh_auction_message(client, int(guild_id), auction)
    except Exception:
        pass
    return discord.Embed(
        title="✅ Sale-Kauf abgeschlossen",
        description=f"**Item:** {auction.get('item_name','Item')}\n**Käufer:** <@{int(user_id)}>\n**Preis:** {'**Gratis**' if int(price) <= 0 else f'**{int(price)} EC**'}",
        color=discord.Color.green(),
        timestamp=_now(),
    )


async def _process_junk_interest_window(client: discord.Client, guild_id: int, auction_id: str, auction: dict, *, reason: str = "time") -> dict:
    # Legacy-Name: verarbeitet die Müll-Würfelphase.
    if not _is_junk_interest_sale(auction):
        return {"processed": False, "delivered": False, "winner_id": 0, "count": 0}
    if str(auction.get("status", "")) != "active":
        return {"processed": False, "delivered": False, "winner_id": 0, "count": 0}
    if auction.get("junk_roll_processed_at") or auction.get("junk_interest_processed_at"):
        return {
            "processed": True,
            "delivered": str(auction.get("status", "")) == "delivered",
            "winner_id": int(auction.get("sold_to", 0) or auction.get("junk_roll_winner_id", 0) or auction.get("junk_lottery_winner_id", 0) or 0),
            "count": int(auction.get("junk_interest_count", 0) or len(_junk_roll_entries(auction))),
        }
    until = _junk_roll_until(auction)
    if until and _now() < until:
        return {"processed": False, "delivered": False, "winner_id": 0, "count": len(_junk_roll_entries(auction))}

    guild = client.get_guild(int(guild_id))
    if not guild:
        return {"processed": False, "delivered": False, "winner_id": 0, "count": len(_junk_roll_entries(auction))}

    valid_entries = [(uid, roll, created) for uid, roll, created in _junk_roll_entries(auction) if _is_ebolus_member(guild, uid)]
    auction["junk_roll_processed_at"] = _now_iso()
    auction["junk_interest_processed_at"] = auction["junk_roll_processed_at"]
    auction["junk_interest_process_reason"] = str(reason)
    auction["junk_interest_count"] = len(valid_entries)

    if valid_entries:
        winner_id, winner_roll, _created = max(valid_entries, key=lambda x: x[1])
        auction["junk_roll_winner_id"] = int(winner_id)
        auction["junk_roll_winner_roll"] = int(winner_roll)
        auction["junk_lottery_winner_id"] = int(winner_id)  # Legacy-Feld für bestehende Anzeigen
        auction["junk_lottery_drawn_at"] = _now_iso()
        auction["junk_lottery_pool"] = [int(uid) for uid, _roll, _created in valid_entries]
        emb = await _finalize_sale_delivery(client, guild, int(guild_id), auction, str(auction_id), int(winner_id), 0, actor_id=0, source="junk_roll")
        save_auctions()
        try:
            member = guild.get_member(int(winner_id)) or await guild.fetch_member(int(winner_id))
            if member and not member.bot:
                await member.send(
                    "🎲 **Müll-Item Roll gewonnen**\n\n"
                    f"Du hast **{auction.get('item_name','Item')}** kostenlos bekommen.\n"
                    f"Dein Gewinnerwurf: **{int(winner_roll)}**"
                )
        except Exception:
            pass
        await _announce_log(
            client,
            int(guild_id),
            "🎲 Müll-Item Roll abgeschlossen",
            f"**Item:** {auction.get('item_name','Item')}\n"
            f"**Würfe:** {len(valid_entries)}\n"
            f"**Gewinner:** <@{int(winner_id)}> mit **{int(winner_roll)}**\n"
            f"**Auktions-ID:** `{auction_id}`",
            discord.Color.green(),
        )
        return {"processed": True, "delivered": True, "winner_id": int(winner_id), "count": len(valid_entries), "embed": emb}

    auction["junk_direct_sale_open_at"] = _now_iso()
    auction["ends_at"] = ""
    save_auctions()
    await _refresh_auction_message(client, int(guild_id), auction)
    await _post_or_refresh_market_message(client, int(guild_id), auction)
    await _post_or_refresh_active_auction_message(client, int(guild_id), auction)
    await _announce_log(
        client,
        int(guild_id),
        "🛒 Müll-Item jetzt Sofortkauf",
        f"**Item:** {auction.get('item_name','Item')}\nNiemand hat innerhalb von {JUNK_ROLL_HOURS} Stunden gewürfelt. Das Item bleibt jetzt als **Gratis-Sofortkauf** offen.\n**Auktions-ID:** `{auction_id}`",
        discord.Color.green(),
    )
    return {"processed": True, "delivered": False, "winner_id": 0, "count": 0}


def _active_need_auctions(guild_id: int) -> list[dict]:
    return [a for a in _active_auctions(guild_id) if _auction_phase(a) == "need"]


def _active_free_auctions(guild_id: int) -> list[dict]:
    return [a for a in _active_auctions(guild_id) if _auction_phase(a) == "free"]


def _active_sale_items(guild_id: int) -> list[dict]:
    items = [a for a in _active_auctions(guild_id) if _auction_phase(a) == "sale"]
    items.sort(key=lambda a: str(a.get("ends_at", "")))
    return items


def _short_auction_line(auction: dict) -> str:
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    top = _highest_bid(auction)
    if _auction_phase(auction) == "sale":
        if _is_junk_interest_sale(auction):
            until = _junk_interest_until(auction)
            if _junk_interest_open(auction) and until:
                bid = f"Würfeln bis **{until.strftime('%d.%m. %H:%M')}**"
                when = "Roll"
            else:
                bid = "**Gratis-Sofortkauf**"
                when = "offen"
        else:
            bid = f"Preis **{int(auction.get('fixed_price', auction.get('start_bid', 0)) or 0)} EC**"
            when = end_dt.strftime("%d.%m. %H:%M") if end_dt else "?"
    else:
        bid = f"Höchstgebot **{int(top.get('amount',0) or 0)} EC**" if top else "noch kein Gebot"
        when = end_dt.strftime("%d.%m. %H:%M") if end_dt else "?"
    return f"• `{auction.get('id')}` – **{auction.get('item_name','Item')}** – {bid} – {when}"


def _auction_portal_embed(guild: discord.Guild, user_id: int | None = None) -> discord.Embed:
    emb = discord.Embed(
        title="🏷️ Auktion",
        description=(
            "Hier findest du alle aktuellen EC-Lootangebote.\n\n"
            "**Need-Auktion**\n"
            "Zeigt Auktionen, die aktuell nur für berechtigte Need-Spieler laufen.\n\n"
            "**Freie Auktion**\n"
            "Hier kannst du freie Auktionen auswählen und mit EC mitbieten.\n\n"
            "**Sale-Kauf**\n"
            "Hier kannst du Items direkt kaufen, die bald auslaufen oder günstiger verkauft werden."
        ),
        color=discord.Color.gold(),
        timestamp=_now(),
    )
    if user_id:
        member = guild.get_member(int(user_id))
        emb.add_field(name="🪙 Dein EC", value=f"**{_ec_balance(guild.id, int(user_id))} EC**", inline=True)
        lock_text = _loot_lock_text_for_member(member)
        if lock_text:
            emb.add_field(
                name="⏳ Lootsperre",
                value=f"Neue Mitglieder können erst nach **{NEW_MEMBER_LOOT_LOCK_DAYS} Tagen** bieten/kaufen.\n{lock_text}",
                inline=False,
            )
    emb.set_footer(text="Gebote und Käufe werden privat bestätigt. EC werden erst beim Kauf oder bei Übergabe abgebucht.")
    return emb


async def _portal_back_to_main(inter: discord.Interaction, guild_id: int, user_id: int):
    try:
        try:
            from bot import member_portal as mp  # type: ignore
        except Exception:
            import member_portal as mp  # type: ignore
        guild = inter.client.get_guild(int(guild_id))
        member = guild.get_member(int(user_id)) if guild else None
        if guild and member and hasattr(mp, "_main_menu_embed") and hasattr(mp, "MemberPortalMainView"):
            await inter.response.edit_message(embed=mp._main_menu_embed(guild, member), view=mp.MemberPortalMainView())  # type: ignore[attr-defined]
            return
    except Exception:
        pass
    await inter.response.edit_message(embed=discord.Embed(title="⚜️ Gildenzentrale", description="Öffne die Gildenzentrale bitte erneut über den Server-Button.", color=discord.Color.gold()), view=None)


class AuctionPortalMenuView(View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"portal_auction:{self.guild_id}:{self.user_id}:{child.custom_id or child.label}"

    @button(label="Need-Auktion", style=ButtonStyle.secondary, custom_id="need")
    async def need(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        auctions = _active_need_auctions(self.guild_id)
        emb = discord.Embed(title="🏷️ Need-Auktionen", color=discord.Color.gold(), timestamp=_now())
        if not auctions:
            emb.description = "Aktuell gibt es keine aktiven Need-Auktionen."
            await inter.response.edit_message(embed=emb, view=AuctionPortalSubView(self.guild_id, self.user_id))
            return
        emb.description = "Wähle eine Need-Auktion aus. Main- und Second-Need werden nie gemischt; bieten können nur die jeweils berechtigten Spieler.\n\n" + "\n".join(_short_auction_line(a) for a in auctions[:20])
        await inter.response.edit_message(embed=emb, view=AuctionSelectView(self.guild_id, self.user_id, "need"))

    @button(label="Freie Auktion", style=ButtonStyle.primary, custom_id="free")
    async def free(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        auctions = _active_free_auctions(self.guild_id)
        emb = discord.Embed(title="⚖️ Freie Auktionen", color=discord.Color.gold(), timestamp=_now())
        if not auctions:
            emb.description = "Aktuell gibt es keine freien Auktionen."
            await inter.response.edit_message(embed=emb, view=AuctionPortalSubView(self.guild_id, self.user_id))
            return
        emb.description = "Wähle eine freie Auktion aus, um mitzubieten.\n\n" + "\n".join(_short_auction_line(a) for a in auctions[:20])
        await inter.response.edit_message(embed=emb, view=AuctionSelectView(self.guild_id, self.user_id, "free"))

    @button(label="Sale-Kauf", style=ButtonStyle.success, custom_id="sale")
    async def sale(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        sales = _active_sale_items(self.guild_id)
        emb = discord.Embed(title="🛒 Sale-Kauf", color=discord.Color.gold(), timestamp=_now())
        if not sales:
            emb.description = "Aktuell gibt es keine Sale-Käufe."
            await inter.response.edit_message(embed=emb, view=AuctionPortalSubView(self.guild_id, self.user_id))
            return
        emb.description = "Wähle ein Sale-Item aus, um es direkt mit EC zu kaufen.\n\n" + "\n".join(_short_auction_line(a) for a in sales[:20])
        await inter.response.edit_message(embed=emb, view=AuctionSelectView(self.guild_id, self.user_id, "sale"))

    @button(label="Zurück", style=ButtonStyle.secondary, custom_id="back")
    async def back(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _portal_back_to_main(inter, self.guild_id, self.user_id)


class AuctionPortalSubView(View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary, custom_id="portal_auc_sub_back_auction")
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))

    @button(label="Gildenzentrale", style=ButtonStyle.secondary, custom_id="portal_auc_sub_back_main")
    async def back_main(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _portal_back_to_main(inter, self.guild_id, self.user_id)


class AuctionSelect(discord.ui.Select):
    def __init__(self, guild_id: int, user_id: int, mode: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.mode = str(mode)
        auctions = _active_sale_items(guild_id) if mode == "sale" else (_active_need_auctions(guild_id) if mode == "need" else _active_free_auctions(guild_id))
        options = []
        for a in auctions[:25]:
            end_dt = _parse_dt(str(a.get("ends_at", "") or ""))
            when = end_dt.strftime("%d.%m. %H:%M") if end_dt else "?"
            if mode == "sale":
                price = int(a.get("fixed_price", a.get("start_bid", 0)) or 0)
                if _is_junk_interest_sale(a):
                    until = _junk_interest_until(a)
                    if _junk_interest_open(a) and until:
                        desc = f"Würfeln bis {until.strftime('%d.%m. %H:%M')}"
                    else:
                        desc = "Gratis-Sofortkauf • offen"
                else:
                    desc = f"Direktkauf {price} EC • bis {when}"
            else:
                top = _highest_bid(a)
                bid = f"{int(top.get('amount',0) or 0)} EC" if top else "noch kein Gebot"
                desc = f"{bid} • bis {when}"
            options.append(discord.SelectOption(label=str(a.get("item_name", "Item"))[:100], value=str(a.get("id")), description=desc[:100]))
        if not options:
            options.append(discord.SelectOption(label="Keine Einträge", value="none", description="Aktuell nichts vorhanden"))
        super().__init__(
            placeholder=("Need-Auktion auswählen …" if mode == "need" else ("Auktion auswählen …" if mode != "sale" else "Sale-Item auswählen …")),
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"portal_auc_select_{self.mode}",
        )

    async def callback(self, inter: discord.Interaction):
        if self.values[0] == "none":
            await inter.response.send_message("Aktuell gibt es hier nichts auszuwählen.", ephemeral=True)
            return
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        aid = self.values[0]
        auc = _auction(self.guild_id, aid)
        if not auc:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        if self.mode == "sale":
            await inter.response.edit_message(embed=_sale_embed(guild, auc), view=PortalSaleBuyView(self.guild_id, self.user_id, aid))
        else:
            await inter.response.edit_message(embed=_auction_embed(guild, auc), view=PortalAuctionBidView(self.guild_id, self.user_id, aid))


class AuctionSelectView(View):
    def __init__(self, guild_id: int, user_id: int, mode: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.mode = str(mode)
        self.add_item(AuctionSelect(self.guild_id, self.user_id, self.mode))

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary, custom_id="portal_auc_select_back_auction")
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))


def _sale_embed(guild: discord.Guild, auction: dict) -> discord.Embed:
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    price = int(auction.get("fixed_price", auction.get("start_bid", 0)) or 0)
    price_text = "**Gratis**" if price <= 0 else f"**{price} EC**"
    item = str(auction.get("item_name", "Item") or "Item")
    status = str(auction.get("status", "active") or "active")

    if status == "delivered":
        buyer = int(auction.get("sold_to", 0) or auction.get("delivered_to", 0) or 0)
        if bool(auction.get("junk_drop", False)) and (auction.get("junk_roll_winner_id") or auction.get("junk_lottery_winner_id")):
            roll = int(auction.get("junk_roll_winner_roll", 0) or 0)
            desc = f"**Item:** {item}\n**Empfänger:** <@{buyer}>\n**Gewinnerwurf:** **{roll}**\n**Preis:** {price_text}"
            emb = discord.Embed(title=f"✅ Müll-Item Roll abgeschlossen: {item}", description=desc, color=discord.Color.green(), timestamp=_now())
            return _add_junk_roll_fields(emb, auction)
        emb = discord.Embed(
            title=f"✅ Sale-Kauf abgeschlossen: {item}",
            description=f"**Item:** {item}\n**Empfänger:** <@{buyer}>\n**Preis:** {price_text}",
            color=discord.Color.green(),
            timestamp=_now(),
        )
        return emb
    if status == "expired":
        return discord.Embed(
            title=f"⌛ Sale-Kauf abgelaufen: {item}",
            description=f"**Item:** {item}\nDas Item wurde nicht gekauft und ist nicht mehr verfügbar.",
            color=discord.Color.dark_grey(),
            timestamp=_now(),
        )
    if status == "cancelled":
        return discord.Embed(
            title=f"❌ Sale-Kauf abgebrochen: {item}",
            description=f"**Item:** {item}\nDieses Angebot wurde beendet.",
            color=discord.Color.red(),
            timestamp=_now(),
        )

    if _is_junk_interest_sale(auction):
        until = _junk_roll_until(auction)
        if _junk_roll_open(auction) and until:
            desc = (
                f"**Item:** {item}\n"
                f"**Ende:** {until.strftime('%d.%m.%Y %H:%M')}\n\n"
                "**Status:** Würfelphase\n\n"
                f"**Aktuell vorne:**\n{_junk_roll_summary(auction)}"
            )
            emb = discord.Embed(title="🧹 Müll-Item im Gratis-Roll", description=desc, color=discord.Color.green(), timestamp=_now())
            return _add_junk_roll_fields(emb, auction)
        desc = (
            f"**Item:** {item}\n\n"
            "**Status:** Gratis-Sofortkauf\n\n"
            "Es hat niemand gewürfelt.\n"
            "Der erste Spieler, der es nimmt, bekommt es direkt."
        )
        return discord.Embed(title="🛒 Müll-Item jetzt im Gratis-Sofortkauf", description=desc, color=discord.Color.green(), timestamp=_now())

    buy_note = "Dieses Item ist kostenlos. Beim Kauf werden keine EC abgebucht." if price <= 0 else "Beim Kauf werden die EC sofort abgebucht."
    emb = discord.Embed(
        title=f"🛒 Sale-Kauf: {item}",
        description=(
            f"Preis: {price_text}\n"
            f"Verfügbar bis: **{end_dt.strftime('%d.%m.%Y %H:%M') if end_dt else '?'}**\n\n"
            f"{buy_note}"
        ),
        color=discord.Color.green(),
        timestamp=_now(),
    )
    return emb


async def _handle_junk_sale_click(inter: discord.Interaction, guild_id: int, auction_id: str, guild: discord.Guild, auc: dict, user_id: int) -> None:
    until = _junk_roll_until(auc)

    if until and _now() < until and not auc.get("junk_roll_processed_at") and not auc.get("junk_interest_processed_at"):
        rolls = _junk_rolls(auc)
        existing = rolls.get(str(int(user_id)))
        if existing:
            await inter.response.send_message(
                f"ℹ️ Du hast für **{auc.get('item_name','Item')}** bereits gewürfelt: 🎲 **{int(existing.get('roll', 0) or 0)}**.",
                ephemeral=True,
            )
            return
        try:
            roll, roll_max = _next_unique_junk_roll(auc)
        except Exception:
            await inter.response.send_message("❌ Es konnte kein freier Würfelwert mehr erzeugt werden. Bitte Leader informieren.", ephemeral=True)
            return
        rolls[str(int(user_id))] = {"user_id": int(user_id), "roll": int(roll), "created_at": _now_iso()}
        auc["junk_rolls"] = rolls
        auc["junk_roll_max"] = int(roll_max)
        # Legacy-Felder mitführen, damit alte Anzeigen/Tools nicht brechen.
        auc["junk_interest_user_ids"] = [int(uid) for uid in _junk_rolls(auc).keys()]
        requests = auc.get("junk_interest_requests") if isinstance(auc.get("junk_interest_requests"), dict) else {}
        requests[str(user_id)] = _now_iso()
        auc["junk_interest_requests"] = requests
        auc["updated_at"] = _now_iso()
        save_auctions()
        try:
            await _refresh_auction_message(inter.client, int(guild_id), auc)
            await _post_or_refresh_market_message(inter.client, int(guild_id), auc)
            await _post_or_refresh_active_auction_message(inter.client, int(guild_id), auc)
            if inter.message:
                view = PortalSaleBuyView(int(guild_id), int(user_id), str(auction_id)) if isinstance(inter.channel, discord.DMChannel) else SaleBuyView(int(guild_id), str(auction_id))
                await inter.message.edit(embed=_sale_embed(guild, auc), view=view)
        except Exception as e:
            print(f"[loot_auction] junk roll refresh failed: {e!r}")
        await inter.response.send_message(
            f"🎲 Du hast für **{auc.get('item_name','Item')}** gewürfelt: **{int(roll)}**.",
            ephemeral=True,
        )
        return

    if until and not auc.get("junk_roll_processed_at") and not auc.get("junk_interest_processed_at"):
        result = await _process_junk_interest_window(inter.client, int(guild_id), str(auction_id), auc, reason="click_after_window")
        if result.get("delivered"):
            winner = int(result.get("winner_id", 0) or 0)
            try:
                if inter.message:
                    await inter.message.edit(embed=_sale_embed(guild, auc), view=None)
            except Exception:
                pass
            if winner == user_id:
                await inter.response.send_message(f"🎲 Die Würfelphase war vorbei. Gewinner: **du** hast **{auc.get('item_name','Item')}** bekommen.", ephemeral=True)
            else:
                await inter.response.send_message(f"🎲 Die Würfelphase war vorbei. Gewinner: <@{winner}>.", ephemeral=True)
            return
        # Keine Würfe vor Ablauf: ab jetzt Sofortkauf, kein Ablauf. Danach weiter zur direkten Übergabe.

    emb = await _finalize_sale_delivery(inter.client, guild, int(guild_id), auc, str(auction_id), int(user_id), 0, actor_id=int(user_id), source="junk_direct_claim")
    try:
        if inter.message:
            await inter.message.edit(embed=emb, view=None)
    except Exception:
        pass
    await inter.response.send_message(embed=emb, ephemeral=True)


async def _buy_sale_item(inter: discord.Interaction, guild_id: int, auction_id: str):
    guild = inter.guild or inter.client.get_guild(int(guild_id))
    if guild is None:
        await inter.response.send_message("❌ Server konnte nicht zugeordnet werden.", ephemeral=True)
        return
    auc = _auction(guild_id, auction_id)
    if not auc or str(auc.get("status", "")) != "active" or _auction_phase(auc) != "sale":
        await inter.response.send_message("❌ Sale-Kauf nicht gefunden oder nicht mehr aktiv.", ephemeral=True)
        return
    end_dt = _parse_dt(str(auc.get("ends_at", "") or ""))
    if end_dt and _now() >= end_dt:
        await inter.response.send_message("❌ Dieses Sale-Item ist bereits abgelaufen.", ephemeral=True)
        return
    user_id = int(inter.user.id)
    if not _is_ebolus_member(guild, user_id):
        await inter.response.send_message("❌ Nur Ebolus-Mitglieder können mit EC kaufen.", ephemeral=True)
        return
    if not await _require_loot_unlocked(inter, guild, user_id):
        return

    price = int(auc.get("fixed_price", auc.get("start_bid", 0)) or 0)
    if _is_junk_interest_sale(auc) and price <= 0:
        await _handle_junk_sale_click(inter, int(guild_id), str(auction_id), guild, auc, user_id)
        return

    if price > 0:
        bal = _ec_balance(guild_id, user_id)
        if bal < price:
            await inter.response.send_message(f"❌ Du hast aktuell nur **{bal} EC**, benötigst aber **{price} EC**.", ephemeral=True)
            return
        ok = _add_ec_transaction(
            int(guild_id), user_id, -price,
            f"Sale-Kauf: {auc.get('item_name','Item')}",
            user_id, str(auction_id),
            meta={"auction_id": str(auction_id), "item_id": auc.get("item_id", ""), "item_name": auc.get("item_name", ""), "kind": "sale"},
        )
        if not ok:
            await inter.response.send_message("❌ DKP/EC-System konnte nicht geladen werden. Keine EC abgebucht.", ephemeral=True)
            return

    emb = await _finalize_sale_delivery(inter.client, guild, int(guild_id), auc, str(auction_id), user_id, price, actor_id=user_id, source="sale_direct_buy")
    try:
        if inter.message:
            await inter.message.edit(embed=emb, view=None)
    except Exception:
        pass
    await inter.response.send_message(embed=emb, ephemeral=True)


class SaleBuyView(View):
    def __init__(self, guild_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)
        auc = _auction(self.guild_id, self.auction_id) or {}
        is_free = int(auc.get("fixed_price", auc.get("start_bid", 0)) or 0) <= 0
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "buy" or child.label == "Sofort kaufen":
                    if _is_junk_interest_sale(auc) and _junk_interest_open(auc):
                        child.label = "Würfeln"
                    elif is_free:
                        child.label = "Gratis nehmen"
                child.custom_id = f"lootsale:{auction_id}:{child.custom_id or child.label}"

    @button(label="Sofort kaufen", style=ButtonStyle.success, custom_id="buy")
    async def buy(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _buy_sale_item(inter, self.guild_id, self.auction_id)

    @button(label="Mein EC", style=ButtonStyle.secondary, custom_id="balance")
    async def balance(self, inter: discord.Interaction, btn: discord.ui.Button):
        bal = _ec_balance(self.guild_id, int(inter.user.id))
        await inter.response.send_message(f"🪙 Dein aktueller Kontostand: **{bal} EC**", ephemeral=True)


class PortalSaleBuyView(View):
    """Sale-Kauf Ansicht in der privaten Gildenzentrale mit Zurück-Buttons."""
    def __init__(self, guild_id: int, user_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.auction_id = str(auction_id)
        auc = _auction(self.guild_id, self.auction_id) or {}
        is_free = int(auc.get("fixed_price", auc.get("start_bid", 0)) or 0) <= 0
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "buy" or child.label == "Sofort kaufen":
                    if _is_junk_interest_sale(auc) and _junk_interest_open(auc):
                        child.label = "Gewürfelt" if str(self.user_id) in _junk_rolls(auc) else "Würfeln"
                    elif is_free:
                        child.label = "Gratis nehmen"
                child.custom_id = f"portalsale:{self.guild_id}:{self.user_id}:{auction_id}:{child.custom_id or child.label}"

    @button(label="Sofort kaufen", style=ButtonStyle.success, custom_id="buy")
    async def buy(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _buy_sale_item(inter, self.guild_id, self.auction_id)

    @button(label="Mein EC", style=ButtonStyle.secondary, custom_id="balance")
    async def balance(self, inter: discord.Interaction, btn: discord.ui.Button):
        bal = _ec_balance(self.guild_id, int(inter.user.id))
        await inter.response.send_message(f"🪙 Dein aktueller Kontostand: **{bal} EC**", ephemeral=True)

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary, custom_id="back_auction")
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))

    @button(label="Gildenzentrale", style=ButtonStyle.secondary, custom_id="back_main")
    async def back_main(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _portal_back_to_main(inter, self.guild_id, self.user_id)


async def open_auction_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(int(guild_id))
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    await inter.response.edit_message(embed=_auction_portal_embed(guild, int(user_id)), view=AuctionPortalMenuView(int(guild_id), int(user_id)))



async def start_loot_drop_auction(inter: discord.Interaction, guild: discord.Guild, item_id: str, actor_id: int | None = None) -> dict:
    """Called by loot_needs.py when Admin -> Loot -> Loot gedroppt is confirmed.

    Creates a virtual guild chest item and starts either a 48h Need auction or,
    if no Need exists, a 24h Free auction.
    """
    actor_id = int(actor_id or getattr(inter.user, "id", 0) or 0)
    item_name = _item_display(guild.id, item_id, fallback=str(item_id))
    main_ids = _main_need_user_ids(guild, item_id)
    secondary_ids = _secondary_need_user_ids(guild, item_id)
    if main_ids:
        phase = "need"
        mode = "main_need"
        eligible = main_ids
    elif secondary_ids:
        phase = "need"
        mode = "secondary_need"
        eligible = secondary_ids
    else:
        phase = "free"
        mode = "all"
        eligible = []
    aid = _new_auction_id()
    chest_id = f"C{aid[1:]}"
    hours = NEED_AUCTION_HOURS if phase == "need" else FREE_AUCTION_HOURS
    if mode == "main_need":
        start_bid = MAIN_NEED_START_BID
    elif mode == "secondary_need":
        start_bid = SECOND_NEED_START_BID
    else:
        start_bid = FREE_START_BID

    chest_obj = {
        "id": chest_id,
        "auction_id": aid,
        "guild_id": int(guild.id),
        "item_id": str(item_id),
        "item_name": item_name,
        "status": "need_auction" if phase == "need" else "free_auction",
        "dropped_at": _now_iso(),
        "created_by": actor_id,
        "expires_at": (_now() + timedelta(hours=SALE_HOURS + FREE_AUCTION_HOURS + (NEED_AUCTION_HOURS if phase == "need" else 0))).isoformat(),
    }
    _gchest(guild.id).setdefault("items", {})[chest_id] = chest_obj
    save_chest()

    auc = {
        "id": aid,
        "guild_id": int(guild.id),
        "kind": "auction",
        "phase": phase,
        "chest_item_id": chest_id,
        "item_id": str(item_id),
        "item_name": item_name,
        "created_at": _now_iso(),
        "created_by": actor_id,
        "source": "loot_drop_menu",
        "ends_at": (_now() + timedelta(hours=hours)).isoformat(),
        "start_bid": int(start_bid),
        "min_increment": (FREE_MIN_INCREMENT if phase == "free" else DEFAULT_MIN_INCREMENT),
        "eligibility_mode": mode,
        "eligible_user_ids": eligible,
        "bids": [],
        "status": "active",
        "message_id": 0,
        "channel_id": 0,
    }
    _gauctions(guild.id).setdefault("auctions", {})[aid] = auc
    save_auctions()

    ch = _auction_channel(inter.client, guild.id, inter.channel)
    if ch:
        msg = await ch.send(embed=_auction_embed(guild, auc), view=AuctionBidView(guild.id, aid))
        auc["message_id"] = int(msg.id)
        auc["channel_id"] = int(getattr(ch, "id", 0) or 0)
        save_auctions()

    await _post_or_refresh_active_auction_message(inter.client, guild.id, auc)

    if phase == "free":
        await _post_or_refresh_market_message(inter.client, guild.id, auc)

    log = _auction_channel(inter.client, guild.id, None)
    if log and log != ch:
        try:
            title = (("🎯 Main-Need-Auktion gestartet" if mode == "main_need" else "🔁 Second-Need-Auktion gestartet") if phase == "need" else "⚖️ Freie Auktion gestartet")
            desc = (
                f"**Item:** {item_name}\n"
                f"**Startgebot:** {start_bid} EC\n"
                f"**Dauer:** {hours} Stunden\n"
                f"**Auktions-ID:** `{aid}`\n"
            )
            if phase == "need":
                desc += "\nBerechtigt: " + (", ".join(f"<@{uid}>" for uid in eligible[:20]) if eligible else "—")
            else:
                desc += "\nKeine offenen Needs gefunden. Das Item ist direkt in der freien Auktion."
            await log.send(embed=discord.Embed(title=title, description=desc, color=discord.Color.gold(), timestamp=_now()))
        except Exception:
            pass

    notified = 0
    failed = 0
    notify_refs: list[dict] = []
    if phase == "need":
        for uid in eligible:
            ref = await _send_auction_tracking_dm(guild, uid, auc)
            if ref:
                notify_refs.append(ref)
                notified += 1
            else:
                failed += 1
            await asyncio.sleep(0.08)
        if notify_refs:
            auc["notify_message_refs"] = notify_refs
            save_auctions()

    return {
        "auction_id": aid,
        "chest_item_id": chest_id,
        "phase": phase,
        "eligibility_mode": mode,
        "item_name": item_name,
        "eligible_user_ids": eligible,
        "notified": notified,
        "failed": failed,
        "channel_id": int(auc.get("channel_id", 0) or 0),
    }


async def _delete_discord_message_ref(client: discord.Client, channel_id: int, message_id: int) -> bool:
    if not channel_id or not message_id:
        return False
    try:
        ch = client.get_channel(int(channel_id))
        if ch is None:
            ch = await client.fetch_channel(int(channel_id))
        msg = await ch.fetch_message(int(message_id))
        await msg.delete()
        return True
    except Exception:
        return False


async def _purge_auction_messages(client: discord.Client, auction: dict) -> int:
    """Delete Discord messages that belong to one auction. Does not touch EC transactions.

    Wichtig: Diese Bereinigung darf niemals die Auktionskanal-Konfiguration anfassen.
    Sie löscht nur die konkreten Nachrichten-Referenzen dieser einen Auktion und
    setzt danach die Message-IDs im Auktionsobjekt zurück, damit spätere Start-/Sync-
    Läufe nicht mehr versuchen, gelöschte Nachrichten zu bearbeiten.
    """
    deleted = 0
    refs = [
        ("channel_id", "message_id"),
        ("market_channel_id", "market_message_id"),
        ("active_channel_id", "active_message_id"),
        ("delivery_channel_id", "delivery_message_id"),
    ]
    seen: set[tuple[int, int]] = set()
    for cid_key, mid_key in refs:
        cid = int(auction.get(cid_key, 0) or 0)
        mid = int(auction.get(mid_key, 0) or 0)
        if not cid or not mid or (cid, mid) in seen:
            continue
        seen.add((cid, mid))
        if await _delete_discord_message_ref(client, cid, mid):
            deleted += 1
        # Egal ob Discord die Nachricht noch gefunden hat: Die Referenz ist ab jetzt
        # für diese Auktion erledigt und darf nicht wieder gesynct werden.
        auction[cid_key] = 0
        auction[mid_key] = 0

    tracking_refs = list(auction.get("notify_message_refs") or [])
    await _delete_auction_tracking_dms(client, auction)
    # Tracking-DMs können vom User längst gelöscht sein. Deshalb nicht blind als
    # Discord-Erfolg zählen, aber die Refs werden sauber entfernt.
    auction["notify_message_refs"] = []
    auction["purged_message_refs_at"] = _now_iso()
    save_auctions()
    return deleted


async def setup_loot_auction(client: discord.Client, tree: app_commands.CommandTree):
    global _client_ref
    _client_ref = client

    # Re-register persistent views for active/closed auctions after restart.
    try:
        # Portal root views use guild/user-specific custom_ids when sent, but adding one generic view keeps the class loaded.
        pass
    except Exception:
        pass
    for gid_str, g in list(auction_state.items()):
        try:
            gid = int(gid_str)
        except Exception:
            continue
        auctions = g.get("auctions") if isinstance(g.get("auctions"), dict) else {}
        for aid, auc in auctions.items():
            try:
                if str(auc.get("status", "")) == "active":
                    if _auction_phase(auc) == "sale":
                        client.add_view(SaleBuyView(gid, str(aid)), message_id=int(auc.get("message_id", 0) or 0) or None)
                        if int(auc.get("market_message_id", 0) or 0):
                            client.add_view(SaleBuyView(gid, str(aid)), message_id=int(auc.get("market_message_id", 0) or 0))
                    else:
                        client.add_view(AuctionBidView(gid, str(aid)), message_id=int(auc.get("message_id", 0) or 0) or None)
                elif str(auc.get("status", "")) == "closed" and int(auc.get("delivery_message_id", 0) or 0):
                    client.add_view(AuctionDeliveryView(gid, str(aid)), message_id=int(auc.get("delivery_message_id", 0) or 0))
            except Exception:
                pass

    if not auction_close_loop.is_running():
        auction_close_loop.start()

    try:
        asyncio.create_task(_sync_active_auction_messages_after_ready(client))
    except Exception as e:
        print(f"[loot_auction] active auction startup sync scheduling failed: {e!r}")

    group = app_commands.Group(name="auction", description="Loot-Auktionen mit Ebolus Coins")

    @group.command(name="set_channel", description="Setzt den Kanal für neue Loot-Auktionen")
    async def auction_set_channel(inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            _gcfg(pick_inter.guild.id)["auction_channel_id"] = int(channel.id)
            save_cfg()
            await pick_inter.response.edit_message(content=f"✅ Auktionskanal gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "📌 Auktionskanal auswählen", _picked)

    @group.command(name="set_channel_id", description="Setzt den Auktionskanal per Kanal-ID, falls Discord ihn nicht vorschlägt")
    async def auction_set_channel_id(inter: discord.Interaction, channel_id: str):
        await _set_auction_cfg_channel_by_id(inter, "auction_channel_id", channel_id, "Auktionskanal")

    @group.command(name="set_log_channel", description="Setzt optional den Log-Kanal für Auktionsabschluss/Übergabe")
    async def auction_set_log_channel(inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            _gcfg(pick_inter.guild.id)["log_channel_id"] = int(channel.id)
            save_cfg()
            await pick_inter.response.edit_message(content=f"✅ Auktions-Log-Kanal gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "🧾 Auktions-Log-Kanal auswählen", _picked)

    @group.command(name="set_log_channel_id", description="Setzt den Log-Kanal per Kanal-ID, falls Discord ihn nicht vorschlägt")
    async def auction_set_log_channel_id(inter: discord.Interaction, channel_id: str):
        await _set_auction_cfg_channel_by_id(inter, "log_channel_id", channel_id, "Auktions-Log-Kanal")

    @group.command(name="set_market_channel", description="Setzt den öffentlichen Kanal für freie Auktionen und Sale-Käufe")
    async def auction_set_market_channel(inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            _gcfg(pick_inter.guild.id)["market_channel_id"] = int(channel.id)
            save_cfg()
            await pick_inter.response.edit_message(content=f"✅ Marktplatz-Kanal gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "📣 Marktplatz-Kanal auswählen", _picked)

    @group.command(name="set_market_channel_id", description="Setzt den Marktplatz-Kanal per Kanal-ID, falls Discord ihn nicht vorschlägt")
    async def auction_set_market_channel_id(inter: discord.Interaction, channel_id: str):
        await _set_auction_cfg_channel_by_id(inter, "market_channel_id", channel_id, "Marktplatz-Kanal")

    @group.command(name="set_active_channel", description="Setzt den Kanal für die Übersicht aktueller Auktionen/Sales")
    async def auction_set_active_channel(inter: discord.Interaction):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            _gcfg(pick_inter.guild.id)["active_channel_id"] = int(channel.id)
            save_cfg()
            await _sync_active_auction_messages(client, pick_inter.guild.id)
            await pick_inter.response.edit_message(content=f"✅ Aktuelle-Auktionen-Kanal gesetzt: {channel.mention}\nAktive Items wurden synchronisiert.", view=None)

        await send_text_channel_picker(inter, "📋 Aktuelle-Auktionen-Kanal auswählen", _picked)

    @group.command(name="set_active_channel_id", description="Setzt den Aktuelle-Items-Kanal per Kanal-ID, falls Discord ihn nicht vorschlägt")
    async def auction_set_active_channel_id(inter: discord.Interaction, channel_id: str):
        await _set_auction_cfg_channel_by_id(inter, "active_channel_id", channel_id, "Aktuelle-Auktionen-Kanal", sync_active=True)

    @group.command(name="start", description="Startet eine EC-Loot-Auktion")
    @app_commands.choices(eligibility=ELIGIBILITY_CHOICES)
    async def auction_start(
        inter: discord.Interaction,
        item: str,
        start_bid: int = DEFAULT_START_BID,
        min_increment: int = DEFAULT_MIN_INCREMENT,
        duration_hours: int = DEFAULT_DURATION_HOURS,
        eligibility: str = "auto",
    ):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if start_bid < 0 or min_increment < 1 or duration_hours < 1 or duration_hours > 240:
            await inter.response.send_message("❌ Ungültige Werte. Dauer: 1–240 Stunden, Mindestschritt mindestens 1.", ephemeral=True)
            return
        guild = inter.guild
        item_id, item_name, matches = _find_item(guild.id, item)
        if not item_id and len(matches) > 1:
            lines = "\n".join(f"• `{mid}` – {name}" for mid, name in matches[:10])
            await inter.response.send_message(
                "❌ Mehrere Items gefunden. Bitte genauer schreiben oder die Item-ID nutzen:\n" + lines,
                ephemeral=True,
            )
            return
        mode, eligible_ids = _eligible_user_ids(guild, item_id, eligibility)
        if eligibility in {"main_need", "secondary_need"} and not eligible_ids:
            await inter.response.send_message("❌ Für dieses Item gibt es aktuell keinen passenden offenen Need. Nutze eligibility = Alle Ebolus-Mitglieder.", ephemeral=True)
            return
        aid = _new_auction_id()
        ends = _now() + timedelta(hours=int(duration_hours))
        auc = {
            "id": aid,
            "guild_id": int(guild.id),
            "kind": "auction",
            "item_id": item_id,
            "item_name": item_name,
            "created_at": _now_iso(),
            "created_by": int(inter.user.id),
            "ends_at": ends.isoformat(),
            "start_bid": int(start_bid),
            "min_increment": int(min_increment),
            "eligibility_mode": mode,
            "eligible_user_ids": eligible_ids,
            "bids": [],
            "status": "active",
            "message_id": 0,
            "channel_id": 0,
        }
        _gauctions(guild.id).setdefault("auctions", {})[aid] = auc
        save_auctions()

        ch = _auction_channel(client, guild.id, inter.channel)
        if not ch:
            await inter.response.send_message("❌ Kein Auktionskanal gefunden. Nutze `/auction set_channel`.", ephemeral=True)
            return
        msg = await ch.send(embed=_auction_embed(guild, auc), view=AuctionBidView(guild.id, aid))
        auc["message_id"] = int(msg.id)
        auc["channel_id"] = int(getattr(ch, "id", 0) or 0)
        save_auctions()
        await _post_or_refresh_active_auction_message(client, guild.id, auc)
        if _auction_phase(auc) == "free":
            await _post_or_refresh_market_message(client, guild.id, auc)
        await inter.response.send_message(f"✅ Auktion gestartet: `{aid}` in {ch.mention}", ephemeral=True)



    @group.command(name="sale_start", description="Startet einen Sofortkauf/Sale-Kauf mit festem EC-Preis")
    async def auction_sale_start(
        inter: discord.Interaction,
        item: str,
        price: int,
        duration_hours: int = 240,
    ):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if price < 1 or duration_hours < 1 or duration_hours > 720:
            await inter.response.send_message("❌ Ungültige Werte. Preis mindestens 1 EC, Dauer 1–720 Stunden.", ephemeral=True)
            return
        guild = inter.guild
        item_id, item_name, matches = _find_item(guild.id, item)
        if not item_id and len(matches) > 1:
            lines = "\n".join(f"• `{mid}` – {name}" for mid, name in matches[:10])
            await inter.response.send_message(
                "❌ Mehrere Items gefunden. Bitte genauer schreiben oder die Item-ID nutzen:\n" + lines,
                ephemeral=True,
            )
            return
        aid = _new_auction_id()
        ends = _now() + timedelta(hours=int(duration_hours))
        auc = {
            "id": aid,
            "guild_id": int(guild.id),
            "kind": "sale",
            "item_id": item_id,
            "item_name": item_name,
            "created_at": _now_iso(),
            "created_by": int(inter.user.id),
            "ends_at": ends.isoformat(),
            "fixed_price": int(price),
            "start_bid": int(price),
            "min_increment": 0,
            "eligibility_mode": "all",
            "eligible_user_ids": [],
            "bids": [],
            "status": "active",
            "message_id": 0,
            "channel_id": 0,
        }
        _gauctions(guild.id).setdefault("auctions", {})[aid] = auc
        save_auctions()
        ch = _auction_channel(client, guild.id, inter.channel)
        if not ch:
            await inter.response.send_message("❌ Kein Auktionskanal gefunden. Nutze `/auction set_channel`.", ephemeral=True)
            return
        msg = await ch.send(embed=_sale_embed(guild, auc), view=SaleBuyView(guild.id, aid))
        auc["message_id"] = int(msg.id)
        auc["channel_id"] = int(getattr(ch, "id", 0) or 0)
        save_auctions()
        await _post_or_refresh_active_auction_message(client, guild.id, auc)
        await _post_or_refresh_market_message(client, guild.id, auc)
        await inter.response.send_message(f"✅ Sale-Kauf gestartet: `{aid}` in {ch.mention}", ephemeral=True)

    @group.command(name="list", description="Zeigt aktive Loot-Auktionen")
    async def auction_list(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        active = _active_auctions(inter.guild.id)
        if not active:
            await inter.response.send_message("Aktuell keine aktiven Auktionen.", ephemeral=True)
            return
        lines = []
        for a in active[:20]:
            end_dt = _parse_dt(str(a.get("ends_at", "") or ""))
            top = _highest_bid(a)
            bid = f"{int(top.get('amount',0) or 0)} EC" if top else "kein Gebot"
            when = end_dt.strftime("%d.%m. %H:%M") if end_dt else "?"
            lines.append(f"• `{a.get('id')}` – **{a.get('item_name')}** – {bid} – Ende {when}")
        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @group.command(name="status", description="Zeigt den Status einer Auktion")
    async def auction_status(inter: discord.Interaction, auction_id: str):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        auc = _auction(inter.guild.id, auction_id)
        if not auc:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        await inter.response.send_message(embed=_auction_embed(inter.guild, auc), ephemeral=True)

    @group.command(name="end", description="Beendet eine Auktion sofort")
    async def auction_end(inter: discord.Interaction, auction_id: str):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        ok = await _close_auction(client, inter.guild.id, auction_id, reason="manual")
        await inter.response.send_message("✅ Auktion beendet." if ok else "❌ Auktion nicht gefunden oder nicht aktiv.", ephemeral=True)

    @group.command(name="cancel", description="Bricht eine Auktion ohne Gewinner/Abbuchung ab")
    async def auction_cancel(inter: discord.Interaction, auction_id: str, reason: str = ""):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        auc = _auction(inter.guild.id, auction_id)
        if not auc or str(auc.get("status", "")) not in {"active", "closed"}:
            await inter.response.send_message("❌ Auktion nicht gefunden oder kann nicht abgebrochen werden.", ephemeral=True)
            return
        auc["status"] = "cancelled"
        auc["cancelled_at"] = _now_iso()
        auc["cancelled_by"] = int(inter.user.id)
        auc["cancel_reason"] = _safe_text(reason)
        await _edit_market_message_final(client, inter.guild.id, auc, final="cancelled")
        await _delete_active_auction_message(client, auc)
        save_auctions()
        await _refresh_auction_message(client, inter.guild.id, auc)
        ch = _auction_channel(client, inter.guild.id, None) or _log_channel(client, inter.guild.id)
        if ch:
            emb = discord.Embed(
                title="❌ Loot-Auktion abgebrochen",
                description=f"**Item:** {auc.get('item_name','Item')}\n**Auktion:** `{auction_id}`\n**Grund:** {_safe_text(reason) or '—'}",
                color=discord.Color.red(),
                timestamp=_now(),
            )
            try:
                await ch.send(embed=emb)
            except Exception:
                pass
        await inter.response.send_message("✅ Auktion abgebrochen.", ephemeral=True)

    @group.command(name="delete", description="Löscht eine Auktion und ihre Bot-Nachrichten endgültig")
    async def auction_delete(inter: discord.Interaction, auction_id: str, confirm: bool = False):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        auc = _auction(inter.guild.id, auction_id)
        if not auc:
            await inter.response.send_message("❌ Auktion nicht gefunden.", ephemeral=True)
            return
        if not confirm:
            await inter.response.send_message(
                "⚠️ Diese Aktion löscht die Auktion und alle zugehörigen Bot-Nachrichten endgültig. "
                "Bereits abgebuchte EC werden **nicht** automatisch zurückerstattet. "
                "Führe den Befehl erneut mit `confirm: True` aus.",
                ephemeral=True,
            )
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        # Früher wurde die Auktion komplett aus der JSON entfernt. Das ist riskant,
        # weil alte Button-Views, Start-Syncs oder Logs danach ins Leere laufen können.
        # Sicherer: Discord-Nachrichten löschen, aber einen gelöschten Tombstone behalten.
        auc["status"] = "deleted"
        auc["deleted_at"] = _now_iso()
        auc["deleted_by"] = int(inter.user.id)
        deleted_messages = await _purge_auction_messages(client, auc)
        save_auctions()
        try:
            await _sync_active_auction_messages(client, inter.guild.id)
        except Exception as e:
            print(f"[loot_auction] active sync after delete failed: {e!r}")
        await inter.followup.send(
            f"✅ Auktion `{auction_id}` gelöscht/archiviert. Gelöschte Bot-Nachrichten: **{deleted_messages}**. "
            "EC-Buchungen und Kanal-Einstellungen wurden nicht verändert.",
            ephemeral=True,
        )

    try:
        tree.add_command(group)
        print("✅ /auction Command-Gruppe registriert")
    except app_commands.CommandAlreadyRegistered:
        pass
