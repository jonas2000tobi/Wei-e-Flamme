from __future__ import annotations

import json
import math
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button, Modal, TextInput
from discord.enums import ButtonStyle

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
FREE_MIN_INCREMENT = 1

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
    c.setdefault("drop_notify_channel_id", 0)
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


def _drop_notify_channel(client: discord.Client, guild_id: int):
    """Separater Kanal für die Meldung: Loot gedroppt + wer per DM benachrichtigt wurde."""
    c = _gcfg(guild_id)
    ch_id = int(c.get("drop_notify_channel_id", 0) or 0)
    guild = client.get_guild(int(guild_id))
    if guild and ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            return ch
    return None


async def _send_drop_notify_channel_message(
    client: discord.Client,
    guild: discord.Guild,
    auction: dict,
    eligible_user_ids: list[int],
    notified_user_ids: list[int],
    failed_user_ids: list[int],
) -> None:
    """Postet eine einzelne Drop-/Benachrichtigungs-Meldung in den separat gesetzten Kanal."""
    ch = _drop_notify_channel(client, int(guild.id))
    if not ch:
        return
    try:
        mode = str(auction.get("eligibility_mode", "all") or "all")
        phase = str(auction.get("phase", "") or "")
        item_name = str(auction.get("item_name", "Unbekanntes Item") or "Unbekanntes Item")
        aid = str(auction.get("id", "") or "")
        start_bid = int(auction.get("start_bid", 0) or 0)
        ends_at = str(auction.get("ends_at", "") or "")

        if mode == "main_need":
            title = "🎁 Loot gedroppt – Main-Need-Spieler benachrichtigt"
            phase_text = "Main-Need-Auktion"
        elif mode == "secondary_need":
            title = "🎁 Loot gedroppt – Second-Need-Spieler benachrichtigt"
            phase_text = "Second-Need-Auktion"
        else:
            title = "🎁 Loot gedroppt – freie Auktion gestartet"
            phase_text = "Freie Auktion"

        desc = (
            f"**Item:** {item_name}\n"
            f"**Auktion:** {phase_text}\n"
            f"**Auktions-ID:** `{aid}`\n"
            f"**Startgebot:** {start_bid} EC\n"
        )
        if ends_at:
            desc += f"**Läuft bis:** `{ends_at}`\n"

        if phase == "need":
            if eligible_user_ids:
                eligible_text = ", ".join(f"<@{uid}>" for uid in eligible_user_ids[:30])
                if len(eligible_user_ids) > 30:
                    eligible_text += f" … +{len(eligible_user_ids) - 30}"
            else:
                eligible_text = "—"

            if notified_user_ids:
                notified_text = ", ".join(f"<@{uid}>" for uid in notified_user_ids[:30])
                if len(notified_user_ids) > 30:
                    notified_text += f" … +{len(notified_user_ids) - 30}"
            else:
                notified_text = "—"

            desc += (
                f"\n**Berechtigte Spieler:** {eligible_text}\n"
                f"**Per DM benachrichtigt:** {notified_text}\n"
                f"**Erfolgreich:** {len(notified_user_ids)} / {len(eligible_user_ids)}"
            )
            if failed_user_ids:
                failed_text = ", ".join(f"<@{uid}>" for uid in failed_user_ids[:20])
                if len(failed_user_ids) > 20:
                    failed_text += f" … +{len(failed_user_ids) - 20}"
                desc += f"\n**DM fehlgeschlagen:** {failed_text}"
        else:
            desc += "\nKeine offenen Main-/Second-Needs gefunden. Es wurden keine Need-DMs verschickt."

        embed = discord.Embed(
            title=title,
            description=desc,
            color=discord.Color.gold(),
            timestamp=_now(),
        )
        await ch.send(embed=embed)
    except Exception as e:
        print(f"[loot_auction] drop notify channel message failed: {e!r}")


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
    # aber im Gildenmenü und in der Auktionskarte unnötig unübersichtlich.
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
        view = (SaleBuyView(int(guild_id), str(auction.get("id", ""))) if _auction_phase(auction) == "sale" else AuctionBidView(int(guild_id), str(auction.get("id", "")))) if auction.get("status") == "active" else None
        await msg.edit(embed=_auction_embed(guild, auction), view=view)
    except Exception as e:
        print(f"[loot_auction] refresh failed: {e!r}")


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


async def _post_or_refresh_market_message(client: discord.Client, guild_id: int, auction: dict) -> None:
    """Postet/aktualisiert die öffentliche Nachricht für Freie Auktion oder Sale-Kauf."""
    phase = _auction_phase(auction)
    if phase not in {"free", "sale"} or str(auction.get("status", "")) != "active":
        return
    guild = client.get_guild(int(guild_id))
    if not guild:
        return
    ch = _market_channel(client, guild_id)
    if not ch:
        return

    view = SaleBuyView(int(guild_id), str(auction.get("id", ""))) if phase == "sale" else AuctionBidView(int(guild_id), str(auction.get("id", "")))
    embed = _sale_embed(guild, auction) if phase == "sale" else _auction_embed(guild, auction)
    content = "🛒 **Neues Sale-Item verfügbar!**" if phase == "sale" else "⚖️ **Item ist jetzt in der freien Auktion verfügbar!**"

    old_cid = int(auction.get("market_channel_id", 0) or 0)
    old_mid = int(auction.get("market_message_id", 0) or 0)
    if old_mid and old_cid == int(getattr(ch, "id", 0) or 0):
        try:
            msg = await ch.fetch_message(old_mid)
            await msg.edit(content=content, embed=embed, view=view)
            return
        except Exception as e:
            print(f"[loot_auction] market message refresh failed: {e!r}")

    # Falls noch eine alte Marktplatz-Nachricht aus einer anderen Phase existiert, erst löschen.
    if old_mid:
        await _delete_market_message(client, auction)
    try:
        msg = await ch.send(content=content, embed=embed, view=view)
        auction["market_channel_id"] = int(getattr(ch, "id", 0) or 0)
        auction["market_message_id"] = int(msg.id)
        auction["market_message_posted_at"] = _now_iso()
        save_auctions()
    except Exception as e:
        print(f"[loot_auction] market message post failed: {e!r}")


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

    # Wenn das Gebot aus dem Gildenmenü/DM kommt, muss auch diese aktuelle DM-Nachricht
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
    """Bietansicht im privaten Gildenmenü mit Zurück-Buttons."""
    def __init__(self, guild_id: int, user_id: int, auction_id: str):
        super().__init__(timeout=300)
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

    @button(label="Gildenmenü", style=ButtonStyle.secondary, custom_id="back_main")
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
        await _delete_market_message(inter.client, auction)
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
        ch = _log_channel(inter.client, self.guild_id)
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
    ch = _log_channel(client, guild_id)
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
        "Bieten kannst du über **Gildenmenü → Auktion → Need-Auktion**."
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
    await _delete_market_message(client, auction)
    await _refresh_auction_message(client, guild_id, auction)
    await _refresh_auction_tracking_dms(client, guild_id, auction)
    await _post_or_refresh_market_message(client, guild_id, auction)
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
            await _delete_market_message(client, auction)
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

    ch = _log_channel(client, guild_id)
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
# Gildenmenü / Portal integration
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
        bid = f"Preis **{int(auction.get('fixed_price', auction.get('start_bid', 0)) or 0)} EC**"
    else:
        bid = f"Höchstgebot **{int(top.get('amount',0) or 0)} EC**" if top else "noch kein Gebot"
    when = end_dt.strftime("%d.%m. %H:%M") if end_dt else "?"
    return f"• `{auction.get('id')}` – **{auction.get('item_name','Item')}** – {bid} – bis {when}"


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
        emb.add_field(name="🪙 Dein EC", value=f"**{_ec_balance(guild.id, int(user_id))} EC**", inline=True)
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
    await inter.response.edit_message(embed=discord.Embed(title="⚜️ Gildenmenü", description="Öffne das Gildenmenü bitte erneut über den Server-Button.", color=discord.Color.gold()), view=None)


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
        super().__init__(timeout=300)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary)
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))

    @button(label="Gildenmenü", style=ButtonStyle.secondary)
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
                desc = f"Direktkauf {price} EC • bis {when}"
            else:
                top = _highest_bid(a)
                bid = f"{int(top.get('amount',0) or 0)} EC" if top else "noch kein Gebot"
                desc = f"{bid} • bis {when}"
            options.append(discord.SelectOption(label=str(a.get("item_name", "Item"))[:100], value=str(a.get("id")), description=desc[:100]))
        if not options:
            options.append(discord.SelectOption(label="Keine Einträge", value="none", description="Aktuell nichts vorhanden"))
        super().__init__(placeholder=("Need-Auktion auswählen …" if mode == "need" else ("Auktion auswählen …" if mode != "sale" else "Sale-Item auswählen …")), min_values=1, max_values=1, options=options)

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
        super().__init__(timeout=300)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.mode = str(mode)
        self.add_item(AuctionSelect(self.guild_id, self.user_id, self.mode))

    @button(label="Zurück zu Auktion", style=ButtonStyle.secondary)
    async def back_auction(self, inter: discord.Interaction, btn: discord.ui.Button):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        await inter.response.edit_message(embed=_auction_portal_embed(guild, self.user_id), view=AuctionPortalMenuView(self.guild_id, self.user_id))


def _sale_embed(guild: discord.Guild, auction: dict) -> discord.Embed:
    end_dt = _parse_dt(str(auction.get("ends_at", "") or ""))
    price = int(auction.get("fixed_price", auction.get("start_bid", 0)) or 0)
    emb = discord.Embed(
        title=f"🛒 Sale-Kauf: {auction.get('item_name','Item')}",
        description=(
            f"Preis: **{price} EC**\n"
            f"Verfügbar bis: **{end_dt.strftime('%d.%m.%Y %H:%M') if end_dt else '?'}**\n\n"
            "Beim Kauf werden die EC sofort abgebucht."
        ),
        color=discord.Color.green(),
        timestamp=_now(),
    )
    return emb


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
    price = int(auc.get("fixed_price", auc.get("start_bid", 0)) or 0)
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
    auc["status"] = "delivered"
    auc["sold_at"] = _now_iso()
    auc["sold_to"] = user_id
    auc["charged_amount"] = price
    _mark_chest_item_status(int(guild_id), auc, "delivered", {"delivered_to": user_id, "delivered_by": user_id, "delivered_at": auc["sold_at"]})
    locked_need = _mark_need_received_for_winner(
        int(guild_id),
        user_id,
        str(auc.get("item_id", "") or ""),
        eligibility_mode=str(auc.get("eligibility_mode", "all") or "all"),
        auction_id=str(auction_id),
    )
    if locked_need:
        auc["locked_need_slot"] = locked_need
    await _delete_auction_tracking_dms(inter.client, auc)
    await _delete_market_message(inter.client, auc)
    save_auctions()
    try:
        await _refresh_auction_message(inter.client, int(guild_id), auc)
    except Exception:
        pass
    emb = discord.Embed(
        title="✅ Sale-Kauf abgeschlossen",
        description=f"**Item:** {auc.get('item_name','Item')}\n**Käufer:** <@{user_id}>\n**Preis:** **{price} EC**",
        color=discord.Color.green(),
        timestamp=_now(),
    )
    ch = _log_channel(inter.client, int(guild_id))
    if ch:
        try:
            await ch.send(embed=emb)
        except Exception:
            pass
    try:
        if inter.message:
            await inter.message.edit(view=None)
    except Exception:
        pass
    await inter.response.send_message(embed=emb, ephemeral=True)


class SaleBuyView(View):
    def __init__(self, guild_id: int, auction_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.custom_id = f"lootsale:{auction_id}:{child.custom_id or child.label}"

    @button(label="Sofort kaufen", style=ButtonStyle.success, custom_id="buy")
    async def buy(self, inter: discord.Interaction, btn: discord.ui.Button):
        await _buy_sale_item(inter, self.guild_id, self.auction_id)

    @button(label="Mein EC", style=ButtonStyle.secondary, custom_id="balance")
    async def balance(self, inter: discord.Interaction, btn: discord.ui.Button):
        bal = _ec_balance(self.guild_id, int(inter.user.id))
        await inter.response.send_message(f"🪙 Dein aktueller Kontostand: **{bal} EC**", ephemeral=True)


class PortalSaleBuyView(View):
    """Sale-Kauf Ansicht im privaten Gildenmenü mit Zurück-Buttons."""
    def __init__(self, guild_id: int, user_id: int, auction_id: str):
        super().__init__(timeout=300)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.auction_id = str(auction_id)
        for child in self.children:
            if isinstance(child, discord.ui.Button):
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

    @button(label="Gildenmenü", style=ButtonStyle.secondary, custom_id="back_main")
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

    if phase == "free":
        await _post_or_refresh_market_message(inter.client, guild.id, auc)

    log = _log_channel(inter.client, guild.id)
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
    notified_user_ids: list[int] = []
    failed_user_ids: list[int] = []
    notify_refs: list[dict] = []
    if phase == "need":
        for uid in eligible:
            ref = await _send_auction_tracking_dm(guild, uid, auc)
            if ref:
                notify_refs.append(ref)
                notified += 1
                notified_user_ids.append(int(uid))
            else:
                failed += 1
                failed_user_ids.append(int(uid))
            await asyncio.sleep(0.08)
        if notify_refs:
            auc["notify_message_refs"] = notify_refs
            save_auctions()

    await _send_drop_notify_channel_message(
        inter.client,
        guild,
        auc,
        [int(uid) for uid in eligible],
        notified_user_ids,
        failed_user_ids,
    )

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
    """Delete Discord messages that belong to one auction. Does not touch EC transactions."""
    deleted = 0
    refs = [
        (int(auction.get("channel_id", 0) or 0), int(auction.get("message_id", 0) or 0)),
        (int(auction.get("market_channel_id", 0) or 0), int(auction.get("market_message_id", 0) or 0)),
        (int(auction.get("delivery_channel_id", 0) or 0), int(auction.get("delivery_message_id", 0) or 0)),
    ]
    seen = set()
    for cid, mid in refs:
        if not cid or not mid or (cid, mid) in seen:
            continue
        seen.add((cid, mid))
        if await _delete_discord_message_ref(client, cid, mid):
            deleted += 1
    tracking_refs = list(auction.get("notify_message_refs") or [])
    await _delete_auction_tracking_dms(client, auction)
    deleted += len(tracking_refs)
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
                    else:
                        client.add_view(AuctionBidView(gid, str(aid)), message_id=int(auc.get("message_id", 0) or 0) or None)
                elif str(auc.get("status", "")) == "closed" and int(auc.get("delivery_message_id", 0) or 0):
                    client.add_view(AuctionDeliveryView(gid, str(aid)), message_id=int(auc.get("delivery_message_id", 0) or 0))
            except Exception:
                pass

    if not auction_close_loop.is_running():
        auction_close_loop.start()

    group = app_commands.Group(name="auction", description="Loot-Auktionen mit Ebolus Coins")

    @group.command(name="set_channel", description="Setzt den Kanal für neue Loot-Auktionen")
    async def auction_set_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        _gcfg(inter.guild.id)["auction_channel_id"] = int(channel.id)
        save_cfg()
        await inter.response.send_message(f"✅ Auktionskanal gesetzt: {channel.mention}", ephemeral=True)

    @group.command(name="set_log_channel", description="Setzt optional den Log-Kanal für Auktionsabschluss/Übergabe")
    async def auction_set_log_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        _gcfg(inter.guild.id)["log_channel_id"] = int(channel.id)
        save_cfg()
        await inter.response.send_message(f"✅ Auktions-Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

    @group.command(name="set_drop_notify_channel", description="Setzt den Kanal für Loot gedroppt + benachrichtigte Spieler")
    async def auction_set_drop_notify_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        _gcfg(inter.guild.id)["drop_notify_channel_id"] = int(channel.id)
        save_cfg()
        await inter.response.send_message(
            f"✅ Drop-Benachrichtigungskanal gesetzt: {channel.mention}\n"
            "Dort postet der Bot einzeln: Loot gedroppt + welche Spieler per DM benachrichtigt wurden.",
            ephemeral=True,
        )

    @group.command(name="set_market_channel", description="Setzt den öffentlichen Kanal für freie Auktionen und Sale-Käufe")
    async def auction_set_market_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None or not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        _gcfg(inter.guild.id)["market_channel_id"] = int(channel.id)
        save_cfg()
        await inter.response.send_message(f"✅ Marktplatz-Kanal gesetzt: {channel.mention}", ephemeral=True)

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
        await _delete_market_message(client, auc)
        save_auctions()
        await _refresh_auction_message(client, inter.guild.id, auc)
        ch = _log_channel(client, inter.guild.id)
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
        deleted_messages = await _purge_auction_messages(client, auc)
        auctions = _gauctions(inter.guild.id).setdefault("auctions", {})
        auctions.pop(str(auction_id), None)
        save_auctions()
        await inter.followup.send(
            f"✅ Auktion `{auction_id}` endgültig gelöscht. Gelöschte Bot-Nachrichten: **{deleted_messages}**. "
            "EC-Buchungen wurden nicht verändert.",
            ephemeral=True,
        )

    try:
        tree.add_command(group)
        print("✅ /auction Command-Gruppe registriert")
    except app_commands.CommandAlreadyRegistered:
        pass
