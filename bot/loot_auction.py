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
LOOT_ITEMS_FILE = DATA_DIR / "loot_items.json"
LOOT_NEEDS_FILE = DATA_DIR / "loot_needs.json"
MEMBER_PORTAL_CFG_FILE = DATA_DIR / "member_portal_cfg.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"

DEFAULT_DURATION_HOURS = 24
DEFAULT_MIN_INCREMENT = 5
DEFAULT_START_BID = 1

ELIGIBILITY_CHOICES = [
    app_commands.Choice(name="Automatisch: Main-Need zuerst, sonst alle Ebolus", value="auto"),
    app_commands.Choice(name="Nur Main-Need-Spieler", value="main_need"),
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


def save_auctions() -> None:
    _save_json(AUCTION_FILE, auction_state)


def save_cfg() -> None:
    _save_json(AUCTION_CFG_FILE, auction_cfg)


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
        return obj
    if isinstance(value, str):
        return {"item_id": value, "received": False}
    return {"item_id": "", "received": False}


def _main_need_user_ids(guild: discord.Guild, item_id: str) -> list[int]:
    if not item_id:
        return []
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
        main = data.get("Main") if isinstance(data, dict) else {}
        if not isinstance(main, dict):
            continue
        for slot_val in main.values():
            obj = _slot_obj(slot_val)
            if str(obj.get("item_id", "") or "") == str(item_id) and not bool(obj.get("received", False)):
                if uid not in out:
                    out.append(uid)
                break
    return out


def _eligible_user_ids(guild: discord.Guild, item_id: str, mode: str) -> tuple[str, list[int]]:
    mode = str(mode or "auto")
    main_users = _main_need_user_ids(guild, item_id)
    if mode == "main_need":
        return "main_need", main_users
    if mode == "all":
        return "all", []
    if main_users:
        return "main_need", main_users
    return "all", []


def _eligibility_text(auction: dict) -> str:
    mode = str(auction.get("eligibility_mode", "all") or "all")
    ids = [int(x) for x in auction.get("eligible_user_ids", []) or []]
    if mode == "main_need":
        if not ids:
            return "Nur Main-Need, aber aktuell keine berechtigten Spieler gefunden."
        lines = ", ".join(f"<@{uid}>" for uid in ids[:12])
        if len(ids) > 12:
            lines += f" … +{len(ids)-12}"
        return f"Nur offene Main-Need-Spieler: {lines}"
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


def _status_label(status: str) -> str:
    return {
        "active": "🟢 Aktiv",
        "closed": "🔒 Beendet",
        "delivered": "✅ Übergeben / abgebucht",
        "cancelled": "❌ Abgebrochen",
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
    emb = discord.Embed(title=f"🏷️ Loot-Auktion: {item_name}", color=color, timestamp=_now())
    desc = [
        f"Status: **{_status_label(status)}**",
        f"Auktions-ID: `{auction_id}`",
        f"Berechtigung: **{_eligibility_text(auction)}**",
    ]
    if end_dt:
        desc.append(f"Ende: **{end_dt.strftime('%d.%m.%Y %H:%M')}**")
    desc.append(f"Startgebot: **{int(auction.get('start_bid', DEFAULT_START_BID) or DEFAULT_START_BID)} EC**")
    desc.append(f"Mindestschritt: **{int(auction.get('min_increment', DEFAULT_MIN_INCREMENT) or DEFAULT_MIN_INCREMENT)} EC**")
    if top:
        desc.append(f"Höchstgebot: **{current} EC** von <@{int(top.get('user_id', 0) or 0)}>")
    else:
        desc.append("Höchstgebot: **noch keines**")
        desc.append(f"Nächstes Mindestgebot: **{min_next} EC**")
    emb.description = "\n".join(desc)

    if not compact:
        bids = auction.get("bids") if isinstance(auction.get("bids"), list) else []
        if bids:
            last = sorted(bids, key=lambda b: str(b.get("created_at", "")), reverse=True)[:8]
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
        view = AuctionBidView(int(guild_id), str(auction.get("id", ""))) if auction.get("status") == "active" else None
        await msg.edit(embed=_auction_embed(guild, auction), view=view)
    except Exception as e:
        print(f"[loot_auction] refresh failed: {e!r}")


async def _place_bid(inter: discord.Interaction, guild_id: int, auction_id: str, amount: int) -> None:
    guild = inter.guild
    if guild is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
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
    if mode == "main_need" and user_id not in eligible:
        await inter.response.send_message("❌ Diese Auktion ist aktuell nur für berechtigte Main-Need-Spieler freigegeben.", ephemeral=True)
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
    await inter.response.send_message(f"✅ Gebot gesetzt: **{int(amount)} EC** für **{auction.get('item_name','Item')}**.", ephemeral=True)


class CustomBidModal(Modal, title="Eigenes EC-Gebot"):
    amount = TextInput(label="Gebot in EC", placeholder="z. B. 50", required=True, max_length=8)

    def __init__(self, guild_id: int, auction_id: str):
        super().__init__(timeout=180)
        self.guild_id = int(guild_id)
        self.auction_id = str(auction_id)

    async def on_submit(self, inter: discord.Interaction):
        try:
            val = int(str(self.amount.value).strip())
        except Exception:
            await inter.response.send_message("❌ Bitte gib eine ganze Zahl ein.", ephemeral=True)
            return
        await _place_bid(inter, self.guild_id, self.auction_id, val)


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


async def _close_auction(client: discord.Client, guild_id: int, auction_id: str, reason: str = "time") -> bool:
    guild = client.get_guild(int(guild_id))
    auction = _auction(guild_id, auction_id)
    if not guild or not auction or str(auction.get("status", "")) != "active":
        return False
    auction["status"] = "closed"
    auction["closed_at"] = _now_iso()
    auction["close_reason"] = reason
    save_auctions()
    await _refresh_auction_message(client, guild_id, auction)
    top = _highest_bid(auction)
    ch = _log_channel(client, guild_id)
    if not ch:
        return True
    if top:
        winner = int(top.get("user_id", 0) or 0)
        amount = int(top.get("amount", 0) or 0)
        emb = discord.Embed(
            title="🏁 Loot-Auktion beendet",
            description=(
                f"**Item:** {auction.get('item_name','Item')}\n"
                f"**Gewinner:** <@{winner}>\n"
                f"**Gebot:** **{amount} EC**\n"
                f"**Auktions-ID:** `{auction_id}`\n\n"
                "Bitte Item übergeben und danach bestätigen, damit EC abgebucht werden."
            ),
            color=discord.Color.gold(),
            timestamp=_now(),
        )
        msg = await ch.send(embed=emb, view=AuctionDeliveryView(guild_id, auction_id))
        auction["delivery_message_id"] = int(msg.id)
        auction["delivery_channel_id"] = int(getattr(ch, "id", 0) or 0)
        save_auctions()
    else:
        emb = discord.Embed(
            title="🏁 Loot-Auktion beendet",
            description=f"**Item:** {auction.get('item_name','Item')}\nKeine Gebote eingegangen.\nAuktions-ID: `{auction_id}`",
            color=discord.Color.dark_grey(),
            timestamp=_now(),
        )
        await ch.send(embed=emb)
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


async def setup_loot_auction(client: discord.Client, tree: app_commands.CommandTree):
    global _client_ref
    _client_ref = client

    # Re-register persistent views for active/closed auctions after restart.
    for gid_str, g in list(auction_state.items()):
        try:
            gid = int(gid_str)
        except Exception:
            continue
        auctions = g.get("auctions") if isinstance(g.get("auctions"), dict) else {}
        for aid, auc in auctions.items():
            try:
                if str(auc.get("status", "")) == "active":
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

    @group.command(name="start", description="Startet eine EC-Loot-Auktion")
    @app_commands.choices(eligibility=ELIGIBILITY_CHOICES)
    async def auction_start(
        inter: discord.Interaction,
        item: str,
        start_bid: int = DEFAULT_START_BID,
        min_increment: int = DEFAULT_MIN_INCREMENT,
        duration_hours: int = DEFAULT_DURATION_HOURS,
        eligibility: app_commands.Choice[str] = ELIGIBILITY_CHOICES[0],
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
        mode, eligible_ids = _eligible_user_ids(guild, item_id, eligibility.value)
        if eligibility.value == "main_need" and not eligible_ids:
            await inter.response.send_message("❌ Für dieses Item gibt es aktuell keinen offenen Main-Need. Nutze eligibility = Alle Ebolus-Mitglieder.", ephemeral=True)
            return
        aid = _new_auction_id()
        ends = _now() + timedelta(hours=int(duration_hours))
        auc = {
            "id": aid,
            "guild_id": int(guild.id),
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
        await inter.response.send_message(f"✅ Auktion gestartet: `{aid}` in {ch.mention}", ephemeral=True)

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

    try:
        tree.add_command(group)
        print("✅ /auction Command-Gruppe registriert")
    except app_commands.CommandAlreadyRegistered:
        pass
