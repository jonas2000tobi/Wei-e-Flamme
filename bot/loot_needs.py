from __future__ import annotations

import json
import re
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button, Select, Modal, TextInput
from discord.enums import ButtonStyle

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ITEMS_FILE = DATA_DIR / "loot_items.json"
NEEDS_FILE = DATA_DIR / "loot_needs.json"
CFG_FILE = DATA_DIR / "loot_cfg.json"
STATE_FILE = DATA_DIR / "loot_state.json"

MEMBER_PORTAL_CFG_FILE = DATA_DIR / "member_portal_cfg.json"
MEMBER_PROFILES_FILE = DATA_DIR / "member_profiles.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"


NEED_SLOTS = [
    "Waffe 1",
    "Waffe 2",
    "Helm",
    "Brust",
    "Hose",
    "Handschuhe",
    "Schuhe",
    "Brosche",
    "Ohrringe",
    "Kette",
    "Armband",
    "Ring 1",
    "Ring 2",
    "Gürtel",
    "Umhang",
]

CATALOG_SLOTS = [
    "Waffe",
    "Helm",
    "Brust",
    "Hose",
    "Handschuhe",
    "Schuhe",
    "Brosche",
    "Ohrringe",
    "Kette",
    "Armband",
    "Ring",
    "Gürtel",
    "Umhang",
]

WEAPON_TYPES = [
    "Schwert & Schild",
    "Großschwert",
    "Dolche",
    "Armbrust",
    "Langbogen",
    "Stab",
    "Zauberstab",
    "Speer",
    "Kugel",
    "Fäustlinge",
]

TABS = ["Main", "Secondary"]
WEAPON_NEED_SLOTS = ["Waffe 1", "Waffe 2"]

GILDENBOSS_KEYWORDS = [
    "gildenboss",
    "gildenbosse",
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


loot_items: dict = _load_json(ITEMS_FILE, {})
loot_needs: dict = _load_json(NEEDS_FILE, {})
loot_cfg: dict = _load_json(CFG_FILE, {})
loot_state: dict = _load_json(STATE_FILE, {})


def save_items() -> None:
    _save_json(ITEMS_FILE, loot_items)


def save_needs() -> None:
    _save_json(NEEDS_FILE, loot_needs)


def save_cfg() -> None:
    _save_json(CFG_FILE, loot_cfg)


def save_state() -> None:
    _save_json(STATE_FILE, loot_state)


def _slug(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "item"


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _load_portal_cfg() -> dict:
    return _load_json(MEMBER_PORTAL_CFG_FILE, {})


def _load_profiles() -> dict:
    return _load_json(MEMBER_PROFILES_FILE, {})


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True

    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False

    leader_cfg = _load_leader_cfg()
    c = leader_cfg.get(str(inter.guild.id)) or {}
    role_id = int(c.get("leader_role_id", 0) or 0)

    if not role_id:
        return False

    role = inter.guild.get_role(role_id)

    return bool(role and role in inter.user.roles)


def _gitems(guild_id: int) -> dict:
    g = loot_items.get(str(guild_id)) or {}
    g.setdefault("items", {})
    loot_items[str(guild_id)] = g
    return g


def _gneeds(guild_id: int) -> dict:
    g = loot_needs.get(str(guild_id)) or {}
    g.setdefault("users", {})
    loot_needs[str(guild_id)] = g
    return g


def _gcfg(guild_id: int) -> dict:
    g = loot_cfg.get(str(guild_id)) or {}
    g.setdefault("leader_channel_id", 0)
    g.setdefault("auto_enabled", True)
    g.setdefault("title_keywords", GILDENBOSS_KEYWORDS.copy())
    loot_cfg[str(guild_id)] = g
    return g


def _gstate(guild_id: int) -> dict:
    g = loot_state.get(str(guild_id)) or {}
    g.setdefault("posted_events", {})
    loot_state[str(guild_id)] = g
    return g


def _blank_slot() -> dict:
    return {
        "item_id": "",
        "received": False,
        "received_at": "",
        "received_by": 0,
    }


def _slot_obj(value: Any) -> dict:
    if isinstance(value, dict):
        obj = dict(value)
        obj.setdefault("item_id", "")
        obj.setdefault("received", False)
        obj.setdefault("received_at", "")
        obj.setdefault("received_by", 0)
        return obj

    if isinstance(value, str):
        if not value:
            return _blank_slot()

        return {
            "item_id": value,
            "received": False,
            "received_at": "",
            "received_by": 0,
        }

    return _blank_slot()


def _slot_item_id(value: Any) -> str:
    return str(_slot_obj(value).get("item_id", "") or "")


def _slot_received(value: Any) -> bool:
    return bool(_slot_obj(value).get("received", False))


def _set_slot_item(data: dict, tab: str, slot: str, item_id: str) -> None:
    data.setdefault(tab, {})
    data[tab][slot] = {
        "item_id": str(item_id),
        "received": False,
        "received_at": "",
        "received_by": 0,
    }


def _clear_slot_item(data: dict, tab: str, slot: str) -> None:
    data.setdefault(tab, {})
    data[tab][slot] = _blank_slot()


def _mark_slot_received(data: dict, tab: str, slot: str, received_by: int) -> bool:
    data.setdefault(tab, {})
    obj = _slot_obj(data[tab].get(slot))

    if not obj.get("item_id"):
        return False

    obj["received"] = True
    obj["received_at"] = _now_iso()
    obj["received_by"] = int(received_by)
    data[tab][slot] = obj
    return True


def _unmark_slot_received(data: dict, tab: str, slot: str) -> bool:
    data.setdefault(tab, {})
    obj = _slot_obj(data[tab].get(slot))

    if not obj.get("item_id"):
        return False

    obj["received"] = False
    obj["received_at"] = ""
    obj["received_by"] = 0
    data[tab][slot] = obj
    return True


def _user_needs(guild_id: int, user_id: int) -> dict:
    g = _gneeds(guild_id)
    users = g.setdefault("users", {})
    u = users.get(str(user_id)) or {}
    u.setdefault("Main", {})
    u.setdefault("Secondary", {})

    changed = False

    for tab in TABS:
        if tab not in u or not isinstance(u[tab], dict):
            u[tab] = {}
            changed = True

        for slot in NEED_SLOTS:
            if slot not in u[tab]:
                u[tab][slot] = _blank_slot()
                changed = True
            else:
                old = u[tab][slot]
                new = _slot_obj(old)

                if old != new:
                    u[tab][slot] = new
                    changed = True

    users[str(user_id)] = u

    if changed:
        loot_needs[str(guild_id)] = g

    return u


def _normalize_catalog_slot(slot: str) -> str:
    slot = (slot or "").strip().lower()

    for s in CATALOG_SLOTS:
        if s.lower() == slot:
            return s

    compact = slot.replace(" ", "")

    aliases = {
        "waffe1": "Waffe",
        "waffe2": "Waffe",
        "weapon": "Waffe",
        "weapons": "Waffe",
        "ring1": "Ring",
        "ring2": "Ring",
    }

    return aliases.get(compact, "")


def _normalize_need_slot(slot: str) -> str:
    s = (slot or "").strip().lower()

    for x in NEED_SLOTS:
        if x.lower() == s:
            return x

    compact = s.replace(" ", "")

    aliases = {
        "waffe1": "Waffe 1",
        "waffe2": "Waffe 2",
        "ring1": "Ring 1",
        "ring2": "Ring 2",
    }

    return aliases.get(compact, "")


def _normalize_weapon_type(value: str | None) -> str:
    value = (value or "").strip().lower()

    if not value:
        return ""

    for w in WEAPON_TYPES:
        if w.lower() == value:
            return w

    aliases = {
        "sns": "Schwert & Schild",
        "schwert": "Schwert & Schild",
        "schild": "Schwert & Schild",
        "schwert und schild": "Schwert & Schild",
        "sword": "Schwert & Schild",
        "sword shield": "Schwert & Schild",
        "sword and shield": "Schwert & Schild",
        "greatsword": "Großschwert",
        "gs": "Großschwert",
        "grossschwert": "Großschwert",
        "großschwert": "Großschwert",
        "dagger": "Dolche",
        "daggers": "Dolche",
        "dolch": "Dolche",
        "dolche": "Dolche",
        "crossbow": "Armbrust",
        "xbow": "Armbrust",
        "armbrust": "Armbrust",
        "longbow": "Langbogen",
        "bow": "Langbogen",
        "bogen": "Langbogen",
        "langbogen": "Langbogen",
        "staff": "Stab",
        "stab": "Stab",
        "wand": "Zauberstab",
        "zauberstab": "Zauberstab",
        "spear": "Speer",
        "speer": "Speer",
        "orb": "Kugel",
        "kugel": "Kugel",
        "gauntlet": "Fäustlinge",
        "gauntlets": "Fäustlinge",
        "faeustlinge": "Fäustlinge",
        "fäustlinge": "Fäustlinge",
    }

    return aliases.get(value, "")


def _catalog_slot_for_need_slot(need_slot: str) -> str:
    if need_slot in ("Waffe 1", "Waffe 2"):
        return "Waffe"

    if need_slot in ("Ring 1", "Ring 2"):
        return "Ring"

    return need_slot


def _normalize_tab(tab: str) -> str:
    tab = (tab or "").strip().lower()

    if tab == "main":
        return "Main"

    if tab in {"secondary", "sec", "zweite", "zweitspec"}:
        return "Secondary"

    return ""


def _all_items(guild_id: int) -> dict:
    return _gitems(guild_id).setdefault("items", {})


def _item_weapon_type(guild_id: int, item_id: str) -> str:
    item = _all_items(guild_id).get(str(item_id)) or {}
    return str(item.get("weapon_type", "") or "").strip()


def _items_for_need_slot(guild_id: int, need_slot: str, weapon_type: str | None = None) -> list[dict]:
    catalog_slot = _catalog_slot_for_need_slot(need_slot)
    items = _all_items(guild_id)
    out = []

    legacy_slots = {catalog_slot}

    if catalog_slot == "Waffe":
        legacy_slots.update({"Waffe 1", "Waffe 2"})

    if catalog_slot == "Ring":
        legacy_slots.update({"Ring 1", "Ring 2"})

    normalized_weapon_type = _normalize_weapon_type(weapon_type) if weapon_type else ""

    for item_id, item in items.items():
        item_slot = str(item.get("slot", ""))

        if item_slot not in legacy_slots:
            continue

        if catalog_slot == "Waffe" and normalized_weapon_type:
            if str(item.get("weapon_type", "") or "") != normalized_weapon_type:
                continue

        i = dict(item)
        i["id"] = item_id
        out.append(i)

    out.sort(key=lambda x: str(x.get("name", "")).lower())
    return out


def _item_name(guild_id: int, item_id: str, with_type: bool = False) -> str:
    if not item_id:
        return "—"

    item = _all_items(guild_id).get(str(item_id))

    if not item:
        return f"Unbekanntes Item ({item_id})"

    name = str(item.get("name", item_id))
    slot = str(item.get("slot", ""))

    if with_type and slot == "Waffe":
        wt = str(item.get("weapon_type", "") or "").strip()
        if wt:
            return f"{name} ({wt})"
        return f"{name} (Unbekannt)"

    return name


def _member_role_id(guild_id: int) -> int:
    portal_cfg = _load_portal_cfg()
    c = portal_cfg.get(str(guild_id)) or {}
    return int(c.get("member_role_id", 0) or 0)


def _member_has_guild_role(guild: discord.Guild, user_id: int) -> bool:
    role_id = _member_role_id(guild.id)

    if not role_id:
        return False

    role = guild.get_role(role_id)

    if not role:
        return False

    member = guild.get_member(user_id)

    return bool(member and role in member.roles and not member.bot)


def _current_guild_role_members(guild: discord.Guild) -> list[discord.Member]:
    role_id = _member_role_id(guild.id)

    if not role_id:
        return []

    role = guild.get_role(role_id)

    if not role:
        return []

    return [m for m in role.members if not m.bot]


def _profile_name(guild: discord.Guild, user_id: int, fallback: str = "Unbekannt") -> str:
    profiles = _load_profiles()
    g = profiles.get(str(guild.id)) or {}
    users = g.get("users") or {}
    p = users.get(str(user_id)) or {}

    member = guild.get_member(user_id)
    return str(p.get("ingame_name") or (member.display_name if member else fallback))


def _cleanup_needs_without_guild_role(guild: discord.Guild) -> int:
    g = _gneeds(guild.id)
    users = g.setdefault("users", {})

    remove = []

    for uid_str in list(users.keys()):
        try:
            uid = int(uid_str)
        except Exception:
            remove.append(uid_str)
            continue

        if not _member_has_guild_role(guild, uid):
            remove.append(uid_str)

    for uid_str in remove:
        users.pop(uid_str, None)

    if remove:
        save_needs()

    return len(remove)


def _find_item_by_name(guild_id: int, catalog_slot: str, name: str) -> Optional[tuple[str, dict]]:
    name_l = name.strip().lower()

    for item_id, item in _all_items(guild_id).items():
        if str(item.get("slot", "")).lower() == catalog_slot.lower() and str(item.get("name", "")).lower() == name_l:
            return item_id, item

    return None


def _make_item_id(guild_id: int, catalog_slot: str, name: str) -> str:
    base = _slug(f"{catalog_slot}-{name}")
    item_id = base
    items = _all_items(guild_id)
    n = 2

    while item_id in items:
        item_id = f"{base}-{n}"
        n += 1

    return item_id


def _need_embed(guild: discord.Guild, user_id: int, tab: str = "Main") -> discord.Embed:
    tab = _normalize_tab(tab) or "Main"
    data = _user_needs(guild.id, user_id)

    member = guild.get_member(user_id)
    name = _profile_name(guild, user_id, member.display_name if member else "Unbekannt")

    emb = discord.Embed(
        title="🎁 Needliste – ebolus",
        description=(
            f"**{name}**\n"
            f"Bereich: **{tab}**\n\n"
            "✅ Erhaltene Items bleiben sichtbar, zählen aber nicht mehr als offener Need."
        ),
        color=discord.Color.gold()
    )

    weapon_lines = []
    armor_lines = []
    jewelry_lines = []
    other_lines = []

    for slot in NEED_SLOTS:
        slot_data = _slot_obj(data.get(tab, {}).get(slot))
        item_id = str(slot_data.get("item_id", "") or "")

        if item_id:
            item_name = _item_name(guild.id, item_id, with_type=True)
            if bool(slot_data.get("received", False)):
                item_name = f"{item_name} ✅ Erhalten"
        else:
            item_name = "—"

        line = f"**{slot}:** {item_name}"

        if slot in ("Waffe 1", "Waffe 2"):
            weapon_lines.append(line)
        elif slot in ("Helm", "Brust", "Hose", "Handschuhe", "Schuhe"):
            armor_lines.append(line)
        elif slot in ("Brosche", "Ohrringe", "Kette", "Armband", "Ring 1", "Ring 2"):
            jewelry_lines.append(line)
        else:
            other_lines.append(line)

    emb.add_field(name="⚔️ Waffen", value="\n".join(weapon_lines) or "—", inline=False)
    emb.add_field(name="🛡️ Rüstung", value="\n".join(armor_lines) or "—", inline=False)
    emb.add_field(name="💍 Schmuck", value="\n".join(jewelry_lines) or "—", inline=False)
    emb.add_field(name="🧥 Sonstiges", value="\n".join(other_lines) or "—", inline=False)

    emb.set_footer(text="Erhaltene Slots können nur durch die Gildenleitung wieder freigegeben werden.")

    return emb


def _format_need_user_full(guild: discord.Guild, user_id: int) -> str:
    data = _user_needs(guild.id, user_id)
    name = _profile_name(guild, user_id)

    lines = [f"**{name}**"]

    for tab in TABS:
        used = []

        for slot in NEED_SLOTS:
            slot_data = _slot_obj(data.get(tab, {}).get(slot))
            item_id = str(slot_data.get("item_id", "") or "")

            if item_id:
                item_name = _item_name(guild.id, item_id, with_type=True)
                if bool(slot_data.get("received", False)):
                    item_name = f"{item_name} ✅ Erhalten"

                used.append(f"{slot}: {item_name}")

        if used:
            lines.append(f"__{tab}__")
            lines.extend(f"• {x}" for x in used)

    if len(lines) == 1:
        lines.append("— keine Einträge")

    return "\n".join(lines)


def _event_participant_ids(obj: dict) -> list[int]:
    ids: list[int] = []

    yes = obj.get("yes") or {}

    for key in ("TANK", "HEAL", "DPS", "BANK"):
        for entry in yes.get(key, []) or []:
            try:
                if isinstance(entry, dict):
                    uid = int(entry.get("id", 0) or 0)
                else:
                    uid = int(entry)

                if uid and uid not in ids:
                    ids.append(uid)
            except Exception:
                continue

    return ids


def _weapon_summary_embed(
    guild: discord.Guild,
    user_ids: list[int],
    title: str,
    subtitle: str
) -> discord.Embed:
    grouped: dict[str, dict[str, dict[str, list[str]]]] = {
        "Main": {},
        "Secondary": {},
    }

    for uid in user_ids:
        if not _member_has_guild_role(guild, uid):
            continue

        data = _user_needs(guild.id, uid)
        name = _profile_name(guild, uid)

        for tab in TABS:
            for slot in WEAPON_NEED_SLOTS:
                slot_data = _slot_obj(data.get(tab, {}).get(slot))

                if bool(slot_data.get("received", False)):
                    continue

                item_id = str(slot_data.get("item_id", "") or "")

                if not item_id:
                    continue

                item_name = _item_name(guild.id, item_id, with_type=False)

                if item_name.startswith("Unbekanntes Item"):
                    continue

                weapon_type = _item_weapon_type(guild.id, item_id) or "Unbekannt"

                grouped[tab].setdefault(weapon_type, {})
                grouped[tab][weapon_type].setdefault(item_name, [])

                if name not in grouped[tab][weapon_type][item_name]:
                    grouped[tab][weapon_type][item_name].append(name)

    emb = discord.Embed(
        title=title,
        description=subtitle,
        color=discord.Color.red()
    )

    for tab in TABS:
        type_map = grouped.get(tab) or {}

        if not type_map:
            emb.add_field(name=f"{tab.upper()}-NEED", value="—", inline=False)
            continue

        lines = []

        sorted_types = sorted(
            type_map.items(),
            key=lambda kv: (WEAPON_TYPES.index(kv[0]) if kv[0] in WEAPON_TYPES else 999, kv[0])
        )

        for weapon_type, item_map in sorted_types:
            lines.append(f"**{weapon_type}**")

            sorted_items = sorted(
                item_map.items(),
                key=lambda kv: (-len(kv[1]), kv[0].lower())
            )

            for item_name, names in sorted_items:
                lines.append(f"• {item_name} — **{len(names)}x**")
                lines.append(f"  {', '.join(names)}")

            lines.append("")

        value = "\n".join(lines).strip()

        if len(value) > 1024:
            value = value[:1000] + "\n… gekürzt"

        emb.add_field(name=f"{tab.upper()}-NEED", value=value or "—", inline=False)

    emb.set_footer(text="Erhaltene Items werden nicht mehr als offener Need gezählt.")

    return emb


async def _send_embed_response(
    inter: discord.Interaction,
    embed: discord.Embed,
    public: bool = False
):
    if not inter.response.is_done():
        await inter.response.send_message(embed=embed, ephemeral=not public)
    else:
        await inter.followup.send(embed=embed, ephemeral=not public)


def _loot_leader_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    c = _gcfg(guild.id)
    ch_id = int(c.get("leader_channel_id", 0) or 0)

    if not ch_id:
        leader_cfg = _load_leader_cfg()
        lc = leader_cfg.get(str(guild.id)) or {}
        ch_id = int(lc.get("internal_channel_id", 0) or 0)

    if not ch_id:
        return None

    ch = guild.get_channel(ch_id)

    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        return ch

    return None


def _received_request_footer(guild_id: int, user_id: int, tab: str, slot: str) -> str:
    return f"loot_received_request|{int(guild_id)}|{int(user_id)}|{tab}|{slot}"


def _parse_received_request_footer(text: str) -> Optional[tuple[int, int, str, str]]:
    raw = str(text or "").strip()

    if not raw.startswith("loot_received_request|"):
        return None

    parts = raw.split("|")

    if len(parts) != 5:
        return None

    try:
        guild_id = int(parts[1])
        user_id = int(parts[2])
        tab = _normalize_tab(parts[3])
        slot = _normalize_need_slot(parts[4])

        if not tab or not slot:
            return None

        return guild_id, user_id, tab, slot

    except Exception:
        return None


async def _send_received_request(
    inter: discord.Interaction,
    guild: discord.Guild,
    user_id: int,
    tab: str,
    slot: str
) -> bool:
    ch = _loot_leader_channel(guild)

    if not ch:
        await inter.response.edit_message(
            embed=discord.Embed(
                title="❌ Leaderkanal fehlt",
                description=(
                    "Es ist kein Leader-/Loot-Kanal gesetzt.\n\n"
                    "Die Gildenleitung muss zuerst einen Kanal setzen:\n"
                    "`/loot_set_leader_channel channel:#gildenleitung`"
                ),
                color=discord.Color.red()
            ),
            view=NeedMainView(guild.id, user_id)
        )
        return False

    data = _user_needs(guild.id, user_id)
    slot_data = _slot_obj(data.get(tab, {}).get(slot))
    item_id = str(slot_data.get("item_id", "") or "")

    if not item_id:
        await inter.response.edit_message(
            embed=discord.Embed(
                title="❌ Kein Item eingetragen",
                description=f"In **{slot}** ist aktuell kein Item eingetragen.",
                color=discord.Color.orange()
            ),
            view=NeedMainView(guild.id, user_id)
        )
        return False

    if bool(slot_data.get("received", False)):
        await inter.response.edit_message(
            embed=discord.Embed(
                title="🔒 Bereits erhalten",
                description=f"**{slot}** ist bereits als erhalten markiert.",
                color=discord.Color.orange()
            ),
            view=NeedMainView(guild.id, user_id)
        )
        return False

    member = guild.get_member(user_id)
    player_name = _profile_name(guild, user_id, member.display_name if member else "Unbekannt")
    item_name = _item_name(guild.id, item_id, with_type=True)

    emb = discord.Embed(
        title="🎁 Item erhalten gemeldet",
        description=(
            f"**{player_name}** meldet ein Item als erhalten.\n\n"
            f"**Slot:** {slot}\n"
            f"**Item:** {item_name}\n\n"
            "Bitte bestätigen oder ablehnen."
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(TZ)
    )
    emb.set_footer(text=_received_request_footer(guild.id, user_id, tab, slot))

    try:
        await ch.send(embed=emb, view=ReceivedReportReviewView())
    except Exception as e:
        await inter.response.edit_message(
            embed=discord.Embed(
                title="❌ Meldung konnte nicht gesendet werden",
                description=f"Fehler: `{e}`",
                color=discord.Color.red()
            ),
            view=NeedMainView(guild.id, user_id)
        )
        return False

    await inter.response.edit_message(
        embed=discord.Embed(
            title="✅ Erhalten-Meldung gesendet",
            description=(
                f"Deine Meldung wurde an die Gildenleitung geschickt.\n\n"
                f"**{slot}:** {item_name}\n\n"
                "Sobald die Gildenleitung bestätigt, wird das Item als ✅ Erhalten markiert."
            ),
            color=discord.Color.gold()
        ),
        view=NeedMainView(guild.id, user_id)
    )
    return True


async def _send_long_need_list(
    inter: discord.Interaction,
    guild: discord.Guild,
    user_ids: list[int],
    public: bool = False
):
    chunks: list[str] = []
    current = ""

    for uid in user_ids:
        block = _format_need_user_full(guild, uid)

        if len(current) + len(block) + 2 > 3800:
            if current.strip():
                chunks.append(current.strip())
            current = block + "\n\n"
        else:
            current += block + "\n\n"

    if current.strip():
        chunks.append(current.strip())

    if not chunks:
        emb = discord.Embed(
            title="🎁 Gesamte Needliste – ebolus",
            description="Keine Needlisten gefunden.",
            color=discord.Color.gold()
        )
        await _send_embed_response(inter, emb, public=public)
        return

    first = True

    for i, chunk in enumerate(chunks, start=1):
        emb = discord.Embed(
            title="🎁 Gesamte Needliste – ebolus",
            description=chunk,
            color=discord.Color.gold()
        )
        emb.set_footer(text=f"Seite {i}/{len(chunks)}")

        if first and not inter.response.is_done():
            await inter.response.send_message(embed=emb, ephemeral=not public)
            first = False
        else:
            await inter.followup.send(embed=emb, ephemeral=not public)


async def open_need_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)

    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.")
        return

    _user_needs(guild_id, user_id)
    save_needs()

    await inter.response.edit_message(
        embed=_need_embed(guild, user_id, "Main"),
        view=NeedMainView(guild_id, user_id)
    )


class NeedMainView(View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)

    @button(label="⭐ Main", style=ButtonStyle.secondary, custom_id="need_show_main", row=0)
    async def btn_show_main(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, "Main"),
            view=NeedMainView(self.guild_id, self.user_id)
        )

    @button(label="🔁 Secondary", style=ButtonStyle.secondary, custom_id="need_show_secondary", row=0)
    async def btn_show_secondary(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, "Secondary"),
            view=NeedMainView(self.guild_id, self.user_id)
        )

    @button(label="➕ Item setzen", style=ButtonStyle.secondary, custom_id="need_add_item", row=1)
    async def btn_add_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste – Item setzen",
            description="Wähle zuerst, ob du den Eintrag für **Main** oder **Secondary** setzen möchtest.",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, action="set")
        )

    @button(label="🗑️ Item entfernen", style=ButtonStyle.secondary, custom_id="need_remove_item", row=1)
    async def btn_remove_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste – Item entfernen",
            description=(
                "Wähle zuerst, ob du den Eintrag aus **Main** oder **Secondary** entfernen möchtest.\n\n"
                "Erhaltene Slots können nur durch die Gildenleitung geändert werden."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, action="clear")
        )

    @button(label="✅ Erhalten melden", style=ButtonStyle.secondary, custom_id="need_report_received", row=2)
    async def btn_report_received(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Item erhalten melden",
            description=(
                "Wähle zuerst, ob du ein Item aus **Main** oder **Secondary** als erhalten melden möchtest.\n\n"
                "Die Meldung geht zur Gildenleitung und wird erst nach Bestätigung übernommen."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, action="report_received")
        )

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_back_portal", row=3)
    async def btn_back(self, inter: discord.Interaction, _):
        try:
            try:
                from bot.member_portal import ensure_portal_menu_for_user  # type: ignore
            except ModuleNotFoundError:
                from member_portal import ensure_portal_menu_for_user  # type: ignore

            await inter.response.defer()
            await ensure_portal_menu_for_user(inter.client, self.guild_id, self.user_id, force_view="main")
        except Exception:
            emb = discord.Embed(
                title="⚜️ Ebolus Kommandozentrale",
                description="Zurück zum Gildenmenü.",
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=emb, view=None)


class NeedTabSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.add_item(NeedTabSelect(guild_id, user_id, action))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_tab_back")
    async def btn_back(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, "Main"),
            view=NeedMainView(self.guild_id, self.user_id)
        )


class NeedTabSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action

        options = [
            discord.SelectOption(label="Main", value="Main", description="Needliste"),
            discord.SelectOption(label="Secondary", value="Secondary", description="Secondary-Needliste"),
        ]

        super().__init__(
            placeholder="Bereich wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"need_tab_select_{action}"
        )

    async def callback(self, inter: discord.Interaction):
        tab = self.values[0]
        if self.action == "set":
            action_text = "Item setzen"
        elif self.action == "clear":
            action_text = "Item entfernen"
        elif self.action == "report_received":
            action_text = "Item erhalten melden"
        else:
            action_text = "Aktion auswählen"

        emb = discord.Embed(
            title="🎁 Needliste – Slot wählen",
            description=f"Bereich: **{tab}**\nAktion: **{action_text}**",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedSlotSelectView(self.guild_id, self.user_id, self.action, tab)
        )


class NeedSlotSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, tab: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.tab = tab
        self.add_item(NeedSlotSelect(guild_id, user_id, action, tab))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_slot_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste",
            description="Wähle Main oder Secondary.",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, self.action)
        )


class NeedSlotSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, tab: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.tab = tab

        options = [
            discord.SelectOption(label=slot, value=slot)
            for slot in NEED_SLOTS
        ]

        super().__init__(
            placeholder="Slot wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"need_slot_select_{action}"
        )

    async def callback(self, inter: discord.Interaction):
        need_slot = self.values[0]
        data = _user_needs(self.guild_id, self.user_id)
        current_slot = _slot_obj(data.get(self.tab, {}).get(need_slot))

        if bool(current_slot.get("received", False)):
            guild = inter.client.get_guild(self.guild_id)
            item_name = _item_name(self.guild_id, str(current_slot.get("item_id", "") or ""), with_type=True)

            emb = discord.Embed(
                title="🔒 Slot gesperrt",
                description=(
                    f"Der Slot **{self.tab} – {need_slot}** ist bereits als erhalten markiert.\n\n"
                    f"Item: **{item_name}** ✅ Erhalten\n\n"
                    "Diesen Slot kann nur die Gildenleitung wieder freigeben."
                ),
                color=discord.Color.orange()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedMainView(self.guild_id, self.user_id)
            )
            return

        if self.action == "clear":
            _clear_slot_item(data, self.tab, need_slot)
            save_needs()

            guild = inter.client.get_guild(self.guild_id)

            if guild:
                await inter.response.edit_message(
                    embed=_need_embed(guild, self.user_id, self.tab),
                    view=NeedMainView(self.guild_id, self.user_id)
                )
            else:
                await inter.response.send_message("✅ Eintrag entfernt.")
            return

        if self.action == "report_received":
            guild = inter.client.get_guild(self.guild_id)

            if not guild:
                await inter.response.send_message("❌ Server nicht gefunden.")
                return

            item_id = str(current_slot.get("item_id", "") or "")

            if not item_id:
                await inter.response.edit_message(
                    embed=discord.Embed(
                        title="❌ Kein Item eingetragen",
                        description=f"In **{self.tab} – {need_slot}** ist aktuell kein Item eingetragen.",
                        color=discord.Color.orange()
                    ),
                    view=NeedMainView(self.guild_id, self.user_id)
                )
                return

            await _send_received_request(inter, guild, self.user_id, self.tab, need_slot)
            return

        if _catalog_slot_for_need_slot(need_slot) == "Waffe":
            emb = discord.Embed(
                title="🎁 Needliste – Waffentyp wählen",
                description=(
                    f"Bereich: **{self.tab}**\n"
                    f"Slot: **{need_slot}**\n\n"
                    "Wähle zuerst den Waffentyp."
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, self.tab, need_slot)
            )
            return

        items = _items_for_need_slot(self.guild_id, need_slot)

        if not items:
            catalog_slot = _catalog_slot_for_need_slot(need_slot)

            emb = discord.Embed(
                title="🎁 Needliste – kein Item hinterlegt",
                description=(
                    f"Für den Slot **{need_slot}** gibt es noch keine passenden Items im Katalog.\n\n"
                    f"Ein Leader muss zuerst ein Item hinzufügen:\n"
                    f"`/loot_item_add slot:{catalog_slot} name:Itemname`"
                ),
                color=discord.Color.orange()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedMainView(self.guild_id, self.user_id)
            )
            return

        emb = discord.Embed(
            title="🎁 Needliste – Item wählen",
            description=(
                f"Bereich: **{self.tab}**\n"
                f"Slot: **{need_slot}**\n"
                f"Item-Katalog: **{_catalog_slot_for_need_slot(need_slot)}**\n\n"
                f"Wähle ein Item aus dem Katalog."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedItemSelectView(self.guild_id, self.user_id, self.tab, need_slot)
        )


class NeedWeaponTypeSelectView(View):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot
        self.add_item(NeedWeaponTypeSelect(guild_id, user_id, tab, need_slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_weapon_type_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste – Slot wählen",
            description=f"Bereich: **{self.tab}**",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedSlotSelectView(self.guild_id, self.user_id, "set", self.tab)
        )


class NeedWeaponTypeSelect(Select):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot

        options = [
            discord.SelectOption(label=w, value=w)
            for w in WEAPON_TYPES
        ]

        super().__init__(
            placeholder="Waffentyp wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="need_weapon_type_select"
        )

    async def callback(self, inter: discord.Interaction):
        weapon_type = self.values[0]
        items = _items_for_need_slot(self.guild_id, self.need_slot, weapon_type=weapon_type)

        if not items:
            emb = discord.Embed(
                title="🎁 Needliste – kein Item hinterlegt",
                description=(
                    f"Für **{weapon_type}** gibt es noch keine Waffen im Katalog.\n\n"
                    f"Ein Leader muss zuerst ein Item hinzufügen:\n"
                    f"`/loot_item_add slot:Waffe weapon_type:{weapon_type} name:Itemname`"
                ),
                color=discord.Color.orange()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, self.tab, self.need_slot)
            )
            return

        emb = discord.Embed(
            title="🎁 Needliste – Waffe wählen",
            description=(
                f"Bereich: **{self.tab}**\n"
                f"Slot: **{self.need_slot}**\n"
                f"Waffentyp: **{weapon_type}**\n\n"
                "Wähle eine Waffe aus dem Katalog."
            ),
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedItemSelectView(self.guild_id, self.user_id, self.tab, self.need_slot, weapon_type=weapon_type)
        )


class NeedItemSelectView(View):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, weapon_type: str | None = None):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot
        self.weapon_type = weapon_type
        self.add_item(NeedItemSelect(guild_id, user_id, tab, need_slot, weapon_type=weapon_type))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_item_back")
    async def btn_back(self, inter: discord.Interaction, _):
        if _catalog_slot_for_need_slot(self.need_slot) == "Waffe":
            emb = discord.Embed(
                title="🎁 Needliste – Waffentyp wählen",
                description=(
                    f"Bereich: **{self.tab}**\n"
                    f"Slot: **{self.need_slot}**"
                ),
                color=discord.Color.gold()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, self.tab, self.need_slot)
            )
            return

        emb = discord.Embed(
            title="🎁 Needliste – Slot wählen",
            description=f"Bereich: **{self.tab}**",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedSlotSelectView(self.guild_id, self.user_id, "set", self.tab)
        )


class NeedItemSelect(Select):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, weapon_type: str | None = None):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot
        self.weapon_type = weapon_type

        items = _items_for_need_slot(guild_id, need_slot, weapon_type=weapon_type)[:25]

        options = [
            discord.SelectOption(
                label=str(item.get("name", item["id"]))[:100],
                value=str(item["id"])[:100],
                description=str(item.get("weapon_type", "") or item.get("slot", ""))[:100]
            )
            for item in items
        ]

        super().__init__(
            placeholder="Item wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="need_item_select"
        )

    async def callback(self, inter: discord.Interaction):
        item_id = self.values[0]
        data = _user_needs(self.guild_id, self.user_id)

        current_slot = _slot_obj(data.get(self.tab, {}).get(self.need_slot))

        if bool(current_slot.get("received", False)):
            emb = discord.Embed(
                title="🔒 Slot gesperrt",
                description="Dieser Slot ist bereits als erhalten markiert und kann nur durch die Gildenleitung geändert werden.",
                color=discord.Color.orange()
            )

            await inter.response.edit_message(
                embed=emb,
                view=NeedMainView(self.guild_id, self.user_id)
            )
            return

        _set_slot_item(data, self.tab, self.need_slot, item_id)
        save_needs()

        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("✅ Need gespeichert.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, self.tab),
            view=NeedMainView(self.guild_id, self.user_id)
        )


class ReceivedReportReviewView(View):
    def __init__(self):
        super().__init__(timeout=None)

    def _get_request_data(self, inter: discord.Interaction) -> Optional[tuple[int, int, str, str]]:
        try:
            if not inter.message or not inter.message.embeds:
                return None

            footer = inter.message.embeds[0].footer.text or ""
            return _parse_received_request_footer(footer)
        except Exception:
            return None

    @button(label="✅ Bestätigen", style=ButtonStyle.success, custom_id="loot_received_confirm")
    async def btn_confirm(self, inter: discord.Interaction, _):
        data = self._get_request_data(inter)

        if not data:
            await inter.response.send_message("❌ Diese Meldung konnte nicht gelesen werden.", ephemeral=True)
            return

        guild_id, user_id, tab, slot = data

        if inter.guild is None or inter.guild.id != guild_id:
            await inter.response.send_message("❌ Falscher Server.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        guild = inter.guild
        needs = _user_needs(guild.id, user_id)
        slot_data = _slot_obj(needs.get(tab, {}).get(slot))
        item_id = str(slot_data.get("item_id", "") or "")

        if not item_id:
            await inter.response.send_message("❌ Der Spieler hat in diesem Slot kein Item mehr eingetragen.", ephemeral=True)
            return

        item_name = _item_name(guild.id, item_id, with_type=True)

        if bool(slot_data.get("received", False)):
            await inter.response.send_message("ℹ️ Dieses Item ist bereits als erhalten markiert.", ephemeral=True)
            return

        _mark_slot_received(needs, tab, slot, inter.user.id)
        save_needs()

        member = guild.get_member(user_id)
        player_name = _profile_name(guild, user_id, member.display_name if member else "Unbekannt")

        emb = discord.Embed(
            title="✅ Item erhalten bestätigt",
            description=(
                f"**{player_name}** wurde bestätigt.\n\n"
                    f"**Slot:** {slot}\n"
                f"**Item:** {item_name} ✅ Erhalten\n\n"
                f"Bestätigt von: {inter.user.mention}"
            ),
            color=discord.Color.green(),
            timestamp=datetime.now(TZ)
        )
        emb.set_footer(text=f"Bestätigt am {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")

        try:
            if member:
                await member.send(
                    f"✅ Deine Meldung wurde bestätigt.\n"
                    f"**{slot}:** {item_name} ✅ Erhalten\n\n"
                    f"Das Item bleibt in deiner Needliste sichtbar, zählt aber nicht mehr als offener Need."
                )
        except Exception:
            pass

        await inter.response.edit_message(embed=emb, view=None)

    @button(label="❌ Ablehnen", style=ButtonStyle.danger, custom_id="loot_received_deny")
    async def btn_deny(self, inter: discord.Interaction, _):
        data = self._get_request_data(inter)

        if not data:
            await inter.response.send_message("❌ Diese Meldung konnte nicht gelesen werden.", ephemeral=True)
            return

        guild_id, user_id, tab, slot = data

        if inter.guild is None or inter.guild.id != guild_id:
            await inter.response.send_message("❌ Falscher Server.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        guild = inter.guild
        needs = _user_needs(guild.id, user_id)
        slot_data = _slot_obj(needs.get(tab, {}).get(slot))
        item_id = str(slot_data.get("item_id", "") or "")
        item_name = _item_name(guild.id, item_id, with_type=True) if item_id else "—"

        member = guild.get_member(user_id)
        player_name = _profile_name(guild, user_id, member.display_name if member else "Unbekannt")

        emb = discord.Embed(
            title="❌ Item-erhalten-Meldung abgelehnt",
            description=(
                f"Die Meldung von **{player_name}** wurde abgelehnt.\n\n"
                    f"**Slot:** {slot}\n"
                f"**Item:** {item_name}\n\n"
                f"Abgelehnt von: {inter.user.mention}"
            ),
            color=discord.Color.red(),
            timestamp=datetime.now(TZ)
        )
        emb.set_footer(text=f"Abgelehnt am {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")

        try:
            if member:
                await member.send(
                    f"❌ Deine Item-erhalten-Meldung wurde abgelehnt.\n"
                    f"**{slot}:** {item_name}\n\n"
                    f"Deine Needliste wurde nicht geändert."
                )
        except Exception:
            pass

        await inter.response.edit_message(embed=emb, view=None)


def _catalog_slot_choices():
    return [
        app_commands.Choice(name=s, value=s)
        for s in CATALOG_SLOTS
    ]


def _weapon_type_choices():
    return [
        app_commands.Choice(name=s, value=s)
        for s in WEAPON_TYPES
    ]


def _tab_choices():
    return [
        app_commands.Choice(name=s, value=s)
        for s in TABS
    ]


def _need_slot_choices():
    return [
        app_commands.Choice(name=s, value=s)
        for s in NEED_SLOTS
    ]


async def _post_auto_event_summary(client: discord.Client, msg_id: str, obj: dict) -> bool:
    try:
        guild_id = int(obj.get("guild_id", 0) or 0)

        if not guild_id:
            return False

        guild = client.get_guild(guild_id)

        if not guild:
            return False

        c = _gcfg(guild_id)
        ch_id = int(c.get("leader_channel_id", 0) or 0)

        if not ch_id:
            leader_cfg = _load_leader_cfg()
            lc = leader_cfg.get(str(guild_id)) or {}
            ch_id = int(lc.get("internal_channel_id", 0) or 0)

        if not ch_id:
            return False

        ch = guild.get_channel(ch_id)

        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return False

        user_ids = _event_participant_ids(obj)

        if not user_ids:
            emb = discord.Embed(
                title="🎯 Waffenbedarf – Gildenbosse",
                description=f"Event: **{obj.get('title', 'Event')}**\nKeine angemeldeten Teilnehmer gefunden.",
                color=discord.Color.orange()
            )
        else:
            emb = _weapon_summary_embed(
                guild,
                user_ids,
                title="🎯 Waffenbedarf – Gildenbosse",
                subtitle=(
                    f"Event: **{obj.get('title', 'Event')}**\n"
                    f"Quelle: Event-Anmeldung\n"
                    f"Teilnehmer: **{len(user_ids)}**"
                )
            )

        await ch.send(embed=emb)
        return True

    except Exception as e:
        print(f"[loot_needs] Auto-Post Fehler: {e!r}")
        return False


def _event_matches_auto(obj: dict) -> bool:
    title = str(obj.get("title", "") or "").lower()
    guild_id = int(obj.get("guild_id", 0) or 0)
    keywords = (_gcfg(guild_id).get("title_keywords") or GILDENBOSS_KEYWORDS)

    return any(str(k).lower() in title for k in keywords)


@tasks.loop(minutes=1)
async def auto_loot_need_eventstart():
    if _client_ref is None:
        return

    try:
        try:
            from bot.event_rsvp_dm import store as event_store  # type: ignore
        except ModuleNotFoundError:
            from event_rsvp_dm import store as event_store  # type: ignore

        now = datetime.now(TZ)

        for msg_id, obj in list(event_store.items()):
            try:
                guild_id = int(obj.get("guild_id", 0) or 0)

                if not guild_id:
                    continue

                c = _gcfg(guild_id)

                if not bool(c.get("auto_enabled", True)):
                    continue

                if not _event_matches_auto(obj):
                    continue

                state = _gstate(guild_id)
                posted = state.setdefault("posted_events", {})

                if str(msg_id) in posted:
                    continue

                when = datetime.fromisoformat(obj.get("when_iso"))

                if now < when:
                    continue

                ok = await _post_auto_event_summary(_client_ref, str(msg_id), obj)

                if ok:
                    posted[str(msg_id)] = _now_iso()
                    save_state()

            except Exception as e:
                print(f"[loot_needs] Auto-Loop Event Fehler: {e!r}")

    except Exception as e:
        print(f"[loot_needs] Auto-Loop Fehler: {e!r}")




# =========================
# Need-System V2: nur noch 5 Needs, kein Main/Secondary-Menü
# =========================
# Intern bleibt der Tab-Name "Main" erhalten, damit bestehende Auktionslogik
# weiter funktioniert. Für Nutzer wird nur noch "Need" angezeigt.
LEGACY_NEED_SLOTS = [
    "Waffe 1", "Waffe 2", "Helm", "Brust", "Hose", "Handschuhe", "Schuhe",
    "Brosche", "Ohrringe", "Kette", "Armband", "Ring 1", "Ring 2", "Gürtel", "Umhang",
]
NEED_SLOTS = [f"Need {i}" for i in range(1, 6)]
TABS = ["Main"]


def _normalize_tab(tab: str) -> str:
    tab_l = (tab or "").strip().lower()
    if tab_l in {"", "main", "need", "needs", "bedarf"}:
        return "Main"
    return ""


def _normalize_need_slot(slot: str) -> str:
    s = (slot or "").strip().lower()
    for x in NEED_SLOTS:
        if x.lower() == s:
            return x
    compact = s.replace(" ", "")
    if compact.startswith("need"):
        try:
            n = int(compact.replace("need", ""))
            if 1 <= n <= 5:
                return f"Need {n}"
        except Exception:
            pass
    return ""


def _tab_choices():
    return [app_commands.Choice(name="Need", value="Main")]


def _need_slot_choices():
    return [app_commands.Choice(name=s, value=s) for s in NEED_SLOTS]


def _collect_existing_need_entries(u: dict) -> list[dict]:
    """Sammelt alte Main/Secondary-Needs in stabiler Reihenfolge und gibt max. später 5 weiter."""
    entries: list[dict] = []

    def add_from(tab_name: str, slots: list[str]):
        tab = u.get(tab_name) or {}
        if not isinstance(tab, dict):
            return
        for slot in slots:
            obj = _slot_obj(tab.get(slot))
            if str(obj.get("item_id", "") or ""):
                entries.append(obj)

    # Falls die neue Struktur schon existiert, immer zuerst behalten.
    add_from("Main", NEED_SLOTS)
    # Danach alte Main-Slots, dann alte Secondary-Slots als Migration.
    add_from("Main", LEGACY_NEED_SLOTS)
    add_from("Secondary", LEGACY_NEED_SLOTS)
    add_from("Secondary", NEED_SLOTS)
    return entries


def _user_needs(guild_id: int, user_id: int) -> dict:
    g = _gneeds(guild_id)
    users = g.setdefault("users", {})
    u = users.get(str(user_id)) or {}
    if not isinstance(u, dict):
        u = {}

    # Migration: aus alten Main/Secondary-Slots werden nur noch Need 1-5.
    entries = _collect_existing_need_entries(u)
    new_main = {slot: _blank_slot() for slot in NEED_SLOTS}
    for slot, obj in zip(NEED_SLOTS, entries[:5]):
        new_main[slot] = _slot_obj(obj)

    old_main = u.get("Main") if isinstance(u.get("Main"), dict) else {}
    old_secondary = u.get("Secondary") if isinstance(u.get("Secondary"), dict) else {}
    migration_needed = (
        set((old_main or {}).keys()) != set(NEED_SLOTS)
        or any(_slot_item_id((old_main or {}).get(s)) != _slot_item_id(new_main.get(s)) for s in NEED_SLOTS)
        or any(_slot_received((old_main or {}).get(s)) != _slot_received(new_main.get(s)) for s in NEED_SLOTS)
        or bool(old_secondary)
    )

    u = {"Main": new_main}
    users[str(user_id)] = u
    loot_needs[str(guild_id)] = g
    if migration_needed:
        save_needs()
    return u


def _catalog_slot_for_need_slot(need_slot: str) -> str:
    # Die 5 Need-Plätze sind generisch. Die Kategorie wird im Menü separat gewählt.
    if need_slot in NEED_SLOTS:
        return ""
    if need_slot in ("Waffe 1", "Waffe 2"):
        return "Waffe"
    if need_slot in ("Ring 1", "Ring 2"):
        return "Ring"
    return need_slot


def _items_for_catalog_slot(guild_id: int, catalog_slot: str, weapon_type: str | None = None) -> list[dict]:
    catalog_slot = _normalize_catalog_slot(catalog_slot)
    normalized_weapon_type = _normalize_weapon_type(weapon_type) if weapon_type else ""
    out: list[dict] = []
    for item_id, item in _all_items(guild_id).items():
        item_slot = _normalize_catalog_slot(str(item.get("slot", "") or ""))
        if item_slot != catalog_slot:
            continue
        if catalog_slot == "Waffe" and normalized_weapon_type:
            if str(item.get("weapon_type", "") or "") != normalized_weapon_type:
                continue
        i = dict(item)
        i["id"] = item_id
        out.append(i)
    out.sort(key=lambda x: (str(x.get("weapon_type", "")), str(x.get("name", "")).lower()))
    return out


def _need_embed(guild: discord.Guild, user_id: int, tab: str = "Main") -> discord.Embed:
    data = _user_needs(guild.id, user_id)
    member = guild.get_member(user_id)
    name = _profile_name(guild, user_id, member.display_name if member else "Unbekannt")

    emb = discord.Embed(
        title="🎁 Needliste – ebolus",
        description=(
            f"**{name}**\n\n"
            "Du kannst maximal **5 Needs** eintragen.\n"
            "Erhaltene Items bleiben sichtbar, zählen aber nicht mehr als offener Need."
        ),
        color=discord.Color.gold()
    )

    lines = []
    for slot in NEED_SLOTS:
        slot_data = _slot_obj((data.get("Main") or {}).get(slot))
        item_id = str(slot_data.get("item_id", "") or "")
        if item_id:
            item_name = _item_name(guild.id, item_id, with_type=True)
            if bool(slot_data.get("received", False)):
                item_name = f"{item_name} ✅ Erhalten"
        else:
            item_name = "—"
        lines.append(f"**{slot}:** {item_name}")

    emb.add_field(name="⭐ Deine Needs", value="\n".join(lines) or "—", inline=False)
    emb.set_footer(text="Nur die ersten 5 vorhandenen alten Needs wurden übernommen; alles Weitere zählt nicht mehr als Need.")
    return emb


def _format_need_user_full(guild: discord.Guild, user_id: int) -> str:
    data = _user_needs(guild.id, user_id)
    name = _profile_name(guild, user_id)
    lines = [f"**{name}**"]
    used = []
    for slot in NEED_SLOTS:
        slot_data = _slot_obj((data.get("Main") or {}).get(slot))
        item_id = str(slot_data.get("item_id", "") or "")
        if item_id:
            item_name = _item_name(guild.id, item_id, with_type=True)
            if bool(slot_data.get("received", False)):
                item_name = f"{item_name} ✅ Erhalten"
            used.append(f"{slot}: {item_name}")
    if used:
        lines.extend(f"• {x}" for x in used)
    else:
        lines.append("— keine Einträge")
    return "\n".join(lines)


def _match_lines(matches: list[dict], limit: int = 30) -> str:
    if not matches:
        return "—"
    lines = []
    for m in matches[:limit]:
        rec = " ✅ Erhalten" if m.get("received") else ""
        lines.append(f"• **{m['name']}** — {m['slot']}{rec}")
    if len(matches) > limit:
        lines.append(f"… und {len(matches) - limit} weitere")
    return "\n".join(lines)


async def open_need_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.")
        return
    _user_needs(guild_id, user_id)
    save_needs()
    await inter.response.edit_message(embed=_need_embed(guild, user_id, "Main"), view=NeedMainView(guild_id, user_id))


class NeedMainView(View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)

    @button(label="➕ Need setzen", style=ButtonStyle.secondary, custom_id="need_add_item_v2", row=0)
    async def btn_add_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Need setzen",
            description="Wähle den Need-Platz, den du setzen oder überschreiben möchtest.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedSlotSelectView(self.guild_id, self.user_id, action="set", tab="Main"))

    @button(label="🗑️ Need entfernen", style=ButtonStyle.secondary, custom_id="need_remove_item_v2", row=0)
    async def btn_remove_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Need entfernen",
            description="Wähle den Need-Platz, den du entfernen möchtest. Erhaltene Needs kann nur die Gildenleitung freigeben.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedSlotSelectView(self.guild_id, self.user_id, action="clear", tab="Main"))

    @button(label="✅ Erhalten melden", style=ButtonStyle.secondary, custom_id="need_report_received_v2", row=1)
    async def btn_report_received(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Item erhalten melden",
            description="Wähle den Need, den du als erhalten melden möchtest. Die Gildenleitung muss das bestätigen.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedSlotSelectView(self.guild_id, self.user_id, action="report_received", tab="Main"))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_back_portal_v2", row=2)
    async def btn_back(self, inter: discord.Interaction, _):
        try:
            try:
                from bot.member_portal import ensure_portal_menu_for_user  # type: ignore
            except ModuleNotFoundError:
                from member_portal import ensure_portal_menu_for_user  # type: ignore
            await inter.response.defer()
            await ensure_portal_menu_for_user(inter.client, self.guild_id, self.user_id, force_view="main")
        except Exception:
            await inter.response.edit_message(embed=discord.Embed(title="⚜️ Ebolus Kommandozentrale", description="Zurück zum Gildenmenü.", color=discord.Color.gold()), view=None)


class NeedSlotSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, tab: str = "Main"):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.tab = "Main"
        self.add_item(NeedSlotSelect(guild_id, user_id, action, "Main"))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_slot_back_v2")
    async def btn_back(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return
        await inter.response.edit_message(embed=_need_embed(guild, self.user_id, "Main"), view=NeedMainView(self.guild_id, self.user_id))


class NeedSlotSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, tab: str = "Main"):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.tab = "Main"
        options = [discord.SelectOption(label=slot, value=slot) for slot in NEED_SLOTS]
        super().__init__(placeholder="Need-Platz wählen", min_values=1, max_values=1, options=options, custom_id=f"need_slot_select_v2_{action}")

    async def callback(self, inter: discord.Interaction):
        need_slot = self.values[0]
        data = _user_needs(self.guild_id, self.user_id)
        current_slot = _slot_obj((data.get("Main") or {}).get(need_slot))

        if bool(current_slot.get("received", False)):
            item_name = _item_name(self.guild_id, str(current_slot.get("item_id", "") or ""), with_type=True)
            emb = discord.Embed(
                title="🔒 Need gesperrt",
                description=f"**{need_slot}** ist bereits als erhalten markiert.\n\nItem: **{item_name}** ✅ Erhalten\n\nDiesen Platz kann nur die Gildenleitung wieder freigeben.",
                color=discord.Color.orange()
            )
            await inter.response.edit_message(embed=emb, view=NeedMainView(self.guild_id, self.user_id))
            return

        if self.action == "clear":
            _clear_slot_item(data, "Main", need_slot)
            save_needs()
            guild = inter.client.get_guild(self.guild_id)
            if guild:
                await inter.response.edit_message(embed=_need_embed(guild, self.user_id, "Main"), view=NeedMainView(self.guild_id, self.user_id))
            else:
                await inter.response.send_message("✅ Need entfernt.")
            return

        if self.action == "report_received":
            guild = inter.client.get_guild(self.guild_id)
            if not guild:
                await inter.response.send_message("❌ Server nicht gefunden.")
                return
            item_id = str(current_slot.get("item_id", "") or "")
            if not item_id:
                await inter.response.edit_message(embed=discord.Embed(title="❌ Kein Item eingetragen", description=f"In **{need_slot}** ist aktuell kein Item eingetragen.", color=discord.Color.orange()), view=NeedMainView(self.guild_id, self.user_id))
                return
            await _send_received_request(inter, guild, self.user_id, "Main", need_slot)
            return

        emb = discord.Embed(
            title="🎁 Kategorie wählen",
            description=f"Need-Platz: **{need_slot}**\n\nWähle die Item-Kategorie.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedCatalogSlotSelectView(self.guild_id, self.user_id, need_slot))


class NeedCatalogSlotSelectView(View):
    def __init__(self, guild_id: int, user_id: int, need_slot: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.need_slot = need_slot
        self.add_item(NeedCatalogSlotSelect(guild_id, user_id, need_slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_catalog_back_v2")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🎁 Need setzen", description="Wähle den Need-Platz.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=NeedSlotSelectView(self.guild_id, self.user_id, "set", "Main"))


class NeedCatalogSlotSelect(Select):
    def __init__(self, guild_id: int, user_id: int, need_slot: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.need_slot = need_slot
        options = [discord.SelectOption(label=s, value=s) for s in CATALOG_SLOTS]
        super().__init__(placeholder="Item-Kategorie wählen", min_values=1, max_values=1, options=options, custom_id="need_catalog_select_v2")

    async def callback(self, inter: discord.Interaction):
        catalog_slot = self.values[0]
        if catalog_slot == "Waffe":
            emb = discord.Embed(
                title="🎁 Waffentyp wählen",
                description=f"Need-Platz: **{self.need_slot}**\nKategorie: **Waffe**\n\nWähle den Waffentyp.",
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=emb, view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, "Main", self.need_slot, catalog_slot="Waffe"))
            return

        items = _items_for_catalog_slot(self.guild_id, catalog_slot)
        if not items:
            emb = discord.Embed(
                title="🎁 Kein Item hinterlegt",
                description=f"Für **{catalog_slot}** gibt es noch keine Items im Katalog.",
                color=discord.Color.orange()
            )
            await inter.response.edit_message(embed=emb, view=NeedCatalogSlotSelectView(self.guild_id, self.user_id, self.need_slot))
            return

        emb = discord.Embed(
            title="🎁 Item wählen",
            description=f"Need-Platz: **{self.need_slot}**\nKategorie: **{catalog_slot}**\n\nWähle ein Item aus dem Katalog.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedItemSelectView(self.guild_id, self.user_id, "Main", self.need_slot, catalog_slot=catalog_slot))


class NeedWeaponTypeSelectView(View):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, catalog_slot: str = "Waffe"):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = "Main"
        self.need_slot = need_slot
        self.catalog_slot = catalog_slot
        self.add_item(NeedWeaponTypeSelect(guild_id, user_id, "Main", need_slot, catalog_slot=catalog_slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_weapon_back_v2")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🎁 Kategorie wählen", description=f"Need-Platz: **{self.need_slot}**", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=NeedCatalogSlotSelectView(self.guild_id, self.user_id, self.need_slot))


class NeedWeaponTypeSelect(Select):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, catalog_slot: str = "Waffe"):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = "Main"
        self.need_slot = need_slot
        self.catalog_slot = catalog_slot
        options = [discord.SelectOption(label=w, value=w) for w in WEAPON_TYPES]
        super().__init__(placeholder="Waffentyp wählen", min_values=1, max_values=1, options=options, custom_id="need_weapon_type_select_v2")

    async def callback(self, inter: discord.Interaction):
        weapon_type = self.values[0]
        items = _items_for_catalog_slot(self.guild_id, "Waffe", weapon_type=weapon_type)
        if not items:
            emb = discord.Embed(title="🎁 Keine Waffe hinterlegt", description=f"Für **{weapon_type}** gibt es noch keine Waffen im Katalog.", color=discord.Color.orange())
            await inter.response.edit_message(embed=emb, view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, "Main", self.need_slot))
            return
        emb = discord.Embed(
            title="🎁 Waffe wählen",
            description=f"Need-Platz: **{self.need_slot}**\nWaffentyp: **{weapon_type}**\n\nWähle eine Waffe aus dem Katalog.",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=NeedItemSelectView(self.guild_id, self.user_id, "Main", self.need_slot, catalog_slot="Waffe", weapon_type=weapon_type))


class NeedItemSelectView(View):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, catalog_slot: str = "", weapon_type: str | None = None):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = "Main"
        self.need_slot = need_slot
        self.catalog_slot = catalog_slot
        self.weapon_type = weapon_type
        self.add_item(NeedItemSelect(guild_id, user_id, "Main", need_slot, catalog_slot=catalog_slot, weapon_type=weapon_type))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_item_back_v2")
    async def btn_back(self, inter: discord.Interaction, _):
        if self.catalog_slot == "Waffe":
            emb = discord.Embed(title="🎁 Waffentyp wählen", description=f"Need-Platz: **{self.need_slot}**", color=discord.Color.gold())
            await inter.response.edit_message(embed=emb, view=NeedWeaponTypeSelectView(self.guild_id, self.user_id, "Main", self.need_slot))
            return
        emb = discord.Embed(title="🎁 Kategorie wählen", description=f"Need-Platz: **{self.need_slot}**", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=NeedCatalogSlotSelectView(self.guild_id, self.user_id, self.need_slot))


class NeedItemSelect(Select):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str, catalog_slot: str = "", weapon_type: str | None = None):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = "Main"
        self.need_slot = need_slot
        self.catalog_slot = catalog_slot
        self.weapon_type = weapon_type
        items = _items_for_catalog_slot(guild_id, catalog_slot, weapon_type=weapon_type)[:25]
        options = [discord.SelectOption(label=str(item.get("name", item["id"]))[:100], value=str(item["id"])[:100], description=str(item.get("weapon_type", "") or item.get("slot", ""))[:100]) for item in items]
        if not options:
            options = [discord.SelectOption(label="Keine Items verfügbar", value="__none__", description="Bitte Katalog prüfen")]
        super().__init__(placeholder="Item wählen", min_values=1, max_values=1, options=options, custom_id="need_item_select_v2")

    async def callback(self, inter: discord.Interaction):
        item_id = self.values[0]
        if item_id == "__none__":
            await inter.response.send_message("❌ Keine Items verfügbar.", ephemeral=True)
            return
        data = _user_needs(self.guild_id, self.user_id)
        current_slot = _slot_obj((data.get("Main") or {}).get(self.need_slot))
        if bool(current_slot.get("received", False)):
            await inter.response.edit_message(embed=discord.Embed(title="🔒 Need gesperrt", description="Dieser Need ist bereits als erhalten markiert.", color=discord.Color.orange()), view=NeedMainView(self.guild_id, self.user_id))
            return
        _set_slot_item(data, "Main", self.need_slot, item_id)
        save_needs()
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("✅ Need gespeichert.")
            return
        await inter.response.edit_message(embed=_need_embed(guild, self.user_id, "Main"), view=NeedMainView(self.guild_id, self.user_id))

async def setup_loot_needs(client: discord.Client, tree: app_commands.CommandTree):
    global _client_ref
    _client_ref = client

    try:
        client.add_view(ReceivedReportReviewView())
    except Exception:
        pass

    if not auto_loot_need_eventstart.is_running():
        auto_loot_need_eventstart.start()
        print("🎁 Loot-Need Auto-Task gestartet.")

    @tree.command(name="loot_set_leader_channel", description="(Leader) Kanal für automatische Loot-/Need-Übersichten setzen")
    async def loot_set_leader_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild.id)
        c["leader_channel_id"] = int(channel.id)
        loot_cfg[str(inter.guild.id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Loot-/Need-Channel gesetzt: {channel.mention}",
            ephemeral=True
        )

    @tree.command(name="loot_item_add", description="(Leader) Item zum Need-Katalog hinzufügen")
    @app_commands.choices(slot=_catalog_slot_choices(), weapon_type=_weapon_type_choices())
    async def loot_item_add(
        inter: discord.Interaction,
        slot: app_commands.Choice[str],
        name: str,
        weapon_type: Optional[app_commands.Choice[str]] = None
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        catalog_slot = _normalize_catalog_slot(slot.value)

        if not catalog_slot:
            await inter.response.send_message("❌ Ungültiger Slot.", ephemeral=True)
            return

        clean_name = _safe_text(name)

        if not clean_name:
            await inter.response.send_message("❌ Itemname fehlt.", ephemeral=True)
            return

        wt = ""

        if catalog_slot == "Waffe":
            if weapon_type is None:
                await inter.response.send_message(
                    "❌ Bei Slot **Waffe** musst du zusätzlich `weapon_type` setzen.",
                    ephemeral=True
                )
                return

            wt = _normalize_weapon_type(weapon_type.value)

            if not wt:
                await inter.response.send_message("❌ Ungültiger Waffentyp.", ephemeral=True)
                return

        existing = _find_item_by_name(self.guild_id, catalog_slot, clean_name)

        if existing:
            await inter.response.send_message(
                f"⚠️ Dieses Item existiert für **{catalog_slot}** bereits.",
                ephemeral=True
            )
            return

        item_id = _make_item_id(inter.guild.id, catalog_slot, clean_name)
        items = _all_items(inter.guild.id)
        obj = {
            "name": clean_name,
            "slot": catalog_slot,
            "created_at": _now_iso(),
            "created_by": int(inter.user.id),
        }

        if catalog_slot == "Waffe":
            obj["weapon_type"] = wt

        items[item_id] = obj

        save_items()

        extra = f"\nWaffentyp: **{wt}**" if wt else ""

        await inter.response.send_message(
            f"✅ Item hinzugefügt:\n**{clean_name}**\nSlot: **{catalog_slot}**{extra}\nID: `{item_id}`",
            ephemeral=True
        )

    @tree.command(name="loot_item_set_weapon_type", description="(Leader) Waffentyp bei bestehender Waffe nachtragen/ändern")
    @app_commands.choices(weapon_type=_weapon_type_choices())
    async def loot_item_set_weapon_type(
        inter: discord.Interaction,
        name: str,
        weapon_type: app_commands.Choice[str]
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        found = _find_item_by_name(inter.guild.id, "Waffe", name)

        if not found:
            await inter.response.send_message(
                f"❌ Waffe **{name}** nicht gefunden.",
                ephemeral=True
            )
            return

        wt = _normalize_weapon_type(weapon_type.value)

        if not wt:
            await inter.response.send_message("❌ Ungültiger Waffentyp.", ephemeral=True)
            return

        item_id, item = found
        item["slot"] = "Waffe"
        item["weapon_type"] = wt
        _all_items(inter.guild.id)[item_id] = item
        save_items()

        await inter.response.send_message(
            f"✅ Waffentyp gesetzt:\n**{item.get('name')}** → **{wt}**",
            ephemeral=True
        )

    @tree.command(name="loot_item_list", description="Zeigt Items aus dem Need-Katalog")
    @app_commands.choices(slot=_catalog_slot_choices(), weapon_type=_weapon_type_choices())
    async def loot_item_list(
        inter: discord.Interaction,
        slot: Optional[app_commands.Choice[str]] = None,
        weapon_type: Optional[app_commands.Choice[str]] = None
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        catalog_slot = _normalize_catalog_slot(slot.value) if slot else ""
        wt = _normalize_weapon_type(weapon_type.value) if weapon_type else ""

        items = []

        for item_id, item in _all_items(inter.guild.id).items():
            if catalog_slot and str(item.get("slot", "")) != catalog_slot:
                continue

            if wt and str(item.get("weapon_type", "") or "") != wt:
                continue

            x = dict(item)
            x["id"] = item_id
            items.append(x)

        items.sort(key=lambda x: (str(x.get("slot", "")), str(x.get("weapon_type", "")), str(x.get("name", "")).lower()))

        if catalog_slot and wt:
            title = f"🎁 Item-Katalog – {catalog_slot} / {wt}"
        elif catalog_slot:
            title = f"🎁 Item-Katalog – {catalog_slot}"
        else:
            title = "🎁 Item-Katalog – alle Items"

        if not items:
            await inter.response.send_message("Keine Items gefunden.", ephemeral=True)
            return

        lines = []

        for item in items[:100]:
            slot_txt = str(item.get("slot", ""))
            wt_txt = str(item.get("weapon_type", "") or "")
            extra = f" / {wt_txt}" if wt_txt else ""
            lines.append(f"• **{item.get('name')}** — {slot_txt}{extra}")

        desc = "\n".join(lines)

        if len(desc) > 3900:
            desc = desc[:3800] + "\n… gekürzt"

        emb = discord.Embed(
            title=title,
            description=desc,
            color=discord.Color.gold()
        )

        emb.set_footer(text=f"Items: {len(items)}")

        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="loot_item_remove", description="(Leader) Item aus dem Need-Katalog entfernen")
    @app_commands.choices(slot=_catalog_slot_choices())
    async def loot_item_remove(inter: discord.Interaction, slot: app_commands.Choice[str], name: str):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        catalog_slot = _normalize_catalog_slot(slot.value)
        found = _find_item_by_name(inter.guild.id, catalog_slot, name)

        if not found:
            await inter.response.send_message(
                f"❌ Item **{name}** im Slot **{catalog_slot}** nicht gefunden.",
                ephemeral=True
            )
            return

        item_id, item = found
        _all_items(inter.guild.id).pop(item_id, None)

        changed_users = 0

        g = _gneeds(inter.guild.id)

        for _uid, data in (g.get("users") or {}).items():
            changed = False

            for tab in TABS:
                for s in NEED_SLOTS:
                    obj = _slot_obj(data.get(tab, {}).get(s))

                    if obj.get("item_id") == item_id:
                        data[tab][s] = _blank_slot()
                        changed = True

            if changed:
                changed_users += 1

        save_items()
        save_needs()

        await inter.response.send_message(
            f"✅ Item entfernt: **{item.get('name')}**\n"
            f"Bereinigte Spieler-Needlisten: **{changed_users}**",
            ephemeral=True
        )

    @tree.command(name="loot_mark_received", description="(Leader) Markiert einen Need-Slot als erhalten und sperrt ihn")
    @app_commands.choices(tab=_tab_choices(), slot=_need_slot_choices())
    async def loot_mark_received(
        inter: discord.Interaction,
        member: discord.Member,
        tab: app_commands.Choice[str],
        slot: app_commands.Choice[str],
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        tab_name = _normalize_tab(tab.value)
        slot_name = _normalize_need_slot(slot.value)

        if not tab_name or not slot_name:
            await inter.response.send_message("❌ Tab oder Slot ungültig.", ephemeral=True)
            return

        data = _user_needs(inter.guild.id, member.id)
        current = _slot_obj(data.get(tab_name, {}).get(slot_name))
        item_id = str(current.get("item_id", "") or "")

        if not item_id:
            await inter.response.send_message(
                f"❌ Bei **{member.display_name}** ist in **{tab_name} – {slot_name}** kein Item eingetragen.",
                ephemeral=True
            )
            return

        if bool(current.get("received", False)):
            await inter.response.send_message(
                f"ℹ️ Dieser Slot ist bereits als erhalten markiert.\n"
                f"**{member.display_name}** — **{slot_name}** — {_item_name(inter.guild.id, item_id, with_type=True)}",
                ephemeral=True
            )
            return

        _mark_slot_received(data, tab_name, slot_name, inter.user.id)
        save_needs()

        item_name = _item_name(inter.guild.id, item_id, with_type=True)

        try:
            await member.send(
                f"✅ Dein Need **{item_name}** wurde von der Gildenleitung als **erhalten** markiert.\n"
                f"Slot: **{slot_name}**\n\n"
                f"Der Slot bleibt sichtbar, zählt aber nicht mehr als offener Need."
            )
        except Exception:
            pass

        await inter.response.send_message(
            f"✅ Als erhalten markiert:\n"
            f"**{member.display_name}**\n"
            f"**{slot_name}:** {item_name} ✅ Erhalten",
            ephemeral=True
        )

    @tree.command(name="loot_unmark_received", description="(Leader) Gibt einen erhaltenen Need-Slot wieder frei")
    @app_commands.choices(tab=_tab_choices(), slot=_need_slot_choices())
    async def loot_unmark_received(
        inter: discord.Interaction,
        member: discord.Member,
        tab: app_commands.Choice[str],
        slot: app_commands.Choice[str],
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        tab_name = _normalize_tab(tab.value)
        slot_name = _normalize_need_slot(slot.value)

        if not tab_name or not slot_name:
            await inter.response.send_message("❌ Tab oder Slot ungültig.", ephemeral=True)
            return

        data = _user_needs(inter.guild.id, member.id)
        current = _slot_obj(data.get(tab_name, {}).get(slot_name))
        item_id = str(current.get("item_id", "") or "")

        if not item_id:
            await inter.response.send_message(
                f"❌ Bei **{member.display_name}** ist in **{tab_name} – {slot_name}** kein Item eingetragen.",
                ephemeral=True
            )
            return

        if not bool(current.get("received", False)):
            await inter.response.send_message("ℹ️ Dieser Slot war nicht als erhalten markiert.", ephemeral=True)
            return

        _unmark_slot_received(data, tab_name, slot_name)
        save_needs()

        item_name = _item_name(inter.guild.id, item_id, with_type=True)

        await inter.response.send_message(
            f"✅ Erhalten-Markierung entfernt:\n"
            f"**{member.display_name}**\n"
            f"**{slot_name}:** {item_name}",
            ephemeral=True
        )

    @tree.command(name="loot_reset_all_needs", description="(Leader) Setzt alle Needlisten zurück, z.B. für T4")
    async def loot_reset_all_needs(
        inter: discord.Interaction,
        confirm: str,
        only_guild_role: bool = True,
    ):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        if confirm != "RESET":
            await inter.response.send_message(
                "❌ Sicherheitswort falsch.\n"
                "Nutze: `/loot_reset_all_needs confirm:RESET`",
                ephemeral=True
            )
            return

        g = _gneeds(inter.guild.id)
        users = g.setdefault("users", {})

        if only_guild_role:
            allowed_ids = {str(m.id) for m in _current_guild_role_members(inter.guild)}
        else:
            allowed_ids = set(users.keys())

        affected = 0

        for uid_str, data in list(users.items()):
            if uid_str not in allowed_ids:
                continue

            changed = False

            for tab in TABS:
                data.setdefault(tab, {})
                for slot in NEED_SLOTS:
                    if _slot_item_id(data.get(tab, {}).get(slot)):
                        data[tab][slot] = _blank_slot()
                        changed = True
                    else:
                        data[tab][slot] = _blank_slot()

            if changed:
                affected += 1

        save_needs()

        await inter.response.send_message(
            f"✅ Alle Needlisten wurden zurückgesetzt.\n"
            f"Betroffene Spieler: **{affected}**\n"
            f"Scope: **{'Nur aktuelle Ebolus-/Gildenrolle' if only_guild_role else 'Alle gespeicherten Spieler'}**",
            ephemeral=True
        )

    @tree.command(name="loot_cleanup", description="(Leader) Entfernt Needlisten von Leuten ohne Ebolus-/Gildenmitglied-Rolle")
    async def loot_cleanup(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        removed = _cleanup_needs_without_guild_role(inter.guild)

        await inter.response.send_message(
            f"✅ Loot-Need-Cleanup abgeschlossen.\nEntfernte Needlisten: **{removed}**",
            ephemeral=True
        )

    @tree.command(name="loot_need_all", description="(Leader) Gesamte Needliste aller aktuellen Ebolus-Mitglieder anzeigen")
    async def loot_need_all(inter: discord.Interaction, public: bool = False):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        removed = _cleanup_needs_without_guild_role(inter.guild)

        g = _gneeds(inter.guild.id)
        users = g.get("users") or {}

        role_members = _current_guild_role_members(inter.guild)
        role_ids = {m.id for m in role_members}

        user_ids = []

        for uid_str in users.keys():
            try:
                uid = int(uid_str)
            except Exception:
                continue

            if uid in role_ids:
                user_ids.append(uid)

        user_ids.sort(key=lambda uid: _profile_name(inter.guild, uid).lower())

        await _send_long_need_list(inter, inter.guild, user_ids, public=public)

        if removed:
            await inter.followup.send(
                f"🧹 Bereinigt: **{removed}** Needliste(n) von Leuten ohne Ebolus-/Gildenmitglied-Rolle entfernt.",
                ephemeral=True
            )

    @tree.command(name="loot_weapon_all", description="(Leader) Waffenbedarf aller aktuellen Ebolus-Mitglieder zusammenfassen")
    async def loot_weapon_all(inter: discord.Interaction, public: bool = False):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        removed = _cleanup_needs_without_guild_role(inter.guild)
        members = _current_guild_role_members(inter.guild)
        user_ids = [m.id for m in members]

        emb = _weapon_summary_embed(
            inter.guild,
            user_ids,
            title="🎯 Waffenbedarf – alle Ebolus-Mitglieder",
            subtitle=f"Quelle: alle Mitglieder mit Ebolus-/Gildenmitglied-Rolle\nMitglieder: **{len(user_ids)}**"
        )

        await _send_embed_response(inter, emb, public=public)

        if removed:
            await inter.followup.send(
                f"🧹 Bereinigt: **{removed}** Needliste(n) von Leuten ohne Ebolus-/Gildenmitglied-Rolle entfernt.",
                ephemeral=True
            )

    @tree.command(name="loot_need_event", description="(Leader) Waffenbedarf aus Raid-/Event-Anmeldung anzeigen")
    async def loot_need_event(inter: discord.Interaction, message_id: str, public: bool = False):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        try:
            try:
                from bot.event_rsvp_dm import store as event_store  # type: ignore
            except ModuleNotFoundError:
                from event_rsvp_dm import store as event_store  # type: ignore
        except Exception:
            await inter.response.send_message("❌ Event-System nicht geladen.", ephemeral=True)
            return

        obj = event_store.get(str(message_id))

        if not obj or int(obj.get("guild_id", 0) or 0) != inter.guild.id:
            await inter.response.send_message("❌ Event nicht gefunden oder falscher Server.", ephemeral=True)
            return

        user_ids = _event_participant_ids(obj)

        emb = _weapon_summary_embed(
            inter.guild,
            user_ids,
            title="🎯 Waffenbedarf – Event-Anmeldung",
            subtitle=(
                f"Event: **{obj.get('title', 'Event')}**\n"
                f"Quelle: Tank/Heal/DPS/Bank aus Raid-Anmeldung\n"
                f"Teilnehmer: **{len(user_ids)}**"
            )
        )

        await _send_embed_response(inter, emb, public=public)

    @tree.command(name="loot_auto_status", description="(Leader) Zeigt Auto-Loot-Need-Status")
    async def loot_auto_status(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild.id)
        ch_id = int(c.get("leader_channel_id", 0) or 0)
        keywords = c.get("title_keywords") or GILDENBOSS_KEYWORDS
        state = _gstate(inter.guild.id)
        posted = state.get("posted_events") or {}

        emb = discord.Embed(
            title="🎁 Loot-Need Auto-Status",
            color=discord.Color.gold()
        )

        emb.add_field(name="Aktiv", value="Ja" if c.get("auto_enabled", True) else "Nein", inline=True)
        emb.add_field(name="Channel", value=f"<#{ch_id}>" if ch_id else "Nicht gesetzt / Fallback Leader-Internal", inline=True)
        emb.add_field(name="Keywords", value=", ".join(str(x) for x in keywords), inline=False)
        emb.add_field(name="Bereits automatisch gepostet", value=str(len(posted)), inline=True)

        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="loot_auto_toggle", description="(Leader) Automatische Waffenübersicht bei Gildenboss-Eventstart an/aus")
    async def loot_auto_toggle(inter: discord.Interaction, enabled: bool):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild.id)
        c["auto_enabled"] = bool(enabled)
        loot_cfg[str(inter.guild.id)] = c
        save_cfg()

        await inter.response.send_message(
            f"✅ Auto-Waffenübersicht ist jetzt: **{'AN' if enabled else 'AUS'}**",
            ephemeral=True
        )

# ===== ADMIN PORTAL LOOT MENU EXTENSIONS =====


def _is_loot_admin_user(client: discord.Client, guild_id: int, user_id: int) -> bool:
    guild = client.get_guild(int(guild_id))
    if not guild:
        return False
    member = guild.get_member(int(user_id))
    if not member or member.bot:
        return False
    perms = getattr(member, "guild_permissions", None)
    if perms and (perms.administrator or perms.manage_guild):
        return True
    try:
        leader_cfg = _load_leader_cfg()
        lc = leader_cfg.get(str(guild_id)) or {}
        role_id = int(lc.get("leader_role_id", 0) or 0)
        role = guild.get_role(role_id) if role_id else None
        if role and role in member.roles:
            return True
    except Exception:
        pass
    try:
        portal_cfg = _load_portal_cfg()
        pc = portal_cfg.get(str(guild_id)) or {}
        roles = pc.get("position_roles") or {}
        for key in ("leader", "advisor", "guardian"):
            role_id = int(roles.get(key, 0) or 0)
            role = guild.get_role(role_id) if role_id else None
            if role and role in member.roles:
                return True
    except Exception:
        pass
    return False

def _need_matches_for_item(guild: discord.Guild, item_id: str, tab_filter: str | None = None, received: bool | None = None) -> list[dict]:
    """Findet Spieler/Slots, die ein bestimmtes Item in der Needliste haben."""
    out: list[dict] = []
    g = _gneeds(guild.id)
    users = g.get("users") or {}

    for uid_str, data in list(users.items()):
        try:
            uid = int(uid_str)
        except Exception:
            continue

        if not _member_has_guild_role(guild, uid):
            continue

        member = guild.get_member(uid)
        name = _profile_name(guild, uid, member.display_name if member else f"User {uid}")

        tabs = [tab_filter] if tab_filter else TABS
        for tab in tabs:
            if tab not in TABS:
                continue
            for slot in NEED_SLOTS:
                slot_data = _slot_obj((data.get(tab) or {}).get(slot))
                if str(slot_data.get("item_id", "") or "") != str(item_id):
                    continue
                is_received = bool(slot_data.get("received", False))
                if received is not None and is_received != bool(received):
                    continue
                out.append({
                    "user_id": uid,
                    "name": name,
                    "tab": tab,
                    "slot": slot,
                    "received": is_received,
                })

    out.sort(key=lambda x: (str(x.get("tab")), str(x.get("slot")), str(x.get("name", "")).lower()))
    return out


def _match_lines(matches: list[dict], limit: int = 30) -> str:
    if not matches:
        return "—"
    lines = []
    for m in matches[:limit]:
        rec = " ✅ Erhalten" if m.get("received") else ""
        lines.append(f"• **{m['name']}** — {m['tab']} — {m['slot']}{rec}")
    if len(matches) > limit:
        lines.append(f"… und {len(matches) - limit} weitere")
    return "\n".join(lines)


async def _notify_main_need_drop(inter: discord.Interaction, guild: discord.Guild, item_id: str) -> None:
    """Loot-drop confirmation.

    New flow: a confirmed drop no longer only sends a simple DM. It creates a virtual
    guild chest item and starts the EC auction chain:
    Need-Auktion 48h -> Freie Auktion 24h -> Sale-Kauf.
    """
    item_name = _item_name(guild.id, item_id, with_type=True)
    matches = _need_matches_for_item(guild, item_id, tab_filter="Main", received=False)

    try:
        try:
            from bot import loot_auction as auction_mod  # type: ignore
        except Exception:
            import loot_auction as auction_mod  # type: ignore

        if hasattr(auction_mod, "start_loot_drop_auction"):
            result = await auction_mod.start_loot_drop_auction(inter, guild, item_id, actor_id=int(inter.user.id))  # type: ignore[attr-defined]
            phase = str(result.get("phase", ""))
            auction_id = str(result.get("auction_id", ""))
            notified = int(result.get("notified", 0) or 0)
            failed = int(result.get("failed", 0) or 0)
            title = "🎯 Need-Auktion gestartet" if phase == "need" else "⚖️ Freie Auktion gestartet"
            desc = (
                f"**Item:** {item_name}\n\n"
                f"**Auktions-ID:** `{auction_id}`\n"
                f"**Ablauf:** {'Need-Auktion 48h → Freie Auktion 24h → Sale-Kauf' if phase == 'need' else 'Freie Auktion 24h → Sale-Kauf'}\n\n"
                f"**Need-Treffer:**\n{_match_lines(matches)}\n\n"
            )
            if phase == "need":
                desc += f"Benachrichtigt: **{notified}**\nNicht erreichbar: **{failed}**"
            else:
                desc += "Keine offenen Needs gefunden. Das Item ist direkt in der freien Auktion."
            emb = discord.Embed(title=title, description=desc, color=discord.Color.green(), timestamp=datetime.now(TZ))
            if not inter.response.is_done():
                await inter.response.send_message(embed=emb, ephemeral=True)
            else:
                await inter.followup.send(embed=emb, ephemeral=True)
            return
    except Exception as e:
        # Fallback to the old notification flow if the auction module is missing/broken.
        try:
            await inter.followup.send(f"⚠️ Auktionssystem konnte nicht gestartet werden: `{e}`. Nutze vorübergehend die alte Benachrichtigung.", ephemeral=True)
        except Exception:
            pass

    notified = 0
    failed = 0
    for m in matches:
        member = guild.get_member(int(m["user_id"]))
        if not member or member.bot:
            failed += 1
            continue
        try:
            await member.send(
                "🎁 **Dein Need-Item ist gedroppt!**\n\n"
                f"**Item:** {item_name}\n"
                f"**Slot:** {m['slot']}\n\n"
                "Bitte melde dich bei der Gildenleitung, wenn du das Item kaufen/beanspruchen möchtest."
            )
            notified += 1
            await asyncio.sleep(0.08)
        except Exception:
            failed += 1

    emb = discord.Embed(
        title="📦 Loot gedroppt",
        description=(
            f"**Item:** {item_name}\n\n"
            "**Benachrichtigt wurden nur offene Needs.**\n\n"
            f"**Need:**\n{_match_lines(matches)}\n\n"
            f"Benachrichtigt: **{notified}**\n"
            f"Nicht erreichbar: **{failed}**"
        ),
        color=discord.Color.gold(),
        timestamp=datetime.now(TZ)
    )
    emb.set_footer(text=f"Ausgelöst von {getattr(inter.user, 'display_name', inter.user.name)}")

    ch = _loot_leader_channel(guild)
    if ch:
        try:
            await ch.send(embed=emb)
        except Exception:
            pass

    if not inter.response.is_done():
        await inter.response.send_message(embed=emb, ephemeral=True)
    else:
        await inter.followup.send(embed=emb, ephemeral=True)


async def open_admin_item_add_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    if inter.guild is not None and not _is_leader_or_admin(inter):
        await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
        return
    emb = discord.Embed(
        title="➕ Item hinzufügen",
        description="Wähle zuerst den Slot. Bei Waffen wird danach der Waffentyp abgefragt.",
        color=discord.Color.gold()
    )
    await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(guild_id, user_id, action="add"))


async def open_admin_loot_drop_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    if inter.guild is not None and not _is_leader_or_admin(inter):
        await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
        return
    emb = discord.Embed(
        title="📦 Loot gedroppt",
        description="Wähle den Slot und danach das Item aus dem Katalog. Danach werden nur Spieler mit offenem **Need** benachrichtigt.",
        color=discord.Color.gold()
    )
    await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(guild_id, user_id, action="drop"))


async def open_admin_mark_received_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    if inter.guild is not None and not _is_leader_or_admin(inter):
        await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
        return
    emb = discord.Embed(
        title="✅ Item erhalten markieren",
        description="Wähle Slot → Item → Spieler. Danach wird dieser Need-Slot als erhalten markiert.",
        color=discord.Color.gold()
    )
    await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(guild_id, user_id, action="mark"))


async def open_admin_unmark_received_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    if inter.guild is not None and not _is_leader_or_admin(inter):
        await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
        return
    emb = discord.Embed(
        title="❌ Erhalten-Markierung entfernen",
        description="Wähle Slot → Item → Spieler. Danach wird die Erhalten-Markierung wieder freigegeben.",
        color=discord.Color.gold()
    )
    await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(guild_id, user_id, action="unmark"))


async def open_admin_item_catalog_menu(inter: discord.Interaction, guild_id: int, user_id: int):
    guild = inter.client.get_guild(guild_id)
    if not guild:
        await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
        return
    emb = discord.Embed(
        title="📋 Item-Katalog",
        description="Wähle einen Slot, um die Items aus dem Katalog anzuzeigen.",
        color=discord.Color.gold()
    )
    await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(guild_id, user_id, action="catalog"))


class AdminLootBackView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_back_to_portal")
    async def btn_back(self, inter: discord.Interaction, _):
        try:
            try:
                from bot.member_portal import AdminLootMenuView  # type: ignore
            except ModuleNotFoundError:
                from member_portal import AdminLootMenuView  # type: ignore
            emb = discord.Embed(
                title="🎁 Admin – Loot",
                description="Wähle eine Loot-Aktion.",
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=emb, view=AdminLootMenuView())
        except Exception:
            await inter.response.send_message("✅ Aktion abgeschlossen.", ephemeral=True)


class AdminLootSlotSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.add_item(AdminLootSlotSelect(guild_id, user_id, action))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_slot_back")
    async def btn_back(self, inter: discord.Interaction, _):
        try:
            try:
                from bot.member_portal import AdminLootMenuView  # type: ignore
            except ModuleNotFoundError:
                from member_portal import AdminLootMenuView  # type: ignore
            emb = discord.Embed(title="🎁 Admin – Loot", description="Wähle eine Loot-Aktion.", color=discord.Color.gold())
            await inter.response.edit_message(embed=emb, view=AdminLootMenuView())
        except Exception:
            await inter.response.send_message("Zurück.", ephemeral=True)


class AdminLootSlotSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        options = [discord.SelectOption(label=s, value=s) for s in CATALOG_SLOTS]
        super().__init__(placeholder="Slot wählen", min_values=1, max_values=1, options=options, custom_id=f"admin_loot_slot_{action}")

    async def callback(self, inter: discord.Interaction):
        slot = self.values[0]
        if self.action == "add":
            if slot == "Waffe":
                emb = discord.Embed(title="➕ Item hinzufügen – Waffentyp", description="Wähle den Waffentyp.", color=discord.Color.gold())
                await inter.response.edit_message(embed=emb, view=AdminLootWeaponTypeSelectView(self.guild_id, self.user_id, slot))
                return
            await inter.response.send_modal(AdminItemAddModal(self.guild_id, self.user_id, slot, ""))
            return

        if slot == "Waffe" and self.action in {"drop", "mark", "unmark"}:
            emb = discord.Embed(
                title="🎁 Waffentyp wählen",
                description=(
                    f"Aktion: **{self.action}**\n"
                    "Wähle zuerst den Waffentyp, damit auch bei mehr als 25 Waffen alle Items erreichbar bleiben."
                ),
                color=discord.Color.gold()
            )
            await inter.response.edit_message(
                embed=emb,
                view=AdminLootActionWeaponTypeSelectView(self.guild_id, self.user_id, self.action, slot)
            )
            return

        if self.action == "catalog":
            items = [dict(v, id=k) for k, v in _all_items(self.guild_id).items() if str(v.get("slot", "")) == slot]
            items.sort(key=lambda x: (str(x.get("weapon_type", "")), str(x.get("name", "")).lower()))
            lines = []
            for item in items[:80]:
                wt = str(item.get("weapon_type", "") or "")
                extra = f" — {wt}" if wt else ""
                lines.append(f"• **{item.get('name')}**{extra}")
            desc = "\n".join(lines) if lines else "Keine Items in diesem Slot."
            if len(desc) > 3900:
                desc = desc[:3800] + "\n… gekürzt"
            emb = discord.Embed(title=f"📋 Item-Katalog – {slot}", description=desc, color=discord.Color.gold())
            emb.set_footer(text=f"Items: {len(items)}")
            await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(self.guild_id, self.user_id, "catalog"))
            return

        items = _items_for_need_slot(self.guild_id, slot)
        if not items:
            emb = discord.Embed(
                title="❌ Kein Item im Katalog",
                description=f"Für **{slot}** sind noch keine Items hinterlegt. Füge zuerst ein Item hinzu.",
                color=discord.Color.orange()
            )
            await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(self.guild_id, self.user_id, self.action))
            return

        emb = discord.Embed(
            title="🎁 Item wählen",
            description=f"Slot: **{slot}**",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(embed=emb, view=AdminLootItemSelectView(self.guild_id, self.user_id, self.action, slot))


class AdminLootWeaponTypeSelectView(View):
    def __init__(self, guild_id: int, user_id: int, slot: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.slot = slot
        self.add_item(AdminLootWeaponTypeSelect(guild_id, user_id, slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_weapon_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="➕ Item hinzufügen", description="Wähle den Slot.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(self.guild_id, self.user_id, "add"))


class AdminLootWeaponTypeSelect(Select):
    def __init__(self, guild_id: int, user_id: int, slot: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.slot = slot
        options = [discord.SelectOption(label=w, value=w) for w in WEAPON_TYPES]
        super().__init__(placeholder="Waffentyp wählen", min_values=1, max_values=1, options=options, custom_id="admin_loot_weapon_type")

    async def callback(self, inter: discord.Interaction):
        await inter.response.send_modal(AdminItemAddModal(self.guild_id, self.user_id, self.slot, self.values[0]))


class AdminLootActionWeaponTypeSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, slot: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.slot = slot
        self.add_item(AdminLootActionWeaponTypeSelect(guild_id, user_id, action, slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_action_weapon_back")
    async def btn_back(self, inter: discord.Interaction, _):
        if self.slot == "Waffe" and self.weapon_type:
            emb = discord.Embed(title="🎁 Waffentyp wählen", description="Wähle den Waffentyp.", color=discord.Color.gold())
            await inter.response.edit_message(embed=emb, view=AdminLootActionWeaponTypeSelectView(self.guild_id, self.user_id, self.action, self.slot))
            return
        emb = discord.Embed(title="🎁 Slot wählen", description="Wähle den Slot.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(self.guild_id, self.user_id, self.action))


class AdminLootActionWeaponTypeSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, slot: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.slot = slot
        options = [discord.SelectOption(label=w, value=w) for w in WEAPON_TYPES]
        super().__init__(
            placeholder="Waffentyp wählen",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"admin_loot_action_weapon_type_{action}"
        )

    async def callback(self, inter: discord.Interaction):
        weapon_type = self.values[0]
        items = _items_for_need_slot(self.guild_id, self.slot, weapon_type=weapon_type)
        if not items:
            emb = discord.Embed(
                title="❌ Kein Item im Katalog",
                description=f"Für **{weapon_type}** sind noch keine Waffen hinterlegt.",
                color=discord.Color.orange()
            )
            await inter.response.edit_message(
                embed=emb,
                view=AdminLootActionWeaponTypeSelectView(self.guild_id, self.user_id, self.action, self.slot)
            )
            return

        emb = discord.Embed(
            title="🎁 Item wählen",
            description=f"Slot: **{self.slot}**\nWaffentyp: **{weapon_type}**",
            color=discord.Color.gold()
        )
        await inter.response.edit_message(
            embed=emb,
            view=AdminLootItemSelectView(self.guild_id, self.user_id, self.action, self.slot, weapon_type=weapon_type)
        )


class AdminItemAddModal(Modal):
    def __init__(self, guild_id: int, user_id: int, slot: str, weapon_type: str):
        super().__init__(title="Item hinzufügen", timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.slot = slot
        self.weapon_type = weapon_type
        self.name = TextInput(label="Itemname im Bot", placeholder="z. B. Hut Sprosses", required=True, max_length=100)
        self.add_item(self.name)

    async def on_submit(self, inter: discord.Interaction):
        if not _is_loot_admin_user(inter.client, self.guild_id, inter.user.id):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        catalog_slot = _normalize_catalog_slot(self.slot)
        clean_name = _safe_text(str(self.name.value or ""))
        if not catalog_slot or not clean_name:
            await inter.response.send_message("❌ Slot oder Itemname ungültig.", ephemeral=True)
            return
        wt = _normalize_weapon_type(self.weapon_type) if catalog_slot == "Waffe" else ""
        if catalog_slot == "Waffe" and not wt:
            await inter.response.send_message("❌ Waffentyp fehlt.", ephemeral=True)
            return
        existing = _find_item_by_name(self.guild_id, catalog_slot, clean_name)
        if existing:
            await inter.response.send_message(f"⚠️ Dieses Item existiert bereits: **{clean_name}**", ephemeral=True)
            return
        item_id = _make_item_id(self.guild_id, catalog_slot, clean_name)
        obj = {"name": clean_name, "slot": catalog_slot, "created_at": _now_iso(), "created_by": int(inter.user.id)}
        if wt:
            obj["weapon_type"] = wt
        _all_items(self.guild_id)[item_id] = obj
        save_items()
        extra = f"\nWaffentyp: **{wt}**" if wt else ""
        emb = discord.Embed(
            title="✅ Item hinzugefügt",
            description=f"**{clean_name}**\nSlot: **{catalog_slot}**{extra}\nID: `{item_id}`",
            color=discord.Color.green()
        )
        await inter.response.send_message(embed=emb, ephemeral=True)


class AdminLootItemSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, slot: str, weapon_type: str | None = None):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.slot = slot
        self.weapon_type = weapon_type
        self.add_item(AdminLootItemSelect(guild_id, user_id, action, slot, weapon_type=weapon_type))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_item_back")
    async def btn_back(self, inter: discord.Interaction, _):
        if self.slot == "Waffe" and self.weapon_type:
            emb = discord.Embed(title="🎁 Waffentyp wählen", description="Wähle den Waffentyp.", color=discord.Color.gold())
            await inter.response.edit_message(embed=emb, view=AdminLootActionWeaponTypeSelectView(self.guild_id, self.user_id, self.action, self.slot))
            return
        emb = discord.Embed(title="🎁 Slot wählen", description="Wähle den Slot.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminLootSlotSelectView(self.guild_id, self.user_id, self.action))


class AdminLootItemSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, slot: str, weapon_type: str | None = None):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.slot = slot
        self.weapon_type = weapon_type
        items = _items_for_need_slot(guild_id, slot, weapon_type=weapon_type)[:25]
        options = [
            discord.SelectOption(
                label=str(item.get("name", item["id"]))[:100],
                value=str(item["id"])[:100],
                description=str(item.get("weapon_type", "") or item.get("slot", ""))[:100]
            )
            for item in items
        ]
        super().__init__(placeholder="Item wählen", min_values=1, max_values=1, options=options, custom_id=f"admin_loot_item_{action}")

    async def callback(self, inter: discord.Interaction):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        item_id = self.values[0]
        item_name = _item_name(self.guild_id, item_id, with_type=True)
        if self.action == "drop":
            matches = _need_matches_for_item(guild, item_id, tab_filter="Main", received=False)
            emb = discord.Embed(
                title="📦 Loot gedroppt – Vorschau",
                description=(
                    f"**Item:** {item_name}\n\n"
                    "Benachrichtigt werden nur Spieler mit offenem **Need**.\n\n"
                    f"**Treffer:**\n{_match_lines(matches)}"
                ),
                color=discord.Color.gold()
            )
            await inter.response.edit_message(embed=emb, view=AdminLootDropConfirmView(self.guild_id, self.user_id, item_id))
            return

        if self.action in {"mark", "unmark"}:
            matches = _need_matches_for_item(guild, item_id, tab_filter=None, received=(self.action == "unmark"))
            if not matches:
                txt = "offenen" if self.action == "mark" else "als erhalten markierten"
                emb = discord.Embed(title="Keine Treffer", description=f"Keine {txt} Need-Einträge für **{item_name}** gefunden.", color=discord.Color.orange())
                await inter.response.edit_message(embed=emb, view=AdminLootBackView())
                return
            emb = discord.Embed(title="👤 Spieler wählen", description=f"Item: **{item_name}**", color=discord.Color.gold())
            await inter.response.edit_message(embed=emb, view=AdminLootUserSelectView(self.guild_id, self.user_id, self.action, item_id, matches))


class AdminLootDropConfirmView(View):
    def __init__(self, guild_id: int, user_id: int, item_id: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.item_id = str(item_id)

    @button(label="✅ Benachrichtigen", style=ButtonStyle.success, custom_id="admin_loot_drop_confirm")
    async def btn_confirm(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        if inter.guild is not None and not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        await _notify_main_need_drop(inter, guild, self.item_id)

    @button(label="❌ Abbrechen", style=ButtonStyle.danger, custom_id="admin_loot_drop_cancel")
    async def btn_cancel(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="Abgebrochen", description="Es wurden keine Spieler benachrichtigt.", color=discord.Color.orange())
        await inter.response.edit_message(embed=emb, view=AdminLootBackView())


class AdminLootUserSelectView(View):
    def __init__(self, guild_id: int, user_id: int, action: str, item_id: str, matches: list[dict]):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.item_id = str(item_id)
        self.matches = matches[:25]
        self.add_item(AdminLootUserSelect(guild_id, user_id, action, item_id, self.matches))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="admin_loot_user_back")
    async def btn_back(self, inter: discord.Interaction, _):
        emb = discord.Embed(title="🎁 Item wählen", description="Wähle erneut ein Item.", color=discord.Color.gold())
        await inter.response.edit_message(embed=emb, view=AdminLootItemSelectView(self.guild_id, self.user_id, self.action, _catalog_slot_for_need_slot(self.matches[0]['slot']) if self.matches else "Waffe"))


class AdminLootUserSelect(Select):
    def __init__(self, guild_id: int, user_id: int, action: str, item_id: str, matches: list[dict]):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.action = action
        self.item_id = str(item_id)
        options = []
        for m in matches[:25]:
            value = f"{m['user_id']}|{m['tab']}|{m['slot']}"
            options.append(discord.SelectOption(label=str(m['name'])[:100], value=value[:100], description=f"{m['tab']} – {m['slot']}"[:100]))
        super().__init__(placeholder="Spieler wählen", min_values=1, max_values=1, options=options, custom_id=f"admin_loot_user_{action}")

    async def callback(self, inter: discord.Interaction):
        guild = inter.client.get_guild(self.guild_id)
        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.", ephemeral=True)
            return
        if inter.guild is not None and not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        try:
            uid_s, tab, slot = self.values[0].split("|", 2)
            uid = int(uid_s)
        except Exception:
            await inter.response.send_message("❌ Auswahl konnte nicht gelesen werden.", ephemeral=True)
            return
        data = _user_needs(guild.id, uid)
        slot_data = _slot_obj((data.get(tab) or {}).get(slot))
        if str(slot_data.get("item_id", "") or "") != self.item_id:
            await inter.response.send_message("❌ Der Need-Eintrag hat sich inzwischen geändert.", ephemeral=True)
            return
        item_name = _item_name(guild.id, self.item_id, with_type=True)
        member = guild.get_member(uid)
        name = _profile_name(guild, uid, member.display_name if member else f"User {uid}")
        if self.action == "mark":
            if bool(slot_data.get("received", False)):
                await inter.response.send_message("ℹ️ Dieser Slot ist bereits als erhalten markiert.", ephemeral=True)
                return
            _mark_slot_received(data, tab, slot, inter.user.id)
            save_needs()
            try:
                if member:
                    await member.send(f"✅ Dein Need **{item_name}** wurde von der Gildenleitung als **erhalten** markiert.\nSlot: **{tab} – {slot}**")
            except Exception:
                pass
            emb = discord.Embed(title="✅ Item erhalten markiert", description=f"**{name}**\n**{tab} – {slot}:** {item_name} ✅ Erhalten", color=discord.Color.green())
            await inter.response.edit_message(embed=emb, view=AdminLootBackView())
            return
        if self.action == "unmark":
            if not bool(slot_data.get("received", False)):
                await inter.response.send_message("ℹ️ Dieser Slot ist nicht als erhalten markiert.", ephemeral=True)
                return
            _unmark_slot_received(data, tab, slot)
            save_needs()
            emb = discord.Embed(title="✅ Erhalten-Markierung entfernt", description=f"**{name}**\n**{tab} – {slot}:** {item_name}", color=discord.Color.green())
            await inter.response.edit_message(embed=emb, view=AdminLootBackView())
            return
