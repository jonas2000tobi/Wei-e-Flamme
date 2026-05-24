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
from discord.ui import View, button, Select
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


def _user_needs(guild_id: int, user_id: int) -> dict:
    g = _gneeds(guild_id)
    users = g.setdefault("users", {})
    u = users.get(str(user_id)) or {}
    u.setdefault("Main", {})
    u.setdefault("Secondary", {})

    for tab in TABS:
        if tab not in u or not isinstance(u[tab], dict):
            u[tab] = {}

        for slot in NEED_SLOTS:
            u[tab].setdefault(slot, "")

    users[str(user_id)] = u
    return u


def _normalize_need_slot(slot: str) -> str:
    slot = (slot or "").strip().lower()

    for s in NEED_SLOTS:
        if s.lower() == slot:
            return s

    compact = slot.replace(" ", "")

    aliases = {
        "waffe1": "Waffe 1",
        "waffe2": "Waffe 2",
        "ring1": "Ring 1",
        "ring2": "Ring 2",
    }

    return aliases.get(compact, "")


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


def _items_for_need_slot(guild_id: int, need_slot: str) -> list[dict]:
    catalog_slot = _catalog_slot_for_need_slot(need_slot)
    items = _all_items(guild_id)
    out = []

    legacy_slots = {catalog_slot}

    if catalog_slot == "Waffe":
        legacy_slots.update({"Waffe 1", "Waffe 2"})

    if catalog_slot == "Ring":
        legacy_slots.update({"Ring 1", "Ring 2"})

    for item_id, item in items.items():
        if str(item.get("slot", "")) in legacy_slots:
            i = dict(item)
            i["id"] = item_id
            out.append(i)

    out.sort(key=lambda x: str(x.get("name", "")).lower())
    return out


def _item_name(guild_id: int, item_id: str) -> str:
    if not item_id:
        return "—"

    item = _all_items(guild_id).get(str(item_id))

    if not item:
        return f"Unbekanntes Item ({item_id})"

    return str(item.get("name", item_id))


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
            "Waffen werden über den gemeinsamen Item-Katalog **Waffe** ausgewählt.\n"
            "Du kannst trotzdem getrennt **Waffe 1** und **Waffe 2** eintragen."
        ),
        color=discord.Color.gold()
    )

    lines = []

    for slot in NEED_SLOTS:
        item_id = str(data.get(tab, {}).get(slot, "") or "")
        item_name = _item_name(guild.id, item_id) if item_id else "—"
        lines.append(f"**{slot}:** {item_name}")

    emb.add_field(name=tab, value="\n".join(lines), inline=False)
    emb.set_footer(text="Für Gildenbosse werden nur Waffe 1 und Waffe 2 ausgewertet.")

    return emb


def _format_need_user_full(guild: discord.Guild, user_id: int) -> str:
    data = _user_needs(guild.id, user_id)
    name = _profile_name(guild, user_id)

    lines = [f"**{name}**"]

    for tab in TABS:
        used = []

        for slot in NEED_SLOTS:
            item_id = str(data.get(tab, {}).get(slot, "") or "")

            if item_id:
                used.append(f"{slot}: {_item_name(guild.id, item_id)}")

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
    grouped: dict[str, dict[str, list[str]]] = {
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
                item_id = str(data.get(tab, {}).get(slot, "") or "")

                if not item_id:
                    continue

                item_name = _item_name(guild.id, item_id)

                if item_name.startswith("Unbekanntes Item"):
                    continue

                grouped[tab].setdefault(item_name, [])

                if name not in grouped[tab][item_name]:
                    grouped[tab][item_name].append(name)

    emb = discord.Embed(
        title=title,
        description=subtitle,
        color=discord.Color.red()
    )

    for tab in TABS:
        item_map = grouped.get(tab) or {}

        if not item_map:
            emb.add_field(name=f"{tab.upper()}-NEED", value="—", inline=False)
            continue

        sorted_items = sorted(
            item_map.items(),
            key=lambda kv: (-len(kv[1]), kv[0].lower())
        )

        lines = []

        for item_name, names in sorted_items:
            lines.append(f"**{item_name} — {len(names)}x**")
            lines.append(", ".join(names))
            lines.append("")

        value = "\n".join(lines).strip()

        if len(value) > 1024:
            value = value[:1000] + "\n… gekürzt"

        emb.add_field(name=f"{tab.upper()}-NEED", value=value or "—", inline=False)

    emb.set_footer(text="Ausgewertet werden Waffe 1 und Waffe 2. Main und Secondary bleiben getrennt.")

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

    @button(label="Main anzeigen", style=ButtonStyle.primary, custom_id="need_show_main", row=0)
    async def btn_show_main(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, "Main"),
            view=NeedMainView(self.guild_id, self.user_id)
        )

    @button(label="Secondary anzeigen", style=ButtonStyle.secondary, custom_id="need_show_secondary", row=0)
    async def btn_show_secondary(self, inter: discord.Interaction, _):
        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("❌ Server nicht gefunden.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, "Secondary"),
            view=NeedMainView(self.guild_id, self.user_id)
        )

    @button(label="Item eintragen", style=ButtonStyle.success, custom_id="need_add_item", row=1)
    async def btn_add_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste – Item eintragen",
            description="Wähle zuerst, ob du den Eintrag für **Main** oder **Secondary** setzen möchtest.",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, action="set")
        )

    @button(label="Item entfernen", style=ButtonStyle.danger, custom_id="need_remove_item", row=1)
    async def btn_remove_item(self, inter: discord.Interaction, _):
        emb = discord.Embed(
            title="🎁 Needliste – Item entfernen",
            description="Wähle zuerst, ob du den Eintrag aus **Main** oder **Secondary** entfernen möchtest.",
            color=discord.Color.gold()
        )

        await inter.response.edit_message(
            embed=emb,
            view=NeedTabSelectView(self.guild_id, self.user_id, action="clear")
        )

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_back_portal", row=2)
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
                title="🏰 ebolus – Gildenmenü",
                description="Zurück zum Gildenmenü.",
                color=discord.Color.blurple()
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
            discord.SelectOption(label="Main", value="Main", description="Main-Needliste"),
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
        action_text = "setzen" if self.action == "set" else "entfernen"

        emb = discord.Embed(
            title="🎁 Needliste – Slot wählen",
            description=f"Bereich: **{tab}**\nAktion: **Item {action_text}**",
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

        if self.action == "clear":
            data = _user_needs(self.guild_id, self.user_id)
            data[self.tab][need_slot] = ""
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


class NeedItemSelectView(View):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str):
        super().__init__(timeout=None)
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot
        self.add_item(NeedItemSelect(guild_id, user_id, tab, need_slot))

    @button(label="⬅️ Zurück", style=ButtonStyle.secondary, custom_id="need_item_back")
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


class NeedItemSelect(Select):
    def __init__(self, guild_id: int, user_id: int, tab: str, need_slot: str):
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.tab = tab
        self.need_slot = need_slot

        items = _items_for_need_slot(guild_id, need_slot)[:25]

        options = [
            discord.SelectOption(
                label=str(item.get("name", item["id"]))[:100],
                value=str(item["id"])[:100]
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
        data[self.tab][self.need_slot] = item_id
        save_needs()

        guild = inter.client.get_guild(self.guild_id)

        if not guild:
            await inter.response.send_message("✅ Need gespeichert.")
            return

        await inter.response.edit_message(
            embed=_need_embed(guild, self.user_id, self.tab),
            view=NeedMainView(self.guild_id, self.user_id)
        )


def _catalog_slot_choices():
    return [
        app_commands.Choice(name=s, value=s)
        for s in CATALOG_SLOTS
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


async def setup_loot_needs(client: discord.Client, tree: app_commands.CommandTree):
    global _client_ref
    _client_ref = client

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
    @app_commands.choices(slot=_catalog_slot_choices())
    async def loot_item_add(inter: discord.Interaction, slot: app_commands.Choice[str], name: str):
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

        existing = _find_item_by_name(inter.guild.id, catalog_slot, clean_name)

        if existing:
            await inter.response.send_message(
                f"⚠️ Dieses Item existiert für **{catalog_slot}** bereits.",
                ephemeral=True
            )
            return

        item_id = _make_item_id(inter.guild.id, catalog_slot, clean_name)
        items = _all_items(inter.guild.id)
        items[item_id] = {
            "name": clean_name,
            "slot": catalog_slot,
            "created_at": _now_iso(),
            "created_by": int(inter.user.id),
        }

        save_items()

        await inter.response.send_message(
            f"✅ Item hinzugefügt:\n**{clean_name}**\nSlot: **{catalog_slot}**\nID: `{item_id}`",
            ephemeral=True
        )

    @tree.command(name="loot_item_list", description="Zeigt Items aus dem Need-Katalog")
    @app_commands.choices(slot=_catalog_slot_choices())
    async def loot_item_list(inter: discord.Interaction, slot: Optional[app_commands.Choice[str]] = None):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        catalog_slot = _normalize_catalog_slot(slot.value) if slot else ""

        if catalog_slot:
            items = []
            for item_id, item in _all_items(inter.guild.id).items():
                if str(item.get("slot", "")) == catalog_slot:
                    x = dict(item)
                    x["id"] = item_id
                    items.append(x)

            items.sort(key=lambda x: str(x.get("name", "")).lower())
            title = f"🎁 Item-Katalog – {catalog_slot}"
        else:
            items = []
            for item_id, item in _all_items(inter.guild.id).items():
                x = dict(item)
                x["id"] = item_id
                items.append(x)

            items.sort(key=lambda x: (str(x.get("slot", "")), str(x.get("name", "")).lower()))
            title = "🎁 Item-Katalog – alle Items"

        if not items:
            await inter.response.send_message("Keine Items gefunden.", ephemeral=True)
            return

        lines = []

        for item in items[:80]:
            lines.append(f"• **{item.get('name')}** — {item.get('slot')}")

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
                    if data.get(tab, {}).get(s) == item_id:
                        data[tab][s] = ""
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
