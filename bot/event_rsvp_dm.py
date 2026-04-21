# RSVP per DM + Buttons direkt im Server-Post:
# - DMs mit Buttons für normale Nutzer
# - Server-Buttons als Fallback / Komfort
# - Beide Wege schreiben in denselben Store
# - Namen werden gespeichert, damit mobil nicht nur IDs/Nummern auftauchen
# - Neue Option: BANK (Reserve)
# - Offene DMs von Nicht-Abstimmern werden ab Eventstart gelöscht

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional, Iterable, List, Any
import asyncio

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

try:
    from bot.event_dm_prefs import is_dm_enabled  # type: ignore
except ModuleNotFoundError:
    from event_dm_prefs import is_dm_enabled      # type: ignore

try:
    from bot.raid_stats import record_response  # type: ignore
except ModuleNotFoundError:
    from raid_stats import record_response      # type: ignore

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RSVP_FILE   = DATA_DIR / "event_rsvp.json"       # Events + Anmeldungen
DM_CFG_FILE = DATA_DIR / "event_rsvp_cfg.json"   # {"guild": {"TANK":id,"HEAL":id,"DPS":id,"LOG_CH":id}}

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg:   Dict[str, dict] = _load(DM_CFG_FILE, {})

def save_store():
    _save(RSVP_FILE, store)

def save_cfg():
    _save(DM_CFG_FILE, cfg)


# ---------------- Utils / Logging ----------------

async def _log(client: discord.Client, guild_id: int, text: str):
    gcfg = cfg.get(str(guild_id)) or {}
    ch_id = int(gcfg.get("LOG_CH", 0) or 0)
    if not ch_id:
        return
    g = client.get_guild(guild_id)
    if not g:
        return
    ch = g.get_channel(ch_id)
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(f"[RSVP-DM] {text}")
        except Exception:
            pass

def _safe_name(name: str) -> str:
    return (name or "").replace("@", "@\u200b").strip() or "Unbekannt"

def _current_display_name(member: Optional[discord.Member], fallback_user: Optional[discord.abc.User] = None) -> str:
    if member is not None:
        return _safe_name(member.display_name)
    if fallback_user is not None:
        return _safe_name(getattr(fallback_user, "display_name", None) or getattr(fallback_user, "name", "Unbekannt"))
    return "Unbekannt"

def _participant_entry(uid: int, name: str) -> dict:
    return {
        "id": int(uid),
        "name": _safe_name(name),
    }

def _entry_user_id(entry: Any) -> int:
    try:
        if isinstance(entry, dict):
            return int(entry.get("id", 0) or 0)
        return int(entry)
    except Exception:
        return 0

def _entry_name(entry: Any, guild: Optional[discord.Guild] = None) -> str:
    if isinstance(entry, dict):
        stored = _safe_name(str(entry.get("name", "") or ""))
        uid = _entry_user_id(entry)
        if guild and uid:
            m = guild.get_member(uid)
            if m:
                return _safe_name(m.display_name)
        return stored or (f"User {uid}" if uid else "Unbekannt")

    try:
        uid = int(entry)
    except Exception:
        return "Unbekannt"

    if guild:
        m = guild.get_member(uid)
        if m:
            return _safe_name(m.display_name)
    return f"User {uid}"

def _maybe_entry(uid: int, name: str, label: str) -> dict:
    return {
        "id": int(uid),
        "name": _safe_name(name),
        "label": (label or "").strip(),
    }

def _maybe_name_and_label(entry: Any, uid_fallback: int, guild: Optional[discord.Guild] = None) -> tuple[str, str]:
    if isinstance(entry, dict):
        uid = int(entry.get("id", uid_fallback) or uid_fallback)
        label = str(entry.get("label", "") or "").strip()
        name = _entry_name({"id": uid, "name": entry.get("name", "")}, guild)
        return name, label

    label = str(entry or "").strip()
    if guild:
        m = guild.get_member(uid_fallback)
        if m:
            return _safe_name(m.display_name), label
    return f"User {uid_fallback}", label

def _format_dm_text(title: str, when: datetime, channel_name_or_ref: str, description: str | None, intro_line: str | None = None) -> str:
    desc = (description or "").strip()

    dm_text = (
        f"📅 **{title}**\n"
        f"🕒 {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
        f"📍 {channel_name_or_ref}\n"
    )

    if desc:
        dm_text += f"\n📝 **Beschreibung:**\n{desc[:500]}\n"

    if intro_line:
        dm_text += f"\n👇 **{intro_line}**"
    else:
        dm_text += "\n👇 **Wähle unten deine Teilnahme:**"

    return dm_text

def _init_event_shape(obj: dict):
    if "yes" not in obj or not isinstance(obj["yes"], dict):
        obj["yes"] = {"TANK": [], "HEAL": [], "DPS": [], "BANK": []}

    for k in ("TANK", "HEAL", "DPS", "BANK"):
        if k not in obj["yes"] or not isinstance(obj["yes"][k], list):
            obj["yes"][k] = []

    if "maybe" not in obj or not isinstance(obj["maybe"], dict):
        obj["maybe"] = {}

    if "no" not in obj or not isinstance(obj["no"], list):
        obj["no"] = []

    obj.setdefault("target_role_id", 0)

    # user_id -> dm_message_id
    if "dm_messages" not in obj or not isinstance(obj["dm_messages"], dict):
        obj["dm_messages"] = {}

    # Legacy-Migration
    migrated = False

    for role_key in ("TANK", "HEAL", "DPS"):
        new_list = []
        for raw in obj["yes"].get(role_key, []):
            if isinstance(raw, dict):
                new_list.append({
                    "id": int(raw.get("id", 0) or 0),
                    "name": _safe_name(str(raw.get("name", "") or f"User {raw.get('id', 0)}"))
                })
            else:
                try:
                    uid = int(raw)
                    new_list.append(_participant_entry(uid, f"User {uid}"))
                except Exception:
                    continue
        if obj["yes"].get(role_key) != new_list:
            obj["yes"][role_key] = new_list
            migrated = True

    if "BANK" not in obj["yes"]:
        obj["yes"]["BANK"] = []
        migrated = True
    else:
        bank_new = []
        for raw in obj["yes"].get("BANK", []):
            if isinstance(raw, dict):
                bank_new.append({
                    "id": int(raw.get("id", 0) or 0),
                    "name": _safe_name(str(raw.get("name", "") or f"User {raw.get('id', 0)}"))
                })
            else:
                try:
                    uid = int(raw)
                    bank_new.append(_participant_entry(uid, f"User {uid}"))
                except Exception:
                    continue
        if obj["yes"].get("BANK") != bank_new:
            obj["yes"]["BANK"] = bank_new
            migrated = True

    maybe_new = {}
    for uid_str, raw in obj.get("maybe", {}).items():
        try:
            uid_i = int(uid_str)
        except Exception:
            continue

        if isinstance(raw, dict):
            maybe_new[str(uid_i)] = {
                "id": uid_i,
                "name": _safe_name(str(raw.get("name", "") or f"User {uid_i}")),
                "label": str(raw.get("label", "") or "").strip()
            }
        else:
            maybe_new[str(uid_i)] = _maybe_entry(uid_i, f"User {uid_i}", str(raw or "").strip())
    if obj.get("maybe") != maybe_new:
        obj["maybe"] = maybe_new
        migrated = True

    no_new = []
    for raw in obj.get("no", []):
        if isinstance(raw, dict):
            no_new.append({
                "id": int(raw.get("id", 0) or 0),
                "name": _safe_name(str(raw.get("name", "") or f"User {raw.get('id', 0)}"))
            })
        else:
            try:
                uid = int(raw)
                no_new.append(_participant_entry(uid, f"User {uid}"))
            except Exception:
                continue
    if obj.get("no") != no_new:
        obj["no"] = no_new
        migrated = True

    if migrated:
        save_store()

def get_role_ids_for_guild(guild_id: int) -> Dict[str, int]:
    g = cfg.get(str(guild_id)) or {}
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS":  int(g.get("DPS",  0) or 0),
    }

def _primary_label(member: Optional[discord.Member], rid_map: Dict[str, int]) -> str:
    if member is None:
        return ""
    g = member.guild
    r = g.get_role(rid_map.get("TANK", 0) or 0)
    if r and r in getattr(member, "roles", []):
        return "Tank"
    r = g.get_role(rid_map.get("HEAL", 0) or 0)
    if r and r in getattr(member, "roles", []):
        return "Heal"
    r = g.get_role(rid_map.get("DPS", 0) or 0)
    if r and r in getattr(member, "roles", []):
        return "DPS"
    names = [getattr(rr, "name", "").lower() for rr in getattr(member, "roles", [])]
    if any("tank" in n for n in names):
        return "Tank"
    if any("heal" in n for n in names):
        return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names):
        return "DPS"
    return ""

def _member_from_event(inter: discord.Interaction, obj: dict) -> Optional[discord.Member]:
    try:
        if inter.guild is not None:
            return inter.guild.get_member(inter.user.id)
        gid = int(obj.get("guild_id", 0) or 0)
        if not gid:
            return None
        g = inter.client.get_guild(gid)
        if not g:
            return None
        return g.get_member(inter.user.id)
    except Exception:
        return None


# ---------------- Embed ----------------

def _voters_set(obj: dict) -> set[int]:
    voted: set[int] = set()
    for k in ("TANK", "HEAL", "DPS", "BANK"):
        voted.update(_entry_user_id(u) for u in obj["yes"].get(k, []))
    voted.update(_entry_user_id(u) for u in obj["no"])
    for uid_str, entry in obj["maybe"].items():
        try:
            uid_i = int(uid_str)
        except Exception:
            uid_i = _entry_user_id(entry)
        if uid_i:
            voted.add(uid_i)
    return voted

def _eligible_members(guild: discord.Guild, obj: dict) -> List[discord.Member]:
    tr_id = int(obj.get("target_role_id", 0) or 0)
    if not tr_id:
        return [m for m in guild.members if not m.bot]
    role = guild.get_role(tr_id)
    if not role:
        return [m for m in guild.members if not m.bot]
    return [m for m in role.members if not m.bot]

def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    _init_event_shape(obj)

    when = datetime.fromisoformat(obj["when_iso"])
    yes = obj["yes"]
    maybe = obj["maybe"]
    no = obj["no"]

    eligible = _eligible_members(guild, obj)
    voted = _voters_set(obj)

    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=(
            (obj.get('description', '') or '') +
            f"\n\n🕒 Zeit: {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"
            f"\n🗳️ Abgestimmt: **{len(voted)}** / **{len(eligible)}**"
            f"\n💡 Wenn du keine DM bekommst oder sie deaktiviert hast: nutze die Buttons direkt unter dieser Ankündigung."
        ).strip(),
        color=discord.Color.blurple()
    )

    tank_names = [_entry_name(u, guild) for u in yes.get("TANK", [])]
    heal_names = [_entry_name(u, guild) for u in yes.get("HEAL", [])]
    dps_names  = [_entry_name(u, guild) for u in yes.get("DPS",  [])]
    bank_names = [_entry_name(u, guild) for u in yes.get("BANK", [])]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})", value="\n".join(dps_names) or "—", inline=True)
    emb.add_field(name=f"🏦 Bank ({len(bank_names)})", value="\n".join(bank_names) or "—", inline=False)

    maybe_lines = []
    for uid_str, entry in maybe.items():
        try:
            uid_i = int(uid_str)
        except Exception:
            uid_i = _entry_user_id(entry)
        name, label = _maybe_name_and_label(entry, uid_i, guild)
        label_txt = f" ({label})" if label else ""
        maybe_lines.append(f"{name}{label_txt}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_entry_name(u, guild) for u in no]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        if r:
            emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="DM-Buttons und Server-Buttons schreiben beide in dieselbe Anmeldung.")
    return emb


# ---------------- DM-Handling ----------------

async def _delete_dm_message_for_user(client: discord.Client, obj: dict, user_id: int) -> bool:
    _init_event_shape(obj)

    dm_map = obj.get("dm_messages") or {}
    mid = dm_map.get(str(user_id))
    if not mid:
        return False

    user = client.get_user(int(user_id))
    if user is None:
        try:
            user = await client.fetch_user(int(user_id))
        except Exception:
            user = None

    deleted = False
    if user is not None:
        try:
            dm = user.dm_channel or await user.create_dm()
            msg = await dm.fetch_message(int(mid))
            await msg.delete()
            deleted = True
        except Exception:
            pass

    dm_map.pop(str(user_id), None)
    obj["dm_messages"] = dm_map
    return deleted

async def _delete_all_pending_dm_messages_for_event(client: discord.Client, obj: dict) -> int:
    _init_event_shape(obj)

    removed = 0
    for uid_str in list((obj.get("dm_messages") or {}).keys()):
        try:
            uid = int(uid_str)
        except Exception:
            obj["dm_messages"].pop(uid_str, None)
            removed += 1
            continue
        try:
            ok = await _delete_dm_message_for_user(client, obj, uid)
            if ok or str(uid) not in obj.get("dm_messages", {}):
                removed += 1
        except Exception:
            obj["dm_messages"].pop(str(uid), None)
            removed += 1
    return removed

async def delete_pending_dm_messages_for_started_events(client: discord.Client) -> int:
    """
    Löscht offene RSVP-DMs für User, die noch nicht abgestimmt haben,
    sobald das Event angefangen hat.
    """
    changed = 0
    now = datetime.now(TZ)

    for _msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
            when = datetime.fromisoformat(obj.get("when_iso"))
        except Exception:
            continue

        if now < when:
            continue

        dm_map = obj.get("dm_messages") or {}
        if not dm_map:
            continue

        # Nur die offen gebliebenen DMs löschen
        for uid_str in list(dm_map.keys()):
            try:
                uid = int(uid_str)
            except Exception:
                obj["dm_messages"].pop(uid_str, None)
                changed += 1
                continue

            # Falls inzwischen doch abgestimmt -> DM auch entfernen
            voted = uid in _voters_set(obj)
            try:
                ok = await _delete_dm_message_for_user(client, obj, uid)
                if ok or voted or str(uid) not in obj.get("dm_messages", {}):
                    changed += 1
            except Exception:
                obj["dm_messages"].pop(str(uid), None)
                changed += 1

    return changed


# ---------------- Gemeinsame Logik ----------------

async def _push_overview(client: discord.Client, msg_id: str, obj: dict):
    guild = client.get_guild(int(obj["guild_id"]))
    if not guild:
        return
    ch = guild.get_channel(int(obj["channel_id"]))
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    try:
        msg = await ch.fetch_message(int(msg_id))
    except Exception:
        return

    emb = build_embed(guild, obj)
    try:
        await msg.edit(embed=emb, view=ServerRaidView(int(msg_id)))
    except Exception:
        pass

async def apply_rsvp(inter: discord.Interaction, msg_id: str, group: str) -> tuple[bool, str]:
    obj = store.get(str(msg_id))
    if not obj:
        return False, "Dieses Event existiert nicht mehr."

    _init_event_shape(obj)

    uid = inter.user.id
    member = _member_from_event(inter, obj)
    display_name = _current_display_name(member, inter.user)

    if group in ("TANK", "HEAL", "DPS"):
        response_key = "yes"
    elif group == "BANK":
        response_key = "bank"
    elif group == "MAYBE":
        response_key = "maybe"
    else:
        response_key = "no"

    # User aus allen Buckets entfernen
    for k in ("TANK", "HEAL", "DPS", "BANK"):
        obj["yes"][k] = [entry for entry in obj["yes"].get(k, []) if _entry_user_id(entry) != uid]

    obj["no"] = [entry for entry in obj.get("no", []) if _entry_user_id(entry) != uid]
    obj["maybe"].pop(str(uid), None)

    if group in ("TANK", "HEAL", "DPS"):
        obj["yes"][group].append(_participant_entry(uid, display_name))
        text = f"Angemeldet als **{group}**."
    elif group == "BANK":
        obj["yes"]["BANK"].append(_participant_entry(uid, display_name))
        text = "Als **Bank / Reserve** eingetragen."
    elif group == "MAYBE":
        rid_map = get_role_ids_for_guild(int(obj["guild_id"]))
        label = _primary_label(member, rid_map)
        obj["maybe"][str(uid)] = _maybe_entry(uid, display_name, label)
        text = "Als **Vielleicht** eingetragen."
    elif group == "NO":
        obj["no"].append(_participant_entry(uid, display_name))
        text = "Als **Abgemeldet** eingetragen."
    else:
        return False, "Ungültige Auswahl."

    save_store()
    record_response(int(obj["guild_id"]), uid, str(msg_id), response_key)

    # Egal ob per Server oder DM abgestimmt: offene Event-DM für den User weg
    try:
        await _delete_dm_message_for_user(inter.client, obj, uid)
    except Exception:
        pass

    save_store()
    await _push_overview(inter.client, str(msg_id), obj)
    return True, text


# ---------------- Views ----------------

class BaseRaidView(View):
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _send_feedback(self, inter: discord.Interaction, text: str):
        if not inter.response.is_done():
            await inter.response.send_message(text, ephemeral=True)
        else:
            await inter.followup.send(text, ephemeral=True)

    async def _after_success(self, inter: discord.Interaction):
        # überschreibbar
        return

    async def _handle(self, inter: discord.Interaction, group: str):
        try:
            ok, text = await apply_rsvp(inter, self.msg_id, group)
            await self._send_feedback(inter, text)
            if ok:
                await self._after_success(inter)
        except Exception as e:
            await self._send_feedback(inter, "❌ Unerwarteter Fehler. Bitte erneut probieren.")
            try:
                gid = 0
                obj = store.get(self.msg_id) or {}
                gid = int(obj.get("guild_id", 0) or 0)
                await _log(inter.client, gid, f"Button-Fehler ({type(self).__name__}): {e!r}")
            except Exception:
                pass


class RaidView(BaseRaidView):
    """Buttons in der DM."""
    async def _after_success(self, inter: discord.Interaction):
        try:
            await inter.message.delete()
        except Exception:
            pass

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="dm_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._handle(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._handle(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="dm_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._handle(inter, "DPS")

    @button(label="🏦 Bank", style=ButtonStyle.secondary, custom_id="dm_rsvp_bank")
    async def btn_bank(self, inter: discord.Interaction, _):
        await self._handle(inter, "BANK")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._handle(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):
        await self._handle(inter, "NO")


class ServerRaidView(BaseRaidView):
    """Buttons direkt unter der Raid-Ankündigung im Server."""
    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="srv_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._handle(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="srv_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._handle(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="srv_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._handle(inter, "DPS")

    @button(label="🏦 Bank", style=ButtonStyle.secondary, custom_id="srv_rsvp_bank")
    async def btn_bank(self, inter: discord.Interaction, _):
        await self._handle(inter, "BANK")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="srv_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._handle(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="srv_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):
        await self._handle(inter, "NO")


# ---------------- Commands / Setup ----------------

def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))

async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    """Registriert Slash-Commands und hängt persistente Views wieder an."""
    for msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
        except Exception:
            pass

        try:
            client.add_view(RaidView(int(msg_id)))
        except Exception:
            pass
        try:
            client.add_view(ServerRaidView(int(msg_id)))
        except Exception:
            pass

    save_store()

    # ---- Admin: Rollen für Maybe-Label
    @tree.command(name="raid_set_roles_dm", description="(Admin) Primärrollen (Tank/Heal/DPS) für Maybe-Label setzen")
    @app_commands.describe(tank_role="Rolle: Tank", heal_role="Rolle: Heal", dps_role="Rolle: DPS")
    async def raid_set_roles_dm(
        inter: discord.Interaction,
        tank_role: discord.Role,
        heal_role: discord.Role,
        dps_role: discord.Role
    ):
        await inter.response.defer(ephemeral=True, thinking=False)
        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        c = cfg.get(str(inter.guild_id)) or {}
        c["TANK"] = int(tank_role.id)
        c["HEAL"] = int(heal_role.id)
        c["DPS"]  = int(dps_role.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()
        await inter.followup.send(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

    # ---- Admin: Log-Kanal
    @tree.command(name="raid_set_log_channel", description="(Admin) Log-Kanal für RSVP-DM (optional)")
    async def raid_set_log_channel(inter: discord.Interaction, channel: discord.TextChannel):
        await inter.response.defer(ephemeral=True, thinking=False)
        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        c = cfg.get(str(inter.guild_id)) or {}
        c["LOG_CH"] = int(channel.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()
        await inter.followup.send(f"✅ Log-Kanal gesetzt: {channel.mention}", ephemeral=True)

    # ---- Admin: Raid erstellen
    @tree.command(name="raid_create_dm", description="(Admin) Raid/Anmeldung erzeugen + Übersicht posten")
    @app_commands.describe(
        title="Titel",
        date="Datum YYYY-MM-DD",
        time="Zeit HH:MM (24h)",
        description="Kurzbeschreibung (optional)",
        channel="Server-Channel für die Übersicht",
        target_role="(Optional) Nur an diese Rolle DMs versenden",
        image_url="Optionales Bild fürs Embed"
    )
    async def raid_create_dm(
        inter: discord.Interaction,
        title: str,
        date: str,
        time: str,
        description: Optional[str] = None,
        channel: Optional[discord.TextChannel] = None,
        target_role: Optional[discord.Role] = None,
        image_url: Optional[str] = None
    ):
        await inter.response.defer(ephemeral=True, thinking=True)
        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.followup.send("❌ Datum/Zeit ungültig. (YYYY-MM-DD / HH:MM)", ephemeral=True)
            return

        ch = channel or inter.channel
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await inter.followup.send("❌ Zielkanal ist kein Textkanal/Thread.", ephemeral=True)
            return

        obj = {
            "guild_id": int(inter.guild_id),
            "channel_id": int(ch.id),
            "title": title.strip(),
            "description": (description or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": [], "BANK": []},
            "maybe": {},
            "no": [],
            "target_role_id": int(target_role.id) if target_role else 0,
            "dm_messages": {}
        }

        # Erst posten, dann mit Buttons editieren, weil wir die Message-ID brauchen
        emb = build_embed(inter.guild, obj)
        msg = await ch.send(embed=emb)
        store[str(msg.id)] = obj
        save_store()

        try:
            await msg.edit(view=ServerRaidView(int(msg.id)))
        except Exception:
            pass

        sent = 0
        skipped_opt_out = 0
        role_obj = inter.guild.get_role(int(obj.get("target_role_id", 0) or 0)) if obj.get("target_role_id") else None

        for m in _eligible_members(inter.guild, obj):
            if not is_dm_enabled(inter.guild_id, m.id):
                skipped_opt_out += 1
                continue
            try:
                dm_text = _format_dm_text(
                    title=title,
                    when=when,
                    channel_name_or_ref=f"Übersicht im Server: #{ch.name}",
                    description=description,
                    intro_line="Wähle unten deine Teilnahme:"
                )
                dm_msg = await m.send(dm_text, view=RaidView(int(msg.id)))
                obj["dm_messages"][str(m.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        save_store()

        ziel = role_obj.mention if role_obj else "alle Mitglieder (ohne Bots)"
        await inter.followup.send(
            f"✅ Raid erstellt: {msg.jump_url}\n"
            f"🎯 Zielgruppe: {ziel}\n"
            f"✉️ DMs versendet: {sent}\n"
            f"🔕 Opt-out übersprungen: {skipped_opt_out}\n"
            f"🖱️ Abstimmung ist zusätzlich direkt unter der Raid-Ankündigung per Button möglich.",
            ephemeral=True
        )

    # ---- Admin: Allen, die noch nicht abgestimmt haben, erneut schicken
    @tree.command(name="raid_resend_missing", description="(Admin) DMs an alle, die noch nicht abgestimmt haben")
    async def raid_resend_missing(inter: discord.Interaction, message_id: str):
        await inter.response.defer(ephemeral=True, thinking=True)
        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        obj = store.get(str(message_id))
        if not obj or int(obj.get("guild_id", 0) or 0) != inter.guild_id:
            await inter.followup.send("❌ Unbekanntes Event/Message-ID.", ephemeral=True)
            return

        _init_event_shape(obj)

        guild = inter.guild
        ch = guild.get_channel(int(obj["channel_id"]))
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await inter.followup.send("❌ Zielkanal existiert nicht mehr.", ephemeral=True)
            return

        when = datetime.fromisoformat(obj["when_iso"])
        if datetime.now(TZ) > when + timedelta(hours=2):
            await inter.followup.send("⚠️ Event ist älter als 2h nach Start – keine Resend.", ephemeral=True)
            return

        eligible = _eligible_members(guild, obj)
        already = _voters_set(obj)
        targets = [m for m in eligible if m.id not in already]

        sent = 0
        skipped_opt_out = 0
        for m in targets:
            if not is_dm_enabled(inter.guild_id, m.id):
                skipped_opt_out += 1
                continue
            try:
                dm_text = _format_dm_text(
                    title=str(obj["title"]),
                    when=when,
                    channel_name_or_ref=f"Übersicht: <#{obj['channel_id']}>",
                    description=obj.get("description"),
                    intro_line="Du hast noch nicht abgestimmt:"
                )
                dm_msg = await m.send(dm_text, view=RaidView(int(message_id)))
                obj["dm_messages"][str(m.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        save_store()

        await inter.followup.send(
            f"✅ Resent an {sent} Nutzer.\n🔕 Opt-out übersprungen: {skipped_opt_out}",
            ephemeral=True
        )

    # ---- Admin: Manuelles Resend an User oder Rolle
    @tree.command(name="raid_resend_to", description="(Admin) DMs gezielt an Rolle oder User für ein Event senden")
    async def raid_resend_to(
        inter: discord.Interaction,
        message_id: str,
        role: Optional[discord.Role] = None,
        user: Optional[discord.Member] = None
    ):
        await inter.response.defer(ephemeral=True, thinking=True)
        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        obj = store.get(str(message_id))
        if not obj or int(obj.get("guild_id", 0) or 0) != inter.guild_id:
            await inter.followup.send("❌ Unbekanntes Event/Message-ID.", ephemeral=True)
            return

        _init_event_shape(obj)

        when = datetime.fromisoformat(obj["when_iso"])
        if datetime.now(TZ) > when + timedelta(hours=2):
            await inter.followup.send("⚠️ Event ist älter als 2h nach Start – keine Resend.", ephemeral=True)
            return

        targets: Iterable[discord.Member]
        if user:
            targets = [user]
        elif role:
            targets = [m for m in role.members if not m.bot]
        else:
            await inter.followup.send("❌ Bitte `role` oder `user` angeben.", ephemeral=True)
            return

        sent = 0
        skipped_opt_out = 0
        for m in targets:
            if not is_dm_enabled(inter.guild_id, m.id):
                skipped_opt_out += 1
                continue
            try:
                dm_text = _format_dm_text(
                    title=str(obj["title"]),
                    when=when,
                    channel_name_or_ref=f"Übersicht: <#{obj['channel_id']}>",
                    description=obj.get("description"),
                    intro_line="Wähle unten deine Teilnahme:"
                )
                dm_msg = await m.send(dm_text, view=RaidView(int(message_id)))
                obj["dm_messages"][str(m.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        save_store()

        await inter.followup.send(
            f"✅ Resent an {sent} Ziel(e).\n🔕 Opt-out übersprungen: {skipped_opt_out}",
            ephemeral=True
        )


# ------------------------------------------------------------
# Auto-Resend für neue Mitglieder
# ------------------------------------------------------------
async def auto_resend_for_new_member(member: discord.Member) -> None:
    """
    Bei on_member_join(member) aufrufen.
    Schickt dem neuen Member die RSVP-DM für alle noch relevanten Events seiner Guild:
      - gleiche Guild
      - Startzeit nicht länger als 2h her oder in Zukunft
      - Zielrolle passt (falls gesetzt)
      - User hat Raid-DMs nicht deaktiviert
    """
    try:
        if member.bot:
            return

        if not is_dm_enabled(member.guild.id, member.id):
            return

        now = datetime.now(TZ)
        sent = 0

        for mid, obj in list(store.items()):
            try:
                _init_event_shape(obj)

                if int(obj.get("guild_id", 0) or 0) != member.guild.id:
                    continue

                when = datetime.fromisoformat(obj.get("when_iso"))
                if now > when + timedelta(hours=2):
                    continue

                tr_id = int(obj.get("target_role_id", 0) or 0)
                if tr_id:
                    r = member.guild.get_role(tr_id)
                    if not (r and r in member.roles):
                        continue

                text = _format_dm_text(
                    title=str(obj.get("title", "Event")),
                    when=when,
                    channel_name_or_ref=f"Übersicht im Server: <#{obj.get('channel_id')}>",
                    description=obj.get("description"),
                    intro_line="Wähle unten deine Teilnahme:"
                )
                try:
                    dm_msg = await member.send(text, view=RaidView(int(mid)))
                    obj["dm_messages"][str(member.id)] = int(dm_msg.id)
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            except Exception:
                continue

        if sent:
            save_store()

        try:
            if sent and hasattr(member, "_state") and hasattr(member._state, "_get_client"):
                client = member._state._get_client()
                await _log(client, member.guild.id, f"Auto-Resend an {member} -> {sent} DM(s).")
        except Exception:
            pass
    except Exception:
        pass
