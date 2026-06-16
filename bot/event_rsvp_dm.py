from __future__ import annotations

import json
import asyncio
from pathlib import Path
from typing import Dict, Optional, Iterable, List, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import View, button
from discord.enums import ButtonStyle

try:
    from bot.event_dm_prefs import is_dm_enabled  # type: ignore
except ModuleNotFoundError:
    from event_dm_prefs import is_dm_enabled  # type: ignore

try:
    from bot.raid_stats import record_response  # type: ignore
except ModuleNotFoundError:
    from raid_stats import record_response  # type: ignore


TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RSVP_FILE = DATA_DIR / "event_rsvp.json"
DM_CFG_FILE = DATA_DIR / "event_rsvp_cfg.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"
ATTENDANCE_FILE = DATA_DIR / "event_attendance.json"


def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg: Dict[str, dict] = _load(DM_CFG_FILE, {})
attendance_store: Dict[str, dict] = _load(ATTENDANCE_FILE, {})


def save_store():
    _save(RSVP_FILE, store)


def save_cfg():
    _save(DM_CFG_FILE, cfg)


def save_attendance():
    _save(ATTENDANCE_FILE, attendance_store)


async def _log(client: discord.Client, guild_id: int, text: str):
    gcfg = cfg.get(str(guild_id)) or {}
    ch_id = int(gcfg.get("LOG_CH", 0) or 0)

    if not ch_id:
        return

    guild = client.get_guild(guild_id)

    if not guild:
        return

    ch = guild.get_channel(ch_id)

    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(f"[RSVP-DM] {text}")
        except Exception:
            pass


def _safe_name(name: str) -> str:
    return (name or "").replace("@", "@\u200b").strip() or "Unbekannt"


def _current_display_name(
    member: Optional[discord.Member],
    fallback_user: Optional[discord.abc.User] = None
) -> str:
    if member is not None:
        return _safe_name(member.display_name)

    if fallback_user is not None:
        return _safe_name(
            getattr(fallback_user, "display_name", None)
            or getattr(fallback_user, "name", "Unbekannt")
        )

    return "Unbekannt"


def _participant_entry(
    uid: int,
    name: str,
    guild_label: str = "",
    source_guild_id: int = 0,
) -> dict:
    obj = {
        "id": int(uid),
        "name": _safe_name(name),
    }

    if guild_label:
        obj["guild_label"] = str(guild_label).strip()

    if source_guild_id:
        obj["source_guild_id"] = int(source_guild_id)

    return obj


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
            member = guild.get_member(uid)

            if member:
                return _safe_name(member.display_name)

        return stored or (f"User {uid}" if uid else "Unbekannt")

    try:
        uid = int(entry)
    except Exception:
        return "Unbekannt"

    if guild:
        member = guild.get_member(uid)

        if member:
            return _safe_name(member.display_name)

    return f"User {uid}"


def _short_guild_label(label: str) -> str:
    label = (label or "").strip()

    if not label:
        return ""

    clean = "".join(ch for ch in label if ch.isalnum())

    if len(clean) <= 4:
        return clean

    return clean[:3].title()


def _entry_display_name(entry: Any, guild: Optional[discord.Guild] = None) -> str:
    name = _entry_name(entry, guild)

    if isinstance(entry, dict):
        label = str(entry.get("guild_label", "") or "").strip()
        short = _short_guild_label(label)

        if short:
            return f"{name} ({short})"

    return name

def _source_label_for_inter(inter: discord.Interaction, obj: dict) -> tuple[str, int]:
    source_guild_id = int(inter.guild_id or 0)

    if not source_guild_id:
        source_guild_id = int(obj.get("guild_id", 0) or 0)

    for mirror in obj.get("mirrors", []) or []:
        try:
            if int(mirror.get("guild_id", 0) or 0) == source_guild_id:
                return str(mirror.get("short_label", "") or mirror.get("label", "") or mirror.get("discord_name", "") or "").strip(), source_guild_id
        except Exception:
            continue

    guild = inter.client.get_guild(source_guild_id) if source_guild_id else None
    return (guild.name if guild else ""), source_guild_id


def _is_alliance_event(obj: dict) -> bool:
    return str(obj.get("scope", "") or "").lower() == "alliance"


def _maybe_entry(uid: int, name: str, label: str) -> dict:
    return {
        "id": int(uid),
        "name": _safe_name(name),
        "label": (label or "").strip(),
    }


def _maybe_name_and_label(
    entry: Any,
    uid_fallback: int,
    guild: Optional[discord.Guild] = None
) -> tuple[str, str]:
    if isinstance(entry, dict):
        uid = int(entry.get("id", uid_fallback) or uid_fallback)
        label = str(entry.get("label", "") or "").strip()
        name = _entry_name({"id": uid, "name": entry.get("name", "")}, guild)
        return name, label

    label = str(entry or "").strip()

    if guild:
        member = guild.get_member(uid_fallback)

        if member:
            return _safe_name(member.display_name), label

    return f"User {uid_fallback}", label


def _format_dm_text(
    title: str,
    when: datetime,
    channel_name_or_ref: str,
    description: str | None,
    intro_line: str | None = None
) -> str:
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

    if "dm_messages" not in obj or not isinstance(obj["dm_messages"], dict):
        obj["dm_messages"] = {}

    if "mirrors" not in obj or not isinstance(obj.get("mirrors"), list):
        obj["mirrors"] = []

    obj.setdefault("scope", "single")
    obj.setdefault("voice_enabled", False)
    obj.setdefault("voice_channel_id", 0)
    obj.setdefault("voice_return_channel_id", 0)
    obj.setdefault("voice_cleanup_done", False)

    migrated = False

    for role_key in ("TANK", "HEAL", "DPS", "BANK"):
        new_list = []

        for raw in obj["yes"].get(role_key, []):
            if isinstance(raw, dict):
                entry = {
                    "id": int(raw.get("id", 0) or 0),
                    "name": _safe_name(str(raw.get("name", "") or f"User {raw.get('id', 0)}")),
                }

                guild_label = str(raw.get("guild_label", "") or "").strip()
                source_guild_id = int(raw.get("source_guild_id", 0) or 0)

                if guild_label:
                    entry["guild_label"] = guild_label

                if source_guild_id:
                    entry["source_guild_id"] = source_guild_id

                new_list.append(entry)
            else:
                try:
                    uid = int(raw)
                    new_list.append(_participant_entry(uid, f"User {uid}"))
                except Exception:
                    continue

        if obj["yes"].get(role_key) != new_list:
            obj["yes"][role_key] = new_list
            migrated = True

    maybe_new = {}

    for uid_str, raw in obj.get("maybe", {}).items():
        try:
            uid_i = int(uid_str)
        except Exception:
            continue

        if isinstance(raw, dict):
            entry = {
                "id": uid_i,
                "name": _safe_name(str(raw.get("name", "") or f"User {uid_i}")),
                "label": str(raw.get("label", "") or "").strip(),
            }

            guild_label = str(raw.get("guild_label", "") or "").strip()
            source_guild_id = int(raw.get("source_guild_id", 0) or 0)

            if guild_label:
                entry["guild_label"] = guild_label

            if source_guild_id:
                entry["source_guild_id"] = source_guild_id

            maybe_new[str(uid_i)] = entry
        else:
            maybe_new[str(uid_i)] = _maybe_entry(
                uid_i,
                f"User {uid_i}",
                str(raw or "").strip()
            )

    if obj.get("maybe") != maybe_new:
        obj["maybe"] = maybe_new
        migrated = True

    no_new = []

    for raw in obj.get("no", []):
        if isinstance(raw, dict):
            entry = {
                "id": int(raw.get("id", 0) or 0),
                "name": _safe_name(str(raw.get("name", "") or f"User {raw.get('id', 0)}")),
            }

            guild_label = str(raw.get("guild_label", "") or "").strip()
            source_guild_id = int(raw.get("source_guild_id", 0) or 0)

            if guild_label:
                entry["guild_label"] = guild_label

            if source_guild_id:
                entry["source_guild_id"] = source_guild_id

            no_new.append(entry)
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
        "DPS": int(g.get("DPS", 0) or 0),
    }


def _primary_label(member: Optional[discord.Member], rid_map: Dict[str, int]) -> str:
    if member is None:
        return ""

    guild = member.guild

    r = guild.get_role(rid_map.get("TANK", 0) or 0)
    if r and r in getattr(member, "roles", []):
        return "Tank"

    r = guild.get_role(rid_map.get("HEAL", 0) or 0)
    if r and r in getattr(member, "roles", []):
        return "Heal"

    r = guild.get_role(rid_map.get("DPS", 0) or 0)
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

        guild = inter.client.get_guild(gid)

        if not guild:
            return None

        return guild.get_member(inter.user.id)

    except Exception:
        return None


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

    voted = _voters_set(obj)

    if _is_alliance_event(obj):
        vote_line = f"🗳️ Abgestimmt: **{len(voted)}**"
        hint_line = "💡 Allianz-Raid: Partner-Server stimmen direkt über diesen Post ab. DMs gibt es nur für den Home-Server."
    else:
        eligible = _eligible_members(guild, obj)
        vote_line = f"🗳️ Abgestimmt: **{len(voted)}** / **{len(eligible)}**"
        hint_line = "💡 Wenn du keine DM bekommst oder sie deaktiviert hast: nutze die Buttons direkt unter dieser Ankündigung."

    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=(
            (obj.get("description", "") or "") +
            f"\n\n🕒 Zeit: {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"
            f"\n{vote_line}"
            f"\n{hint_line}"
        ).strip(),
        color=discord.Color.blurple()
    )

    tank_names = [_entry_display_name(u, guild) for u in yes.get("TANK", [])]
    heal_names = [_entry_display_name(u, guild) for u in yes.get("HEAL", [])]
    dps_names = [_entry_display_name(u, guild) for u in yes.get("DPS", [])]
    bank_names = [_entry_display_name(u, guild) for u in yes.get("BANK", [])]

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
        guild_label = str(entry.get("guild_label", "") or "").strip() if isinstance(entry, dict) else ""
        if guild_label:
            short = _short_guild_label(guild_label)
            name = f"{name} ({short})" if short else name
        label_txt = f" ({label})" if label else ""
        maybe_lines.append(f"{name}{label_txt}")

    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_entry_display_name(u, guild) for u in no]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    tr_id = int(obj.get("target_role_id", 0) or 0)

    if tr_id:
        role = guild.get_role(tr_id)

        if role:
            emb.add_field(name="🎯 Zielgruppe", value=role.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="DM-Buttons und Server-Buttons schreiben beide in dieselbe Anmeldung.")

    return emb


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


def _portal_protected_titles() -> set[str]:
    return {
        "⚜️ Ebolus Kommandozentrale",
        "🏰 ebolus – Gildenmenü",

        "👤 Persönlich",
        "🎁 Loot & Bedarf",
        "📅 Gilde",
        "🛡️ Kontakt & Hilfe",

        "👤 Dein Gildenprofil",
        "📅 Ebolus Gildenkalender",
        "📅 Gildenkalender – ebolus",
        "📅 Gilden-Events",
        "❓ Hilfe – Ebolus Gildenbot",
        "❓ Hilfe – ebolus Gildenbot",
        "👥 Ebolus Mitglieder",
        "👥 Mitgliederliste – ebolus",
        "📜 Regeln & Lootsystem",
        "📜 Regeln & Lootsystem – ebolus",
        "🎁 Needliste – ebolus",
    }


def _is_portal_message(msg: discord.Message) -> bool:
    if not msg.embeds:
        return False

    title = msg.embeds[0].title or ""
    return title in _portal_protected_titles()


def _pending_dm_message_ids_to_keep(user_id: int, current_msg_id: str | None = None) -> set[int]:
    """
    Behält offene Raid-DMs anderer Events, wenn:
    - Event noch nicht vorbei ist
    - User dort noch nicht abgestimmt hat
    - es nicht die gerade geklickte Event-DM ist
    """
    keep: set[int] = set()
    now = datetime.now(TZ)

    for msg_id, obj in list(store.items()):
        try:
            if current_msg_id and str(msg_id) == str(current_msg_id):
                continue

            _init_event_shape(obj)

            when = datetime.fromisoformat(obj.get("when_iso"))
            if now > when + timedelta(hours=2):
                continue

            if int(user_id) in _voters_set(obj):
                continue

            dm_map = obj.get("dm_messages") or {}
            mid = dm_map.get(str(user_id))

            if mid:
                keep.add(int(mid))

        except Exception:
            continue

    return keep


def _known_dm_message_ids_for_user(user_id: int) -> dict[int, str]:
    """
    Map: DM-Message-ID -> Event-Message-ID
    """
    known: dict[int, str] = {}

    for msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
            dm_map = obj.get("dm_messages") or {}
            mid = dm_map.get(str(user_id))

            if mid:
                known[int(mid)] = str(msg_id)

        except Exception:
            continue

    return known


async def _delete_irrelevant_bot_dm_messages_for_user(
    client: discord.Client,
    user_id: int,
    current_msg_id: str | None = None,
    limit: int = 200
) -> int:
    """
    Löscht alte/erledigte Bot-DMs.
    Schützt:
    - aktives Gildenmenü und Portal-Unterseiten
    - offene Raid-DMs anderer Events, bei denen der User noch nicht abgestimmt hat

    Löscht:
    - aktuelle Raid-DM nach Auswahl
    - alte Bot-DMs
    - erledigte Raid-DMs
    - gestartete/veraltete Raid-DMs

    Wichtig:
    Das Gildenmenü wird hier NICHT neu gesendet und NICHT editiert.
    """
    if client.user is None:
        return 0

    user = client.get_user(int(user_id))

    if user is None:
        try:
            user = await client.fetch_user(int(user_id))
        except Exception:
            return 0

    keep_dm_ids = _pending_dm_message_ids_to_keep(user_id, current_msg_id=current_msg_id)
    known_dm_ids = _known_dm_message_ids_for_user(user_id)

    deleted = 0
    deleted_or_stale_dm_ids: set[int] = set()

    try:
        dm = user.dm_channel or await user.create_dm()

        async for msg in dm.history(limit=limit):
            try:
                if msg.author.id != client.user.id:
                    continue

                if _is_portal_message(msg):
                    continue

                if msg.id in keep_dm_ids:
                    continue

                await msg.delete()
                deleted += 1
                deleted_or_stale_dm_ids.add(int(msg.id))
                await asyncio.sleep(0.05)

            except Exception:
                pass

    except Exception:
        pass

    changed_store = False

    for dm_id, event_msg_id in known_dm_ids.items():
        try:
            if dm_id in keep_dm_ids:
                continue

            obj = store.get(str(event_msg_id))
            if not obj:
                continue

            dm_map = obj.get("dm_messages") or {}

            if dm_map.get(str(user_id)) == dm_id:
                dm_map.pop(str(user_id), None)
                obj["dm_messages"] = dm_map
                changed_store = True

        except Exception:
            continue

    if changed_store:
        save_store()

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



def _attendance_guild_bucket(guild_id: int) -> dict:
    g = attendance_store.get(str(guild_id)) or {}
    g.setdefault("events", {})
    attendance_store[str(guild_id)] = g
    return g


def _attendance_participants_from_event(obj: dict) -> list[dict]:
    _init_event_shape(obj)
    out: list[dict] = []
    seen: set[int] = set()

    for role_key in ("TANK", "HEAL", "DPS", "BANK"):
        for entry in obj.get("yes", {}).get(role_key, []) or []:
            uid = _entry_user_id(entry)
            if not uid or uid in seen:
                continue
            seen.add(uid)
            out.append({
                "id": int(uid),
                "name": _entry_name(entry),
                "signup": role_key,
            })

    return out


def ensure_attendance_snapshot(client: discord.Client, msg_id: str, obj: dict) -> dict | None:
    """
    Speichert eine Event-/Teilnehmer-Kopie für die spätere Anwesenheitsprüfung.
    Dadurch bleibt die Anwesenheit auswertbar, auch wenn der normale RSVP-Post später bereinigt wird.
    """
    try:
        _init_event_shape(obj)
        guild_id = int(obj.get("guild_id", 0) or 0)
        if not guild_id:
            return None

        g = _attendance_guild_bucket(guild_id)
        events = g.setdefault("events", {})
        event_id = str(msg_id)
        snap = events.get(event_id)

        participants = _attendance_participants_from_event(obj)

        if not isinstance(snap, dict):
            snap = {
                "event_id": event_id,
                "guild_id": guild_id,
                "channel_id": int(obj.get("channel_id", 0) or 0),
                "message_id": event_id,
                "title": str(obj.get("title", "Event") or "Event"),
                "description": str(obj.get("description", "") or ""),
                "when_iso": str(obj.get("when_iso", "") or ""),
                "created_at": datetime.now(TZ).isoformat(),
                "participants": participants,
                "attendance": {},
            }
        else:
            # Teilnehmerliste aktualisieren, aber bereits gesetzte Anwesenheit behalten.
            old_att = snap.get("attendance") if isinstance(snap.get("attendance"), dict) else {}
            snap["guild_id"] = guild_id
            snap["channel_id"] = int(obj.get("channel_id", 0) or 0)
            snap["message_id"] = event_id
            snap["title"] = str(obj.get("title", "Event") or "Event")
            snap["description"] = str(obj.get("description", "") or "")
            snap["when_iso"] = str(obj.get("when_iso", "") or "")
            snap["participants"] = participants
            snap["attendance"] = old_att

        events[event_id] = snap
        save_attendance()
        return snap

    except Exception as e:
        print(f"[event_rsvp_dm] Attendance-Snapshot Fehler: {e!r}")
        return None


def get_attendance_events_for_guild(guild_id: int) -> list[dict]:
    g = _attendance_guild_bucket(int(guild_id))
    events = list((g.get("events") or {}).values())

    def _key(ev: dict):
        try:
            return datetime.fromisoformat(str(ev.get("when_iso", "")))
        except Exception:
            return datetime.min.replace(tzinfo=TZ)

    events.sort(key=_key, reverse=True)
    return events


def get_attendance_event(guild_id: int, event_id: str) -> dict | None:
    g = _attendance_guild_bucket(int(guild_id))
    ev = (g.get("events") or {}).get(str(event_id))
    return ev if isinstance(ev, dict) else None


def set_attendance_status(guild_id: int, event_id: str, user_id: int, status: str, marked_by: int) -> bool:
    ev = get_attendance_event(int(guild_id), str(event_id))
    if not ev:
        return False

    valid = {"present", "absent", "excused"}
    attendance = ev.setdefault("attendance", {})

    if status == "clear":
        attendance.pop(str(user_id), None)
    elif status in valid:
        attendance[str(user_id)] = {
            "status": status,
            "marked_by": int(marked_by),
            "marked_at": datetime.now(TZ).isoformat(),
        }
    else:
        return False

    save_attendance()
    return True


async def _cleanup_event_voice_for_obj(client: discord.Client, obj: dict) -> bool:
    """
    Verschiebt Mitglieder aus einem Event-Voice in den gespeicherten Sammel-Voice
    und löscht danach den Event-Voice. Gibt True zurück, wenn der Event-Status
    geändert wurde.
    """
    try:
        _init_event_shape(obj)

        if bool(obj.get("voice_cleanup_done", False)):
            return False

        voice_channel_id = int(obj.get("voice_channel_id", 0) or 0)

        if not voice_channel_id:
            return False

        guild_id = int(obj.get("guild_id", 0) or 0)
        guild = client.get_guild(guild_id) if guild_id else None

        if guild is None:
            return False

        channel = guild.get_channel(voice_channel_id)

        if not isinstance(channel, discord.VoiceChannel):
            obj["voice_cleanup_done"] = True
            return True

        return_channel_id = int(obj.get("voice_return_channel_id", 0) or 0)
        return_channel = guild.get_channel(return_channel_id) if return_channel_id else None

        moved = 0

        if isinstance(return_channel, discord.VoiceChannel):
            for member in list(channel.members):
                try:
                    await member.move_to(return_channel, reason="Event-Voice wird automatisch geschlossen")
                    moved += 1
                    await asyncio.sleep(0.1)
                except Exception:
                    continue

        try:
            await channel.delete(reason="Event-Voice automatisch nach Eventende gelöscht")
        except Exception as e:
            print(f"[event_rsvp_dm] Event-Voice konnte nicht gelöscht werden: {e!r}")
            return False

        obj["voice_cleanup_done"] = True
        print(f"[event_rsvp_dm] Event-Voice gelöscht: {voice_channel_id}, verschoben: {moved}")
        return True

    except Exception as e:
        print(f"[event_rsvp_dm] Event-Voice Cleanup Fehler: {e!r}")
        return False


async def delete_pending_dm_messages_for_started_events(client: discord.Client) -> int:
    changed = 0
    now = datetime.now(TZ)

    for _msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
            when = datetime.fromisoformat(obj.get("when_iso"))
        except Exception:
            continue

        if now >= when + timedelta(hours=2):
            try:
                if await _cleanup_event_voice_for_obj(client, obj):
                    changed += 1
            except Exception as e:
                print(f"[delete_pending_dm_messages_for_started_events] Voice-Cleanup Fehler: {e!r}")

        if now < when:
            continue

        dm_map = obj.get("dm_messages") or {}

        if not dm_map:
            continue

        for uid_str in list(dm_map.keys()):
            try:
                uid = int(uid_str)
            except Exception:
                obj["dm_messages"].pop(uid_str, None)
                changed += 1
                continue

            voted = uid in _voters_set(obj)

            try:
                ok = await _delete_dm_message_for_user(client, obj, uid)

                if ok or voted or str(uid) not in obj.get("dm_messages", {}):
                    changed += 1

            except Exception:
                obj["dm_messages"].pop(str(uid), None)
                changed += 1

    return changed


async def _push_overview(client: discord.Client, msg_id: str, obj: dict):
    _init_event_shape(obj)

    if _is_alliance_event(obj) and obj.get("mirrors"):
        master_id = int(obj.get("message_id", msg_id) or msg_id)

        for mirror in list(obj.get("mirrors") or []):
            try:
                guild = client.get_guild(int(mirror.get("guild_id", 0) or 0))

                if not guild:
                    continue

                ch = guild.get_channel(int(mirror.get("channel_id", 0) or 0))

                if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                    continue

                msg = await ch.fetch_message(int(mirror.get("message_id", 0) or 0))
                emb = build_embed(guild, obj)
                await msg.edit(embed=emb, view=ServerRaidView(master_id))

            except Exception:
                continue

        return

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


async def _refresh_existing_portal_for_user(client: discord.Client, guild_id: int, user_id: int) -> bool:
    """
    Aktualisiert nur ein bereits vorhandenes Gildenmenü per msg.edit(...).
    Wichtig: Diese Funktion sendet KEINE neue DM.
    """
    try:
        try:
            from bot.member_portal import (  # type: ignore
                _fetch_portal_message,
                _main_menu_embed,
                MemberPortalMainView,
            )
        except ModuleNotFoundError:
            from member_portal import (  # type: ignore
                _fetch_portal_message,
                _main_menu_embed,
                MemberPortalMainView,
            )

        guild = client.get_guild(int(guild_id))

        if not guild:
            return False

        member = guild.get_member(int(user_id))

        if not member or member.bot:
            return False

        msg = await _fetch_portal_message(client, guild.id, member.id)

        if msg is None:
            return False

        await msg.edit(embed=_main_menu_embed(guild, member), view=MemberPortalMainView())
        return True

    except Exception as e:
        print(f"[event_rsvp_dm] Portal-Refresh User Fehler: {e!r}")
        return False


async def _refresh_existing_portals_for_members(
    client: discord.Client,
    guild: discord.Guild,
    members: Iterable[discord.Member],
    delay: float = 0.08,
) -> int:
    refreshed = 0
    seen: set[int] = set()

    for member in members:
        try:
            if member.bot or member.id in seen:
                continue

            seen.add(member.id)

            if await _refresh_existing_portal_for_user(client, guild.id, member.id):
                refreshed += 1

            if delay > 0:
                await asyncio.sleep(delay)

        except Exception:
            continue

    return refreshed


async def _refresh_existing_portals_for_event(client: discord.Client, guild: discord.Guild, obj: dict) -> int:
    """Aktualisiert bestehende Portal-Startseiten der Event-Zielgruppe. Sendet keine neuen DMs."""
    try:
        _init_event_shape(obj)
        members = _eligible_members(guild, obj)
        return await _refresh_existing_portals_for_members(client, guild, members)
    except Exception as e:
        print(f"[event_rsvp_dm] Portal-Refresh Event Fehler: {e!r}")
        return 0


def _schedule_portal_refresh_for_user(client: discord.Client, guild_id: int, user_id: int) -> None:
    try:
        asyncio.create_task(_refresh_existing_portal_for_user(client, guild_id, user_id))
    except Exception:
        pass


def _schedule_portal_refresh_for_event(client: discord.Client, guild: Optional[discord.Guild], obj: dict) -> None:
    if guild is None:
        return

    try:
        asyncio.create_task(_refresh_existing_portals_for_event(client, guild, obj))
    except Exception:
        pass


async def _member_allowed_for_target_role(inter: discord.Interaction, obj: dict, user_id: int) -> tuple[bool, str]:
    """Blockt RSVP über Serverbuttons, wenn ein Event eine Zielrolle hat."""
    role_id = int(obj.get("target_role_id", 0) or 0)

    if not role_id:
        return True, ""

    guild_id = int(inter.guild_id or obj.get("guild_id", 0) or 0)
    guild = inter.client.get_guild(guild_id) if guild_id else None

    if guild is None:
        return False, "Dieses Event hat eine Zielrolle. Dein Server konnte nicht geprüft werden."

    role = guild.get_role(role_id)

    if role is None:
        return False, "Die Zielrolle dieses Events wurde nicht gefunden. Bitte melde dich bei der Gildenleitung."

    member = guild.get_member(int(user_id))

    if member is None:
        try:
            member = await guild.fetch_member(int(user_id))
        except Exception:
            member = None

    if member is None or member.bot or role not in getattr(member, "roles", []):
        return False, f"Du gehörst nicht zur Zielgruppe dieses Events ({role.mention}) und kannst dich dafür nicht anmelden."

    return True, ""


def _reminder_label(minutes: int, target: str) -> str:
    if minutes >= 1440 and minutes % 1440 == 0:
        time_txt = f"{minutes // 1440} Tag(e) vorher"
    elif minutes >= 60 and minutes % 60 == 0:
        time_txt = f"{minutes // 60} Stunde(n) vorher"
    else:
        time_txt = f"{minutes} Minute(n) vorher"

    if target == "yes":
        target_txt = "angemeldete Teilnehmer"
    elif target == "all":
        target_txt = "Zielgruppe"
    else:
        target_txt = "fehlende Abstimmungen"

    return f"{time_txt} an {target_txt}"


def _reminder_voters(obj: dict) -> set[int]:
    try:
        _init_event_shape(obj)
        return _voters_set(obj)
    except Exception:
        return set()


def _reminder_yes_participants(obj: dict) -> set[int]:
    ids: set[int] = set()
    try:
        _init_event_shape(obj)
        for key in ("TANK", "HEAL", "DPS", "BANK"):
            for entry in obj.get("yes", {}).get(key, []) or []:
                uid = _entry_user_id(entry)
                if uid:
                    ids.add(uid)
    except Exception:
        pass
    return ids


def _reminder_target_members(guild: discord.Guild, obj: dict, target: str) -> list[discord.Member]:
    try:
        members = _eligible_members(guild, obj)
    except Exception:
        members = [m for m in guild.members if not m.bot]

    if target == "yes":
        allowed = _reminder_yes_participants(obj)
        return [m for m in members if m.id in allowed and not m.bot]

    if target == "all":
        return [m for m in members if not m.bot]

    voted = _reminder_voters(obj)
    return [m for m in members if m.id not in voted and not m.bot]


async def _send_event_reminder(client: discord.Client, msg_id: str, obj: dict, reminder: dict) -> int:
    guild_id = int(obj.get("guild_id", 0) or 0)
    guild = client.get_guild(guild_id) if guild_id else None

    if guild is None:
        return 0

    try:
        when = datetime.fromisoformat(obj.get("when_iso", ""))
    except Exception:
        return 0

    minutes = int(reminder.get("minutes", 0) or 0)
    target = str(reminder.get("target", "missing") or "missing")
    label = _reminder_label(minutes, target)
    members = _reminder_target_members(guild, obj, target)

    sent = 0
    for member in members:
        try:
            if not is_dm_enabled(guild.id, member.id):
                continue

            if target == "yes":
                intro = "Reminder: Du bist für dieses Event angemeldet."
            elif target == "all":
                intro = "Reminder für dieses Event."
            else:
                intro = "Reminder: Du hast für dieses Event noch nicht abgestimmt."

            dm_text = _format_dm_text(
                title=str(obj.get("title", "Event")),
                when=when,
                channel_name_or_ref=f"Übersicht: <#{obj.get('channel_id')}>",
                description=obj.get("description"),
                intro_line=intro,
            )
            dm_text += f"\n\n⏰ **Reminder:** {label}"

            dm_msg = await member.send(dm_text, view=RaidView(int(msg_id)))
            obj.setdefault("dm_messages", {})[str(member.id)] = int(dm_msg.id)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    return sent


@tasks.loop(minutes=1)
async def event_reminder_loop():
    now = datetime.now(TZ)
    changed = False

    for msg_id, obj in list(store.items()):
        try:
            _init_event_shape(obj)
            when = datetime.fromisoformat(obj.get("when_iso", ""))

            if now >= when:
                try:
                    if ensure_attendance_snapshot(event_reminder_loop._client, str(msg_id), obj):  # type: ignore[attr-defined]
                        changed = True
                except Exception as e:
                    print(f"[event_reminder_loop] Attendance-Snapshot Fehler: {e!r}")

            if now >= when + timedelta(hours=2):
                try:
                    if await _cleanup_event_voice_for_obj(event_reminder_loop._client, obj):  # type: ignore[attr-defined]
                        changed = True
                except Exception as e:
                    print(f"[event_reminder_loop] Voice-Cleanup Fehler: {e!r}")

            reminders = obj.get("reminders") or []

            if not isinstance(reminders, list) or not reminders:
                continue

            if now > when + timedelta(hours=2):
                continue

            sent_map = obj.setdefault("reminder_sent", {})

            for idx, reminder in enumerate(reminders):
                try:
                    minutes = int(reminder.get("minutes", 0) or 0)
                    if minutes <= 0:
                        continue

                    due_at = when - timedelta(minutes=minutes)
                    key = f"{idx}:{minutes}:{reminder.get('target', 'missing')}"

                    if sent_map.get(key):
                        continue

                    if now < due_at:
                        continue

                    sent = await _send_event_reminder(event_reminder_loop._client, str(msg_id), obj, reminder)  # type: ignore[attr-defined]
                    sent_map[key] = {"sent_at": now.isoformat(), "sent": int(sent)}
                    changed = True

                except Exception as e:
                    print(f"[event_reminder_loop] Reminder Fehler: {e!r}")
                    continue

        except Exception as e:
            print(f"[event_reminder_loop] Event Fehler: {e!r}")
            continue

    if changed:
        save_store()


async def apply_rsvp(inter: discord.Interaction, msg_id: str, group: str) -> tuple[bool, str]:
    obj = store.get(str(msg_id))

    if not obj:
        return False, "Dieses Event existiert nicht mehr."

    _init_event_shape(obj)

    uid = inter.user.id

    allowed, reason = await _member_allowed_for_target_role(inter, obj, uid)
    if not allowed:
        return False, f"❌ {reason}"

    member = _member_from_event(inter, obj)
    display_name = _current_display_name(member, inter.user)
    guild_label, source_guild_id = _source_label_for_inter(inter, obj) if _is_alliance_event(obj) else ("", int(inter.guild_id or obj.get("guild_id", 0) or 0))

    if group in ("TANK", "HEAL", "DPS"):
        response_key = "yes"
    elif group == "BANK":
        response_key = "bank"
    elif group == "MAYBE":
        response_key = "maybe"
    else:
        response_key = "no"

    for k in ("TANK", "HEAL", "DPS", "BANK"):
        obj["yes"][k] = [
            entry for entry in obj["yes"].get(k, [])
            if _entry_user_id(entry) != uid
        ]

    obj["no"] = [
        entry for entry in obj.get("no", [])
        if _entry_user_id(entry) != uid
    ]

    obj["maybe"].pop(str(uid), None)

    if group in ("TANK", "HEAL", "DPS"):
        obj["yes"][group].append(_participant_entry(uid, display_name, guild_label, source_guild_id))
        text = f"Angemeldet als **{group}**."

    elif group == "BANK":
        obj["yes"]["BANK"].append(_participant_entry(uid, display_name, guild_label, source_guild_id))
        text = "Als **Bank / Reserve** eingetragen."

    elif group == "MAYBE":
        rid_map = get_role_ids_for_guild(int(obj["guild_id"]))
        label = _primary_label(member, rid_map)
        maybe_obj = _maybe_entry(uid, display_name, label)
        if guild_label:
            maybe_obj["guild_label"] = guild_label
        if source_guild_id:
            maybe_obj["source_guild_id"] = int(source_guild_id)
        obj["maybe"][str(uid)] = maybe_obj
        text = "Als **Vielleicht** eingetragen."

    elif group == "NO":
        obj["no"].append(_participant_entry(uid, display_name, guild_label, source_guild_id))
        text = "Als **Abgemeldet** eingetragen."

    else:
        return False, "Ungültige Auswahl."

    save_store()
    record_response(int(obj["guild_id"]), uid, str(msg_id), response_key)
    await _push_overview(inter.client, str(msg_id), obj)

    # Gildenmenü-Startseite aktualisieren, aber nur vorhandene Portal-DM bearbeiten.
    # Es wird keine neue Portal-DM gesendet.
    refresh_guild_ids = {int(obj.get("guild_id", 0) or 0)}

    if source_guild_id:
        refresh_guild_ids.add(int(source_guild_id))

    for gid in refresh_guild_ids:
        if gid:
            _schedule_portal_refresh_for_user(inter.client, gid, uid)

    return True, text


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
        try:
            await _delete_irrelevant_bot_dm_messages_for_user(
                inter.client,
                inter.user.id,
                current_msg_id=self.msg_id,
                limit=200
            )
        except Exception:
            pass

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


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True

    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False

    try:
        leader_cfg = _load(LEADER_CONTACT_CFG_FILE, {})
        c = leader_cfg.get(str(inter.guild.id)) or {}
        role_id = int(c.get("leader_role_id", 0) or 0)

        if not role_id:
            return False

        role = inter.guild.get_role(role_id)
        return bool(role and role in inter.user.roles)

    except Exception:
        return False


def _alliance_home_guild_id(default: int = 0) -> int:
    try:
        try:
            from bot.alliance_config import _home_guild_id  # type: ignore
        except ModuleNotFoundError:
            from alliance_config import _home_guild_id  # type: ignore

        return int(_home_guild_id(default=default) or default or 0)

    except Exception:
        return int(default or 0)


def _require_alliance_home_leader(inter: discord.Interaction) -> tuple[bool, str]:
    if inter.guild is None or inter.guild_id is None:
        return False, "❌ Nur im Server nutzbar."

    home_id = _alliance_home_guild_id(default=inter.guild_id)

    if int(inter.guild_id) != int(home_id):
        return False, "❌ Allianz-Raids können nur auf dem Home-/Ebolus-Server erstellt werden."

    if not _is_leader_or_admin(inter):
        return False, "❌ Nur Leader/Admins."

    return True, ""


def _import_alliance_config():
    try:
        from bot.alliance_config import get_alliance_group  # type: ignore
        return get_alliance_group
    except ModuleNotFoundError:
        from alliance_config import get_alliance_group  # type: ignore
        return get_alliance_group


def _import_alliance_template():
    try:
        from bot.alliance_config import get_alliance_template  # type: ignore
        return get_alliance_template
    except ModuleNotFoundError:
        from alliance_config import get_alliance_template  # type: ignore
        return get_alliance_template


ALLIANCE_EVENT_TYPES = [
    "NM Raid",
    "HM Raid",
    "PvP Schlacht",
    "Dimensionsprüfung",
]


def _normalize_alliance_event_type(value: str) -> str:
    raw = (value or "").strip().lower()

    aliases = {
        "nm": "NM Raid",
        "nm raid": "NM Raid",
        "normal raid": "NM Raid",
        "normalraid": "NM Raid",
        "hm": "HM Raid",
        "hm raid": "HM Raid",
        "hardmode": "HM Raid",
        "hardmode raid": "HM Raid",
        "pvp": "PvP Schlacht",
        "pvp schlacht": "PvP Schlacht",
        "schlacht": "PvP Schlacht",
        "dimensionsprüfung": "Dimensionsprüfung",
        "dimensionspruefung": "Dimensionsprüfung",
        "dimension": "Dimensionsprüfung",
        "dimensionen": "Dimensionsprüfung",
    }

    if raw in aliases:
        return aliases[raw]

    for event_type in ALLIANCE_EVENT_TYPES:
        if event_type.lower() == raw:
            return event_type

    return ""


def _alliance_event_type_text() -> str:
    return ", ".join(ALLIANCE_EVENT_TYPES)


def _server_channel_id_for_event(server_cfg: dict, event_type: str) -> int:
    event_channels = server_cfg.get("event_channels") or {}
    channel_obj = event_channels.get(event_type) or {}
    cid = int(channel_obj.get("channel_id", 0) or 0)

    if cid:
        return cid

    return int(server_cfg.get("channel_id", 0) or 0)


async def _send_home_dms_for_alliance_event(
    guild: discord.Guild,
    obj: dict,
    master_msg_id: int,
    channel_name_or_ref: str,
) -> tuple[int, int]:
    sent = 0
    skipped_opt_out = 0

    for member in _eligible_members(guild, obj):
        if not is_dm_enabled(guild.id, member.id):
            skipped_opt_out += 1
            continue

        try:
            when = datetime.fromisoformat(obj["when_iso"])
            dm_text = _format_dm_text(
                title=str(obj.get("title", "Event")),
                when=when,
                channel_name_or_ref=channel_name_or_ref,
                description=obj.get("description"),
                intro_line="Wähle unten deine Teilnahme:"
            )

            dm_msg = await member.send(dm_text, view=RaidView(int(master_msg_id)))
            obj["dm_messages"][str(member.id)] = int(dm_msg.id)
            sent += 1
            await asyncio.sleep(0.05)

        except Exception:
            pass

    return sent, skipped_opt_out


async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
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

    try:
        event_reminder_loop._client = client  # type: ignore[attr-defined]
        if not event_reminder_loop.is_running():
            event_reminder_loop.start()
            print("⏰ Event-Reminder-Task gestartet.")
    except Exception as e:
        print(f"[event_rsvp_dm] Reminder-Task Startfehler: {e!r}")

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
        c["DPS"] = int(dps_role.id)
        cfg[str(inter.guild_id)] = c
        save_cfg()

        await inter.followup.send(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

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
            "reminders": [],
            "reminder_sent": {},
            "dm_messages": {}
        }

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

        for member in _eligible_members(inter.guild, obj):
            if not is_dm_enabled(inter.guild_id, member.id):
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

                dm_msg = await member.send(dm_text, view=RaidView(int(msg.id)))
                obj["dm_messages"][str(member.id)] = int(dm_msg.id)
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

        # Bestehende Gildenmenüs der Zielgruppe aktualisieren, ohne neue Portal-DMs zu senden.
        _schedule_portal_refresh_for_event(inter.client, inter.guild, obj)

    async def _create_alliance_raid_impl(
        inter: discord.Interaction,
        group: str,
        event_type: str,
        title: str,
        date: str,
        time: str,
        description: Optional[str] = None,
        target_role: Optional[discord.Role] = None,
        image_url: Optional[str] = None,
    ):
        if inter.guild is None or inter.guild_id is None:
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        ok, msg = _require_alliance_home_leader(inter)

        if not ok:
            await inter.followup.send(msg, ephemeral=True)
            return

        normalized_event_type = _normalize_alliance_event_type(event_type)

        if not normalized_event_type:
            await inter.followup.send(
                f"❌ Ungültiger Eventtyp. Erlaubt: {_alliance_event_type_text()}",
                ephemeral=True
            )
            return

        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.followup.send("❌ Datum/Zeit ungültig. (YYYY-MM-DD / HH:MM)", ephemeral=True)
            return

        try:
            get_alliance_group = _import_alliance_config()
            group_obj = get_alliance_group(group)
        except Exception as e:
            await inter.followup.send(f"❌ Allianz-Konfiguration konnte nicht geladen werden: `{e}`", ephemeral=True)
            return

        if not group_obj:
            await inter.followup.send("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = group_obj.get("servers") or {}

        if not servers:
            await inter.followup.send("❌ In dieser Allianz-Gruppe sind keine Server/Channels hinterlegt.", ephemeral=True)
            return

        home_server = servers.get(str(inter.guild.id))

        if not home_server:
            await inter.followup.send(
                "❌ Der Home-/Ebolus-Server ist in dieser Allianz-Gruppe nicht hinterlegt.\n"
                "Nutze zuerst `/alliance_server_add_home`.",
                ephemeral=True
            )
            return

        home_channel_id = _server_channel_id_for_event(home_server, normalized_event_type)
        home_channel = inter.guild.get_channel(home_channel_id)

        if not isinstance(home_channel, (discord.TextChannel, discord.Thread)):
            await inter.followup.send(
                f"❌ Home-Zielchannel für **{normalized_event_type}** wurde nicht gefunden.\n"
                f"Setze ihn mit `/alliance_event_channel_set group:{group} event_type:{normalized_event_type} channel:#channel`.",
                ephemeral=True
            )
            return

        obj = {
            "scope": "alliance",
            "alliance_group": str(group_obj.get("name", group)),
            "event_type": normalized_event_type,
            "guild_id": int(inter.guild.id),
            "channel_id": int(home_channel.id),
            "title": title.strip(),
            "description": (description or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": [], "BANK": []},
            "maybe": {},
            "no": [],
            "target_role_id": int(target_role.id) if target_role else 0,
            "dm_messages": {},
            "mirrors": [],
        }

        home_emb = build_embed(inter.guild, obj)
        home_msg = await home_channel.send(embed=home_emb)
        master_id = int(home_msg.id)
        obj["message_id"] = master_id

        obj["mirrors"].append({
            "guild_id": int(inter.guild.id),
            "discord_name": inter.guild.name,
            "label": str(home_server.get("label", inter.guild.name)),
            "short_label": str(home_server.get("short_label", home_server.get("label", inter.guild.name))),
            "channel_id": int(home_channel.id),
            "channel_name": getattr(home_channel, "name", ""),
            "message_id": master_id,
            "send_dm": bool(home_server.get("send_dm", True)),
            "home": True,
        })

        store[str(master_id)] = obj
        save_store()

        try:
            await home_msg.edit(view=ServerRaidView(master_id))
        except Exception:
            pass

        posted = [f"✅ **{inter.guild.name}** → {home_channel.mention}"]
        failed = []

        for guild_id_str, server_cfg in servers.items():
            try:
                gid = int(guild_id_str)

                if gid == inter.guild.id:
                    continue

                guild = inter.client.get_guild(gid)

                if not guild:
                    failed.append(f"❌ `{server_cfg.get('label', guild_id_str)}` — Bot sieht den Server nicht")
                    continue

                target_channel_id = _server_channel_id_for_event(server_cfg, normalized_event_type)
                ch = guild.get_channel(target_channel_id)

                if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                    failed.append(
                        f"❌ `{server_cfg.get('label', guild.name)}` — Channel für {normalized_event_type} nicht gefunden"
                    )
                    continue

                emb = build_embed(guild, obj)
                msg = await ch.send(embed=emb, view=ServerRaidView(master_id))

                obj["mirrors"].append({
                    "guild_id": int(guild.id),
                    "discord_name": guild.name,
                    "label": str(server_cfg.get("label", guild.name)),
                    "short_label": str(server_cfg.get("short_label", server_cfg.get("label", guild.name))),
                    "channel_id": int(ch.id),
                    "channel_name": getattr(ch, "name", ""),
                    "message_id": int(msg.id),
                    "send_dm": False,
                    "home": False,
                })

                posted.append(f"✅ **{server_cfg.get('label', guild.name)}** → <#{ch.id}>")
                await asyncio.sleep(0.15)

            except Exception as e:
                failed.append(f"❌ `{server_cfg.get('label', guild_id_str)}` — {e}")

        sent = 0
        skipped_opt_out = 0

        if bool(home_server.get("send_dm", True)):
            sent, skipped_opt_out = await _send_home_dms_for_alliance_event(
                inter.guild,
                obj,
                master_id,
                f"Allianz-Übersicht im Server: #{getattr(home_channel, 'name', 'raid')}"
            )

        store[str(master_id)] = obj
        save_store()
        await _push_overview(inter.client, str(master_id), obj)

        result = (
            f"✅ Allianz-Raid erstellt.\n"
            f"Gruppe: **{group_obj.get('name', group)}**\n"
            f"Eventtyp: **{normalized_event_type}**\n"
            f"Master-Message-ID: `{master_id}`\n"
            f"✉️ Home-DMs versendet: **{sent}**\n"
            f"🔕 Home-Opt-out übersprungen: **{skipped_opt_out}**\n\n"
            f"**Gepostet:**\n" + "\n".join(posted)
        )

        if failed:
            result += "\n\n**Fehler:**\n" + "\n".join(failed)

        if len(result) > 1900:
            result = result[:1850] + "\n… gekürzt"

        await inter.followup.send(result, ephemeral=True)

        # Home-/Ebolus-Gildenmenüs aktualisieren, ohne neue Portal-DMs zu senden.
        _schedule_portal_refresh_for_event(inter.client, inter.guild, obj)

    @tree.command(name="alliance_raid_create", description="(Leader) Allianz-Raid auf alle Server einer Allianz-Gruppe posten")
    @app_commands.describe(
        group="Name der Allianz-Gruppe",
        event_type="NM Raid / HM Raid / PvP Schlacht / Dimensionsprüfung",
        title="Titel",
        date="Datum YYYY-MM-DD",
        time="Zeit HH:MM (24h)",
        description="Kurzbeschreibung (optional)",
        target_role="Optional: Nur diese Home-/Ebolus-Rolle bekommt DMs",
        image_url="Optionales Bild fürs Embed"
    )
    async def alliance_raid_create(
        inter: discord.Interaction,
        group: str,
        event_type: str,
        title: str,
        date: str,
        time: str,
        description: Optional[str] = None,
        target_role: Optional[discord.Role] = None,
        image_url: Optional[str] = None
    ):
        await inter.response.defer(ephemeral=True, thinking=True)
        await _create_alliance_raid_impl(inter, group, event_type, title, date, time, description, target_role, image_url)

    @tree.command(name="alliance_raid_from_template", description="(Leader) Allianz-Raid aus gespeichertem Template erstellen")
    @app_commands.describe(
        name="Name des Templates",
        date="Datum YYYY-MM-DD",
        time="Optional: andere Uhrzeit HH:MM",
        title="Optional: anderer Titel",
        description="Optional: andere Beschreibung",
        target_role="Optional: andere Home-/Ebolus-Zielrolle für DMs",
        image_url="Optionales Bild fürs Embed"
    )
    async def alliance_raid_from_template(
        inter: discord.Interaction,
        name: str,
        date: str,
        time: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        target_role: Optional[discord.Role] = None,
        image_url: Optional[str] = None,
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        try:
            get_alliance_template = _import_alliance_template()
            tmpl = get_alliance_template(name)
        except Exception as e:
            await inter.followup.send(f"❌ Template-System konnte nicht geladen werden: `{e}`", ephemeral=True)
            return

        if not tmpl:
            await inter.followup.send("❌ Template nicht gefunden.", ephemeral=True)
            return

        if target_role is None and inter.guild is not None:
            role_id = int(tmpl.get("target_role_id", 0) or 0)
            if role_id:
                role = inter.guild.get_role(role_id)
                if isinstance(role, discord.Role):
                    target_role = role

        await _create_alliance_raid_impl(
            inter=inter,
            group=str(tmpl.get("group", "")),
            event_type=str(tmpl.get("event_type", "")),
            title=str(title or tmpl.get("title", "Allianz-Raid")),
            date=date,
            time=str(time or tmpl.get("default_time", "21:00")),
            description=str(description if description is not None else tmpl.get("description", "")),
            target_role=target_role,
            image_url=image_url,
        )

    @tree.command(name="alliance_raid_template", description="(Leader) Zeigt Copy-Paste-Vorlagen für Allianz-Raids")
    async def alliance_raid_template(inter: discord.Interaction):
        await inter.response.defer(ephemeral=True, thinking=False)

        ok, msg = _require_alliance_home_leader(inter)

        if not ok:
            await inter.followup.send(msg, ephemeral=True)
            return

        text = (
            "**🤝 Allianz-Raid Vorlagen**\n\n"
            "**HM Raid:**\n"
            "`/alliance_raid_create group:HM Raid Allianz title:HM Raid date:2026-05-30 time:21:00 description:HM Raid – bitte Rolle wählen`\n\n"
            "**Gildenbosse:**\n"
            "`/alliance_raid_create group:Gildenboss Allianz title:Gildenbosse date:2026-05-30 time:20:00 description:Gildenbosse – Anmeldung für Loot-/Needübersicht`\n\n"
            "**PvP Event:**\n"
            "`/alliance_raid_create group:PvP Allianz title:Allianz PvP date:2026-05-30 time:20:30 description:Allianz-PvP – bitte anmelden`\n\n"
            "**Mit Zielrolle für Home-DMs:**\n"
            "`/alliance_raid_create group:HM Raid Allianz title:HM Raid date:2026-05-30 time:21:00 description:HM Raid target_role:@Raid`\n\n"
            "**Hinweis:**\n"
            "Partner-Server bekommen keine DMs. Dort läuft nur der Channel-Post mit Buttons."
        )

        await inter.followup.send(text, ephemeral=True)

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

        for member in targets:
            if not is_dm_enabled(inter.guild_id, member.id):
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

                dm_msg = await member.send(dm_text, view=RaidView(int(message_id)))
                obj["dm_messages"][str(member.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)

            except Exception:
                pass

        save_store()

        await inter.followup.send(
            f"✅ Resent an {sent} Nutzer.\n🔕 Opt-out übersprungen: {skipped_opt_out}",
            ephemeral=True
        )

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

        for member in targets:
            if not is_dm_enabled(inter.guild_id, member.id):
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

                dm_msg = await member.send(dm_text, view=RaidView(int(message_id)))
                obj["dm_messages"][str(member.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)

            except Exception:
                pass

        save_store()

        await inter.followup.send(
            f"✅ Resent an {sent} Ziel(e).\n🔕 Opt-out übersprungen: {skipped_opt_out}",
            ephemeral=True
        )

    @tree.command(name="raid_delete", description="(Admin) Löscht ein Raid-/Event inkl. Serverpost und DMs")
    async def raid_delete(inter: discord.Interaction, message_id: str):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        obj = store.get(str(message_id))

        if not obj:
            await inter.followup.send("❌ Unbekanntes Event/Message-ID.", ephemeral=True)
            return

        _init_event_shape(obj)

        if int(obj.get("guild_id", 0) or 0) != inter.guild_id:
            await inter.followup.send("❌ Dieses Event gehört nicht zu diesem Home-Server.", ephemeral=True)
            return

        deleted_posts = 0
        failed_posts = []
        deleted_dms = 0

        # Serverposts löschen.
        if _is_alliance_event(obj) and obj.get("mirrors"):
            for mirror in list(obj.get("mirrors") or []):
                try:
                    guild = inter.client.get_guild(int(mirror.get("guild_id", 0) or 0))

                    if not guild:
                        failed_posts.append(f"{mirror.get('label', 'Unbekannt')} — Server nicht gefunden")
                        continue

                    ch = guild.get_channel(int(mirror.get("channel_id", 0) or 0))

                    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                        failed_posts.append(f"{mirror.get('label', guild.name)} — Channel nicht gefunden")
                        continue

                    try:
                        msg = await ch.fetch_message(int(mirror.get("message_id", 0) or 0))
                        await msg.delete()
                        deleted_posts += 1
                    except Exception:
                        failed_posts.append(f"{mirror.get('label', guild.name)} — Post nicht gefunden oder keine Rechte")

                    await asyncio.sleep(0.05)

                except Exception as e:
                    failed_posts.append(f"{mirror.get('label', 'Unbekannt')} — {e}")

        else:
            guild = inter.guild

            try:
                ch = guild.get_channel(int(obj["channel_id"]))

                if isinstance(ch, (discord.TextChannel, discord.Thread)):
                    try:
                        msg = await ch.fetch_message(int(message_id))
                        await msg.delete()
                        deleted_posts += 1
                    except Exception:
                        failed_posts.append("Serverpost nicht gefunden oder keine Rechte")

            except Exception as e:
                failed_posts.append(str(e))

        # DMs löschen.
        for uid_str in list((obj.get("dm_messages") or {}).keys()):
            try:
                uid = int(uid_str)
                ok = await _delete_dm_message_for_user(inter.client, obj, uid)

                if ok:
                    deleted_dms += 1

                await asyncio.sleep(0.05)

            except Exception:
                pass

        try:
            await _cleanup_event_voice_for_obj(inter.client, obj)
        except Exception:
            pass

        refresh_members = []

        try:
            if inter.guild is not None:
                refresh_members = list(_eligible_members(inter.guild, obj))
        except Exception:
            refresh_members = []

        refresh_members = []

        try:
            if inter.guild is not None:
                refresh_members = list(_eligible_members(inter.guild, obj))
        except Exception:
            refresh_members = []

        store.pop(str(message_id), None)
        save_store()

        text = (
            f"✅ Raid/Event gelöscht.\n"
            f"🧾 Serverposts gelöscht: **{deleted_posts}**\n"
            f"✉️ DMs gelöscht: **{deleted_dms}**"
        )

        if failed_posts:
            text += "\n\n⚠️ Nicht gelöscht:\n" + "\n".join(f"• {x}" for x in failed_posts[:10])

        if len(text) > 1900:
            text = text[:1850] + "\n… gekürzt"

        await inter.followup.send(text, ephemeral=True)

        # Nach dem Löschen bestehende Gildenmenüs aktualisieren, damit das Event dort verschwindet.
        if inter.guild is not None and refresh_members:
            try:
                asyncio.create_task(_refresh_existing_portals_for_members(inter.client, inter.guild, refresh_members))
            except Exception:
                pass


    @tree.command(name="alliance_raid_delete", description="(Leader) Löscht einen Allianz-Raid inkl. aller Mirror-Posts")
    async def alliance_raid_delete(inter: discord.Interaction, message_id: str):
        await inter.response.defer(ephemeral=True, thinking=True)

        ok, msg = _require_alliance_home_leader(inter)

        if not ok:
            await inter.followup.send(msg, ephemeral=True)
            return

        obj = store.get(str(message_id))

        if not obj:
            await inter.followup.send("❌ Unbekannter Allianz-Raid / Message-ID nicht gefunden.", ephemeral=True)
            return

        _init_event_shape(obj)

        if not _is_alliance_event(obj):
            await inter.followup.send(
                "❌ Das ist kein Allianz-Raid. Normale Raids bitte mit `/raid_delete` löschen.",
                ephemeral=True
            )
            return

        if int(obj.get("guild_id", 0) or 0) != inter.guild_id:
            await inter.followup.send("❌ Dieser Allianz-Raid gehört nicht zu diesem Home-/Ebolus-Server.", ephemeral=True)
            return

        deleted_posts = 0
        failed_posts = []
        deleted_dms = 0

        for mirror in list(obj.get("mirrors") or []):
            try:
                guild = inter.client.get_guild(int(mirror.get("guild_id", 0) or 0))

                if not guild:
                    failed_posts.append(f"{mirror.get('label', 'Unbekannt')} — Server nicht gefunden")
                    continue

                ch = guild.get_channel(int(mirror.get("channel_id", 0) or 0))

                if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                    failed_posts.append(f"{mirror.get('label', guild.name)} — Channel nicht gefunden")
                    continue

                try:
                    msg_obj = await ch.fetch_message(int(mirror.get("message_id", 0) or 0))
                    await msg_obj.delete()
                    deleted_posts += 1
                except Exception:
                    failed_posts.append(f"{mirror.get('label', guild.name)} — Post nicht gefunden oder keine Rechte")

                await asyncio.sleep(0.05)

            except Exception as e:
                failed_posts.append(f"{mirror.get('label', 'Unbekannt')} — {e}")

        for uid_str in list((obj.get("dm_messages") or {}).keys()):
            try:
                uid = int(uid_str)
                ok_deleted = await _delete_dm_message_for_user(inter.client, obj, uid)

                if ok_deleted:
                    deleted_dms += 1

                await asyncio.sleep(0.05)

            except Exception:
                pass

        try:
            await _cleanup_event_voice_for_obj(inter.client, obj)
        except Exception:
            pass

        store.pop(str(message_id), None)
        save_store()

        text = (
            f"✅ Allianz-Raid gelöscht.\n"
            f"🧾 Mirror-Posts gelöscht: **{deleted_posts}**\n"
            f"✉️ Home-DMs gelöscht: **{deleted_dms}**"
        )

        if failed_posts:
            text += "\n\n⚠️ Nicht gelöscht:\n" + "\n".join(f"• {x}" for x in failed_posts[:10])

        if len(text) > 1900:
            text = text[:1850] + "\n… gekürzt"

        await inter.followup.send(text, ephemeral=True)

        # Nach dem Löschen bestehende Home-Gildenmenüs aktualisieren, damit der Allianz-Raid dort verschwindet.
        if inter.guild is not None and refresh_members:
            try:
                asyncio.create_task(_refresh_existing_portals_for_members(inter.client, inter.guild, refresh_members))
            except Exception:
                pass


async def auto_resend_for_new_member(member: discord.Member) -> None:
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
                    role = member.guild.get_role(tr_id)

                    if not (role and role in member.roles):
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
