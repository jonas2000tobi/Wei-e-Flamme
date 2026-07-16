from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

try:
    from bot import runtime_db  # type: ignore
except Exception:  # pragma: no cover
    import runtime_db  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

PROFILE_DEFAULTS: dict[str, Any] = {
    "display_name": "Gilde",
    "short_name": "Gilde",
    "bot_display_name": "Gildenknecht",
    "timezone": "Europe/Berlin",
    "logo_url": "",
    "banner_url": "",
    "accent_color": "#d6a84f",
    "invite_url": "",
    "status": "active",
}

ROLE_KEYS = {
    "member": "dashboard_member_role_id",
    "admin": "dashboard_admin_role_ids",
    "allowed": "dashboard_allowed_role_ids",
    "leader": "guild_role_leader_id",
    "advisor": "guild_role_advisor_id",
    "guardian": "guild_role_guardian_id",
    "voice_allowed": "guild_role_voice_allowed_ids",
    "voice_blocked": "guild_role_voice_blocked_ids",
}

MULTI_ROLE_KINDS = {"admin", "allowed", "voice_allowed", "voice_blocked"}

CHANNEL_KEYS = {
    "events": "guild_channel_events_id",
    "loot": "guild_channel_loot_id",
    "ec_log": "guild_channel_ec_log_id",
    "audit": "guild_channel_audit_id",
    "welcome": "guild_channel_welcome_id",
    "announcements": "guild_channel_announcements_id",
    "news": "guild_channel_news_id",
    "guides": "guild_channel_guides_id",
    "voice_category": "guild_channel_voice_category_id",
    "voice_return": "guild_channel_voice_return_id",
    "absences": "guild_channel_absences_id",
    "member_portal": "guild_channel_member_portal_id",
    "leader_contact_public": "guild_channel_leader_contact_public_id",
    "leader_contact_internal": "guild_channel_leader_contact_internal_id",
    "weekly_report": "guild_channel_weekly_report_id",
    "auction_active": "guild_channel_auction_active_id",
    "auction_market": "guild_channel_auction_market_id",
}

RULE_KEYS: dict[str, dict[str, int | str]] = {
    "loot_need_hours": {"setting": "guild_rule_loot_need_hours", "default": 48, "min": 1, "max": 720},
    "loot_free_hours": {"setting": "guild_rule_loot_free_hours", "default": 24, "min": 1, "max": 720},
    "loot_sale_hours": {"setting": "guild_rule_loot_sale_hours", "default": 240, "min": 1, "max": 2160},
    "loot_main_start_bid": {"setting": "guild_rule_loot_main_start_bid", "default": 30, "min": 1, "max": 100000},
    "loot_secondary_start_bid": {"setting": "guild_rule_loot_secondary_start_bid", "default": 15, "min": 1, "max": 100000},
    "loot_free_start_bid": {"setting": "guild_rule_loot_free_start_bid", "default": 5, "min": 1, "max": 100000},
    "loot_sale_price": {"setting": "guild_rule_loot_sale_price", "default": 1, "min": 0, "max": 100000},
    "loot_need_increment": {"setting": "guild_rule_loot_need_increment", "default": 5, "min": 1, "max": 100000},
    "loot_free_increment": {"setting": "guild_rule_loot_free_increment", "default": 1, "min": 1, "max": 100000},
    "loot_new_member_lock_days": {"setting": "guild_rule_loot_new_member_lock_days", "default": 7, "min": 0, "max": 365},
    "loot_junk_roll_hours": {"setting": "guild_rule_loot_junk_roll_hours", "default": 24, "min": 1, "max": 720},
}

# Gildenbezogene IDs dürfen bei einem Serverwechsel nicht übernommen werden.
DISCORD_BOUND_SETTING_KEYS = set(ROLE_KEYS.values()) | set(CHANNEL_KEYS.values()) | {
    "dashboard_news_channel_name",
    "dashboard_guides_channel_name",
    "dashboard_announcements_channel_name",
}

# Nur sichere, spielerbezogene JSON-Daten übernehmen. Konfigurationsdateien
# enthalten alte Rollen-/Kanal-IDs und dürfen bei einem Serverwechsel nicht
# in den neuen Discord-Server kopiert werden.
LEGACY_SCOPED_JSON_FILES = (
    "member_profiles.json",
    "dkp_balances.json",
    "dkp_transactions.json",
    "loot_items.json",
    "loot_needs.json",
    "guild_chest.json",
)



def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_profile(guild: discord.Guild | int, discord_name: str = "") -> dict[str, Any]:
    guild_id = int(getattr(guild, "id", guild))
    fallback = str(getattr(guild, "name", discord_name) or discord_name or f"Guild {guild_id}").strip()
    profile = runtime_db.get_guild_profile(guild_id)
    if not profile:
        runtime_db.upsert_guild_profile(
            guild_id,
            display_name=fallback,
            short_name=fallback,
            bot_display_name=f"{fallback} Knecht",
            timezone_name="Europe/Berlin",
            status="active",
            discord_name=fallback,
        )
        profile = runtime_db.get_guild_profile(guild_id) or {}
    elif fallback and str(profile.get("discord_name") or "") != fallback:
        runtime_db.upsert_guild_profile(guild_id, discord_name=fallback)
        profile = runtime_db.get_guild_profile(guild_id) or profile
    return normalized_profile(profile, fallback=fallback, guild_id=guild_id)


def normalized_profile(profile: Optional[dict[str, Any]], *, fallback: str = "Gilde", guild_id: int = 0) -> dict[str, Any]:
    out = dict(PROFILE_DEFAULTS)
    if isinstance(profile, dict):
        out.update({k: v for k, v in profile.items() if v is not None})
    display = str(out.get("display_name") or fallback or "Gilde").strip()
    out["display_name"] = display
    out["short_name"] = str(out.get("short_name") or display).strip()
    out["bot_display_name"] = str(out.get("bot_display_name") or f"{display} Knecht").strip()
    out["guild_id"] = int(out.get("guild_id") or guild_id or 0)
    return out


def get_profile(guild_id: int, fallback: str = "Gilde") -> dict[str, Any]:
    return normalized_profile(runtime_db.get_guild_profile(int(guild_id)), fallback=fallback, guild_id=int(guild_id))


def display_name(guild: discord.Guild | int, fallback: str = "Gilde") -> str:
    gid = int(getattr(guild, "id", guild))
    discord_name = str(getattr(guild, "name", fallback) or fallback)
    return str(get_profile(gid, discord_name).get("display_name") or discord_name)


def role_ids(guild_id: int, kind: str) -> list[int]:
    key = ROLE_KEYS.get(str(kind or "").lower())
    if not key:
        return []
    raw = runtime_db.get_guild_setting(int(guild_id), key, [])
    if not isinstance(raw, list):
        raw = [raw]
    out: list[int] = []
    for item in raw:
        try:
            value = int(item or 0)
        except Exception:
            continue
        if value and value not in out:
            out.append(value)
    return out


def channel_id(guild_id: int, kind: str) -> int:
    key = CHANNEL_KEYS.get(str(kind or "").lower())
    if not key:
        return 0
    try:
        return int(runtime_db.get_guild_setting(int(guild_id), key, 0) or 0)
    except Exception:
        return 0


def rule_value(guild_id: int, kind: str) -> int:
    spec = RULE_KEYS.get(str(kind or "").lower())
    if not spec:
        raise KeyError(f"Unbekannte Gildenregel: {kind}")
    default = int(spec["default"])
    try:
        raw = runtime_db.get_guild_setting(int(guild_id), str(spec["setting"]), default)
        value = int(raw)
    except Exception:
        value = default
    return max(int(spec["min"]), min(int(spec["max"]), value))


def set_rule_value(guild_id: int, kind: str, value: int) -> int:
    spec = RULE_KEYS.get(str(kind or "").lower())
    if not spec:
        raise KeyError(f"Unbekannte Gildenregel: {kind}")
    clean = max(int(spec["min"]), min(int(spec["max"]), int(value)))
    runtime_db.set_guild_setting(int(guild_id), str(spec["setting"]), clean)
    return clean


def role_mapping_configured(guild_id: int, kind: str) -> bool:
    key = ROLE_KEYS.get(str(kind or "").lower())
    if not key:
        return False
    try:
        return key in runtime_db.get_all_guild_settings(int(guild_id))
    except Exception:
        return False


def channel_mapping_configured(guild_id: int, kind: str) -> bool:
    key = CHANNEL_KEYS.get(str(kind or "").lower())
    if not key:
        return False
    try:
        return key in runtime_db.get_all_guild_settings(int(guild_id))
    except Exception:
        return False


def guild_config_snapshot(guild: discord.Guild) -> dict[str, Any]:
    profile = ensure_profile(guild)
    roles: dict[str, Any] = {}
    for kind in ROLE_KEYS:
        ids = role_ids(guild.id, kind)
        rows = []
        for rid in ids:
            role = guild.get_role(rid)
            rows.append({"id": rid, "name": str(getattr(role, "name", "")) if role else "", "exists": role is not None})
        roles[kind] = rows
    channels: dict[str, Any] = {}
    for kind in CHANNEL_KEYS:
        cid = channel_id(guild.id, kind)
        ch = guild.get_channel(cid) if cid else None
        channels[kind] = {"id": cid, "name": str(getattr(ch, "name", "")) if ch else "", "exists": ch is not None if cid else False}
    rules = {kind: rule_value(guild.id, kind) for kind in RULE_KEYS}
    return {"profile": profile, "roles": roles, "channels": channels, "rules": rules}


def _is_admin(inter: discord.Interaction) -> bool:
    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False
    p = inter.user.guild_permissions
    if p.administrator or p.manage_guild:
        return True
    configured = set(role_ids(inter.guild.id, "admin") + role_ids(inter.guild.id, "leader"))
    return bool(configured & {int(r.id) for r in inter.user.roles})


def _copy_scoped_json(source_guild_id: int, target_guild_id: int, active_user_ids: set[int]) -> dict[str, int]:
    """Kopiert nur sichere, servergebundene Alt-JSON-Daten.

    Benutzerbezogene Bereiche werden auf Mitglieder begrenzt, die im neuen
    Discord-Server tatsächlich aktiv sind. Itemkatalog und Gildentruhe sind
    dagegen gildenweite Daten und werden vollständig übernommen.
    """
    counts: dict[str, int] = {}
    source_key, target_key = str(source_guild_id), str(target_guild_id)
    active_keys = {str(int(uid)) for uid in active_user_ids}

    def filter_user_map(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return {str(uid): row for uid, row in value.items() if str(uid) in active_keys}

    def row_user_id(row: Any) -> str:
        if not isinstance(row, dict):
            return ""
        for key in ("user_id", "discord_user_id", "discord_id", "member_id", "target_user_id", "winner_user_id", "buyer_user_id"):
            raw = str(row.get(key) or "").strip()
            if raw:
                return raw
        return ""

    def filter_rows(value: Any) -> Any:
        if isinstance(value, list):
            return [row for row in value if row_user_id(row) in active_keys]
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, row in value.items():
                uid = row_user_id(row)
                if uid:
                    if uid in active_keys:
                        out[str(key)] = row
                elif str(key) in active_keys:
                    out[str(key)] = row
            return out
        return value

    for filename in LEGACY_SCOPED_JSON_FILES:
        path = DATA_DIR / filename
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict) or source_key not in data:
            continue

        scoped = data.get(source_key)
        cloned = json.loads(json.dumps(scoped, ensure_ascii=False))

        if filename == "member_profiles.json" and isinstance(cloned, dict):
            cloned["users"] = filter_user_map(cloned.get("users"))
            if "members" in cloned:
                cloned["members"] = filter_user_map(cloned.get("members"))
            if "profiles" in cloned:
                cloned["profiles"] = filter_user_map(cloned.get("profiles"))
            if "absences" in cloned:
                cloned["absences"] = filter_user_map(cloned.get("absences"))
        elif filename == "dkp_balances.json" and isinstance(cloned, dict):
            cloned["users"] = filter_user_map(cloned.get("users"))
            if "balances" in cloned:
                cloned["balances"] = filter_user_map(cloned.get("balances"))
        elif filename == "dkp_transactions.json":
            cloned = filter_rows(cloned)
        elif filename == "loot_needs.json" and isinstance(cloned, dict):
            cloned["users"] = filter_user_map(cloned.get("users"))
            for key in ("items", "rows", "need_items"):
                if key in cloned:
                    cloned[key] = filter_rows(cloned.get(key))
        # loot_items.json und guild_chest.json enthalten gildenweite Katalog-/Truhendaten.

        data[target_key] = cloned
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
        counts[filename] = 1
    return counts


def _set_alliance_home_guild(target_guild_id: int, *, source_guild_id: int = 0, discord_name: str = "") -> bool:
    """Aktualisiert das noch globale Allianz-Home ohne Railway-Eingriff.

    Alte Allianz-Kanalzuordnungen werden beim Rehome bewusst auf 0 gesetzt.
    """
    path = DATA_DIR / "alliance_config.json"
    data = _load_json_dict(path)
    root = data.get("_global") if isinstance(data.get("_global"), dict) else {}
    root["home_guild_id"] = int(target_guild_id)
    groups = root.get("groups") if isinstance(root.get("groups"), dict) else {}
    if source_guild_id and int(source_guild_id) != int(target_guild_id):
        for group in groups.values():
            if not isinstance(group, dict):
                continue
            servers = group.get("servers") if isinstance(group.get("servers"), dict) else {}
            old = servers.pop(str(int(source_guild_id)), None)
            if isinstance(old, dict):
                moved = dict(old)
                moved["discord_name"] = str(discord_name or moved.get("discord_name") or "Neue Gilde")
                moved["channel_id"] = 0
                moved["event_channels"] = {
                    str(k): {**(v if isinstance(v, dict) else {}), "channel_id": 0}
                    for k, v in (moved.get("event_channels") or {}).items()
                }
                servers[str(int(target_guild_id))] = moved
            group["servers"] = servers
    root["groups"] = groups
    data["_global"] = root
    _save_json_dict(path, data)
    # Das Modul hält die JSON-Struktur im RAM. Bei bereits importiertem Modul
    # aktualisieren wir sie ebenfalls, damit kein Neustart nötig ist.
    try:
        try:
            from bot import alliance_config as alliance_module  # type: ignore
        except Exception:
            import alliance_config as alliance_module  # type: ignore
        alliance_module.alliance_cfg.clear()
        alliance_module.alliance_cfg.update(data)
    except Exception:
        pass
    return True


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_json_dict(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def sync_legacy_compatibility(guild_id: int) -> dict[str, bool]:
    """Spiegelt zentrale Rollen/Kanäle in die noch verwendeten JSON-Configs.

    So wirken Änderungen aus dem Dashboard auch in älteren Modulen, bis deren
    gesamte Fachkonfiguration direkt aus Postgres liest. Es werden ausschließlich
    Discord-Zuordnungen gespiegelt, keine fachlichen EC-/Loot-Regeln überschrieben.
    """
    gid = str(int(guild_id))
    changed: dict[str, bool] = {}

    portal_path = DATA_DIR / "member_portal_cfg.json"
    portal = _load_json_dict(portal_path)
    pc = portal.get(gid) if isinstance(portal.get(gid), dict) else {}
    before = json.dumps(pc, sort_keys=True, ensure_ascii=False)
    member_ids = role_ids(int(guild_id), "member")
    if role_mapping_configured(int(guild_id), "member"):
        pc["member_role_id"] = member_ids[0] if member_ids else 0
    positions = pc.get("position_roles") if isinstance(pc.get("position_roles"), dict) else {}
    for kind in ("leader", "advisor", "guardian"):
        ids = role_ids(int(guild_id), kind)
        if role_mapping_configured(int(guild_id), kind):
            positions[kind] = ids[0] if ids else 0
    pc["position_roles"] = positions
    for kind, field in (("absences", "absence_channel_id"), ("member_portal", "portal_post_channel_id"), ("voice_category", "event_voice_category_id"), ("voice_return", "event_voice_return_channel_id")):
        cid = channel_id(int(guild_id), kind)
        if channel_mapping_configured(int(guild_id), kind):
            pc[field] = cid
    portal[gid] = pc
    if json.dumps(pc, sort_keys=True, ensure_ascii=False) != before:
        _save_json_dict(portal_path, portal)
        changed[portal_path.name] = True

    leader_path = DATA_DIR / "leader_contact_cfg.json"
    leader = _load_json_dict(leader_path)
    lc = leader.get(gid) if isinstance(leader.get(gid), dict) else {}
    before = json.dumps(lc, sort_keys=True, ensure_ascii=False)
    leader_ids = role_ids(int(guild_id), "leader")
    if role_mapping_configured(int(guild_id), "leader"):
        lc["leader_role_id"] = leader_ids[0] if leader_ids else 0
    mappings = {
        "leader_contact_public": "public_channel_id",
        "leader_contact_internal": "internal_channel_id",
    }
    for kind, field in mappings.items():
        cid = channel_id(int(guild_id), kind)
        if channel_mapping_configured(int(guild_id), kind):
            lc[field] = cid
    leader[gid] = lc
    if json.dumps(lc, sort_keys=True, ensure_ascii=False) != before:
        _save_json_dict(leader_path, leader)
        changed[leader_path.name] = True

    dkp_path = DATA_DIR / "dkp_cfg.json"
    dkp = _load_json_dict(dkp_path)
    dc = dkp.get(gid) if isinstance(dkp.get(gid), dict) else {}
    before = json.dumps(dc, sort_keys=True, ensure_ascii=False)
    ec_log = channel_id(int(guild_id), "ec_log")
    if channel_mapping_configured(int(guild_id), "ec_log"):
        dc["log_channel_id"] = ec_log
    if role_mapping_configured(int(guild_id), "admin"):
        dc["dashboard_admin_role_ids"] = role_ids(int(guild_id), "admin")
    if role_mapping_configured(int(guild_id), "allowed"):
        dc["dashboard_allowed_role_ids"] = role_ids(int(guild_id), "allowed")
    dkp[gid] = dc
    if json.dumps(dc, sort_keys=True, ensure_ascii=False) != before:
        _save_json_dict(dkp_path, dkp)
        changed[dkp_path.name] = True

    auction_path = DATA_DIR / "loot_auction_cfg.json"
    auctions = _load_json_dict(auction_path)
    ac = auctions.get(gid) if isinstance(auctions.get(gid), dict) else {}
    before = json.dumps(ac, sort_keys=True, ensure_ascii=False)
    loot_channel = channel_id(int(guild_id), "loot")
    if channel_mapping_configured(int(guild_id), "loot"):
        ac["auction_channel_id"] = loot_channel
    if channel_mapping_configured(int(guild_id), "ec_log"):
        ac["log_channel_id"] = ec_log
    active_channel = channel_id(int(guild_id), "auction_active")
    market_channel = channel_id(int(guild_id), "auction_market")
    if channel_mapping_configured(int(guild_id), "auction_active"):
        ac["active_channel_id"] = active_channel
    if channel_mapping_configured(int(guild_id), "auction_market"):
        ac["market_channel_id"] = market_channel
    auctions[gid] = ac
    if json.dumps(ac, sort_keys=True, ensure_ascii=False) != before:
        _save_json_dict(auction_path, auctions)
        changed[auction_path.name] = True

    weekly_path = DATA_DIR / "weekly_report_cfg.json"
    weekly = _load_json_dict(weekly_path)
    wc = weekly.get(gid) if isinstance(weekly.get(gid), dict) else {}
    before = json.dumps(wc, sort_keys=True, ensure_ascii=False)
    weekly_channel = channel_id(int(guild_id), "weekly_report")
    if channel_mapping_configured(int(guild_id), "weekly_report"):
        # Unterstützt beide in älteren Ständen verwendeten Feldnamen.
        wc["channel_id"] = weekly_channel
        wc["report_channel_id"] = weekly_channel
    weekly[gid] = wc
    if json.dumps(wc, sort_keys=True, ensure_ascii=False) != before:
        _save_json_dict(weekly_path, weekly)
        changed[weekly_path.name] = True

    return changed


async def _sync_bot_nickname(guild: discord.Guild, profile: Optional[dict[str, Any]] = None) -> bool:
    profile = profile or ensure_profile(guild)
    desired = str(profile.get("bot_display_name") or "").strip()[:32]
    me = guild.me
    if not desired or me is None or str(getattr(me, "display_name", "")) == desired:
        return False
    try:
        await me.edit(nick=desired, reason="Zentrale Gildenkonfiguration")
        return True
    except Exception:
        return False


async def setup_guild_config(bot: commands.Bot, tree: app_commands.CommandTree) -> None:
    for guild in bot.guilds:
        try:
            profile = await asyncio.to_thread(ensure_profile, guild)
            await _sync_bot_nickname(guild, profile)
        except Exception as exc:
            print(f"⚠️ Guild-Profil konnte für {guild.id} nicht angelegt werden: {exc!r}")

    guild_group = app_commands.Group(name="guild", description="Zentrale Gilden- und Discord-Konfiguration")

    @guild_group.command(name="setup", description="Konfiguriert Name und Grunddaten dieser Gilde ohne Railway.")
    @app_commands.describe(name="Anzeigename der Gilde", short_name="Kurzer Name", bot_name="Anzeigename des Bots", timezone_name="IANA-Zeitzone, z. B. Europe/Berlin")
    async def guild_setup(inter: discord.Interaction, name: str, short_name: str = "", bot_name: str = "", timezone_name: str = "Europe/Berlin"):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_admin(inter):
            await inter.response.send_message("❌ Dafür brauchst du Server verwalten oder die konfigurierte Adminrolle.", ephemeral=True)
            return
        clean = str(name or "").strip()[:100]
        if not clean:
            await inter.response.send_message("❌ Gildenname fehlt.", ephemeral=True)
            return
        before = get_profile(inter.guild.id, inter.guild.name)
        runtime_db.upsert_guild_profile(
            inter.guild.id,
            display_name=clean,
            short_name=(short_name or clean)[:50],
            bot_display_name=(bot_name or f"{clean} Knecht")[:100],
            timezone_name=(timezone_name or "Europe/Berlin")[:80],
            status="active",
            discord_name=inter.guild.name,
        )
        await asyncio.to_thread(_set_alliance_home_guild, inter.guild.id, discord_name=inter.guild.name)
        await _sync_bot_nickname(inter.guild, get_profile(inter.guild.id, inter.guild.name))
        await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
        runtime_db.write_audit_log(
            guild_id=inter.guild.id,
            actor_id=inter.user.id,
            action="guild_profile_update",
            target_type="guild",
            target_id=str(inter.guild.id),
            summary=f"Gildenprofil auf {clean} aktualisiert",
            old_value=before,
            new_value=get_profile(inter.guild.id, inter.guild.name),
        )
        await inter.response.send_message(f"✅ Gilde konfiguriert: **{clean}**. Railway musste nicht geändert werden.", ephemeral=True)

    @guild_group.command(name="set_role", description="Ordnet eine Discord-Rolle einer Gildenfunktion zu.")
    @app_commands.describe(kind="Funktion der Rolle", role="Discord-Rolle")
    @app_commands.choices(kind=[app_commands.Choice(name=x, value=x) for x in ROLE_KEYS])
    async def guild_set_role(inter: discord.Interaction, kind: app_commands.Choice[str], role: discord.Role):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        key = ROLE_KEYS[kind.value]
        if kind.value in MULTI_ROLE_KINDS:
            value = role_ids(inter.guild.id, kind.value)
            if int(role.id) not in value:
                value.append(int(role.id))
        else:
            value = int(role.id)
        runtime_db.set_guild_setting(inter.guild.id, key, value)
        await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
        runtime_db.write_audit_log(guild_id=inter.guild.id, actor_id=inter.user.id, action="guild_role_map_set", target_type="role", target_id=str(role.id), summary=f"{kind.value}: {role.name}", new_value={"kind": kind.value, "role_id": role.id})
        await inter.response.send_message(f"✅ **{kind.value}** → {role.mention}", ephemeral=True)

    @guild_group.command(name="set_channel", description="Ordnet einen Discord-Kanal einer Gildenfunktion zu.")
    @app_commands.describe(kind="Funktion des Kanals", channel="Discord-Kanal")
    @app_commands.choices(kind=[app_commands.Choice(name=x, value=x) for x in CHANNEL_KEYS])
    async def guild_set_channel(
        inter: discord.Interaction,
        kind: app_commands.Choice[str],
        channel: discord.TextChannel | discord.VoiceChannel | discord.CategoryChannel | discord.ForumChannel,
    ):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        runtime_db.set_guild_setting(inter.guild.id, CHANNEL_KEYS[kind.value], int(channel.id))
        await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
        runtime_db.write_audit_log(guild_id=inter.guild.id, actor_id=inter.user.id, action="guild_channel_map_set", target_type="channel", target_id=str(channel.id), summary=f"{kind.value}: {channel.name}", new_value={"kind": kind.value, "channel_id": channel.id})
        await inter.response.send_message(f"✅ **{kind.value}** → {getattr(channel, 'mention', '#' + channel.name)}", ephemeral=True)

    @guild_group.command(name="clear_role", description="Entfernt eine zentrale Rollenzuordnung.")
    @app_commands.choices(kind=[app_commands.Choice(name=x, value=x) for x in ROLE_KEYS])
    async def guild_clear_role(inter: discord.Interaction, kind: app_commands.Choice[str]):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        runtime_db.set_guild_setting(inter.guild.id, ROLE_KEYS[kind.value], [] if kind.value in MULTI_ROLE_KINDS else 0)
        await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
        await inter.response.send_message(f"✅ Rollenzuordnung **{kind.value}** entfernt.", ephemeral=True)

    @guild_group.command(name="clear_channel", description="Entfernt eine zentrale Kanalzuordnung.")
    @app_commands.choices(kind=[app_commands.Choice(name=x, value=x) for x in CHANNEL_KEYS])
    async def guild_clear_channel(inter: discord.Interaction, kind: app_commands.Choice[str]):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        runtime_db.set_guild_setting(inter.guild.id, CHANNEL_KEYS[kind.value], 0)
        await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
        await inter.response.send_message(f"✅ Kanalzuordnung **{kind.value}** entfernt.", ephemeral=True)

    @guild_group.command(name="set_rule", description="Ändert eine Gildenregel ohne Code oder Railway.")
    @app_commands.describe(kind="Regel", value="Neuer ganzzahliger Wert")
    @app_commands.choices(kind=[app_commands.Choice(name=x, value=x) for x in RULE_KEYS])
    async def guild_set_rule(inter: discord.Interaction, kind: app_commands.Choice[str], value: int):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        spec = RULE_KEYS[kind.value]
        if value < int(spec["min"]) or value > int(spec["max"]):
            await inter.response.send_message(f"❌ Erlaubt: {spec['min']} bis {spec['max']}.", ephemeral=True)
            return
        before = rule_value(inter.guild.id, kind.value)
        clean = set_rule_value(inter.guild.id, kind.value, value)
        runtime_db.write_audit_log(
            guild_id=inter.guild.id,
            actor_id=inter.user.id,
            action="guild_rule_set",
            target_type="guild_rule",
            target_id=kind.value,
            summary=f"{kind.value}: {before} → {clean}",
            old_value={"value": before},
            new_value={"value": clean},
        )
        await inter.response.send_message(f"✅ **{kind.value}** = **{clean}**", ephemeral=True)

    @guild_group.command(name="branding", description="Setzt Logo, Banner, Farbe und Einladungslink ohne Railway.")
    async def guild_branding(inter: discord.Interaction, logo_url: str = "", banner_url: str = "", accent_color: str = "#d6a84f", invite_url: str = ""):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        current = get_profile(inter.guild.id, inter.guild.name)
        runtime_db.upsert_guild_profile(
            inter.guild.id,
            logo_url=(logo_url or current.get("logo_url") or "")[:1000],
            banner_url=(banner_url or current.get("banner_url") or "")[:1000],
            accent_color=(accent_color or current.get("accent_color") or "#d6a84f")[:20],
            invite_url=(invite_url or current.get("invite_url") or "")[:1000],
        )
        await inter.response.send_message("✅ Branding gespeichert. Das Dashboard übernimmt es mit dem nächsten Snapshot.", ephemeral=True)

    @guild_group.command(name="show", description="Zeigt die zentrale Gildenkonfiguration.")
    async def guild_config_cmd(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        cfg = guild_config_snapshot(inter.guild)
        p = cfg["profile"]
        role_lines = [f"• {k}: " + (", ".join(f"<@&{r['id']}>" for r in rows) if rows else "—") for k, rows in cfg["roles"].items()]
        channel_lines = [f"• {k}: " + (f"<#{row['id']}>" if row.get("id") else "—") for k, row in cfg["channels"].items()]
        emb = discord.Embed(title=f"⚙️ {p['display_name']} – Konfiguration", color=0xD6A84F)
        emb.add_field(name="Grunddaten", value=f"Bot: {p['bot_display_name']}\nZeitzone: {p['timezone']}\nStatus: {p['status']}", inline=False)
        emb.add_field(name="Rollen", value="\n".join(role_lines)[:1024], inline=False)
        emb.add_field(name="Kanäle", value="\n".join(channel_lines)[:1024], inline=False)
        rule_lines = [f"• {kind}: {value}" for kind, value in cfg.get("rules", {}).items()]
        emb.add_field(name="Regeln", value="\n".join(rule_lines)[:1024] or "—", inline=False)
        await inter.response.send_message(embed=emb, ephemeral=True)

    @guild_group.command(name="rehome", description="Übernimmt mitgekommene Mitglieder und Historie aus einer alten Guild-ID.")
    @app_commands.describe(source_guild_id="Alte Discord-Guild-ID", copy_legacy_json="Vorhandene JSON-Altdaten zusätzlich kopieren")
    async def guild_rehome(inter: discord.Interaction, source_guild_id: str, copy_legacy_json: bool = True):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur für Server-Admins/Leitung.", ephemeral=True)
            return
        if not str(source_guild_id).isdigit():
            await inter.response.send_message("❌ Alte Guild-ID ist ungültig.", ephemeral=True)
            return
        source_id = int(source_guild_id)
        if source_id == inter.guild.id:
            await inter.response.send_message("❌ Quelle und Ziel sind identisch.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True, thinking=True)
        configured_member_roles = set(role_ids(inter.guild.id, "member"))
        active_ids = {
            int(m.id)
            for m in inter.guild.members
            if not m.bot and (
                not configured_member_roles
                or bool(configured_member_roles.intersection({int(r.id) for r in getattr(m, "roles", [])}))
            )
        }
        if not active_ids:
            await inter.followup.send("❌ Keine übernehmbaren Mitglieder gefunden. Prüfe zuerst die Mitgliederrolle mit `/guild set_role`.", ephemeral=True)
            return
        try:
            result = await asyncio.to_thread(runtime_db.rehome_guild_data, source_id, inter.guild.id, sorted(active_ids))
            json_counts = await asyncio.to_thread(_copy_scoped_json, source_id, inter.guild.id, active_ids) if copy_legacy_json else {}
            ensure_profile(inter.guild)
            await asyncio.to_thread(_set_alliance_home_guild, inter.guild.id, source_guild_id=source_id, discord_name=inter.guild.name)
            await asyncio.to_thread(sync_legacy_compatibility, inter.guild.id)
            runtime_db.write_audit_log(guild_id=inter.guild.id, actor_id=inter.user.id, action="guild_rehome", target_type="guild", target_id=str(source_id), summary=f"Datenübernahme {source_id} → {inter.guild.id}", new_value={"database": result, "json": json_counts})
            lines = [f"• {k}: {v}" for k, v in (result.get("counts") or {}).items()]
            await inter.followup.send("✅ Umzug abgeschlossen. Rollen, Kanäle, Events und offene Auktionen wurden bewusst **nicht** übernommen.\n" + ("\n".join(lines) if lines else "Keine passenden Altdaten gefunden."), ephemeral=True)
        except Exception as exc:
            await inter.followup.send(f"❌ Umzug fehlgeschlagen: `{type(exc).__name__}: {exc}`", ephemeral=True)
    @tasks.loop(seconds=45)
    async def guild_config_refresh_loop() -> None:
        for current_guild in list(bot.guilds):
            try:
                profile = await asyncio.to_thread(ensure_profile, current_guild)
                await asyncio.to_thread(sync_legacy_compatibility, current_guild.id)
                await _sync_bot_nickname(current_guild, profile)
            except Exception as exc:
                print(f"⚠️ Zentrale Gildenkonfiguration konnte für {getattr(current_guild, 'id', 0)} nicht synchronisiert werden: {exc!r}")

    @guild_config_refresh_loop.before_loop
    async def before_guild_config_refresh_loop() -> None:
        await bot.wait_until_ready()

    tree.add_command(guild_group)
    if not guild_config_refresh_loop.is_running():
        guild_config_refresh_loop.start()
