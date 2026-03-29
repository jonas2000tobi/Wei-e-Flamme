# RSVP per DM: Buttons in DM, Übersicht im Server-Channel wird live aktualisiert.
# Zusätzlich:
# - DM Opt-out
# - Statistiksystem
# - Kalenderübersicht
# - Rollenbasierte Auto-Pings

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional, Iterable, List, Tuple
import asyncio

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RSVP_FILE      = DATA_DIR / "event_rsvp.json"          # Events + Anmeldungen
DM_CFG_FILE    = DATA_DIR / "event_rsvp_cfg.json"      # Rollen, Log-Kanal, Comp, Auto-Ping
DM_PREFS_FILE  = DATA_DIR / "event_rsvp_prefs.json"    # DM Opt-out
DM_STATS_FILE  = DATA_DIR / "event_rsvp_stats.json"    # Spielerstatistiken
PING_LOG_FILE  = DATA_DIR / "event_rsvp_ping_log.json" # Auto-Ping Log

DEFAULT_COMP = {"TANK": 1, "HEAL": 1, "DPS": 3}
AUTO_PING_MINUTES = (120, 30)


def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg: Dict[str, dict] = _load(DM_CFG_FILE, {})
prefs: Dict[str, dict] = _load(DM_PREFS_FILE, {})
stats: Dict[str, dict] = _load(DM_STATS_FILE, {})
ping_log: Dict[str, bool] = _load(PING_LOG_FILE, {})

def save_store(): _save(RSVP_FILE, store)
def save_cfg(): _save(DM_CFG_FILE, cfg)
def save_prefs(): _save(DM_PREFS_FILE, prefs)
def save_stats(): _save(DM_STATS_FILE, stats)
def save_ping_log(): _save(PING_LOG_FILE, ping_log)


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


def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"


def _init_event_shape(obj: dict):
    if "yes" not in obj or not isinstance(obj["yes"], dict):
        obj["yes"] = {"TANK": [], "HEAL": [], "DPS": []}
    for k in ("TANK", "HEAL", "DPS"):
        if k not in obj["yes"] or not isinstance(obj["yes"][k], list):
            obj["yes"][k] = []
    if "maybe" not in obj or not isinstance(obj["maybe"], dict):
        obj["maybe"] = {}
    if "no" not in obj or not isinstance(obj["no"], list):
        obj["no"] = []
    obj.setdefault("target_role_id", 0)
    obj.setdefault("invited_ids", [])


def get_role_ids_for_guild(guild_id: int) -> Dict[str, int]:
    g = cfg.get(str(guild_id)) or {}
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS": int(g.get("DPS", 0) or 0),
    }


def _guild_cfg(guild_id: int) -> dict:
    g = cfg.get(str(guild_id)) or {}
    g.setdefault("COMP", DEFAULT_COMP.copy())
    g.setdefault("AUTO_PING_ENABLED", False)
    cfg[str(guild_id)] = g
    return g


def _desired_comp(guild_id: int) -> Dict[str, int]:
    g = _guild_cfg(guild_id)
    raw = g.get("COMP") or {}
    return {
        "TANK": int(raw.get("TANK", DEFAULT_COMP["TANK"]) or 0),
        "HEAL": int(raw.get("HEAL", DEFAULT_COMP["HEAL"]) or 0),
        "DPS": int(raw.get("DPS", DEFAULT_COMP["DPS"]) or 0),
    }


def _auto_ping_enabled(guild_id: int) -> bool:
    g = _guild_cfg(guild_id)
    return bool(g.get("AUTO_PING_ENABLED", False))


def _set_auto_ping_enabled(guild_id: int, enabled: bool) -> None:
    g = _guild_cfg(guild_id)
    g["AUTO_PING_ENABLED"] = bool(enabled)
    cfg[str(guild_id)] = g
    save_cfg()


def _set_comp(guild_id: int, tank: int, heal: int, dps: int) -> None:
    g = _guild_cfg(guild_id)
    g["COMP"] = {"TANK": max(0, int(tank)), "HEAL": max(0, int(heal)), "DPS": max(0, int(dps))}
    cfg[str(guild_id)] = g
    save_cfg()


def _user_dm_enabled(guild_id: int, user_id: int) -> bool:
    g = prefs.get(str(guild_id)) or {}
    u = g.get(str(user_id)) or {}
    return bool(u.get("raid_dm_enabled", True))


def _set_user_dm_enabled(guild_id: int, user_id: int, enabled: bool) -> None:
    g = prefs.get(str(guild_id)) or {}
    u = g.get(str(user_id)) or {}
    u["raid_dm_enabled"] = bool(enabled)
    g[str(user_id)] = u
    prefs[str(guild_id)] = g
    save_prefs()


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


# ---------------- Statistik ----------------

def _stats_bucket(guild_id: int, user_id: int) -> dict:
    g = stats.setdefault(str(guild_id), {})
    u = g.setdefault(str(user_id), {
        "invited": 0,
        "responded": 0,
        "yes": 0,
        "maybe": 0,
        "no": 0,
        "tank": 0,
        "heal": 0,
        "dps": 0,
        "events_answered": []
    })
    return u


def _mark_invited(guild_id: int, user_id: int) -> None:
    u = _stats_bucket(guild_id, user_id)
    u["invited"] += 1
    save_stats()


def _mark_response(guild_id: int, user_id: int, event_id: str, group: str) -> None:
    u = _stats_bucket(guild_id, user_id)

    answered = set(str(x) for x in u.get("events_answered", []))
    if event_id not in answered:
        u["responded"] += 1
        answered.add(event_id)
        u["events_answered"] = sorted(answered)

    if group in ("TANK", "HEAL", "DPS"):
        u["yes"] += 1
        if group == "TANK":
            u["tank"] += 1
        elif group == "HEAL":
            u["heal"] += 1
        elif group == "DPS":
            u["dps"] += 1
    elif group == "MAYBE":
        u["maybe"] += 1
    elif group == "NO":
        u["no"] += 1

    save_stats()


def _stats_summary(guild_id: int, user_id: int) -> dict:
    u = _stats_bucket(guild_id, user_id)
    invited = int(u.get("invited", 0))
    responded = int(u.get("responded", 0))
    yes = int(u.get("yes", 0))
    maybe = int(u.get("maybe", 0))
    no = int(u.get("no", 0))

    response_rate = round((responded / invited) * 100, 1) if invited > 0 else 0.0
    commit_rate = round((yes / invited) * 100, 1) if invited > 0 else 0.0

    return {
        "invited": invited,
        "responded": responded,
        "yes": yes,
        "maybe": maybe,
        "no": no,
        "tank": int(u.get("tank", 0)),
        "heal": int(u.get("heal", 0)),
        "dps": int(u.get("dps", 0)),
        "response_rate": response_rate,
        "commit_rate": commit_rate,
    }


def _top_stats_lines(guild: discord.Guild, limit: int = 10) -> List[str]:
    g = stats.get(str(guild.id)) or {}
    scored: List[Tuple[int, float, dict]] = []
    for uid_str in g.keys():
        uid = int(uid_str)
        s = _stats_summary(guild.id, uid)
        score = (s["responded"] * 2) + (s["yes"] * 3) + (s["maybe"] * 1) - (s["no"] * 0.25)
        scored.append((uid, score, s))

    scored.sort(key=lambda x: x[1], reverse=True)
    lines = []
    for i, (uid, _, s) in enumerate(scored[:limit], start=1):
        m = guild.get_member(uid)
        name = m.display_name if m else f"<@{uid}>"
        lines.append(
            f"{i}. **{name}** — Antworten: {s['responded']}, Zusagen: {s['yes']}, Quote: {s['response_rate']}%"
        )
    return lines


# ---------------- Embed / Planung ----------------

def _voters_set(obj: dict) -> set[int]:
    voted: set[int] = set()
    for k in ("TANK", "HEAL", "DPS"):
        voted.update(int(u) for u in obj["yes"].get(k, []))
    voted.update(int(u) for u in obj["no"])
    voted.update(int(uid) for uid in obj["maybe"].keys())
    return voted


def _eligible_members(guild: discord.Guild, obj: dict) -> List[discord.Member]:
    tr_id = int(obj.get("target_role_id", 0) or 0)
    if not tr_id:
        return [m for m in guild.members if not m.bot]
    role = guild.get_role(tr_id)
    if not role:
        return [m for m in guild.members if not m.bot]
    return [m for m in role.members if not m.bot]


def _missing_roles_for_event(guild_id: int, obj: dict) -> Dict[str, int]:
    comp = _desired_comp(guild_id)
    yes = obj.get("yes") or {}
    return {
        "TANK": max(0, comp["TANK"] - len(yes.get("TANK", []))),
        "HEAL": max(0, comp["HEAL"] - len(yes.get("HEAL", []))),
        "DPS": max(0, comp["DPS"] - len(yes.get("DPS", []))),
    }


def _missing_roles_text(guild: discord.Guild, obj: dict) -> str:
    missing = _missing_roles_for_event(guild.id, obj)
    rid_map = get_role_ids_for_guild(guild.id)

    parts = []
    for key in ("TANK", "HEAL", "DPS"):
        count = missing[key]
        if count <= 0:
            continue
        rid = rid_map.get(key, 0)
        role = guild.get_role(rid) if rid else None
        target = role.mention if role else key.title()
        parts.append(f"{count}x {target}")

    return ", ".join(parts) if parts else "Keine fehlenden Rollen."


def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    when = datetime.fromisoformat(obj["when_iso"])
    yes = obj["yes"]
    maybe = obj["maybe"]
    no = obj["no"]

    eligible = _eligible_members(guild, obj)
    voted = _voters_set(obj)
    missing_text = _missing_roles_text(guild, obj)

    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=(
            (obj.get("description", "") or "") +
            f"\n\n🕒 Zeit: {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)"
            f"\n🗳️ Abgestimmt: **{len(voted)}** / **{len(eligible)}**"
            f"\n📌 Fehlend laut Setup: **{missing_text}**"
        ).strip(),
        color=discord.Color.blurple()
    )

    tank_names = [_mention(guild, int(u)) for u in yes.get("TANK", [])]
    heal_names = [_mention(guild, int(u)) for u in yes.get("HEAL", [])]
    dps_names = [_mention(guild, int(u)) for u in yes.get("DPS", [])]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})", value="\n".join(dps_names) or "—", inline=True)

    maybe_lines = []
    for uid_str, rlab in maybe.items():
        try:
            uid_i = int(uid_str)
        except Exception:
            continue
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, int(u)) for u in no]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        if r:
            emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="(An-/Abmeldung läuft per DM-Buttons)")
    return emb


def _calendar_lines_for_guild(guild_id: int, days: int = 7) -> List[Tuple[datetime, str]]:
    now = datetime.now(TZ)
    until = now + timedelta(days=days)
    out: List[Tuple[datetime, str]] = []

    for _, obj in store.items():
        try:
            if int(obj.get("guild_id", 0) or 0) != guild_id:
                continue
            when = datetime.fromisoformat(obj["when_iso"])
            if now <= when <= until:
                out.append((when, obj.get("title", "Event")))
        except Exception:
            continue

    out.sort(key=lambda x: x[0])
    return out


# ---------------- DM View ----------------

class RaidView(View):
    """Buttons laufen in der DM. Nach Klick: Übersicht aktualisieren + DM löschen."""
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _push_overview(self, inter: discord.Interaction, obj: dict):
        guild = inter.client.get_guild(obj["guild_id"])
        if not guild:
            return
        ch = guild.get_channel(obj["channel_id"])
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        try:
            msg = await ch.fetch_message(int(self.msg_id))
        except Exception:
            return
        emb = build_embed(guild, obj)
        try:
            await msg.edit(embed=emb)
        except Exception:
            pass

    async def _confirm_and_delete_dm(self, inter: discord.Interaction, text: str):
        try:
            if not inter.response.is_done():
                await inter.response.send_message(text, ephemeral=True)
            else:
                await inter.followup.send(text, ephemeral=True)
        except Exception:
            pass
        try:
            await inter.message.delete()
        except Exception:
            pass

    async def _update(self, inter: discord.Interaction, group: str):
        try:
            obj = store.get(self.msg_id)
            if not obj:
                await self._confirm_and_delete_dm(inter, "Dieses Event existiert nicht mehr.")
                return

            _init_event_shape(obj)

            uid = inter.user.id

            for k in ("TANK", "HEAL", "DPS"):
                obj["yes"][k] = [int(u) for u in obj["yes"].get(k, []) if int(u) != uid]
            obj["no"] = [int(u) for u in obj.get("no", []) if int(u) != uid]
            obj["maybe"].pop(str(uid), None)

            if group in ("TANK", "HEAL", "DPS"):
                obj["yes"][group].append(uid)
                text = f"Angemeldet als **{group}**."
            elif group == "MAYBE":
                member = _member_from_event(inter, obj)
                rid_map = get_role_ids_for_guild(obj["guild_id"])
                label = _primary_label(member, rid_map)
                obj["maybe"][str(uid)] = label
                text = "Als **Vielleicht** eingetragen."
            elif group == "NO":
                obj["no"].append(uid)
                text = "Als **Abgemeldet** eingetragen."
            else:
                text = "Aktualisiert."

            save_store()
            _mark_response(obj["guild_id"], uid, self.msg_id, group)
            await self._push_overview(inter, obj)
            await self._confirm_and_delete_dm(inter, text)

        except Exception as e:
            await _log(inter.client, store.get(self.msg_id, {}).get("guild_id", 0), f"Button-Fehler: {e!r}")
            await self._confirm_and_delete_dm(inter, "❌ Unerwarteter Fehler. Bitte erneut probieren.")

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="dm_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._update(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._update(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="dm_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._update(inter, "DPS")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._update(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):
        await self._update(inter, "NO")


# ---------------- Auto-Pings ----------------

async def _send_missing_role_ping(client: discord.Client, guild: discord.Guild, event_id: str, obj: dict, minutes_before: int):
    ch = guild.get_channel(int(obj["channel_id"]))
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return

    missing = _missing_roles_for_event(guild.id, obj)
    rid_map = get_role_ids_for_guild(guild.id)

    lines = []
    mentions = []
    for key in ("TANK", "HEAL", "DPS"):
        cnt = missing[key]
        if cnt <= 0:
            continue
        rid = rid_map.get(key, 0)
        role = guild.get_role(rid) if rid else None
        if role:
            mentions.append(role.mention)
            lines.append(f"- {cnt}x {role.mention}")
        else:
            lines.append(f"- {cnt}x {key.title()}")

    if not lines:
        return

    when = datetime.fromisoformat(obj["when_iso"])
    text = (
        f"⚠️ **{obj.get('title', 'Raid')}** startet in **{minutes_before} Minuten**.\n"
        f"Es fehlen noch:\n" +
        "\n".join(lines) +
        f"\n\n🕒 Start: {when.strftime('%d.%m.%Y %H:%M')}"
    )

    prefix = " ".join(dict.fromkeys(mentions)).strip()
    final = f"{prefix}\n{text}" if prefix else text

    await ch.send(final)
    await _log(client, guild.id, f"Auto-Ping gesendet für Event {event_id} ({minutes_before} Min vorher).")


async def auto_ping_due_events(client: discord.Client):
    now = datetime.now(TZ)

    for event_id, obj in list(store.items()):
        try:
            guild_id = int(obj.get("guild_id", 0) or 0)
            if guild_id <= 0:
                continue
            if not _auto_ping_enabled(guild_id):
                continue

            guild = client.get_guild(guild_id)
            if not guild:
                continue

            when = datetime.fromisoformat(obj["when_iso"])
            if now > when + timedelta(hours=2):
                continue

            delta_min = int((when - now).total_seconds() // 60)

            for target_min in AUTO_PING_MINUTES:
                log_key = f"{event_id}:{target_min}"
                if ping_log.get(log_key):
                    continue

                # Fenster: exakt in der Minute
                if target_min - 1 <= delta_min <= target_min:
                    await _send_missing_role_ping(client, guild, event_id, obj, target_min)
                    ping_log[log_key] = True
                    save_ping_log()

        except Exception:
            continue


# ---------------- Commands / Setup ----------------

def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    """Registriert alle Slash-Commands und re-attached persistente DM-Views."""
    # Persistente Views nach Neustart
    for msg_id in list(store.keys()):
        try:
            client.add_view(RaidView(int(msg_id)))
        except Exception:
            pass

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
        c["DPS"] = int(dps_role.id)
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

    # ---- Admin: gewünschte Raid-Komposition
    @tree.command(name="raid_set_comp", description="(Admin) Soll-Besetzung für Auto-Pings und Übersicht setzen")
    async def raid_set_comp(
        inter: discord.Interaction,
        tank: app_commands.Range[int, 0, 20],
        heal: app_commands.Range[int, 0, 20],
        dps: app_commands.Range[int, 0, 50],
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        _set_comp(inter.guild_id, tank, heal, dps)
        await inter.response.send_message(
            f"✅ Soll-Besetzung gespeichert:\n🛡️ Tank: {tank}\n💚 Heal: {heal}\n🗡️ DPS: {dps}",
            ephemeral=True
        )

    # ---- Admin: Auto-Pings an/aus
    @tree.command(name="raid_autoping", description="(Admin) Rollenbasierte Auto-Pings an- oder ausschalten")
    async def raid_autoping(inter: discord.Interaction, enabled: bool):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return
        _set_auto_ping_enabled(inter.guild_id, enabled)
        await inter.response.send_message(
            f"✅ Rollenbasierte Auto-Pings sind jetzt **{'aktiv' if enabled else 'deaktiviert'}**.",
            ephemeral=True
        )

    # ---- Admin: fehlende Rollen sofort pingen
    @tree.command(name="raid_ping_missing_roles", description="(Admin) Pingt fehlende Rollen für ein Event sofort")
    async def raid_ping_missing_roles(inter: discord.Interaction, message_id: str):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        obj = store.get(str(message_id))
        if not obj or int(obj.get("guild_id", 0) or 0) != inter.guild_id:
            await inter.response.send_message("❌ Unbekanntes Event/Message-ID.", ephemeral=True)
            return

        missing = _missing_roles_for_event(inter.guild_id, obj)
        if sum(missing.values()) <= 0:
            await inter.response.send_message("✅ Es fehlen aktuell keine Rollen.", ephemeral=True)
            return

        await _send_missing_role_ping(inter.client, inter.guild, str(message_id), obj, 0)
        await inter.response.send_message("✅ Fehlende Rollen wurden gepingt.", ephemeral=True)

    # ---- User: DM-Opt-out
    @tree.command(name="raid_dm", description="Raid-DMs an/aus oder Status anzeigen")
    @app_commands.describe(mode="on / off / status")
    @app_commands.choices(mode=[
        app_commands.Choice(name="on", value="on"),
        app_commands.Choice(name="off", value="off"),
        app_commands.Choice(name="status", value="status"),
    ])
    async def raid_dm(inter: discord.Interaction, mode: app_commands.Choice[str]):
        current = _user_dm_enabled(inter.guild_id, inter.user.id)

        if mode.value == "status":
            await inter.response.send_message(
                f"📩 Raid-DMs sind für dich aktuell **{'aktiv' if current else 'deaktiviert'}**.",
                ephemeral=True
            )
            return

        enabled = mode.value == "on"
        _set_user_dm_enabled(inter.guild_id, inter.user.id, enabled)
        await inter.response.send_message(
            f"✅ Raid-DMs sind für dich jetzt **{'aktiv' if enabled else 'deaktiviert'}**.",
            ephemeral=True
        )

    # ---- User/Admin: persönliche Statistik
    @tree.command(name="raid_stats", description="Zeigt Raid-Statistiken eines Spielers")
    async def raid_stats(inter: discord.Interaction, member: Optional[discord.Member] = None):
        target = member or inter.user
        s = _stats_summary(inter.guild_id, target.id)
        text = (
            f"📊 **Raid-Statistik für {target.mention}**\n\n"
            f"Einladungen: **{s['invited']}**\n"
            f"Antworten: **{s['responded']}**\n"
            f"Zusagen: **{s['yes']}**\n"
            f"Vielleicht: **{s['maybe']}**\n"
            f"Absagen: **{s['no']}**\n\n"
            f"🛡️ Tank: **{s['tank']}**\n"
            f"💚 Heal: **{s['heal']}**\n"
            f"🗡️ DPS: **{s['dps']}**\n\n"
            f"Antwortquote: **{s['response_rate']}%**\n"
            f"Zusagequote: **{s['commit_rate']}%**"
        )
        await inter.response.send_message(text, ephemeral=True)

    # ---- Admin/User: Topliste
    @tree.command(name="raid_stats_top", description="Zeigt die aktivsten Raid-Spieler")
    async def raid_stats_top(inter: discord.Interaction):
        lines = _top_stats_lines(inter.guild, limit=10)
        if not lines:
            await inter.response.send_message("Noch keine Statistikdaten vorhanden.", ephemeral=True)
            return
        emb = discord.Embed(
            title="📈 Raid-Statistik Topliste",
            description="\n".join(lines),
            color=discord.Color.gold()
        )
        await inter.response.send_message(embed=emb, ephemeral=True)

    # ---- Kalender
    @tree.command(name="raid_calendar", description="Zeigt kommende Events der nächsten 7 Tage")
    async def raid_calendar(inter: discord.Interaction):
        lines = _calendar_lines_for_guild(inter.guild_id, days=7)
        if not lines:
            await inter.response.send_message("📅 Keine kommenden Events in den nächsten 7 Tagen gefunden.", ephemeral=True)
            return

        formatted = [
            f"• **{dt.strftime('%a %d.%m. %H:%M')}** — {title}"
            for dt, title in lines
        ]
        emb = discord.Embed(
            title="📅 Nächste Events (7 Tage)",
            description="\n".join(formatted),
            color=discord.Color.blue()
        )
        await inter.response.send_message(embed=emb, ephemeral=True)

    # ---- Admin: Raid erstellen
    @tree.command(name="raid_create_dm", description="(Admin) Raid/Anmeldung per DM erzeugen + Übersicht posten")
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
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": title.strip(),
            "description": (description or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": [],
            "target_role_id": int(target_role.id) if target_role else 0,
            "invited_ids": [],
        }

        emb = build_embed(inter.guild, obj)
        msg = await ch.send(embed=emb)
        store[str(msg.id)] = obj

        sent = 0
        role_obj = inter.guild.get_role(int(obj.get("target_role_id", 0) or 0)) if obj.get("target_role_id") else None

        invited_ids: List[int] = []
        for m in _eligible_members(inter.guild, obj):
            invited_ids.append(m.id)
            _mark_invited(inter.guild_id, m.id)

            if not _user_dm_enabled(inter.guild_id, m.id):
                continue

            try:
                dm_text = (
                    f"**{title}** – Anmeldung\n"
                    f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                    f"• Übersicht im Server: #{ch.name}\n\n"
                    f"Wähle unten deine Teilnahme."
                )
                await m.send(dm_text, view=RaidView(int(msg.id)))
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        obj["invited_ids"] = invited_ids
        store[str(msg.id)] = obj
        save_store()

        ziel = role_obj.mention if role_obj else "alle Mitglieder (ohne Bots)"
        await inter.followup.send(
            f"✅ Raid erstellt: {msg.jump_url}\n"
            f"🎯 Zielgruppe: {ziel}\n"
            f"✉️ DMs versendet: {sent}\n"
            f"👥 Eingeladen insgesamt: {len(invited_ids)}",
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
        targets = [m for m in eligible if m.id not in already and _user_dm_enabled(guild.id, m.id)]

        sent = 0
        for m in targets:
            try:
                await m.send(
                    f"**{obj['title']}** – du hast noch nicht abgestimmt.\n"
                    f"• Übersicht: <#{obj['channel_id']}>",
                    view=RaidView(int(message_id))
                )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        await inter.followup.send(f"✅ Resent an {sent} Nutzer.", ephemeral=True)

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
        for m in targets:
            if not _user_dm_enabled(inter.guild_id, m.id):
                continue
            try:
                await m.send(
                    f"**{obj['title']}** – Anmeldung\n• Übersicht: <#{obj['channel_id']}>",
                    view=RaidView(int(message_id))
                )
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        await inter.followup.send(f"✅ Resent an {sent} Ziel(e).", ephemeral=True)


# ------------------------------------------------------------
# Auto-Resend für neue Mitglieder (Join nach Event-Start)
# ------------------------------------------------------------

async def auto_resend_for_new_member(member: discord.Member) -> None:
    """
    Bei on_member_join(member) aufrufen.
    Schickt dem neuen Member die RSVP-DM für alle noch relevanten Events seiner Guild:
      - Event gehört zur gleichen Guild
      - Startzeit nicht länger als 2h her oder in Zukunft
      - UND (falls gesetzt) Member besitzt die Zielrolle
      - UND User hat Raid-DMs nicht deaktiviert
    """
    try:
        if member.bot:
            return
        if not _user_dm_enabled(member.guild.id, member.id):
            return

        now = datetime.now(TZ)
        sent = 0

        for mid, obj in list(store.items()):
            try:
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

                text = (
                    f"**{obj.get('title', 'Event')}** – Anmeldung\n"
                    f"• {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)\n"
                    f"• Übersicht im Server: <#{obj.get('channel_id')}>"
                )
                try:
                    await member.send(text, view=RaidView(int(mid)))
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
            except Exception:
                continue

        try:
            if sent and hasattr(member, "_state") and hasattr(member._state, "_get_client"):
                client = member._state._get_client()
                await _log(client, member.guild.id, f"Auto-Resend an {member} -> {sent} DM(s).")
        except Exception:
            pass
    except Exception:
        pass
