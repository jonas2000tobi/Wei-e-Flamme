from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional

import discord

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
from discord import app_commands


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALLIANCE_FILE = DATA_DIR / "alliance_config.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"


_JSON_LOCK = threading.RLock()


def _load_json(path: Path, default):
    try:
        if not path.exists():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception as e:
        print(f"[{Path(__file__).stem}] JSON-Lesefehler {path.name}: {e!r}")
        return default


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(obj, indent=2, ensure_ascii=False)
    with _JSON_LOCK:
        try:
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, path)
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


alliance_cfg: dict = _load_json(ALLIANCE_FILE, {})


def save_alliance_cfg() -> None:
    _save_json(ALLIANCE_FILE, alliance_cfg)


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _normalize_key(name: str) -> str:
    return (
        (name or "")
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


def _clean_short_label(value: str) -> str:
    value = (value or "").strip()
    value = "".join(ch for ch in value if ch.isalnum())
    return value[:6]


EVENT_TYPES = [
    "NM Raid",
    "HM Raid",
    "PvP Schlacht",
    "Dimensionsprüfung",
]


def _normalize_event_type(value: str) -> str:
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

    for event_type in EVENT_TYPES:
        if event_type.lower() == raw:
            return event_type

    return ""


def _event_type_text() -> str:
    return ", ".join(EVENT_TYPES)


def _alliance_root() -> dict:
    root = alliance_cfg.setdefault("_global", {})
    root.setdefault("home_guild_id", 0)
    root.setdefault("groups", {})
    return root


def _home_guild_id(default: int = 0) -> int:
    root = _alliance_root()
    return int(root.get("home_guild_id", 0) or default or 0)


def _groups() -> dict:
    return _alliance_root().setdefault("groups", {})



def _templates() -> dict:
    return _alliance_root().setdefault("templates", {})


def _template_key(name: str) -> str:
    return _normalize_key(name)


def get_alliance_template(name: str) -> Optional[dict]:
    key = _template_key(name)
    if not key:
        return None
    return _templates().get(key)


def list_alliance_templates() -> dict:
    return _templates()


def _group_obj(group_name: str) -> Optional[dict]:
    key = _normalize_key(group_name)

    if not key:
        return None

    return _groups().get(key)


def get_alliance_group(group_name: str) -> Optional[dict]:
    return _group_obj(group_name)


def list_alliance_groups() -> dict:
    return _groups()


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

    if not role_id:
        return False

    role = inter.guild.get_role(role_id)
    return bool(role and int(role.id) in {int(r.id) for r in inter.user.roles})


def _is_home_guild(inter: discord.Interaction) -> bool:
    if inter.guild is None:
        return False

    home_id = _home_guild_id(default=inter.guild.id)
    return int(inter.guild.id) == int(home_id)


def _require_home_leader(inter: discord.Interaction) -> tuple[bool, str]:
    if inter.guild is None:
        return False, "❌ Nur im Server nutzbar."

    if not _is_home_guild(inter):
        return False, "❌ Dieser Befehl ist nur auf dem Home-/Ebolus-Server nutzbar."

    if not _is_leader_or_admin(inter):
        return False, "❌ Nur Leader/Admins."

    return True, ""


def _require_partner_admin(inter: discord.Interaction) -> tuple[bool, str]:
    if inter.guild is None:
        return False, "❌ Nur im Server nutzbar."

    if not _is_admin(inter):
        return False, "❌ Nur Server-Admins oder Manage-Server."

    return True, ""


def _server_line(guild_id: str, s: dict) -> str:
    ch_id = int(s.get("channel_id", 0) or 0)
    label = s.get("label", "Unbenannt")
    short_label = s.get("short_label", "—")
    server_name = s.get("discord_name", "Unbekannter Discord")
    dms = "Ja" if s.get("send_dm", False) else "Nein"
    partner = "Ja" if s.get("partner_registered", False) else "Nein"
    channel_txt = f"<#{ch_id}>" if ch_id else "—"

    event_channels = s.get("event_channels") or {}
    event_lines = []
    for event_type in EVENT_TYPES:
        ec = event_channels.get(event_type) or {}
        ec_id = int(ec.get("channel_id", 0) or 0)
        event_lines.append(f"  {event_type}: {f'<#{ec_id}>' if ec_id else '—'}")

    return (
        f"• **{label}** ({short_label})\n"
        f"  Discord: `{server_name}`\n"
        f"  Server-ID: `{guild_id}`\n"
        f"  Fallback-Channel: {channel_txt}\n"
        f"  Event-Channels:\n" + "\n".join(event_lines) + "\n"
        f"  DMs: **{dms}**\n"
        f"  Partner-registriert: **{partner}**"
    )


async def setup_alliance_config(client: discord.Client, tree: app_commands.CommandTree):
    @tree.command(name="alliance_home_set", description="(Leader) Setzt diesen Discord als Home-/Ebolus-Server")
    async def alliance_home_set(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        root = _alliance_root()
        root["home_guild_id"] = int(inter.guild.id)
        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Home-/Ebolus-Server gesetzt:\n**{inter.guild.name}** `{inter.guild.id}`",
            ephemeral=True
        )

    @tree.command(name="alliance_status", description="(Leader) Zeigt globale Allianz-Konfiguration")
    async def alliance_status(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        home_id = _home_guild_id()
        home_guild = client.get_guild(home_id) if home_id else None
        groups = _groups()

        emb = discord.Embed(title="🤝 Allianz-Konfiguration", color=discord.Color.blurple())
        emb.add_field(
            name="Home-/Ebolus-Server",
            value=f"{home_guild.name if home_guild else 'Unbekannt / nicht gesetzt'} `{home_id or '—'}`",
            inline=False
        )
        emb.add_field(name="Allianz-Gruppen", value=str(len(groups)), inline=True)
        emb.add_field(name="Dieser Server", value=f"{inter.guild.name} `{inter.guild.id}`", inline=False)
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_group_create", description="(Leader) Allianz-Gruppe anlegen")
    async def alliance_group_create(inter: discord.Interaction, name: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(name)
        if not key:
            await inter.response.send_message("❌ Gruppenname ungültig.", ephemeral=True)
            return

        groups = _groups()
        if key in groups:
            await inter.response.send_message("⚠️ Diese Allianz-Gruppe existiert bereits.", ephemeral=True)
            return

        groups[key] = {"name": name.strip(), "created_by": int(inter.user.id), "servers": {}}
        save_alliance_cfg()
        await inter.response.send_message(f"✅ Allianz-Gruppe erstellt:\n**{name.strip()}**\nKey: `{key}`", ephemeral=True)

    @tree.command(name="alliance_group_delete", description="(Leader) Allianz-Gruppe löschen")
    async def alliance_group_delete(inter: discord.Interaction, group: str, confirm: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return
        if confirm != "DELETE":
            await inter.response.send_message("❌ Sicherheitswort falsch.\nNutze `confirm:DELETE`.", ephemeral=True)
            return

        key = _normalize_key(group)
        groups = _groups()
        if key not in groups:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        old_name = groups[key].get("name", key)
        groups.pop(key, None)
        save_alliance_cfg()
        await inter.response.send_message(
            f"✅ Allianz-Gruppe gelöscht:\n**{old_name}**\n\nAlte Events werden dadurch nicht rückwirkend gelöscht.",
            ephemeral=True
        )

    @tree.command(name="alliance_group_list", description="(Leader) Allianz-Gruppen anzeigen")
    async def alliance_group_list(inter: discord.Interaction):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        groups = list_alliance_groups()
        if not groups:
            await inter.response.send_message("📋 Keine Allianz-Gruppen angelegt.", ephemeral=True)
            return

        lines = []
        for key, group in groups.items():
            servers = group.get("servers") or {}
            lines.append(f"• **{group.get('name', key)}** `/{key}` — Server: **{len(servers)}**")

        emb = discord.Embed(title="🤝 Allianz-Gruppen", description="\n".join(lines), color=discord.Color.blurple())
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_group_show", description="(Leader) Allianz-Gruppe anzeigen")
    async def alliance_group_show(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.get("servers") or {}
        emb = discord.Embed(title=f"🤝 Allianz-Gruppe: {obj.get('name', key)}", color=discord.Color.blurple())
        emb.description = "Noch keine Discord-Server in dieser Gruppe." if not servers else "\n\n".join(_server_line(guild_id, s) for guild_id, s in servers.items())
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_server_add_home", description="(Leader) Home-/Ebolus-Server zu Allianz-Gruppe hinzufügen")
    async def alliance_server_add_home(
        inter: discord.Interaction,
        group: str,
        label: str,
        short_label: str,
        send_dm: bool = True
    ):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        short = _clean_short_label(short_label)
        if not short:
            await inter.response.send_message("❌ short_label darf nur Buchstaben/Zahlen enthalten, z. B. `EB`.", ephemeral=True)
            return

        key = _normalize_key(group)
        groups = _groups()
        if key not in groups:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            target_guild = channel.guild
            if target_guild.id != pick_inter.guild.id:
                await pick_inter.response.edit_message(content="❌ Für Home-Add muss der Channel auf diesem Server liegen.", view=None)
                return
            groups[key].setdefault("servers", {})
            groups[key]["servers"][str(target_guild.id)] = {
                "guild_id": int(target_guild.id),
                "discord_name": target_guild.name,
                "label": label.strip() or target_guild.name,
                "short_label": short,
                "channel_id": int(channel.id),
                "channel_name": channel.name,
                "send_dm": bool(send_dm),
                "home": True,
                "partner_registered": False,
                "added_by": int(pick_inter.user.id),
                "event_channels": {},
            }
            save_alliance_cfg()
            await pick_inter.response.edit_message(
                content=(
                    f"✅ Home-Server zur Allianz-Gruppe hinzugefügt:\n"
                    f"Gruppe: **{groups[key].get('name', key)}**\n"
                    f"Label: **{label.strip() or target_guild.name}**\n"
                    f"Kürzel: **{short}**\n"
                    f"Discord: **{target_guild.name}**\n"
                    f"Channel: {channel.mention}\n"
                    f"DMs: **{'AN' if send_dm else 'AUS'}**"
                ),
                view=None,
            )

        await send_text_channel_picker(inter, "🤝 Home-Allianz-Channel auswählen", _picked)

    @tree.command(name="alliance_partner_register", description="(Partner-Admin) Diesen Discord für eine Allianz-Gruppe registrieren")
    async def alliance_partner_register(
        inter: discord.Interaction,
        group: str,
        label: str,
        short_label: str
    ):
        ok, msg = _require_partner_admin(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        short = _clean_short_label(short_label)
        if not short:
            await inter.response.send_message("❌ short_label darf nur Buchstaben/Zahlen enthalten, z. B. `Ga`.", ephemeral=True)
            return

        home_id = _home_guild_id()
        if not home_id:
            await inter.response.send_message("❌ Home-/Ebolus-Server ist noch nicht gesetzt. Auf Ebolus zuerst `/alliance_home_set` ausführen.", ephemeral=True)
            return

        key = _normalize_key(group)
        groups = _groups()
        if key not in groups:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden. Die Gruppe muss zuerst auf dem Ebolus-Server angelegt werden.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            if channel.guild.id != pick_inter.guild.id:
                await pick_inter.response.edit_message(content="❌ Der Channel muss auf diesem Discord-Server liegen.", view=None)
                return

            groups[key].setdefault("servers", {})
            groups[key]["servers"][str(pick_inter.guild.id)] = {
                "guild_id": int(pick_inter.guild.id),
                "discord_name": pick_inter.guild.name,
                "label": label.strip() or pick_inter.guild.name,
                "short_label": short,
                "channel_id": int(channel.id),
                "channel_name": channel.name,
                "send_dm": False,
                "home": int(pick_inter.guild.id) == int(home_id),
                "partner_registered": True,
                "registered_by": int(pick_inter.user.id),
                "event_channels": {},
            }
            save_alliance_cfg()
            await pick_inter.response.edit_message(
                content=(
                    f"✅ Partner-Discord registriert:\n"
                    f"Gruppe: **{groups[key].get('name', key)}**\n"
                    f"Label: **{label.strip() or pick_inter.guild.name}**\n"
                    f"Kürzel: **{short}**\n"
                    f"Channel: {channel.mention}\n\n"
                    f"DMs sind für Partner-Server automatisch **AUS**."
                ),
                view=None,
            )

        await send_text_channel_picker(inter, "🤝 Partner-Allianz-Channel auswählen", _picked)

    @tree.command(name="alliance_partner_unregister", description="(Partner-Admin) Diesen Discord aus einer Allianz-Gruppe entfernen")
    async def alliance_partner_unregister(inter: discord.Interaction, group: str, confirm: str):
        ok, msg = _require_partner_admin(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if confirm != "REMOVE":
            await inter.response.send_message("❌ Sicherheitswort falsch.\nNutze `confirm:REMOVE`.", ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        if str(inter.guild.id) not in servers:
            await inter.response.send_message("❌ Dieser Discord ist nicht in dieser Allianz-Gruppe registriert.", ephemeral=True)
            return

        old = servers.pop(str(inter.guild.id), {})
        save_alliance_cfg()
        await inter.response.send_message(
            f"✅ Dieser Discord wurde aus der Allianz-Gruppe entfernt:\n**{old.get('label', inter.guild.name)}**\n\nNeue Allianz-Raids werden hier nicht mehr gepostet. Alte Posts bleiben bestehen.",
            ephemeral=True
        )

    @tree.command(name="alliance_partner_status", description="(Partner-Admin) Zeigt Registrierung dieses Discords")
    async def alliance_partner_status(inter: discord.Interaction):
        ok, msg = _require_partner_admin(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        found = []
        for key, group in _groups().items():
            servers = group.get("servers") or {}
            s = servers.get(str(inter.guild.id))
            if s:
                found.append((key, group, s))

        if not found:
            await inter.response.send_message("📋 Dieser Discord ist in keiner Allianz-Gruppe registriert.", ephemeral=True)
            return

        lines = []
        for key, group, s in found:
            ch_id = int(s.get("channel_id", 0) or 0)
            lines.append(
                f"• Gruppe: **{group.get('name', key)}**\n"
                f"  Label: **{s.get('label', inter.guild.name)}**\n"
                f"  Kürzel: **{s.get('short_label', '—')}**\n"
                f"  Channel: {f'<#{ch_id}>' if ch_id else '—'}"
            )

        emb = discord.Embed(title=f"🤝 Partner-Status – {inter.guild.name}", description="\n\n".join(lines), color=discord.Color.blurple())
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_server_remove", description="(Leader) Discord-Server aus Allianz-Gruppe entfernen")
    async def alliance_server_remove(inter: discord.Interaction, group: str, guild_id: str, confirm: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return
        if confirm != "REMOVE":
            await inter.response.send_message("❌ Sicherheitswort falsch.\nNutze `confirm:REMOVE`.", ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        if str(guild_id) not in servers:
            await inter.response.send_message("❌ Server ist nicht in dieser Allianz-Gruppe.", ephemeral=True)
            return

        old = servers.pop(str(guild_id), {})
        save_alliance_cfg()
        await inter.response.send_message(
            f"✅ Server aus Allianz-Gruppe entfernt:\n**{old.get('label', guild_id)}** / `{guild_id}`\n\nNeue Allianz-Raids werden dort nicht mehr gepostet. Alte Posts bleiben bestehen.",
            ephemeral=True
        )

    @tree.command(name="alliance_server_rename", description="(Leader) Anzeigename und Kürzel eines Allianz-Servers ändern")
    async def alliance_server_rename(inter: discord.Interaction, group: str, guild_id: str, new_label: str, new_short_label: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        short = _clean_short_label(new_short_label)
        if not short:
            await inter.response.send_message("❌ new_short_label darf nur Buchstaben/Zahlen enthalten, z. B. `SG`.", ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        if str(guild_id) not in servers:
            await inter.response.send_message("❌ Server ist nicht in dieser Allianz-Gruppe.", ephemeral=True)
            return

        servers[str(guild_id)]["label"] = new_label.strip()
        servers[str(guild_id)]["short_label"] = short
        save_alliance_cfg()
        await inter.response.send_message(f"✅ Allianz-Server umbenannt:\n`{guild_id}` → **{new_label.strip()}** ({short})", ephemeral=True)

    @tree.command(name="alliance_server_set_channel_home", description="(Leader) Zielchannel des Home-Servers ändern")
    async def alliance_server_set_channel_home(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        if str(inter.guild.id) not in servers:
            await inter.response.send_message("❌ Home-Server ist noch nicht in dieser Allianz-Gruppe. Nutze zuerst `/alliance_server_add_home`.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            servers[str(pick_inter.guild.id)]["channel_id"] = int(channel.id)
            servers[str(pick_inter.guild.id)]["channel_name"] = channel.name
            servers[str(pick_inter.guild.id)]["discord_name"] = pick_inter.guild.name
            save_alliance_cfg()
            await pick_inter.response.edit_message(content=f"✅ Home-Zielchannel geändert:\nServer: **{pick_inter.guild.name}**\nChannel: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "🤝 Home-Zielchannel auswählen", _picked)

    @tree.command(name="alliance_event_channel_set", description="(Leader) Eventtyp-Channel für den Home-/Ebolus-Server setzen")
    async def alliance_event_channel_set(inter: discord.Interaction, group: str, event_type: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        normalized = _normalize_event_type(event_type)
        if not normalized:
            await inter.response.send_message(f"❌ Ungültiger Eventtyp. Erlaubt: {_event_type_text()}", ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        server = servers.get(str(inter.guild.id))
        if not server:
            await inter.response.send_message("❌ Home-Server ist noch nicht in dieser Gruppe. Nutze zuerst `/alliance_server_add_home`.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            event_channels = server.setdefault("event_channels", {})
            event_channels[normalized] = {
                "channel_id": int(channel.id),
                "channel_name": channel.name,
            }
            save_alliance_cfg()
            await pick_inter.response.edit_message(
                content=f"✅ Event-Channel gesetzt:\nGruppe: **{obj.get('name', key)}**\nEventtyp: **{normalized}**\nServer: **{pick_inter.guild.name}**\nChannel: {channel.mention}",
                view=None,
            )

        await send_text_channel_picker(inter, "🤝 Allianz-Event-Channel auswählen", _picked)

    @tree.command(name="alliance_partner_channel_set", description="(Partner-Admin) Eventtyp-Channel für diesen Partner-Server setzen")
    async def alliance_partner_event_channel_set(inter: discord.Interaction, group: str, event_type: str):
        ok, msg = _require_partner_admin(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        normalized = _normalize_event_type(event_type)
        if not normalized:
            await inter.response.send_message(f"❌ Ungültiger Eventtyp. Erlaubt: {_event_type_text()}", ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})
        server = servers.get(str(inter.guild.id))
        if not server:
            await inter.response.send_message("❌ Dieser Discord ist noch nicht registriert. Nutze zuerst `/alliance_partner_register`.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            event_channels = server.setdefault("event_channels", {})
            event_channels[normalized] = {
                "channel_id": int(channel.id),
                "channel_name": channel.name,
            }
            save_alliance_cfg()
            await pick_inter.response.edit_message(
                content=f"✅ Partner-Event-Channel gesetzt:\nGruppe: **{obj.get('name', key)}**\nEventtyp: **{normalized}**\nServer: **{pick_inter.guild.name}**\nChannel: {channel.mention}",
                view=None,
            )

        await send_text_channel_picker(inter, "🤝 Partner-Event-Channel auswählen", _picked)

    @tree.command(name="alliance_event_channel_list", description="(Leader) Eventtyp-Channels einer Allianz-Gruppe anzeigen")
    async def alliance_event_channel_list(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.get("servers") or {}
        if not servers:
            await inter.response.send_message("📋 Keine Server in dieser Gruppe.", ephemeral=True)
            return

        lines = []
        for guild_id, s in servers.items():
            lines.append(f"**{s.get('label', guild_id)}** ({s.get('short_label', '—')})")
            ecs = s.get("event_channels") or {}
            for et in EVENT_TYPES:
                ec = ecs.get(et) or {}
                cid = int(ec.get("channel_id", 0) or 0)
                lines.append(f"• {et}: {f'<#{cid}>' if cid else '—'}")
            lines.append("")

        emb = discord.Embed(
            title=f"📌 Event-Channels – {obj.get('name', key)}",
            description="\n".join(lines).strip(),
            color=discord.Color.blurple()
        )
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_template_add", description="(Leader) Allianz-Raid-Template speichern")
    async def alliance_template_add(
        inter: discord.Interaction,
        name: str,
        group: str,
        event_type: str,
        title: str,
        description: str,
        default_time: str = "21:00",
        target_role: Optional[discord.Role] = None
    ):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        normalized = _normalize_event_type(event_type)
        if not normalized:
            await inter.response.send_message(f"❌ Ungültiger Eventtyp. Erlaubt: {_event_type_text()}", ephemeral=True)
            return

        if not _group_obj(group):
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        tkey = _template_key(name)
        if not tkey:
            await inter.response.send_message("❌ Template-Name ungültig.", ephemeral=True)
            return

        # simple HH:MM check
        try:
            hh, mm = [int(x) for x in default_time.split(":")]
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                raise ValueError
        except Exception:
            await inter.response.send_message("❌ default_time muss im Format HH:MM sein, z. B. `21:00`.", ephemeral=True)
            return

        templates = _templates()
        templates[tkey] = {
            "name": name.strip(),
            "group": group.strip(),
            "event_type": normalized,
            "title": title.strip(),
            "description": description.strip(),
            "default_time": default_time.strip(),
            "target_role_id": int(target_role.id) if target_role else 0,
            "created_by": int(inter.user.id),
        }
        save_alliance_cfg()

        role_txt = target_role.mention if target_role else "—"
        await inter.response.send_message(
            f"✅ Allianz-Template gespeichert:\n**{name.strip()}**\nGruppe: **{group.strip()}**\nEventtyp: **{normalized}**\nStandardzeit: **{default_time.strip()}**\nZielrolle: {role_txt}",
            ephemeral=True
        )

    @tree.command(name="alliance_template_list", description="(Leader) Allianz-Raid-Templates anzeigen")
    async def alliance_template_list(inter: discord.Interaction):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        templates = list_alliance_templates()
        if not templates:
            await inter.response.send_message("📋 Keine Allianz-Templates gespeichert.", ephemeral=True)
            return

        lines = []
        for key, t in templates.items():
            lines.append(f"• **{t.get('name', key)}** — {t.get('event_type', '—')} — {t.get('group', '—')} — {t.get('default_time', '—')}")

        emb = discord.Embed(title="📋 Allianz-Raid-Templates", description="\n".join(lines), color=discord.Color.blurple())
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_template_show", description="(Leader) Allianz-Raid-Template anzeigen")
    async def alliance_template_show(inter: discord.Interaction, name: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        t = get_alliance_template(name)
        if not t:
            await inter.response.send_message("❌ Template nicht gefunden.", ephemeral=True)
            return

        role_id = int(t.get("target_role_id", 0) or 0)
        role_txt = f"<@&{role_id}>" if role_id else "—"
        emb = discord.Embed(title=f"📋 Template: {t.get('name', name)}", color=discord.Color.blurple())
        emb.add_field(name="Gruppe", value=str(t.get("group", "—")), inline=False)
        emb.add_field(name="Eventtyp", value=str(t.get("event_type", "—")), inline=True)
        emb.add_field(name="Standardzeit", value=str(t.get("default_time", "—")), inline=True)
        emb.add_field(name="Zielrolle", value=role_txt, inline=True)
        emb.add_field(name="Titel", value=str(t.get("title", "—")), inline=False)
        emb.add_field(name="Beschreibung", value=str(t.get("description", "—"))[:1000] or "—", inline=False)
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_template_delete", description="(Leader) Allianz-Raid-Template löschen")
    async def alliance_template_delete(inter: discord.Interaction, name: str, confirm: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        if confirm != "DELETE":
            await inter.response.send_message("❌ Sicherheitswort falsch. Nutze `confirm:DELETE`.", ephemeral=True)
            return

        key = _template_key(name)
        templates = _templates()
        if key not in templates:
            await inter.response.send_message("❌ Template nicht gefunden.", ephemeral=True)
            return

        old = templates.pop(key)
        save_alliance_cfg()
        await inter.response.send_message(f"✅ Template gelöscht: **{old.get('name', name)}**", ephemeral=True)

    @tree.command(name="alliance_server_list", description="(Leader) Alle Server einer Allianz-Gruppe anzeigen")
    async def alliance_server_list(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)
        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(key)
        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.get("servers") or {}
        if not servers:
            await inter.response.send_message("📋 In dieser Allianz-Gruppe sind noch keine Server.", ephemeral=True)
            return

        emb = discord.Embed(
            title=f"🤝 Allianz-Server – {obj.get('name', key)}",
            description="\n\n".join(_server_line(guild_id, s) for guild_id, s in servers.items()),
            color=discord.Color.blurple()
        )
        await inter.response.send_message(embed=emb, ephemeral=True)
