from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALLIANCE_FILE = DATA_DIR / "alliance_config.json"
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"


def _load_json(path: Path, default):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


alliance_cfg: dict = _load_json(ALLIANCE_FILE, {})


def save_alliance_cfg() -> None:
    _save_json(ALLIANCE_FILE, alliance_cfg)


def _load_leader_cfg() -> dict:
    return _load_json(LEADER_CONTACT_CFG_FILE, {})


def _guild_cfg(guild_id: int) -> dict:
    g = alliance_cfg.get(str(guild_id)) or {}
    g.setdefault("home_guild_id", int(guild_id))
    g.setdefault("groups", {})
    alliance_cfg[str(guild_id)] = g
    return g


def _normalize_key(name: str) -> str:
    return (
        (name or "")
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


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
    return bool(role and role in inter.user.roles)


def _is_home_guild(inter: discord.Interaction) -> bool:
    if inter.guild is None:
        return False

    g = _guild_cfg(inter.guild.id)
    home_id = int(g.get("home_guild_id", inter.guild.id) or inter.guild.id)
    return int(inter.guild.id) == home_id


def _require_home_leader(inter: discord.Interaction) -> tuple[bool, str]:
    if inter.guild is None:
        return False, "❌ Nur im Server nutzbar."

    if not _is_home_guild(inter):
        return False, "❌ Dieser Befehl ist nur auf dem Home-/Ebolus-Server nutzbar."

    if not _is_leader_or_admin(inter):
        return False, "❌ Nur Leader/Admins."

    return True, ""


def _group_obj(home_guild_id: int, group_name: str) -> Optional[dict]:
    g = _guild_cfg(home_guild_id)
    key = _normalize_key(group_name)

    if not key:
        return None

    return (g.get("groups") or {}).get(key)


def get_alliance_group(home_guild_id: int, group_name: str) -> Optional[dict]:
    return _group_obj(home_guild_id, group_name)


def list_alliance_groups(home_guild_id: int) -> dict:
    return _guild_cfg(home_guild_id).get("groups") or {}


async def setup_alliance_config(client: discord.Client, tree: app_commands.CommandTree):
    @tree.command(name="alliance_home_set", description="(Leader) Setzt diesen Discord als Home-/Ebolus-Server")
    async def alliance_home_set(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        g = _guild_cfg(inter.guild.id)
        g["home_guild_id"] = int(inter.guild.id)
        alliance_cfg[str(inter.guild.id)] = g
        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Home-/Ebolus-Server gesetzt:\n**{inter.guild.name}** `{inter.guild.id}`",
            ephemeral=True
        )

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

        g = _guild_cfg(inter.guild.id)
        groups = g.setdefault("groups", {})

        if key in groups:
            await inter.response.send_message("⚠️ Diese Allianz-Gruppe existiert bereits.", ephemeral=True)
            return

        groups[key] = {
            "name": name.strip(),
            "servers": {}
        }

        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Allianz-Gruppe erstellt:\n**{name.strip()}**\nKey: `{key}`",
            ephemeral=True
        )

    @tree.command(name="alliance_group_delete", description="(Leader) Allianz-Gruppe löschen")
    async def alliance_group_delete(inter: discord.Interaction, group: str, confirm: str):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        if confirm != "DELETE":
            await inter.response.send_message(
                "❌ Sicherheitswort falsch.\nNutze `confirm:DELETE`.",
                ephemeral=True
            )
            return

        key = _normalize_key(group)
        g = _guild_cfg(inter.guild.id)
        groups = g.setdefault("groups", {})

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

        groups = list_alliance_groups(inter.guild.id)

        if not groups:
            await inter.response.send_message("📋 Keine Allianz-Gruppen angelegt.", ephemeral=True)
            return

        lines = []

        for key, group in groups.items():
            servers = group.get("servers") or {}
            lines.append(f"• **{group.get('name', key)}** `/{key}` — Server: **{len(servers)}**")

        emb = discord.Embed(
            title="🤝 Allianz-Gruppen",
            description="\n".join(lines),
            color=discord.Color.blurple()
        )

        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_group_show", description="(Leader) Allianz-Gruppe anzeigen")
    async def alliance_group_show(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(inter.guild.id, key)

        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.get("servers") or {}

        emb = discord.Embed(
            title=f"🤝 Allianz-Gruppe: {obj.get('name', key)}",
            color=discord.Color.blurple()
        )

        if not servers:
            emb.description = "Noch keine Discord-Server in dieser Gruppe."
        else:
            lines = []

            for guild_id, s in servers.items():
                ch_id = int(s.get("channel_id", 0) or 0)
                label = s.get("label", "Unbenannt")
                server_name = s.get("discord_name", "Unbekannter Discord")
                dms = "Ja" if s.get("send_dm", False) else "Nein"

                channel_txt = f"<#{ch_id}>" if ch_id else "—"
                lines.append(
                    f"• **{label}**\n"
                    f"  Discord: `{server_name}`\n"
                    f"  Server-ID: `{guild_id}`\n"
                    f"  Channel: {channel_txt}\n"
                    f"  DMs: **{dms}**"
                )

            emb.description = "\n\n".join(lines)

        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="alliance_server_add", description="(Leader) Discord-Server zu Allianz-Gruppe hinzufügen")
    async def alliance_server_add(
        inter: discord.Interaction,
        group: str,
        label: str,
        channel: discord.TextChannel,
        send_dm: bool = False
    ):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        g = _guild_cfg(inter.guild.id)
        groups = g.setdefault("groups", {})

        if key not in groups:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        target_guild = channel.guild

        groups[key].setdefault("servers", {})
        groups[key]["servers"][str(target_guild.id)] = {
            "guild_id": int(target_guild.id),
            "discord_name": target_guild.name,
            "label": label.strip() or target_guild.name,
            "channel_id": int(channel.id),
            "channel_name": channel.name,
            "send_dm": bool(send_dm),
            "added_by": int(inter.user.id),
        }

        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Server zur Allianz-Gruppe hinzugefügt:\n"
            f"Gruppe: **{groups[key].get('name', key)}**\n"
            f"Label: **{label.strip() or target_guild.name}**\n"
            f"Discord: **{target_guild.name}**\n"
            f"Channel: {channel.mention}\n"
            f"DMs: **{'AN' if send_dm else 'AUS'}**",
            ephemeral=True
        )

    @tree.command(name="alliance_server_remove", description="(Leader) Discord-Server aus Allianz-Gruppe entfernen")
    async def alliance_server_remove(inter: discord.Interaction, group: str, guild_id: str, confirm: str):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        if confirm != "REMOVE":
            await inter.response.send_message(
                "❌ Sicherheitswort falsch.\nNutze `confirm:REMOVE`.",
                ephemeral=True
            )
            return

        key = _normalize_key(group)
        obj = _group_obj(inter.guild.id, key)

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
            f"✅ Server aus Allianz-Gruppe entfernt:\n"
            f"**{old.get('label', guild_id)}** / `{guild_id}`\n\n"
            f"Neue Allianz-Raids werden dort nicht mehr gepostet. Alte Posts bleiben bestehen.",
            ephemeral=True
        )

    @tree.command(name="alliance_server_rename", description="(Leader) Anzeigenamen eines Allianz-Servers ändern")
    async def alliance_server_rename(inter: discord.Interaction, group: str, guild_id: str, new_label: str):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(inter.guild.id, key)

        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.setdefault("servers", {})

        if str(guild_id) not in servers:
            await inter.response.send_message("❌ Server ist nicht in dieser Allianz-Gruppe.", ephemeral=True)
            return

        servers[str(guild_id)]["label"] = new_label.strip()
        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Allianz-Server umbenannt:\n`{guild_id}` → **{new_label.strip()}**",
            ephemeral=True
        )

    @tree.command(name="alliance_server_set_channel", description="(Leader) Zielchannel eines Allianz-Servers ändern")
    async def alliance_server_set_channel(inter: discord.Interaction, group: str, channel: discord.TextChannel):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(inter.guild.id, key)

        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        target_guild = channel.guild
        servers = obj.setdefault("servers", {})

        if str(target_guild.id) not in servers:
            await inter.response.send_message(
                "❌ Dieser Discord-Server ist noch nicht in der Allianz-Gruppe. Nutze zuerst `/alliance_server_add`.",
                ephemeral=True
            )
            return

        servers[str(target_guild.id)]["channel_id"] = int(channel.id)
        servers[str(target_guild.id)]["channel_name"] = channel.name
        servers[str(target_guild.id)]["discord_name"] = target_guild.name
        save_alliance_cfg()

        await inter.response.send_message(
            f"✅ Zielchannel geändert:\n"
            f"Server: **{target_guild.name}**\n"
            f"Channel: {channel.mention}",
            ephemeral=True
        )

    @tree.command(name="alliance_server_list", description="(Leader) Alle Server einer Allianz-Gruppe anzeigen")
    async def alliance_server_list(inter: discord.Interaction, group: str):
        ok, msg = _require_home_leader(inter)

        if not ok:
            await inter.response.send_message(msg, ephemeral=True)
            return

        key = _normalize_key(group)
        obj = _group_obj(inter.guild.id, key)

        if not obj:
            await inter.response.send_message("❌ Allianz-Gruppe nicht gefunden.", ephemeral=True)
            return

        servers = obj.get("servers") or {}

        if not servers:
            await inter.response.send_message("📋 In dieser Allianz-Gruppe sind noch keine Server.", ephemeral=True)
            return

        lines = []

        for guild_id, s in servers.items():
            ch_id = int(s.get("channel_id", 0) or 0)
            lines.append(
                f"• **{s.get('label', 'Unbenannt')}**\n"
                f"  Discord: `{s.get('discord_name', 'Unbekannt')}`\n"
                f"  Guild-ID: `{guild_id}`\n"
                f"  Channel: {f'<#{ch_id}>' if ch_id else '—'}\n"
                f"  DMs: **{'AN' if s.get('send_dm', False) else 'AUS'}**"
            )

        emb = discord.Embed(
            title=f"🤝 Allianz-Server – {obj.get('name', key)}",
            description="\n\n".join(lines),
            color=discord.Color.blurple()
        )

        await inter.response.send_message(embed=emb, ephemeral=True)
