from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord

try:
    from bot import guild_config as central_guild_config  # type: ignore
except Exception:
    import guild_config as central_guild_config  # type: ignore

UTC = timezone.utc


def _log_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    try:
        cid = int(central_guild_config.channel_id(guild.id, "server_log") or 0)
    except Exception:
        cid = 0
    ch = guild.get_channel(cid) if cid else None
    return ch if isinstance(ch, discord.TextChannel) else None


async def _send(guild: discord.Guild, *, title: str, description: str, color: discord.Color, fields=None, thumbnail: str | None = None):
    ch = _log_channel(guild)
    if ch is None:
        return
    emb = discord.Embed(title=title, description=description, color=color, timestamp=datetime.now(UTC))
    for name, value, inline in (fields or []):
        emb.add_field(name=name, value=value or "—", inline=inline)
    if thumbnail:
        emb.set_thumbnail(url=thumbnail)
    emb.set_footer(text=f"Guild-ID: {guild.id}")
    try:
        await ch.send(embed=emb, allowed_mentions=discord.AllowedMentions.none())
    except Exception as exc:
        print(f"[server_log] Send fehlgeschlagen in {guild.id}: {exc!r}", flush=True)


async def _find_recent_audit(guild: discord.Guild, action: discord.AuditLogAction, target_id: int, *, seconds: int = 8):
    # Audit-Log-Einträge können minimal verzögert eintreffen.
    await asyncio.sleep(0.8)
    try:
        async for entry in guild.audit_logs(limit=8, action=action):
            target = getattr(entry, "target", None)
            if int(getattr(target, "id", 0) or 0) != int(target_id):
                continue
            created = getattr(entry, "created_at", None)
            if created and datetime.now(UTC) - created > timedelta(seconds=seconds):
                continue
            return entry
    except (discord.Forbidden, discord.HTTPException):
        return None
    except Exception:
        return None
    return None


def _actor_reason_fields(entry):
    if not entry:
        return []
    actor = getattr(entry, "user", None)
    actor_txt = f"{actor} (`{getattr(actor, 'id', 0)}`)" if actor else "Unbekannt"
    reason = getattr(entry, "reason", None) or "Kein Grund angegeben"
    return [("Ausgeführt von", actor_txt, True), ("Grund", reason, False)]


async def setup_server_log(client: discord.Client, tree=None):
    async def on_member_join(member: discord.Member):
        if member.bot:
            return
        await _send(
            member.guild,
            title="📥 Serverbeitritt",
            description=f"**{member}** ist dem Server beigetreten.",
            color=discord.Color.green(),
            fields=[("Mitglied", f"{member.mention} (`{member.id}`)", False), ("Account erstellt", discord.utils.format_dt(member.created_at, "R"), True)],
            thumbnail=member.display_avatar.url,
        )

    async def on_member_remove(member: discord.Member):
        if member.bot:
            return
        entry = await _find_recent_audit(member.guild, discord.AuditLogAction.kick, member.id)
        if entry:
            await _send(
                member.guild,
                title="🥾 Mitglied gekickt",
                description=f"**{member}** wurde vom Server entfernt.",
                color=discord.Color.orange(),
                fields=[("Mitglied", f"{member} (`{member.id}`)", False), *_actor_reason_fields(entry)],
                thumbnail=member.display_avatar.url,
            )
        else:
            await _send(
                member.guild,
                title="📤 Server verlassen",
                description=f"**{member}** hat den Server verlassen.",
                color=discord.Color.dark_grey(),
                fields=[("Mitglied", f"{member} (`{member.id}`)", False)],
                thumbnail=member.display_avatar.url,
            )

    async def on_member_ban(guild: discord.Guild, user: discord.User):
        entry = await _find_recent_audit(guild, discord.AuditLogAction.ban, user.id)
        await _send(
            guild,
            title="🔨 Mitglied gebannt",
            description=f"**{user}** wurde gebannt.",
            color=discord.Color.red(),
            fields=[("Nutzer", f"{user} (`{user.id}`)", False), *_actor_reason_fields(entry)],
            thumbnail=user.display_avatar.url,
        )

    async def on_member_unban(guild: discord.Guild, user: discord.User):
        entry = await _find_recent_audit(guild, discord.AuditLogAction.unban, user.id)
        await _send(
            guild,
            title="♻️ Bann aufgehoben",
            description=f"Der Bann von **{user}** wurde aufgehoben.",
            color=discord.Color.green(),
            fields=[("Nutzer", f"{user} (`{user.id}`)", False), *_actor_reason_fields(entry)],
            thumbnail=user.display_avatar.url,
        )

    async def on_member_update(before: discord.Member, after: discord.Member):
        if before.bot:
            return
        before_ids = {r.id for r in before.roles}
        after_ids = {r.id for r in after.roles}
        added = [r for r in after.roles if r.id not in before_ids and not r.is_default()]
        removed = [r for r in before.roles if r.id not in after_ids and not r.is_default()]
        if added or removed:
            fields = [("Mitglied", f"{after.mention} (`{after.id}`)", False)]
            if added:
                fields.append(("Rolle hinzugefügt", ", ".join(r.mention for r in added), False))
            if removed:
                fields.append(("Rolle entfernt", ", ".join(r.mention for r in removed), False))
            try:
                action = discord.AuditLogAction.member_role_update
                entry = await _find_recent_audit(after.guild, action, after.id, seconds=10)
                fields.extend(_actor_reason_fields(entry))
            except Exception:
                pass
            await _send(after.guild, title="🎭 Rollen geändert", description=f"Rollen von **{after}** wurden geändert.", color=discord.Color.blurple(), fields=fields, thumbnail=after.display_avatar.url)
        if before.nick != after.nick:
            await _send(
                after.guild,
                title="✏️ Nickname geändert",
                description=f"Nickname von **{after}** wurde geändert.",
                color=discord.Color.blurple(),
                fields=[("Vorher", before.nick or before.name, True), ("Nachher", after.nick or after.name, True), ("Mitglied", f"{after.mention} (`{after.id}`)", False)],
                thumbnail=after.display_avatar.url,
            )

    async def on_guild_role_create(role: discord.Role):
        await _send(role.guild, title="➕ Rolle erstellt", description=f"Rolle **{role.name}** wurde erstellt.", color=discord.Color.green(), fields=[("Rollen-ID", str(role.id), False)])

    async def on_guild_role_delete(role: discord.Role):
        await _send(role.guild, title="➖ Rolle gelöscht", description=f"Rolle **{role.name}** wurde gelöscht.", color=discord.Color.red(), fields=[("Rollen-ID", str(role.id), False)])

    async def on_guild_role_update(before: discord.Role, after: discord.Role):
        changes = []
        if before.name != after.name:
            changes.append(f"Name: `{before.name}` → `{after.name}`")
        if before.permissions.value != after.permissions.value:
            changes.append("Berechtigungen wurden geändert")
        if before.colour != after.colour:
            changes.append("Farbe wurde geändert")
        if changes:
            await _send(after.guild, title="🛠️ Rolle geändert", description=f"**{after.name}**\n" + "\n".join(changes), color=discord.Color.gold(), fields=[("Rollen-ID", str(after.id), False)])

    async def on_guild_channel_create(channel):
        await _send(channel.guild, title="➕ Kanal erstellt", description=f"**{channel.name}** wurde erstellt.", color=discord.Color.green(), fields=[("Kanal", getattr(channel, "mention", channel.name), True), ("Kanal-ID", str(channel.id), True)])

    async def on_guild_channel_delete(channel):
        await _send(channel.guild, title="➖ Kanal gelöscht", description=f"**{channel.name}** wurde gelöscht.", color=discord.Color.red(), fields=[("Kanal-ID", str(channel.id), False)])

    async def on_guild_channel_update(before, after):
        changes = []
        if before.name != after.name:
            changes.append(f"Name: `{before.name}` → `{after.name}`")
        if getattr(before, "category_id", None) != getattr(after, "category_id", None):
            changes.append("Kategorie wurde geändert")
        if getattr(before, "position", None) != getattr(after, "position", None):
            changes.append("Position wurde geändert")
        if getattr(before, "overwrites", None) != getattr(after, "overwrites", None):
            changes.append("Berechtigungen wurden geändert")
        if changes:
            await _send(after.guild, title="🛠️ Kanal geändert", description=f"{getattr(after, 'mention', after.name)}\n" + "\n".join(changes), color=discord.Color.gold(), fields=[("Kanal-ID", str(after.id), False)])

    client.add_listener(on_member_join, "on_member_join")
    client.add_listener(on_member_remove, "on_member_remove")
    client.add_listener(on_member_ban, "on_member_ban")
    client.add_listener(on_member_unban, "on_member_unban")
    client.add_listener(on_member_update, "on_member_update")
    client.add_listener(on_guild_role_create, "on_guild_role_create")
    client.add_listener(on_guild_role_delete, "on_guild_role_delete")
    client.add_listener(on_guild_role_update, "on_guild_role_update")
    client.add_listener(on_guild_channel_create, "on_guild_channel_create")
    client.add_listener(on_guild_channel_delete, "on_guild_channel_delete")
    client.add_listener(on_guild_channel_update, "on_guild_channel_update")
    print("✅ Server-Log-System geladen")
