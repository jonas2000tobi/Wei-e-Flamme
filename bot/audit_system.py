from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import discord
from discord import app_commands

try:
    from bot.runtime_db import init_runtime_db, db_status, write_audit_log, fetch_audit_logs, count_audit_logs  # type: ignore
except Exception:
    from runtime_db import init_runtime_db, db_status, write_audit_log, fetch_audit_logs, count_audit_logs  # type: ignore


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def audit_log(
    *,
    guild_id: Optional[int],
    actor_id: Optional[int],
    action: str,
    target_type: str = "",
    target_id: str = "",
    summary: str = "",
    old_value: Any = None,
    new_value: Any = None,
    metadata: Any = None,
) -> int:
    """Öffentliche Helferfunktion für spätere Module.

    Andere Bot-Dateien können später nur `audit_log(...)` aufrufen, ohne zu
    wissen, ob darunter SQLite/Postgres/sonstwas liegt.
    """
    return write_audit_log(
        guild_id=guild_id,
        actor_id=actor_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        summary=summary,
        old_value=old_value,
        new_value=new_value,
        metadata=metadata,
    )


def _format_dt(value: str) -> str:
    try:
        dt = datetime.fromisoformat(str(value or ""))
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(value or "?")


async def setup_audit_system(client: discord.Client, tree: app_commands.CommandTree):
    info = init_runtime_db()
    print(f"✅ Runtime-DB initialisiert: {info.get('backend')} {info.get('path')}")

    @tree.command(name="audit_status", description="Admin: Status der neuen Runtime-/Audit-Datenbank")
    async def audit_status(inter: discord.Interaction):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return
        st = db_status()
        guild_count = count_audit_logs(inter.guild.id)
        total_count = count_audit_logs(None)
        emb = discord.Embed(
            title="🧾 Audit-/Runtime-Datenbank",
            color=discord.Color.gold(),
        )
        emb.add_field(name="Backend", value=str(st.get("backend", "?")), inline=True)
        emb.add_field(name="Initialisiert", value="Ja" if st.get("initialized") else "Nein", inline=True)
        emb.add_field(name="DATABASE_URL", value="gesetzt" if st.get("database_url_configured") else "nicht gesetzt", inline=True)
        emb.add_field(name="Audit-Logs Server", value=str(guild_count), inline=True)
        emb.add_field(name="Audit-Logs Gesamt", value=str(total_count), inline=True)
        emb.add_field(name="Speicher", value=f"`{st.get('path')}`", inline=False)
        if st.get("database_url_kind"):
            emb.add_field(name="DATABASE_URL-Typ", value=str(st.get("database_url_kind")), inline=True)
        if st.get("postgres_error"):
            emb.add_field(name="Postgres-Fehler", value=f"```{str(st.get('postgres_error'))[:900]}```", inline=False)
        await inter.response.send_message(embed=emb, ephemeral=True)

    @tree.command(name="audit_last", description="Admin: Letzte Audit-Einträge anzeigen")
    @app_commands.describe(limit="Anzahl Einträge, maximal 20")
    async def audit_last(inter: discord.Interaction, limit: int = 10):
        if inter.guild is None or not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return
        rows = fetch_audit_logs(inter.guild.id, max(1, min(int(limit or 10), 20)))
        if not rows:
            await inter.response.send_message("🧾 Noch keine Audit-Einträge für diesen Server.", ephemeral=True)
            return
        lines: list[str] = []
        for r in rows:
            actor = int(r.get("actor_id") or 0)
            who = f"<@{actor}>" if actor else "System"
            action = str(r.get("action") or "?")
            summary = str(r.get("summary") or "")
            when = _format_dt(str(r.get("created_at") or ""))
            line = f"• **{when}** — `{action}` — {who}"
            if summary:
                line += f"\n  {summary[:180]}"
            lines.append(line)
        emb = discord.Embed(
            title="🧾 Letzte Audit-Einträge",
            description="\n".join(lines)[:3900],
            color=discord.Color.gold(),
        )
        await inter.response.send_message(embed=emb, ephemeral=True)

    @client.event
    async def on_app_command_completion(inter: discord.Interaction, command: app_commands.Command):
        try:
            audit_log(
                guild_id=inter.guild_id,
                actor_id=getattr(inter.user, "id", None),
                action="slash_command",
                target_type="command",
                target_id=getattr(command, "qualified_name", getattr(command, "name", "?")),
                summary=f"Slash-Command ausgeführt: /{getattr(command, 'qualified_name', getattr(command, 'name', '?'))}",
                metadata={
                    "channel_id": getattr(inter.channel, "id", None),
                    "user_name": getattr(inter.user, "display_name", str(inter.user)),
                },
            )
        except Exception as e:
            print(f"[audit_system] command completion audit failed: {e!r}")
