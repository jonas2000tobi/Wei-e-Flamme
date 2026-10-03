from __future__ import annotations

import asyncio
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any

import discord
from discord.ext import tasks

try:
    from bot import runtime_db  # type: ignore
    from bot.module_registry import is_module_enabled  # type: ignore
    from bot.world_events_source import EVENT_BY_KEY, merge_events, parse_wakayashi_events  # type: ignore
except Exception:  # pragma: no cover
    import runtime_db  # type: ignore
    from module_registry import is_module_enabled  # type: ignore
    from world_events_source import EVENT_BY_KEY, merge_events, parse_wakayashi_events  # type: ignore

SOURCE_KEY = "wakayashi"
DISPLAY_SOURCE = "Oblivion"
OVERLAY_URL = str(
    os.getenv("WORLD_EVENTS_SOURCE_URL")
    or "https://wakayashi.gg/overlay/aion2/event-timer?server=Europa&sound=0&theme=dark"
).strip()
MAIN_URL = str(os.getenv("WORLD_EVENTS_MAIN_URL") or "https://wakayashi.gg/aion2").strip()
POLL_SECONDS = max(30, int(os.getenv("WORLD_EVENTS_POLL_SECONDS") or "60"))
MAIN_REFRESH_SECONDS = max(180, int(os.getenv("WORLD_EVENTS_MAIN_REFRESH_SECONDS") or "300"))
CHANNEL_NAME = str(os.getenv("WORLD_EVENTS_CHANNEL_NAME") or "welt-events").strip() or "welt-events"

_client: discord.Client | None = None
_main_cache: dict[str, dict[str, Any]] = {}
_main_cache_at: datetime | None = None


def _fetch_text_sync(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; OblivionGuildBot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
            "Cache-Control": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        raw = response.read(2_000_000)
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


async def _fetch_text(url: str) -> str:
    return await asyncio.to_thread(_fetch_text_sync, url)


def _discord_ts(value: str, style: str = "R") -> str:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return f"<t:{int(dt.timestamp())}:{style}>"
    except Exception:
        return "—"


def build_world_events_embed(events: list[dict[str, Any]], observed_at: datetime) -> discord.Embed:
    active = [event for event in events if str(event.get("status") or "").upper() == "ACTIVE"]
    upcoming = [event for event in events if event.get("scheduled_at") and event not in active]
    upcoming.sort(key=lambda event: str(event.get("scheduled_at") or ""))

    embed = discord.Embed(
        title="🌍 AION 2 · Welt-Events",
        description="Live-Übersicht für **Europa**",
        color=0xD6A84F,
        timestamp=observed_at,
    )
    if active:
        lines = []
        for event in active[:4]:
            meta = EVENT_BY_KEY.get(str(event.get("key") or ""), {})
            lines.append(f"{meta.get('icon', '🟢')} **{event.get('name') or 'Event'}** · läuft gerade")
        embed.add_field(name="🟢 Läuft gerade", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="🟢 Läuft gerade", value="Aktuell kein Event als live gemeldet.", inline=False)

    if upcoming:
        lines = []
        for event in upcoming[:6]:
            meta = EVENT_BY_KEY.get(str(event.get("key") or ""), {})
            when = str(event.get("scheduled_at") or "")
            lines.append(
                f"{meta.get('icon', '⏱️')} **{event.get('name') or 'Event'}** · "
                f"{_discord_ts(when, 't')} · {_discord_ts(when, 'R')}"
            )
        embed.add_field(name="⏱ Als Nächstes", value="\n".join(lines), inline=False)
    else:
        embed.add_field(name="⏱ Als Nächstes", value="Keine kommenden Zeiten gefunden.", inline=False)

    embed.add_field(
        name="🗃️ Historie",
        value="Die beobachteten Zeiten werden parallel gespeichert, damit wir später echte Muster vergleichen können.",
        inline=False,
    )
    embed.set_footer(text=f"Daten: {DISPLAY_SOURCE}")
    return embed


async def _ensure_board_channel(guild: discord.Guild) -> discord.TextChannel | None:
    stored = int(runtime_db.get_module_setting(guild.id, "game_information", "world_events_channel_id", 0) or 0)
    if stored:
        channel = guild.get_channel(stored)
        if isinstance(channel, discord.TextChannel):
            return channel
    channel = discord.utils.get(guild.text_channels, name=CHANNEL_NAME)
    if isinstance(channel, discord.TextChannel):
        runtime_db.set_module_setting(guild.id, "game_information", "world_events_channel_id", int(channel.id))
        return channel
    me = guild.me
    if me is None or not me.guild_permissions.manage_channels:
        print(f"⚠️ Welt-Events: Kanal #{CHANNEL_NAME} fehlt in {guild.name}, Bot hat kein 'Kanäle verwalten'.")
        return None
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=False,
            add_reactions=False,
            create_public_threads=False,
            create_private_threads=False,
        ),
        me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            embed_links=True,
            read_message_history=True,
            manage_messages=True,
        ),
    }
    try:
        channel = await guild.create_text_channel(
            CHANNEL_NAME,
            overwrites=overwrites,
            reason="Oblivion Welt-Event-Dashboard",
        )
        runtime_db.set_module_setting(guild.id, "game_information", "world_events_channel_id", int(channel.id))
        return channel
    except Exception as exc:
        print(f"⚠️ Welt-Events: Kanal konnte in {guild.name} nicht erstellt werden: {exc!r}")
        return None


async def _upsert_board(guild: discord.Guild, events: list[dict[str, Any]], observed_at: datetime) -> None:
    channel = await _ensure_board_channel(guild)
    if channel is None:
        return
    message_id = int(runtime_db.get_module_setting(guild.id, "game_information", "world_events_message_id", 0) or 0)
    embed = build_world_events_embed(events, observed_at)
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(content=None, embed=embed)
            return
        except discord.NotFound:
            pass
        except Exception as exc:
            print(f"⚠️ Welt-Events: bestehendes Board in {guild.name} konnte nicht aktualisiert werden: {exc!r}")
    try:
        message = await channel.send(embed=embed)
        runtime_db.set_module_setting(guild.id, "game_information", "world_events_message_id", int(message.id))
        try:
            await message.pin(reason="Oblivion Welt-Event-Dashboard")
        except Exception:
            pass
    except Exception as exc:
        print(f"⚠️ Welt-Events: Board konnte in {guild.name} nicht gesendet werden: {exc!r}")


async def _collect_events() -> tuple[list[dict[str, Any]], datetime]:
    global _main_cache, _main_cache_at
    observed = datetime.now(timezone.utc)
    overlay_html = await _fetch_text(OVERLAY_URL)
    overlay = parse_wakayashi_events(overlay_html, now=observed, main_page=False)
    if len(overlay) < 2:
        raise RuntimeError(f"Wakayashi-Overlay enthielt zu wenige Events ({len(overlay)}).")

    refresh_main = _main_cache_at is None or (observed - _main_cache_at).total_seconds() >= MAIN_REFRESH_SECONDS
    if refresh_main:
        try:
            main_html = await _fetch_text(MAIN_URL)
            main_events = parse_wakayashi_events(main_html, now=observed, main_page=True)
            if main_events:
                _main_cache = {str(event["key"]): event for event in main_events if event.get("key")}
                _main_cache_at = observed
        except Exception as exc:
            print(f"⚠️ Welt-Events: große Wakayashi-Seite konnte nicht aktualisiert werden: {exc!r}")
    return merge_events(overlay, list(_main_cache.values())), observed


@tasks.loop(seconds=POLL_SECONDS)
async def world_event_loop() -> None:
    client = _client
    if client is None or not client.is_ready():
        return
    try:
        events, observed = await _collect_events()
        result = await asyncio.to_thread(
            runtime_db.record_world_event_snapshot,
            events,
            source=SOURCE_KEY,
            observed_at=observed,
        )
        for guild in list(client.guilds):
            try:
                if not is_module_enabled(int(guild.id), "game_information"):
                    continue
                await _upsert_board(guild, events, observed)
            except Exception as exc:
                print(f"⚠️ Welt-Events: Guild-Update {guild.name} fehlgeschlagen: {exc!r}")
        if result.get("snapshot_inserted"):
            print(
                f"🌍 Welt-Events aktualisiert: {len(events)} Events · "
                f"DB-Vorkommen={result.get('occurrence_updates', 0)}"
            )
    except Exception as exc:
        # Quellfehler löschen keine Daten und überschreiben die letzte funktionierende Board-Nachricht nicht.
        print(f"⚠️ Welt-Events Poll fehlgeschlagen: {type(exc).__name__}: {exc}")


@world_event_loop.before_loop
async def _before_world_event_loop() -> None:
    if _client is not None:
        await _client.wait_until_ready()


async def setup_world_events(client: discord.Client, tree: Any = None) -> None:
    del tree
    global _client
    _client = client
    if not world_event_loop.is_running():
        world_event_loop.start()
        print(f"🌍 Welt-Event-Collector gestartet: alle {POLL_SECONDS}s · Daten: {DISPLAY_SOURCE}")
