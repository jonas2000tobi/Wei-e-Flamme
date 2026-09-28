from __future__ import annotations

from typing import Awaitable, Callable, Iterable

import discord
from discord.ui import View, Select
from discord.enums import ButtonStyle

TextChannelCallback = Callable[[discord.Interaction, discord.TextChannel], Awaitable[None]]
VoiceChannelCallback = Callable[[discord.Interaction, discord.VoiceChannel], Awaitable[None]]


def _cat_name(ch: discord.abc.GuildChannel) -> str:
    cat = getattr(ch, "category", None)
    return str(getattr(cat, "name", "Ohne Kategorie") or "Ohne Kategorie")


def _truncate(text: str, limit: int = 100) -> str:
    text = str(text or "")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _sort_key(ch: discord.abc.GuildChannel):
    return (
        getattr(getattr(ch, "category", None), "position", 9999) if getattr(ch, "category", None) else 9999,
        getattr(ch, "position", 9999),
        str(getattr(ch, "name", "")).lower(),
    )


def _dedupe_channels(channels: Iterable[discord.abc.GuildChannel]) -> list[discord.abc.GuildChannel]:
    seen: set[int] = set()
    out: list[discord.abc.GuildChannel] = []
    for ch in channels:
        cid = int(getattr(ch, "id", 0) or 0)
        if not cid or cid in seen:
            continue
        seen.add(cid)
        out.append(ch)
    return out


def _cached_text_channels(guild: discord.Guild) -> list[discord.TextChannel]:
    # guild.text_channels ist manchmal nicht vollständig genug für unsere Zwecke.
    # Bleibt aber als Fallback drin, falls REST fetch_channels nicht geht.
    chans = [ch for ch in (getattr(guild, "text_channels", []) or []) if isinstance(ch, discord.TextChannel)]
    return sorted(_dedupe_channels(chans), key=_sort_key)  # type: ignore[arg-type]


def _cached_voice_channels(guild: discord.Guild) -> list[discord.VoiceChannel]:
    chans = [ch for ch in (getattr(guild, "voice_channels", []) or []) if isinstance(ch, discord.VoiceChannel)]
    return sorted(_dedupe_channels(chans), key=_sort_key)  # type: ignore[arg-type]


async def _fetch_text_channels(guild: discord.Guild) -> list[discord.TextChannel]:
    """Hole Textkanäle möglichst vollständig über Discord REST.

    Wichtig: Die native Slash-Channel-Auswahl und teilweise guild.text_channels
    können Kanäle unterschlagen. fetch_channels() liefert die Serverkanäle frischer
    und ist deshalb die Basis für unsere eigene Auswahl. Fallback ist der Cache.
    """
    fetched: list[discord.abc.GuildChannel] = []
    try:
        fetched = list(await guild.fetch_channels())
    except Exception as e:
        print(f"[channel_picker] fetch text channels failed, using cache: {e!r}")

    channels: list[discord.TextChannel] = []
    for ch in fetched:
        if isinstance(ch, discord.TextChannel):
            channels.append(ch)

    # Cache zusätzlich mergen, falls REST aus irgendeinem Grund weniger liefert.
    channels.extend(_cached_text_channels(guild))
    return sorted(_dedupe_channels(channels), key=_sort_key)  # type: ignore[arg-type]


async def _fetch_voice_channels(guild: discord.Guild) -> list[discord.VoiceChannel]:
    fetched: list[discord.abc.GuildChannel] = []
    try:
        fetched = list(await guild.fetch_channels())
    except Exception as e:
        print(f"[channel_picker] fetch voice channels failed, using cache: {e!r}")

    channels: list[discord.VoiceChannel] = []
    for ch in fetched:
        if isinstance(ch, discord.VoiceChannel):
            channels.append(ch)

    channels.extend(_cached_voice_channels(guild))
    return sorted(_dedupe_channels(channels), key=_sort_key)  # type: ignore[arg-type]


class _TextChannelSelect(Select):
    def __init__(self, parent: "TextChannelPickerView"):
        self.parent_view_ref = parent
        options: list[discord.SelectOption] = []
        for ch in parent.page_items():
            options.append(discord.SelectOption(
                label=_truncate(f"#{ch.name}"),
                value=str(ch.id),
                description=_truncate(_cat_name(ch)),
            ))
        if not options:
            options.append(discord.SelectOption(label="Keine Textkanäle gefunden", value="0", description="Keine Auswahl möglich"))
        super().__init__(placeholder="Textkanal auswählen …", min_values=1, max_values=1, options=options)

    async def callback(self, inter: discord.Interaction):
        parent = self.parent_view_ref
        if int(inter.user.id) != int(parent.user_id):
            await inter.response.send_message("❌ Diese Auswahl gehört nicht dir.", ephemeral=True)
            return
        channel_id = int(self.values[0]) if self.values and str(self.values[0]).isdigit() else 0
        if not channel_id:
            await inter.response.send_message("❌ Kein Kanal auswählbar.", ephemeral=True)
            return
        channel = parent.guild.get_channel(channel_id)
        if channel is None:
            try:
                fetched = await inter.client.fetch_channel(channel_id)
                channel = fetched if isinstance(fetched, discord.abc.GuildChannel) else None
            except Exception:
                channel = None
        if not isinstance(channel, discord.TextChannel):
            await inter.response.send_message("❌ Kanal nicht gefunden oder kein normaler Textkanal.", ephemeral=True)
            return
        try:
            await parent.on_select(inter, channel)
        except Exception as exc:
            print(f"[channel_picker] text channel callback failed: {exc!r}", flush=True)
            message = f"❌ Kanal konnte nicht gespeichert werden: {type(exc).__name__}: {exc}"
            if inter.response.is_done():
                await inter.followup.send(message, ephemeral=True)
            else:
                await inter.response.send_message(message, ephemeral=True)


class TextChannelPickerView(View):
    def __init__(self, guild: discord.Guild, user_id: int, on_select: TextChannelCallback, *, title: str = "Textkanal auswählen", channels: list[discord.TextChannel] | None = None, page: int = 0):
        super().__init__(timeout=180)
        self.guild = guild
        self.user_id = int(user_id)
        self.on_select = on_select
        self.title = title
        self.channels = sorted(_dedupe_channels(channels or _cached_text_channels(guild)), key=_sort_key)  # type: ignore[arg-type]
        self.page = max(0, int(page))
        self.per_page = 25
        self.max_page = max(0, (len(self.channels) - 1) // self.per_page)
        if self.page > self.max_page:
            self.page = self.max_page
        self._rebuild()

    def page_items(self) -> list[discord.TextChannel]:
        start = self.page * self.per_page
        return self.channels[start:start + self.per_page]

    def _rebuild(self) -> None:
        self.clear_items()
        self.add_item(_TextChannelSelect(self))
        if self.max_page > 0:
            prev_btn = discord.ui.Button(label="◀️ Zurück", style=ButtonStyle.secondary, disabled=self.page <= 0)
            next_btn = discord.ui.Button(label="Weiter ▶️", style=ButtonStyle.secondary, disabled=self.page >= self.max_page)

            async def _prev(inter: discord.Interaction):
                await self._turn_page(inter, -1)

            async def _next(inter: discord.Interaction):
                await self._turn_page(inter, 1)

            prev_btn.callback = _prev  # type: ignore[method-assign]
            next_btn.callback = _next  # type: ignore[method-assign]
            self.add_item(prev_btn)
            self.add_item(next_btn)

    async def _turn_page(self, inter: discord.Interaction, direction: int):
        if int(inter.user.id) != int(self.user_id):
            await inter.response.send_message("❌ Diese Auswahl gehört nicht dir.", ephemeral=True)
            return
        self.page = max(0, min(self.max_page, self.page + direction))
        self._rebuild()
        await inter.response.edit_message(content=self.message_text(), view=self)

    def message_text(self) -> str:
        total = len(self.channels)
        if self.max_page > 0:
            return f"{self.title}\nSeite **{self.page + 1}/{self.max_page + 1}** · **{total}** Textkanäle gefunden. Mit **Weiter ▶️** durchblättern."
        return f"{self.title}\n**{total}** Textkanäle gefunden."


class _VoiceChannelSelect(Select):
    def __init__(self, parent: "VoiceChannelPickerView"):
        self.parent_view_ref = parent
        options: list[discord.SelectOption] = []
        for ch in parent.page_items():
            members = len(getattr(ch, "members", []) or [])
            options.append(discord.SelectOption(
                label=_truncate(f"🔊 {ch.name}"),
                value=str(ch.id),
                description=_truncate(f"{_cat_name(ch)} • {members} Nutzer"),
            ))
        if not options:
            options.append(discord.SelectOption(label="Keine Voice-Kanäle gefunden", value="0", description="Keine Auswahl möglich"))
        super().__init__(placeholder="Voice-Kanal auswählen …", min_values=1, max_values=1, options=options)

    async def callback(self, inter: discord.Interaction):
        parent = self.parent_view_ref
        if int(inter.user.id) != int(parent.user_id):
            await inter.response.send_message("❌ Diese Auswahl gehört nicht dir.", ephemeral=True)
            return
        channel_id = int(self.values[0]) if self.values and str(self.values[0]).isdigit() else 0
        if not channel_id:
            await inter.response.send_message("❌ Kein Voice-Kanal auswählbar.", ephemeral=True)
            return
        channel = parent.guild.get_channel(channel_id)
        if channel is None:
            try:
                fetched = await inter.client.fetch_channel(channel_id)
                channel = fetched if isinstance(fetched, discord.abc.GuildChannel) else None
            except Exception:
                channel = None
        if not isinstance(channel, discord.VoiceChannel):
            await inter.response.send_message("❌ Kanal nicht gefunden oder kein Voice-Kanal.", ephemeral=True)
            return
        try:
            await parent.on_select(inter, channel)
        except Exception as exc:
            print(f"[channel_picker] voice channel callback failed: {exc!r}", flush=True)
            message = f"❌ Voice-Kanal konnte nicht gespeichert werden: {type(exc).__name__}: {exc}"
            if inter.response.is_done():
                await inter.followup.send(message, ephemeral=True)
            else:
                await inter.response.send_message(message, ephemeral=True)


class VoiceChannelPickerView(View):
    def __init__(self, guild: discord.Guild, user_id: int, on_select: VoiceChannelCallback, *, title: str = "Voice-Kanal auswählen", channels: list[discord.VoiceChannel] | None = None, page: int = 0):
        super().__init__(timeout=180)
        self.guild = guild
        self.user_id = int(user_id)
        self.on_select = on_select
        self.title = title
        self.channels = sorted(_dedupe_channels(channels or _cached_voice_channels(guild)), key=_sort_key)  # type: ignore[arg-type]
        self.page = max(0, int(page))
        self.per_page = 25
        self.max_page = max(0, (len(self.channels) - 1) // self.per_page)
        if self.page > self.max_page:
            self.page = self.max_page
        self._rebuild()

    def page_items(self) -> list[discord.VoiceChannel]:
        start = self.page * self.per_page
        return self.channels[start:start + self.per_page]

    def _rebuild(self) -> None:
        self.clear_items()
        self.add_item(_VoiceChannelSelect(self))
        if self.max_page > 0:
            prev_btn = discord.ui.Button(label="◀️ Zurück", style=ButtonStyle.secondary, disabled=self.page <= 0)
            next_btn = discord.ui.Button(label="Weiter ▶️", style=ButtonStyle.secondary, disabled=self.page >= self.max_page)

            async def _prev(inter: discord.Interaction):
                await self._turn_page(inter, -1)

            async def _next(inter: discord.Interaction):
                await self._turn_page(inter, 1)

            prev_btn.callback = _prev  # type: ignore[method-assign]
            next_btn.callback = _next  # type: ignore[method-assign]
            self.add_item(prev_btn)
            self.add_item(next_btn)

    async def _turn_page(self, inter: discord.Interaction, direction: int):
        if int(inter.user.id) != int(self.user_id):
            await inter.response.send_message("❌ Diese Auswahl gehört nicht dir.", ephemeral=True)
            return
        self.page = max(0, min(self.max_page, self.page + direction))
        self._rebuild()
        await inter.response.edit_message(content=self.message_text(), view=self)

    def message_text(self) -> str:
        total = len(self.channels)
        if self.max_page > 0:
            return f"{self.title}\nSeite **{self.page + 1}/{self.max_page + 1}** · **{total}** Voice-Kanäle gefunden. Mit **Weiter ▶️** durchblättern."
        return f"{self.title}\n**{total}** Voice-Kanäle gefunden."


async def send_text_channel_picker(inter: discord.Interaction, title: str, on_select: TextChannelCallback, *, ephemeral: bool = True) -> None:
    if inter.guild is None:
        if inter.response.is_done():
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
        else:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    # Discord erwartet innerhalb weniger Sekunden eine Bestätigung. fetch_channels()
    # kann bei größeren Servern oder langsamer REST-Antwort länger dauern.
    deferred_here = False
    if not inter.response.is_done():
        await inter.response.defer(ephemeral=ephemeral, thinking=True)
        deferred_here = True

    channels = await _fetch_text_channels(inter.guild)
    view = TextChannelPickerView(inter.guild, int(inter.user.id), on_select, title=title, channels=channels)
    if deferred_here:
        await inter.edit_original_response(content=view.message_text(), view=view)
    else:
        await inter.followup.send(view.message_text(), view=view, ephemeral=ephemeral)


async def send_voice_channel_picker(inter: discord.Interaction, title: str, on_select: VoiceChannelCallback, *, ephemeral: bool = True) -> None:
    if inter.guild is None:
        if inter.response.is_done():
            await inter.followup.send("❌ Nur im Server nutzbar.", ephemeral=True)
        else:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return

    deferred_here = False
    if not inter.response.is_done():
        await inter.response.defer(ephemeral=ephemeral, thinking=True)
        deferred_here = True

    channels = await _fetch_voice_channels(inter.guild)
    view = VoiceChannelPickerView(inter.guild, int(inter.user.id), on_select, title=title, channels=channels)
    if deferred_here:
        await inter.edit_original_response(content=view.message_text(), view=view)
    else:
        await inter.followup.send(view.message_text(), view=view, ephemeral=ephemeral)
