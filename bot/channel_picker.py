from __future__ import annotations

from typing import Awaitable, Callable

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


def _text_channels(guild: discord.Guild) -> list[discord.TextChannel]:
    return sorted(list(getattr(guild, "text_channels", []) or []), key=lambda c: (
        getattr(getattr(c, "category", None), "position", 9999) if getattr(c, "category", None) else 9999,
        getattr(c, "position", 9999),
        str(c.name).lower(),
    ))


def _voice_channels(guild: discord.Guild) -> list[discord.VoiceChannel]:
    return sorted(list(getattr(guild, "voice_channels", []) or []), key=lambda c: (
        getattr(getattr(c, "category", None), "position", 9999) if getattr(c, "category", None) else 9999,
        getattr(c, "position", 9999),
        str(c.name).lower(),
    ))


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
        if not isinstance(channel, discord.TextChannel):
            await inter.response.send_message("❌ Kanal nicht gefunden oder kein Textkanal.", ephemeral=True)
            return
        await parent.on_select(inter, channel)


class TextChannelPickerView(View):
    def __init__(self, guild: discord.Guild, user_id: int, on_select: TextChannelCallback, *, title: str = "Textkanal auswählen", page: int = 0):
        super().__init__(timeout=180)
        self.guild = guild
        self.user_id = int(user_id)
        self.on_select = on_select
        self.title = title
        self.channels = _text_channels(guild)
        self.page = max(0, int(page))
        self.per_page = 25
        self.max_page = max(0, (len(self.channels) - 1) // self.per_page)
        self._rebuild()

    def page_items(self) -> list[discord.TextChannel]:
        start = self.page * self.per_page
        return self.channels[start:start + self.per_page]

    def _rebuild(self) -> None:
        self.clear_items()
        self.add_item(_TextChannelSelect(self))
        if self.max_page > 0:
            prev_btn = discord.ui.Button(label="◀️", style=ButtonStyle.secondary, disabled=self.page <= 0)
            next_btn = discord.ui.Button(label="▶️", style=ButtonStyle.secondary, disabled=self.page >= self.max_page)

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
        if self.max_page > 0:
            return f"{self.title}\nSeite **{self.page + 1}/{self.max_page + 1}** – alle Textkanäle werden vom Bot selbst gelistet."
        return f"{self.title}\nAlle Textkanäle werden vom Bot selbst gelistet."


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
        if not isinstance(channel, discord.VoiceChannel):
            await inter.response.send_message("❌ Kanal nicht gefunden oder kein Voice-Kanal.", ephemeral=True)
            return
        await parent.on_select(inter, channel)


class VoiceChannelPickerView(View):
    def __init__(self, guild: discord.Guild, user_id: int, on_select: VoiceChannelCallback, *, title: str = "Voice-Kanal auswählen", page: int = 0):
        super().__init__(timeout=180)
        self.guild = guild
        self.user_id = int(user_id)
        self.on_select = on_select
        self.title = title
        self.channels = _voice_channels(guild)
        self.page = max(0, int(page))
        self.per_page = 25
        self.max_page = max(0, (len(self.channels) - 1) // self.per_page)
        self._rebuild()

    def page_items(self) -> list[discord.VoiceChannel]:
        start = self.page * self.per_page
        return self.channels[start:start + self.per_page]

    def _rebuild(self) -> None:
        self.clear_items()
        self.add_item(_VoiceChannelSelect(self))
        if self.max_page > 0:
            prev_btn = discord.ui.Button(label="◀️", style=ButtonStyle.secondary, disabled=self.page <= 0)
            next_btn = discord.ui.Button(label="▶️", style=ButtonStyle.secondary, disabled=self.page >= self.max_page)

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
        if self.max_page > 0:
            return f"{self.title}\nSeite **{self.page + 1}/{self.max_page + 1}** – alle Voice-Kanäle werden vom Bot selbst gelistet."
        return f"{self.title}\nAlle Voice-Kanäle werden vom Bot selbst gelistet."


async def send_text_channel_picker(inter: discord.Interaction, title: str, on_select: TextChannelCallback, *, ephemeral: bool = True) -> None:
    if inter.guild is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return
    view = TextChannelPickerView(inter.guild, int(inter.user.id), on_select, title=title)
    await inter.response.send_message(view.message_text(), view=view, ephemeral=ephemeral)


async def send_voice_channel_picker(inter: discord.Interaction, title: str, on_select: VoiceChannelCallback, *, ephemeral: bool = True) -> None:
    if inter.guild is None:
        await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
        return
    view = VoiceChannelPickerView(inter.guild, int(inter.user.id), on_select, title=title)
    await inter.response.send_message(view.message_text(), view=view, ephemeral=ephemeral)
