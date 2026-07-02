from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.enums import ButtonStyle

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker, VoiceChannelPickerView  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker, VoiceChannelPickerView  # type: ignore
from discord.ui import View, button, Modal, TextInput

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LEADER_CONTACT_CFG_FILE = DATA_DIR / "leader_contact_cfg.json"
VOICE_TRACK_FILE = DATA_DIR / "voice_creator_channels.json"
VOICE_EMPTY_DELETE_DELAY_SECONDS = 60

VOICE_ALLOWED_ROLE_NAMES = ("Ebolus", "Allianz", "Freunde")
VOICE_BLOCKED_ROLE_NAMES = ("Raider", "Bewerber")


def _norm_role_name(name: str) -> str:
    return str(name or "").strip().casefold()


def _find_role_by_name(guild: discord.Guild, role_name: str) -> Optional[discord.Role]:
    target = _norm_role_name(role_name)
    for role in guild.roles:
        if _norm_role_name(role.name) == target:
            return role
    return None


def _voice_channel_overwrites(guild: discord.Guild) -> dict:
    """
    Voice-Panel-Regel:
    - Rollenlose sehen/beitreten nicht (@everyone denied)
    - Raider/Bewerber sehen/beitreten nicht
    - Ebolus/Allianz/Freunde sehen und treten bei, solange User-Limit nicht voll ist
    """
    overwrites: dict = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=False,
            connect=False,
        )
    }

    missing_allowed: list[str] = []
    missing_blocked: list[str] = []

    for role_name in VOICE_ALLOWED_ROLE_NAMES:
        role = _find_role_by_name(guild, role_name)
        if role is None:
            missing_allowed.append(role_name)
            continue
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            speak=True,
            use_voice_activation=True,
        )

    for role_name in VOICE_BLOCKED_ROLE_NAMES:
        role = _find_role_by_name(guild, role_name)
        if role is None:
            missing_blocked.append(role_name)
            continue
        overwrites[role] = discord.PermissionOverwrite(
            view_channel=False,
            connect=False,
            speak=False,
        )

    if missing_allowed or missing_blocked:
        print(
            f"[VOICE-PANEL] role_config_warning guild={guild.id} "
            f"missing_allowed={missing_allowed} missing_blocked={missing_blocked}",
            flush=True,
        )

    return overwrites


def _load_json(path: Path, default):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        print(f"[voice_creator] JSON speichern fehlgeschlagen: {path} {e!r}", flush=True)


def _load_tracked_voice_channels() -> dict:
    data = _load_json(VOICE_TRACK_FILE, {})
    return data if isinstance(data, dict) else {}


def _save_tracked_voice_channels(data: dict) -> None:
    _save_json(VOICE_TRACK_FILE, data)


def _track_voice_channel(voice: discord.VoiceChannel, *, source_channel_id: int, creator_id: int, user_limit: int) -> None:
    data = _load_tracked_voice_channels()
    data[str(voice.id)] = {
        "guild_id": int(voice.guild.id),
        "channel_id": int(voice.id),
        "source_channel_id": int(source_channel_id),
        "creator_id": int(creator_id),
        "user_limit": int(user_limit),
        "name": str(voice.name),
    }
    _save_tracked_voice_channels(data)


def _untrack_voice_channel(channel_id: int) -> None:
    data = _load_tracked_voice_channels()
    if str(channel_id) in data:
        data.pop(str(channel_id), None)
        _save_tracked_voice_channels(data)


def _is_tracked_voice_channel(channel_id: Optional[int]) -> bool:
    if not channel_id:
        return False
    data = _load_tracked_voice_channels()
    return str(channel_id) in data


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True
    if inter.guild is None or not isinstance(inter.user, discord.Member):
        return False
    cfg = _load_json(LEADER_CONTACT_CFG_FILE, {})
    c = cfg.get(str(inter.guild.id)) or {}
    role_id = int(c.get("leader_role_id", 0) or 0)
    role = inter.guild.get_role(role_id) if role_id else None
    return bool(role and role in inter.user.roles)


def _clean_channel_name(raw: str) -> str:
    value = str(raw or "").strip().lower()
    value = value.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    value = re.sub(r"[^a-z0-9 _\-]", "", value)
    value = re.sub(r"[\s_]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value[:80] or "sprachkanal"


async def _delete_tracked_voice_if_empty(channel: discord.abc.GuildChannel | None) -> None:
    if not isinstance(channel, discord.VoiceChannel):
        return
    if not _is_tracked_voice_channel(int(channel.id)):
        return

    # Direkt vor dem Löschen erneut prüfen, damit niemand gelöscht wird, der wieder betreten wurde.
    if len(getattr(channel, "members", []) or []) > 0:
        return

    channel_id = int(channel.id)
    guild_id = int(channel.guild.id)
    channel_name = str(channel.name)
    try:
        await channel.delete(reason="Ebolus Voice-Panel: automatisch gelöscht, weil leer")
        _untrack_voice_channel(channel_id)
        print(
            f"[VOICE-PANEL] auto_delete guild={guild_id} channel={channel_id} name={channel_name} reason=empty",
            flush=True,
        )
    except discord.NotFound:
        _untrack_voice_channel(channel_id)
    except discord.Forbidden:
        print(
            f"[VOICE-PANEL] auto_delete_failed guild={guild_id} channel={channel_id} name={channel_name} error=Forbidden",
            flush=True,
        )
    except Exception as e:
        print(
            f"[VOICE-PANEL] auto_delete_failed guild={guild_id} channel={channel_id} name={channel_name} error={e!r}",
            flush=True,
        )


class VoiceCreateModal(Modal, title="Sprachkanal erstellen"):
    def __init__(self, source_channel_id: int):
        super().__init__(timeout=300)
        self.source_channel_id = int(source_channel_id)

        self.channel_name = TextInput(
            label="Name des Sprachkanals",
            placeholder="z. B. Dungeon Gruppe 1",
            min_length=2,
            max_length=80,
            required=True,
        )
        self.user_limit = TextInput(
            label="Personenanzahl",
            placeholder="z. B. 5 oder 6",
            min_length=1,
            max_length=2,
            required=True,
        )
        self.add_item(self.channel_name)
        self.add_item(self.user_limit)

    async def on_submit(self, inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return

        source = inter.guild.get_channel(self.source_channel_id)
        if not isinstance(source, discord.TextChannel):
            source = inter.channel if isinstance(inter.channel, discord.TextChannel) else None
        if not isinstance(source, discord.TextChannel):
            await inter.response.send_message("❌ Textkanal konnte nicht erkannt werden.", ephemeral=True)
            return

        try:
            limit = int(str(self.user_limit.value).strip())
        except Exception:
            await inter.response.send_message("❌ Personenanzahl muss eine Zahl sein.", ephemeral=True)
            return
        if limit < 1 or limit > 99:
            await inter.response.send_message("❌ Personenanzahl muss zwischen 1 und 99 liegen.", ephemeral=True)
            return

        name = _clean_channel_name(str(self.channel_name.value))
        try:
            overwrites = _voice_channel_overwrites(inter.guild)
            voice = await inter.guild.create_voice_channel(
                name=name,
                category=source.category,
                user_limit=limit,
                overwrites=overwrites,
                reason=f"Voice-Panel genutzt von {inter.user} ({inter.user.id})",
            )
            _track_voice_channel(voice, source_channel_id=int(source.id), creator_id=int(inter.user.id), user_limit=limit)

            # Möglichst direkt unter den Textkanal einsortieren.
            try:
                await voice.edit(position=int(source.position) + 1)
            except Exception:
                pass
            print(
                f"[VOICE-PANEL] guild={inter.guild.id} creator={inter.user.id} channel={voice.id} name={voice.name} limit={limit} source_text={source.id}",
                flush=True,
            )
            await inter.response.send_message(
                f"✅ Sprachkanal erstellt: {voice.mention} • Limit: **{limit}**\n"
                f"ℹ️ Der Kanal wird automatisch gelöscht, sobald der letzte Spieler raus ist.",
                ephemeral=True,
            )
        except discord.Forbidden:
            await inter.response.send_message("❌ Mir fehlen Rechte zum Erstellen oder Verschieben von Sprachkanälen.", ephemeral=True)
        except Exception as e:
            print(f"[voice_creator] Fehler beim Erstellen: {e!r}", flush=True)
            await inter.response.send_message(f"❌ Sprachkanal konnte nicht erstellt werden: `{type(e).__name__}`", ephemeral=True)


class VoiceCreatePanel(View):
    def __init__(self):
        super().__init__(timeout=None)

    @button(label="Sprachkanal erstellen", style=ButtonStyle.primary, custom_id="ebolus_voice_create_open")
    async def open_modal(self, inter: discord.Interaction, btn: discord.ui.Button):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not isinstance(inter.channel, discord.TextChannel):
            await inter.response.send_message("❌ Bitte nutze den Button in einem Textkanal.", ephemeral=True)
            return
        await inter.response.send_modal(VoiceCreateModal(int(inter.channel.id)))


async def setup_voice_creator(client: discord.Client, tree: app_commands.CommandTree):
    client.add_view(VoiceCreatePanel())

    if not getattr(client, "_ebolus_voice_autodelete_listener", False):
        async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
            before_channel = getattr(before, "channel", None)
            after_channel = getattr(after, "channel", None)

            # Nur reagieren, wenn jemand einen getrackten Bot-Voice-Channel wirklich verlässt/wechseln.
            if not isinstance(before_channel, discord.VoiceChannel):
                return
            if isinstance(after_channel, discord.VoiceChannel) and int(after_channel.id) == int(before_channel.id):
                return
            if not _is_tracked_voice_channel(int(before_channel.id)):
                return

            await asyncio.sleep(VOICE_EMPTY_DELETE_DELAY_SECONDS)

            fresh_channel = before_channel.guild.get_channel(int(before_channel.id))
            if fresh_channel is None:
                _untrack_voice_channel(int(before_channel.id))
                return
            await _delete_tracked_voice_if_empty(fresh_channel)

        client.add_listener(on_voice_state_update, "on_voice_state_update")
        setattr(client, "_ebolus_voice_autodelete_listener", True)

    voice_panel = app_commands.Group(name="voice_panel", description="Voice-Panel verwalten")

    @voice_panel.command(name="post", description="Leader: Panel zum Erstellen von Sprachkanälen posten")
    async def post(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, kanal: discord.TextChannel):
            emb = discord.Embed(
                title="🔊 Sprachkanal erstellen",
                description=(
                    "Klicke auf den Button, gib einen Namen und eine Personenanzahl ein.\n"
                    "Der Sprachkanal wird in derselben Kategorie wie dieser Textkanal erstellt.\n"
                    "Sichtbar/beitretbar ist er für Ebolus, Allianz und Freunde.\n"
                    "Rollenlose, Raider und Bewerber werden ausgeschlossen.\n\n"
                    "Leere erstellte Sprachkanäle werden automatisch gelöscht, sobald der letzte Spieler raus ist."
                ),
                color=discord.Color.blurple(),
            )
            emb.set_footer(text="Ebolus Voice-Panel")
            try:
                await kanal.send(embed=emb, view=VoiceCreatePanel())
                await pick_inter.response.edit_message(content=f"✅ Voice-Panel wurde in {kanal.mention} gepostet.", view=None)
            except discord.Forbidden:
                await pick_inter.response.edit_message(content="❌ Mir fehlen Rechte, um dort zu schreiben.", view=None)
            except Exception as e:
                print(f"[voice_creator] Panel-Post Fehler: {e!r}", flush=True)
                await pick_inter.response.edit_message(content=f"❌ Panel konnte nicht gepostet werden: `{type(e).__name__}`", view=None)

        await send_text_channel_picker(inter, "🔊 Textkanal fürs Voice-Panel auswählen", _picked)

    async def _move_members(inter: discord.Interaction, source: discord.VoiceChannel, target: discord.VoiceChannel) -> None:
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        me = inter.guild.me
        if me is not None and not inter.guild.me.guild_permissions.move_members:
            await inter.response.send_message("❌ Mir fehlt die Berechtigung **Mitglieder verschieben**.", ephemeral=True)
            return
        members = list(getattr(source, "members", []) or [])
        if not members:
            await inter.response.send_message(f"ℹ️ In {source.mention} ist niemand.", ephemeral=True)
            return
        await inter.response.edit_message(content=f"⏳ Verschiebe **{len(members)}** Nutzer von {source.mention} nach {target.mention} …", view=None)
        moved = 0
        failed = 0
        for member in members:
            try:
                await member.move_to(target, reason=f"Voice-Move durch {inter.user}")
                moved += 1
                await asyncio.sleep(0.15)
            except Exception as e:
                print(f"[voice_creator] move failed {getattr(member, 'id', '?')}: {e!r}", flush=True)
                failed += 1
        await inter.followup.send(
            f"✅ Voice-Move fertig.\nVon: {source.mention}\nNach: {target.mention}\nVerschoben: **{moved}**\nFehler: **{failed}**",
            ephemeral=True,
        )

    @voice_panel.command(name="move_all", description="Leader: Alle Nutzer aus einem Voice in einen anderen Voice verschieben")
    async def move_all(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        async def _source_picked(source_inter: discord.Interaction, source: discord.VoiceChannel):
            async def _target_picked(target_inter: discord.Interaction, target: discord.VoiceChannel):
                if int(source.id) == int(target.id):
                    await target_inter.response.edit_message(content="❌ Quelle und Ziel sind derselbe Voice-Kanal.", view=None)
                    return
                await _move_members(target_inter, source, target)

            view_msg = f"Quelle gewählt: {source.mention}\nJetzt Ziel-Voice auswählen."
            target_view = VoiceChannelPickerView(source_inter.guild, int(source_inter.user.id), _target_picked, title="🔊 Ziel-Voice auswählen")
            await source_inter.response.edit_message(content=target_view.message_text() + "\n" + view_msg, view=target_view)

        await send_voice_channel_picker(inter, "🔊 Quell-Voice auswählen", _source_picked)

    @voice_panel.command(name="move_my_voice", description="Leader: Alle aus deinem aktuellen Voice in einen Ziel-Voice verschieben")
    async def move_my_voice(inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        member = inter.user if isinstance(inter.user, discord.Member) else None
        source = getattr(getattr(member, "voice", None), "channel", None)
        if not isinstance(source, discord.VoiceChannel):
            await inter.response.send_message("❌ Du bist aktuell in keinem Voice-Kanal.", ephemeral=True)
            return

        async def _target_picked(target_inter: discord.Interaction, target: discord.VoiceChannel):
            if int(source.id) == int(target.id):
                await target_inter.response.edit_message(content="❌ Quelle und Ziel sind derselbe Voice-Kanal.", view=None)
                return
            await _move_members(target_inter, source, target)

        await send_voice_channel_picker(inter, f"🔊 Ziel-Voice auswählen für Quelle {source.name}", _target_picked)

    try:
        tree.add_command(voice_panel)
    except Exception:
        pass

    print("🔊 Voice-Creator geladen: /voice_panel post + Auto-Löschung + Rollenrechte")
