from __future__ import annotations

import json
import asyncio
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timedelta, date

import discord
from discord import app_commands

try:
    from bot.event_dm_prefs import is_dm_enabled  # type: ignore
except ModuleNotFoundError:
    from event_dm_prefs import is_dm_enabled  # type: ignore

try:
    from bot.event_rsvp_dm import (
        store,
        save_store,
        TZ,
        build_embed,
        ServerRaidView,
        RaidView,
        _eligible_members,
        _format_dm_text,
    )  # type: ignore
except ModuleNotFoundError:
    from event_rsvp_dm import (
        store,
        save_store,
        TZ,
        build_embed,
        ServerRaidView,
        RaidView,
        _eligible_members,
        _format_dm_text,
    )  # type: ignore


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TEMPLATE_FILE = DATA_DIR / "raid_templates.json"


def _load() -> dict:
    try:
        return json.loads(TEMPLATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(obj: dict) -> None:
    TEMPLATE_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


templates: dict = _load()


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _gcfg(guild_id: int) -> dict:
    g = templates.get(str(guild_id)) or {}
    g.setdefault("templates", {})
    templates[str(guild_id)] = g
    return g


def _normalize_name(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "_")


def _parse_weekday(value: str) -> int:
    v = (value or "").strip().lower()

    mapping = {
        "mo": 0,
        "montag": 0,
        "monday": 0,
        "di": 1,
        "dienstag": 1,
        "tuesday": 1,
        "mi": 2,
        "mittwoch": 2,
        "wednesday": 2,
        "do": 3,
        "donnerstag": 3,
        "thursday": 3,
        "fr": 4,
        "freitag": 4,
        "friday": 4,
        "sa": 5,
        "samstag": 5,
        "saturday": 5,
        "so": 6,
        "sonntag": 6,
        "sunday": 6,
    }

    if v in mapping:
        return mapping[v]

    try:
        num = int(v)
        if 0 <= num <= 6:
            return num
    except Exception:
        pass

    raise ValueError("Wochentag ungültig. Nutze z.B. Montag, Dienstag, Donnerstag, Freitag oder 0-6.")


def _next_date_for_weekday(target_weekday: int) -> date:
    today = datetime.now(TZ).date()
    days_ahead = (target_weekday - today.weekday()) % 7

    if days_ahead == 0:
        days_ahead = 7

    return today + timedelta(days=days_ahead)


def _parse_time_hhmm(value: str) -> tuple[int, int]:
    try:
        hh, mm = [int(x) for x in value.strip().split(":")]
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError
        return hh, mm
    except Exception:
        raise ValueError("Zeit ungültig. Nutze HH:MM, z.B. 21:30.")


def _parse_date(value: Optional[str], weekday: int) -> date:
    if value and value.strip():
        try:
            y, m, d = [int(x) for x in value.strip().split("-")]
            return date(y, m, d)
        except Exception:
            raise ValueError("Datum ungültig. Nutze YYYY-MM-DD.")

    return _next_date_for_weekday(weekday)


def _parse_reminders(value: str) -> List[int]:
    if not value or not value.strip():
        return []

    out: List[int] = []

    for part in value.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue

    return out


async def _resolve_media_url(client: discord.Client, guild: discord.Guild, tpl: dict) -> Optional[str]:
    image_url = (tpl.get("image_url") or "").strip()

    if image_url:
        return image_url

    media_channel_id = int(tpl.get("media_channel_id", 0) or 0)
    media_message_id = int(tpl.get("media_message_id", 0) or 0)
    attachment_index = int(tpl.get("attachment_index", 0) or 0)

    if not media_channel_id or not media_message_id:
        return None

    ch = guild.get_channel(media_channel_id)

    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return None

    try:
        msg = await ch.fetch_message(media_message_id)

        if not msg.attachments:
            return None

        if attachment_index < 0 or attachment_index >= len(msg.attachments):
            attachment_index = 0

        return msg.attachments[attachment_index].url

    except Exception:
        return None


async def _create_event_from_template(
    client: discord.Client,
    guild: discord.Guild,
    tpl: dict,
    event_date: date,
    override_channel: Optional[discord.TextChannel] = None,
) -> tuple[Optional[discord.Message], int, int]:
    hh, mi = _parse_time_hhmm(str(tpl.get("time", "21:30")))
    when = datetime(event_date.year, event_date.month, event_date.day, hh, mi, tzinfo=TZ)

    channel_id = int(tpl.get("channel_id", 0) or 0)
    target_role_id = int(tpl.get("target_role_id", 0) or 0)

    ch = override_channel or guild.get_channel(channel_id)

    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        raise RuntimeError("Zielchannel nicht gefunden oder ungültig.")

    image_url = await _resolve_media_url(client, guild, tpl)

    obj = {
        "guild_id": int(guild.id),
        "channel_id": int(ch.id),
        "title": str(tpl.get("title", "Event")).strip(),
        "description": str(tpl.get("description", "") or "").strip(),
        "when_iso": when.isoformat(),
        "image_url": image_url,
        "yes": {"TANK": [], "HEAL": [], "DPS": [], "BANK": []},
        "maybe": {},
        "no": [],
        "target_role_id": target_role_id,
        "dm_messages": {},
    }

    emb = build_embed(guild, obj)
    msg = await ch.send(embed=emb)

    store[str(msg.id)] = obj
    save_store()

    try:
        await msg.edit(view=ServerRaidView(int(msg.id)))
    except Exception:
        pass

    send_dm = bool(tpl.get("send_dm", True))

    sent = 0
    skipped_opt_out = 0

    if send_dm:
        for m in _eligible_members(guild, obj):
            if not is_dm_enabled(guild.id, m.id):
                skipped_opt_out += 1
                continue

            try:
                dm_text = _format_dm_text(
                    title=obj["title"],
                    when=when,
                    channel_name_or_ref=f"Übersicht im Server: #{ch.name}",
                    description=obj.get("description"),
                    intro_line="Wähle unten deine Teilnahme:",
                )

                dm_msg = await m.send(dm_text, view=RaidView(int(msg.id)))
                obj["dm_messages"][str(m.id)] = int(dm_msg.id)
                sent += 1
                await asyncio.sleep(0.05)

            except Exception:
                pass

        save_store()

    return msg, sent, skipped_opt_out


async def setup_raid_templates(client: discord.Client, tree: app_commands.CommandTree):
    @tree.command(name="raid_template_create", description="(Admin) Erstellt eine Raid-/Event-Vorlage")
    @app_commands.describe(
        name="Interner Vorlagenname, z.B. gildenraid",
        title="Titel des Events",
        weekday="Wochentag, z.B. Donnerstag/Freitag/Samstag oder 0-6",
        time="Uhrzeit HH:MM",
        channel="Channel für den Serverpost",
        target_role="Optionale Zielrolle für DMs und Abstimmquote",
        description="Beschreibung",
        duration_min="Dauer in Minuten",
        image_url="Direkte Bild-URL optional",
        send_dm="Soll der Bot DMs verschicken?"
    )
    async def raid_template_create(
        inter: discord.Interaction,
        name: str,
        title: str,
        weekday: str,
        time: str,
        channel: discord.TextChannel,
        target_role: Optional[discord.Role] = None,
        description: Optional[str] = None,
        duration_min: int = 120,
        image_url: Optional[str] = None,
        send_dm: bool = True,
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        try:
            wd = _parse_weekday(weekday)
            _parse_time_hhmm(time)
        except Exception as e:
            await inter.followup.send(f"❌ {e}", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)

        g["templates"][key] = {
            "name": key,
            "title": title.strip(),
            "description": (description or "").strip(),
            "weekday": wd,
            "time": time.strip(),
            "duration_min": int(duration_min),
            "channel_id": int(channel.id),
            "target_role_id": int(target_role.id) if target_role else 0,
            "image_url": (image_url or "").strip(),
            "media_channel_id": 0,
            "media_message_id": 0,
            "attachment_index": 0,
            "pre_reminders": [],
            "send_dm": bool(send_dm),
            "post_server": True,
        }

        templates[str(inter.guild_id)] = g
        _save(templates)

        await inter.followup.send(
            f"✅ Vorlage `{key}` erstellt.\n"
            f"📅 Wochentag: `{wd}`\n"
            f"🕒 Zeit: `{time}`\n"
            f"📢 Channel: {channel.mention}\n"
            f"🎯 Zielrolle: {target_role.mention if target_role else '—'}",
            ephemeral=True
        )

    @tree.command(name="raid_template_set_media", description="(Admin) Speichert Mediennachricht/Bild für eine Vorlage")
    async def raid_template_set_media(
        inter: discord.Interaction,
        name: str,
        message_id: str,
        channel: Optional[discord.TextChannel] = None,
        attachment_index: int = 0,
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        ch = channel or inter.channel

        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await inter.followup.send("❌ Channel ungültig.", ephemeral=True)
            return

        try:
            msg = await ch.fetch_message(int(message_id))
        except Exception:
            await inter.followup.send("❌ Nachricht nicht gefunden.", ephemeral=True)
            return

        if not msg.attachments:
            await inter.followup.send("❌ Diese Nachricht hat kein Attachment/Bild.", ephemeral=True)
            return

        if attachment_index < 0 or attachment_index >= len(msg.attachments):
            attachment_index = 0

        tpl["media_channel_id"] = int(ch.id)
        tpl["media_message_id"] = int(msg.id)
        tpl["attachment_index"] = int(attachment_index)
        tpl["image_url"] = ""

        _save(templates)

        await inter.followup.send(
            f"✅ Medium für `{key}` gespeichert.\n"
            f"Channel: <#{ch.id}>\n"
            f"Message-ID: `{msg.id}`\n"
            f"Attachment: `{attachment_index}`",
            ephemeral=True
        )

    @tree.command(name="raid_template_set_description", description="(Admin) Ändert Beschreibung einer Vorlage")
    async def raid_template_set_description(inter: discord.Interaction, name: str, description: str):
        await inter.response.defer(ephemeral=True, thinking=False)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        tpl["description"] = description.strip()
        _save(templates)

        await inter.followup.send(f"✅ Beschreibung für `{key}` geändert.", ephemeral=True)

    @tree.command(name="raid_template_set_target", description="(Admin) Setzt Zielrolle einer Vorlage")
    async def raid_template_set_target(inter: discord.Interaction, name: str, target_role: Optional[discord.Role] = None):
        await inter.response.defer(ephemeral=True, thinking=False)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        tpl["target_role_id"] = int(target_role.id) if target_role else 0
        _save(templates)

        await inter.followup.send(
            f"✅ Zielrolle für `{key}` gesetzt: {target_role.mention if target_role else '—'}",
            ephemeral=True
        )

    @tree.command(name="raid_template_set_channel", description="(Admin) Setzt Zielchannel einer Vorlage")
    async def raid_template_set_channel(inter: discord.Interaction, name: str, channel: discord.TextChannel):
        await inter.response.defer(ephemeral=True, thinking=False)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        tpl["channel_id"] = int(channel.id)
        _save(templates)

        await inter.followup.send(f"✅ Channel für `{key}` gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="raid_template_set_reminders", description="(Admin) Setzt Reminder-Minuten, z.B. 1440,180,60")
    async def raid_template_set_reminders(inter: discord.Interaction, name: str, minutes: str):
        await inter.response.defer(ephemeral=True, thinking=False)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        tpl["pre_reminders"] = _parse_reminders(minutes)
        _save(templates)

        await inter.followup.send(
            f"✅ Reminder für `{key}` gesetzt: `{tpl['pre_reminders']}` Minuten vorher.",
            ephemeral=True
        )

    @tree.command(name="raid_template_run", description="(Admin) Erstellt ein Event aus Vorlage")
    @app_commands.describe(
        name="Vorlagenname",
        date_override="Optional: YYYY-MM-DD",
        channel_override="Optional anderer Zielchannel"
    )
    async def raid_template_run(
        inter: discord.Interaction,
        name: str,
        date_override: Optional[str] = None,
        channel_override: Optional[discord.TextChannel] = None,
    ):
        await inter.response.defer(ephemeral=True, thinking=True)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        try:
            event_date = _parse_date(date_override, int(tpl.get("weekday", 4)))
            msg, sent, skipped = await _create_event_from_template(
                client=client,
                guild=inter.guild,
                tpl=tpl,
                event_date=event_date,
                override_channel=channel_override,
            )
        except Exception as e:
            await inter.followup.send(f"❌ Event konnte nicht erstellt werden: {e}", ephemeral=True)
            return

        await inter.followup.send(
            f"✅ Event aus Vorlage `{key}` erstellt.\n"
            f"📅 Datum: `{event_date.strftime('%d.%m.%Y')}`\n"
            f"🔗 {msg.jump_url if msg else 'Kein Link'}\n"
            f"✉️ DMs versendet: {sent}\n"
            f"🔕 Opt-out übersprungen: {skipped}",
            ephemeral=True
        )

    @tree.command(name="raid_template_list", description="Zeigt alle Raid-/Event-Vorlagen")
    async def raid_template_list(inter: discord.Interaction):
        g = _gcfg(inter.guild_id)
        all_tpl = g.get("templates") or {}

        if not all_tpl:
            await inter.response.send_message("📋 Keine Vorlagen vorhanden.", ephemeral=True)
            return

        lines = []

        for key, tpl in all_tpl.items():
            ch_id = int(tpl.get("channel_id", 0) or 0)
            role_id = int(tpl.get("target_role_id", 0) or 0)

            lines.append(
                f"• `{key}` — **{tpl.get('title', 'Event')}** "
                f"| Wochentag `{tpl.get('weekday')}` "
                f"| `{tpl.get('time')}` "
                f"| <#{ch_id}> "
                f"| Zielrolle: {f'<@&{role_id}>' if role_id else '—'}"
            )

        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @tree.command(name="raid_template_show", description="Zeigt Details einer Vorlage")
    async def raid_template_show(inter: discord.Interaction, name: str):
        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)
        tpl = g["templates"].get(key)

        if not tpl:
            await inter.response.send_message("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        role_id = int(tpl.get("target_role_id", 0) or 0)
        ch_id = int(tpl.get("channel_id", 0) or 0)

        text = (
            f"**Vorlage `{key}`**\n"
            f"**Titel:** {tpl.get('title', '—')}\n"
            f"**Beschreibung:** {tpl.get('description', '—') or '—'}\n"
            f"**Wochentag:** `{tpl.get('weekday')}`\n"
            f"**Zeit:** `{tpl.get('time')}`\n"
            f"**Dauer:** `{tpl.get('duration_min')}` Minuten\n"
            f"**Channel:** <#{ch_id}>\n"
            f"**Zielrolle:** {f'<@&{role_id}>' if role_id else '—'}\n"
            f"**Image URL:** `{tpl.get('image_url') or '—'}`\n"
            f"**Media Channel ID:** `{tpl.get('media_channel_id', 0)}`\n"
            f"**Media Message ID:** `{tpl.get('media_message_id', 0)}`\n"
            f"**Attachment Index:** `{tpl.get('attachment_index', 0)}`\n"
            f"**Reminder:** `{tpl.get('pre_reminders', [])}`\n"
            f"**DMs:** `{'Ja' if tpl.get('send_dm', True) else 'Nein'}`"
        )

        await inter.response.send_message(text, ephemeral=True)

    @tree.command(name="raid_template_delete", description="(Admin) Löscht eine Vorlage")
    async def raid_template_delete(inter: discord.Interaction, name: str):
        await inter.response.defer(ephemeral=True, thinking=False)

        if not _is_admin(inter):
            await inter.followup.send("❌ Nur Admin/Manage Server.", ephemeral=True)
            return

        key = _normalize_name(name)
        g = _gcfg(inter.guild_id)

        if key not in g["templates"]:
            await inter.followup.send("❌ Vorlage nicht gefunden.", ephemeral=True)
            return

        g["templates"].pop(key, None)
        _save(templates)

        await inter.followup.send(f"✅ Vorlage `{key}` gelöscht.", ephemeral=True)
