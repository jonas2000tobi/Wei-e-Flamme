from __future__ import annotations
import json
import os
import threading
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands

try:
    from bot.module_registry import FeatureGroup, is_module_enabled, any_guild_has_module  # type: ignore
except Exception:
    from module_registry import FeatureGroup, is_module_enabled, any_guild_has_module  # type: ignore
from discord.ui import View, button, Modal, TextInput
from discord.enums import ButtonStyle

try:
    from bot.channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
except Exception:
    from channel_picker import send_text_channel_picker, send_voice_channel_picker  # type: ignore
from zoneinfo import ZoneInfo

try:
    from bot import runtime_db  # type: ignore
except Exception:
    import runtime_db  # type: ignore

try:
    from bot import guild_config as central_guild_config  # type: ignore
except Exception:
    try:
        import guild_config as central_guild_config  # type: ignore
    except Exception:
        central_guild_config = None  # type: ignore

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CFG_FILE = DATA_DIR / "leader_contact_cfg.json"
_JSON_LOCK = threading.RLock()


def _load_cfg() -> dict:
    try:
        return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cfg(obj: dict) -> None:
    tmp = CFG_FILE.with_name(f".{CFG_FILE.name}.{os.getpid()}.tmp")
    payload = json.dumps(obj, indent=2, ensure_ascii=False)
    with _JSON_LOCK:
        try:
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, CFG_FILE)
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


cfg: dict = _load_cfg()


def _gcfg(guild_id: int) -> dict:
    c = cfg.get(str(guild_id)) or {}
    c.setdefault("public_channel_id", 0)
    c.setdefault("internal_channel_id", 0)
    c.setdefault("archive_channel_id", 0)
    c.setdefault("ticket_category_id", 0)
    c.setdefault("leader_role_id", 0)
    c.setdefault("contact_post_channel_id", 0)
    c.setdefault("contact_post_message_id", 0)
    try:
        if central_guild_config is not None:
            ids = central_guild_config.role_ids(int(guild_id), "leader")
            if central_guild_config.role_mapping_configured(int(guild_id), "leader"):
                c["leader_role_id"] = ids[0] if ids else 0
            if central_guild_config.channel_mapping_configured(int(guild_id), "leader_contact_public"):
                c["public_channel_id"] = central_guild_config.channel_id(int(guild_id), "leader_contact_public")
            if central_guild_config.channel_mapping_configured(int(guild_id), "leader_contact_internal"):
                c["internal_channel_id"] = central_guild_config.channel_id(int(guild_id), "leader_contact_internal")
            if central_guild_config.channel_mapping_configured(int(guild_id), "leader_contact_archive"):
                c["archive_channel_id"] = central_guild_config.channel_id(int(guild_id), "leader_contact_archive")
            if central_guild_config.channel_mapping_configured(int(guild_id), "leader_ticket_category"):
                c["ticket_category_id"] = central_guild_config.channel_id(int(guild_id), "leader_ticket_category")
    except Exception:
        pass
    cfg[str(guild_id)] = c
    return c


def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))


def _safe_text(s: str) -> str:
    return (s or "").replace("@", "@\u200b").strip()


def _leader_role(guild: discord.Guild) -> Optional[discord.Role]:
    rid = int((_gcfg(guild.id).get("leader_role_id") or 0))
    return guild.get_role(rid) if rid else None


def _internal_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id = int((_gcfg(guild.id).get("internal_channel_id") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


def _archive_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id = int((_gcfg(guild.id).get("archive_channel_id") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


def _ticket_category(guild: discord.Guild) -> Optional[discord.CategoryChannel]:
    ch_id = int((_gcfg(guild.id).get("ticket_category_id") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, discord.CategoryChannel) else None


def _public_channel(guild: discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id = int((_gcfg(guild.id).get("public_channel_id") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


def _is_leader_or_admin(inter: discord.Interaction) -> bool:
    if _is_admin(inter):
        return True
    if inter.guild is None:
        return False
    role = _leader_role(inter.guild)
    if not role or not isinstance(inter.user, discord.Member):
        return False
    return int(role.id) in {int(r.id) for r in inter.user.roles}


def _replace_status_field(embed: discord.Embed, text: str) -> discord.Embed:
    try:
        new_embed = discord.Embed.from_dict(embed.to_dict())
    except Exception:
        new_embed = discord.Embed(title=embed.title, description=embed.description, color=embed.color)

    kept = []
    for field in list(new_embed.fields):
        if field.name != "Status":
            kept.append((field.name, field.value, field.inline))
    new_embed.clear_fields()
    for name, value, inline in kept:
        new_embed.add_field(name=name, value=value, inline=inline)
    new_embed.add_field(name="Status", value=text, inline=False)
    return new_embed


async def _archive_ticket(inter: discord.Interaction, embed: discord.Embed, actor_name: str) -> bool:
    guild = inter.guild
    if guild is None:
        return False
    archive_ch = _archive_channel(guild)
    if archive_ch is None:
        return False

    try:
        archived = discord.Embed.from_dict(embed.to_dict())
    except Exception:
        archived = embed

    actor = getattr(inter.user, "mention", None) or f"**{_safe_text(actor_name)}**"
    source = "—"
    if inter.message is not None:
        source = f"<#{inter.message.channel.id}> · Nachricht `{inter.message.id}`"
    archived.add_field(name="Archiviert von", value=str(actor), inline=False)
    archived.add_field(name="Ursprung", value=source, inline=False)
    old_footer = str(getattr(getattr(archived, "footer", None), "text", "") or "").strip()
    stamp = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    archived.set_footer(text=(old_footer + " · " if old_footer else "") + f"Archiviert {stamp}")
    await archive_ch.send(embed=archived, allowed_mentions=discord.AllowedMentions.none())
    return True



def _embed_field_value(embed: discord.Embed, field_name: str) -> str:
    wanted = str(field_name or "").strip().casefold()
    for field in list(embed.fields):
        if str(field.name or "").strip().casefold() == wanted:
            return str(field.value or "").strip()
    return ""


def _replace_or_add_field(embed: discord.Embed, name: str, value: str, *, inline: bool = False) -> discord.Embed:
    try:
        new_embed = discord.Embed.from_dict(embed.to_dict())
    except Exception:
        new_embed = discord.Embed(title=embed.title, description=embed.description, color=embed.color)
    kept: list[tuple[str, str, bool]] = []
    wanted = str(name or "").strip().casefold()
    for field in list(new_embed.fields):
        if str(field.name or "").strip().casefold() != wanted:
            kept.append((field.name, field.value, field.inline))
    new_embed.clear_fields()
    for field_name, field_value, field_inline in kept:
        new_embed.add_field(name=field_name, value=field_value, inline=field_inline)
    new_embed.add_field(name=name, value=value, inline=inline)
    return new_embed


def _ticket_member_from_embed(guild: discord.Guild, embed: discord.Embed) -> Optional[discord.Member]:
    raw = _embed_field_value(embed, "User-ID")
    try:
        uid = int(raw)
    except Exception:
        uid = 0
    if not uid:
        return None
    member = guild.get_member(uid)
    return member if isinstance(member, discord.Member) else None


def _ticket_channel_slug(member: discord.Member, source_message_id: int) -> str:
    raw = str(member.display_name or member.name or member.id).casefold()
    slug = re.sub(r"[^a-z0-9äöüß_-]+", "-", raw, flags=re.IGNORECASE)
    slug = re.sub(r"-+", "-", slug).strip("-_") or str(member.id)
    suffix = str(int(source_message_id))[-4:]
    return (f"ticket-{slug}-{suffix}")[:95]


def _existing_ticket_channel(guild: discord.Guild, source_message_id: int) -> Optional[discord.TextChannel]:
    marker = f"Leader-Ticket Message-ID {int(source_message_id)}"
    for channel in guild.text_channels:
        if marker in str(channel.topic or ""):
            return channel
    return None


async def _ensure_private_ticket_channel(inter: discord.Interaction) -> tuple[Optional[discord.TextChannel], str]:
    if inter.guild is None or inter.message is None or not inter.message.embeds:
        return None, "Ticket-Nachricht nicht gefunden."
    embed = inter.message.embeds[0]
    member = _ticket_member_from_embed(inter.guild, embed)
    if member is None:
        return None, "Für anonyme Meldungen kann kein privater Ticket-Chat geöffnet werden."

    existing = _existing_ticket_channel(inter.guild, int(inter.message.id))
    if existing is not None:
        return existing, "Vorhandener Ticket-Chat verwendet."

    leader_role = _leader_role(inter.guild)
    if not isinstance(leader_role, discord.Role):
        return None, "Keine Leader-Rolle konfiguriert."
    internal = _internal_channel(inter.guild)
    category = _ticket_category(inter.guild)
    if category is None:
        category = internal.category if isinstance(internal, discord.TextChannel) else None
    me = inter.guild.me

    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        inter.guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
        leader_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
    }
    if me is not None:
        overwrites[me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
        )

    try:
        channel = await inter.guild.create_text_channel(
            _ticket_channel_slug(member, int(inter.message.id)),
            category=category,
            overwrites=overwrites,
            topic=f"Privater Leader-Contact-Chat mit {member} · Leader-Ticket Message-ID {inter.message.id}",
            reason=f"Leader-Contact Ticket für {member} ({member.id})",
        )
    except discord.Forbidden:
        return None, "Dem Bot fehlen Rechte zum Erstellen eines privaten Ticket-Channels."
    except Exception as exc:
        return None, f"Ticket-Channel konnte nicht erstellt werden: {type(exc).__name__}: {exc}"

    summary = discord.Embed(
        title=f"📨 Leader-Ticket · {member.display_name}",
        description="Privater Gesprächskanal zwischen Mitglied und Gildenleitung.",
        color=discord.Color.blurple(),
        timestamp=datetime.now(TZ),
    )
    topic = _embed_field_value(embed, "Thema") or "—"
    message = _embed_field_value(embed, "Nachricht") or "—"
    summary.add_field(name="Mitglied", value=member.mention, inline=False)
    summary.add_field(name="Thema", value=topic[:1024], inline=False)
    summary.add_field(name="Ursprüngliche Nachricht", value=message[:1024], inline=False)
    summary.add_field(name="Leader-Ticket", value=inter.message.jump_url, inline=False)
    try:
        summary.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    await channel.send(content=member.mention, embed=summary, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
    return channel, "Ticket-Chat erstellt."

class LeaderStatusView(View):
    def __init__(self, anonymous: bool = False):
        super().__init__(timeout=None)
        self.anonymous = bool(anonymous)
        if self.anonymous:
            for child in list(self.children):
                if getattr(child, "custom_id", None) == "leader_status_open_chat":
                    self.remove_item(child)

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if inter.guild is None or not is_module_enabled(inter.guild.id, "leader_contact"):
            if not inter.response.is_done():
                await inter.response.send_message(
                    "❌ Das **Leader Contact System** ist derzeit deaktiviert.", ephemeral=True
                )
            return False
        return True

    async def _edit_status(self, inter: discord.Interaction, new_status: str):
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        if not inter.message or not inter.message.embeds:
            await inter.response.send_message("❌ Nachricht/Embed nicht gefunden.", ephemeral=True)
            return

        embed = inter.message.embeds[0]
        new_embed = _replace_status_field(embed, new_status)

        try:
            await inter.message.edit(embed=new_embed, view=self)
            await inter.response.send_message("✅ Status aktualisiert.", ephemeral=True)
        except Exception as e:
            if not inter.response.is_done():
                await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)
            else:
                await inter.followup.send(f"❌ Fehler: {e}", ephemeral=True)

    @button(label="👀 Übernommen", style=ButtonStyle.primary, custom_id="leader_status_claim")
    async def btn_claim(self, inter: discord.Interaction, _):
        name = inter.user.display_name if hasattr(inter.user, "display_name") else inter.user.name
        await self._edit_status(inter, f"👀 Übernommen von **{_safe_text(name)}**")

    @button(label="💬 Ticket öffnen", style=ButtonStyle.secondary, custom_id="leader_status_open_chat")
    async def btn_open_chat(self, inter: discord.Interaction, _):
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if not inter.message or not inter.message.embeds:
            await inter.response.send_message("❌ Ticket-Nachricht nicht gefunden.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True)
        channel, info = await _ensure_private_ticket_channel(inter)
        if channel is None:
            await inter.followup.send(f"❌ {info}", ephemeral=True)
            return
        try:
            updated = _replace_or_add_field(inter.message.embeds[0], "Ticket-Chat", channel.mention, inline=False)
            await inter.message.edit(embed=updated, view=self)
        except Exception:
            pass
        await inter.followup.send(f"✅ {info} {channel.mention}", ephemeral=True)

    @button(label="✅ Erledigt", style=ButtonStyle.success, custom_id="leader_status_done")
    async def btn_done(self, inter: discord.Interaction, _):
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return
        if not inter.message or not inter.message.embeds:
            await inter.response.send_message("❌ Nachricht/Embed nicht gefunden.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)
        name = inter.user.display_name if hasattr(inter.user, "display_name") else inter.user.name
        new_embed = _replace_status_field(inter.message.embeds[0], f"✅ Erledigt von **{_safe_text(name)}**")
        ticket_ch = _existing_ticket_channel(inter.guild, int(inter.message.id)) if inter.guild else None
        if ticket_ch is not None:
            try:
                await ticket_ch.send(f"✅ Dieses Leader-Ticket wurde von {inter.user.mention} als **erledigt** markiert.", allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
            except Exception:
                pass
        archive_ch = _archive_channel(inter.guild) if inter.guild else None

        if archive_ch is None:
            await inter.message.edit(embed=new_embed, view=self)
            await inter.followup.send("✅ Status aktualisiert. Kein Archivkanal gesetzt, daher bleibt das Ticket hier stehen.", ephemeral=True)
            return

        try:
            await _archive_ticket(inter, new_embed, name)
            await inter.message.delete()
            await inter.followup.send(f"✅ Ticket erledigt und nach {getattr(archive_ch, 'mention', '#Archiv')} archiviert.", ephemeral=True)
        except Exception as e:
            try:
                await inter.message.edit(embed=new_embed, view=self)
            except Exception:
                pass
            if not inter.response.is_done():
                await inter.response.send_message(f"❌ Archivieren fehlgeschlagen: {e}", ephemeral=True)
            else:
                await inter.followup.send(f"❌ Archivieren fehlgeschlagen: {e}", ephemeral=True)

    @button(label="🗑️ Löschen", style=ButtonStyle.danger, custom_id="leader_status_delete")
    async def btn_delete(self, inter: discord.Interaction, _):
        if not _is_leader_or_admin(inter):
            await inter.response.send_message("❌ Nur Leader/Admins.", ephemeral=True)
            return

        try:
            await inter.message.delete()
        except Exception as e:
            if not inter.response.is_done():
                await inter.response.send_message(f"❌ Fehler beim Löschen: {e}", ephemeral=True)
            else:
                await inter.followup.send(f"❌ Fehler beim Löschen: {e}", ephemeral=True)


class ContactModal(Modal):
    def __init__(self, anonymous: bool):
        title = "Anonyme Meldung" if anonymous else "Leader kontaktieren"
        super().__init__(title=title, timeout=300)
        self.anonymous = anonymous

        self.topic = TextInput(
            label="Thema",
            placeholder="z. B. Hilfe, Beschwerde, Konflikt, Frage",
            required=True,
            max_length=120
        )
        self.message = TextInput(
            label="Nachricht",
            placeholder="Schreib hier dein Anliegen rein",
            required=True,
            max_length=1500,
            style=discord.TextStyle.paragraph
        )

        self.add_item(self.topic)
        self.add_item(self.message)

    async def on_submit(self, inter: discord.Interaction):
        if inter.guild is None:
            await inter.response.send_message("❌ Nur im Server nutzbar.", ephemeral=True)
            return
        if not is_module_enabled(inter.guild.id, "leader_contact"):
            await inter.response.send_message(
                "❌ Das **Leader Contact System** ist derzeit deaktiviert.", ephemeral=True
            )
            return

        guild = inter.guild
        leader_role = _leader_role(guild)
        internal_ch = _internal_channel(guild)

        if internal_ch is None:
            await inter.response.send_message("❌ Interner Leader-Kanal ist nicht gesetzt.", ephemeral=True)
            return

        now = datetime.now(TZ)
        ping_txt = leader_role.mention if leader_role else None

        topic = _safe_text(str(self.topic.value))
        msg = _safe_text(str(self.message.value))

        if self.anonymous:
            emb = discord.Embed(
                title="🕶️ Anonyme Meldung",
                color=discord.Color.dark_red(),
                timestamp=now
            )
            emb.add_field(name="Thema", value=topic or "—", inline=False)
            emb.add_field(name="Nachricht", value=msg or "—", inline=False)
            emb.add_field(name="Status", value="🆕 Offen", inline=False)
            emb.set_footer(text=f"{now.strftime('%d.%m.%Y %H:%M')} (Europe/Berlin)")
        else:
            member = inter.user if isinstance(inter.user, discord.Member) else None
            display_name = _safe_text(member.display_name if member else inter.user.name)
            user_id = int(inter.user.id)

            emb = discord.Embed(
                title="📨 Neue Leader-Anfrage",
                color=discord.Color.blurple(),
                timestamp=now
            )
            emb.add_field(name="Von", value=display_name, inline=True)
            emb.add_field(name="User-ID", value=str(user_id), inline=True)
            emb.add_field(name="Thema", value=topic or "—", inline=False)
            emb.add_field(name="Nachricht", value=msg or "—", inline=False)
            emb.add_field(name="Status", value="🆕 Offen", inline=False)
            emb.set_footer(text=f"{now.strftime('%d.%m.%Y %H:%M')} (Europe/Berlin)")

        try:
            await internal_ch.send(
                content=ping_txt,
                embed=emb,
                view=LeaderStatusView(anonymous=self.anonymous)
            )
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Anfrage nicht senden: {e}", ephemeral=True)
            return

        if self.anonymous:
            await inter.response.send_message("✅ Deine anonyme Meldung wurde an die Leader gesendet.", ephemeral=True)
        else:
            await inter.response.send_message("✅ Deine Nachricht wurde an die Leader gesendet.", ephemeral=True)


class LeaderContactView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if inter.guild is None or not is_module_enabled(inter.guild.id, "leader_contact"):
            await inter.response.send_message(
                "❌ Das **Leader Contact System** ist derzeit deaktiviert.", ephemeral=True
            )
            return False
        return True

    @button(label="📨 Leader kontaktieren", style=ButtonStyle.primary, custom_id="leader_contact_normal")
    async def btn_normal(self, inter: discord.Interaction, _):
        await inter.response.send_modal(ContactModal(anonymous=False))

    @button(label="🕶️ Anonyme Meldung", style=ButtonStyle.secondary, custom_id="leader_contact_anonymous")
    async def btn_anon(self, inter: discord.Interaction, _):
        await inter.response.send_modal(ContactModal(anonymous=True))


async def setup_leader_contact(client: discord.Client, tree: app_commands.CommandTree):
    leader_group = FeatureGroup(module_key="leader_contact", 
        name="leader",
        description="Kontakt zur Gildenleitung verwalten",
    )
    tree.add_command(leader_group)
    try:
        client.add_view(LeaderContactView())
    except Exception:
        pass

    try:
        client.add_view(LeaderStatusView())
    except Exception:
        pass

    @leader_group.command(name="public_channel", description="(Admin) Öffentlichen Kontakt-Channel setzen")
    async def leadercontact_public(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            c = _gcfg(pick_inter.guild_id)
            c["public_channel_id"] = int(channel.id)
            cfg[str(pick_inter.guild_id)] = c
            _save_cfg(cfg)
            await pick_inter.response.edit_message(content=f"✅ Öffentlicher Kontakt-Channel gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "📣 Öffentlichen Kontakt-Channel auswählen", _picked)

    @leader_group.command(name="internal_channel", description="(Admin) Internen Leader-Channel setzen")
    async def leadercontact_internal(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            c = _gcfg(pick_inter.guild_id)
            c["internal_channel_id"] = int(channel.id)
            cfg[str(pick_inter.guild_id)] = c
            _save_cfg(cfg)
            await pick_inter.response.edit_message(content=f"✅ Interner Leader-Channel gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "🔒 Internen Leader-Channel auswählen", _picked)

    @leader_group.command(name="archive_channel", description="(Admin) Archivkanal für erledigte Leader-Tickets setzen")
    async def leadercontact_archive(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        async def _picked(pick_inter: discord.Interaction, channel: discord.TextChannel):
            c = _gcfg(pick_inter.guild_id)
            c["archive_channel_id"] = int(channel.id)
            cfg[str(pick_inter.guild_id)] = c
            _save_cfg(cfg)
            try:
                runtime_db.set_guild_setting(int(pick_inter.guild_id), "guild_channel_leader_contact_archive_id", int(channel.id))
            except Exception:
                pass
            await pick_inter.response.edit_message(content=f"✅ Leader-Ticket-Archiv gesetzt: {channel.mention}", view=None)

        await send_text_channel_picker(inter, "🗃️ Archivkanal für erledigte Leader-Tickets auswählen", _picked)

    @leader_group.command(name="ticket_category", description="(Admin) Kategorie für private Leader-Ticket-Chats setzen")
    async def leadercontact_ticket_category(inter: discord.Interaction, category: discord.CategoryChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return
        c = _gcfg(inter.guild_id)
        c["ticket_category_id"] = int(category.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)
        try:
            runtime_db.set_guild_setting(int(inter.guild_id), "guild_channel_leader_ticket_category_id", int(category.id))
        except Exception:
            pass
        await inter.response.send_message(f"✅ Private Leader-Ticket-Chats werden in **{category.name}** erstellt.", ephemeral=True)

    @leader_group.command(name="role", description="(Admin) Leader-Rolle setzen")
    async def leadercontact_role(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["leader_role_id"] = int(role.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Leader-Rolle gesetzt: {role.mention}", ephemeral=True)

    @leader_group.command(name="status", description="(Admin) Zeigt die aktuelle Leader-Kontakt-Konfiguration")
    async def leadercontact_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        guild = inter.guild

        public_ch = guild.get_channel(int(c.get("public_channel_id", 0) or 0))
        internal_ch = guild.get_channel(int(c.get("internal_channel_id", 0) or 0))
        archive_ch = guild.get_channel(int(c.get("archive_channel_id", 0) or 0))
        ticket_category = guild.get_channel(int(c.get("ticket_category_id", 0) or 0))
        role = guild.get_role(int(c.get("leader_role_id", 0) or 0))

        text = (
            f"**Leader-Kontakt Status**\n"
            f"• Öffentlicher Channel: {public_ch.mention if isinstance(public_ch, discord.TextChannel) else '—'}\n"
            f"• Interner Channel: {internal_ch.mention if isinstance(internal_ch, discord.TextChannel) else '—'}\n"
            f"• Archiv: {archive_ch.mention if isinstance(archive_ch, discord.TextChannel) else '—'}\n"
            f"• Ticket-Kategorie: {ticket_category.name if isinstance(ticket_category, discord.CategoryChannel) else 'Kategorie des internen Channels'}\n"
            f"• Leader-Rolle: {role.mention if role else '—'}\n"
            f"• Kontakt-Post Channel-ID: `{c.get('contact_post_channel_id', 0)}`\n"
            f"• Kontakt-Post Message-ID: `{c.get('contact_post_message_id', 0)}`"
        )
        await inter.response.send_message(text, ephemeral=True)

    @leader_group.command(name="post", description="(Admin) Postet die Kontakt-Nachricht im öffentlichen Kontakt-Channel")
    async def leadercontact_post(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        guild = inter.guild
        public_ch = _public_channel(guild)
        if public_ch is None:
            await inter.response.send_message("❌ Öffentlicher Kontakt-Channel ist nicht gesetzt.", ephemeral=True)
            return

        emb = discord.Embed(
            title="📨 Leader kontaktieren",
            description=(
                "Du brauchst Hilfe, willst etwas melden oder hast eine Beschwerde?\n\n"
                "Nutze einfach einen der Buttons unten.\n"
                "Du kannst die Leader **normal** oder **anonym** kontaktieren."
            ),
            color=discord.Color.blurple()
        )
        emb.add_field(
            name="Optionen",
            value="• **📨 Leader kontaktieren**\n• **🕶️ Anonyme Meldung**",
            inline=False
        )
        emb.set_footer(text="Nur die Leader sehen deine Anfrage.")

        try:
            msg = await public_ch.send(embed=emb, view=LeaderContactView())
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Kontakt-Post nicht senden: {e}", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["contact_post_channel_id"] = int(msg.channel.id)
        c["contact_post_message_id"] = int(msg.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Kontakt-Post erstellt: {msg.jump_url}", ephemeral=True)

    @leader_group.command(name="repost", description="(Admin) Erstellt einen neuen Kontakt-Post")
    async def leadercontact_repost(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        guild = inter.guild
        public_ch = _public_channel(guild)
        if public_ch is None:
            await inter.response.send_message("❌ Öffentlicher Kontakt-Channel ist nicht gesetzt.", ephemeral=True)
            return

        emb = discord.Embed(
            title="📨 Leader kontaktieren",
            description=(
                "Du brauchst Hilfe, willst etwas melden oder hast eine Beschwerde?\n\n"
                "Nutze einfach einen der Buttons unten.\n"
                "Du kannst die Leader **normal** oder **anonym** kontaktieren."
            ),
            color=discord.Color.blurple()
        )
        emb.add_field(
            name="Optionen",
            value="• **📨 Leader kontaktieren**\n• **🕶️ Anonyme Meldung**",
            inline=False
        )
        emb.set_footer(text="Nur die Leader sehen deine Anfrage.")

        try:
            msg = await public_ch.send(embed=emb, view=LeaderContactView())
        except Exception as e:
            await inter.response.send_message(f"❌ Konnte Kontakt-Post nicht senden: {e}", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["contact_post_channel_id"] = int(msg.channel.id)
        c["contact_post_message_id"] = int(msg.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Neuer Kontakt-Post erstellt: {msg.jump_url}", ephemeral=True)
