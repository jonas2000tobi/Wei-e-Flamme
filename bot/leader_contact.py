from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ui import View, button, Modal, TextInput
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CFG_FILE = DATA_DIR / "leader_contact_cfg.json"


def _load_cfg() -> dict:
    try:
        return json.loads(CFG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cfg(obj: dict) -> None:
    CFG_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


cfg: dict = _load_cfg()


def _gcfg(guild_id: int) -> dict:
    c = cfg.get(str(guild_id)) or {}
    c.setdefault("public_channel_id", 0)
    c.setdefault("internal_channel_id", 0)
    c.setdefault("leader_role_id", 0)
    c.setdefault("contact_post_channel_id", 0)
    c.setdefault("contact_post_message_id", 0)
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
    return role in inter.user.roles


def _replace_status_field(embed: discord.Embed, text: str) -> discord.Embed:
    new_embed = discord.Embed(
        title=embed.title,
        description=embed.description,
        color=embed.color
    )

    for field in embed.fields:
        if field.name != "Status":
            new_embed.add_field(name=field.name, value=field.value, inline=field.inline)

    new_embed.add_field(name="Status", value=text, inline=False)

    if embed.footer and embed.footer.text:
        new_embed.set_footer(text=embed.footer.text)

    return new_embed


class LeaderStatusView(View):
    def __init__(self):
        super().__init__(timeout=None)

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

    @button(label="✅ Erledigt", style=ButtonStyle.success, custom_id="leader_status_done")
    async def btn_done(self, inter: discord.Interaction, _):
        name = inter.user.display_name if hasattr(inter.user, "display_name") else inter.user.name
        await self._edit_status(inter, f"✅ Erledigt von **{_safe_text(name)}**")

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
        super().__init__(title=title, timeout=None)
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
                view=LeaderStatusView()
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

    @button(label="📨 Leader kontaktieren", style=ButtonStyle.primary, custom_id="leader_contact_normal")
    async def btn_normal(self, inter: discord.Interaction, _):
        await inter.response.send_modal(ContactModal(anonymous=False))

    @button(label="🕶️ Anonyme Meldung", style=ButtonStyle.secondary, custom_id="leader_contact_anonymous")
    async def btn_anon(self, inter: discord.Interaction, _):
        await inter.response.send_modal(ContactModal(anonymous=True))


async def setup_leader_contact(client: discord.Client, tree: app_commands.CommandTree):
    try:
        client.add_view(LeaderContactView())
    except Exception:
        pass

    try:
        client.add_view(LeaderStatusView())
    except Exception:
        pass

    @tree.command(name="leadercontact_set_public_channel", description="(Admin) Öffentlichen Kontakt-Channel setzen")
    async def leadercontact_set_public_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["public_channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Öffentlicher Kontakt-Channel gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="leadercontact_set_internal_channel", description="(Admin) Internen Leader-Channel setzen")
    async def leadercontact_set_internal_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["internal_channel_id"] = int(channel.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Interner Leader-Channel gesetzt: {channel.mention}", ephemeral=True)

    @tree.command(name="leadercontact_set_role", description="(Admin) Leader-Rolle setzen")
    async def leadercontact_set_role(inter: discord.Interaction, role: discord.Role):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        c["leader_role_id"] = int(role.id)
        cfg[str(inter.guild_id)] = c
        _save_cfg(cfg)

        await inter.response.send_message(f"✅ Leader-Rolle gesetzt: {role.mention}", ephemeral=True)

    @tree.command(name="leadercontact_status", description="(Admin) Zeigt die aktuelle Leader-Kontakt-Konfiguration")
    async def leadercontact_status(inter: discord.Interaction):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admins.", ephemeral=True)
            return

        c = _gcfg(inter.guild_id)
        guild = inter.guild

        public_ch = guild.get_channel(int(c.get("public_channel_id", 0) or 0))
        internal_ch = guild.get_channel(int(c.get("internal_channel_id", 0) or 0))
        role = guild.get_role(int(c.get("leader_role_id", 0) or 0))

        text = (
            f"**Leader-Kontakt Status**\n"
            f"• Öffentlicher Channel: {public_ch.mention if isinstance(public_ch, discord.TextChannel) else '—'}\n"
            f"• Interner Channel: {internal_ch.mention if isinstance(internal_ch, discord.TextChannel) else '—'}\n"
            f"• Leader-Rolle: {role.mention if role else '—'}\n"
            f"• Kontakt-Post Channel-ID: `{c.get('contact_post_channel_id', 0)}`\n"
            f"• Kontakt-Post Message-ID: `{c.get('contact_post_message_id', 0)}`"
        )
        await inter.response.send_message(text, ephemeral=True)

    @tree.command(name="leadercontact_post", description="(Admin) Postet die Kontakt-Nachricht im öffentlichen Kontakt-Channel")
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

    @tree.command(name="leadercontact_repost", description="(Admin) Erstellt einen neuen Kontakt-Post")
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
