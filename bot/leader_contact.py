from __future__ import annotations
import json
import asyncio
import os
import threading
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands

try:
    from bot.json_store import load_json_file, save_json_atomic  # type: ignore
except Exception:
    from json_store import load_json_file, save_json_atomic  # type: ignore

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
    return load_json_file(CFG_FILE, {}, context=__name__)


def _save_cfg(obj: dict) -> None:
    save_json_atomic(CFG_FILE, obj, context=__name__)


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



def _ticket_db_init() -> None:
    if not getattr(runtime_db,'_INITIALIZED',False): runtime_db.init_runtime_db()
    backend=getattr(runtime_db,'_BACKEND','sqlite')
    schemas=[
    '''CREATE TABLE IF NOT EXISTS leader_tickets (id {ID}, guild_id {BIG} NOT NULL, creator_user_id {BIG}, creator_name TEXT NOT NULL DEFAULT '', anonymous {BOOL} NOT NULL DEFAULT {FALSE}, subject TEXT NOT NULL DEFAULT '', original_message TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'open', assigned_to_id {BIG}, assigned_to_name TEXT NOT NULL DEFAULT '', discord_internal_channel_id {BIG}, discord_internal_message_id {BIG}, ticket_channel_id {BIG}, created_at TEXT NOT NULL, claimed_at TEXT, closed_at TEXT, closed_by_id {BIG}, closed_by_name TEXT NOT NULL DEFAULT '')''',
    '''CREATE TABLE IF NOT EXISTS leader_ticket_messages (id {ID}, ticket_id {BIG} NOT NULL, guild_id {BIG} NOT NULL, discord_message_id {BIG}, author_id {BIG}, author_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '', attachments_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS leader_ticket_notes (id {ID}, ticket_id {BIG} NOT NULL, guild_id {BIG} NOT NULL, author_id {BIG}, author_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS leader_ticket_actions (id {ID}, ticket_id {BIG} NOT NULL, guild_id {BIG} NOT NULL, action_type TEXT NOT NULL, actor_id {BIG}, actor_name TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', result_text TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, processed_at TEXT)''']
    if backend=='postgres':
        conn=runtime_db._pg_connect();
        try:
            with conn.cursor() as cur:
                for q in schemas: cur.execute(q.format(ID='BIGSERIAL PRIMARY KEY',BIG='BIGINT',BOOL='BOOLEAN',FALSE='FALSE'))
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect();
        try:
            for q in schemas: conn.execute(q.format(ID='INTEGER PRIMARY KEY AUTOINCREMENT',BIG='INTEGER',BOOL='INTEGER',FALSE='0'))
            conn.commit()
        finally: conn.close()

def _ticket_create(guild_id:int, creator_id:int|None, creator_name:str, anonymous:bool, subject:str, message:str, internal_channel_id:int, internal_message_id:int)->int:
    _ticket_db_init(); now=datetime.now(TZ).isoformat(); backend=getattr(runtime_db,'_BACKEND','sqlite')
    vals=(guild_id,creator_id,creator_name,bool(anonymous),subject,message,internal_channel_id,internal_message_id,now)
    if backend=='postgres':
        conn=runtime_db._pg_connect();
        try:
            with conn.cursor() as cur:
                cur.execute('''INSERT INTO leader_tickets(guild_id,creator_user_id,creator_name,anonymous,subject,original_message,discord_internal_channel_id,discord_internal_message_id,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id''',vals); row=cur.fetchone()
            conn.commit(); return int((row or {}).get('id') or 0)
        finally: conn.close()
    conn=runtime_db._sqlite_connect();
    try:
        cur=conn.execute('''INSERT INTO leader_tickets(guild_id,creator_user_id,creator_name,anonymous,subject,original_message,discord_internal_channel_id,discord_internal_message_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)''',vals); conn.commit(); return int(cur.lastrowid or 0)
    finally: conn.close()

def _ticket_by_internal_message(guild_id:int,message_id:int)->dict:
    _ticket_db_init(); backend=getattr(runtime_db,'_BACKEND','sqlite');
    if backend=='postgres':
        conn=runtime_db._pg_connect();
        try:
            with conn.cursor() as cur: cur.execute('SELECT * FROM leader_tickets WHERE guild_id=%s AND discord_internal_message_id=%s',(guild_id,message_id)); row=cur.fetchone(); return dict(row) if row else {}
        finally: conn.close()
    conn=runtime_db._sqlite_connect();
    try:
        row=conn.execute('SELECT * FROM leader_tickets WHERE guild_id=? AND discord_internal_message_id=?',(guild_id,message_id)).fetchone(); return dict(row) if row else {}
    finally: conn.close()

def _ticket_by_id(ticket_id:int)->dict:
    _ticket_db_init(); backend=getattr(runtime_db,'_BACKEND','sqlite')
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur: cur.execute('SELECT * FROM leader_tickets WHERE id=%s',(int(ticket_id),)); row=cur.fetchone(); return dict(row) if row else {}
        finally: conn.close()
    conn=runtime_db._sqlite_connect()
    try:
        row=conn.execute('SELECT * FROM leader_tickets WHERE id=?',(int(ticket_id),)).fetchone(); return dict(row) if row else {}
    finally: conn.close()

def _ticket_update(ticket_id:int, **fields)->None:
    if not ticket_id or not fields:return
    allowed={'status','assigned_to_id','assigned_to_name','claimed_at','ticket_channel_id','closed_at','closed_by_id','closed_by_name'}; data={k:v for k,v in fields.items() if k in allowed}
    if not data:return
    _ticket_db_init(); backend=getattr(runtime_db,'_BACKEND','sqlite'); cols=list(data); vals=[data[k] for k in cols]
    if backend=='postgres':
        conn=runtime_db._pg_connect();
        try:
            with conn.cursor() as cur: cur.execute('UPDATE leader_tickets SET '+','.join(f'{k}=%s' for k in cols)+' WHERE id=%s',tuple(vals+[ticket_id]))
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect();
        try: conn.execute('UPDATE leader_tickets SET '+','.join(f'{k}=?' for k in cols)+' WHERE id=?',tuple(vals+[ticket_id])); conn.commit()
        finally: conn.close()

def _ticket_add_message(ticket_id:int,guild_id:int,msg:discord.Message)->None:
    """Speichert oder repariert eine Ticketnachricht anhand ihrer Discord-Message-ID."""
    _ticket_db_init(); atts=json.dumps([{'name':a.filename,'url':a.url} for a in msg.attachments],ensure_ascii=False); now=(msg.created_at or datetime.now(TZ)).isoformat(); content=str(msg.content or ''); author_name=getattr(msg.author,'display_name',getattr(msg.author,'name',str(msg.author.id))); backend=getattr(runtime_db,'_BACKEND','sqlite')
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT id,content FROM leader_ticket_messages WHERE ticket_id=%s AND discord_message_id=%s ORDER BY id DESC LIMIT 1',(ticket_id,msg.id)); existing=cur.fetchone()
                if existing:
                    cur.execute('UPDATE leader_ticket_messages SET guild_id=%s,author_id=%s,author_name=%s,content=%s,attachments_json=%s,created_at=%s WHERE id=%s',(guild_id,msg.author.id,author_name,content,atts,now,int(existing.get('id'))))
                else:
                    cur.execute('INSERT INTO leader_ticket_messages(ticket_id,guild_id,discord_message_id,author_id,author_name,content,attachments_json,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(ticket_id,guild_id,msg.id,msg.author.id,author_name,content,atts,now))
            conn.commit()
        finally: conn.close()
    else:
        conn=runtime_db._sqlite_connect()
        try:
            existing=conn.execute('SELECT id,content FROM leader_ticket_messages WHERE ticket_id=? AND discord_message_id=? ORDER BY id DESC LIMIT 1',(ticket_id,msg.id)).fetchone()
            if existing:
                conn.execute('UPDATE leader_ticket_messages SET guild_id=?,author_id=?,author_name=?,content=?,attachments_json=?,created_at=? WHERE id=?',(guild_id,msg.author.id,author_name,content,atts,now,int(existing['id'])))
            else:
                conn.execute('INSERT INTO leader_ticket_messages(ticket_id,guild_id,discord_message_id,author_id,author_name,content,attachments_json,created_at) VALUES(?,?,?,?,?,?,?,?)',(ticket_id,guild_id,msg.id,msg.author.id,author_name,content,atts,now))
            conn.commit()
        finally: conn.close()

async def _sync_ticket_channel_history(guild: discord.Guild, ticket: dict, *, limit: int = 250) -> int:
    channel_id=int(ticket.get('ticket_channel_id') or 0); ticket_id=int(ticket.get('id') or 0)
    if not channel_id or not ticket_id:return 0
    channel=guild.get_channel(channel_id)
    if channel is None:
        try: channel=await guild.fetch_channel(channel_id)
        except Exception: return 0
    if not isinstance(channel,discord.TextChannel):return 0
    saved=0
    try:
        async for msg in channel.history(limit=limit,oldest_first=True):
            if msg.author.bot: continue
            _ticket_add_message(ticket_id,guild.id,msg); saved+=1
    except Exception as exc:
        print(f"[leader_contact] Ticket-History Sync {ticket_id}: {exc!r}",flush=True)
    return saved

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
    """Schnelle Cache-Auflösung des konfigurierten Archivkanals."""
    ch_id = int((_gcfg(guild.id).get("archive_channel_id") or 0))
    ch = guild.get_channel(ch_id)
    return ch if isinstance(ch, (discord.TextChannel, discord.Thread)) else None


async def _resolve_archive_channel_for_guild(client:discord.Client, guild:discord.Guild) -> Optional[discord.abc.Messageable]:
    ch_id=int((_gcfg(guild.id).get("archive_channel_id") or 0))
    if not ch_id:return None
    ch=guild.get_channel(ch_id)
    if isinstance(ch,(discord.TextChannel,discord.Thread)):return ch
    try: fetched=await client.fetch_channel(ch_id)
    except Exception:return None
    return fetched if isinstance(fetched,(discord.TextChannel,discord.Thread)) else None

async def _resolve_archive_channel(inter: discord.Interaction) -> Optional[discord.abc.Messageable]:
    if inter.guild is None:return None
    return await _resolve_archive_channel_for_guild(inter.client, inter.guild)


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


async def _archive_ticket_message(client:discord.Client,guild:discord.Guild,message:discord.Message,embed:discord.Embed,actor_name:str,actor_id:int=0)->tuple[bool,Optional[discord.abc.Messageable],str]:
    archive_ch=await _resolve_archive_channel_for_guild(client,guild)
    if archive_ch is None:return False,None,"Kein gültiger Archivkanal konfiguriert oder Kanal nicht erreichbar."
    if isinstance(archive_ch,discord.TextChannel):
        me=guild.me
        if me is not None:
            perms=archive_ch.permissions_for(me)
            if not perms.view_channel or not perms.send_messages:return False,archive_ch,"Dem Bot fehlt im Archivkanal `Kanal ansehen` oder `Nachrichten senden`."
    try: archived=discord.Embed.from_dict(embed.to_dict())
    except Exception: archived=embed
    actor=f"<@{int(actor_id)}>" if actor_id else f"**{_safe_text(actor_name)}**"
    archived.add_field(name="Archiviert von",value=actor,inline=False)
    archived.add_field(name="Ursprung",value=f"<#{message.channel.id}> · Nachricht `{message.id}`",inline=False)
    old_footer=str(getattr(getattr(archived,"footer",None),"text","") or "").strip(); stamp=datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    archived.set_footer(text=(old_footer+" · " if old_footer else "")+f"Archiviert {stamp}")
    try:await archive_ch.send(embed=archived,allowed_mentions=discord.AllowedMentions.none())
    except Exception as exc:return False,archive_ch,f"{type(exc).__name__}: {exc}"
    return True,archive_ch,""

async def _archive_ticket(inter:discord.Interaction,embed:discord.Embed,actor_name:str)->tuple[bool,Optional[discord.abc.Messageable],str]:
    if inter.guild is None or inter.message is None:return False,None,"Guild/Nachricht nicht verfügbar."
    return await _archive_ticket_message(inter.client,inter.guild,inter.message,embed,actor_name,int(getattr(inter.user,"id",0) or 0))



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


async def _ensure_private_ticket_channel_for_message(guild:discord.Guild, message:discord.Message) -> tuple[Optional[discord.TextChannel], str]:
    if not message.embeds:return None,"Ticket-Nachricht nicht gefunden."
    embed=message.embeds[0]
    member=_ticket_member_from_embed(guild,embed)
    if member is None:return None,"Für anonyme Meldungen kann kein privater Ticket-Chat geöffnet werden."
    existing=_existing_ticket_channel(guild,int(message.id))
    if existing is not None:return existing,"Vorhandener Ticket-Chat verwendet."
    leader_role=_leader_role(guild)
    if not isinstance(leader_role,discord.Role):return None,"Keine Leader-Rolle konfiguriert."
    internal=_internal_channel(guild); category=_ticket_category(guild)
    if category is None:category=internal.category if isinstance(internal,discord.TextChannel) else None
    me=guild.me
    overwrites={
        guild.default_role:discord.PermissionOverwrite(view_channel=False),
        member:discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True,embed_links=True),
        leader_role:discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True,embed_links=True),
    }
    if me is not None:overwrites[me]=discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,manage_channels=True,manage_messages=True)
    try:
        channel=await guild.create_text_channel(_ticket_channel_slug(member,int(message.id)),category=category,overwrites=overwrites,topic=f"Privater Leader-Contact-Chat mit {member} · Leader-Ticket Message-ID {message.id}",reason=f"Leader-Contact Ticket für {member} ({member.id})")
    except discord.Forbidden:return None,"Dem Bot fehlen Rechte zum Erstellen eines privaten Ticket-Channels."
    except Exception as exc:return None,f"Ticket-Channel konnte nicht erstellt werden: {type(exc).__name__}: {exc}"
    summary=discord.Embed(title=f"📨 Leader-Ticket · {member.display_name}",description="Privater Gesprächskanal zwischen Mitglied und Gildenleitung.",color=discord.Color.blurple(),timestamp=datetime.now(TZ))
    summary.add_field(name="Mitglied",value=member.mention,inline=False)
    summary.add_field(name="Thema",value=(_embed_field_value(embed,"Thema") or "—")[:1024],inline=False)
    summary.add_field(name="Ursprüngliche Nachricht",value=(_embed_field_value(embed,"Nachricht") or "—")[:1024],inline=False)
    summary.add_field(name="Leader-Ticket",value=message.jump_url,inline=False)
    try:summary.set_thumbnail(url=member.display_avatar.url)
    except Exception:pass
    await channel.send(content=member.mention,embed=summary,allowed_mentions=discord.AllowedMentions(users=True,roles=False,everyone=False))
    return channel,"Ticket-Chat erstellt."

async def _ensure_private_ticket_channel(inter:discord.Interaction)->tuple[Optional[discord.TextChannel],str]:
    if inter.guild is None or inter.message is None:return None,"Ticket-Nachricht nicht gefunden."
    return await _ensure_private_ticket_channel_for_message(inter.guild,inter.message)

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
        try:
            t=_ticket_by_internal_message(inter.guild.id,inter.message.id); _ticket_update(int(t.get('id') or 0),status='claimed',assigned_to_id=int(inter.user.id),assigned_to_name=name,claimed_at=datetime.now(TZ).isoformat())
        except Exception: pass

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
        try:
            t=_ticket_by_internal_message(inter.guild.id,inter.message.id); _ticket_update(int(t.get('id') or 0),status='conversation',ticket_channel_id=int(channel.id))
        except Exception: pass
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
        t={}
        try:
            t=_ticket_by_internal_message(inter.guild.id,inter.message.id)
            if t and ticket_ch is not None:
                await _sync_ticket_channel_history(inter.guild,t)
        except Exception as exc:
            print(f"[leader_contact] Ticket laden/synchronisieren fehlgeschlagen: {exc!r}")
        ok, archive_ch, archive_error = await _archive_ticket(inter, new_embed, name)
        if not ok:
            # Niemals das Original löschen, solange die Archivkopie nicht bestätigt ist.
            try:
                await inter.message.edit(embed=new_embed, view=self)
            except Exception:
                pass
            if archive_ch is None and "Kein gültiger Archivkanal" in archive_error:
                await inter.followup.send(
                    "❌ Ticket wurde **nicht abgeschlossen**, weil die Archivkopie fehlt. "
                    "Bitte `/leader archive_channel` bzw. `leader_contact_archive` prüfen. "
                    f"Details: {archive_error}",
                    ephemeral=True,
                )
            else:
                mention = getattr(archive_ch, "mention", "#Archiv") if archive_ch is not None else "#Archiv"
                await inter.followup.send(
                    f"❌ Ticket bleibt hier stehen: Kopieren nach {mention} fehlgeschlagen. **{archive_error}**",
                    ephemeral=True,
                )
            return

        # Erst nach bestätigter Discord-Archivkopie dauerhaft abschließen.
        try:
            _ticket_update(int(t.get('id') or 0),status='done',closed_at=datetime.now(TZ).isoformat(),closed_by_id=int(inter.user.id),closed_by_name=name)
            if ticket_ch is not None and t.get('creator_user_id'):
                member=inter.guild.get_member(int(t.get('creator_user_id')))
                if member:
                    await ticket_ch.set_permissions(member,view_channel=True,send_messages=False,read_message_history=True)
        except Exception as exc:
            print(f"[leader_contact] Ticket close DB/lock fehlgeschlagen: {exc!r}")

        try:
            await inter.message.delete()
        except Exception as exc:
            await inter.followup.send(
                f"✅ Archivkopie wurde in {getattr(archive_ch, 'mention', '#Archiv')} erstellt, "
                f"aber die aktive Ticket-Nachricht konnte nicht gelöscht werden: {type(exc).__name__}: {exc}",
                ephemeral=True,
            )
            return
        await inter.followup.send(f"✅ Ticket erledigt und nach {getattr(archive_ch, 'mention', '#Archiv')} archiviert.", ephemeral=True)

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
            sent_ticket = await internal_ch.send(
                content=ping_txt, embed=emb, view=LeaderStatusView(anonymous=self.anonymous)
            )
            try:
                _ticket_create(guild.id, None if self.anonymous else int(inter.user.id), "Anonym" if self.anonymous else str(getattr(inter.user,'display_name',inter.user.name)), self.anonymous, topic, msg, int(sent_ticket.channel.id), int(sent_ticket.id))
            except Exception as exc:
                print(f"[leader_contact] Ticket-DB create fehlgeschlagen: {exc!r}")
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



def _ticket_action_finish(action_id:int,status:str,result_text:str)->None:
    backend=getattr(runtime_db,'_BACKEND','sqlite'); now=datetime.now(TZ).isoformat()
    if backend=='postgres':
        conn=runtime_db._pg_connect()
        try:
            with conn.cursor() as cur:cur.execute('UPDATE leader_ticket_actions SET status=%s,result_text=%s,processed_at=%s WHERE id=%s',(status,result_text[:1000],now,action_id))
            conn.commit()
        finally:conn.close()
    else:
        conn=runtime_db._sqlite_connect()
        try:conn.execute('UPDATE leader_ticket_actions SET status=?,result_text=?,processed_at=? WHERE id=?',(status,result_text[:1000],now,action_id));conn.commit()
        finally:conn.close()

async def _process_ticket_dashboard_action(client:discord.Client,row:dict)->str:
    ticket=_ticket_by_id(int(row.get('ticket_id') or 0)); guild=client.get_guild(int(row.get('guild_id') or 0))
    if not ticket or guild is None:raise RuntimeError('Ticket oder Guild nicht gefunden.')
    ch=guild.get_channel(int(ticket.get('discord_internal_channel_id') or 0))
    if ch is None:
        try:ch=await client.fetch_channel(int(ticket.get('discord_internal_channel_id') or 0))
        except Exception:ch=None
    if ch is None:raise RuntimeError('Interner Leader-Channel nicht gefunden.')
    try:message=await ch.fetch_message(int(ticket.get('discord_internal_message_id') or 0))
    except Exception as exc:raise RuntimeError(f'Ticket-Nachricht nicht gefunden: {exc}')
    if not message.embeds:raise RuntimeError('Ticket-Embed nicht gefunden.')
    action=str(row.get('action_type') or ''); actor_name=str(row.get('actor_name') or row.get('actor_id') or 'Dashboard'); actor_id=int(row.get('actor_id') or 0)
    view=LeaderStatusView(anonymous=bool(ticket.get('anonymous')))
    if action=='claim':
        emb=_replace_status_field(message.embeds[0],f"👀 Übernommen von **{_safe_text(actor_name)}**")
        await message.edit(embed=emb,view=view);_ticket_update(int(ticket['id']),status='claimed',assigned_to_id=actor_id or None,assigned_to_name=actor_name,claimed_at=datetime.now(TZ).isoformat());return 'Ticket übernommen.'
    if action=='open_chat':
        if bool(ticket.get('anonymous')):raise RuntimeError('Anonyme Tickets können keinen privaten Chat öffnen.')
        channel,info=await _ensure_private_ticket_channel_for_message(guild,message)
        if channel is None:raise RuntimeError(info)
        emb=_replace_or_add_field(message.embeds[0],'Ticket-Chat',channel.mention,inline=False);await message.edit(embed=emb,view=view);_ticket_update(int(ticket['id']),status='conversation',ticket_channel_id=int(channel.id));return info
    if action=='done':
        emb=_replace_status_field(message.embeds[0],f"✅ Erledigt von **{_safe_text(actor_name)}**")
        ticket_ch=_existing_ticket_channel(guild,int(message.id))
        if ticket_ch is not None:
            await _sync_ticket_channel_history(guild,ticket)
        ok,archive_ch,err=await _archive_ticket_message(client,guild,message,emb,actor_name,actor_id)
        if not ok:raise RuntimeError(err)
        _ticket_update(int(ticket['id']),status='done',closed_at=datetime.now(TZ).isoformat(),closed_by_id=actor_id or None,closed_by_name=actor_name)
        if ticket_ch is not None and ticket.get('creator_user_id'):
            member=guild.get_member(int(ticket.get('creator_user_id')))
            if member:await ticket_ch.set_permissions(member,view_channel=True,send_messages=False,read_message_history=True)
            try:await ticket_ch.send(f"✅ Dieses Leader-Ticket wurde von **{_safe_text(actor_name)}** als **erledigt** markiert.",allowed_mentions=discord.AllowedMentions.none())
            except Exception:pass
        await message.delete();return f"Ticket erledigt und nach {getattr(archive_ch,'mention','#Archiv')} archiviert."
    raise RuntimeError('Unbekannte Ticket-Aktion.')

async def _ticket_dashboard_action_loop(client:discord.Client)->None:
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            _ticket_db_init();backend=getattr(runtime_db,'_BACKEND','sqlite');rows=[]
            if backend=='postgres':
                conn=runtime_db._pg_connect()
                try:
                    with conn.cursor() as cur:cur.execute("SELECT * FROM leader_ticket_actions WHERE status='pending' ORDER BY id LIMIT 20");rows=[dict(r) for r in cur.fetchall()]
                finally:conn.close()
            else:
                conn=runtime_db._sqlite_connect()
                try:rows=[dict(r) for r in conn.execute("SELECT * FROM leader_ticket_actions WHERE status='pending' ORDER BY id LIMIT 20").fetchall()]
                finally:conn.close()
            for row in rows:
                try:res=await _process_ticket_dashboard_action(client,row);_ticket_action_finish(int(row['id']),'done',res)
                except Exception as exc:_ticket_action_finish(int(row['id']),'error',f'{type(exc).__name__}: {exc}')
        except Exception as exc:print(f"[leader_contact] Dashboard-Ticket-Queue: {exc!r}")
        await asyncio.sleep(5)

async def _ticket_history_sync_loop(client: discord.Client) -> None:
    """Hält private Ticket-Chats im Dashboard-Archiv aktuell und repariert ältere leere Einträge."""
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            _ticket_db_init(); backend=getattr(runtime_db,'_BACKEND','sqlite'); rows=[]
            if backend=='postgres':
                conn=runtime_db._pg_connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM leader_tickets WHERE ticket_channel_id IS NOT NULL AND status IN ('open','claimed','conversation','done') ORDER BY id DESC LIMIT 100")
                        rows=[dict(r) for r in cur.fetchall()]
                finally: conn.close()
            else:
                conn=runtime_db._sqlite_connect()
                try: rows=[dict(r) for r in conn.execute("SELECT * FROM leader_tickets WHERE ticket_channel_id IS NOT NULL AND status IN ('open','claimed','conversation','done') ORDER BY id DESC LIMIT 100").fetchall()]
                finally: conn.close()
            for row in rows:
                guild=client.get_guild(int(row.get('guild_id') or 0))
                if guild is None: continue
                await _sync_ticket_channel_history(guild,row,limit=250)
        except Exception as exc:
            print(f"[leader_contact] Ticket-History Loop: {exc!r}",flush=True)
        await asyncio.sleep(90)

async def _ticket_cleanup_loop(client:discord.Client) -> None:
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            _ticket_db_init(); backend=getattr(runtime_db,'_BACKEND','sqlite'); rows=[]
            if backend=='postgres':
                conn=runtime_db._pg_connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id,guild_id,ticket_channel_id,closed_at FROM leader_tickets WHERE status='done' AND ticket_channel_id IS NOT NULL AND closed_at IS NOT NULL")
                        rows=[dict(r) for r in cur.fetchall()]
                finally: conn.close()
            else:
                conn=runtime_db._sqlite_connect()
                try: rows=[dict(r) for r in conn.execute("SELECT id,guild_id,ticket_channel_id,closed_at FROM leader_tickets WHERE status='done' AND ticket_channel_id IS NOT NULL AND closed_at IS NOT NULL").fetchall()]
                finally: conn.close()
            now=datetime.now(TZ)
            for row in rows:
                try:
                    closed=datetime.fromisoformat(str(row.get('closed_at')).replace('Z','+00:00'))
                    if closed.tzinfo is None: closed=closed.replace(tzinfo=TZ)
                    if (now-closed.astimezone(TZ)).total_seconds() < 7*86400: continue
                    guild=client.get_guild(int(row.get('guild_id') or 0)); channel=guild.get_channel(int(row.get('ticket_channel_id') or 0)) if guild else None
                    if channel is not None:
                        try: await channel.delete(reason='Leader-Ticket seit 7 Tagen erledigt')
                        except Exception: continue
                    _ticket_update(int(row.get('id') or 0),ticket_channel_id=None)
                except Exception: continue
        except Exception as exc: print(f"[leader_contact] Ticket-Cleanup: {exc!r}")
        await asyncio.sleep(21600)

async def setup_leader_contact(client: discord.Client, tree: app_commands.CommandTree):
    _ticket_db_init()
    asyncio.create_task(_ticket_cleanup_loop(client))
    asyncio.create_task(_ticket_dashboard_action_loop(client))
    asyncio.create_task(_ticket_history_sync_loop(client))
    async def _ticket_message_listener(message:discord.Message):
        if not message.guild or message.author.bot: return
        try:
            backend=getattr(runtime_db,'_BACKEND','sqlite')
            if backend=='postgres':
                conn=runtime_db._pg_connect();
                try:
                    with conn.cursor() as cur: cur.execute("SELECT id FROM leader_tickets WHERE guild_id=%s AND ticket_channel_id=%s AND status IN ('conversation','claimed','open') ORDER BY id DESC LIMIT 1",(message.guild.id,message.channel.id)); row=cur.fetchone()
                finally: conn.close()
            else:
                conn=runtime_db._sqlite_connect();
                try: row=conn.execute("SELECT id FROM leader_tickets WHERE guild_id=? AND ticket_channel_id=? AND status IN ('conversation','claimed','open') ORDER BY id DESC LIMIT 1",(message.guild.id,message.channel.id)).fetchone()
                finally: conn.close()
            if row: _ticket_add_message(int(dict(row).get('id') or 0),message.guild.id,message)
        except Exception as exc: print(f"[leader_contact] ticket message log: {exc!r}")
    client.add_listener(_ticket_message_listener,'on_message')
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
