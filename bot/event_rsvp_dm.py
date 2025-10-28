# bot/event_rsvp_dm.py
# -----------------------------------------------------------
# DM-basiertes Raid-Anmelde-System mit Auto-Löschung nach 2 h
# -----------------------------------------------------------

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional
import asyncio

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RSVP_FILE = DATA_DIR / "event_rsvp.json"
DM_CFG_FILE = DATA_DIR / "event_rsvp_cfg.json"
DM_TRACK_FILE = DATA_DIR / "event_dm_sent.json"  # <- neu: verfolgt gesendete DMs

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg: Dict[str, dict] = _load(DM_CFG_FILE, {})
sent_dm: Dict[str, list] = _load(DM_TRACK_FILE, {})  # {msg_id:[message_ids]}

def save_store(): _save(RSVP_FILE, store)
def save_cfg(): _save(DM_CFG_FILE, cfg)
def save_sent_dm(): _save(DM_TRACK_FILE, sent_dm)

# ---------------- Logging ----------------
async def _log(client: discord.Client, guild_id: int, text: str):
    gcfg = cfg.get(str(guild_id)) or {}
    ch_id = int(gcfg.get("LOG_CH", 0) or 0)
    if not ch_id:
        return
    g = client.get_guild(guild_id)
    if not g:
        return
    ch = g.get_channel(ch_id)
    if isinstance(ch, (discord.TextChannel, discord.Thread)):
        try:
            await ch.send(f"[RSVP-DM] {text}")
        except Exception:
            pass

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

# ---------------- Struktur sichern ----------------
def _init_event_shape(obj: dict):
    if "yes" not in obj or not isinstance(obj["yes"], dict):
        obj["yes"] = {"TANK": [], "HEAL": [], "DPS": []}
    for k in ("TANK", "HEAL", "DPS"):
        if k not in obj["yes"]: obj["yes"][k] = []
    obj.setdefault("maybe", {})
    obj.setdefault("no", [])
    obj.setdefault("target_role_id", 0)

# ---------------- Embed ----------------
def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    when = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=f"{obj.get('description','')}\n🕒 {when.strftime('%a, %d.%m.%Y %H:%M')} Europe/Berlin",
        color=discord.Color.blurple()
    )
    yes = obj["yes"]; maybe = obj["maybe"]; no = obj["no"]

    def list_or_dash(lst): return "\n".join(lst) if lst else "—"
    emb.add_field(name=f"🛡️ Tank ({len(yes['TANK'])})", value=list_or_dash([_mention(guild,u) for u in yes["TANK"]]), inline=True)
    emb.add_field(name=f"💚 Heal ({len(yes['HEAL'])})", value=list_or_dash([_mention(guild,u) for u in yes["HEAL"]]), inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(yes['DPS'])})",  value=list_or_dash([_mention(guild,u) for u in yes["DPS"]]), inline=True)

    maybel = [f"{_mention(guild,int(k))} ({v})" if v else _mention(guild,int(k)) for k,v in maybe.items()]
    emb.add_field(name=f"❔ Vielleicht ({len(maybel)})", value=list_or_dash(maybel), inline=False)
    emb.add_field(name=f"❌ Abgemeldet ({len(no)})", value=list_or_dash([_mention(guild,u) for u in no]), inline=False)

    tr_id = int(obj.get("target_role_id",0))
    if tr_id and (r := guild.get_role(tr_id)): emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)
    if obj.get("image_url"): emb.set_image(url=obj["image_url"])
    emb.set_footer(text="An-/Abmeldung läuft per DM-Buttons")
    return emb

# ---------------- View ----------------
class RaidView(View):
    def __init__(self, msg_id:int): super().__init__(timeout=None); self.msg_id=str(msg_id)

    async def _update(self, inter:discord.Interaction, group:str):
        try:
            obj=store.get(self.msg_id)
            if not obj: return await inter.response.send_message("Event existiert nicht mehr.", ephemeral=True)
            _init_event_shape(obj)
            uid=inter.user.id
            for k in("TANK","HEAL","DPS"): obj["yes"][k]=[x for x in obj["yes"][k] if x!=uid]
            obj["no"]=[x for x in obj["no"] if x!=uid]
            obj["maybe"].pop(str(uid),None)

            if group in("TANK","HEAL","DPS"):
                obj["yes"][group].append(uid); text=f"Angemeldet als {group}"
            elif group=="MAYBE":
                obj["maybe"][str(uid)]=""; text="Als Vielleicht eingetragen."
            elif group=="NO":
                obj["no"].append(uid); text="Abgemeldet."
            else: text="Aktualisiert."
            save_store()

            guild=inter.client.get_guild(obj["guild_id"])
            if guild and (ch:=guild.get_channel(obj["channel_id"])):
                try:
                    msg=await ch.fetch_message(int(self.msg_id))
                    await msg.edit(embed=build_embed(guild,obj))
                except Exception: pass
            await inter.response.send_message(text, ephemeral=True)
        except Exception as e:
            await _log(inter.client, store.get(self.msg_id,{}).get("guild_id",0), f"Button-Fehler: {e}")

    @button(label="🛡️ Tank", style=ButtonStyle.primary)  async def t(self,i,_): await self._update(i,"TANK")
    @button(label="💚 Heal", style=ButtonStyle.secondary) async def h(self,i,_): await self._update(i,"HEAL")
    @button(label="🗡️ DPS",  style=ButtonStyle.secondary) async def d(self,i,_): await self._update(i,"DPS")
    @button(label="❔ Vielleicht",style=ButtonStyle.secondary) async def m(self,i,_): await self._update(i,"MAYBE")
    @button(label="❌ Abmelden", style=ButtonStyle.danger)   async def n(self,i,_): await self._update(i,"NO")

# ---------------- Commands ----------------
def _is_admin(i:discord.Interaction)->bool:
    p=getattr(i.user,"guild_permissions",None)
    return bool(p and (p.administrator or p.manage_guild))

async def setup_rsvp_dm(client:discord.Client, tree:app_commands.CommandTree):
    # Alte Views wiederherstellen
    for msg_id in list(store.keys()):
        try: client.add_view(RaidView(int(msg_id)))
        except Exception: pass

    @tree.command(name="raid_set_log_channel",description="(Admin) Log-Kanal setzen")
    async def setlog(i:discord.Interaction,channel:discord.TextChannel):
        if not _is_admin(i): return await i.response.send_message("Nur Admin.",ephemeral=True)
        c=cfg.get(str(i.guild_id),{}); c["LOG_CH"]=channel.id; cfg[str(i.guild_id)]=c; save_cfg()
        await i.response.send_message(f"✅ Log-Kanal: {channel.mention}",ephemeral=True)

    @tree.command(name="raid_create_dm",description="(Admin) Raid/Anmeldung per DM erzeugen")
    async def create(i:discord.Interaction,title:str,date:str,time:str,
                     channel:Optional[discord.TextChannel]=None,
                     target_role:Optional[discord.Role]=None,
                     image_url:Optional[str]=None):
        if not _is_admin(i): return await i.response.send_message("Nur Admin.",ephemeral=True)
        try:
            y,m,d=[int(x) for x in date.split("-")]; h,mi=[int(x) for x in time.split(":")]
            when=datetime(y,m,d,h,mi,tzinfo=TZ)
        except: return await i.response.send_message("❌ Datum/Zeit ungültig.",ephemeral=True)

        ch=channel or i.channel
        obj={"guild_id":i.guild_id,"channel_id":ch.id,"title":title,
             "description":"","when_iso":when.isoformat(),"image_url":image_url or None,
             "yes":{"TANK":[],"HEAL":[],"DPS":[]}, "maybe":{}, "no":[],
             "target_role_id":int(target_role.id) if target_role else 0}
        emb=build_embed(i.guild,obj)
        msg=await ch.send(embed=emb)
        store[str(msg.id)]=obj; save_store()

        sent_dm[str(msg.id)]=[]
        role=target_role if target_role else None
        count=0
        for m in i.guild.members:
            if m.bot: continue
            if role and role not in m.roles: continue
            try:
                t=f"**{title}**\n{when.strftime('%a %d.%m %H:%M')} Europe/Berlin\nÜbersicht: #{ch.name}"
                dm=await m.send(t,view=RaidView(int(msg.id)))
                sent_dm[str(msg.id)].append(dm.id); count+=1
            except: pass
        save_sent_dm()

        # Auto-Löschung in 2 h nach Start
        async def _cleanup():
            await asyncio.sleep(max((when-datetime.now(TZ)).total_seconds()+7200,0))
            await _delete_old_dms(i.client,int(msg.id))
        asyncio.create_task(_cleanup())

        await i.response.send_message(f"✅ Raid erstellt – {count} DMs versendet.",ephemeral=True)

# ---------------- DM-Löschung ----------------
async def _delete_old_dms(client:discord.Client,msg_id:int):
    msg_key=str(msg_id)
    if msg_key not in sent_dm: return
    for guild in client.guilds:
        for m in guild.members:
            if m.bot: continue
            try:
                async for dm in m.history(limit=50):
                    if dm.id in sent_dm[msg_key]:
                        try: await dm.delete()
                        except: pass
            except: pass
    sent_dm.pop(msg_key,None); save_sent_dm()

# ---------------- Auto-Resend ----------------
async def auto_resend_for_new_member(member:discord.Member):
    if member.bot: return
    now=datetime.now(TZ); sent=0
    for mid,obj in store.items():
        try:
            if obj["guild_id"]!=member.guild.id: continue
            when=datetime.fromisoformat(obj["when_iso"])
            if now>when+timedelta(hours=2): continue
            tr=int(obj.get("target_role_id",0))
            if tr and (r:=member.guild.get_role(tr)) and r not in member.roles: continue
            t=f"**{obj['title']}**\n{when.strftime('%a %d.%m %H:%M')} Europe/Berlin"
            dm=await member.send(t,view=RaidView(int(mid)))
            sent_dm.setdefault(mid,[]).append(dm.id); sent+=1
        except: pass
    if sent: save_sent_dm()
