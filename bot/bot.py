# bot.py
from __future__ import annotations

import os, json, threading, time, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time as time_cls, date as date_cls
from calendar import monthrange
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import tasks
from flask import Flask
from zoneinfo import ZoneInfo

# ======================== Grundkonfig ========================
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TZ = ZoneInfo("Europe/Berlin")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CFG_FILE         = DATA_DIR / "guild_configs.json"
POST_LOG_FILE    = DATA_DIR / "post_log.json"
RSVP_STORE_FILE  = DATA_DIR / "event_rsvp.json"
RSVP_CFG_FILE    = DATA_DIR / "event_rsvp_cfg.json"
SCORE_FILE       = DATA_DIR / "flammenscore.json"
SCORE_CFG_FILE   = DATA_DIR / "flammenscore_cfg.json"
SCORE_META_FILE  = DATA_DIR / "flammenscore_meta.json"

# ======================== Keepalive (Flask) ========================
app = Flask(__name__)

@app.get("/")
def ok():
    return "ok"

def _run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

threading.Thread(target=_run_flask, daemon=True).start()

def keep_alive():
    url = os.getenv("KEEPALIVE_URL", "").strip()
    if not url:
        return
    while True:
        try:
            requests.get(url, timeout=10)
        except Exception as e:
            print("Self-ping failed:", e)
        time.sleep(300)

threading.Thread(target=keep_alive, daemon=True).start()

# ======================== Discord Setup ========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ======================== Parser / Utils ========================
DOW_MAP = {
    "mon":0,"monday":0,"0":0, "tue":1,"tuesday":1,"1":1, "wed":2,"wednesday":2,"2":2,
    "thu":3,"thursday":3,"3":3, "fri":4,"friday":4,"4":4, "sat":5,"saturday":5,"5":5,
    "sun":6,"sunday":6,"6":6
}

def parse_weekdays(s: str) -> List[int]:
    if not s:
        return []
    out=[]
    for p in [x.strip().lower() for x in s.split(",") if x.strip()]:
        if p not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{p}'. Use Mon..Sun or 0..6 (0=Mon).")
        out.append(DOW_MAP[p])
    return sorted(set(out))

def parse_time_hhmm(s: str) -> time_cls:
    try:
        hh, mm = s.strip().split(":")
        return time_cls(int(hh), int(mm))
    except Exception:
        raise ValueError("start_time must be 'HH:MM' 24h.")

def parse_premins(s: str) -> List[int]:
    if not s or not s.strip():
        return []
    mins=[int(p.strip()) for p in s.split(",") if p.strip()]
    return sorted(set([m for m in mins if m>0]))

def parse_date_yyyy_mm_dd(s: str) -> date_cls:
    try:
        y,m,d = s.strip().split("-")
        return date_cls(int(y), int(m), int(d))
    except Exception:
        raise ValueError("date must be 'YYYY-MM-DD'.")

def _now() -> datetime:
    return datetime.now(TZ)

def _in_window(now: datetime, ts: datetime, window_sec: int = 60) -> bool:
    return 0 <= (now - ts).total_seconds() < window_sec

# ======================== Datenmodelle (Events) ========================
@dataclass
class Event:
    name: str
    weekdays: List[int]
    start_hhmm: str
    duration_min: int
    pre_reminders: List[int]
    mention_role_id: Optional[int] = None
    channel_id: Optional[int] = None
    description: str = ""
    one_time_date: Optional[str] = None  # YYYY-MM-DD

    def next_occurrence_start(self, ref_dt: datetime) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            d = parse_date_yyyy_mm_dd(self.one_time_date)
            dt = datetime.combine(d, start_t, tzinfo=TZ)
            return dt if dt >= ref_dt else None
        today = ref_dt.date()
        for add in range(0, 8):
            d = today + timedelta(days=add)
            if d.weekday() in self.weekdays:
                dt = datetime.combine(d, start_t, tzinfo=TZ)
                if dt >= ref_dt:
                    return dt
        return None

    def occurrence_start_on_date(self, date_) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            d = parse_date_yyyy_mm_dd(self.one_time_date)
            if d != date_:
                return None
            return datetime.combine(d, start_t, tzinfo=TZ)
        if date_.weekday() not in self.weekdays:
            return None
        return datetime.combine(date_, start_t, tzinfo=TZ)

@dataclass
class GuildConfig:
    guild_id: int
    announce_channel_id: Optional[int] = None
    events: Dict[str, Event] = None

    def to_dict(self):
        return {
            "guild_id": self.guild_id,
            "announce_channel_id": self.announce_channel_id,
            "events": {k: asdict(v) for k,v in (self.events or {}).items()},
        }

    @staticmethod
    def from_dict(d):
        evs = {k: Event(**v) for k,v in (d.get("events") or {}).items()}
        return GuildConfig(
            guild_id=d["guild_id"],
            announce_channel_id=d.get("announce_channel_id"),
            events=evs,
        )

# ======================== Persistenz ========================
def _load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

rsvp_store: Dict[str, dict] = _load_json(RSVP_STORE_FILE, {})
rsvp_cfg:   Dict[str, dict] = _load_json(RSVP_CFG_FILE, {})
scores:     Dict[str, dict] = _load_json(SCORE_FILE, {})      # {gid:{uid:{...}}}
score_cfg:  Dict[str, dict] = _load_json(SCORE_CFG_FILE, {})  # {gid:{weights...}}
score_meta: Dict[str, dict] = _load_json(SCORE_META_FILE, {}) # {gid:{last_reset_ym:"YYYY-MM"}}

def _save_rsvp():       _save_json(RSVP_STORE_FILE, rsvp_store)
def _save_rsvp_cfg():   _save_json(RSVP_CFG_FILE, rsvp_cfg)
def _save_scores():     _save_json(SCORE_FILE, scores)
def _save_score_cfg():  _save_json(SCORE_CFG_FILE, score_cfg)
def _save_score_meta(): _save_json(SCORE_META_FILE, score_meta)

configs: Dict[int, GuildConfig] = (lambda: {int(g): GuildConfig.from_dict(c) for g,c in (_load_json(CFG_FILE, {}) or {}).items()})()
post_log: Set[str] = set(_load_json(POST_LOG_FILE, []))

def save_all(cfgs: Dict[int, GuildConfig]):
    raw = {str(gid): cfg.to_dict() for gid,cfg in cfgs.items()}
    _save_json(CFG_FILE, raw)

def save_post_log(log: Set[str]):
    _save_json(POST_LOG_FILE, sorted(list(log)))

# ======================== Helper ========================
def get_or_create_guild_cfg(guild_id: int) -> GuildConfig:
    cfg = configs.get(guild_id)
    if not cfg:
        cfg = GuildConfig(guild_id=guild_id, events={})
        configs[guild_id] = cfg
        save_all(configs)
    return cfg

async def ensure_text_channel(guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.TextChannel]:
    if not channel_id:
        return None
    ch = guild.get_channel(channel_id)
    return ch if isinstance(ch, discord.TextChannel) else None

def is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))

# ======================== RSVP / Raid ========================
def _get_role_ids(guild: discord.Guild) -> Dict[str, int]:
    g = rsvp_cfg.get(str(guild.id)) or {}
    return {"TANK": int(g.get("TANK", 0)), "HEAL": int(g.get("HEAL", 0)), "DPS": int(g.get("DPS", 0))}

def _get_guild_role_filter_id(guild_id: int) -> int:
    g = rsvp_cfg.get(str(guild_id)) or {}
    try:
        return int(g.get("GUILD_ROLE", 0))
    except Exception:
        return 0

def _set_guild_role_id(guild_id: int, role_id: int) -> None:
    g = rsvp_cfg.get(str(guild_id)) or {}
    g["GUILD_ROLE"] = int(role_id)
    rsvp_cfg[str(guild_id)] = g
    _save_rsvp_cfg()

def _label_from_member(member: discord.Member) -> str:
    rid = _get_role_ids(member.guild)
    if rid["TANK"] and discord.utils.get(member.roles, id=rid["TANK"]): return "Tank"
    if rid["HEAL"] and discord.utils.get(member.roles, id=rid["HEAL"]): return "Heal"
    if rid["DPS"] and discord.utils.get(member.roles, id=rid["DPS"]): return "DPS"
    names = [r.name.lower() for r in member.roles]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

async def _get_role_member_ids(guild: discord.Guild, role_id: int) -> Set[int]:
    role = guild.get_role(role_id)
    if not role:
        return set()
    cached = {m.id for m in role.members}
    if cached:
        return cached
    ids: Set[int] = set()
    try:
        async for m in guild.fetch_members(limit=None):
            if role in m.roles:
                ids.add(m.id)
    except Exception:
        pass
    return ids

async def _build_embed_async(guild: discord.Guild, obj: dict) -> discord.Embed:
    dt = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"{obj['title']}",
        description=f"{obj.get('description','')}\n\n⏰ Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple(),
    )
    tank_names = [_mention(guild, u) for u in obj["yes"]["TANK"]]
    heal_names = [_mention(guild, u) for u in obj["yes"]["HEAL"]]
    dps_names  = [_mention(guild, u) for u in obj["yes"]["DPS"]]
    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})", value="\n".join(dps_names) or "—", inline=True)

    maybe_lines = []
    for uid, rlab in obj["maybe"].items():
        uid_i = int(uid)
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, u) for u in obj["no"]]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    gr_id = _get_guild_role_filter_id(guild.id)
    if gr_id:
        role_member_ids = await _get_role_member_ids(guild, gr_id)
        if role_member_ids:
            voted_ids = set(
                obj["yes"]["TANK"] + obj["yes"]["HEAL"] + obj["yes"]["DPS"]
                + [int(k) for k in obj["maybe"].keys()] + obj["no"]
            )
            voted_in_guild = len(voted_ids & role_member_ids)
            total = len(role_member_ids)
            pct = int(round((voted_in_guild / total) * 100)) if total else 0
            emb.add_field(name="🏰 Gildenbeteiligung",
                          value=f"{voted_in_guild} / {total} (**{pct}%**)", inline=False)
    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])
    emb.set_footer(text="Klicke unten auf die Buttons, um dich anzumelden.")
    return emb

class RaidView(discord.ui.View):
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _credit_rsvp(self, inter: discord.Interaction):
        gid = inter.guild_id; uid = inter.user.id
        b = _score_bucket(gid, uid)
        if self.msg_id not in b["credited_rsvp"]:
            b["credited_rsvp"].append(self.msg_id)
            b["rsvp"] += 1
            _save_scores()

    async def _update(self, interaction: discord.Interaction, group: str):
        if self.msg_id not in rsvp_store:
            await interaction.response.send_message("Dieses Event ist nicht mehr vorhanden.", ephemeral=True)
            return
        obj = rsvp_store[self.msg_id]
        uid = interaction.user.id

        for k in ("TANK", "HEAL", "DPS"):
            if uid in obj["yes"][k]:
                obj["yes"][k].remove(uid)
        obj["no"] = [u for u in obj["no"] if u != uid]
        obj["maybe"].pop(str(uid), None)

        if group in ("TANK", "HEAL", "DPS"):
            obj["yes"][group].append(uid); txt=f"Angemeldet als **{group}**."
        elif group == "MAYBE":
            obj["maybe"][str(uid)] = _label_from_member(interaction.user); txt="Als **Vielleicht** eingetragen."
        elif group == "NO":
            obj["no"].append(uid); txt="Als **Abgemeldet** eingetragen."
        else:
            txt="Aktualisiert."

        _save_rsvp()
        await self._credit_rsvp(interaction)

        guild = interaction.guild
        emb = await _build_embed_async(guild, obj)
        ch = guild.get_channel(obj["channel_id"])
        try:
            msg = await ch.fetch_message(int(self.msg_id))
            await msg.edit(embed=emb, view=self)
        except Exception:
            pass

        await interaction.response.send_message(txt, ephemeral=True)

    @discord.ui.button(label="Tank", style=discord.ButtonStyle.secondary, emoji="🛡️", custom_id="rsvp_tank")
    async def btn_tank(self, interaction: discord.Interaction, _):
        await self._update(interaction, "TANK")

    @discord.ui.button(label="Heal", style=discord.ButtonStyle.secondary, emoji="💚", custom_id="rsvp_heal")
    async def btn_heal(self, interaction: discord.Interaction, _):
        await self._update(interaction, "HEAL")

    @discord.ui.button(label="DPS", style=discord.ButtonStyle.secondary, emoji="🗡️", custom_id="rsvp_dps")
    async def btn_dps(self, interaction: discord.Interaction, _):
        await self._update(interaction, "DPS")

    @discord.ui.button(label="Vielleicht", style=discord.ButtonStyle.secondary, emoji="❔", custom_id="rsvp_maybe")
    async def btn_maybe(self, interaction: discord.Interaction, _):
        await self._update(interaction, "MAYBE")

    @discord.ui.button(label="Abmelden", style=discord.ButtonStyle.danger, emoji="❌", custom_id="rsvp_no")
    async def btn_no(self, interaction: discord.Interaction, _):
        await self._update(interaction, "NO")

def register_rsvp_slash_commands():
    @tree.command(name="raid_set_roles", description="Rollen für Tank/Heal/DPS festlegen (pro Server).")
    @app_commands.describe(tank_role="Rolle für Tank", heal_role="Rolle für Heal", dps_role="Rolle für DPS")
    async def raid_set_roles(inter: discord.Interaction, tank_role: discord.Role, heal_role: discord.Role, dps_role: discord.Role):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        rsvp_cfg[str(inter.guild_id)] = {"TANK": tank_role.id, "HEAL": heal_role.id, "DPS": dps_role.id}
        _save_rsvp_cfg()
        await inter.response.send_message(f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}", ephemeral=True)

    @tree.command(name="raid_create", description="Raid-/Event-Anmeldung mit Buttons erstellen.")
    @app_commands.describe(title="Titel", date="YYYY-MM-DD", time="HH:MM", channel="Zielkanal", image_url="Bild-URL", description="Info")
    async def raid_create(inter: discord.Interaction, title: str, date: str, time: str,
                          channel: Optional[discord.TextChannel] = None, image_url: Optional[str] = None,
                          description: Optional[str] = None):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        try:
            yyyy, mm, dd = [int(x) for x in date.split("-")]
            hh, mi = [int(x) for x in time.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig (YYYY-MM-DD / HH:MM).", ephemeral=True); return

        ch = channel or inter.channel
        obj = {
            "guild_id": inter.guild_id, "channel_id": ch.id, "title": title.strip(),
            "description": (description or "").strip(), "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": []}, "maybe": {}, "no": []
        }
        emb = await _build_embed_async(inter.guild, obj)
        view = RaidView(0)
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)
        rsvp_store[str(msg.id)] = obj; _save_rsvp()
        inter.client.add_view(RaidView(msg.id), message_id=msg.id)
        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}", ephemeral=True)

    @tree.command(name="raid_show", description="Embed/Listen neu aufbauen.")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_show(inter: discord.Interaction, message_id: str):
        if message_id not in rsvp_store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        obj = rsvp_store[message_id]
        emb = await _build_embed_async(inter.guild, obj)
        ch = inter.guild.get_channel(obj["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(embed=emb, view=RaidView(int(message_id)))
            await inter.response.send_message("✅ Aktualisiert.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    @tree.command(name="raid_close", description="Buttons sperren (nur Admin).")
    @app_commands.describe(message_id="ID der Raid-Nachricht")
    async def raid_close(inter: discord.Interaction, message_id: str):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        if message_id not in rsvp_store:
            await inter.response.send_message("❌ Unbekannte message_id.", ephemeral=True); return
        ch = inter.guild.get_channel(rsvp_store[message_id]["channel_id"])
        try:
            msg = await ch.fetch_message(int(message_id)); await msg.edit(view=None)
            await inter.response.send_message("🔒 Gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    @tree.command(name="raid_set_guildrole", description="Gildenrolle für Quote/Filter setzen.")
    @app_commands.describe(guild_role='Rolle, deren Mitglieder gezählt werden sollen')
    async def raid_set_guildrole(inter: discord.Interaction, guild_role: discord.Role):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        _set_guild_role_id(inter.guild_id, guild_role.id)
        await inter.response.send_message(f"✅ Gildenrolle gesetzt: {guild_role.mention}", ephemeral=True)

# ======================== Flammenscore ========================
WEIGHTS_DEFAULT = {"voice_min":0.20,"message":0.50,"react_given":0.20,"react_recv":0.30,"rsvp":3.00}

def _guild_weights(gid: int) -> Dict[str,float]:
    return {**WEIGHTS_DEFAULT, **(score_cfg.get(str(gid)) or {})}

def _score_bucket(gid: int, uid: int) -> dict:
    g = scores.setdefault(str(gid), {})
    return g.setdefault(str(uid), {"voice_ms":0,"messages":0,"reacts_given":0,"reacts_recv":0,"rsvp":0,"credited_rsvp":[]})

def _calc_flammenscore(gid: int, uid: int) -> Tuple[float, Dict[str,float]]:
    u=_score_bucket(gid,uid); w=_guild_weights(gid)
    voice_min=u["voice_ms"]/60000.0
    parts={"voice":voice_min*w["voice_min"],"msg":u["messages"]*w["message"],
           "rg":u["reacts_given"]*w["react_given"],"rr":u["reacts_recv"]*w["react_recv"],"rsvp":u["rsvp"]*w["rsvp"]}
    return sum(parts.values()), parts

# ---- Live-Voice ----
voice_sessions: Dict[Tuple[int,int], datetime] = {}
message_author_cache: Dict[int,int] = {}

def _cache_author(message_id: int, author_id: int, cap: int = 2000):
    if len(message_author_cache) >= cap:
        message_author_cache.pop(next(iter(message_author_cache)))
    message_author_cache[message_id] = author_id

# ---- Top-Ermittlung + Rendering ----
def _collect_sorted_totals(guild: discord.Guild) -> List[Tuple[float,int]]:
    gid = guild.id
    data = scores.get(str(gid)) or {}
    gr_id = _get_guild_role_filter_id(gid)
    gr = guild.get_role(gr_id) if gr_id else None
    arr: List[Tuple[float,int]] = []
    for uid_str in data.keys():
        uid = int(uid_str)
        m = guild.get_member(uid)
        if not m or m.bot:
            continue
        if gr and gr not in m.roles:
            continue
        total,_ = _calc_flammenscore(gid, uid)
        arr.append((total, uid))
    arr.sort(reverse=True, key=lambda x: x[0])
    return arr

def _render_top_table(guild: discord.Guild, limit: int = 10) -> str:
    rows = _collect_sorted_totals(guild)[:limit]
    if not rows:
        return "Noch keine Daten."
    top = rows[0][0] or 1.0
    # dynamische Breite für Namen
    names = []
    for _, uid in rows:
        m = guild.get_member(uid)
        names.append(m.display_name if m else f"<@{uid}>")
    name_w = max(6, min(28, max(len(n) for n in names)))
    header = f"{'#':>2}  {'Name':<{name_w}}  {'Flammen':>8}"
    sep    = "-" * (len(header))
    lines  = [header, sep]
    for i, ((total, uid), name) in enumerate(zip(rows, names), start=1):
        pct = total/top*100.0
        lines.append(f"{i:>2}  {name[:name_w]:<{name_w}}  {pct:>6.1f}%")
    return "```\n" + "\n".join(lines) + "\n```"

# ======================== Scheduler ========================
@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = _now().replace(second=0, microsecond=0)

    # 0) Voice-Tick
    changed=False
    for (gid, uid), start in list(voice_sessions.items()):
        delta = (_now() - start).total_seconds()
        if delta >= 30:
            b=_score_bucket(gid, uid)
            b["voice_ms"] += int(delta*1000)
            voice_sessions[(gid, uid)] = _now()
            changed=True
    if changed:
        _save_scores()

    # 1) Event-Reminder
    changed_log=False
    for guild in client.guilds:
        cfg = configs.get(guild.id)
        if not cfg or not cfg.events: continue
        for ev in list(cfg.events.values()):
            channel_id = ev.channel_id or cfg.announce_channel_id
            channel = await ensure_text_channel(guild, channel_id)
            if not channel: continue
            start_dt = ev.occurrence_start_on_date(now.date())
            if not start_dt: continue
            end_dt = start_dt + timedelta(minutes=ev.duration_min)

            for m in ev.pre_reminders:
                pre_dt = start_dt - timedelta(minutes=m)
                key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:pre{m}"
                if _in_window(now, pre_dt) and key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    body = f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip()
                    if ev.description: body += f"\n{ev.description}"
                    await channel.send(body)
                    post_log.add(key); changed_log=True

            key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
            if _in_window(now, start_dt) and key not in post_log:
                role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                if ev.description: body += f"\n{ev.description}"
                await channel.send(body)
                post_log.add(key); changed_log=True

            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        del cfg.events[ev.name.lower()]; save_all(configs)
                except Exception:
                    pass
    if changed_log:
        save_post_log(post_log)

    # 2) Wochen-Leaderboard Fr 18:00
    if now.weekday()==4 and now.hour==18 and now.minute==0:
        for guild in client.guilds:
            cfg = configs.get(guild.id)
            if not cfg or not cfg.announce_channel_id: continue
            ch = guild.get_channel(cfg.announce_channel_id)
            if not isinstance(ch, discord.TextChannel): continue
            key = f"weekly_lb:{guild.id}:{now.date().isoformat()}"
            if key in post_log: continue
            table = _render_top_table(guild, 10)
            emb = discord.Embed(title="🔥 Flammen – Wochen-Topliste", color=discord.Color.orange())
            emb.description = table
            emb.set_footer(text=f"Stand: {now.strftime('%d.%m.%Y %H:%M')} • Reset am Monatsende")
            try:
                await ch.send(embed=emb); post_log.add(key); save_post_log(post_log)
            except Exception as e:
                print("weekly leaderboard post failed:", e)

    # 3) Monatsreset (EOM 00:00)
    if now.day == monthrange(now.year, now.month)[1] and now.hour==0 and now.minute==0:
        ym = now.strftime("%Y-%m")
        for g in client.guilds:
            last = (score_meta.get(str(g.id)) or {}).get("last_reset_ym","")
            if last == ym: continue
            scores[str(g.id)] = {}; _save_scores()
            score_meta.setdefault(str(g.id),{})["last_reset_ym"]=ym; _save_score_meta()

@scheduler_loop.before_loop
async def _before_scheduler(): await client.wait_until_ready()

# ======================== Score-Hooks ========================
@client.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot: return
    _score_bucket(message.guild.id, message.author.id)["messages"] += 1
    _save_scores()
    _cache_author(message.id, message.author.id)

@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id == client.user.id: return
    _score_bucket(payload.guild_id, payload.user_id)["reacts_given"] += 1
    author_id = message_author_cache.get(payload.message_id)
    if author_id is None:
        try:
            ch = client.get_channel(payload.channel_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                msg = await ch.fetch_message(payload.message_id)
                author_id = msg.author.id; _cache_author(payload.message_id, author_id)
        except Exception:
            author_id = None
    if author_id and author_id != payload.user_id:
        _score_bucket(payload.guild_id, author_id)["reacts_recv"] += 1
    _save_scores()

@client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id == client.user.id: return
    b = _score_bucket(payload.guild_id, payload.user_id)
    if b["reacts_given"] > 0: b["reacts_given"] -= 1
    author_id = message_author_cache.get(payload.message_id)
    if author_id and author_id != payload.user_id:
        br = _score_bucket(payload.guild_id, author_id)
        if br["reacts_recv"] > 0: br["reacts_recv"] -= 1
    _save_scores()

@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or not member.guild: return
    gid, uid = member.guild.id, member.id
    if before.channel is None and after.channel is not None:
        voice_sessions[(gid,uid)] = _now()            # join
    elif before.channel is not None and after.channel is None:
        start = voice_sessions.pop((gid,uid), None)   # leave
        if start:
            _score_bucket(gid,uid)["voice_ms"] += int((_now()-start).total_seconds()*1000); _save_scores()
    elif before.channel and after.channel and before.channel.id != after.channel.id:
        start = voice_sessions.pop((gid,uid), None)   # move
        if start:
            _score_bucket(gid,uid)["voice_ms"] += int((_now()-start).total_seconds()*1000); _save_scores()
        voice_sessions[(gid,uid)] = _now()

# ======================== Core Slash Commands (guild) ========================
def register_core_commands():
    @tree.command(name="set_announce_channel", description="Standard-Kanal für Erinnerungen setzen.")
    @app_commands.describe(channel="Ziel-Textkanal")
    async def set_announce_channel(inter: discord.Interaction, channel: discord.TextChannel):
        if not is_admin(inter):
            await inter.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True); return
        cfg = get_or_create_guild_cfg(inter.guild_id)
        cfg.announce_channel_id = channel.id; save_all(configs)
        await inter.response.send_message(f"✅ Standard-Kanal: {channel.mention}", ephemeral=True)

    @tree.command(name="add_event", description="Event anlegen (wiederkehrend ODER einmalig).")
    @app_commands.describe(
        name="Event-Name",
        weekdays='Kommagetrennt: "Mon,Wed,Sat" oder "0,3,5" (0=Mon..6=Sun). Ignoriert, wenn date gesetzt ist.',
        start_time='Start "HH:MM" (Europa/Berlin)', duration_min="Dauer in Minuten",
        pre_reminders='Vorab-Minuten, z. B. "30,10,5" (optional)',
        mention_role="(optional) Rolle für Ping)", post_channel="(optional) Kanal (sonst Standard)",
        description="(optional) Zusatztext", date='(optional) Einmalig: "YYYY-MM-DD"'
    )
    async def add_event(inter: discord.Interaction, name: str, weekdays: str, start_time: str, duration_min: int,
                        pre_reminders: str = "", mention_role: Optional[discord.Role] = None,
                        post_channel: Optional[discord.TextChannel] = None, description: str = "", date: str = ""):
        if not is_admin(inter):
            await inter.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True); return
        try:
            t = parse_time_hhmm(start_time)
            pre = parse_premins(pre_reminders or "")
            one_time_date = None; days: List[int] = []
            if date.strip():
                one_time_date = parse_date_yyyy_mm_dd(date.strip()).isoformat()
            else:
                days = parse_weekdays(weekdays)
                if not days:
                    await inter.response.send_message("❌ Entweder 'weekdays' angeben ODER 'date' setzen.", ephemeral=True); return
        except ValueError as e:
            await inter.response.send_message(f"❌ {e}", ephemeral=True); return
        cfg = get_or_create_guild_cfg(inter.guild_id)
        ev = Event(name=name.strip(), weekdays=days, start_hhmm=f"{t.hour:02d}:{t.minute:02d}", duration_min=duration_min,
                   pre_reminders=pre, mention_role_id=(mention_role.id if mention_role else None),
                   channel_id=(post_channel.id if post_channel else None), description=(description or "").strip(),
                   one_time_date=one_time_date)
        cfg.events[name.lower()] = ev; save_all(configs)
        when = one_time_date if one_time_date else ",".join(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d] for d in ev.weekdays)
        target = post_channel.mention if post_channel else (inter.guild.get_channel(cfg.announce_channel_id).mention if cfg.announce_channel_id else "—")
        await inter.response.send_message(f"✅ Event **{ev.name}** angelegt → {when} {ev.start_hhmm}, {ev.duration_min} Min, Kanal {target}.", ephemeral=True)

    @tree.command(name="list_events", description="Alle Events anzeigen.")
    async def list_events(inter: discord.Interaction):
        cfg = get_or_create_guild_cfg(inter.guild_id)
        if not cfg.events:
            await inter.response.send_message("ℹ️ Keine Events konfiguriert.", ephemeral=True); return
        dow_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        lines=[]
        for ev in cfg.events.values():
            when = ev.one_time_date if ev.one_time_date else ",".join(dow_names[d] for d in ev.weekdays)
            role = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else "—"
            chan = f"<#{ev.channel_id}>" if ev.channel_id else (f"<#{cfg.announce_channel_id}>" if cfg.announce_channel_id else "—")
            pre = ", ".join(str(m) for m in ev.pre_reminders) if ev.pre_reminders else "—"
            desc = (ev.description[:60] + "…") if ev.description and len(ev.description) > 60 else (ev.description or "—")
            lines.append(f"• **{ev.name}** — {when} {ev.start_hhmm} ({ev.duration_min} Min), Pre: {pre}, Role: {role}, Channel: {chan}, Desc: {desc}")
        await inter.response.send_message("\n".join(lines)[:1990], ephemeral=True)

    @tree.command(name="remove_event", description="Event löschen.")
    async def remove_event(inter: discord.Interaction, name: str):
        if not is_admin(inter):
            await inter.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True); return
        cfg = get_or_create_guild_cfg(inter.guild_id)
        if name.lower() in cfg.events:
            del cfg.events[name.lower()]; save_all(configs)
            await inter.response.send_message(f"✅ Event **{name}** gelöscht.", ephemeral=True)
        else:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True)

    @tree.command(name="test_event_ping", description="Test-Ping (keine Planung).")
    async def test_event_ping(inter: discord.Interaction, name: str):
        cfg = get_or_create_guild_cfg(inter.guild_id)
        ev = cfg.events.get(name.lower())
        if not ev:
            await inter.response.send_message("❌ Event nicht gefunden.", ephemeral=True); return
        channel = await ensure_text_channel(inter.guild, ev.channel_id or cfg.announce_channel_id)
        if not channel:
            await inter.response.send_message("❌ Kein Zielkanal. Setze /set_announce_channel.", ephemeral=True); return
        role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
        body = f"🔔 **{ev.name}** — Test-Ping {role_mention}".strip()
        if ev.description: body += f"\n{ev.description}"
        await channel.send(body); await inter.response.send_message("✅ Test-Ping raus.", ephemeral=True)

    # ---- Score-Befehle ----
    @tree.command(name="flammenscore_me", description="Zeigt deinen Flammenscore und Rang.")
    async def flammenscore_me(inter: discord.Interaction):
        gid = inter.guild_id; uid = inter.user.id
        data = scores.get(str(gid)) or {}
        all_totals=[_calc_flammenscore(gid,int(u))[0] for u in data.keys()]
        my_total, parts = _calc_flammenscore(gid, uid)
        rank = (sorted(all_totals, reverse=True).index(my_total)+1) if (my_total>0 and all_totals) else 0
        lines=[f"**Rang:** {rank}/{len(all_totals)}" if rank else f"**Rang:** –/{len(all_totals)}",
               f"**Score:** {my_total:.1f}",
               f"• Voice: {parts['voice']:.1f}", f"• Messages: {parts['msg']:.1f}",
               f"• Reaktionen gegeben: {parts['rg']:.1f}", f"• Reaktionen erhalten: {parts['rr']:.1f}",
               f"• RSVP: {parts['rsvp']:.1f}"]
        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @tree.command(name="flammenscore_top", description="Topliste (Platz · Name · Flammen%).")
    @app_commands.describe(limit="Anzahl Einträge (1–25, Standard 10)")
    async def flammenscore_top(inter: discord.Interaction, limit: Optional[int]=10):
        limit=max(1,min(25,limit or 10))
        table=_render_top_table(inter.guild, limit)
        emb = discord.Embed(title="🔥 Flammen – Topliste", color=discord.Color.orange())
        emb.description = table
        await inter.response.send_message(embed=emb, ephemeral=True)

    # ---- Admin: Sync (guild-only) ----
    @tree.command(name="wf_admin_sync", description="Befehle in diesem Server sauber neu syncen.")
    async def wf_admin_sync(inter: discord.Interaction):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        try:
            guild_obj = discord.Object(id=inter.guild_id)
            tree.clear_commands(guild=guild_obj)
            await tree.sync(guild=guild_obj)
            await inter.response.send_message("✅ Guild-Sync erledigt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Sync-Fehler: {e}", ephemeral=True)

# ======================== Persistent Views + Lifecycle ========================
def reregister_persistent_views_on_start():
    for msg_id, obj in list(rsvp_store.items()):
        g = client.get_guild(obj["guild_id"])
        if not g: continue
        try:
            client.add_view(RaidView(int(msg_id)), message_id=int(msg_id))
        except Exception as e:
            print("add_view (RSVP) failed:", e)

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    for g in client.guilds:
        try: await g.chunk()
        except Exception as e: print("guild.chunk() failed:", e)

    # Kein Global-Geraffel -> keine Duplikate
    try:
        tree.clear_commands(); await tree.sync()
        for g in client.guilds:
            guild_obj = discord.Object(id=g.id)
            tree.clear_commands(guild=guild_obj)
            await tree.sync(guild=guild_obj)
    except Exception as e:
        print("Initial prune failed:", e)

    register_core_commands()
    register_rsvp_slash_commands()

    try:
        for g in client.guilds:
            await tree.sync(guild=discord.Object(id=g.id))
        print(f"Synced commands for {len(client.guilds)} guild(s).")
    except Exception as e:
        print("Command sync failed:", e)

    reregister_persistent_views_on_start()
    scheduler_loop.start()

# ======================== Start ========================
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
