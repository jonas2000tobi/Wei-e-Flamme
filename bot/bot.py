# bot.py
from __future__ import annotations

import os, json, threading, time, requests, re, random
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, time as time_cls, date as date_cls
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Literal

import discord
from discord import app_commands
from discord.ext import tasks
from flask import Flask
from zoneinfo import ZoneInfo

# ======================== Grundkonfig ========================
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TZ = ZoneInfo("Europe/Berlin")
DEBUG = os.getenv("DEBUG_SCORE", "0") == "1"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CFG_FILE          = DATA_DIR / "guild_configs.json"
POST_LOG_FILE     = DATA_DIR / "post_log.json"
RSVP_STORE_FILE   = DATA_DIR / "event_rsvp.json"
RSVP_CFG_FILE     = DATA_DIR / "event_rsvp_cfg.json"
SCORE_FILE        = DATA_DIR / "flammenscore.json"
SCORE_CFG_FILE    = DATA_DIR / "flammenscore_cfg.json"
SCORE_META_FILE   = DATA_DIR / "flammenscore_meta.json"
ONBOARD_META_FILE = DATA_DIR / "onboarding_meta.json"
# WFWorld Persistenz
WFWORLD_FILE      = DATA_DIR / "wfworld.json"   # {gid:{users:{uid:{...}}, casino:{...}}}

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
intents.voice_states = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ======================== Parser / Utils ========================
TEXT_DAY_MAP = {
    "mo":0,"montag":0,"mon":0,"monday":0,"0":0,"1":0,
    "di":1,"dienstag":1,"tue":1,"tues":1,"tuesday":1,"2":1,
    "mi":2,"mittwoch":2,"wed":2,"wednesday":2,"3":2,
    "do":3,"donnerstag":3,"thu":3,"thur":3,"thurs":3,"thursday":3,"4":3,
    "fr":4,"freitag":4,"fri":4,"friday":4,"5":4,
    "sa":5,"samstag":5,"sat":5,"saturday":5,"6":5,
    "so":6,"sonntag":6,"sun":6,"sunday":6,"7":6
}
ALL_DAY_TOKENS = {"all","alle","daily","everyday","taeglich","täglich","jeden","mo-so","mon-sun","1-7","0-6"}
WEEKDAY_TOKENS = {"werktag","werktage","weekdays","mo-fr","mon-fri","1-5","0-4"}
GER_SHORT = ["Mo","Di","Mi","Do","Fr","Sa","So"]

def fmt_weekdays(days: List[int]) -> str:
    return ",".join(GER_SHORT[d] for d in sorted(set(days)))

def _tok_split(s: str) -> List[str]:
    return [t for t in re.split(r"[,\;/\s]+", s.strip()) if t]

def _map_token_to_day(tok: str) -> int:
    t = tok.strip().lower()
    if t in TEXT_DAY_MAP:
        return TEXT_DAY_MAP[t]
    if t.isdigit():
        n=int(t)
        if n==0: return 0
        if 1<=n<=7: return n-1
    raise ValueError(f"Unbekannter Wochentag-Token: '{tok}'")

def parse_weekdays(s: str) -> List[int]:
    if not s or not s.strip(): return []
    parts=_tok_split(s)
    if any(p.lower() in ALL_DAY_TOKENS for p in parts): return list(range(7))
    if any(p.lower() in WEEKDAY_TOKENS for p in parts): return list(range(5))
    out:Set[int]=set()
    for p in parts:
        pl=p.lower()
        if "-" in pl:
            a,b=pl.split("-",1); ai=_map_token_to_day(a); bi=_map_token_to_day(b)
            if ai<=bi:
                for d in range(ai,bi+1): out.add(d)
            else:
                for d in list(range(ai,7))+list(range(0,bi+1)): out.add(d)
        else:
            out.add(_map_token_to_day(pl))
    return sorted(out)

def parse_time_hhmm(s: str) -> time_cls:
    try:
        hh, mm = s.strip().split(":")
        return time_cls(int(hh), int(mm))
    except Exception:
        raise ValueError("start_time must be 'HH:MM' 24h.")

def parse_premins(s: str) -> List[int]:
    if not s or not s.strip(): return []
    mins=[int(p.strip()) for p in re.split(r"[,\s;]+", s) if p.strip()]
    return sorted(set([m for m in mins if m>0]))

def parse_date_yyyy_mm_dd(s: str) -> date_cls:
    try:
        y,m,d = s.strip().split("-")
        return date_cls(int(y), int(m), int(d))
    except Exception:
        raise ValueError("date must be 'YYYY-MM-DD'.")

def parse_options_block(s: str) -> Tuple[List[int], str]:
    """1. Zeile: Vorwarnungen (30,10,5); ab Zeile 2: Beschreibung.
       Alternativ: pre=30,10,5; desc=Text
    """
    if not s or not s.strip(): return [], ""
    txt=s.strip()
    m_pre = re.search(r"(?i)\bpre\s*=\s*([0-9,\s;]+)", txt)
    m_desc= re.search(r"(?i)\b(desc|beschreibung)\s*=\s*(.+)", txt, flags=re.S)
    if m_pre or m_desc:
        pre=parse_premins(m_pre.group(1)) if m_pre else []
        desc=m_desc.group(2).strip() if m_desc else ""
        return pre, desc
    lines=txt.splitlines()
    pre=parse_premins(lines[0]) if lines else []
    desc="\n".join(lines[1:]).strip() if len(lines)>1 else ""
    return pre, desc

def _now() -> datetime: return datetime.now(TZ)
def _in_window(now: datetime, ts: datetime, window_sec: int = 60) -> bool:
    return 0 <= (now - ts).total_seconds() < window_sec
def _ephemeral_ok(inter: discord.Interaction) -> bool: return inter.guild_id is not None

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
            if d != date_: return None
            return datetime.combine(d, start_t, tzinfo=TZ)
        if date_.weekday() not in self.weekdays: return None
        return datetime.combine(date_, start_t, tzinfo=TZ)

@dataclass
class GuildConfig:
    guild_id: int
    announce_channel_id: Optional[int] = None
    events: Dict[str, Event] = field(default_factory=dict)

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

# Stores
rsvp_store: Dict[str, dict] = _load_json(RSVP_STORE_FILE, {})
rsvp_cfg:   Dict[str, dict] = _load_json(RSVP_CFG_FILE, {})
scores:     Dict[str, dict] = _load_json(SCORE_FILE, {})
score_cfg:  Dict[str, dict] = _load_json(SCORE_CFG_FILE, {})
score_meta: Dict[str, dict] = _load_json(SCORE_META_FILE, {})
onboard_meta: Dict[str, dict] = _load_json(ONBOARD_META_FILE, {})
# WFWorld
wfworld: Dict[str, dict] = _load_json(WFWORLD_FILE, {})

def _save_rsvp():       _save_json(RSVP_STORE_FILE, rsvp_store)
def _save_rsvp_cfg():   _save_json(RSVP_CFG_FILE, rsvp_cfg)
def _save_scores():     _save_json(SCORE_FILE, scores)
def _save_score_cfg():  _save_json(SCORE_CFG_FILE, score_cfg)
def _save_score_meta(): _save_json(SCORE_META_FILE, score_meta)
def _save_onboard_meta(): _save_json(ONBOARD_META_FILE, onboard_meta)
def _save_wfworld():    _save_json(WFWORLD_FILE, wfworld)

def load_all() -> Dict[int, GuildConfig]:
    raw = _load_json(CFG_FILE, {})
    return {int(gid): GuildConfig.from_dict(cfg) for gid,cfg in raw.items()} if raw else {}

def save_all(cfgs: Dict[int, GuildConfig]):
    raw = {str(gid): cfg.to_dict() for gid,cfg in cfgs.items()}
    _save_json(CFG_FILE, raw)

def load_post_log() -> Set[str]:
    return set(_load_json(POST_LOG_FILE, []))

def save_post_log(log: Set[str]):
    _save_json(POST_LOG_FILE, sorted(list(log)))

configs: Dict[int, GuildConfig] = load_all()
post_log: Set[str] = load_post_log()

# ======================== Helper ========================
def get_or_create_guild_cfg(guild_id: int) -> GuildConfig:
    cfg = configs.get(guild_id)
    if not cfg:
        cfg = GuildConfig(guild_id=guild_id, events={})
        configs[guild_id] = cfg
        save_all(configs)
    return cfg

async def ensure_text_channel(guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.TextChannel]:
    if not channel_id: return None
    ch = guild.get_channel(channel_id)
    return ch if isinstance(ch, discord.TextChannel) else None

def is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

# ======================== RSVP / Raid ========================
def _get_role_ids(guild: discord.Guild) -> Dict[str, int]:
    g = rsvp_cfg.get(str(guild.id)) or {}
    return {"TANK": int(g.get("TANK", 0) or 0),
            "HEAL": int(g.get("HEAL", 0) or 0),
            "DPS":  int(g.get("DPS",  0) or 0)}

def _get_guild_role_filter_id(guild_id: int) -> int:
    g = rsvp_cfg.get(str(guild_id)) or {}
    try: return int(g.get("GUILD_ROLE", 0))
    except Exception: return 0

def _set_guild_role_id(guild_id: int, role_id: int) -> None:
    g = rsvp_cfg.get(str(guild_id)) or {}
    g["GUILD_ROLE"] = int(role_id)
    rsvp_cfg[str(guild_id)] = g
    _save_rsvp_cfg()

def _set_role_ids(gid: int, tank_id: int, heal_id: int, dps_id: int):
    g = rsvp_cfg.get(str(gid)) or {}
    g["TANK"] = int(tank_id); g["HEAL"] = int(heal_id); g["DPS"] = int(dps_id)
    rsvp_cfg[str(gid)] = g
    _save_rsvp_cfg()

async def _get_role_member_ids(guild: discord.Guild, role_id: int) -> Set[int]:
    role = guild.get_role(role_id)
    if not role: return set()
    cached = {m.id for m in role.members}
    if cached: return cached
    ids: Set[int] = set()
    try:
        async for m in guild.fetch_members(limit=None):
            if role in m.roles: ids.add(m.id)
    except Exception: pass
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
    if obj.get("image_url"): emb.set_image(url=obj["image_url"])
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
            obj["yes"][group].append(uid)
            txt = f"Angemeldet als **{group}**."
        elif group == "MAYBE":
            names = [r.name.lower() for r in interaction.user.roles]
            rlab = "Tank" if any("tank" in n for n in names) else ("Heal" if any("heal" in n for n in names) else ("DPS" if any(("dps" in n) or ("dd" in n) for n in names) else ""))
            obj["maybe"][str(uid)] = rlab
            txt = "Als **Vielleicht** eingetragen."
        elif group == "NO":
            obj["no"].append(uid)
            txt = "Als **Abgemeldet** eingetragen."
        else:
            txt = "Aktualisiert."

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

# ======================== Flammenscore ========================
WEIGHTS_DEFAULT = {
    "voice_min":   0.20,
    "message":     0.50,
    "react_given": 0.20,
    "react_recv":  0.30,
    "rsvp":        3.00
}

def _guild_weights(gid: int) -> Dict[str, float]:
    g = score_cfg.get(str(gid)) or {}
    return {**WEIGHTS_DEFAULT, **g}

def _score_bucket(gid: int, uid: int) -> dict:
    g = scores.setdefault(str(gid), {})
    u = g.setdefault(str(uid), {
        "voice_ms": 0,
        "messages": 0,
        "reacts_given": 0,
        "reacts_recv": 0,
        "rsvp": 0,
        "credited_rsvp": []
    })
    return u

def _calc_flammenscore(gid: int, uid: int) -> Tuple[float, Dict[str, float]]:
    u = _score_bucket(gid, uid)
    w = _guild_weights(gid)
    voice_min = u["voice_ms"] / 60000.0
    parts = {
        "voice": voice_min * w["voice_min"],
        "msg":   u["messages"]     * w["message"],
        "rg":    u["reacts_given"] * w["react_given"],
        "rr":    u["reacts_recv"]  * w["react_recv"],
        "rsvp":  u["rsvp"]         * w["rsvp"],
    }
    total = sum(parts.values())
    return total, parts

voice_sessions: Dict[Tuple[int,int], datetime] = {}
def _voice_start(gid: int, uid: int):
    voice_sessions[(gid, uid)] = _now()
def _voice_end(gid: int, uid: int):
    start = voice_sessions.pop((gid, uid), None)
    if start:
        delta_ms = int((_now() - start).total_seconds() * 1000)
        b = _score_bucket(gid, uid)
        b["voice_ms"] += max(0, delta_ms)
        _save_scores()

async def _seed_voice_sessions():
    for g in client.guilds:
        for ch in g.voice_channels:
            for m in ch.members:
                if not m.bot:
                    _voice_start(g.id, m.id)

message_author_cache: Dict[int, int] = {}
def _cache_author(message_id: int, author_id: int, cap: int = 4000):
    if len(message_author_cache) >= cap:
        message_author_cache.pop(next(iter(message_author_cache)))
    message_author_cache[message_id] = author_id

def _format_leaderboard_lines_simple(guild: discord.Guild, limit: int = 10) -> List[str]:
    gid=guild.id; data=scores.get(str(gid)) or {}
    if not data: return []
    scored=[]
    for uid_str in data.keys():
        uid=int(uid_str); total,_=_calc_flammenscore(gid,uid)
        scored.append((uid,total))
    scored.sort(key=lambda t:t[1], reverse=True)
    total_possible = sum(t for _,t in scored) or 1e-9
    lines=[]
    for i,(uid,total) in enumerate(scored[:limit], start=1):
        m=guild.get_member(uid); name=m.display_name if m else f"<@{uid}>"
        pct=(total/total_possible)*100.0
        lines.append(f"{i}. {name} — {pct:.1f}%")
    return lines

async def _post_weekly_leaderboard_if_due(now: datetime):
    if not (now.weekday()==4 and now.hour==18 and now.minute==0): return
    for guild in client.guilds:
        cfg=configs.get(guild.id)
        if not cfg or not cfg.announce_channel_id: continue
        ch=guild.get_channel(cfg.announce_channel_id)
        if not isinstance(ch, discord.TextChannel): continue
        key=f"weekly_lb:{guild.id}:{now.date().isoformat()}"
        if key in post_log: continue
        lines=_format_leaderboard_lines_simple(guild, limit=10)
        if not lines: continue
        emb=discord.Embed(title="🔥 Flammenscore – Wochen-Topliste",
                          description="\n".join(lines),
                          color=discord.Color.orange())
        emb.set_footer(text=f"Stand: {now.strftime('%d.%m.%Y %H:%M')} • Reset am 30. jeden Monats")
        try:
            await ch.send(embed=emb)
            post_log.add(key); save_post_log(post_log)
        except Exception as e:
            print("weekly leaderboard post failed:", e)

def _month_key(dt: datetime) -> str: return dt.strftime("%Y-%m")
def _get_last_reset_ym(gid: int) -> str: return (score_meta.get(str(gid)) or {}).get("last_reset_ym","")
def _set_last_reset_ym(gid: int, ym: str):
    g = score_meta.get(str(gid)) or {}
    g["last_reset_ym"]=ym; score_meta[str(gid)]=g; _save_score_meta()

async def _monthly_reset_if_due(now: datetime):
    if not (now.day == 30 and now.hour == 0 and now.minute == 0): return
    ym = _month_key(now)
    for guild in client.guilds:
        last = _get_last_reset_ym(guild.id)
        if last == ym: continue
        scores[str(guild.id)] = {}; _save_scores(); _set_last_reset_ym(guild.id, ym)

# ======================== ONBOARDING ========================
pending_onboarding: Dict[Tuple[int,int], dict] = {}
def _meta_g(gid: int) -> dict: return onboard_meta.get(str(gid)) or {}
def _set_meta(gid: int, **kwargs):
    m = onboard_meta.get(str(gid)) or {}
    m.update({k:v for k,v in kwargs.items() if v is not None})
    onboard_meta[str(gid)] = m; _save_onboard_meta()
def _get_newbie_role_id(gid: int) -> int:
    try: return int(_meta_g(gid).get("newbie_role_id", 0))
    except Exception: return 0
def _set_newbie_role_id(gid: int, role_id: int): _set_meta(gid, newbie_role_id=int(role_id))

class OnboardView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id; self.user_id = user_id
        self.primary: Optional[str] = None; self.exp: Optional[str] = None
        self.rules_ok = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.select(placeholder="Wähle deine Primärrolle", min_values=1, max_values=1, options=[
        discord.SelectOption(label="DD", description="Schaden", emoji="🗡️"),
        discord.SelectOption(label="Tank", description="Frontline", emoji="🛡️"),
        discord.SelectOption(label="Heal", description="Support", emoji="💚"),
    ])
    async def select_primary(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.primary = select.values[0]
        await interaction.response.send_message(f"Primärrolle: **{self.primary}**", ephemeral=_ephemeral_ok(interaction))

    @discord.ui.select(placeholder="Erfahrung", min_values=1, max_values=1, options=[
        discord.SelectOption(label="Erfahren", emoji="🔥"),
        discord.SelectOption(label="Unerfahren", emoji="🌱"),
    ])
    async def select_exp(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.exp = select.values[0]
        await interaction.response.send_message(f"Erfahrung: **{self.exp}**", ephemeral=_ephemeral_ok(interaction))

    @discord.ui.button(label="Regeln akzeptiert", style=discord.ButtonStyle.success, emoji="📜")
    async def btn_rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.rules_ok = True
        await interaction.response.send_message("Regeln akzeptiert.", ephemeral=_ephemeral_ok(interaction))

    @discord.ui.button(label="Bestätigen (an Gildenleitung)", style=discord.ButtonStyle.primary, emoji="✅")
    async def btn_confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (self.primary and self.exp and self.rules_ok):
            await interaction.response.send_message("Bitte **Primärrolle**, **Erfahrung** wählen und **Regeln** bestätigen.", ephemeral=_ephemeral_ok(interaction))
            return
        guild = client.get_guild(self.guild_id)
        member = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        choice = self.primary; experienced = (self.exp == "Erfahren")
        await _queue_onboarding_review(guild, member, choice, experienced)
        await interaction.response.send_message("Danke! Die Gildenleitung prüft kurz deine Angaben.", ephemeral=_ephemeral_ok(interaction))
        self.stop()

async def _send_onboarding_dm(member: discord.Member):
    if member.bot: return
    emb = discord.Embed(
        title="Willkommen bei Weiße Flamme",
        description=("Wähle bitte deine **Primärrolle**, gib deine **Erfahrung** an und bestätige die Regeln. "
                     "Danach prüft die **Gildenleitung** kurz und schaltet dich frei."),
        color=discord.Color.orange()
    )
    view = OnboardView(member.guild.id, member.id)
    try:
        await member.send(embed=emb, view=view)
    except discord.Forbidden:
        if member.guild.system_channel:
            try:
                await member.guild.system_channel.send(
                    f"{member.mention} bitte öffne deine DMs – ich habe dir fürs Onboarding soeben geschrieben.")
            except Exception:
                pass

async def _finalize_onboarding(member: discord.Member, choice: str, experienced: bool):
    gid = member.guild.id; roles_to_add: List[discord.Role] = []

    guild_role_id = _get_guild_role_filter_id(gid)
    if guild_role_id:
        r = member.guild.get_role(guild_role_id)
        if r: roles_to_add.append(r)

    rid_map = _get_role_ids(member.guild)
    key = "DPS" if choice == "DD" else ("TANK" if choice == "Tank" else "HEAL")
    role_id = rid_map.get(key) or 0
    if role_id:
        r = member.guild.get_role(int(role_id))
        if r: roles_to_add.append(r)

    if not experienced:
        nb_id = _get_newbie_role_id(gid)
        if nb_id:
            r = member.guild.get_role(nb_id)
            if r: roles_to_add.append(r)

    add_list = [r for r in roles_to_add if r and r not in member.roles]
    if add_list:
        try: await member.add_roles(*add_list, reason="Onboarding abgeschlossen (Staff-Approve)")
        except Exception as e: print("add_roles failed:", e)

    meta = _meta_g(gid); wc_id = int(meta.get("welcome_channel_id", 0) or 0)
    ch = member.guild.get_channel(wc_id) if wc_id else None
    if isinstance(ch, discord.TextChannel):
        try:
            await ch.send(
                f"🔥 Willkommen {member.mention} in **Weiße Flamme**!\n"
                f"Primärrolle: **{choice}**, Erfahrung: **{'Erfahren' if experienced else 'Unerfahren'}**.")
        except Exception as e:
            print("welcome post failed:", e)

class _OnboardReviewView(discord.ui.View):
    def __init__(self, gid: int, uid: int):
        super().__init__(timeout=3600)
        self.gid = gid; self.uid = uid

    def _allowed(self, inter: discord.Interaction) -> bool: return is_admin(inter)

    async def _close(self, inter: discord.Interaction, label: str):
        for c in self.children:
            if isinstance(c, discord.ui.Button): c.disabled = True
        try: await inter.message.edit(content=f"{inter.message.content}\n**Status:** {label}", view=self)
        except Exception: pass

    @discord.ui.button(label="Annehmen", emoji="✅", style=discord.ButtonStyle.success, custom_id="wf_ob_approve")
    async def approve(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return
        payload = pending_onboarding.pop((self.gid, self.uid), None)
        if not payload:
            await inter.response.send_message("❌ Keine offenen Daten.", ephemeral=True); return
        guild = inter.guild
        mem = guild.get_member(self.uid) or await guild.fetch_member(self.uid)
        await _finalize_onboarding(mem, payload["role_choice"], payload["experienced"])
        await inter.response.send_message("✅ Angenommen. Rollen vergeben & Willkommenspost raus.", ephemeral=True)
        await self._close(inter, "Angenommen")

    @discord.ui.button(label="Ablehnen", emoji="❌", style=discord.ButtonStyle.danger, custom_id="wf_ob_reject")
    async def reject(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return
        pending_onboarding.pop((self.gid, self.uid), None)
        await inter.response.send_message("🛑 Abgelehnt.", ephemeral=True)
        await self._close(inter, "Abgelehnt")

async def _queue_onboarding_review(guild: discord.Guild, member: discord.Member, role_choice: str, experienced: bool):
    pending_onboarding[(guild.id, member.id)] = {"role_choice": role_choice, "experienced": experienced, "ts": _now().isoformat()}
    meta = _meta_g(guild.id); sc_id = int(meta.get("staff_channel_id", 0) or 0)
    ch = guild.get_channel(sc_id) if sc_id else None
    if not isinstance(ch, discord.TextChannel):
        print("[onboarding] Kein Staff-Channel gesetzt."); return
    lines = [
        f"👤 **User:** {member.mention} (`{member.id}`)",
        f"🧭 **Rolle:** {'DPS' if role_choice=='DD' else role_choice}",
        f"📚 **Erfahrung:** {'Erfahren' if experienced else 'Unerfahren'}",
        f"Bitte **✅/❌** wählen.",
    ]
    try:
        await ch.send(
            content=f"**Onboarding-Review:** {member.display_name}",
            embed=discord.Embed(description="\n".join(lines), color=discord.Color.orange()),
            view=_OnboardReviewView(guild.id, member.id)
        )
    except Exception as e:
        print("queue review failed:", e)

# ======================== UI: Picker ========================
class TextChannelPicker(discord.ui.ChannelSelect):
    def __init__(self, meta_key: Literal["welcome_channel_id","staff_channel_id"]):
        super().__init__(channel_types=[discord.ChannelType.text], placeholder="Kanal wählen", min_values=1, max_values=1)
        self.meta_key = meta_key
    async def callback(self, inter: discord.Interaction):
        ch: discord.abc.GuildChannel = self.values[0]
        _set_meta(inter.guild_id, **{self.meta_key: ch.id})
        label = "Willkommens-Kanal" if self.meta_key=="welcome_channel_id" else "Staff-Review-Kanal"
        await inter.response.send_message(f"✅ {label}: {ch.mention}", ephemeral=True)

class SingleRolePicker(discord.ui.RoleSelect):
    def __init__(self, mode: Literal["guild","tank","heal","dps","newbie"]):
        super().__init__(min_values=1, max_values=1)
        self.mode=mode
    async def callback(self, inter: discord.Interaction):
        r: discord.Role = self.values[0]
        gid = inter.guild_id
        if self.mode=="guild":
            _set_guild_role_id(gid, r.id)
            await inter.response.send_message(f"✅ Mitgliedsrolle gesetzt: {r.mention}", ephemeral=True); return
        if self.mode=="newbie":
            _set_newbie_role_id(gid, r.id)
            await inter.response.send_message(f"✅ NEWBIE-Rolle gesetzt: {r.mention}", ephemeral=True); return
        cur = _get_role_ids(inter.guild)
        if self.mode=="tank": cur["TANK"]=r.id
        elif self.mode=="heal": cur["HEAL"]=r.id
        elif self.mode=="dps": cur["DPS"]=r.id
        _set_role_ids(gid, cur["TANK"], cur["HEAL"], cur["DPS"])
        await inter.response.send_message("✅ Rollen verknüpft.", ephemeral=True)

# ======================== WFWorld – Persistenz & Helper ========================
def _wf_g(gid: int) -> dict:
    g = wfworld.get(str(gid))
    if not g:
        g = {"users": {}, "casino": {"server_seed_hash": ""}}
        wfworld[str(gid)] = g
        _save_wfworld()
    return g

def _wf_u(gid: int, uid: int) -> dict:
    g = _wf_g(gid)
    u = g["users"].get(str(uid))
    if not u:
        u = {
            "balance": 0,
            "cooldowns": {
                "fishing_ready_at": "1970-01-01T00:00:00+00:00",
                "mining_ready_at":  "1970-01-01T00:00:00+00:00",
                "hunting_ready_at": "1970-01-01T00:00:00+00:00",
                "daily_ready_at":   "1970-01-01T00:00:00+00:00",
            },
            "inventory": {
                "fish": {},
                "ore": {},
                "bar": {},
                "materials": {},
                "consumables": {},
                "runes": {},
                "weapons": []
            },
            "ledger": []
        }
        g["users"][str(uid)] = u
        _save_wfworld()
    return u

def _dt_from_iso(s: str) -> datetime:
    try: return datetime.fromisoformat(s)
    except Exception: return datetime(1970,1,1, tzinfo=TZ)

def _set_cd(u: dict, key: str, dt: datetime): u["cooldowns"][key] = dt.isoformat(); _save_wfworld()
def _get_cd(u: dict, key: str) -> datetime: return _dt_from_iso(u["cooldowns"].get(key, "1970-01-01T00:00:00+00:00"))
def _pay(u: dict, delta: int, reason: str):
    u["balance"] = max(0, int(u["balance"]) + int(delta))
    u["ledger"].append({"ts": _now().isoformat(), "delta": int(delta), "reason": reason})
    if len(u["ledger"]) > 200: u["ledger"] = u["ledger"][-200:]
    _save_wfworld()
def _inv_add(bucket: dict, key: str, qty: int=1): bucket[key] = int(bucket.get(key, 0)) + int(qty)

def _has_wf_role(member: discord.Member) -> Tuple[bool, str]:
    role_id = _get_guild_role_filter_id(member.guild.id)
    if not role_id:
        # Falls Gate-Rolle nicht gesetzt ist: freigeben (Admins sowieso)
        return (True, "not-set")
    has = any(r.id == role_id for r in member.roles)
    return (has or member.guild_permissions.administrator, f"<@&{role_id}>")

def S(amount: int) -> str:
    return f"{amount:,}".replace(",", " ") + " Sollant"

# ---- WFWorld Konfig ----
FISH_TABLE = [
    ("gemein",        50,  60),
    ("ungewöhnlich", 120,  25),
    ("selten",       300,  10),
    ("episch",       800,   4),
    ("mondlachs",   1800,   1),
]
FISH_EMOJI = "🎣"; MINING_EMOJI = "⛏️"; HUNT_EMOJI = "🏹"; DAILY_EMOJI = "🎁"
WF_COOLDOWNS = {
    "fishing": timedelta(hours=3),
    "mining":  timedelta(hours=3),
    "hunting": timedelta(minutes=30),
    "daily":   timedelta(hours=24),
}
DAILY_REWARD = 2000

def _weighted_choice(table):
    total = sum(w for _,_,w in table)
    r = random.uniform(0, total); upto = 0
    for item, _, weight in table:
        if upto + weight >= r: return item
        upto += weight
    return table[-1][0]

def _fish_reward(fid: str) -> int:
    for k, val, _ in FISH_TABLE:
        if k == fid: return val
    return 0

def _fish_sell_value(fishes: Dict[str,int]) -> int:
    return sum(_fish_reward(k) * int(v) for k,v in fishes.items())

# ======================== UI: Menüs ========================
def hub_embed(guild: discord.Guild) -> discord.Embed:
    return discord.Embed(
        title="Weiße Flamme – Hub",
        description=(
            "Wähle unten aus.\n\n"
            "• **🏆 Flammenscore** – Mein Score / Topliste\n"
            "• **📅 Events** – Raid/RSVP & wiederkehrende Erinnerungen (Admin)\n"
            "• **🧭 Onboarding** – Kanäle/Rollen & Test-DM (Admin)\n"
            "• **⚙️ Admin** – Sync & Tools\n"
            "• **🎮 WF-Spielwelt** – Wirtschaft & Mini-Games\n"
        ),
        color=discord.Color.orange()
    )

def score_embed_intro() -> discord.Embed:
    return discord.Embed(title="🏆 Flammenscore", description="Wähle:", color=discord.Color.orange())
def events_embed_intro() -> discord.Embed:
    return discord.Embed(title="📅 Events", description="Erstellen/Verwalten (nur Admin).", color=discord.Color.blurple())
def onboard_embed_intro() -> discord.Embed:
    return discord.Embed(title="🧭 Onboarding", description="Rollen & Kanäle setzen, Test-DM.", color=discord.Color.green())
def admin_embed_intro() -> discord.Embed:
    return discord.Embed(title="⚙️ Admin-Tools", description="Sync & Hilfen.", color=discord.Color.dark_grey())

class BackToHubMixin:
    @discord.ui.button(label="Zurück", emoji="⬅️", style=discord.ButtonStyle.secondary, row=4, custom_id="back_hub")
    async def back(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.edit_message(embed=hub_embed(inter.guild), view=HubView())

# ---- WFWorld Views/Actions ----
def wfworld_embed(user: discord.Member) -> discord.Embed:
    u = _wf_u(user.guild.id, user.id)
    e = discord.Embed(
        title="🎮 WFWorld",
        description=f"Kontostand: **{S(u['balance'])}**",
        color=discord.Color.orange(),
    )
    e.set_footer(text="Privat/ephemeral. Öffentliche Posts nur für Boss/Casino (später).")
    return e

class WfWorldBackMixin:
    @discord.ui.button(label="Menü", emoji="🏰", style=discord.ButtonStyle.secondary, row=4, custom_id="wf_back_world")
    async def back_world(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.edit_message(embed=wfworld_embed(inter.user), view=WFWorldView())

class WFWorldView(discord.ui.View, BackToHubMixin):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Beruf", emoji="🛠️", style=discord.ButtonStyle.primary, custom_id="wf_prof")
    async def v_prof(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.edit_message(embed=wfworld_embed(inter.user), view=ProfessionView())

    @discord.ui.button(label="Weltboss", emoji="🐉", style=discord.ButtonStyle.primary, custom_id="wf_boss")
    async def v_boss(self, inter: discord.Interaction, _btn: discord.ui.Button):
        e = discord.Embed(title="🐉 Weltboss", description="Fenster & Loot-Vorschau – kommt bald.", color=discord.Color.red())
        await inter.response.edit_message(embed=e, view=WorldBossView())

    @discord.ui.button(label="Schmied", emoji="🛠️", style=discord.ButtonStyle.secondary, custom_id="wf_smith")
    async def v_smith(self, inter: discord.Interaction, _btn: discord.ui.Button):
        e=discord.Embed(title="🛠️ Schmied", description="Schmelzen & Upgrades – Stub (nächster Schritt).", color=discord.Color.blurple())
        await inter.response.edit_message(embed=e, view=SmithView())

    @discord.ui.button(label="Gildenhändler", emoji="🛒", style=discord.ButtonStyle.secondary, custom_id="wf_vendor")
    async def v_vendor(self, inter: discord.Interaction, _btn: discord.ui.Button):
        e=discord.Embed(title="🛒 Gildenhändler", description="Buffs/Runen – Stub.", color=discord.Color.green())
        await inter.response.edit_message(embed=e, view=VendorView())

    @discord.ui.button(label="Glücksspiel", emoji="🎲", style=discord.ButtonStyle.secondary, custom_id="wf_casino")
    async def v_casino(self, inter: discord.Interaction, _btn: discord.ui.Button):
        e=discord.Embed(title="🎲 Glücksspiel", description="Provably Fair – Stub. Edge 2–8 %, Limits, Verlust-Cap.", color=discord.Color.gold())
        await inter.response.edit_message(embed=e, view=CasinoView())

    @discord.ui.button(label="Bank", emoji="🏦", style=discord.ButtonStyle.secondary, custom_id="wf_bank")
    async def v_bank(self, inter: discord.Interaction, _btn: discord.ui.Button):
        u=_wf_u(inter.guild_id, inter.user.id)
        e=discord.Embed(title="🏦 Bank", description=f"Kontostand: **{S(u['balance'])}**\n(Überweisungen/Fees: später)", color=discord.Color.dark_blue())
        await inter.response.edit_message(embed=e, view=BankView())

    @discord.ui.button(label="Inventar", emoji="🎒", style=discord.ButtonStyle.secondary, custom_id="wf_inv")
    async def v_inv(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await show_inventory(inter)

    @discord.ui.button(label="Status", emoji="🕒", style=discord.ButtonStyle.secondary, custom_id="wf_status")
    async def v_status(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await show_status(inter)

    @discord.ui.button(label="Hilfe/Fairness", emoji="📖", style=discord.ButtonStyle.secondary, custom_id="wf_help")
    async def v_help(self, inter: discord.Interaction, _btn: discord.ui.Button):
        e=discord.Embed(
            title="📖 Hilfe & Fairness",
            description=("• Server-Cooldowns verhindern Spam\n"
                         "• Glücksspiel später mit Server-Seed-Hash & Runden-Hash\n"
                         "• Risiko: Gewinne nicht garantiert – verliere nur, was du verkraftest"),
            color=discord.Color.light_grey()
        )
        await inter.response.edit_message(embed=e, view=HelpView())

class ProfessionView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Angeln", emoji=FISH_EMOJI, style=discord.ButtonStyle.primary, custom_id="wf_prof_fish")
    async def v_fishing(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await show_fishing(inter)

    @discord.ui.button(label="Bergbau", emoji=MINING_EMOJI, style=discord.ButtonStyle.primary, custom_id="wf_prof_mine")
    async def v_mining(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await show_mining(inter)

    @discord.ui.button(label="Jagd", emoji=HUNT_EMOJI, style=discord.ButtonStyle.primary, custom_id="wf_prof_hunt")
    async def v_hunt(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await show_hunting(inter)

    @discord.ui.button(label="Daily", emoji=DAILY_EMOJI, style=discord.ButtonStyle.success, custom_id="wf_prof_daily")
    async def v_daily(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await claim_daily(inter)

# Sub-Views (Stubs)
class WorldBossView(discord.ui.View, WfWorldBackMixin): 
    def __init__(self): super().__init__(timeout=None)
class SmithView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)
class VendorView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)
class CasinoView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)
class BankView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)
class HelpView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=None)

# ---- Beruf: Angeln ----
class FishingStartView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=60)

    @discord.ui.button(label="Angeln starten", emoji=FISH_EMOJI, style=discord.ButtonStyle.primary, custom_id="wf_fish_start")
    async def fish_start(self, inter: discord.Interaction, _btn: discord.ui.Button):
        u=_wf_u(inter.guild_id, inter.user.id)
        ready=_get_cd(u, "fishing_ready_at")
        if ready>_now():
            await inter.response.send_message(f"⏳ Cooldown: **{_fmt_dur(ready)}**", ephemeral=True); return
        pulls=random.randint(2,4)
        out: Dict[str,int]={}
        for _ in range(pulls):
            fid=_weighted_choice(FISH_TABLE); out[fid]=out.get(fid,0)+1
            _inv_add(u["inventory"]["fish"], fid, 1)
        _set_cd(u, "fishing_ready_at", _now()+WF_COOLDOWNS["fishing"])
        lines=[f"• {k} ×{v} (+{S(_fish_reward(k)*v)})" for k,v in out.items()]
        e=discord.Embed(title="🎣 Fang", description="\n".join(lines) or "Nichts gefangen.", color=discord.Color.blue())
        e.set_footer(text="Du kannst Fische im Inventar verkaufen.")
        await inter.response.edit_message(embed=e, view=InventorySellView())

class InventorySellView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=60)

    @discord.ui.button(label="Alle Fische verkaufen", emoji="💰", style=discord.ButtonStyle.success, custom_id="wf_sell_fish_all")
    async def sell_all(self, inter: discord.Interaction, _btn: discord.ui.Button):
        u=_wf_u(inter.guild_id, inter.user.id)
        fish=u["inventory"]["fish"]
        val=_fish_sell_value(fish)
        if val<=0:
            await inter.response.send_message("Keine Fische im Inventar.", ephemeral=True); return
        fish.clear(); _pay(u, val, "Fischverkauf")
        e=discord.Embed(title="💰 Verkauf", description=f"Erlös: **{S(val)}**\nKontostand: **{S(u['balance'])}**", color=discord.Color.gold())
        await inter.response.edit_message(embed=e, view=WFWorldView())

async def show_fishing(inter: discord.Interaction):
    u=_wf_u(inter.guild_id, inter.user.id)
    ready=_get_cd(u, "fishing_ready_at")
    if ready>_now():
        e=discord.Embed(title="🎣 Angeln", description=f"Cooldown: **{_fmt_dur(ready)}**", color=discord.Color.blue())
        await inter.response.edit_message(embed=e, view=ProfessionView())
    else:
        e=discord.Embed(title="🎣 Angeln", description="Bereit. Fang simuliert, 2–4 Fische.\nVerkauf im Inventar.", color=discord.Color.blue())
        await inter.response.edit_message(embed=e, view=FishingStartView())

# ---- Beruf: Bergbau (Stub) ----
ORE_TABLE = [("kupfer", 1, 60), ("eisen",1,30), ("stahl",1,10)]
class MiningStartView(discord.ui.View, WfWorldBackMixin):
    def __init__(self): super().__init__(timeout=60)
    @discord.ui.button(label="Bergbau starten", emoji=MINING_EMOJI, style=discord.ButtonStyle.primary, custom_id="wf_mine_start")
    async def mine(self, inter: discord.Interaction, _btn: discord.ui.Button):
        u=_wf_u(inter.guild_id, inter.user.id)
        ready=_get_cd(u, "mining_ready_at")
        if ready>_now():
            await inter.response.send_message(f"⏳ Cooldown: **{_fmt_dur(ready)}**", ephemeral=True); return
        pulls=random.randint(1,3)
        out={}
        for _ in range(pulls):
            total=sum(w for _,_,w in ORE_TABLE); r=random.uniform(0,total); acc=0
            for ore,qty,w in ORE_TABLE:
                acc+=w
                if r<=acc:
                    _inv_add(u["inventory"]["ore"], ore, qty)
                    out[ore]=out.get(ore,0)+qty
                    break
        _set_cd(u, "mining_ready_at", _now()+WF_COOLDOWNS["mining"])
        lines=[f"• {k} ×{v}" for k,v in out.items()]
        e=discord.Embed(title="⛏️ Bergbau", description="\n".join(lines) or "Nichts gefunden.", color=discord.Color.dark_grey())
        e.set_footer(text="Schmelzen beim Schmied (bald).")
        await inter.response.edit_message(embed=e, view=ProfessionView())

async def show_mining(inter: discord.Interaction):
    u=_wf_u(inter.guild_id, inter.user.id)
    ready=_get_cd(u, "mining_ready_at")
    if ready>_now():
        e=discord.Embed(title="⛏️ Bergbau", description=f"Cooldown: **{_fmt_dur(ready)}**", color=discord.Color.dark_grey())
        await inter.response.edit_message(embed=e, view=ProfessionView())
    else:
        e=discord.Embed(title="⛏️ Bergbau", description="Bereit. 1–3 Erze, seltene Adern später.", color=discord.Color.dark_grey())
        await inter.response.edit_message(embed=e, view=MiningStartView())

# ---- Beruf: Jagd (Stub) ----
async def show_hunting(inter: discord.Interaction):
    u=_wf_u(inter.guild_id, inter.user.id)
    ready=_get_cd(u, "hunting_ready_at")
    if ready>_now():
        e=discord.Embed(title="🏹 Jagd", description=f"Cooldown: **{_fmt_dur(ready)}**", color=discord.Color.green())
        await inter.response.edit_message(embed=e, view=ProfessionView())
    else:
        _set_cd(u, "hunting_ready_at", _now()+WF_COOLDOWNS["hunting"])
        e=discord.Embed(title="🏹 Jagd", description="Minispiele (QTE/Memory) kommen. Cooldown gesetzt.", color=discord.Color.green())
        await inter.response.edit_message(embed=e, view=ProfessionView())

# ---- Inventar/Status/Hilfe ----
def _fmt_dur(ts: datetime) -> str:
    now=_now()
    if ts<=now: return "bereit"
    delta=ts-now
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m, _ = divmod(rem, 60)
    if h>0: return f"{h} h {m} min"
    return f"{m} min"

async def show_inventory(inter: discord.Interaction):
    u=_wf_u(inter.guild_id, inter.user.id)
    inv=u["inventory"]
    fish=inv["fish"]; ores=inv["ore"]; bars=inv["bar"]
    lines=[]
    if fish: lines.append("**Fische**:\n" + "\n".join(f"• {k} ×{v}" for k,v in fish.items()) + f"\nWert gesamt: **{S(_fish_sell_value(fish))}**")
    if ores: lines.append("**Erze**:\n" + "\n".join(f"• {k} ×{v}" for k,v in ores.items()))
    if bars: lines.append("**Barren**:\n" + "\n".join(f"• {k} ×{v}" for k,v in bars.items()))
    desc = "\n\n".join(lines) if lines else "Leer."
    e=discord.Embed(title="🎒 Inventar", description=desc, color=discord.Color.teal())
    await inter.response.edit_message(embed=e, view=InventorySellView())

async def show_status(inter: discord.Interaction):
    u=_wf_u(inter.guild_id, inter.user.id)
    items=[
        ("Angeln", _get_cd(u,"fishing_ready_at")),
        ("Bergbau", _get_cd(u,"mining_ready_at")),
        ("Jagd", _get_cd(u,"hunting_ready_at")),
        ("Daily", _get_cd(u,"daily_ready_at")),
    ]
    lines=[f"• {name}: **{_fmt_dur(dt)}**" for name,dt in items]
    e=discord.Embed(title="🕒 Status & Cooldowns", description="\n".join(lines), color=discord.Color.dark_teal())
    e.add_field(name="Kontostand", value=f"**{S(u['balance'])}**", inline=False)
    await inter.response.edit_message(embed=e, view=WFWorldView())

class HubView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="WF-Spielwelt", emoji="🎮", style=discord.ButtonStyle.secondary, custom_id="hub_game")
    async def game(self, inter: discord.Interaction, _btn: discord.ui.Button):
        ok, need = _has_wf_role(inter.user)
        if not ok:
            await inter.response.send_message(f"❌ Zugriff nur mit Rolle {need}.", ephemeral=True); return
        await inter.response.edit_message(embed=wfworld_embed(inter.user), view=WFWorldView())

    @discord.ui.button(label="Flammenscore", emoji="🏆", style=discord.ButtonStyle.primary, custom_id="hub_score")
    async def score(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.edit_message(embed=score_embed_intro(), view=ScoreView())

    @discord.ui.button(label="Events", emoji="📅", style=discord.ButtonStyle.primary, custom_id="hub_events")
    async def events(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return
        await inter.response.edit_message(embed=events_embed_intro(), view=EventsView())

    @discord.ui.button(label="Onboarding", emoji="🧭", style=discord.ButtonStyle.primary, custom_id="hub_onboard")
    async def onboard(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return
        await inter.response.edit_message(embed=onboard_embed_intro(), view=OnboardAdminView())

    @discord.ui.button(label="Admin", emoji="⚙️", style=discord.ButtonStyle.secondary, custom_id="hub_admin")
    async def admin(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return
        await inter.response.edit_message(embed=admin_embed_intro(), view=AdminToolsView())

class ScoreView(discord.ui.View, BackToHubMixin):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Mein Score", emoji="👤", style=discord.ButtonStyle.primary)
    async def my_score(self, inter: discord.Interaction, _btn: discord.ui.Button):
        gid = inter.guild_id; uid = inter.user.id
        data = scores.get(str(gid)) or {}
        arr = []
        for uid_str in data.keys():
            u = int(uid_str); tot,_ = _calc_flammenscore(gid,u)
            arr.append((u,tot))
        arr.sort(key=lambda t:t[1], reverse=True)
        pos = next((i for i,(u,_) in enumerate(arr, start=1) if u==uid), 0)
        my_total = next((tot for u,tot in arr if u==uid), 0.0)
        _, parts = _calc_flammenscore(gid, uid)
        lines = [
            f"**Rang:** {pos}/{len(arr)}" if pos else f"**Rang:** –/{len(arr)}",
            f"**Score:** {my_total:.1f}",
            f"• Voice: {parts['voice']:.1f}",
            f"• Messages: {parts['msg']:.1f}",
            f"• Reaktionen gegeben: {parts['rg']:.1f}",
            f"• Reaktionen erhalten: {parts['rr']:.1f}",
            f"• RSVP: {parts['rsvp']:.1f}",
        ]
        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Topliste (Top 10)", emoji="🏅", style=discord.ButtonStyle.secondary)
    async def top(self, inter: discord.Interaction, _btn: discord.ui.Button):
        lines = _format_leaderboard_lines_simple(inter.guild, limit=10)
        if not lines:
            await inter.response.send_message("Noch keine Daten.")
            return
        emb = discord.Embed(title="🔥 Flammen – Topliste", description="\n".join(lines), color=discord.Color.orange())
        await inter.response.send_message(embed=emb)  # öffentlich

    @discord.ui.button(label="Test +1(Admin)", emoji="🧪", style=discord.ButtonStyle.secondary, row=3)
    async def test_admin(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin.", ephemeral=True); return
        b = _score_bucket(inter.guild_id, inter.user.id)
        b["messages"] += 1; _save_scores()
        await inter.response.send_message("Test: +1 Message gezählt.", ephemeral=True)

# ----- Events / Onboarding Admin Views aus deinem Bestand -----
def events_embed_intro() -> discord.Embed:
    return discord.Embed(title="📅 Events", description="Erstellen/Verwalten (nur Admin).", color=discord.Color.blurple())

class EventsView(discord.ui.View, BackToHubMixin):
    def __init__(self): super().__init__(timeout=None)

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Raid mit RSVP", emoji="📝", style=discord.ButtonStyle.primary, custom_id="wf_ev_rsvp")
    async def create(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.send_modal(CreateRaidModal())

    @discord.ui.button(label="Wiederkehrendes Event", emoji="🔁", style=discord.ButtonStyle.success, custom_id="wf_ev_recurring")
    async def recurring(self, inter: discord.Interaction, _btn: discord.ui.Button):
        try:
            await inter.response.send_modal(CreateRecurringEventModal())
        except Exception as e:
            if not inter.response.is_done():
                await inter.response.send_message(f"❌ Konnte das Formular nicht öffnen: {e}", ephemeral=True)
            else:
                try: await inter.followup.send(f"❌ Konnte das Formular nicht öffnen: {e}", ephemeral=True)
                except Exception: pass

    @discord.ui.button(label="Event-Liste", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="wf_ev_list")
    async def list_events(self, inter: discord.Interaction, _btn: discord.ui.Button):
        cfg = get_or_create_guild_cfg(inter.guild_id)
        if not cfg.events:
            await inter.response.send_message("Keine Events gespeichert.", ephemeral=True); return
        lines=[]
        for e in cfg.events.values():
            typ = "Once" if e.one_time_date else "Recurring"
            extra = f"\n• Beschreibung: {e.description}" if e.description else ""
            lines.append(f"• **{e.name}** ({typ}) — {e.start_hhmm}, Tage: {fmt_weekdays(e.weekdays)}{extra}")
        await inter.response.send_message("\n".join(lines), ephemeral=True)

    @discord.ui.button(label="Event löschen", emoji="🧹", style=discord.ButtonStyle.danger, custom_id="wf_ev_delete")
    async def delete_event(self, inter: discord.Interaction, _btn: discord.ui.Button):
        cfg = get_or_create_guild_cfg(inter.guild_id)
        if not cfg.events:
            await inter.response.send_message("Keine Events vorhanden.", ephemeral=True); return
        key = sorted(cfg.events.keys())[0]; name = cfg.events[key].name
        del cfg.events[key]; save_all(configs)
        await inter.response.send_message(f"🗑️ Event **{name}** gelöscht.", ephemeral=True)

    @discord.ui.button(label="Standard-Kanal", emoji="📣", style=discord.ButtonStyle.secondary, custom_id="wf_ev_set_default_channel")
    async def set_default_channel(self, inter: discord.Interaction, _btn: discord.ui.Button):
        class _AnnouncePicker(discord.ui.ChannelSelect):
            def __init__(self):
                super().__init__(channel_types=[discord.ChannelType.text], placeholder="Ankündigungs-Kanal wählen", min_values=1, max_values=1)
            async def callback(self, inner: discord.Interaction):
                ch: discord.TextChannel = self.values[0]
                cfg = get_or_create_guild_cfg(inner.guild_id)
                cfg.announce_channel_id = ch.id; save_all(configs)
                await inner.response.send_message(f"✅ Ankündigungs-Kanal gesetzt: {ch.mention}", ephemeral=True)
        v = discord.ui.View(); v.add_item(_AnnouncePicker())
        await inter.response.send_message("Ankündigungs-Kanal wählen:", view=v, ephemeral=True)

class OnboardAdminView(discord.ui.View, BackToHubMixin):
    def __init__(self): super().__init__(timeout=None)

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Willkommens-Kanal", emoji="📣", style=discord.ButtonStyle.secondary)
    async def set_welcome(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(TextChannelPicker("welcome_channel_id"))
        await inter.response.send_message("Kanal wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="Staff-Review-Kanal", emoji="🛡️", style=discord.ButtonStyle.secondary)
    async def set_staff(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(TextChannelPicker("staff_channel_id"))
        await inter.response.send_message("Kanal wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="Tank-Rolle", emoji="🛡️", style=discord.ButtonStyle.secondary)
    async def set_tank(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(SingleRolePicker("tank"))
        await inter.response.send_message("Tank-Rolle wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="Heal-Rolle", emoji="💚", style=discord.ButtonStyle.secondary)
    async def set_heal(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(SingleRolePicker("heal"))
        await inter.response.send_message("Heal-Rolle wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="DPS-Rolle", emoji="🗡️", style=discord.ButtonStyle.secondary)
    async def set_dps(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(SingleRolePicker("dps"))
        await inter.response.send_message("DPS-Rolle wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="Mitgliedsrolle (WF)", emoji="🏰", style=discord.ButtonStyle.secondary)
    async def set_guildrole(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(SingleRolePicker("guild"))
        await inter.response.send_message("Rolle wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="NEWBIE-Rolle", emoji="🌱", style=discord.ButtonStyle.secondary)
    async def set_newbie(self, inter: discord.Interaction, _btn: discord.ui.Button):
        v=discord.ui.View(); v.add_item(SingleRolePicker("newbie"))
        await inter.response.send_message("Rolle wählen:", view=v, ephemeral=True)

    @discord.ui.button(label="Onboarding Test-DM", emoji="✉️", style=discord.ButtonStyle.primary)
    async def test_dm(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await _send_onboarding_dm(inter.user)
        await inter.response.send_message("✅ DM verschickt (falls DMs offen).", ephemeral=True)

class AdminToolsView(discord.ui.View, BackToHubMixin):
    def __init__(self): super().__init__(timeout=None)

    async def interaction_check(self, inter: discord.Interaction) -> bool:
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Commands Clean+Sync", emoji="🔁", style=discord.ButtonStyle.secondary)
    async def sync(self, inter: discord.Interaction, _btn: discord.ui.Button):
        await inter.response.defer(thinking=True, ephemeral=True)
        try:
            await _clean_and_sync_commands_for_guild(inter.guild_id)
            await inter.followup.send("✅ Commands bereinigt (Guild-Scope geleert) & global synchronisiert.", ephemeral=True)
        except Exception as e:
            await inter.followup.send(f"❌ Sync-Fehler: {e}", ephemeral=True)

# ======================== Modals (Events) ========================
class CreateRaidModal(discord.ui.Modal):
    def __init__(self, channel_id: int | None = None):
        super().__init__(title="Raid/Event erstellen")
        self._channel_id = channel_id
        self.title_in = discord.ui.TextInput(label="Titel", placeholder="z.B. Raid heute Abend", max_length=100)
        self.date_in  = discord.ui.TextInput(label="Datum (YYYY-MM-DD)", placeholder="2025-10-01")
        self.time_in  = discord.ui.TextInput(label="Zeit (HH:MM 24h)", placeholder="20:00")
        self.desc_in  = discord.ui.TextInput(label="Beschreibung (optional)", style=discord.TextStyle.paragraph, required=False, max_length=400)
        self.img_in   = discord.ui.TextInput(label="Bild-URL (optional)", required=False)
        for c in (self.title_in, self.date_in, self.time_in, self.desc_in, self.img_in): self.add_item(c)

    async def on_submit(self, inter: discord.Interaction):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin/Manage Server.", ephemeral=True); return
        try:
            yyyy, mm, dd = [int(x) for x in self.date_in.value.split("-")]
            hh, mi = [int(x) for x in self.time_in.value.split(":")]
            when = datetime(yyyy, mm, dd, hh, mi, tzinfo=TZ)
        except Exception:
            await inter.response.send_message("❌ Datum/Zeit ungültig (YYYY-MM-DD / HH:MM).", ephemeral=True); return

        ch = inter.channel if self._channel_id is None else inter.guild.get_channel(self._channel_id)
        if not isinstance(ch, discord.TextChannel):
            await inter.response.send_message("❌ Zielkanal ungültig.", ephemeral=True); return

        obj = {
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": self.title_in.value.strip(),
            "description": (self.desc_in.value or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": ((self.img_in.value or "").strip() or None),
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": []
        }
        emb = await _build_embed_async(inter.guild, obj)
        view = RaidView(0)
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)
        rsvp_store[str(msg.id)] = obj; _save_rsvp()
        client.add_view(RaidView(msg.id), message_id=msg.id)
        await inter.response.send_message(f"✅ Raid erstellt: {msg.jump_url}", ephemeral=True)

class CreateRecurringEventModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Wiederkehrendes Event")
        self.name_in  = discord.ui.TextInput(label="Name", placeholder="Gildenbesprechung", max_length=60)
        self.dow_in   = discord.ui.TextInput(label="Wochentage", placeholder="Mo,Mi | Mon,Wed | Mo-So | täglich")
        self.time_in  = discord.ui.TextInput(label="Start (HH:MM)", placeholder="20:00")
        self.dur_in   = discord.ui.TextInput(label="Dauer (Minuten)", placeholder="60")
        self.opts_in  = discord.ui.TextInput(
            label="Vorwarnungen/Beschreibung (optional)",
            style=discord.TextStyle.paragraph,
            required=False,
            placeholder="Erste Zeile: Vorwarnungen z.B. 30,10,5\nAb Zeile 2: Beschreibung…"
        )
        for c in (self.name_in, self.dow_in, self.time_in, self.dur_in, self.opts_in): self.add_item(c)

    async def on_submit(self, inter: discord.Interaction):
        if not is_admin(inter):
            await inter.response.send_message("Nur Admin.", ephemeral=True); return
        try:
            dows = parse_weekdays(self.dow_in.value)
            start = parse_time_hhmm(self.time_in.value)
            dur = int(self.dur_in.value)
            pre, desc = parse_options_block(self.opts_in.value)
        except Exception as e:
            await inter.response.send_message(f"❌ Ungültige Eingaben: {e}", ephemeral=True); return
        cfg = get_or_create_guild_cfg(inter.guild_id)
        ev = Event(
            name=self.name_in.value.strip(),
            weekdays=dows,
            start_hhmm=start.strftime("%H:%M"),
            duration_min=dur,
            pre_reminders=pre,
            mention_role_id=None,
            channel_id=cfg.announce_channel_id,
            description=desc
        )
        cfg.events[ev.name.lower()] = ev; save_all(configs)
        await inter.response.send_message(
            f"✅ Wiederkehrendes Event gespeichert.\n"
            f"• Start: {ev.start_hhmm}\n"
            f"• Tage: {fmt_weekdays(ev.weekdays)}\n"
            f"• Beschreibung: {(ev.description or '—')}",
            ephemeral=True
        )

# ======================== Slash-Commands ========================
@tree.command(name="wf", description="Weiße Flamme – Menü anzeigen")
async def wf(inter: discord.Interaction):
    await inter.response.send_message(embed=hub_embed(inter.guild), view=HubView(), ephemeral=True)

@tree.command(name="wf_admin_sync_hard", description="(Admin) Global sync & Guild-Duplikate entfernen")
async def wf_admin_sync_hard(inter: discord.Interaction):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin.", ephemeral=True); return
    await inter.response.defer(thinking=True, ephemeral=True)
    try:
        await _clean_and_sync_commands_for_guild(inter.guild_id)
        await inter.followup.send("✅ Hard-Sync: Guild-Scope geleert, global synchronisiert.", ephemeral=True)
    except Exception as e:
        await inter.followup.send(f"❌ Fehler: {e}", ephemeral=True)

@tree.command(name="wf_debug_status", description="Debug: Intents/Perms anzeigen (Channel)")
async def wf_debug_status(inter: discord.Interaction):
    me: discord.Member = inter.guild.me
    ch: discord.TextChannel = inter.channel
    p = ch.permissions_for(me)
    flags = [
        f"read={p.read_messages}", f"history={p.read_message_history}",
        f"send={p.send_messages}", f"add_react={p.add_reactions}",
        f"ext_emoji={p.external_emojis}", f"view_channel={p.view_channel}"
    ]
    await inter.response.send_message(
        f"Intents: MC={client.intents.message_content} Members={client.intents.members} Voice={client.intents.voice_states}\n"
        f"Perms in #{ch.name}: " + ", ".join(flags),
        ephemeral=True
    )

@tree.command(name="wf_event_recurring", description="(Admin) Wiederkehrendes Event anlegen (Fallback ohne Modal)")
@app_commands.describe(
    name="Name",
    days="Wochentage (z. B. Mo,Mi / Mon,Wed / Mo-So / täglich)",
    start="Start HH:MM",
    duration_min="Dauer in Minuten",
    options="1. Zeile: Vorwarnungen (z.B. 30,10,5); ab Zeile 2: Beschreibung"
)
async def wf_event_recurring(inter: discord.Interaction,
                             name: str,
                             days: str,
                             start: str,
                             duration_min: int,
                             options: str = ""):
    if not is_admin(inter):
        await inter.response.send_message("Nur Admin.", ephemeral=True); return
    try:
        dows = parse_weekdays(days)
        t = parse_time_hhmm(start)
        pre, desc = parse_options_block(options)
    except Exception as e:
        await inter.response.send_message(f"❌ Ungültig: {e}", ephemeral=True); return

    cfg = get_or_create_guild_cfg(inter.guild_id)
    ev = Event(
        name=name.strip(),
        weekdays=dows,
        start_hhmm=t.strftime("%H:%M"),
        duration_min=int(duration_min),
        pre_reminders=pre,
        mention_role_id=None,
        channel_id=cfg.announce_channel_id,
        description=desc.strip()
    )
    cfg.events[ev.name.lower()] = ev; save_all(configs)
    await inter.response.send_message(
        f"✅ Wiederkehrendes Event gespeichert.\n"
        f"• Start: {ev.start_hhmm}\n"
        f"• Tage: {fmt_weekdays(ev.weekdays)}\n"
        f"• Beschreibung: {(ev.description or '—')}",
        ephemeral=True
    )

# ======================== Lifecycle & Command Sync ========================
async def _clean_and_sync_commands_for_guild(guild_id: int):
    """Nur globale Commands nutzen; Guild-Scope leeren um Duplikate zu vermeiden."""
    await tree.sync()  # global
    guild_obj = discord.Object(id=guild_id)
    tree.clear_commands(guild=guild_obj)   # Guild-spezifische löschen
    await tree.sync(guild=guild_obj)       # commit clear

def reregister_persistent_views_on_start():
    to_drop=[]
    for msg_id, obj in list(rsvp_store.items()):
        g = client.get_guild(obj.get("guild_id", 0))
        if not g: 
            to_drop.append(msg_id); continue
        try:
            client.add_view(RaidView(int(msg_id)), message_id=int(msg_id))
        except Exception as e:
            print("add_view (RSVP) failed:", e); to_drop.append(msg_id)
    for mid in to_drop:
        rsvp_store.pop(mid, None)
    if to_drop: _save_rsvp()

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")
    for g in client.guilds:
        try: await g.chunk()
        except Exception as e: print("guild.chunk() failed:", e)
    await _seed_voice_sessions()
    reregister_persistent_views_on_start()
    # Global sync einmal & alle Guild-Scopes leeren (Duplikate weg):
    await tree.sync()
    for g in client.guilds:
        await _clean_and_sync_commands_for_guild(g.id)
    print(f"Synced (global) for {len(client.guilds)} guild(s).")
    scheduler_loop.start(); voice_tick_loop.start()

@client.event
async def on_guild_join(guild: discord.Guild):
    try:
        await tree.sync()                 # global
        await _clean_and_sync_commands_for_guild(guild.id)  # Guild-Scope leer
    except Exception as e:
        print("sync on guild_join failed:", e)

@client.event
async def on_member_join(member: discord.Member):
    if member.bot: return
    await _send_onboarding_dm(member)

# ======================== Scheduler ========================
@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = _now().replace(second=0, microsecond=0)
    changed = False

    # 1) Event-Reminder
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
                    post_log.add(key); changed = True

            key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
            if _in_window(now, start_dt) and key not in post_log:
                role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                if ev.description: body += f"\n{ev.description}"
                await channel.send(body)
                post_log.add(key); changed = True

            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        del cfg.events[ev.name.lower()]; save_all(configs)
                except Exception: pass

    if changed: save_post_log(post_log)

    # 2) Wöchentliche Topliste (Fr 18:00)
    await _post_weekly_leaderboard_if_due(now)
    # 3) Monatlicher Reset (30. 00:00)
    await _monthly_reset_if_due(now)

@scheduler_loop.before_loop
async def _before_scheduler():
    await client.wait_until_ready()

@tasks.loop(seconds=60.0)
async def voice_tick_loop():
    now = _now()
    for (gid, uid), start in list(voice_sessions.items()):
        delta_ms = int((now - start).total_seconds() * 1000)
        b = _score_bucket(gid, uid); b["voice_ms"] += delta_ms
        voice_sessions[(gid, uid)] = now
    if voice_sessions: _save_scores()

@voice_tick_loop.before_loop
async def _before_voice_tick():
    await client.wait_until_ready()

# ======================== Score Hooks ========================
@client.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot: return
    me = message.guild.me
    if not message.channel.permissions_for(me).read_messages: return
    b = _score_bucket(message.guild.id, message.author.id)
    b["messages"] += 1; _save_scores()
    _cache_author(message.id, message.author.id)

@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id == client.user.id: return
    b = _score_bucket(payload.guild_id, payload.user_id); b["reacts_given"] += 1
    author_id = message_author_cache.get(payload.message_id)
    if author_id is None:
        try:
            ch = client.get_channel(payload.channel_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                msg = await ch.fetch_message(payload.message_id); author_id = msg.author.id
                _cache_author(payload.message_id, author_id)
        except Exception: author_id = None
    if author_id and author_id != payload.user_id:
        br = _score_bucket(payload.guild_id, author_id); br["reacts_recv"] += 1
    _save_scores()

@client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or payload.user_id == client.user.id: return
    b = _score_bucket(payload.guild_id, payload.user_id)
    if b["reacts_given"] > 0: b["reacts_given"] -= 1
    author_id = message_author_cache.get(payload.message_id)
    if author_id is None:
        try:
            ch = client.get_channel(payload.channel_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                msg = await ch.fetch_message(payload.message_id); author_id = msg.author.id
                _cache_author(payload.message_id, author_id)
        except Exception: author_id = None
    if author_id and author_id != payload.user_id:
        br = _score_bucket(payload.guild_id, author_id)
        if br["reacts_recv"] > 0: br["reacts_recv"] -= 1
    _save_scores()

@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or not member.guild: return
    gid = member.guild.id; uid = member.id
    if before.channel is None and after.channel is not None:
        _voice_start(gid, uid)
    elif before.channel is not None and after.channel is None:
        _voice_end(gid, uid)
    elif (before.channel is not None and after.channel is not None and before.channel.id != after.channel.id):
        _voice_end(gid, uid); _voice_start(gid, uid)

# ======================== Start ========================
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
