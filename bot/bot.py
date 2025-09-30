# bot.py
from __future__ import annotations

import os, json, threading, time, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time as time_cls, date as date_cls
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

# Onboarding-Review + Kanäle
ONBOARD_META_FILE = DATA_DIR / "onboarding_meta.json"   # {"<gid>":{"welcome_channel_id":int,"staff_channel_id":int}}
onboard_meta: Dict[str, dict] = {}

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

def _ephemeral_ok(inter: discord.Interaction) -> bool:
    # In DMs sind ephemerals nicht erlaubt → dort False
    return inter.guild_id is not None

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

# Stores
rsvp_store: Dict[str, dict] = _load_json(RSVP_STORE_FILE, {})
rsvp_cfg:   Dict[str, dict] = _load_json(RSVP_CFG_FILE, {})
scores:     Dict[str, dict] = _load_json(SCORE_FILE, {})      # {gid:{uid:{...}}}
score_cfg:  Dict[str, dict] = _load_json(SCORE_CFG_FILE, {})  # {gid:{weights...}}
score_meta: Dict[str, dict] = _load_json(SCORE_META_FILE, {}) # {gid:{last_reset_ym:"YYYY-MM"}}
onboard_meta: Dict[str, dict] = _load_json(ONBOARD_META_FILE, {})

def _save_rsvp():       _save_json(RSVP_STORE_FILE, rsvp_store)
def _save_rsvp_cfg():   _save_json(RSVP_CFG_FILE, rsvp_cfg)
def _save_scores():     _save_json(SCORE_FILE, scores)
def _save_score_cfg():  _save_json(SCORE_CFG_FILE, score_cfg)
def _save_score_meta(): _save_json(SCORE_META_FILE, score_meta)
def _save_onboard_meta(): _save_json(ONBOARD_META_FILE, onboard_meta)

# Konfig laden
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
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS":  int(g.get("DPS",  0) or 0),
    }

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

def _set_role_ids(gid: int, tank_id: int, heal_id: int, dps_id: int):
    g = rsvp_cfg.get(str(gid)) or {}
    g["TANK"] = int(tank_id)
    g["HEAL"] = int(heal_id)
    g["DPS"]  = int(dps_id)
    rsvp_cfg[str(gid)] = g
    _save_rsvp_cfg()

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

        # remove user from all buckets
        for k in ("TANK", "HEAL", "DPS"):
            if uid in obj["yes"][k]:
                obj["yes"][k].remove(uid)
        obj["no"] = [u for u in obj["no"] if u != uid]
        obj["maybe"].pop(str(uid), None)

        # add to selected
        if group in ("TANK", "HEAL", "DPS"):
            obj["yes"][group].append(uid)
            txt = f"Angemeldet als **{group}**."
        elif group == "MAYBE":
            # Label-Fallback anhand Rollenname
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

def register_rsvp_slash_commands():
    @tree.command(name="raid_set_roles", description="Rollen für Tank/Heal/DPS festlegen (pro Server).")
    @app_commands.describe(tank_role="Rolle für Tank", heal_role="Rolle für Heal", dps_role="Rolle für DPS")
    async def raid_set_roles(inter: discord.Interaction, tank_role: discord.Role, heal_role: discord.Role, dps_role: discord.Role):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        _set_role_ids(inter.guild_id, tank_role.id, heal_role.id, dps_role.id)
        await inter.response.send_message(
            f"✅ Gespeichert:\n🛡️ {tank_role.mention}\n💚 {heal_role.mention}\n🗡️ {dps_role.mention}",
            ephemeral=True
        )

    @tree.command(name="raid_create", description="Raid-/Event-Anmeldung mit Buttons erstellen.")
    @app_commands.describe(title="Titel (im Embed)", date="Datum YYYY-MM-DD", time="Zeit HH:MM (24h)",
                           channel="Zielkanal", image_url="Optionales Bild-URL", description="Zusätzliche Info")
    async def raid_create(inter: discord.Interaction, title: str, date: str, time: str,
                          channel: Optional[discord.TextChannel] = None,
                          image_url: Optional[str] = None, description: Optional[str] = None):
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
            "guild_id": inter.guild_id,
            "channel_id": ch.id,
            "title": title.strip(),
            "description": (description or "").strip(),
            "when_iso": when.isoformat(),
            "image_url": (image_url or "").strip() or None,
            "yes": {"TANK": [], "HEAL": [], "DPS": []},
            "maybe": {},
            "no": []
        }
        emb = await _build_embed_async(inter.guild, obj)
        view = RaidView(0)
        msg = await ch.send(embed=emb, view=view)
        view.msg_id = str(msg.id)
        rsvp_store[str(msg.id)] = obj
        _save_rsvp()

        client.add_view(RaidView(msg.id), message_id=msg.id)

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
            msg = await ch.fetch_message(int(message_id))
            await msg.edit(view=None)
            await inter.response.send_message("🔒 Gesperrt.", ephemeral=True)
        except Exception as e:
            await inter.response.send_message(f"❌ Fehler: {e}", ephemeral=True)

    @tree.command(name="raid_set_guildrole", description="Gildenrolle (Mitgliedsrolle) setzen – wird für Quote/Filter & Onboarding genutzt.")
    async def raid_set_guildrole(inter: discord.Interaction, guild_role: discord.Role):
        if not is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        _set_guild_role_id(inter.guild_id, guild_role.id)
        await inter.response.send_message(f"✅ Gildenrolle gesetzt: {guild_role.mention}", ephemeral=True)

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
        "credited_rsvp": []   # message_ids (str), einmalige Gutschrift je RSVP-Post
    })
    return u

def _calc_flammenscore(gid: int, uid: int) -> Tuple[float, Dict[str, float]]:
    u = _score_bucket(gid, uid)
    w = _guild_weights(gid)
    voice_min = u["voice_ms"] / 60000.0
    parts = {
        "voice": voice_min * w["voice_min"],
        "msg":   u["messages"]   * w["message"],
        "rg":    u["reacts_given"] * w["react_given"],
        "rr":    u["reacts_recv"]  * w["react_recv"],
        "rsvp":  u["rsvp"]       * w["rsvp"],
    }
    total = sum(parts.values())
    return total, parts

# Voice-Session-Map: (gid, uid) -> start_dt
voice_sessions: Dict[Tuple[int,int], datetime] = {}
def _voice_start(gid: int, uid: int):
    voice_sessions[(gid, uid)] = _now()
def _voice_end(gid: int, uid: int):
    key = (gid, uid)
    start = voice_sessions.pop(key, None)
    if start:
        delta_ms = int((_now() - start).total_seconds() * 1000)
        b = _score_bucket(gid, uid)
        b["voice_ms"] += max(0, delta_ms)
        _save_scores()

# Message-Author Cache für Reaction „received“
message_author_cache: Dict[int, int] = {}  # message_id -> author_id
def _cache_author(message_id: int, author_id: int, cap: int = 2000):
    if len(message_author_cache) >= cap:
        message_author_cache.pop(next(iter(message_author_cache)))
    message_author_cache[message_id] = author_id

def _format_leaderboard_lines_simple(guild: discord.Guild, limit: int = 10) -> List[str]:
    gid=guild.id; data=scores.get(str(gid)) or {}
    if not data:
        return []
    # Prozent relativ zur Summe – verhindert „100% trotz anderer Aktivität“
    total_possible = sum(_calc_flammenscore(gid,int(uid))[0] for uid in data.keys())
    total_possible = max(total_possible, 1e-9)
    scored=[]
    for uid_str in data.keys():
        uid=int(uid_str); total,_=_calc_flammenscore(gid,uid)
        scored.append((uid,total))
    scored.sort(key=lambda t:t[1], reverse=True)
    lines=[]
    for i,(uid,total) in enumerate(scored[:limit], start=1):
        m=guild.get_member(uid); name=m.display_name if m else f"<@{uid}>"
        pct=(total/total_possible)*100.0
        lines.append(f"{i}. {name} — {pct:.1f}%")
    return lines

async def _post_weekly_leaderboard_if_due(now: datetime):
    # Freitag 18:00
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

def _month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def _get_last_reset_ym(gid: int) -> str:
    g = score_meta.get(str(gid)) or {}
    return g.get("last_reset_ym", "")

def _set_last_reset_ym(gid: int, ym: str):
    g = score_meta.get(str(gid)) or {}
    g["last_reset_ym"] = ym
    score_meta[str(gid)] = g
    _save_score_meta()

async def _monthly_reset_if_due(now: datetime):
    # Reset am 30. des Monats um 00:00
    if not (now.day == 30 and now.hour == 0 and now.minute == 0):
        return
    ym = _month_key(now)
    for guild in client.guilds:
        last = _get_last_reset_ym(guild.id)
        if last == ym:
            continue
        scores[str(guild.id)] = {}  # kompletter Monatsreset
        _save_scores()
        _set_last_reset_ym(guild.id, ym)
        print(f"[Flammenscore] Reset for guild {guild.id} @ {ym}-30")

@tree.command(name="flammenscore_me", description="Zeigt deinen Flammenscore und Rang.")
async def flammenscore_me(inter: discord.Interaction):
    gid = inter.guild_id; uid = inter.user.id
    data = scores.get(str(gid)) or {}
    arr = []
    for uid_str in data.keys():
        u = int(uid_str)
        tot,_ = _calc_flammenscore(gid,u)
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

@tree.command(name="flammenscore_top", description="Topliste (Platz · Name · %).")
@app_commands.describe(limit="Anzahl (1–25, Standard 10)")
async def flammenscore_top(inter: discord.Interaction, limit: Optional[int] = 10):
    limit = max(1, min(25, limit or 10))
    lines = _format_leaderboard_lines_simple(inter.guild, limit=limit)
    if not lines:
        await inter.response.send_message("Noch keine Daten.", ephemeral=True); return
    emb = discord.Embed(title="🔥 Flammen – Topliste", description="\n".join(lines), color=discord.Color.orange())
    await inter.response.send_message(embed=emb, ephemeral=True)

# ======================== ONBOARDING (mit Staff-Review) ========================
# Speichert Pending-Auswahl bis Staff entscheidet
pending_onboarding: Dict[Tuple[int,int], dict] = {}

def _meta_g(gid: int) -> dict:
    return onboard_meta.get(str(gid)) or {}

def _set_meta(gid: int, **kwargs):
    m = onboard_meta.get(str(gid)) or {}
    m.update({k:v for k,v in kwargs.items() if v is not None})
    onboard_meta[str(gid)] = m
    _save_onboard_meta()

def _get_newbie_role_id(gid: int) -> int:
    g = rsvp_cfg.get(str(gid)) or {}
    try: return int(g.get("NEWBIE", 0))
    except Exception: return 0

def _set_newbie_role_id(gid: int, role_id: int):
    g = rsvp_cfg.get(str(gid)) or {}
    g["NEWBIE"] = int(role_id)
    rsvp_cfg[str(gid)] = g
    _save_rsvp_cfg()

class OnboardView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.user_id = user_id
        self.primary: Optional[str] = None  # "DD"|"Tank"|"Heal"
        self.exp: Optional[str] = None      # "Erfahren"|"Unerfahren"
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

    @discord.ui.button(label="Regeln gelesen & akzeptiert", style=discord.ButtonStyle.success, emoji="📜")
    async def btn_rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.rules_ok = True
        await interaction.response.send_message("Regeln akzeptiert.", ephemeral=_ephemeral_ok(interaction))

    @discord.ui.button(label="Bestätigen (an Gildenleitung senden)", style=discord.ButtonStyle.primary, emoji="✅")
    async def btn_confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (self.primary and self.exp and self.rules_ok):
            await interaction.response.send_message("Bitte **Primärrolle**, **Erfahrung** wählen und **Regeln** bestätigen.", ephemeral=_ephemeral_ok(interaction))
            return
        guild = client.get_guild(self.guild_id)
        member = guild.get_member(self.user_id) or await guild.fetch_member(self.user_id)
        choice = self.primary  # "DD"|"Tank"|"Heal"
        experienced = (self.exp == "Erfahren")
        # in Queue packen + Staff-Review posten
        await _queue_onboarding_review(guild, member, choice, experienced)
        await interaction.response.send_message("Danke! Die Gildenleitung prüft kurz deine Angaben. Du bekommst gleich Zugriff, sobald bestätigt. ✋", ephemeral=_ephemeral_ok(interaction))
        self.stop()

async def _send_onboarding_dm(member: discord.Member):
    if member.bot: return
    emb = discord.Embed(
        title="Willkommen bei Weiße Flamme",
        description=("Wähle bitte deine **Primärrolle**, gib deine **Erfahrung** an und bestätige, "
                     "dass du die Regeln gelesen hast. Danach prüft die **Gildenleitung** kurz und schaltet dich frei."),
        color=discord.Color.orange()
    )
    view = OnboardView(member.guild.id, member.id)
    try:
        await member.send(embed=emb, view=view)
    except discord.Forbidden:
        # Fallback-Hinweis im Systemkanal (falls sichtbar)
        if member.guild.system_channel:
            try:
                await member.guild.system_channel.send(
                    f"{member.mention} bitte öffne deine DMs – für das Onboarding habe ich dir soeben geschrieben.")
            except Exception:
                pass

async def _finalize_onboarding(member: discord.Member, choice: str, experienced: bool):
    """Vergibt Rollen + sendet Willkommenspost. choice: 'DD'|'Tank'|'Heal'"""
    gid = member.guild.id
    roles_to_add: List[discord.Role] = []

    # Gildenrolle (Weiße Flamme)
    guild_role_id = _get_guild_role_filter_id(gid)
    if guild_role_id:
        r = member.guild.get_role(guild_role_id)
        if r: roles_to_add.append(r)

    # Klassenrolle aus rsvp_cfg (DPS/TANK/HEAL)
    rid_map = _get_role_ids(member.guild)  # {"TANK":id, "HEAL":id, "DPS":id}
    key = "DPS" if choice == "DD" else ("TANK" if choice == "Tank" else "HEAL")
    role_id = rid_map.get(key) or 0
    if role_id:
        r = member.guild.get_role(int(role_id))
        if r: roles_to_add.append(r)

    # Newbie?
    if not experienced:
        nb_id = _get_newbie_role_id(gid)
        if nb_id:
            r = member.guild.get_role(nb_id)
            if r: roles_to_add.append(r)

    add_list = [r for r in roles_to_add if r and r not in member.roles]
    if add_list:
        try:
            await member.add_roles(*add_list, reason="Onboarding abgeschlossen (Staff-Approve)")
        except Exception as e:
            print("add_roles failed:", e)

    # Willkommenspost
    meta = _meta_g(gid)
    wc_id = int(meta.get("welcome_channel_id", 0) or 0)
    ch = member.guild.get_channel(wc_id) if wc_id else None
    if isinstance(ch, discord.TextChannel):
        try:
            await ch.send(
                f"🔥 Willkommen {member.mention} in **Weiße Flamme**!\n"
                f"Primärrolle: **{choice}**, Erfahrung: **{'Erfahren' if experienced else 'Unerfahren'}**.\n"
                f"Glut an, rein ins Gefecht! 🔥"
            )
        except Exception as e:
            print("welcome post failed:", e)

class _OnboardReviewView(discord.ui.View):
    def __init__(self, gid: int, uid: int):
        super().__init__(timeout=3600)
        self.gid = gid
        self.uid = uid

    def _allowed(self, inter: discord.Interaction) -> bool:
        return is_admin(inter)

    async def _close(self, inter: discord.Interaction, label: str):
        for c in self.children:
            if isinstance(c, discord.ui.Button): c.disabled = True
        try:
            await inter.message.edit(content=f"{inter.message.content}\n**Status:** {label}", view=self)
        except Exception:
            pass

    @discord.ui.button(label="Annehmen", emoji="✅", style=discord.ButtonStyle.success, custom_id="wf_ob_approve")
    async def approve(self, inter: discord.Interaction, _btn: discord.ui.Button):
        if not self._allowed(inter):
            await inter.response.send_message("❌ Keine Berechtigung.", ephemeral=True); return
        payload = pending_onboarding.pop((self.gid, self.uid), None)
        if not payload:
            await inter.response.send_message("❌ Keine offenen Daten (bereits entschieden oder abgelaufen).", ephemeral=True); return
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
        # Optional: Kick
        # try: await inter.guild.kick(discord.Object(id=self.uid), reason="Onboarding abgelehnt")
        # except Exception: pass
        await inter.response.send_message("🛑 Abgelehnt.", ephemeral=True)
        await self._close(inter, "Abgelehnt")

async def _queue_onboarding_review(guild: discord.Guild, member: discord.Member, role_choice: str, experienced: bool):
    pending_onboarding[(guild.id, member.id)] = {"role_choice": role_choice, "experienced": experienced, "ts": _now().isoformat()}
    meta = _meta_g(guild.id)
    sc_id = int(meta.get("staff_channel_id", 0) or 0)
    ch = guild.get_channel(sc_id) if sc_id else None
    if not isinstance(ch, discord.TextChannel):
        print(f"[onboarding] Kein Staff-Channel gesetzt. Nutze /wf_set_staff_channel.")
        return
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

# ---- Onboarding-Admin-Befehle ----
@tree.command(name="wf_set_welcome_channel", description="Kanal für öffentliche Begrüßung setzen.")
async def wf_set_welcome_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    _set_meta(inter.guild_id, welcome_channel_id=channel.id)
    await inter.response.send_message(f"✅ Willkommens-Kanal: {channel.mention}", ephemeral=True)

@tree.command(name="wf_set_staff_channel", description="Kanal für Onboarding-Review (✅/❌) setzen.")
async def wf_set_staff_channel(inter: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    _set_meta(inter.guild_id, staff_channel_id=channel.id)
    await inter.response.send_message(f"✅ Staff-Review-Kanal: {channel.mention}", ephemeral=True)

@tree.command(name="wf_onboarding_set_newbie_role", description="NEWBIE-Rolle (optional) setzen.")
async def wf_onboarding_set_newbie_role(inter: discord.Interaction, role: discord.Role):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    _set_newbie_role_id(inter.guild_id, role.id)
    await inter.response.send_message(f"✅ NEWBIE-Rolle: {role.mention}", ephemeral=True)

@tree.command(name="wf_onboarding_link", description="Vorhandene Rollen verknüpfen (Weiße Flamme + Tank/Heal/DPS).")
@app_commands.describe(wf="Mitgliedsrolle (Weiße Flamme)", tank="Tank-Rolle", heal="Heal-Rolle", dps="DPS/DD-Rolle")
async def wf_onboarding_link(inter: discord.Interaction, wf: discord.Role, tank: discord.Role, heal: discord.Role, dps: discord.Role):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    _set_guild_role_id(inter.guild_id, wf.id)
    _set_role_ids(inter.guild_id, tank.id, heal.id, dps.id)
    await inter.response.send_message("✅ Verknüpft.", ephemeral=True)

@tree.command(name="wf_onboarding_test", description="Onboarding-DM an dich schicken (Test).")
async def wf_onboarding_test(inter: discord.Interaction):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    await _send_onboarding_dm(inter.user)
    await inter.response.send_message("✅ DM verschickt (falls DMs offen).", ephemeral=True)

# ======================== Admin: Command-Sync ========================
@tree.command(name="wf_admin_sync", description="(Admin) Befehle für diesen Server sofort synchronisieren.")
async def wf_admin_sync(inter: discord.Interaction):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    guild_obj = discord.Object(id=inter.guild_id)
    try:
        tree.copy_global_to(guild=guild_obj)
        synced = await tree.sync(guild=guild_obj)
        await inter.response.send_message(f"✅ Synced {len(synced)} Commands.", ephemeral=True)
    except Exception as e:
        await inter.response.send_message(f"❌ Sync-Fehler: {e}", ephemeral=True)

@tree.command(name="wf_admin_sync_hard", description="(Admin) Harte Neu-Registrierung der Befehle für diesen Server.")
async def wf_admin_sync_hard(inter: discord.Interaction):
    if not is_admin(inter):
        await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
    guild_obj = discord.Object(id=inter.guild_id)
    try:
        tree.clear_commands(guild=guild_obj)
        tree.copy_global_to(guild=guild_obj)
        synced = await tree.sync(guild=guild_obj)
        await inter.response.send_message(f"🧹 Hard-Sync ok. {len(synced)} Commands neu registriert.", ephemeral=True)
    except Exception as e:
        await inter.response.send_message(f"❌ Hard-Sync-Fehler: {e}", ephemeral=True)

# ======================== Lifecycle & Scheduler ========================
def reregister_persistent_views_on_start():
    for msg_id, obj in list(rsvp_store.items()):
        g = client.get_guild(obj["guild_id"])
        if not g:
            continue
        try:
            client.add_view(RaidView(int(msg_id)), message_id=int(msg_id))
        except Exception as e:
            print("add_view (RSVP) failed:", e)

async def _sync_all_guilds_now():
    for g in client.guilds:
        try:
            guild_obj = discord.Object(id=g.id)
            tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)
        except Exception as e:
            print(f"sync for guild {g.id} failed:", e)

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    # Member-Cache laden (für role.members / get_member)
    for g in client.guilds:
        try:
            await g.chunk()
        except Exception as e:
            print("guild.chunk() failed:", e)

    reregister_persistent_views_on_start()
    register_rsvp_slash_commands()

    # Sofortiger Guild-Sync
    await _sync_all_guilds_now()
    print(f"Synced commands for {len(client.guilds)} guild(s).")

    scheduler_loop.start()

@client.event
async def on_guild_join(guild: discord.Guild):
    try:
        guild_obj = discord.Object(id=guild.id)
        tree.copy_global_to(guild=guild_obj)
        await tree.sync(guild=guild_obj)
    except Exception as e:
        print("sync on guild_join failed:", e)

@client.event
async def on_member_join(member: discord.Member):
    if member.bot: return
    await _send_onboarding_dm(member)

@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = _now().replace(second=0, microsecond=0)
    changed = False

    # 1) Event-Reminder
    for guild in client.guilds:
        cfg = configs.get(guild.id)
        if not cfg or not cfg.events:
            continue
        for ev in list(cfg.events.values()):
            channel_id = ev.channel_id or cfg.announce_channel_id
            channel = await ensure_text_channel(guild, channel_id)
            if not channel:
                continue

            start_dt = ev.occurrence_start_on_date(now.date())
            if not start_dt:
                continue
            end_dt = start_dt + timedelta(minutes=ev.duration_min)

            for m in ev.pre_reminders:
                pre_dt = start_dt - timedelta(minutes=m)
                key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:pre{m}"
                if _in_window(now, pre_dt) and key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    body = f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip()
                    if ev.description:
                        body += f"\n{ev.description}"
                    await channel.send(body)
                    post_log.add(key)
                    changed = True

            key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
            if _in_window(now, start_dt) and key not in post_log:
                role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                if ev.description:
                    body += f"\n{ev.description}"
                await channel.send(body)
                post_log.add(key)
                changed = True

            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        del cfg.events[ev.name.lower()]
                        save_all(configs)
                except Exception:
                    pass

    if changed:
        save_post_log(post_log)

    # 2) Wöchentliches Leaderboard (Fr 18:00)
    await _post_weekly_leaderboard_if_due(now)

    # 3) Monatlicher Reset (30. 00:00)
    await _monthly_reset_if_due(now)

@scheduler_loop.before_loop
async def _before_scheduler():
    await client.wait_until_ready()

# ======================== Flammenscore Event Hooks ========================
@client.event
async def on_message(message: discord.Message):
    if not message.guild or message.author.bot:
        return
    b = _score_bucket(message.guild.id, message.author.id)
    b["messages"] += 1
    _save_scores()
    _cache_author(message.id, message.author.id)

@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == client.user.id:
        return
    # gegeben
    b = _score_bucket(payload.guild_id, payload.user_id)
    b["reacts_given"] += 1
    # erhalten
    author_id = message_author_cache.get(payload.message_id)
    if author_id is None:
        try:
            ch = client.get_channel(payload.channel_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                msg = await ch.fetch_message(payload.message_id)
                author_id = msg.author.id
                _cache_author(payload.message_id, author_id)
        except Exception:
            author_id = None
    if author_id and author_id != payload.user_id:
        br = _score_bucket(payload.guild_id, author_id)
        br["reacts_recv"] += 1
    _save_scores()

@client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None:
        return
    if payload.user_id == client.user.id:
        return
    # gegeben
    b = _score_bucket(payload.guild_id, payload.user_id)
    if b["reacts_given"] > 0:
        b["reacts_given"] -= 1
    # erhalten
    author_id = message_author_cache.get(payload.message_id)
    if author_id is None:
        try:
            ch = client.get_channel(payload.channel_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                msg = await ch.fetch_message(payload.message_id)
                author_id = msg.author.id
                _cache_author(payload.message_id, author_id)
        except Exception:
            author_id = None
    if author_id and author_id != payload.user_id:
        br = _score_bucket(payload.guild_id, author_id)
        if br["reacts_recv"] > 0:
            br["reacts_recv"] -= 1
    _save_scores()

@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or not member.guild:
        return
    gid = member.guild.id; uid = member.id
    if before.channel is None and after.channel is not None:
        _voice_start(gid, uid)               # join
    elif before.channel is not None and after.channel is None:
        _voice_end(gid, uid)                 # leave
    elif (before.channel is not None and after.channel is not None and before.channel.id != after.channel.id):
        _voice_end(gid, uid); _voice_start(gid, uid)  # move

# ======================== Start ========================
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
