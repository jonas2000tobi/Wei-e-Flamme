import os, threading, time, requests
from flask import Flask
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, time as time_cls, date as date_cls
from pathlib import Path
from typing import Dict, List, Optional, Set
import discord
from discord import app_commands
from discord.ext import tasks
from zoneinfo import ZoneInfo
# --- RSVP Imports (für Raid-Teilnahme-Board) ---
from bot_event_rsvp import (
    register_rsvp_commands,  # slash commands /raid_create, /raid_set_roles usw.
    load_rsvp_from_disk,     # lädt gespeicherte Anmeldungen nach Neustart
    persistent_rsvp_view,    # die View mit Buttons (Tank/Heal/DPS/Vielleicht/Abmelden)
)


# --- Mini-Webserver für Railway Healthcheck ---
app = Flask(__name__)

@app.get("/")
def ok():
    return "ok"  # Healthcheck für Railway

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

threading.Thread(target=run_flask, daemon=True).start()
# --- Ende Mini-Webserver ---

# --- Selbst-Ping (optional, hält Free-Pläne aktiv) ---
def keep_alive():
    while True:
        try:
            # Trage hier deine Railway-URL ein, z. B. https://wei-e-flamme.up.railway.app
            requests.get("https://wei-e-flamme.up.railway.app")
        except Exception as e:
            print("Self-ping failed:", e)
        time.sleep(300)  # alle 5 Minuten

threading.Thread(target=keep_alive, daemon=True).start()
# --- Ende Selbst-Ping ---

# TL Event Reminder Discord Bot
# Features:
# - Slash-Commands, serverweit
# - Pro Event eigener Ziel-Channel (Channel-Auswahl beim Erstellen)
# - Beschreibungstext pro Event
# - Wiederkehrend (Wochentage) ODER einmalig (Datum)
# - Vorab-Erinnerungen X Minuten vorher
# - Zeitzone Europe/Berlin
# - JSON-Persistenz (ohne DB)

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TZ = ZoneInfo("Europe/Berlin")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CFG_FILE = DATA_DIR / "guild_configs.json"
POST_LOG_FILE = DATA_DIR / "post_log.json"

# ---- Parsing ----
DOW_MAP = {
    "mon": 0, "monday": 0, "0": 0,
    "tue": 1, "tuesday": 1, "1": 1,
    "wed": 2, "wednesday": 2, "2": 2,
    "thu": 3, "thursday": 3, "3": 3,
    "fri": 4, "friday": 4, "4": 4,
    "sat": 5, "saturday": 5, "5": 5,
    "sun": 6, "sunday": 6, "6": 6,
}

def parse_weekdays(s: str) -> List[int]:
    if not s:
        return []
    days = []
    for part in [p.strip().lower() for p in s.split(",") if p.strip()]:
        if part not in DOW_MAP:
            raise ValueError(f"Unknown weekday '{part}'. Use Mon..Sun or 0..6 (0=Mon).")
        days.append(DOW_MAP[part])
    return sorted(set(days))

def parse_time_hhmm(s: str) -> time_cls:
    try:
        hh, mm = s.strip().split(":")
        return time_cls(int(hh), int(mm))
    except Exception:
        raise ValueError("start_time must be 'HH:MM' 24h.")

def parse_premins(s: str) -> List[int]:
    if not s or not s.strip():
        return []
    mins = []
    for part in [p.strip() for p in s.split(",") if p.strip()]:
        mins.append(int(part))
    return sorted(set([m for m in mins if m > 0]))

def parse_date_yyyy_mm_dd(s: str) -> date_cls:
    try:
        y, m, d = s.strip().split("-")
        return date_cls(int(y), int(m), int(d))
    except Exception:
        raise ValueError("date must be 'YYYY-MM-DD' if provided.")

# ---- Modelle ----
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
    one_time_date: Optional[str] = None

    def next_occurrence_start(self, ref_dt: datetime) -> Optional[datetime]:
        start_t = parse_time_hhmm(self.start_hhmm)
        if self.one_time_date:
            dt_date = parse_date_yyyy_mm_dd(self.one_time_date)
            dt = datetime.combine(dt_date, start_t, tzinfo=TZ)
            return dt if dt >= ref_dt else None
        today = ref_dt.date()
        for add_days in range(0, 8):
            d = today + timedelta(days=add_days)
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
            "events": {k: asdict(v) for k, v in (self.events or {}).items()},
        }

    @staticmethod
    def from_dict(d):
        evs = {k: Event(**v) for k, v in (d.get("events") or {}).items()}
        return GuildConfig(
            guild_id=d["guild_id"],
            announce_channel_id=d.get("announce_channel_id"),
            events=evs,
        )

# ---- Persistenz ----
def load_all() -> Dict[int, GuildConfig]:
    if CFG_FILE.exists():
        raw = json.loads(CFG_FILE.read_text(encoding="utf-8"))
        return {int(gid): GuildConfig.from_dict(cfg) for gid, cfg in raw.items()}
    return {}

def save_all(cfgs: Dict[int, GuildConfig]):
    raw = {str(gid): cfg.to_dict() for gid, cfg in cfgs.items()}
    CFG_FILE.write_text(json.dumps(raw, indent=2), encoding="utf-8")

def load_post_log() -> Set[str]:
    if POST_LOG_FILE.exists():
        return set(json.loads(POST_LOG_FILE.read_text(encoding="utf-8")))
    return set()

def save_post_log(log: Set[str]):
    POST_LOG_FILE.write_text(json.dumps(sorted(list(log))), encoding="utf-8")

# ---- Bot Setup ----
intents = discord.Intents.default()
intents.guilds = True
intents.members = True          # <— WICHTIG
intents.message_content = False
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


configs: Dict[int, GuildConfig] = load_all()
post_log: Set[str] = load_post_log()

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
    perms = interaction.user.guild_permissions if interaction.user else None
    return bool(perms and (perms.administrator or perms.manage_guild))

# ---- Commands ----
@tree.command(name="set_announce_channel", description="Standard-Kanal für Erinnerungen setzen.")
@app_commands.describe(channel="Ziel-Textkanal")
async def set_announce_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    cfg.announce_channel_id = channel.id
    save_all(configs)
    await interaction.response.send_message(f"✅ Standard-Kanal: {channel.mention}", ephemeral=True)

@tree.command(name="add_event", description="Event anlegen (wiederkehrend ODER einmalig).")
@app_commands.describe(
    name="Event-Name",
    weekdays='Kommagetrennt: "Mon,Wed,Sat" oder "0,3,5" (0=Mon..6=Sun). Ignoriert, wenn date gesetzt ist.',
    start_time='Start "HH:MM" (Europa/Berlin)',
    duration_min="Dauer in Minuten",
    pre_reminders='Vorab-Minuten, z. B. "30,10,5" (optional)',
    mention_role="(optional) Rolle, die gepingt werden soll",
    post_channel="(optional) Kanal für dieses Event (sonst Standard)",
    description="(optional) Zusatztext unter der Erinnerung",
    date='(optional) Einmalig: Datum "YYYY-MM-DD" – ignoriert weekdays',
)
async def add_event(
    interaction: discord.Interaction,
    name: str,
    weekdays: str,
    start_time: str,
    duration_min: int,
    pre_reminders: str = "",
    mention_role: Optional[discord.Role] = None,
    post_channel: Optional[discord.TextChannel] = None,
    description: str = "",
    date: str = "",
):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return

    try:
        t = parse_time_hhmm(start_time)
        pre = parse_premins(pre_reminders or "")
        one_time_date = None
        days: List[int] = []

        if date.strip():
            one_time_date = parse_date_yyyy_mm_dd(date.strip()).isoformat()
        else:
            days = parse_weekdays(weekdays)
            if not days:
                await interaction.response.send_message("❌ Entweder 'weekdays' angeben ODER 'date' setzen.", ephemeral=True)
                return
    except ValueError as e:
        await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        return

    cfg = get_or_create_guild_cfg(interaction.guild_id)
    ev = Event(
        name=name.strip(),
        weekdays=days,
        start_hhmm=f"{t.hour:02d}:{t.minute:02d}",
        duration_min=duration_min,
        pre_reminders=pre,
        mention_role_id=(mention_role.id if mention_role else None),
        channel_id=(post_channel.id if post_channel else None),
        description=(description or "").strip(),
        one_time_date=one_time_date,
    )
    cfg.events[name.lower()] = ev
    save_all(configs)

    when = one_time_date if one_time_date else ",".join(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d] for d in ev.weekdays)
    target = post_channel.mention if post_channel else (interaction.guild.get_channel(cfg.announce_channel_id).mention if cfg.announce_channel_id else "—")
    await interaction.response.send_message(f"✅ Event **{ev.name}** angelegt → {when} {ev.start_hhmm}, Dauer {ev.duration_min} Min, Kanal {target}.", ephemeral=True)

@tree.command(name="list_events", description="Alle Events anzeigen.")
async def list_events(interaction: discord.Interaction):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if not cfg.events:
        await interaction.response.send_message("ℹ️ Keine Events konfiguriert.", ephemeral=True)
        return
    lines = []
    dow_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    for ev in cfg.events.values():
        when = ev.one_time_date if ev.one_time_date else ",".join(dow_names[d] for d in ev.weekdays)
        role = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else "—"
        chan = f"<#{ev.channel_id}>" if ev.channel_id else (f"<#{cfg.announce_channel_id}>" if cfg.announce_channel_id else "—")
        pre = ", ".join(str(m) for m in ev.pre_reminders) if ev.pre_reminders else "—"
        desc = (ev.description[:60] + "…") if ev.description and len(ev.description) > 60 else (ev.description or "—")
        lines.append(f"• **{ev.name}** — {when} {ev.start_hhmm} ({ev.duration_min} Min), Pre: {pre}, Role: {role}, Channel: {chan}, Desc: {desc}")
    msg = "\n".join(lines)
    await interaction.response.send_message(msg[:1990], ephemeral=True)

@tree.command(name="remove_event", description="Event löschen.")
async def remove_event(interaction: discord.Interaction, name: str):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ Admin-/Manage Server-Recht nötig.", ephemeral=True)
        return
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    if name.lower() in cfg.events:
        del cfg.events[name.lower()]
        save_all(configs)
        await interaction.response.send_message(f"✅ Event **{name}** gelöscht.", ephemeral=True)
    else:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)

@tree.command(name="test_event_ping", description="Test-Ping (keine Planung).")
async def test_event_ping(interaction: discord.Interaction, name: str):
    cfg = get_or_create_guild_cfg(interaction.guild_id)
    ev = cfg.events.get(name.lower())
    if not ev:
        await interaction.response.send_message("❌ Event nicht gefunden.", ephemeral=True)
        return

    channel = await ensure_text_channel(interaction.guild, ev.channel_id or cfg.announce_channel_id)
    if not channel:
        await interaction.response.send_message("❌ Kein Zielkanal. Setze /set_announce_channel oder nutze post_channel beim Event.", ephemeral=True)
        return

    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
    body = f"🔔 **{ev.name}** — Test-Ping {role_mention}".strip()
    if ev.description:
        body += f"\n{ev.description}"
    await channel.send(body)
    await interaction.response.send_message("✅ Test-Ping raus.", ephemeral=True)

# ---- Scheduler ----
@tasks.loop(seconds=30.0)
async def scheduler_loop():
    now = datetime.now(TZ).replace(second=0, microsecond=0)
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
                if pre_dt == now and key not in post_log:
                    role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                    body = f"⏳ **{ev.name}** startet in **{m} Min** ({start_dt.strftime('%H:%M')} Uhr). {role_mention}".strip()
                    if ev.description:
                        body += f"\n{ev.description}"
                    await channel.send(body)
                    post_log.add(key)

            key = f"{guild.id}:{ev.name}:{start_dt.isoformat()}:start"
            if start_dt == now and key not in post_log:
                role_mention = f"<@&{ev.mention_role_id}>" if ev.mention_role_id else ""
                body = f"🚀 **{ev.name}** ist **jetzt live**! Läuft bis {end_dt.strftime('%H:%M')} Uhr. {role_mention}".strip()
                if ev.description:
                    body += f"\n{ev.description}"
                await channel.send(body)
                post_log.add(key)

            if ev.one_time_date:
                try:
                    d = parse_date_yyyy_mm_dd(ev.one_time_date)
                    if now.date() > d:
                        del cfg.events[ev.name.lower()]
                        save_all(configs)
                except Exception:
                    pass

    save_post_log(post_log)

@scheduler_loop.before_loop
async def _before_scheduler():
    await client.wait_until_ready()

@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")

    # ① RSVP-Daten von der Platte laden (damit Buttons nach Neustart wieder funktionieren)
    load_rsvp_from_disk()

    # ② Persistent View registrieren (Buttons sind nach Restarts sofort klickbar)
    client.add_view(persistent_rsvp_view)

    # ③ Slash-Commands für RSVP registrieren/syncen
    try:
        await register_rsvp_commands(tree)
        synced = await tree.sync()
        print(f"Synced {len(synced)} commands.")
    except Exception as e:
        print("Command sync failed:", e)

    # falls du einen Scheduler hast, hier wieder starten:
    # scheduler_loop.start()


# ====== RSVP Event mit Bild, Rollen-Auswertung und Buttons ======
# Speichert Teilnahme-Status in data/event_rsvp.json (persistiert Neustarts).
# Buttons sind persistent (timeout=None); on_ready() registriert sie erneut.

from collections import defaultdict

# --- KONFIG: Rollen-Erkennung Tank/Heal/DPS ---
# Variante A (stabiler): trage hier die ID eurer Tank/Heal/DPS-Rollen ein (Zahlen).
ROLE_IDS = {
    "TANK":  0,  # z.B. 123456789012345678  (0 lassen, wenn ihr nur Namen nutzt)
    "HEAL":  0,
    "DPS":   0,
}
# Variante B (Fallback): Erkennung über Namen, wenn ROLE_IDS=0 sind
ROLE_NAMES = {
    "TANK": "Tank",
    "HEAL": "Heal",
    "DPS":  "DPS",
}

DATA_DIR.mkdir(exist_ok=True)
RSVP_FILE = DATA_DIR / "event_rsvp.json"

def _load_rsvp() -> dict:
    if RSVP_FILE.exists():
        try:
            return json.loads(RSVP_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_rsvp(blob: dict) -> None:
    RSVP_FILE.write_text(json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8")

def _role_label_from_member(member: discord.Member) -> str:
    """Gibt 'Tank' | 'Heal' | 'DPS' | 'Unassigned' zurück – nach IDs oder Namen."""
    # Nach IDs (wenn gesetzt)
    if ROLE_IDS.get("TANK"):
        if any(r.id == ROLE_IDS["TANK"] for r in member.roles):
            return "Tank"
    if ROLE_IDS.get("HEAL"):
        if any(r.id == ROLE_IDS["HEAL"] for r in member.roles):
            return "Heal"
    if ROLE_IDS.get("DPS"):
        if any(r.id == ROLE_IDS["DPS"] for r in member.roles):
            return "DPS"

    # Fallback: nach Namen
    tank_n = ROLE_NAMES.get("TANK", "").lower()
    heal_n = ROLE_NAMES.get("HEAL", "").lower()
    dps_n  = ROLE_NAMES.get("DPS", "").lower()
    rl = [r.name.lower() for r in member.roles]
    if tank_n and any(tank_n == x for x in rl):
        return "Tank"
    if heal_n and any(heal_n == x for x in rl):
        return "Heal"
    if dps_n and any(dps_n == x for x in rl):
        return "DPS"

    return "Unassigned"

def _ensure_event(store: dict, message_id: str, payload: dict) -> dict:
    """Legt die Datenstruktur für dieses Event an, falls neu."""
    if message_id not in store:
        store[message_id] = {
            "title": payload.get("title", ""),
            "when_text": payload.get("when_text", ""),
            "channel_id": payload.get("channel_id"),
            "image_url": payload.get("image_url", ""),
            "description": payload.get("description", ""),
            # Teilnehmer
            "yes": {},      # user_id -> {"name": "...", "role": "Tank/Heal/DPS/..."}
            "maybe": {},    # dito
            "no": {},       # dito
        }
    return store[message_id]

def _format_embed_for_event(ev: dict, guild: discord.Guild) -> discord.Embed:
    """Baut den hübschen Embed inkl. Bild & Spalten für Yes/Maybe/No mit Rollen."""
    title = ev.get("title") or "Event"
    desc  = ev.get("description") or ""
    when  = ev.get("when_text") or ""

    embed = discord.Embed(
        title=title,
        description=f"{desc}\n\n**Zeit:** {when}",
        color=discord.Color.blue()
    )
    if ev.get("image_url"):
        embed.set_image(url=ev["image_url"])

    # YES nach Rolle gruppieren
    yes_by_role = defaultdict(list)
    for uid, info in ev.get("yes", {}).items():
        yes_by_role[info.get("role","Unassigned")].append(info.get("name", f"<@{uid}>"))

    # YES Feld
    if ev.get("yes"):
        parts = []
        for role in ("Tank","Heal","DPS","Unassigned"):
            if yes_by_role.get(role):
                parts.append(f"**{role} ({len(yes_by_role[role])})**: " + ", ".join(yes_by_role[role]))
        embed.add_field(name="✅ Zusage", value="\n".join(parts)[:1024] or "—", inline=False)
    else:
        embed.add_field(name="✅ Zusage", value="—", inline=False)

    # MAYBE Liste mit (Rolle)
    if ev.get("maybe"):
        maybe_list = []
        for uid, info in ev["maybe"].items():
            nm = info.get("name", f"<@{uid}>")
            rl = info.get("role","Unassigned")
            maybe_list.append(f"{nm} ({rl})")
        embed.add_field(name="❔ Vielleicht", value="\n".join(maybe_list)[:1024] or "—", inline=False)
    else:
        embed.add_field(name="❔ Vielleicht", value="—", inline=False)

    # NO Liste
    if ev.get("no"):
        no_list = [info.get("name", f"<@{uid}>") for uid, info in ev["no"].items()]
        embed.add_field(name="❌ Abgesagt", value=", ".join(no_list)[:1024] or "—", inline=False)
    else:
        embed.add_field(name="❌ Abgesagt", value="—", inline=False)

    return embed

class RSVPView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # persistent

    @discord.ui.button(label="Ja", style=discord.ButtonStyle.success, emoji="✅", custom_id="rsvp_yes")
    async def rsvp_yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _handle_rsvp_click(interaction, status="yes")

    @discord.ui.button(label="Vielleicht", style=discord.ButtonStyle.primary, emoji="❔", custom_id="rsvp_maybe")
    async def rsvp_maybe(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _handle_rsvp_click(interaction, status="maybe")

    @discord.ui.button(label="Nein", style=discord.ButtonStyle.danger, emoji="❌", custom_id="rsvp_no")
    async def rsvp_no(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _handle_rsvp_click(interaction, status="no")

async def _handle_rsvp_click(interaction: discord.Interaction, status: str):
    """Schreibt den Klick in die JSON und aktualisiert den Embed."""
    if not interaction.guild or not interaction.message:
        await interaction.response.send_message("Nur in einem Server-Event nutzbar.", ephemeral=True)
        return

    store = _load_rsvp()
    mid = str(interaction.message.id)
    ev = store.get(mid)
    if not ev:
        await interaction.response.send_message("Daten für dieses Event fehlen (neu posten?).", ephemeral=True)
        return

    user = interaction.user
    uid = str(user.id)
    name = getattr(user, "display_name", user.name)
    role = _role_label_from_member(user)

    # aus allen Listen entfernen
    for bucket in ("yes","maybe","no"):
        ev.get(bucket, {}).pop(uid, None)

    # in Ziel-Liste eintragen
    ev.setdefault(status, {})[uid] = {"name": name, "role": role}

    # Embed aktualisieren
    embed = _format_embed_for_event(ev, interaction.guild)
    try:
        await interaction.message.edit(embed=embed, view=RSVPView())
    except Exception as e:
        print("edit failed:", e)

    _save_rsvp(store)
    await interaction.response.send_message("✅ Aktualisiert.", ephemeral=True)

@tree.command(name="event_create", description="Erstellt ein RSVP-Event mit Bild und Rollen-Tracking.")
@app_commands.describe(
    title="Titel des Events",
    date="Datum YYYY-MM-DD", 
    time="Zeit HH:MM (24h)", 
    channel="Channel für den Post",
    image_url="(optional) Bild-URL für den Embed",
    description="(optional) Beschreibung unter dem Titel"
)
async def event_create(
    interaction: discord.Interaction,
    title: str,
    date: str,
    time: str,
    channel: discord.TextChannel,
    image_url: str = "",
    description: str = "",
):
    # Zeit-Text hübsch bauen (wir nutzen deinen TZ=Europe/Berlin)
    when_text = f"{date} {time} (Europa/Berlin)"

    # Embed initial
    ev_blob = {
        "title": title.strip(),
        "when_text": when_text,
        "channel_id": channel.id,
        "image_url": image_url.strip(),
        "description": description.strip(),
    }
    embed = _format_embed_for_event(
        {**ev_blob, "yes": {}, "maybe": {}, "no": {}},
        interaction.guild
    )

    view = RSVPView()
    msg = await channel.send(embed=embed, view=view)

    # Persistenz ablegen
    store = _load_rsvp()
    _ensure_event(store, str(msg.id), ev_blob)
    _save_rsvp(store)

    await interaction.response.send_message(f"✅ Event erstellt in {channel.mention}.", ephemeral=True)

# Beim Start persistenten View registrieren (damit Buttons nach Neustart gehen)
@client.event
async def on_ready():
    # Deine bestehende on_ready() ruft scheduler_loop.start() etc – also NICHT ersetzen,
    # sondern nur sicherstellen, dass die View registriert ist.
    try:
        client.add_view(RSVPView())
    except Exception as e:
        print("add_view failed:", e)
    # (Dein bestehender on_ready()-Inhalt bleibt wie gehabt.)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_BOT_TOKEN environment variable.")
    client.run(TOKEN)
