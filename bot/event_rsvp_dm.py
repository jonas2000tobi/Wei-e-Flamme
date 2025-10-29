# /bot/event_rsvp_dm.py
# RSVP per DM: Users klicken in der DM (Tank/Heal/DPS/Vielleicht/Abmelden),
# die Übersicht im Server-Channel (Embed) wird live aktualisiert.
# discord.py 2.4.x

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional

import discord
from discord import app_commands
from discord.ui import View, button
from discord.enums import ButtonStyle
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

# --------------------------- Persistenz / Config ---------------------------
TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("bot/data")  # liegen unter /bot/data
DATA_DIR.mkdir(parents=True, exist_ok=True)

RSVP_FILE     = DATA_DIR / "event_rsvp.json"      # Events + Anmeldungen (Übersicht im Server)
DM_CFG_FILE   = DATA_DIR / "event_rsvp_cfg.json"  # Rollen-IDs (Tank/Heal/DPS) + Log-Channel
# cfg[str(guild_id)] = {"TANK": role_id, "HEAL": role_id, "DPS": role_id, "LOG_CH": channel_id}

def _load(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

store: Dict[str, dict] = _load(RSVP_FILE, {})
cfg:   Dict[str, dict] = _load(DM_CFG_FILE, {})

def save_store(): _save(RSVP_FILE, store)
def save_cfg():   _save(DM_CFG_FILE, cfg)

# --------------------------- Utils / Logging ---------------------------
async def _log(client: discord.Client, guild_id: int, text: str):
    """Optionalen Log-Kanal benutzen, wenn gesetzt."""
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

def _init_event_shape(obj: dict):
    """Sichert die Struktur yes/maybe/no ab (defensiv gegen alte Saves)."""
    if "yes" not in obj or not isinstance(obj["yes"], dict):
        obj["yes"] = {"TANK": [], "HEAL": [], "DPS": []}
    for k in ("TANK", "HEAL", "DPS"):
        if k not in obj["yes"] or not isinstance(obj["yes"][k], list):
            obj["yes"][k] = []
    if "maybe" not in obj or not isinstance(obj["maybe"], dict):
        obj["maybe"] = {}
    if "no" not in obj or not isinstance(obj["no"], list):
        obj["no"] = []
    # Zielrolle-Feld immer vorhanden halten (0 = keine Einschränkung)
    obj.setdefault("target_role_id", 0)

def get_role_ids_for_guild(guild_id: int) -> Dict[str, int]:
    g = cfg.get(str(guild_id)) or {}
    return {
        "TANK": int(g.get("TANK", 0) or 0),
        "HEAL": int(g.get("HEAL", 0) or 0),
        "DPS":  int(g.get("DPS",  0) or 0),
    }

def _member_from_event(inter: discord.Interaction, obj: dict) -> Optional[discord.Member]:
    """In DMs ist interaction.guild None → Member über guild_id aus dem Event holen."""
    try:
        if inter.guild is not None:
            return inter.guild.get_member(inter.user.id)
        gid = int(obj.get("guild_id", 0) or 0)
        if not gid:
            return None
        g = inter.client.get_guild(gid)
        if not g:
            return None
        return g.get_member(inter.user.id)
    except Exception:
        return None

def _primary_label(member: Optional[discord.Member], rid_map: Dict[str, int]) -> str:
    """Gibt 'Tank'/'Heal'/'DPS' zurück – robust, auch wenn member None ist."""
    if member is None:
        return ""
    r = member.guild.get_role(rid_map.get("TANK", 0) or 0)
    if r and r in getattr(member, "roles", []): return "Tank"
    r = member.guild.get_role(rid_map.get("HEAL", 0) or 0)
    if r and r in getattr(member, "roles", []): return "Heal"
    r = member.guild.get_role(rid_map.get("DPS", 0) or 0)
    if r and r in getattr(member, "roles", []): return "DPS"
    names = [getattr(rr, "name", "").lower() for rr in getattr(member, "roles", [])]
    if any("tank" in n for n in names): return "Tank"
    if any("heal" in n for n in names): return "Heal"
    if any("dps" in n for n in names) or any("dd" in n for n in names): return "DPS"
    return ""

def build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    when = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"📅 {obj['title']}",
        description=f"{obj.get('description','')}\n\n🕒 Zeit: {when.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple()
    )
    yes = obj["yes"]; maybe = obj["maybe"]; no = obj["no"]

    tank_names = [_mention(guild, int(u)) for u in yes.get("TANK", [])]
    heal_names = [_mention(guild, int(u)) for u in yes.get("HEAL", [])]
    dps_names  = [_mention(guild, int(u)) for u in yes.get("DPS",  [])]

    emb.add_field(name=f"🛡️ Tank ({len(tank_names)})", value="\n".join(tank_names) or "—", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "—", inline=True)
    emb.add_field(name=f"🗡️ DPS ({len(dps_names)})",  value="\n".join(dps_names)  or "—", inline=True)

    maybe_lines = []
    for uid_str, rlab in maybe.items():
        try:
            uid_i = int(uid_str)
        except Exception:
            continue
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "—", inline=False)

    no_names = [_mention(guild, int(u)) for u in no]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "—", inline=False)

    tr_id = int(obj.get("target_role_id", 0) or 0)
    if tr_id:
        r = guild.get_role(tr_id)
        if r:
            emb.add_field(name="🎯 Zielgruppe", value=r.mention, inline=False)

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="(An-/Abmeldung läuft per DM-Buttons)")
    return emb

# --------------------------- DM View ---------------------------
class RaidView(View):
    """Diese View läuft **in der DM**. Sie editiert die Übersicht im Server-Channel."""
    def __init__(self, msg_id: int):
        super().__init__(timeout=None)
        self.msg_id = str(msg_id)

    async def _push_overview(self, inter: discord.Interaction, obj: dict):
        guild = inter.client.get_guild(obj["guild_id"])
        if not guild:
            return
        ch = guild.get_channel(obj["channel_id"])
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return
        try:
            msg = await ch.fetch_message(int(self.msg_id))
        except Exception:
            return
        emb = build_embed(guild, obj)
        try:
            await msg.edit(embed=emb)  # Buttons im Server-Post bleiben wie sie sind
        except Exception:
            pass

    async def _safe_reply(self, inter: discord.Interaction, text: str):
        try:
            await inter.response.send_message(text)
        except discord.InteractionResponded:
            try:
                await inter.followup.send(text)
            except Exception:
                pass
        except Exception:
            pass

    async def _update(self, inter: discord.Interaction, group: str):
        try:
            obj = store.get(self.msg_id)
            if not obj:
                await self._safe_reply(inter, "Dieses Event existiert nicht mehr.")
                return

            _init_event_shape(obj)

            uid = inter.user.id

            # User aus allen Buckets entfernen
            for k in ("TANK", "HEAL", "DPS"):
                obj["yes"][k] = [int(u) for u in obj["yes"].get(k, []) if int(u) != uid]
            obj["no"] = [int(u) for u in obj.get("no", []) if int(u) != uid]
            obj["maybe"].pop(str(uid), None)

            if group in ("TANK", "HEAL", "DPS"):
                obj["yes"][group].append(uid)
                text = f"Angemeldet als **{group}**."
            elif group == "MAYBE":
                member = _member_from_event(inter, obj)
                rid_map = get_role_ids_for_guild(obj["guild_id"])
                label = _primary_label(member, rid_map)  # "Tank"/"Heal"/"DPS" oder ""
                obj["maybe"][str(uid)] = label
                text = "Als **Vielleicht** eingetragen."
            elif group == "NO":
                obj["no"].append(uid)
                text = "Als **Abgemeldet** eingetragen."
            else:
                text = "Aktualisiert."

            save_store()
            await self._push_overview(inter, obj)
            await self._safe_reply(inter, text)

        except Exception as e:
            try:
                await _log(inter.client, store.get(self.msg_id, {}).get("guild_id", 0), f"Button-Fehler: {e!r}")
            except Exception:
                pass
            await self._safe_reply(inter, "❌ Unerwarteter Fehler bei der Anmeldung. Probier es bitte nochmal.")

    @button(label="🛡️ Tank", style=ButtonStyle.primary, custom_id="dm_rsvp_tank")
    async def btn_tank(self, inter: discord.Interaction, _):
        await self._update(inter, "TANK")

    @button(label="💚 Heal", style=ButtonStyle.secondary, custom_id="dm_rsvp_heal")
    async def btn_heal(self, inter: discord.Interaction, _):
        await self._update(inter, "HEAL")

    @button(label="🗡️ DPS", style=ButtonStyle.secondary, custom_id="dm_rsvp_dps")
    async def btn_dps(self, inter: discord.Interaction, _):
        await self._update(inter, "DPS")

    @button(label="❔ Vielleicht", style=ButtonStyle.secondary, custom_id="dm_rsvp_maybe")
    async def btn_maybe(self, inter: discord.Interaction, _):
        await self._update(inter, "MAYBE")

    @button(label="❌ Abmelden", style=ButtonStyle.danger, custom_id="dm_rsvp_no")
    async def btn_no(self, inter: discord.Interaction, _):
        await self._update(inter, "NO")

# --------------------------- Commands / Setup ---------------------------
def _is_admin(inter: discord.Interaction) -> bool:
    perms = getattr(inter.user, "guild_permissions", None)
    return bool(perms and (perms.administrator or perms.manage_guild))

async def setup_rsvp_dm(client: discord.Client, tree: app_commands.CommandTree):
    """
    Registriert:
    - /raid_set_roles_dm     – Tank/Heal/DPS für Maybe-Label
    - /raid_set_log_channel  – optionaler Log-Kanal für Fehler
    - /raid_create_dm        – Erstellt Raid (Server-Übersicht) & verschickt DMs mit Buttons
                               (optional: target_role & image_url)
    """
    # Persistente DM-Views nach Neustart
    for msg_id in list(store.keys()):
        try:
            client.add_view(RaidView(int(msg_id)))
        except Exception:
            pass

    @tree.command(name="raid_set_roles_dm", description="(Admin) Primärrollen (Tank/Heal/DPS) für Maybe-Label setzen")
    @app_commands.describe(tank_role="Rolle: Tank", heal_role="Rolle: Heal", dps_role="Rolle: DPS")
    async def raid_set_roles_dm(
        inter: discord.Interaction,
        tank_role: discord.Role,
        heal_role: discord.Role,
        dps_role: discord.Role
    ):
        if not _is_admin(inter):
            await inter.response.send_message("❌ Nur Admin/Manage Server.", ephemeral=True); return
        c = cfg.get(str(inter.guild_id)) or {}
        c["TANK"] = int(tank_role.id)
        c["HEAL"] = int(heal_role.id)
        c["DPS"]  = int(dps_role.id)
        cfg[str(inter.guild_id)] = c; save_cfg()
        await inter.response.send_message(
            f"✅ Gespeichert:\n🛡️ {tank
