import discord
from datetime import datetime

def _mention(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.mention if m else f"<@{uid}>"

def _get_guild_role_id(guild_id: int) -> int:
    # Dummy-Implementierung für die Rolle, bitte bei dir anpassen.
    return 0

def _build_embed(guild: discord.Guild, obj: dict) -> discord.Embed:
    dt = datetime.fromisoformat(obj["when_iso"])
    emb = discord.Embed(
        title=f"{obj['title']}",
        description=f"{obj.get('description','')}" + f"\n\n🕓 Zeit: {dt.strftime('%a, %d.%m.%Y %H:%M')} (Europe/Berlin)",
        color=discord.Color.blurple(),
    )

    tank_names = [_mention(guild, u) for u in obj["yes"]["TANK"]]
    heal_names = [_mention(guild, u) for u in obj["yes"]["HEAL"]]
    dps_names  = [_mention(guild, u) for u in obj["yes"]["DPS"]]

    emb.add_field(name=f"🛡 Tank ({len(tank_names)})", value="\n".join(tank_names) or "-", inline=True)
    emb.add_field(name=f"💚 Heal ({len(heal_names)})", value="\n".join(heal_names) or "-", inline=True)
    emb.add_field(name=f"⚔ DPS ({len(dps_names)})", value="\n".join(dps_names) or "-", inline=True)

    maybe_lines = []
    for uid, rlab in obj["maybe"].items():
        uid_i = int(uid)
        label = f" ({rlab})" if rlab else ""
        maybe_lines.append(f"{_mention(guild, uid_i)}{label}")
    emb.add_field(name=f"❔ Vielleicht ({len(maybe_lines)})", value="\n".join(maybe_lines) or "-", inline=False)

    no_names = [_mention(guild, u) for u in obj["no"]]
    emb.add_field(name=f"❌ Abgemeldet ({len(no_names)})", value="\n".join(no_names) or "-", inline=False)

    # === Gildenrollen-Statistik (z. B. Weisse Flamme) ===
    gr_id = _get_guild_role_id(guild.id)
    gr = guild.get_role(gr_id)
    if gr:
        total = len(gr.members)
        voted_ids = set(
            obj["yes"]["TANK"] + obj["yes"]["HEAL"] + obj["yes"]["DPS"]
            + [int(k) for k in obj["maybe"].keys()]
            + obj["no"]
        )
        voted_in_guild = 0
        for uid in voted_ids:
            m = guild.get_member(uid)
            if m and gr in m.roles:
                voted_in_guild += 1
        emb.add_field(
            name=f"🏰 {gr.name}",
            value=f"{voted_in_guild} / {total} haben abgestimmt",
            inline=False,
        )

    if obj.get("image_url"):
        emb.set_image(url=obj["image_url"])

    emb.set_footer(text="Klicke unten auf die Buttons, um dich anzumelden.")
    return emb
