# ====== CONFIG: Rollenerkennung Tank/Heal/DPS ======
# EINE der beiden Methoden nutzen:
# 1) NACH ROLLEN-ID (empfohlen, stabil): IDs als int eintragen
ROLE_IDS = {
    "TANK":  123456789012345678,   # <--- HIER deine Tank-Rollen-ID
    "HEAL":  234567890123456789,   # <--- HIER deine Heal-Rollen-ID
    "DPS":   345678901234567890,   # <--- HIER deine DPS-Rollen-ID
}
# 2) ALTERNATIV nach ROLLEN-NAME (falls IDs dir nicht vorliegen)
ROLE_NAMES = {
    "TANK": "Tank",
    "HEAL": "Heal",
    "DPS":  "DPS",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
RSVP_FILE = DATA_DIR / "event_rsvp.json"   # Persistenz für Button-Klicks

def _load_rsvp() -> dict:
    if RSVP_FILE.exists():
        try:
            return json.loads(RSVP_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_rsvp(blob: dict) -> None:
    RSVP_FILE.write_text(json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8")

def role_label_from_member(member: discord.Member) -> str:
    """Gibt 'Tank'|'Heal'|'DPS' oder '-' zurück, basierend auf Member-Rollen."""
    # erst ID-basierte Erkennung
    if any(r.id == ROLE_IDS.get("TANK") for r in member.roles if ROLE_IDS.get("TANK")):
        return "Tank"
    if any(r.id == ROLE_IDS.get("HEAL") for r in member.roles if ROLE_IDS.get("HEAL")):
        return "Heal"
    if any(r.id == ROLE_IDS.get("DPS")  for r in member.roles if ROLE_IDS.get("DPS")):
        return "DPS"
    # dann Name-basierte Erkennung (fallback)
    names = {r.name.lower() for r in member.roles}
    if ROLE_NAMES.get("TANK") and ROLE_NAMES["TANK"].lower() in names:
        return "Tank"
    if ROLE_NAMES.get("HEAL") and ROLE_NAMES["HEAL"].lower() in names:
        return "Heal"
    if ROLE_NAMES.get("DPS")  and ROLE_NAMES["DPS"].lower()  in names:
        return "DPS"
    return "-"

def _ensure_event_struct(db: dict, message_id: int):
    if str(message_id) not in db:
        db[str(message_id)] = {"yes": [], "maybe": [], "no": []}

async def _render_embed_and_update(message: discord.Message, title: str, description: str, image_url: str | None):
    """Liest RSVP-Daten, baut die Embed (mit Rollen-Zusammenfassung) und editiert die Nachricht."""
    db = _load_rsvp()
    _ensure_event_struct(db, message.id)
    yes_ids   = db[str(message.id)]["yes"]
    maybe_ids = db[str(message.id)]["maybe"]
    no_ids    = db[str(message.id)]["no"]

    # Member-Objekte holen
    guild = message.guild
    def get_member(uid: int) -> discord.Member | None:
        return guild.get_member(uid)

    # YES: Rollen zählen
    role_counts = {"Tank": 0, "Heal": 0, "DPS": 0}
    yes_mentions: list[str] = []
    for uid in yes_ids:
        m = get_member(uid)
        if not m: 
            continue
        lbl = role_label_from_member(m)
        if lbl in role_counts: 
            role_counts[lbl] += 1
        yes_mentions.append(m.mention)

    # MAYBE: mit Rollenhint hinter Name
    maybe_lines: list[str] = []
    for uid in maybe_ids:
        m = get_member(uid)
        if not m:
            continue
        lbl = role_label_from_member(m)
        suffix = f" ({lbl})" if lbl != "-" else ""
        maybe_lines.append(f"{m.mention}{suffix}")

    # NO:
    no_mentions = [get_member(uid).mention for uid in no_ids if get_member(uid)]

    # Embed bauen
    emb = discord.Embed(
        title=title,
        description=description,
        color=discord.Color.green()
    )
    # Zusammenfassung Attendees
    summary = f"**Tank** {role_counts['Tank']}  |  **Heal** {role_counts['Heal']}  |  **DPS** {role_counts['DPS']}"
    emb.add_field(
        name=f"✅ Attendees ({len(yes_mentions)}) – {summary}",
        value=("\n".join(yes_mentions) if yes_mentions else "—"),
        inline=False
    )
    emb.add_field(
        name=f"❓ Maybe ({len(maybe_lines)})",
        value=("\n".join(maybe_lines) if maybe_lines else "—"),
        inline=False
    )
    emb.add_field(
        name=f"❌ No ({len(no_mentions)})",
        value=("\n".join(no_mentions) if no_mentions else "—"),
        inline=False
    )
    if image_url:
        emb.set_image(url=image_url)

    await message.edit(embed=emb)

class RSVPView(discord.ui.View):
    """Buttons (Yes/Maybe/No) + Verknüpfung mit der Event-Nachricht."""
    def __init__(self, title: str, description: str, image_url: str | None, timeout: float | None = None):
        super().__init__(timeout=timeout)
        self.title = title
        self.description = description
        self.image_url = image_url

    async def _set_state(self, interaction: discord.Interaction, target_list: str):
        assert target_list in ("yes", "maybe", "no")
        db = _load_rsvp()
        msg_id = interaction.message.id
        _ensure_event_struct(db, msg_id)
        uid = interaction.user.id

        # aus anderen Listen entfernen
        for k in ("yes", "maybe", "no"):
            if uid in db[str(msg_id)][k]:
                db[str(msg_id)][k].remove(uid)

        # hinzufügen in Ziel-Liste
        db[str(msg_id)][target_list].append(uid)
        _save_rsvp(db)

        await _render_embed_and_update(
            interaction.message, self.title, self.description, self.image_url
        )
        await interaction.response.defer()  # keine Extra-Antwort im Chat

    @discord.ui.button(label="Zusage", style=discord.ButtonStyle.success, emoji="✅")
    async def yes_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_state(interaction, "yes")

    @discord.ui.button(label="Vielleicht", style=discord.ButtonStyle.secondary, emoji="❓")
    async def maybe_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_state(interaction, "maybe")

    @discord.ui.button(label="Absage", style=discord.ButtonStyle.danger, emoji="❌")
    async def no_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_state(interaction, "no")

# --------- Slash-Command: /create_event ----------
@tree.command(name="create_event", description="Event mit Buttons (Zusage/Maybe/No) erstellen.")
@app_commands.describe(
    title="Titel der Veranstaltung",
    datetime_text="Start (frei: z.B. '2025-09-29 22:50') – rein informativ für die Embed",
    description="Beschreibung/Details",
    channel="Ziel-Textkanal",
    image_url="Optionales Bild (Direkt-URL)"
)
async def create_event(
    interaction: discord.Interaction,
    title: str,
    datetime_text: str,
    description: str,
    channel: discord.TextChannel,
    image_url: str = ""
):
    # einfache Infozeile oben in der Embed
    header = f"**Zeit:** {datetime_text}\n" if datetime_text.strip() else ""
    full_desc = header + (description or "")

    view = RSVPView(title=title, description=full_desc, image_url=(image_url or None))

    emb = discord.Embed(
        title=title,
        description=full_desc,
        color=discord.Color.green()
    )
    if image_url:
        emb.set_image(url=image_url)

    # Platzhalter-Felder
    emb.add_field(name="✅ Attendees (0) – **Tank 0 | Heal 0 | DPS 0**", value="—", inline=False)
    emb.add_field(name="❓ Maybe (0)", value="—", inline=False)
    emb.add_field(name="❌ No (0)", value="—", inline=False)

    msg = await channel.send(embed=emb, view=view)

    # RSVP-Struktur initial anlegen
    db = _load_rsvp()
    _ensure_event_struct(db, msg.id)
    _save_rsvp(db)

    await interaction.response.send_message(f"✅ Event gepostet in {channel.mention}.", ephemeral=True)
