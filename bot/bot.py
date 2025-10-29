import os
import discord
from discord.ext import commands
from discord import app_commands

# ---------- CONFIG ----------
# Intents für DMs, Member und Guild Commands
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.dm_messages = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- TOKEN HANDLING ----------
def _get_token() -> str | None:
    """Versucht mehrere mögliche Env-Variablen für das Discord-Token."""
    for key in ["DISCORD_TOKEN", "DISCORD_BOT_TOKEN", "TOKEN"]:
        val = os.getenv(key)
        if val and val.strip():
            print(f"✅ Token gefunden unter Variable: {key}")
            return val.strip()
    print("❌ Kein gültiges Token in Environment-Variablen gefunden!")
    return None


# ---------- EVENTS ----------
@bot.event
async def on_ready():
    print(f"✅ Eingeloggt als {bot.user} (ID: {bot.user.id})")
    print("✅ Lade Cogs ...")
    await load_extensions()
    print("✅ Alle Module geladen.")
    try:
        synced = await bot.tree.sync()
        print(f"✅ Slash-Commands synchronisiert: {len(synced)}")
    except Exception as e:
        print(f"⚠️ Fehler beim Synchronisieren der Slash-Commands: {e}")


async def load_extensions():
    """Lädt alle Cogs aus dem Ordner bot/cogs automatisch."""
    for filename in os.listdir(os.path.join(os.path.dirname(__file__), "cogs")):
        if filename.endswith(".py") and not filename.startswith("__"):
            ext_name = f"bot.cogs.{filename[:-3]}"
            try:
                await bot.load_extension(ext_name)
                print(f"🧩 Modul geladen: {ext_name}")
            except Exception as e:
                print(f"❌ Fehler beim Laden von {ext_name}: {e}")


# ---------- BASIC COMMANDS ----------
@bot.tree.command(name="ping", description="Testet, ob der Bot reagiert.")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("🏓 Pong! Bot läuft einwandfrei.", ephemeral=True)


# ---------- MAIN ----------
def main():
    print("🚀 Starte Weiße Flamme Discord-Bot ...")

    # Debug: alle relevanten Keys anzeigen (nicht die Werte)
    debug_keys = [k for k in os.environ.keys() if "DISCORD" in k or "TOKEN" in k]
    print(f"ENV DEBUG – gefundene Keys: {debug_keys}")

    token = _get_token()
    if not token:
        print("❌ Kein Token gefunden! Bitte setze eine Umgebungsvariable DISCORD_TOKEN oder DISCORD_BOT_TOKEN.")
        return

    try:
        bot.run(token)
    except Exception as e:
        print(f"❌ Fehler beim Starten des Bots: {e}")


if __name__ == "__main__":
    main()
