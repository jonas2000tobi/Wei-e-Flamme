# bot/join_hook.py
from __future__ import annotations
import discord

# robust import – funktioniert egal ob event_rsvp_dm in "bot/" liegt oder flach
try:
    from bot.event_rsvp_dm import auto_resend_for_new_member
except ModuleNotFoundError:
    from event_rsvp_dm import auto_resend_for_new_member


def register_join_hook(client: discord.Client) -> None:
    """
    Registriert den on_member_join-Listener ohne deine bestehende bot.py umzubauen.
    Einfach einmal nach dem Client-Objekt aufrufen: register_join_hook(client)
    """

    async def _on_member_join(member: discord.Member):
        if member.bot:
            return
        try:
            await auto_resend_for_new_member(member)
        except Exception:
            # keine harten crashes bei join
            pass

    # Listener anhängen
    client.add_listener(_on_member_join, "on_member_join")
