# bot/join_hook.py
from __future__ import annotations
import discord

# Robust import – funktioniert egal ob event_rsvp_dm in "bot/" liegt oder flach
try:
    from bot.event_rsvp_dm import auto_resend_for_new_member
except ModuleNotFoundError:
    from event_rsvp_dm import auto_resend_for_new_member


def register_join_hook(client: discord.Client) -> None:
    """
    Registriert einen on_member_join-Handler, ohne deine große bot.py umzubauen.
    Funktioniert mit verschiedenen discord.py-Forks/Versionen:
    - versucht add_listener
    - fallback: überschreibt/verkettet client.on_member_join
    """

    async def _on_member_join(member: discord.Member):
        if member.bot:
            return
        try:
            await auto_resend_for_new_member(member)
        except Exception:
            # Kein harter Crash bei Join
            pass

    # 1) Neuerer/klassischer Weg
    try:
        # Manche Forks haben add_listener nicht -> AttributeError
        client.add_listener(_on_member_join, "on_member_join")
        return
    except Exception:
        pass

    # 2) Fallback: vorhandenen Handler sauber verketten oder setzen
    existing = getattr(client, "on_member_join", None)

    if callable(existing):
        async def _chained(member: discord.Member):
            # Erst bestehende Logik laufen lassen
            try:
                await existing(member)
            finally:
                # Dann unser Hook
                try:
                    await _on_member_join(member)
                except Exception:
                    pass
        setattr(client, "on_member_join", _chained)
    else:
        # Kein bestehender Handler – direkt setzen
        setattr(client, "on_member_join", _on_member_join)
