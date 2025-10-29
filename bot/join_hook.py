# /bot/join_hook.py
from __future__ import annotations
import discord

# Robust import – unabhängig vom Arbeitsverzeichnis
try:
    from bot.event_rsvp_dm import auto_resend_for_new_member
except ModuleNotFoundError:
    from event_rsvp_dm import auto_resend_for_new_member

try:
    from bot.onboarding import send_onboarding_dm
except ModuleNotFoundError:
    from onboarding import send_onboarding_dm


def register_join_hook(client: discord.Client) -> None:
    """
    Registriert einen on_member_join-Handler, ohne die Haupt-bot.py umzubauen.
    - versucht add_listener
    - fallback: überschreibt/verkettet client.on_member_join
    """

    async def _on_member_join(member: discord.Member):
        if member.bot:
            return
        try:
            # 1) Onboarding-DM
            await send_onboarding_dm(member)
        except Exception:
            pass
        try:
            # 2) RSVP-Auto-Resend
            await auto_resend_for_new_member(member)
        except Exception:
            pass

    # 1) Neuerer Weg
    try:
        client.add_listener(_on_member_join, "on_member_join")
        return
    except Exception:
        pass

    # 2) Fallback: vorhandenen Handler sauber verketten oder setzen
    existing = getattr(client, "on_member_join", None)

    if callable(existing):
        async def _chained(member: discord.Member):
            try:
                await existing(member)
            finally:
                try:
                    await _on_member_join(member)
                except Exception:
                    pass
        setattr(client, "on_member_join", _chained)
    else:
        setattr(client, "on_member_join", _on_member_join)
