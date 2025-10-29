# /bot/join_hook.py
from __future__ import annotations
import discord

try:
    from bot.event_rsvp_dm import auto_resend_for_new_member  # type: ignore
except ModuleNotFoundError:
    from event_rsvp_dm import auto_resend_for_new_member  # type: ignore

try:
    from bot.onboarding import send_onboarding_dm  # type: ignore
except ModuleNotFoundError:
    from onboarding import send_onboarding_dm  # type: ignore


def register_join_hook(client: discord.Client) -> None:
    async def _on_member_join(member: discord.Member):
        if member.bot:
            return
        try:
            await send_onboarding_dm(member)
        except Exception:
            pass
        try:
            await auto_resend_for_new_member(member)
        except Exception:
            pass

    try:
        client.add_listener(_on_member_join, "on_member_join")
        return
    except Exception:
        pass

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
