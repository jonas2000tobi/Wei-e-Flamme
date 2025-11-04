# bot/join_hook.py
# Onboarding-DM robust + erneutes Onboarding bei Rejoin.
from __future__ import annotations
import json
from pathlib import Path
from typing import Callable, Awaitable, Set

import discord

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = DATA_DIR / "onboarding_sent.json"   # {"<guild_id>": ["user_id", ...]}

def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_state(obj: dict) -> None:
    STATE_FILE.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

_sent_cache = _load_state()  # guild_id -> list[str user_id]

def _already_sent(gid: int, uid: int) -> bool:
    arr = set(_sent_cache.get(str(gid), []))
    return str(uid) in arr

def _mark_sent(gid: int, uid: int) -> None:
    arr: Set[str] = set(_sent_cache.get(str(gid), []))
    arr.add(str(uid))
    _sent_cache[str(gid)] = sorted(arr)
    _save_state(_sent_cache)

def _clear_sent(gid: int, uid: int) -> None:
    arr: Set[str] = set(_sent_cache.get(str(gid), []))
    if str(uid) in arr:
        arr.remove(str(uid))
        _sent_cache[str(gid)] = sorted(arr)
        _save_state(_sent_cache)

async def _try_send_onboarding(
    member: discord.Member,
    send_onboarding_dm: Callable[[discord.Member], Awaitable[None]],
    auto_resend_for_new_member: Callable[[discord.Member], Awaitable[None]],
    reason: str
) -> None:
    try:
        if member.bot:
            return
        if _already_sent(member.guild.id, member.id):
            return

        # 1) Onboarding-DM
        try:
            await send_onboarding_dm(member)
            _mark_sent(member.guild.id, member.id)
            print(f"[join_hook] Onboarding-DM an {member} ({reason}) gesendet.")
        except Exception as e:
            # DM kann an Privacy scheitern – trotzdem markieren wir NICHT als sent
            # damit Admins notfalls manuell /onboarding_reset_user nicht brauchen.
            print(f"[join_hook] Onboarding-DM an {member} fehlgeschlagen: {e}")

        # 2) Laufende Raid-Events an neue Member
        try:
            await auto_resend_for_new_member(member)
        except Exception as e:
            print(f"[join_hook] Auto-Resend an {member} fehlgeschlagen: {e}")

    except Exception as e:
        print(f"[join_hook] _try_send_onboarding Fehler: {e}")

def register_join_hook(
    client: discord.Client,
    send_onboarding_dm: Callable[[discord.Member], Awaitable[None]],
    auto_resend_for_new_member: Callable[[discord.Member], Awaitable[None]],
) -> None:
    """
    Listener:
      - on_member_join: sofort versuchen
      - on_member_update: falls Screening aktiv (pending True->False)
      - on_member_remove: Merker löschen, damit Rejoin wieder Onboarding bekommt
    """
    @client.event
    async def on_member_join(member: discord.Member):
        await _try_send_onboarding(member, send_onboarding_dm, auto_resend_for_new_member, reason="join")

    @client.event
    async def on_member_update(before: discord.Member, after: discord.Member):
        try:
            if before.guild.id != after.guild.id:
                return
            if getattr(before, "pending", False) and not getattr(after, "pending", False):
                await _try_send_onboarding(after, send_onboarding_dm, auto_resend_for_new_member, reason="pending->False")
        except Exception as e:
            print(f"[join_hook] on_member_update Fehler: {e}")

    @client.event
    async def on_member_remove(member: discord.Member):
        # Rejoin = wieder wie neu behandeln
        try:
            _clear_sent(member.guild.id, member.id)
            print(f"[join_hook] Merker für {member} entfernt (Leave/Rejoin).")
        except Exception as e:
            print(f"[join_hook] on_member_remove Fehler: {e}")
