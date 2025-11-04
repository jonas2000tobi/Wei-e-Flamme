# bot/join_hook.py
# Robust: Onboarding-DM auch bei Discord-"Review erforderlich" (pending=True).
# Schickt DM sowohl beim Join als auch NACH dem Regeln-Akzeptieren (pending->False),
# verhindert doppelte DMs mit einer kleinen Persistenz.

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


async def _try_send_onboarding(
    member: discord.Member,
    send_onboarding_dm: Callable[[discord.Member], Awaitable[None]],
    auto_resend_for_new_member: Callable[[discord.Member], Awaitable[None]],
    reason: str
) -> None:
    """Schickt Onboarding-DM + Event-Resend, wenn noch nicht gesendet."""
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
            # DM kann an Privacy scheitern – stillschweigend ignorieren.
            print(f"[join_hook] Onboarding-DM an {member} fehlgeschlagen: {e}")

        # 2) Laufende Raid-Events an neue Member (falls vorhanden)
        try:
            await auto_resend_for_new_member(member)
        except Exception as e:
            print(f"[join_hook] Auto-Resend-Events an {member} fehlgeschlagen: {e}")

    except Exception as e:
        print(f"[join_hook] _try_send_onboarding Fehler: {e}")


def register_join_hook(
    client: discord.Client,
    send_onboarding_dm: Callable[[discord.Member], Awaitable[None]],
    auto_resend_for_new_member: Callable[[discord.Member], Awaitable[None]],
) -> None:
    """
    Registriert zwei Listener:
      - on_member_join: sofort versuchen zu DM'en
      - on_member_update: falls Discord Screening aktiv ist, erst NACH pending->False DM schicken
    """
    # Sofort bei Join probieren (funktioniert oft auch mit pending=True)
    @client.event
    async def on_member_join(member: discord.Member):
        await _try_send_onboarding(member, send_onboarding_dm, auto_resend_for_new_member, reason="join")

    # Wenn Screening aktiv ist: pending geht von True -> False, DANN nochmal sicher DM versuchen
    @client.event
    async def on_member_update(before: discord.Member, after: discord.Member):
        try:
            # Nur reagieren, wenn es tatsächlich die gleiche Person und Guild ist
            if before.guild.id != after.guild.id:
                return
            # Nur wenn pending von True auf False wechselt (Regeln akzeptiert / Review abgeschlossen)
            if getattr(before, "pending", False) and not getattr(after, "pending", False):
                await _try_send_onboarding(after, send_onboarding_dm, auto_resend_for_new_member, reason="pending->False")
        except Exception as e:
            print(f"[join_hook] on_member_update Fehler: {e}")
