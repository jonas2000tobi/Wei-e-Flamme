# bot/join_hook.py
# Robust: Onboarding-DM + Auto-Resend bei Join/Screening-Freischaltung,
# und Merker-Löschung bei Leave (damit Rejoin wieder DM bekommt).

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
    arr = _sent_cache.get(str(gid), [])
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
    reason: str,
) -> None:
    try:
        if member.bot:
            return

        # Schon gesendet? -> nichts tun
        if _already_sent(member.guild.id, member.id):
            print(f"[join_hook] Skip: {member} bereits bedient.")
            return

        # 1) Onboarding-DM
        try:
            await send_onboarding_dm(member)
            _mark_sent(member.guild.id, member.id)
            print(f"[join_hook] Onboarding-DM an {member} gesendet (reason={reason}).")
        except Exception as e:
            # NICHT markieren, damit spätere Versuche/Manuell möglich sind
            print(f"[join_hook] Onboarding-DM an {member} fehlgeschlagen (reason={reason}): {e!r}")

        # 2) Laufende Raid-Events als DM (immer versuchen, unabhängig davon ob 1) geklappt hat)
        try:
            await auto_resend_for_new_member(member)
            print(f"[join_hook] Auto-Resend für {member} ausgeführt.")
        except Exception as e:
            print(f"[join_hook] Auto-Resend an {member} fehlgeschlagen: {e!r}")

    except Exception as e:
        print(f"[join_hook] _try_send_onboarding Fehler: {e!r}")

def _chain_listener(client: discord.Client, name: str, ours):
    """
    Sicheres Registrieren:
    - Bevorzugt: client.add_listener(ours, name)
    - Fallback: bestehende client.on_... in Kaskade erweitern (nicht überschreiben)
    """
    # Bevorzugt: add_listener
    if hasattr(client, "add_listener"):
        try:
            client.add_listener(ours, name)  # type: ignore[attr-defined]
            print(f"[join_hook] add_listener -> {name}")
            return
        except Exception as e:
            print(f"[join_hook] add_listener failed for {name}: {e!r}")

    # Fallback: Kaskade
    existing = getattr(client, name, None)
    if existing and callable(existing):
        async def chained(*args, **kwargs):
            try:
                await existing(*args, **kwargs)
            except Exception as e:
                print(f"[join_hook] existing {name} error: {e!r}")
            try:
                await ours(*args, **kwargs)
            except Exception as e:
                print(f"[join_hook] ours {name} error: {e!r}")
        setattr(client, name, chained)
        print(f"[join_hook] chained -> {name}")
    else:
        setattr(client, name, ours)
        print(f"[join_hook] set -> {name}")

def register_join_hook(
    client: discord.Client,
    send_onboarding_dm: Callable[[discord.Member], Awaitable[None]],
    auto_resend_for_new_member: Callable[[discord.Member], Awaitable[None]],
) -> None:
    """
    Registriert Listener:
      - on_member_join: sofort versuchen
      - on_member_update: falls Membership Screening aktiv (pending True->False)
      - on_member_remove: Merker löschen (damit Rejoin wieder Onboarding erhält)
    """

    # Sanity-Checks (helfen beim Diagnostizieren)
    intents = getattr(client, "intents", None)
    if not intents or not intents.members:
        print("⚠️ [join_hook] WARN: Intents.members ist AUS! Aktiviere im Code UND im Dev-Portal 'Server Members Intent'.")
    else:
        print("✅ [join_hook] Intents.members aktiv.")

    async def _on_member_join(member: discord.Member):
        print(f"[join_hook] on_member_join: {member} (pending={getattr(member,'pending', None)})")
        await _try_send_onboarding(member, send_onboarding_dm, auto_resend_for_new_member, reason="join")

    async def _on_member_update(before: discord.Member, after: discord.Member):
        try:
            if getattr(before, "guild", None) is None or getattr(after, "guild", None) is None:
                return
            if before.guild.id != after.guild.id:
                return
            if getattr(before, "pending", False) and not getattr(after, "pending", False):
                print(f"[join_hook] on_member_update: {after} pending True->False")
                await _try_send_onboarding(after, send_onboarding_dm, auto_resend_for_new_member, reason="pending->False")
        except Exception as e:
            print(f"[join_hook] on_member_update Fehler: {e!r}")

    async def _on_member_remove(member: discord.Member):
        try:
            _clear_sent(member.guild.id, member.id)
            print(f"[join_hook] Merker für {member} entfernt (Leave/Rejoin).")
        except Exception as e:
            print(f"[join_hook] on_member_remove Fehler: {e!r}")

    _chain_listener(client, "on_member_join", _on_member_join)
    _chain_listener(client, "on_member_update", _on_member_update)
    _chain_listener(client, "on_member_remove", _on_member_remove)
