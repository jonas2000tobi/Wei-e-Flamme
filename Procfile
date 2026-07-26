from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_release_validator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALIDATION_ALLOW_BYTECODE", "1")
    module = importlib.import_module("tools.validate_release")
    assert module.main() == 0


def test_dashboard_secret_and_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DASHBOARD_SESSION_SECRET", raising=False)
    web = importlib.import_module("dashboard_web.main")
    with pytest.raises(RuntimeError):
        web._session_secret()
    monkeypatch.setenv("DASHBOARD_SESSION_SECRET", "x" * 64)
    assert web._session_secret() == "x" * 64
    assert web._safe_local_path("https://evil.example/x", "/") == "/"
    assert web._safe_local_path("//evil.example/x", "/") == "/"
    assert web._safe_local_path("/member?tab=ec", "/") == "/member?tab=ec"



def test_basic_auth_requires_password(monkeypatch: pytest.MonkeyPatch) -> None:
    web = importlib.import_module("dashboard_web.main")
    monkeypatch.setenv("DASHBOARD_SESSION_SECRET", "z" * 64)
    monkeypatch.setenv("DASHBOARD_AUTH_MODE", "basic")
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    with pytest.raises(RuntimeError):
        web._validate_dashboard_security_config()
    monkeypatch.setenv("DASHBOARD_PASSWORD", "very-strong-test-password")
    web._validate_dashboard_security_config()

def test_member_snapshot_is_reduced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHBOARD_SESSION_SECRET", "y" * 64)
    web = importlib.import_module("dashboard_web.main")
    monkeypatch.setattr(
        web,
        "_snapshot_payload",
        lambda: {
            "ok": True,
            "guild_id": "1",
            "snapshot": {
                "schema_version": 1,
                "generated_at": "now",
                "guild": {"id": 1, "name": "Beer and Buffs", "secret": "no"},
                "auth": {"admin_member_ids": ["99"]},
                "settings": {"internal": True},
                "system": {"database_url": "secret"},
                "profiles": {"items": [{"user_id": "42", "display_name": "Jonas"}, {"user_id": "99"}]},
                "ec": {
                    "balances": {"items": [{"user_id": "42", "balance": 50}, {"user_id": "99", "balance": 999}]},
                    "transactions": {"items": [{"user_id": "42", "amount": 5}, {"user_id": "99", "amount": 999}]},
                },
                "loot": {
                    "needs": {"items": [{"user_id": "42", "item": "Stab"}, {"user_id": "99", "item": "intern"}]},
                    "auctions": {"items": [{
                        "auction_id": "a1", "item_name": "Stab", "status": "active",
                        "bids": [{"user_id": 42, "amount": 10}, {"user_id": 99, "amount": 20}],
                        "eligible_users": [{"user_id": 42}, {"user_id": 99}],
                        "winner_user_id": 99,
                    }]},
                },
                "events": {"items": []},
            },
        },
    )
    token = web._make_token({"user_id": "42", "username": "Jonas", "exp": 9999999999})
    from starlette.requests import Request

    request = Request({
        "type": "http",
        "method": "GET",
        "scheme": "https",
        "server": ("example.test", 443),
        "path": "/api/snapshot",
        "query_string": b"",
        "headers": [(b"cookie", f"{web.SESSION_COOKIE}={token}".encode())],
    })
    payload = web._member_snapshot_payload(request)
    snapshot = payload["snapshot"]
    assert "auth" not in snapshot and "settings" not in snapshot and "system" not in snapshot
    auction = snapshot["loot"]["auctions"]["items"][0]
    assert "bids" not in auction and "eligible_users" not in auction
    assert auction["my_bids"] == [{"amount": 10, "created_at": None}]
    assert snapshot["me"]["ec"]["balances"]["items"] == [{"user_id": "42", "balance": 50}]


def test_ec_request_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("discord")
    dkp = importlib.import_module("bot.dkp_system")
    monkeypatch.setattr(dkp, "dkp_balances", {})
    monkeypatch.setattr(dkp, "dkp_transactions", {})
    monkeypatch.setattr(dkp, "dkp_cfg", {"1": {"start_balance": 40, "weekly_event_limit": 40}})
    monkeypatch.setattr(dkp, "_save_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(dkp, "_phase3_upsert_ec_change", lambda tx: {"ok": True})
    monkeypatch.setattr(dkp, "_railway_log_transaction", lambda tx: None)

    first = dkp._add_transaction(1, 42, 5, "Test", 7, "manual_adjust", meta={"request_id": "same-request"})
    second = dkp._add_transaction(1, 42, 5, "Test", 7, "manual_adjust", meta={"request_id": "same-request"})

    assert first["id"] == second["id"]
    assert dkp.get_balance(1, 42) == 45
    assert len(dkp._gtx(1)) == 2  # Startguthaben + genau eine Korrektur


def test_auction_and_rsvp_locks_serialize() -> None:
    pytest.importorskip("discord")
    import asyncio

    auction = importlib.import_module("bot.loot_auction")
    rsvp = importlib.import_module("bot.event_rsvp_dm")

    async def exercise() -> None:
        auction._AUCTION_LOCKS.clear()
        rsvp._RSVP_LOCKS.clear()
        assert auction._auction_lock(1, "a") is auction._auction_lock(1, "a")
        assert auction._auction_lock(1, "a") is not auction._auction_lock(1, "b")
        assert rsvp._rsvp_lock("e") is rsvp._rsvp_lock("e")

        order: list[str] = []

        async def worker(name: str, pause: float) -> None:
            async with auction._auction_lock(1, "a"):
                order.append(f"{name}:start")
                await asyncio.sleep(pause)
                order.append(f"{name}:end")

        first = asyncio.create_task(worker("first", 0.02))
        await asyncio.sleep(0)
        second = asyncio.create_task(worker("second", 0))
        await asyncio.gather(first, second)
        assert order == ["first:start", "first:end", "second:start", "second:end"]

    asyncio.run(exercise())


def test_voice_fetch_open_only_uses_database_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlite3
    runtime_db = importlib.import_module("bot.runtime_db")
    db_path = tmp_path / "runtime.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE voice_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            left_at TEXT,
            duration_seconds INTEGER,
            event_id TEXT,
            source TEXT,
            metadata_json TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO voice_sessions (guild_id,user_id,channel_id,joined_at,left_at,source) VALUES (1,10,100,'2026-07-26T10:00:00+00:00',NULL,'test')"
    )
    conn.execute(
        "INSERT INTO voice_sessions (guild_id,user_id,channel_id,joined_at,left_at,source) VALUES (1,11,100,'2026-07-26T09:00:00+00:00','2026-07-26T09:30:00+00:00','test')"
    )
    conn.commit()
    conn.close()

    def connect():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(runtime_db, "_INITIALIZED", True)
    monkeypatch.setattr(runtime_db, "_BACKEND", "sqlite")
    monkeypatch.setattr(runtime_db, "_sqlite_connect", connect)
    rows = runtime_db.fetch_voice_sessions(
        1,
        since_iso="1970-01-01T00:00:00+00:00",
        until_iso="2026-07-27T00:00:00+00:00",
        open_only=True,
        limit=100,
    )
    assert [int(row["user_id"]) for row in rows] == [10]


def test_attendance_review_and_queue_share_one_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHBOARD_SESSION_SECRET", "a" * 64)
    web = importlib.import_module("dashboard_web.main")

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple | None]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str, params=None) -> None:
            self.calls.append((" ".join(sql.split()), params))

    class Connection:
        def __init__(self) -> None:
            self.cursor_obj = Cursor()
            self.commits = 0
            self.closed = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self) -> None:
            self.commits += 1

        def close(self) -> None:
            self.closed += 1

    fake = Connection()
    monkeypatch.setattr(web, "_database_url", lambda: "postgresql://test")
    monkeypatch.setattr(web, "_ensure_attendance_review_tables", lambda: None)
    monkeypatch.setattr(web, "_pg_connect", lambda: fake)

    request_id = web._attendance_review_save_and_enqueue(
        1,
        "event-1",
        {"items": [{"user_id": 42, "status": "present"}]},
        {"user_id": "7", "username": "Admin"},
    )
    assert request_id.startswith("attendance_1_event-1_")
    assert fake.commits == 1
    assert fake.closed == 1
    statements = [sql for sql, _ in fake.cursor_obj.calls]
    assert len(statements) == 3
    assert any("dashboard_event_attendance_review" in sql for sql in statements)
    assert any("dashboard_admin_action_log" in sql for sql in statements)
    assert any("dashboard_attendance_action_requests" in sql for sql in statements)


def test_combined_release_guards_are_present() -> None:
    root = ROOT
    dkp = (root / "bot" / "dkp_system.py").read_text(encoding="utf-8")
    rsvp = (root / "bot" / "event_rsvp_dm.py").read_text(encoding="utf-8")
    voice = (root / "bot" / "voice_attendance.py").read_text(encoding="utf-8")
    web = (root / "dashboard_web" / "main.py").read_text(encoding="utf-8")
    assert "stale_processing_max_retries" in dkp
    assert "def apply_attendance_updates" in rsvp
    assert "open_only=True" in voice
    assert "def _attendance_review_save_and_enqueue" in web
    assert 'version="2.2.0"' in web
