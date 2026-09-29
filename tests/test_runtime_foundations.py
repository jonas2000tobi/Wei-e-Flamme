from __future__ import annotations

import json
from pathlib import Path

from bot.aion2_game import normalize_faction, role_for_class
from bot.role_keys import normalize_role_key, normalize_yes_buckets


def test_support_is_canonical_role() -> None:
    assert normalize_role_key("heal") == "SUPPORT"
    assert normalize_role_key("healer") == "SUPPORT"
    assert normalize_role_key("heiler") == "SUPPORT"
    assert normalize_role_key("support") == "SUPPORT"
    assert normalize_yes_buckets({"HEAL": [1], "SUPPORT": [2], "TANK": [3]}) == {
        "TANK": [3],
        "SUPPORT": [1, 2],
        "DPS": [],
        "BANK": [],
    }


def test_aion2_roles_and_factions() -> None:
    assert role_for_class("Kleriker") == "SUPPORT"
    assert role_for_class("Cleric") == "SUPPORT"
    assert role_for_class("Kantor") == "SUPPORT"
    assert role_for_class("Chanter") == "SUPPORT"
    assert role_for_class("Templer") == "TANK"
    assert normalize_faction("Asmodian") == "ASMODIA"
    assert normalize_faction("Elyos") == "ELYOS"


def test_runtime_db_sqlite_dev_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    from bot import runtime_db

    old_path = runtime_db.SQLITE_PATH
    old_initialized = runtime_db._INITIALIZED
    old_backend = runtime_db._BACKEND
    try:
        runtime_db.SQLITE_PATH = tmp_path / "runtime.sqlite3"
        runtime_db._INITIALIZED = False
        runtime_db._BACKEND = "unknown"
        assert runtime_db.init_runtime_db()["backend"] == "sqlite"
        runtime_db.upsert_aion2_profile(
            1,
            2,
            character_name="Test",
            class_name="Kleriker",
            main_role="HEAL",
            faction="ELYOS",
        )
        assert runtime_db.get_aion2_profile(1, 2)["main_role"] == "SUPPORT"
        runtime_db.set_runtime_document("json_store", "data/example.json", {"ok": True})
        assert runtime_db.get_runtime_document("json_store", "data/example.json") == {"ok": True}
    finally:
        runtime_db.SQLITE_PATH = old_path
        runtime_db._INITIALIZED = old_initialized
        runtime_db._BACKEND = old_backend


def test_json_store_database_mode_imports_legacy_once(tmp_path: Path, monkeypatch) -> None:
    from bot import json_store

    class FakeRuntimeDB:
        def __init__(self) -> None:
            self.docs: dict[tuple[str, str], object] = {}

        def get_runtime_document(self, namespace: str, key: str, default=None):
            return self.docs.get((namespace, key), default)

        def set_runtime_document(self, namespace: str, key: str, value):
            self.docs[(namespace, key)] = value
            return True

    fake = FakeRuntimeDB()
    monkeypatch.setenv("DATABASE_URL", "postgresql://configured-for-test")
    monkeypatch.setattr(json_store, "_runtime_db_module", lambda: fake)

    path = tmp_path / "data" / "example.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"legacy": 1}), encoding="utf-8")
    assert json_store.load_json_file(path, {}) == {"legacy": 1}

    # Nach dem ersten Import darf eine lokale Datei die DB nicht wieder überstimmen.
    path.write_text(json.dumps({"legacy": 999}), encoding="utf-8")
    assert json_store.load_json_file(path, {}) == {"legacy": 1}

    json_store.save_json_atomic(path, {"db": 2})
    assert json_store.load_json_file(path, {}) == {"db": 2}
    assert json.loads(path.read_text(encoding="utf-8")) == {"legacy": 999}


def test_eager_legacy_directory_migration_never_overwrites_db(tmp_path: Path, monkeypatch) -> None:
    from bot import json_store

    class FakeRuntimeDB:
        def __init__(self) -> None:
            self.docs: dict[tuple[str, str], object] = {
                ("json_store", "data/already.json"): {"db": True}
            }

        def get_runtime_document(self, namespace: str, key: str, default=None):
            return self.docs.get((namespace, key), default)

        def set_runtime_document(self, namespace: str, key: str, value):
            self.docs[(namespace, key)] = value
            return True

    fake = FakeRuntimeDB()
    monkeypatch.setenv("DATABASE_URL", "postgresql://configured-for-test")
    monkeypatch.setattr(json_store, "_runtime_db_module", lambda: fake)

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "already.json").write_text('{"legacy": true}', encoding="utf-8")
    (data_dir / "new.json").write_text('{"value": 7}', encoding="utf-8")

    # _document_key() benutzt ab dem letzten Verzeichnis "data" stabile Keys.
    result = json_store.migrate_legacy_json_directory(data_dir)
    assert result["imported"] == 1
    assert result["existing"] == 1
    assert fake.docs[("json_store", "data/already.json")] == {"db": True}
    assert fake.docs[("json_store", "data/new.json")] == {"value": 7}


def test_configured_postgres_never_falls_back_to_sqlite(monkeypatch) -> None:
    from bot import runtime_db

    old_initialized = runtime_db._INITIALIZED
    old_backend = runtime_db._BACKEND
    old_error = runtime_db._POSTGRES_ERROR
    try:
        monkeypatch.setenv("DATABASE_URL", "postgresql://runtime-test")
        monkeypatch.setattr(runtime_db, "_init_postgres", lambda: (_ for _ in ()).throw(ConnectionError("down")))
        runtime_db._INITIALIZED = False
        runtime_db._BACKEND = "unknown"
        try:
            runtime_db.init_runtime_db()
            raise AssertionError("Postgres failure must be fatal")
        except RuntimeError as exc:
            assert "SQLite-Fallback" in str(exc)
        assert runtime_db._BACKEND == "postgres_error"
    finally:
        runtime_db._INITIALIZED = old_initialized
        runtime_db._BACKEND = old_backend
        runtime_db._POSTGRES_ERROR = old_error
