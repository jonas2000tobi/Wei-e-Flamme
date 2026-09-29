from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Any

_LOCKS: dict[str, RLock] = {}
_MIGRATED_KEYS: set[str] = set()


def _lock_for(path: Path) -> RLock:
    key = str(path.resolve())
    lock = _LOCKS.get(key)
    if lock is None:
        lock = RLock()
        _LOCKS[key] = lock
    return lock


def warn_json_store(context: str, message: str, exc: BaseException | None = None) -> None:
    prefix = f"[json_store:{context or 'unknown'}]"
    if exc is None:
        print(f"{prefix} {message}", flush=True)
    else:
        print(f"{prefix} {message}: {type(exc).__name__}: {exc}", flush=True)


def _runtime_db_module():
    try:
        from bot import runtime_db  # type: ignore
        return runtime_db
    except Exception:
        try:
            import runtime_db  # type: ignore
            return runtime_db
        except Exception:
            return None


def _use_database_store() -> bool:
    """Railway/Production nutzt bei gesetzter DATABASE_URL ausschließlich DB-State.

    Ohne DATABASE_URL bleibt der Dateispeicher als lokaler Entwicklungsmodus erhalten.
    """
    return bool(str(os.getenv("DATABASE_URL") or "").strip())


def _document_key(path: Path) -> str:
    path = Path(path)
    # Stabile Keys unabhängig vom absoluten Railway-Pfad.
    parts = list(path.parts)
    if "data" in parts:
        idx = len(parts) - 1 - parts[::-1].index("data")
        rel = Path(*parts[idx:]).as_posix()
    else:
        rel = path.name
    return rel[:500]


def _read_legacy_file(path: Path, default: Any, *, context: str, check_type: bool) -> Any:
    try:
        if not path.exists():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        if check_type and default is not None and not isinstance(data, type(default)):
            warn_json_store(context or path.name, f"Typ passt nicht bei {path.name}; nutze Default")
            return default
        return data
    except Exception as exc:
        warn_json_store(context or path.name, f"JSON konnte nicht gelesen werden ({path})", exc)
        return default


def load_json_file(path: Path, default: Any, *, context: str = "", check_type: bool = True) -> Any:
    """Kompatible Runtime-Dokument-API.

    In Produktion (DATABASE_URL gesetzt) ist PostgreSQL die einzige Wahrheit. Beim
    ersten Zugriff wird eine vorhandene Legacy-JSON einmalig importiert, falls in
    der DB noch kein Dokument existiert. Danach werden lokale Dateien ignoriert.
    Lokal ohne DATABASE_URL bleibt das bisherige atomare JSON-Verhalten bestehen.
    """
    path = Path(path)
    if not _use_database_store():
        return _read_legacy_file(path, default, context=context, check_type=check_type)

    runtime_db = _runtime_db_module()
    if runtime_db is None:
        raise RuntimeError("DATABASE_URL ist gesetzt, aber runtime_db konnte nicht geladen werden.")

    key = _document_key(path)
    marker = f"json_store:{key}"
    sentinel = object()
    try:
        data = runtime_db.get_runtime_document("json_store", key, sentinel)
        if data is sentinel:
            # Einmalige Migration der alten Railway-/Repo-Datei in PostgreSQL.
            legacy = _read_legacy_file(path, default, context=context, check_type=check_type)
            runtime_db.set_runtime_document("json_store", key, legacy)
            if marker not in _MIGRATED_KEYS:
                print(f"[json_store] Legacy-Dokument nach PostgreSQL übernommen: {key}", flush=True)
                _MIGRATED_KEYS.add(marker)
            data = legacy
        if check_type and default is not None and not isinstance(data, type(default)):
            warn_json_store(context or path.name, f"DB-Typ passt nicht bei {key}; nutze Default")
            return default
        return data
    except Exception as exc:
        # Kein Dateifallback bei gesetzter DATABASE_URL: Split-Brain verhindern.
        warn_json_store(context or path.name, f"Runtime-Dokument konnte nicht aus PostgreSQL gelesen werden ({key})", exc)
        raise



def migrate_legacy_json_directory(directory: Path) -> dict[str, Any]:
    """Übernimmt vorhandene Runtime-JSONs eines alten Deployments einmalig in PostgreSQL.

    Die Migration läuft nur, wenn ``DATABASE_URL`` gesetzt ist. Bereits vorhandene
    DB-Dokumente werden niemals überschrieben. Damit kann der Bot nach dem Cutover
    ohne spätere Datei-Fallbacks starten und auch selten gelesene Altzustände gehen
    nicht erst beim ersten Feature-Aufruf verloren.
    """
    directory = Path(directory)
    result: dict[str, Any] = {"enabled": _use_database_store(), "imported": 0, "existing": 0, "skipped": 0, "errors": []}
    if not result["enabled"]:
        return result

    runtime_db = _runtime_db_module()
    if runtime_db is None:
        raise RuntimeError("DATABASE_URL ist gesetzt, aber runtime_db konnte nicht geladen werden.")
    if not directory.exists():
        return result

    sentinel = object()
    for path in sorted(directory.glob("*.json")):
        key = _document_key(path)
        try:
            current = runtime_db.get_runtime_document("json_store", key, sentinel)
            if current is not sentinel:
                result["existing"] += 1
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                result["skipped"] += 1
                result["errors"].append(f"{path.name}: {type(exc).__name__}: {exc}")
                continue
            runtime_db.set_runtime_document("json_store", key, payload)
            result["imported"] += 1
            _MIGRATED_KEYS.add(f"json_store:{key}")
        except Exception as exc:
            result["errors"].append(f"{path.name}: {type(exc).__name__}: {exc}")
            raise
    return result

def save_json_atomic(path: Path, obj: Any, *, context: str = "") -> None:
    """Speichert Runtime-State atomar.

    Mit DATABASE_URL wird ausschließlich PostgreSQL beschrieben. Ohne DATABASE_URL
    bleibt der lokale Dateimodus für Entwicklung/Tests erhalten.
    """
    path = Path(path)
    if _use_database_store():
        runtime_db = _runtime_db_module()
        if runtime_db is None:
            raise RuntimeError("DATABASE_URL ist gesetzt, aber runtime_db konnte nicht geladen werden.")
        key = _document_key(path)
        try:
            runtime_db.set_runtime_document("json_store", key, obj)
            return
        except Exception as exc:
            warn_json_store(context or path.name, f"Runtime-Dokument konnte nicht nach PostgreSQL geschrieben werden ({key})", exc)
            raise

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, indent=2, ensure_ascii=False)
    lock = _lock_for(path)
    tmp_name = ""
    with lock:
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp", delete=False) as tmp:
                tmp_name = tmp.name
                tmp.write(payload)
                tmp.write("\n")
                tmp.flush()
                try:
                    os.fsync(tmp.fileno())
                except OSError:
                    pass
            os.replace(tmp_name, path)
        except Exception as exc:
            if tmp_name:
                try:
                    Path(tmp_name).unlink(missing_ok=True)
                except Exception:
                    pass
            warn_json_store(context or path.name, f"JSON konnte nicht gespeichert werden ({path})", exc)
            raise
