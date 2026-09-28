from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from threading import RLock
from typing import Any

_LOCKS: dict[str, RLock] = {}


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


def load_json_file(path: Path, default: Any, *, context: str = "", check_type: bool = True) -> Any:
    """Liest JSON robust und meldet kaputte Dateien sichtbar im Railway-Log.

    default wird zurückgegeben, wenn die Datei fehlt, JSON defekt ist oder der Typ
    nicht zum erwarteten default-Typ passt.
    """
    path = Path(path)
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


def save_json_atomic(path: Path, obj: Any, *, context: str = "") -> None:
    """Schreibt JSON atomar: erst Temp-Datei, dann os.replace.

    Dadurch bleibt die alte Datei erhalten, falls Railway/Bot genau beim Schreiben
    stoppt oder ein Fehler passiert.
    """
    path = Path(path)
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
