from __future__ import annotations

import ast
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = ROOT / "bot"
DASHBOARD = ROOT / "dashboard_web" / "main.py"


def fail(message: str) -> None:
    raise AssertionError(message)


def validate_python() -> int:
    count = 0
    for path in ROOT.rglob("*.py"):
        ast.parse(path.read_text("utf-8"), filename=str(path))
        count += 1
    return count


def validate_json() -> int:
    count = 0
    for path in ROOT.rglob("*.json"):
        json.loads(path.read_text("utf-8"))
        count += 1
    return count


def validate_commands() -> tuple[int, dict[str, int]]:
    roots: set[str] = set()
    children: dict[str, list[str]] = defaultdict(list)
    for path in BOT.glob("*.py"):
        source = path.read_text("utf-8")
        roots.update(re.findall(r"@tree\.command\s*\(\s*name\s*=\s*[\"']([^\"']+)", source))
        roots.update(re.findall(r"app_commands\.Group\s*\(\s*name\s*=\s*[\"']([^\"']+)", source))
        for group_var, command in re.findall(
            r"@([A-Za-z_]\w*)\.command\s*\(\s*name\s*=\s*[\"']([^\"']+)", source
        ):
            if group_var != "tree":
                children[group_var].append(command)
    if len(roots) > 100:
        fail(f"Zu viele Top-Level-Kommandos: {len(roots)}")
    for group_var, names in children.items():
        if len(names) > 25:
            fail(f"Command-Gruppe {group_var} hat {len(names)} Unterbefehle")
        if len(names) != len(set(names)):
            fail(f"Doppelte Unterbefehle in {group_var}")
    return len(roots), {name: len(items) for name, items in sorted(children.items())}


def route_auth_map() -> dict[tuple[str, str], str]:
    source = DASHBOARD.read_text("utf-8")
    lines = source.splitlines()
    tree = ast.parse(source)
    out: dict[tuple[str, str], str] = {}
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        routes: list[tuple[str, str]] = []
        for decorator in node.decorator_list:
            if not (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and isinstance(decorator.func.value, ast.Name)
                and decorator.func.value.id == "app"
                and decorator.func.attr in {"get", "post", "put", "patch", "delete"}
            ):
                continue
            if not decorator.args or not isinstance(decorator.args[0], ast.Constant):
                continue
            routes.append((decorator.func.attr.upper(), str(decorator.args[0].value)))
        if not routes:
            continue
        segment = "\n".join(lines[node.lineno - 1 : (node.end_lineno or node.lineno)])
        auth = "admin" if "Depends(_admin_auth)" in segment else "member" if "Depends(_auth)" in segment else "public"
        for route in routes:
            out[route] = auth
    return out


def validate_dashboard_security() -> None:
    source = DASHBOARD.read_text("utf-8")
    routes = route_auth_map()
    required_admin = {
        ("GET", "/api/admin/snapshot"),
        ("GET", "/api/settings"),
        ("GET", "/api/audit"),
        ("GET", "/api/system"),
        ("POST", "/database/init"),
        ("POST", "/database/mirror-snapshot"),
        ("GET", "/api/database-status"),
        ("GET", "/api/database-audit"),
    }
    for route in required_admin:
        if routes.get(route) != "admin":
            fail(f"Route nicht admin-only: {route} -> {routes.get(route)}")
    if ("GET", "/database/init") in routes or ("GET", "/database/mirror-snapshot") in routes:
        fail("Zustandsändernde Datenbankaktion ist noch als GET registriert")
    secret_section = source[source.index("def _session_secret") : source.index("def _validate_dashboard_security_config")]
    if '_env("DATABASE_URL")' in secret_section or '_env("DASHBOARD_PASSWORD")' in secret_section:
        fail("Session-Secret enthält weiterhin einen unsicheren Fallback")
    if "CSRF-Schutz: Herkunft nicht nachweisbar" not in source:
        fail("Same-Origin/CSRF-Schutz fehlt")
    if "def _safe_local_path" not in source:
        fail("Open-Redirect-Schutz fehlt")


def validate_runtime_guards() -> None:
    auction = (BOT / "loot_auction.py").read_text("utf-8")
    dkp = (BOT / "dkp_system.py").read_text("utf-8")
    rsvp = (BOT / "event_rsvp_dm.py").read_text("utf-8")
    onboarding = (BOT / "onboarding.py").read_text("utf-8")
    needs = (BOT / "loot_needs.py").read_text("utf-8")
    portal = (BOT / "member_portal.py").read_text("utf-8")
    if "async with _auction_lock" not in auction or '"processing"' not in auction:
        fail("Auktions-Lock/Processing-Zustand fehlt")
    if "DKP_PENDING_TX_FILE" not in dkp or "_EC_TRANSACTION_LOCK" not in dkp:
        fail("EC-Journal/Lock fehlt")
    if "request_id" not in dkp or "_phase3_upsert_ec_change" not in dkp:
        fail("EC-Idempotenz/inkrementeller Upsert fehlt")
    if "async with _rsvp_lock" not in rsvp or "_phase3_upsert_event_from_store" not in rsvp:
        fail("RSVP-Lock/inkrementeller Upsert fehlt")
    if "onboarding_sessions.json" not in onboarding or "client.add_view" not in onboarding:
        fail("Persistentes Onboarding fehlt")
    if "def _phase3_upsert_needs_scope" not in needs or re.search(r"(?<!def )\bsave_needs\(\)", needs):
        fail("Needs verwenden noch Full-Mirror statt gezielter Upserts")
    if "def _phase3_upsert_profile_targets_to_pg" not in portal or re.search(r"(?<!def )\bsave_profiles\(\)", portal):
        fail("Profile verwenden noch Full-Mirror statt gezielter Upserts")


def validate_repository() -> None:
    forbidden = [BOT / "bot(5).py", BOT / "bot(5) (1).py", ROOT / "Wei-e-Flamme-railway-ready.zip"]
    for path in forbidden:
        if path.exists():
            fail(f"Altdatei noch vorhanden: {path.relative_to(ROOT)}")
    if not os.getenv("VALIDATION_ALLOW_BYTECODE"):
        if any(ROOT.rglob("*.pyc")) or any(p.is_dir() for p in ROOT.rglob("__pycache__")):
            fail("Bytecode-Dateien im Release")
    requirements = (BOT / "requirements.txt").read_text("utf-8")
    if "from __future__" in requirements or "def " in requirements:
        fail("bot/requirements.txt enthält weiterhin Python-Code")
    json.loads((ROOT / "dashboard_web" / "railpack.json").read_text("utf-8"))
    if "playwright install" in (ROOT / "dashboard_web" / "start.sh").read_text("utf-8"):
        fail("Chromium wird weiterhin beim Start installiert")


def main() -> int:
    python_files = validate_python()
    json_files = validate_json()
    root_count, groups = validate_commands()
    validate_dashboard_security()
    validate_runtime_guards()
    validate_repository()
    print(f"OK: {python_files} Python-Dateien, {json_files} JSON-Dateien")
    print(f"OK: {root_count} geschätzte Top-Level-Kommandos")
    print("OK: Gruppen", json.dumps(groups, ensure_ascii=False, sort_keys=True))
    print("OK: Dashboard-, EC-, Auktions-, RSVP-, Deployment- und Repository-Prüfungen")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"FEHLER: {exc}", file=sys.stderr)
        raise SystemExit(1)
