from __future__ import annotations

"""Zentraler Modulkatalog für die generische MMO-Gildenplattform.

Dieses Modul enthält bewusst keine Discord-, FastAPI- oder Datenbankimporte.
Dadurch können Bot und Dashboard dieselben Modulnamen, Abhängigkeiten und
Routen-/Command-Zuordnungen verwenden.
"""

from dataclasses import dataclass
from typing import Iterable


DEFAULT_ONBOARDING_WELCOME_SLOGANS: tuple[str, ...] = (
    "Ein neuer Held betritt das Schlachtfeld.",
    "Verstärkung ist eingetroffen. Ob sie was taugt, sehen wir später.",
    "Willkommen! Die Raidleitung übernimmt keinerlei Haftung.",
    "Ein wildes {user} ist erschienen!",
    "Da ist ja unser nächstes Opfer für die Mechaniken.",
    "Möge dein Loot besser sein als dein Würfelglück.",
    "Die Gilde ist wieder um eine fragwürdige Entscheidung reicher.",
    "{user} hat den Server gefunden. Jetzt gibt es kein Zurück mehr.",
    "Noch ein Name mehr auf der Anwesenheitsliste. Willkommen, {user}!",
    "Die Gruppe wächst. Die Ausreden bei Wipes vermutlich auch.",
    "Willkommen bei {guild} – bitte Mechaniken erst ignorieren, wenn sie erklärt wurden.",
    "Mitglied #{member_count} ist angekommen. Das kann ja heiter werden.",
)


@dataclass(frozen=True)
class ModuleDefinition:
    key: str
    label: str
    description: str
    icon: str
    core: bool = False
    dependencies: tuple[str, ...] = ()


MODULES: tuple[ModuleDefinition, ...] = (
    ModuleDefinition(
        "guild",
        "Gilde & Mitglieder",
        "Gildenprofil, Mitgliederliste, Rollen, Rechte und Grundkonfiguration.",
        "👥",
        core=True,
    ),
    ModuleDefinition(
        "events",
        "Raids & Events",
        "Eventerstellung, RSVP, Kalender, Erinnerungen und Raid-Templates.",
        "📅",
        core=True,
    ),
    ModuleDefinition(
        "dashboard",
        "Dashboard & System",
        "Dashboard, Login, Berechtigungen, Audit und technische Grundfunktionen.",
        "🧩",
        core=True,
    ),
    ModuleDefinition(
        "attendance",
        "Attendance",
        "Raid-Anwesenheit, Reviews, Statistiken und optionale Voice-Vorschläge.",
        "✅",
    ),
    ModuleDefinition(
        "points",
        "DKP / Punktesystem",
        "Optionale Gildenpunkte, Eventbelohnungen, Transaktionen und Verfall.",
        "💰",
    ),
    ModuleDefinition(
        "loot",
        "Loot Management",
        "Loot-Drops, Loot-Historie, Gildenbestand und Verteilungsverwaltung.",
        "🎁",
    ),
    ModuleDefinition(
        "needlists",
        "Needlists / Wunschlisten",
        "Main-/Secondary-Bedarf, Itemwünsche, Builds und erhaltene Gegenstände.",
        "📋",
    ),
    ModuleDefinition(
        "auctions",
        "Auktionssystem",
        "Gebote, Sofortkauf, Loot-Auktionen und Übergabeverwaltung.",
        "🔨",
        dependencies=("points", "loot"),
    ),
    ModuleDefinition(
        "alliance",
        "Allianzsystem",
        "Partnergilden, mehrere Discord-Server und Event-Mirroring.",
        "🤝",
    ),
    ModuleDefinition(
        "member_portal",
        "Member Portal",
        "Persönliche Discord-Gildenzentrale und Webportal-Funktionen.",
        "🏠",
    ),
    ModuleDefinition(
        "onboarding",
        "Onboarding & Recruitment",
        "Automatisches Onboarding, Welcome Cards, Bewerber und Staff-Review.",
        "🚪",
    ),
    ModuleDefinition(
        "leader_contact",
        "Leader Contact",
        "Leitungskontakt, interne Anfragen und anonyme Meldungen.",
        "📨",
    ),
    ModuleDefinition(
        "voice",
        "Voice Management",
        "Temporäre Voice-Channels, Voice-Panel und Voice-Tracking.",
        "🎙️",
    ),
    ModuleDefinition(
        "analytics",
        "Analytics & Reports",
        "Statistiken, Planung, Fairness-Hinweise und Wochenberichte.",
        "📊",
    ),
    ModuleDefinition(
        "game_integration",
        "Game Integration",
        "Spielspezifische Charakter-, Build- und Integrationsfunktionen.",
        "🎮",
    ),
    ModuleDefinition(
        "game_database",
        "Game Database",
        "Optionale Item-/Ausrüstungsdatenbank und externe Importer.",
        "🗃️",
    ),
    ModuleDefinition(
        "game_information",
        "Game Information",
        "Optionale News, Guides, Server-/Weltstatus und externe Karten.",
        "🌐",
        dependencies=("game_integration",),
    ),
)

MODULE_BY_KEY = {m.key: m for m in MODULES}
CORE_MODULE_KEYS = frozenset(m.key for m in MODULES if m.core)
OPTIONAL_MODULE_KEYS = tuple(m.key for m in MODULES if not m.core)

# Top-Level Discord Slash-Command-Gruppe -> Feature-Modul.
# Core-Gruppen (guild, raid, event, template, audit, dashboard) fehlen bewusst.
COMMAND_MODULE_MAP: dict[str, str] = {
    "alliance": "alliance",
    "attendance": "attendance",
    "dkp": "points",
    "loot": "needlists",
    "auction": "auctions",
    "portal": "member_portal",
    "onboarding": "onboarding",
    "leader": "leader_contact",
    "voice_panel": "voice",
    "report": "analytics",
}


def module_definition(key: str) -> ModuleDefinition | None:
    return MODULE_BY_KEY.get(str(key or "").strip().lower())


def module_default_enabled(key: str) -> bool:
    definition = module_definition(key)
    return bool(definition and definition.core)


def module_dependencies(key: str) -> tuple[str, ...]:
    definition = module_definition(key)
    return definition.dependencies if definition else ()


def dependent_modules(key: str) -> tuple[str, ...]:
    needle = str(key or "").strip().lower()
    return tuple(m.key for m in MODULES if needle in m.dependencies)


def normalized_states(raw: dict[str, object] | None = None) -> dict[str, bool]:
    raw = raw or {}
    out: dict[str, bool] = {}
    for definition in MODULES:
        if definition.core:
            out[definition.key] = True
        else:
            out[definition.key] = bool(raw.get(definition.key, False))
    return out


def resolved_enable_set(key: str) -> tuple[str, ...]:
    """Modul plus rekursiv benötigte Abhängigkeiten."""
    result: list[str] = []

    def visit(current: str) -> None:
        definition = module_definition(current)
        if not definition or definition.core or current in result:
            return
        for dep in definition.dependencies:
            visit(dep)
        result.append(current)

    visit(str(key or "").strip().lower())
    return tuple(result)


def resolved_disable_set(key: str) -> tuple[str, ...]:
    """Modul plus alle optionalen Module, die davon abhängen."""
    needle = str(key or "").strip().lower()
    result: list[str] = []

    def visit(current: str) -> None:
        for dependent in dependent_modules(current):
            visit(dependent)
        if current in OPTIONAL_MODULE_KEYS and current not in result:
            result.append(current)

    visit(needle)
    return tuple(result)


def required_modules_for_dashboard_path(path: str) -> tuple[str, ...]:
    """Liefert optionale Module, die für eine Dashboard-Route aktiv sein müssen."""
    p = "/" + str(path or "").lstrip("/")

    # Mehrfach-Abhängigkeiten zuerst.
    if p.startswith("/attendance/") and ("/ec-preview" in p or p.endswith("/ec-award")):
        return ("attendance", "points")
    if p.startswith("/portal/member/") and p.endswith("/need-change"):
        return ("member_portal", "needlists")

    prefix_rules: tuple[tuple[tuple[str, ...], str], ...] = (
        (("/attendance", "/attendance-archive", "/attendance-stats", "/api/attendance", "/export/attendance", "/admin/attendance"), "attendance"),
        (("/ec", "/ec-queue", "/api/ec", "/export/ec", "/api/ec-award-requests", "/admin/ec-award-requests"), "points"),
        (("/auctions", "/auction/", "/member/auctions", "/api/auction/", "/admin/auction/", "/export/auctions"), "auctions"),
        (("/loot", "/loot-check", "/loot-history", "/api/loot", "/export/loot", "/admin/loot",), "loot"),
        (("/needs", "/character-editor", "/api/needs", "/export/needs",), "needlists"),
        (("/portal", "/my-profile", "/admin-portal", "/api/portal/", "/api/need-change-requests"), "member_portal"),
        (("/voice", "/api/voice"), "voice"),
        (("/analytics", "/planning", "/fairness", "/api/analytics", "/api/planning", "/api/fairness", "/api/leadership", "/export/fairness"), "analytics"),
        (("/items", "/api/items"), "game_database"),
        (("/tnl/builds",), "game_integration"),
        (("/tnl/news", "/tnl/guides", "/status", "/api/game-status-live"), "game_information"),
    )
    for prefixes, module_key in prefix_rules:
        if any(p == prefix or p.startswith(prefix.rstrip("/") + "/") for prefix in prefixes):
            return (module_key,)

    # Member-Loot ist kein /loot-Prefix.
    if p.startswith("/member/") and p.endswith("/loot"):
        return ("loot",)
    if p.startswith("/api/member/") and p.endswith("/loot"):
        return ("loot",)
    if p.startswith("/export/member_") and p.endswith("_loot.csv"):
        return ("loot",)
    if p == "/member/ec":
        return ("points",)
    return ()


def labels(keys: Iterable[str]) -> str:
    values = []
    for key in keys:
        definition = module_definition(key)
        values.append(definition.label if definition else str(key))
    return ", ".join(values)
