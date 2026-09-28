from __future__ import annotations

from typing import Any, Iterable
from pathlib import Path
import sys

from discord import app_commands

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from guild_modules import (
        CORE_MODULE_KEYS,
        MODULES,
        OPTIONAL_MODULE_KEYS,
        module_default_enabled,
        module_definition,
        normalized_states,
    )
except Exception:  # pragma: no cover
    from ..guild_modules import (  # type: ignore
        CORE_MODULE_KEYS,
        MODULES,
        OPTIONAL_MODULE_KEYS,
        module_default_enabled,
        module_definition,
        normalized_states,
    )

try:
    from bot import runtime_db  # type: ignore
except Exception:  # pragma: no cover
    import runtime_db  # type: ignore


class FeatureDisabled(app_commands.CheckFailure):
    def __init__(self, module_key: str):
        self.module_key = str(module_key or "")
        definition = module_definition(self.module_key)
        label = definition.label if definition else self.module_key
        super().__init__(f"Das Modul „{label}“ ist für diese Gilde deaktiviert.")


def is_module_enabled(guild_id: int | None, module_key: str) -> bool:
    definition = module_definition(module_key)
    if definition is None:
        return False
    if definition.core:
        return True
    if not guild_id:
        return False
    value = runtime_db.get_module_setting(int(guild_id), definition.key, "enabled", None)
    if value is None:
        return module_default_enabled(definition.key)
    return bool(value)


def set_module_enabled(guild_id: int, module_key: str, enabled: bool) -> bool:
    definition = module_definition(module_key)
    if definition is None or definition.core:
        return False
    return bool(runtime_db.set_module_enabled(int(guild_id), definition.key, bool(enabled)))


def module_states(guild_id: int | None) -> dict[str, bool]:
    if not guild_id:
        return normalized_states()
    rows = runtime_db.get_all_module_settings(int(guild_id))
    raw: dict[str, object] = {}
    for module_key, values in rows.items():
        if isinstance(values, dict) and "enabled" in values:
            raw[module_key] = bool(values.get("enabled"))
    return normalized_states(raw)


def any_guild_has_module(guilds: Iterable[Any], module_key: str) -> bool:
    for guild in guilds:
        gid = int(getattr(guild, "id", 0) or 0)
        if gid and is_module_enabled(gid, module_key):
            return True
    return False


class FeatureGroup(app_commands.Group):
    """Slash-Command-Gruppe, die pro Guild über module_settings geschaltet wird."""

    def __init__(self, *args: Any, module_key: str, **kwargs: Any):
        self.module_key = str(module_key or "").strip().lower()
        super().__init__(*args, **kwargs)

    async def interaction_check(self, interaction):  # discord.py Group hook
        guild_id = getattr(interaction, "guild_id", None)
        if not is_module_enabled(guild_id, self.module_key):
            raise FeatureDisabled(self.module_key)
        return True
