from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
import urllib.parse

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse

from aion2_feature import normalize_class, normalize_faction, role_for_class


def build_aion2_router(
    *,
    admin_auth: Callable[..., Any],
    get_guild_id: Callable[[], int],
    ensure_tables: Callable[[], None],
    pg_connect: Callable[[], Any],
) -> APIRouter:
    """Aion-2-Schreibwege des Dashboards.

    Die Router-Fabrik hält ``main.py`` frei von Feature-spezifischer SQL-/Form-
    Logik, ohne globale Zustände oder einen Import-Zyklus zum Dashboard zu bauen.
    """
    router = APIRouter(tags=["aion2"])

    @router.post("/admin/member/{user_id}/aion2")
    async def admin_member_aion2_save(
        user_id: int,
        request: Request,
        _: bool = Depends(admin_auth),
    ):
        raw = (await request.body()).decode("utf-8", errors="replace")
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        guild_id = int(get_guild_id() or 0)
        if not guild_id:
            raise HTTPException(status_code=400, detail="Guild-ID fehlt")

        character_name = str((form.get("character_name") or [""])[0]).strip()[:120]
        faction = normalize_faction((form.get("faction") or [""])[0])
        class_name = normalize_class((form.get("class_name") or [""])[0])
        main_role = role_for_class(class_name)
        gearscore = str((form.get("gearscore") or [""])[0]).strip()[:40]
        raw_level = str((form.get("level") or [""])[0]).strip()
        level = int(raw_level) if raw_level.isdigit() else None

        ensure_tables()
        conn = pg_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO aion2_profiles(
                           guild_id,user_id,character_name,class_name,main_role,faction,level,gearscore,updated_at
                       ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT(guild_id,user_id) DO UPDATE SET
                         character_name=EXCLUDED.character_name,
                         class_name=EXCLUDED.class_name,
                         main_role=EXCLUDED.main_role,
                         faction=EXCLUDED.faction,
                         level=EXCLUDED.level,
                         gearscore=EXCLUDED.gearscore,
                         updated_at=EXCLUDED.updated_at""",
                    (
                        guild_id,
                        int(user_id),
                        character_name,
                        class_name,
                        main_role,
                        faction,
                        level,
                        gearscore,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return RedirectResponse(f"/member/{int(user_id)}#aion2", status_code=303)

    return router
