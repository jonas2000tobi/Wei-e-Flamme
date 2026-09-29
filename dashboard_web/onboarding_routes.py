from __future__ import annotations

import re
import urllib.parse
from typing import Any, Callable, Iterable

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse


def _form_bool(value: Any) -> bool:
    return str(value or "0").strip().lower() in {"1", "true", "yes", "on"}


def _parse_form(raw: bytes) -> dict[str, str]:
    parsed = urllib.parse.parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
    return {str(k): str((v or [""])[0]) for k, v in parsed.items()}


def build_onboarding_router(
    *,
    admin_auth: Callable[..., Any],
    get_guild_id: Callable[[], int],
    module_enabled: Callable[[str, int], bool],
    set_module_setting: Callable[[int, str, str, Any], Any],
    set_guild_setting: Callable[[int, str, Any], Any],
    default_welcome_slogans: Iterable[str],
) -> APIRouter:
    """Admin-Schreibwege für das generische Onboarding."""
    router = APIRouter(tags=["onboarding"])

    def guild_id_or_400() -> int:
        guild_id = int(get_guild_id() or 0)
        if not guild_id:
            raise HTTPException(status_code=400, detail="Guild-ID fehlt")
        return guild_id

    def onboarding_enabled_or_redirect(guild_id: int):
        if module_enabled("onboarding", guild_id):
            return None
        return RedirectResponse(
            "/admin-settings?" + urllib.parse.urlencode(
                {"section": "modules", "msg": "Onboarding & Recruitment ist deaktiviert."}
            ),
            status_code=303,
        )

    @router.post("/admin/onboarding-aion2-settings")
    async def admin_onboarding_aion2_settings(
        request: Request,
        _: bool = Depends(admin_auth),
    ):
        guild_id = guild_id_or_400()
        redirect = onboarding_enabled_or_redirect(guild_id)
        if redirect:
            return redirect
        form = _parse_form(await request.body())
        set_module_setting(guild_id, "onboarding", "aion2_enabled", _form_bool(form.get("aion2_enabled")))
        return RedirectResponse("/admin-settings#modules", status_code=303)

    @router.post("/admin/onboarding-welcome-settings")
    async def admin_onboarding_welcome_settings(
        request: Request,
        _: bool = Depends(admin_auth),
    ):
        guild_id = guild_id_or_400()
        redirect = onboarding_enabled_or_redirect(guild_id)
        if redirect:
            return redirect
        form = _parse_form(await request.body())
        welcome_enabled = _form_bool(form.get("welcome_enabled"))
        update_on_leave = _form_bool(form.get("welcome_update_on_leave"))
        channel_raw = str(form.get("welcome_channel_id") or "").strip()
        channel_id = int(channel_raw) if channel_raw.isdigit() else 0

        slogans: list[str] = []
        for line in str(form.get("welcome_slogans") or "").splitlines():
            clean = re.sub(r"\s+", " ", str(line or "").strip())
            if not clean:
                continue
            slogans.append(clean[:300])
            if len(slogans) >= 50:
                break
        if not slogans:
            slogans = [str(x) for x in default_welcome_slogans]

        set_guild_setting(guild_id, "guild_channel_welcome_id", channel_id)
        set_module_setting(guild_id, "onboarding", "welcome_enabled", welcome_enabled)
        set_module_setting(guild_id, "onboarding", "welcome_update_on_leave", update_on_leave)
        set_module_setting(guild_id, "onboarding", "welcome_slogans", slogans)

        msg = "Welcome-System gespeichert."
        if welcome_enabled and not channel_id:
            msg += " Es ist noch kein Welcome-Kanal ausgewählt; bis dahin wird keine öffentliche Welcome Card gesendet."
        return RedirectResponse(
            "/admin-settings?" + urllib.parse.urlencode({"section": "modules", "msg": msg}),
            status_code=303,
        )

    @router.post("/admin/onboarding-application-settings")
    async def admin_onboarding_application_settings(
        request: Request,
        _: bool = Depends(admin_auth),
    ):
        guild_id = guild_id_or_400()
        redirect = onboarding_enabled_or_redirect(guild_id)
        if redirect:
            return redirect
        form = _parse_form(await request.body())
        enabled = _form_bool(form.get("application_chat_enabled"))
        category_raw = str(form.get("application_category_id") or "").strip()
        lead_raw = str(form.get("application_lead_role_id") or "").strip()
        category_id = int(category_raw) if category_raw.isdigit() else 0
        lead_role_id = int(lead_raw) if lead_raw.isdigit() else 0

        set_module_setting(guild_id, "onboarding", "application_chat_enabled", enabled)
        set_module_setting(guild_id, "onboarding", "application_category_id", category_id)
        set_module_setting(guild_id, "onboarding", "application_lead_role_id", lead_role_id)

        msg = "Bewerbungs-Chat gespeichert."
        missing: list[str] = []
        if enabled and not category_id:
            missing.append("Bewerbungs-Kategorie")
        if enabled and not lead_role_id:
            missing.append("Lead-Rolle")
        if missing:
            msg += " Fehlt noch: " + ", ".join(missing) + "."
        return RedirectResponse(
            "/admin-settings?" + urllib.parse.urlencode({"section": "modules", "msg": msg}),
            status_code=303,
        )

    return router
