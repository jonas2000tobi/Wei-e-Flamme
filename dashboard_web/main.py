from __future__ import annotations

import html
import json
import os
import secrets
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="Ebo Dashboard", version="0.5.0")
security = HTTPBasic(auto_error=False)


def _database_url() -> str:
    return str(os.getenv("DATABASE_URL") or "").strip()


def _normalized_database_url() -> str:
    url = _database_url()
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _pg_connect():
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    return psycopg.connect(_normalized_database_url(), row_factory=dict_row, connect_timeout=10)


def _auth(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> bool:
    password = str(os.getenv("DASHBOARD_PASSWORD") or "").strip()
    if not password:
        # Für den allerersten Test erlaubt. Auf Railway danach unbedingt setzen.
        return True
    username = str(os.getenv("DASHBOARD_USERNAME") or "admin").strip() or "admin"
    if not credentials:
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate": "Basic"})
    ok_user = secrets.compare_digest(credentials.username, username)
    ok_pw = secrets.compare_digest(credentials.password, password)
    if not (ok_user and ok_pw):
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate": "Basic"})
    return True


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _dt(value: Any) -> str:
    s = str(value or "")
    if not s:
        return "—"
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    except Exception:
        return s[:19]


def _short(value: Any, n: int = 80) -> str:
    s = str(value or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(".", "").replace(",", ".") if isinstance(value, str) and "," in value else value)
    except Exception:
        return default


def _latest_snapshot_row() -> Optional[dict[str, Any]]:
    if not _database_url():
        return None
    guild_id = str(os.getenv("DASHBOARD_GUILD_ID") or "").strip()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            if guild_id:
                cur.execute(
                    """
                    SELECT * FROM dashboard_snapshots
                    WHERE guild_id = %s
                    ORDER BY published_at DESC, id DESC
                    LIMIT 1
                    """,
                    (int(guild_id),),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM dashboard_snapshots
                    ORDER BY published_at DESC, id DESC
                    LIMIT 1
                    """
                )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _snapshot_payload() -> dict[str, Any]:
    row = _latest_snapshot_row()
    if not row:
        return {"ok": False, "error": "Noch kein Dashboard-Snapshot in Postgres gefunden."}
    try:
        snap = json.loads(row.get("snapshot_json") or "{}")
    except Exception as exc:
        return {"ok": False, "error": f"Snapshot JSON kaputt: {type(exc).__name__}: {exc}"}
    return {
        "ok": True,
        "id": row.get("id"),
        "guild_id": row.get("guild_id"),
        "guild_name": row.get("guild_name"),
        "generated_at": row.get("generated_at"),
        "published_at": row.get("published_at"),
        "snapshot": snap,
    }


def _card(title: str, value: Any, sub: str = "") -> str:
    return f"""
    <div class="card">
      <div class="card-title">{_e(title)}</div>
      <div class="card-value">{_e(value)}</div>
      <div class="card-sub">{_e(sub)}</div>
    </div>
    """


def _raw(html_value: str) -> dict[str, str]:
    return {"__html__": str(html_value or "")}


def _cell(value: Any) -> str:
    if isinstance(value, dict) and "__html__" in value:
        return str(value.get("__html__") or "")
    return _e(value)


def _member_link(user_id: Any, label: Any) -> dict[str, str]:
    uid = _user_id(user_id)
    text = _e(label or f"User {uid}")
    if not uid:
        return _raw(text)
    return _raw(f'<a class="link" href="/member/{uid}">{text}</a>')


def _event_link(event_id: Any, label: Any) -> dict[str, str]:
    eid = str(event_id or "").strip()
    text = _e(label or eid or "Event")
    if not eid:
        return _raw(text)
    return _raw(f'<a class="link" href="/event/{_e(eid)}">{text}</a>')


def _table(headers: list[str], rows: list[list[Any]], *, searchable: bool = True, placeholder: str = "Tabelle durchsuchen…") -> str:
    if not rows:
        return '<div class="empty">Keine Daten vorhanden.</div>'
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{_cell(c)}</td>" for c in row) + "</tr>" for row in rows)
    search = f'<input class="table-search" type="search" placeholder="{_e(placeholder)}" oninput="filterNextTable(this)">' if searchable else ""
    return f"{search}<div class='table-wrap'><table class='searchable-table'><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def _profile_name_map(snap: dict[str, Any]) -> dict[int, str]:
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    names: dict[int, str] = {}
    for p in profiles:
        if not isinstance(p, dict):
            continue
        try:
            uid = int(p.get("user_id") or 0)
        except Exception:
            uid = 0
        if uid:
            names[uid] = str(p.get("display_name") or p.get("ingame_name") or f"User {uid}")
    return names


def _fmt_ec(value: Any) -> str:
    n = _num(value)
    if abs(n - round(n)) < 0.0001:
        return str(int(round(n)))
    return f"{n:.1f}"


def _user_id(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _balance_map(snap: dict[str, Any]) -> dict[int, float]:
    balances = (((snap.get("ec") or {}).get("balances") or {}).get("top") or [])
    out: dict[int, float] = {}
    for b in balances:
        if not isinstance(b, dict):
            continue
        uid = _user_id(b.get("user_id") or b.get("member_id") or b.get("discord_id"))
        if uid:
            out[uid] = _num(b.get("balance"), 0)
    return out


def _need_user_ids(snap: dict[str, Any]) -> set[int]:
    needs = ((snap.get("loot") or {}).get("needs") or {})
    ids: set[int] = set()
    for n in needs.get("items") or []:
        if not isinstance(n, dict):
            continue
        uid = _user_id(n.get("user_id") or n.get("member_id") or n.get("discord_id"))
        if uid:
            ids.add(uid)
    return ids


def _needs_by_user(snap: dict[str, Any]) -> dict[int, dict[str, Any]]:
    needs = ((snap.get("loot") or {}).get("needs") or {})
    rows = needs.get("items") or needs.get("sample") or []
    out: dict[int, dict[str, Any]] = {}
    for n in rows:
        if not isinstance(n, dict):
            continue
        uid = _user_id(n.get("user_id") or n.get("member_id") or n.get("discord_id"))
        if uid:
            out[uid] = n
    return out


def _tx_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    txs = (((snap.get("ec") or {}).get("transactions") or {}).get("recent") or [])
    out = []
    for tx in txs:
        if not isinstance(tx, dict):
            continue
        uid = _user_id(tx.get("user_id") or tx.get("target_user_id") or tx.get("member_id"))
        if uid == int(user_id):
            out.append(tx)
    return out[:limit]


def _voice_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    sessions = ((snap.get("voice") or {}).get("recent_sessions") or [])
    out = []
    for v in sessions:
        if not isinstance(v, dict):
            continue
        uid = _user_id(v.get("user_id") or v.get("member_id"))
        if uid == int(user_id):
            out.append(v)
    return out[:limit]


def _auctions_for_user(snap: dict[str, Any], user_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    out = []
    for a in auctions:
        if not isinstance(a, dict):
            continue
        if _user_id(a.get("top_bid_user_id")) == int(user_id) or _user_id(a.get("winner_user_id")) == int(user_id):
            out.append(a)
    return out[:limit]


def _need_list_html(title: str, items: Any) -> str:
    arr = items if isinstance(items, list) else []
    if not arr:
        return f"<h3>{_e(title)}</h3><div class='empty'>Keine Einträge.</div>"
    lis = "".join(f"<li>{_e(x)}</li>" for x in arr[:80])
    more = f"<p class='muted'>+ {len(arr) - 80} weitere</p>" if len(arr) > 80 else ""
    return f"<h3>{_e(title)} <span class='pill'>{len(arr)}</span></h3><ul class='need-list'>{lis}</ul>{more}"


def _safe_percent(part: float, total: float) -> str:
    if not total:
        return "0 %"
    return f"{round((part / total) * 100)} %"


def _analytics_from_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    events = ((snap.get("events") or {}).get("items") or [])
    balances = (((snap.get("ec") or {}).get("balances") or {}).get("top") or [])
    auctions = (((snap.get("loot") or {}).get("auctions") or {}).get("items") or [])
    auction_status = (((snap.get("loot") or {}).get("auctions") or {}).get("by_status") or {})
    needs = (((snap.get("loot") or {}).get("needs") or {}))
    voice = snap.get("voice") or {}
    audit = snap.get("audit") or {}

    roles = Counter(str(p.get("main_role") or "Unbekannt") for p in profiles if isinstance(p, dict))
    gs_values = [_num(p.get("gearscore"), 0) for p in profiles if isinstance(p, dict) and _num(p.get("gearscore"), 0) > 0]
    ec_values = [_num(b.get("balance"), 0) for b in balances if isinstance(b, dict)]
    total_ec = sum(ec_values)
    avg_ec = total_ec / len(ec_values) if ec_values else 0
    avg_gs = sum(gs_values) / len(gs_values) if gs_values else 0

    total_event_participants = sum(int(_num(ev.get("participant_count"), 0)) for ev in events if isinstance(ev, dict))
    total_maybe = sum(int(_num(ev.get("maybe_count"), 0)) for ev in events if isinstance(ev, dict))
    total_no = sum(int(_num(ev.get("no_count"), 0)) for ev in events if isinstance(ev, dict))
    voice_enabled = sum(1 for ev in events if isinstance(ev, dict) and ev.get("voice_enabled"))

    active_statuses = {"open", "active", "running", "bidding", "roll", "sale", "free", "main", "secondary"}
    active_auctions = [a for a in auctions if isinstance(a, dict) and str(a.get("status") or "").lower() in active_statuses]
    total_listed_bids = sum(int(_num(a.get("bid_count"), 0)) for a in auctions if isinstance(a, dict))
    recent_voice_seconds = sum(int(_num(v.get("duration_seconds"), 0)) for v in (voice.get("recent_sessions") or []) if isinstance(v, dict))

    role_member_count = int(_num(((snap.get("guild") or {}).get("member_filter") or {}).get("eligible_count"), 0))
    missing_profiles = max(0, role_member_count - len(profiles))
    missing_ec = max(0, role_member_count - len(ec_values))
    missing_needs = max(0, role_member_count - int(_num(needs.get("user_count"), 0)))

    return {
        "role_distribution": roles.most_common(),
        "role_member_count": role_member_count,
        "missing_profiles": missing_profiles,
        "missing_ec": missing_ec,
        "missing_needs": missing_needs,
        "profile_coverage": _safe_percent(len(profiles), role_member_count),
        "ec_coverage": _safe_percent(len(ec_values), role_member_count),
        "need_coverage": _safe_percent(int(_num(needs.get("user_count"), 0)), role_member_count),
        "avg_gearscore": avg_gs,
        "gearscore_count": len(gs_values),
        "total_ec": total_ec,
        "avg_ec": avg_ec,
        "ec_count": len(ec_values),
        "total_event_participants": total_event_participants,
        "total_maybe": total_maybe,
        "total_no": total_no,
        "voice_enabled_events": voice_enabled,
        "active_auctions": len(active_auctions),
        "auction_status": auction_status,
        "total_listed_bids": total_listed_bids,
        "need_entries_estimated": needs.get("need_entries_estimated", 0),
        "recent_voice_hours": recent_voice_seconds / 3600,
        "audit_total": audit.get("logs_total", 0),
    }


def _bars(items: list[tuple[Any, Any]], *, max_items: int = 8) -> str:
    if not items:
        return '<div class="empty">Keine Daten vorhanden.</div>'
    parsed = [(str(k or "—"), float(_num(v))) for k, v in items[:max_items]]
    max_v = max([v for _, v in parsed] or [1]) or 1
    rows = []
    for label, value in parsed:
        width = max(4, int((value / max_v) * 100)) if max_v else 4
        rows.append(f"""
        <div class="bar-row">
          <div class="bar-label">{_e(label)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div>
          <div class="bar-value">{_e(int(value) if value.is_integer() else round(value, 1))}</div>
        </div>
        """)
    return "".join(rows)


def _render_dashboard(data: dict[str, Any]) -> str:
    if not data.get("ok"):
        return _html_shell(
            "Ebo Dashboard",
            f"""
            <section class="panel">
              <h1>📊 Ebo Dashboard</h1>
              <p class="muted">{_e(data.get('error'))}</p>
              <p>Starte den Bot mit der aktuellen Version und warte bis zu 5 Minuten. Oder nutze im Discord <code>/dashboard_status</code>, damit direkt ein Snapshot veröffentlicht wird.</p>
            </section>
            """,
        )

    snap: dict[str, Any] = data.get("snapshot") or {}
    guild = snap.get("guild") or {}
    profiles = snap.get("profiles") or {}
    events = snap.get("events") or {}
    ec = snap.get("ec") or {}
    loot = snap.get("loot") or {}
    voice = snap.get("voice") or {}
    audit = snap.get("audit") or {}
    event_checks = snap.get("event_checks") or {}
    member_filter = guild.get("member_filter") or {}
    analytics = _analytics_from_snapshot(snap)
    names = _profile_name_map(snap)
    balances_by_user = _balance_map(snap)
    need_user_ids = _need_user_ids(snap)
    needs_by_user = _needs_by_user(snap)

    role_line = "Gildenrolle nicht gesetzt"
    if isinstance(member_filter, dict) and member_filter.get("mode") == "discord_role":
        role_line = f"Rolle: {member_filter.get('role_name')} · {member_filter.get('eligible_count', 0)} Mitglieder"

    cards = "".join([
        _card("Rollenmitglieder", (member_filter or {}).get("eligible_count", 0), role_line),
        _card("Profile", profiles.get("count", 0), f"ausgefiltert: {profiles.get('stale_count', 0)}"),
        _card("Events", events.get("count", 0), f"Voice-Events: {analytics['voice_enabled_events']}"),
        _card("EC-Konten", (ec.get("balances") or {}).get("count", 0), f"Ø {_fmt_ec(analytics['avg_ec'])} EC"),
        _card("Auktionen", (loot.get("auctions") or {}).get("count", 0), f"aktiv: {analytics['active_auctions']}"),
        _card("Need-User", (loot.get("needs") or {}).get("user_count", 0), f"Needs ca.: {analytics['need_entries_estimated']}"),
        _card("Voice", voice.get("sessions_total", 0), f"offen: {voice.get('sessions_open', 0)}"),
        _card("Audit", audit.get("logs_total", 0), "Einträge"),
    ])

    profile_rows = []
    low_profile_rows = []
    for p in profiles.get("items") or []:
        uid = _user_id(p.get("user_id"))
        ec_value = balances_by_user.get(uid)
        need_state = "ja" if uid in need_user_ids else "nein"
        gs = _num(p.get("gearscore"), 0)
        profile_rows.append([_member_link(uid, p.get("display_name")), p.get("ingame_name"), p.get("main_role"), p.get("gearscore"), _fmt_ec(ec_value) if ec_value is not None else "—", need_state])
        if gs <= 0 or not p.get("main_role") or ec_value is None:
            low_profile_rows.append([_member_link(uid, p.get("display_name")), p.get("ingame_name"), p.get("main_role") or "—", p.get("gearscore") or "—", _fmt_ec(ec_value) if ec_value is not None else "kein EC-Konto"])

    event_rows = []
    for ev in events.get("items") or []:
        event_rows.append([_event_link(ev.get("event_id"), ev.get("title")), _dt(ev.get("when_iso")), ev.get("participant_count"), ev.get("maybe_count"), ev.get("no_count"), "ja" if ev.get("voice_enabled") else "nein"])

    all_balances = [b for b in ((ec.get("balances") or {}).get("top") or []) if isinstance(b, dict)]
    sorted_balances = sorted(all_balances, key=lambda b: _num(b.get("balance"), 0), reverse=True)
    balance_rows = []
    for b in sorted_balances:
        balance_rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("balance"))])
    bottom_balance_rows = []
    for b in list(reversed(sorted_balances))[:12]:
        bottom_balance_rows.append([_member_link(b.get("user_id"), b.get("display_name")), _fmt_ec(b.get("balance"))])

    auction_rows = []
    for a in (loot.get("auctions") or {}).get("items") or []:
        leader = "—"
        uid = int(_num(a.get("top_bid_user_id"), 0))
        if a.get("top_bid_amount") is not None:
            leader = f"{names.get(uid, f'User {uid}')} / {_fmt_ec(a.get('top_bid_amount'))} EC"
        auction_rows.append([a.get("item_name"), a.get("status"), a.get("phase"), a.get("bid_count"), leader, _dt(a.get("ends_at"))])

    voice_rows = []
    for v in voice.get("recent_sessions") or []:
        seconds = int(_num(v.get("duration_seconds"), 0))
        minutes = round(seconds / 60, 1) if seconds else "—"
        voice_rows.append([v.get("member_name") or v.get("user_id"), v.get("channel_name") or v.get("channel_id"), _dt(v.get("joined_at")), _dt(v.get("left_at")), minutes])

    audit_rows = []
    for a in audit.get("recent_logs") or []:
        audit_rows.append([_dt(a.get("created_at")), a.get("action"), _short(a.get("summary"), 120), a.get("actor_id")])

    status_items = []
    for k, v in (analytics.get("auction_status") or {}).items():
        status_items.append((str(k), int(_num(v))))
    status_items.sort(key=lambda x: x[1], reverse=True)

    quality_cards = "".join([
        _card("Profil-Abdeckung", analytics.get("profile_coverage"), f"fehlt: {analytics.get('missing_profiles', 0)}"),
        _card("EC-Abdeckung", analytics.get("ec_coverage"), f"fehlt: {analytics.get('missing_ec', 0)}"),
        _card("Need-Abdeckung", analytics.get("need_coverage"), f"ohne Need: {analytics.get('missing_needs', 0)}"),
        _card("Snapshot", _dt(data.get("published_at")), "letzte Veröffentlichung"),
    ])

    body = f"""
    <nav class="topnav">
      <a href="#overview">Übersicht</a>
      <a href="#analytics">Analytics</a>
      <a href="#quality">Datenqualität</a>
      <a href="#members">Mitglieder</a>
      <a href="#events">Events</a>
      <a href="#loot">Loot</a>
      <a href="#logs">Logs</a>
      <a href="/api/snapshot">JSON</a>
    </nav>

    <section class="hero" id="overview">
      <div>
        <div class="eyebrow">Read-only Dashboard</div>
        <h1>🏰 {_e(guild.get('name') or data.get('guild_name') or 'Ebolus')}</h1>
        <p>Snapshot veröffentlicht: <strong>{_e(_dt(data.get('published_at')))}</strong> · generiert: {_e(_dt(data.get('generated_at')))}</p>
        <p class="muted">{_e(role_line)} · alte JSON-Einträge werden nur ausgeblendet, nicht gelöscht.</p>
      </div>
      <a class="btn" href="/api/snapshot">JSON ansehen</a>
    </section>

    <section class="grid">{cards}</section>

    <section class="panel" id="analytics">
      <h2>📈 Analytics Schnellblick</h2>
      <div class="analytics-grid">
        <div class="metric"><span>Ø Gearscore</span><strong>{_e(round(analytics['avg_gearscore']))}</strong><small>{_e(analytics['gearscore_count'])} Profile mit GS</small></div>
        <div class="metric"><span>EC gesamt</span><strong>{_e(_fmt_ec(analytics['total_ec']))}</strong><small>über {analytics['ec_count']} Konten</small></div>
        <div class="metric"><span>Event-Zusagen</span><strong>{_e(analytics['total_event_participants'])}</strong><small>im Snapshot</small></div>
        <div class="metric"><span>Gebote gelistet</span><strong>{_e(analytics['total_listed_bids'])}</strong><small>in geladenen Auktionen</small></div>
      </div>
      <div class="split">
        <div><h3>Rollenverteilung</h3>{_bars(analytics['role_distribution'])}</div>
        <div><h3>Auktionsstatus</h3>{_bars(status_items)}</div>
      </div>
    </section>

    <section class="panel" id="quality">
      <h2>🧹 Datenqualität</h2>
      <p class="muted">Diese Werte helfen beim Aufräumen vor Vermietung/Massennutzung. Es wird nichts gelöscht, nur angezeigt.</p>
      <div class="grid mini-grid">{quality_cards}</div>
      <h3>Auffällige Profile</h3>
      {_table(['Name','Ingame','Rolle','GS','EC'], low_profile_rows[:40], placeholder='Auffälligkeiten durchsuchen…')}
    </section>

    <section class="panel" id="members"><h2>👥 Mitgliederprofile</h2>{_table(['Name','Ingame','Rolle','GS','EC','Needliste'], profile_rows, placeholder='Mitglieder durchsuchen…')}</section>
    <section class="panel" id="events"><h2>📅 Events</h2>{_table(['Event','Zeit','Teilnehmer','Vielleicht','Abgemeldet','Voice'], event_rows, placeholder='Events durchsuchen…')}</section>
    <section class="panel"><h2>🪙 EC-Konten</h2>{_table(['Spieler','EC'], balance_rows, placeholder='EC-Konten durchsuchen…')}<h3>Unterste EC-Konten</h3>{_table(['Spieler','EC'], bottom_balance_rows, placeholder='Unterste EC-Konten durchsuchen…')}</section>
    <section class="panel" id="loot"><h2>🎁 Auktionen</h2>{_table(['Item','Status','Phase','Gebote','Führend','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel"><h2>🎙️ Voice-Sessions</h2>{_table(['Spieler','Kanal','Rein','Raus','Minuten'], voice_rows, placeholder='Voice-Sessions durchsuchen…')}</section>
    <section class="panel" id="logs"><h2>🧾 Audit-Log</h2>{_table(['Zeit','Aktion','Zusammenfassung','Actor'], audit_rows, placeholder='Audit-Logs durchsuchen…')}</section>
    """
    return _html_shell("Ebo Dashboard", body)


def _render_member_detail(data: dict[str, Any], user_id: int) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    profiles = ((snap.get("profiles") or {}).get("items") or [])
    balances = _balance_map(snap)
    needs_by_user = _needs_by_user(snap)
    profile = None
    for p in profiles:
        if isinstance(p, dict) and _user_id(p.get("user_id")) == int(user_id):
            profile = p
            break
    if not profile:
        return _html_shell(
            "Mitglied nicht gefunden",
            "<section class='panel'><h1>❌ Mitglied nicht gefunden</h1><p class='muted'>Dieses Mitglied ist nicht im aktuellen Dashboard-Snapshot oder hat nicht die gesetzte Gildenrolle.</p><p><a class='btn' href='/'>Zurück</a></p></section>",
        )

    display = profile.get("display_name") or profile.get("ingame_name") or f"User {user_id}"
    ec_value = balances.get(int(user_id))
    need_info = needs_by_user.get(int(user_id), {})
    main_needs = need_info.get("main") if isinstance(need_info, dict) else []
    secondary_needs = need_info.get("secondary") if isinstance(need_info, dict) else []

    tx_rows = []
    for tx in _tx_for_user(snap, user_id):
        tx_rows.append([_dt(tx.get("created_at")), _fmt_ec(tx.get("amount")), tx.get("raw_type"), _short(tx.get("reason"), 140)])

    voice_rows = []
    for v in _voice_for_user(snap, user_id):
        seconds = int(_num(v.get("duration_seconds"), 0))
        minutes = round(seconds / 60, 1) if seconds else "—"
        voice_rows.append([v.get("channel_name") or v.get("channel_id"), _dt(v.get("joined_at")), _dt(v.get("left_at")), minutes])

    auction_rows = []
    for a in _auctions_for_user(snap, user_id):
        auction_rows.append([a.get("item_name"), a.get("status"), a.get("phase"), _fmt_ec(a.get("top_bid_amount")) if a.get("top_bid_amount") is not None else "—", _dt(a.get("ends_at"))])

    cards = "".join([
        _card("Ingame", profile.get("ingame_name") or "—", "Profil"),
        _card("Rolle", profile.get("main_role") or "—", "Main-Rolle"),
        _card("Gearscore", profile.get("gearscore") or "—", "Profilwert"),
        _card("EC", _fmt_ec(ec_value) if ec_value is not None else "—", "aktueller Kontostand"),
    ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="#needs">Needs</a><a href="#ec">EC</a><a href="#voice">Voice</a><a href="/api/snapshot">JSON</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Mitglied</div>
        <h1>👤 {_e(display)}</h1>
        <p class="muted">User-ID: {_e(user_id)} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
      </div>
      <a class="btn" href="/">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel" id="needs">
      <h2>🎁 Needliste</h2>
      <div class="split">
        <div>{_need_list_html('Main-Needs', main_needs)}</div>
        <div>{_need_list_html('Secondary-Needs', secondary_needs)}</div>
      </div>
    </section>
    <section class="panel" id="ec"><h2>🪙 Letzte EC-Buchungen</h2>{_table(['Zeit','Betrag','Typ','Grund'], tx_rows, placeholder='Buchungen durchsuchen…')}</section>
    <section class="panel"><h2>🎁 Auktionen mit aktueller Führung/Gewinn</h2>{_table(['Item','Status','Phase','Gebot','Ende'], auction_rows, placeholder='Auktionen durchsuchen…')}</section>
    <section class="panel" id="voice"><h2>🎙️ Voice-Sessions</h2>{_table(['Kanal','Rein','Raus','Minuten'], voice_rows, placeholder='Voice durchsuchen…')}</section>
    """
    return _html_shell(f"{display} · Ebo Dashboard", body)


def _event_by_id(snap: dict[str, Any], event_id: str) -> Optional[dict[str, Any]]:
    for ev in ((snap.get("events") or {}).get("items") or []):
        if isinstance(ev, dict) and str(ev.get("event_id") or "") == str(event_id):
            return ev
    return None


def _participant_rows(people: Any) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for p in people or []:
        if not isinstance(p, dict):
            continue
        rows.append([_member_link(p.get("user_id"), p.get("display_name")), "ja" if p.get("is_dashboard_member") else "nein"])
    return rows


def _role_signup_html(event: dict[str, Any]) -> str:
    parts = ((event.get("participants") or {}).get("yes") or [])
    if not parts:
        return "<div class='empty'>Keine Zusagen vorhanden.</div>"
    blocks: list[str] = []
    for group in parts:
        if not isinstance(group, dict):
            continue
        role = group.get("role") or "Unbekannt"
        people = group.get("participants") or []
        blocks.append(f"""
        <div class="subpanel">
          <h3>{_e(role)} <span class='pill'>{_e(len(people))}</span></h3>
          {_table(['Spieler','Gildenrolle'], _participant_rows(people), placeholder=f'{role} durchsuchen…')}
        </div>
        """)
    return "".join(blocks)


def _render_event_detail(data: dict[str, Any], event_id: str) -> str:
    if not data.get("ok"):
        return _html_shell("Ebo Dashboard", f"<section class='panel'><h1>📊 Ebo Dashboard</h1><p class='muted'>{_e(data.get('error'))}</p></section>")
    snap: dict[str, Any] = data.get("snapshot") or {}
    event = _event_by_id(snap, event_id)
    if not event:
        return _html_shell(
            "Event nicht gefunden",
            "<section class='panel'><h1>❌ Event nicht gefunden</h1><p class='muted'>Dieses Event ist nicht im aktuellen Dashboard-Snapshot.</p><p><a class='btn' href='/#events'>Zurück</a></p></section>",
        )

    participants = event.get("participants") or {}
    maybe_rows = _participant_rows(participants.get("maybe") or [])
    no_rows = _participant_rows(participants.get("no") or [])
    yes_counts = event.get("yes_counts") or {}
    role_items = sorted([(str(k), int(_num(v))) for k, v in yes_counts.items()], key=lambda x: x[0].lower())

    cards = "".join([
        _card("Teilnehmer", event.get("participant_count", 0), "alle Rückmeldungen"),
        _card("Vielleicht", event.get("maybe_count", 0), "unsicher"),
        _card("Abgemeldet", event.get("no_count", 0), "Nein/abgemeldet"),
        _card("Voice", "ja" if event.get("voice_enabled") else "nein", event.get("voice_channel_id") or event.get("voice_last_channel_id") or "kein Voice"),
    ])

    body = f"""
    <nav class="topnav"><a href="/">← Übersicht</a><a href="#signups">Zusagen</a><a href="#maybe">Vielleicht</a><a href="#no">Abgemeldet</a><a href="/api/snapshot">JSON</a></nav>
    <section class="hero">
      <div>
        <div class="eyebrow">Event</div>
        <h1>📅 {_e(event.get('title') or event_id)}</h1>
        <p class="muted">Event-ID: {_e(event_id)} · Zeit: {_e(_dt(event.get('when_iso')))} · Snapshot: {_e(_dt(data.get('published_at')))}</p>
        {f"<p>{_e(event.get('description'))}</p>" if event.get('description') else ""}
      </div>
      <a class="btn" href="/#events">Zurück</a>
    </section>
    <section class="grid">{cards}</section>
    <section class="panel"><h2>📊 Rollenverteilung</h2>{_bars(role_items, max_items=12)}</section>
    <section class="panel" id="signups"><h2>✅ Zusagen nach Rolle</h2>{_role_signup_html(event)}</section>
    <section class="panel" id="maybe"><h2>🟡 Vielleicht</h2>{_table(['Spieler','Gildenrolle'], maybe_rows, placeholder='Vielleicht durchsuchen…')}</section>
    <section class="panel" id="no"><h2>❌ Abgemeldet</h2>{_table(['Spieler','Gildenrolle'], no_rows, placeholder='Abmeldungen durchsuchen…')}</section>
    """
    return _html_shell(f"{event.get('title') or 'Event'} · Ebo Dashboard", body)


def _html_shell(title: str, body: str) -> str:
    auth_note = ""
    if not str(os.getenv("DASHBOARD_PASSWORD") or "").strip():
        auth_note = '<div class="warn">⚠️ DASHBOARD_PASSWORD ist nicht gesetzt. Dashboard ist aktuell ohne Login erreichbar.</div>'
    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_e(title)}</title>
  <style>
    :root {{ --bg:#0f1014; --panel:#181a22; --panel2:#20232d; --text:#f1eadb; --muted:#a8a193; --gold:#d6a84f; --line:#333746; --red:#d96868; --green:#81c784; }}
    * {{ box-sizing:border-box; }} html {{ scroll-behavior:smooth; }}
    body {{ margin:0; font-family:Inter, system-ui, Segoe UI, sans-serif; background:radial-gradient(circle at top,#27212a 0,#0f1014 42%); color:var(--text); }}
    main {{ max-width:1240px; margin:0 auto; padding:22px 18px 60px; }}
    .topnav {{ position:sticky; top:0; z-index:5; display:flex; gap:8px; flex-wrap:wrap; padding:10px; margin:-22px -18px 18px; background:rgba(15,16,20,.88); backdrop-filter:blur(10px); border-bottom:1px solid var(--line); }}
    .topnav a {{ color:var(--text); text-decoration:none; padding:8px 10px; border:1px solid var(--line); border-radius:999px; background:rgba(24,26,34,.85); font-size:13px; }}
    .topnav a:hover {{ border-color:var(--gold); color:var(--gold); }}
    .hero {{ display:flex; justify-content:space-between; gap:18px; align-items:center; padding:26px; border:1px solid var(--line); background:linear-gradient(135deg,rgba(214,168,79,.16),rgba(24,26,34,.94)); border-radius:18px; margin-bottom:18px; }}
    .eyebrow {{ color:var(--gold); text-transform:uppercase; letter-spacing:.12em; font-size:12px; font-weight:700; }}
    h1,h2,h3 {{ margin:0 0 8px; }} p {{ color:var(--muted); }}
    .muted {{ color:var(--muted); }}
    .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:18px 0; }}
    .mini-grid {{ margin:12px 0 18px; }}
    .card,.panel {{ background:rgba(24,26,34,.92); border:1px solid var(--line); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.25); }}
    .card {{ padding:16px; }} .card-title {{ color:var(--muted); font-size:13px; }} .card-value {{ font-size:28px; font-weight:800; color:var(--gold); }} .card-sub {{ color:var(--muted); font-size:12px; }}
    .panel {{ padding:18px; margin:14px 0; scroll-margin-top:70px; }}
    .subpanel {{ background:rgba(32,35,45,.72); border:1px solid var(--line); border-radius:14px; padding:14px; margin:12px 0; }}
    .analytics-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin:12px 0 18px; }}
    .metric {{ background:var(--panel2); border:1px solid var(--line); border-radius:14px; padding:14px; }}
    .metric span {{ display:block; color:var(--muted); font-size:13px; }} .metric strong {{ display:block; color:var(--gold); font-size:28px; }} .metric small {{ color:var(--muted); }}
    .split {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .bar-row {{ display:grid; grid-template-columns:110px 1fr 44px; gap:10px; align-items:center; margin:8px 0; }} .bar-label,.bar-value {{ color:var(--muted); font-size:13px; }}
    .bar-track {{ height:10px; background:#0b0c10; border:1px solid var(--line); border-radius:999px; overflow:hidden; }} .bar-fill {{ height:100%; background:linear-gradient(90deg,var(--gold),#f1d28a); }}
    .table-search {{ width:100%; max-width:420px; margin:8px 0 12px; padding:10px 12px; border-radius:10px; border:1px solid var(--line); background:#08090d; color:var(--text); outline:none; }}
    .table-search:focus {{ border-color:var(--gold); }}
    .table-wrap {{ overflow-x:auto; }} table {{ width:100%; border-collapse:collapse; font-size:14px; }} th,td {{ padding:10px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ color:var(--gold); font-size:12px; text-transform:uppercase; letter-spacing:.05em; }} tr:hover td {{ background:rgba(255,255,255,.025); }}
    .btn {{ display:inline-block; padding:10px 14px; border-radius:10px; background:var(--gold); color:#111; font-weight:800; text-decoration:none; white-space:nowrap; }}
    .link {{ color:var(--gold); text-decoration:none; font-weight:700; }} .link:hover {{ text-decoration:underline; }}
    .pill {{ display:inline-block; padding:2px 8px; border:1px solid var(--line); border-radius:999px; color:var(--gold); font-size:12px; vertical-align:middle; }}
    .need-list {{ margin:8px 0 16px; padding-left:22px; color:var(--text); }} .need-list li {{ margin:5px 0; }}
    code {{ background:#05060a; border:1px solid var(--line); padding:2px 5px; border-radius:6px; }}
    .empty {{ color:var(--muted); padding:10px 0; }} .warn {{ background:#3a250d; border:1px solid #8a5b18; padding:12px 14px; border-radius:12px; margin-bottom:14px; color:#ffe0a3; }}
    @media(max-width:1000px) {{ .grid,.analytics-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .split {{ grid-template-columns:1fr; }} .hero {{ flex-direction:column; align-items:flex-start; }} }}
    @media(max-width:560px) {{ .grid,.analytics-grid {{ grid-template-columns:1fr; }} .bar-row {{ grid-template-columns:90px 1fr 38px; }} }}
  </style>
</head>
<body><main>{auth_note}{body}</main><script>
function filterNextTable(input) {{
  const wrap = input.nextElementSibling;
  if (!wrap) return;
  const table = wrap.querySelector('table');
  if (!table) return;
  const q = (input.value || '').toLowerCase().trim();
  for (const row of table.querySelectorAll('tbody tr')) {{
    const text = row.innerText.toLowerCase();
    row.style.display = (!q || text.includes(q)) ? '' : 'none';
  }}
}}
</script></body>
</html>"""


@app.get("/healthz")
def healthz():
    return {"ok": True, "database_url": bool(_database_url())}


@app.get("/api/snapshot")
def api_snapshot(_: bool = Depends(_auth)):
    return JSONResponse(_snapshot_payload())


@app.get("/api/analytics")
def api_analytics(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    return JSONResponse({"ok": True, "analytics": _analytics_from_snapshot(snap)})


@app.get("/api/quality")
def api_quality(_: bool = Depends(_auth)):
    payload = _snapshot_payload()
    if not payload.get("ok"):
        return JSONResponse(payload, status_code=404)
    snap = payload.get("snapshot") or {}
    analytics = _analytics_from_snapshot(snap)
    return JSONResponse({
        "ok": True,
        "role_member_count": analytics.get("role_member_count", 0),
        "missing_profiles": analytics.get("missing_profiles", 0),
        "missing_ec": analytics.get("missing_ec", 0),
        "missing_needs": analytics.get("missing_needs", 0),
        "profile_coverage": analytics.get("profile_coverage"),
        "ec_coverage": analytics.get("ec_coverage"),
        "need_coverage": analytics.get("need_coverage"),
    })


@app.get("/event/{event_id}", response_class=HTMLResponse)
def event_detail(event_id: str, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_event_detail(_snapshot_payload(), str(event_id)))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/member/{user_id}", response_class=HTMLResponse)
def member_detail(user_id: int, _: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_member_detail(_snapshot_payload(), int(user_id)))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )


@app.get("/", response_class=HTMLResponse)
def index(_: bool = Depends(_auth)):
    try:
        return HTMLResponse(_render_dashboard(_snapshot_payload()))
    except Exception as exc:
        return HTMLResponse(
            _html_shell("Ebo Dashboard Fehler", f"<section class='panel'><h1>❌ Dashboard-Fehler</h1><p>{_e(type(exc).__name__)}: {_e(exc)}</p></section>"),
            status_code=500,
        )
