from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

# psycopg wird bewusst erst in connect() importiert. So kann der Importer --help
# auch in Umgebungen laufen, in denen Requirements noch nicht installiert sind.


def _database_url() -> str:
    return str(os.getenv("DATABASE_URL") or "").strip()


def _normalized_database_url() -> str:
    url = _database_url()
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def connect():
    try:
        import psycopg  # type: ignore
        from psycopg.rows import dict_row  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"psycopg fehlt oder kann nicht geladen werden: {type(exc).__name__}: {exc}")
    url = _normalized_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL fehlt")
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=10)


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def ensure_item_catalog_schema(conn=None) -> None:
    close = False
    if conn is None:
        conn = connect()
        close = True
    try:
        try:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            conn.commit()
        except Exception:
            # Fallback: Suche funktioniert auch ohne trigram index, nur langsamer.
            try:
                conn.rollback()
            except Exception:
                pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS item_catalog (
                id BIGSERIAL PRIMARY KEY,

                source TEXT NOT NULL DEFAULT 'questlog',
                source_url TEXT NOT NULL UNIQUE,
                source_item_id TEXT,
                locale TEXT NOT NULL DEFAULT 'en',

                name TEXT NOT NULL,
                slug TEXT,
                main_category TEXT NOT NULL,
                sub_category TEXT,
                rarity TEXT,

                item_level INTEGER,
                required_level INTEGER,
                damage_min NUMERIC,
                damage_max NUMERIC,
                defense NUMERIC,

                stats JSONB NOT NULL DEFAULT '{}'::jsonb,
                abilities JSONB NOT NULL DEFAULT '[]'::jsonb,
                traits JSONB NOT NULL DEFAULT '[]'::jsonb,

                image_url TEXT,
                icon_url TEXT,

                classification_confidence TEXT NOT NULL DEFAULT 'medium',
                raw_text TEXT,
                raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,

                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        # Migration-safe additions for older local copies.
        for ddl in [
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS source_item_id TEXT",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS locale TEXT NOT NULL DEFAULT 'en'",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS slug TEXT",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS item_level INTEGER",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS required_level INTEGER",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS damage_min NUMERIC",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS damage_max NUMERIC",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS defense NUMERIC",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS stats JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS abilities JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS traits JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS image_url TEXT",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS icon_url TEXT",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS classification_confidence TEXT NOT NULL DEFAULT 'medium'",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS raw_text TEXT",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS raw_data JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()",
            "ALTER TABLE item_catalog ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
        ]:
            conn.execute(ddl)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS item_catalog_image_overrides (
                source_url TEXT PRIMARY KEY,
                image_url TEXT NOT NULL DEFAULT '',
                updated_by_id TEXT,
                updated_by_name TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_category ON item_catalog (main_category, sub_category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_rarity ON item_catalog (rarity)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_active ON item_catalog (is_active)")
        conn.commit()
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_name_trgm ON item_catalog USING gin (name gin_trgm_ops)")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            conn.execute("CREATE INDEX IF NOT EXISTS idx_item_catalog_name_lower ON item_catalog (lower(name))")
        conn.commit()
    finally:
        if close:
            conn.close()


def upsert_item(conn, item: dict[str, Any]) -> None:
    source_url = str(item.get("source_url") or "").strip()
    name = str(item.get("name") or "").strip()
    main_category = str(item.get("main_category") or "misc").strip() or "misc"
    if not source_url or not name:
        raise ValueError("source_url und name sind Pflicht")

    conn.execute(
        """
        INSERT INTO item_catalog (
            source,
            source_url,
            source_item_id,
            locale,
            name,
            slug,
            main_category,
            sub_category,
            rarity,
            item_level,
            required_level,
            damage_min,
            damage_max,
            defense,
            stats,
            abilities,
            traits,
            image_url,
            icon_url,
            classification_confidence,
            raw_text,
            raw_data,
            is_active,
            last_seen_at,
            updated_at
        )
        VALUES (
            %(source)s,
            %(source_url)s,
            %(source_item_id)s,
            %(locale)s,
            %(name)s,
            %(slug)s,
            %(main_category)s,
            %(sub_category)s,
            %(rarity)s,
            %(item_level)s,
            %(required_level)s,
            %(damage_min)s,
            %(damage_max)s,
            %(defense)s,
            %(stats)s::jsonb,
            %(abilities)s::jsonb,
            %(traits)s::jsonb,
            %(image_url)s,
            %(icon_url)s,
            %(classification_confidence)s,
            %(raw_text)s,
            %(raw_data)s::jsonb,
            TRUE,
            now(),
            now()
        )
        ON CONFLICT (source_url)
        DO UPDATE SET
            source = EXCLUDED.source,
            source_item_id = EXCLUDED.source_item_id,
            locale = EXCLUDED.locale,
            name = EXCLUDED.name,
            slug = EXCLUDED.slug,
            main_category = EXCLUDED.main_category,
            sub_category = EXCLUDED.sub_category,
            rarity = EXCLUDED.rarity,
            item_level = EXCLUDED.item_level,
            required_level = EXCLUDED.required_level,
            damage_min = EXCLUDED.damage_min,
            damage_max = EXCLUDED.damage_max,
            defense = EXCLUDED.defense,
            stats = EXCLUDED.stats,
            abilities = EXCLUDED.abilities,
            traits = EXCLUDED.traits,
            image_url = EXCLUDED.image_url,
            icon_url = EXCLUDED.icon_url,
            classification_confidence = EXCLUDED.classification_confidence,
            raw_text = EXCLUDED.raw_text,
            raw_data = EXCLUDED.raw_data,
            is_active = TRUE,
            last_seen_at = now(),
            updated_at = now()
        """,
        {
            "source": item.get("source") or "questlog",
            "source_url": source_url,
            "source_item_id": item.get("source_item_id"),
            "locale": item.get("locale") or "en",
            "name": name,
            "slug": item.get("slug"),
            "main_category": main_category,
            "sub_category": item.get("sub_category"),
            "rarity": item.get("rarity"),
            "item_level": item.get("item_level"),
            "required_level": item.get("required_level"),
            "damage_min": item.get("damage_min"),
            "damage_max": item.get("damage_max"),
            "defense": item.get("defense"),
            "stats": _json_dumps(item.get("stats") or {}),
            "abilities": _json_dumps(item.get("abilities") or []),
            "traits": _json_dumps(item.get("traits") or []),
            "image_url": item.get("image_url"),
            "icon_url": item.get("icon_url"),
            "classification_confidence": item.get("classification_confidence") or "medium",
            "raw_text": item.get("raw_text"),
            "raw_data": _json_dumps(item.get("raw_data") or {}),
        },
    )


def query_items(
    *,
    q: str = "",
    category: str = "",
    sub_category: str = "",
    rarity: str = "",
    confidence: str = "",
    sort: str = "",
    active_only: bool = True,
    limit: int = 200,
    offset: int = 0,
) -> list[dict[str, Any]]:
    ensure_item_catalog_schema()
    where: list[str] = []
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 500)), "offset": max(0, int(offset or 0))}
    if active_only:
        where.append("ic.is_active = TRUE")
    if q:
        where.append("ic.name ILIKE %(q)s")
        params["q"] = f"%{q.strip()}%"
    if category:
        where.append("ic.main_category = %(category)s")
        params["category"] = category.strip()
    if sub_category:
        where.append("ic.sub_category = %(sub_category)s")
        params["sub_category"] = sub_category.strip()
    if rarity:
        where.append("ic.rarity = %(rarity)s")
        params["rarity"] = rarity.strip()
    if confidence:
        where.append("ic.classification_confidence = %(confidence)s")
        params["confidence"] = confidence.strip()
    sql_where = "WHERE " + " AND ".join(where) if where else ""
    sort_key = str(sort or "").strip().lower()
    order_by = {
        "level_desc": "COALESCE(ic.item_level, ic.required_level, -1) DESC, ic.name ASC",
        "level_asc": "CASE WHEN COALESCE(ic.item_level, ic.required_level) IS NULL THEN 1 ELSE 0 END, COALESCE(ic.item_level, ic.required_level) ASC, ic.name ASC",
        "name": "ic.name ASC",
        "rarity": "CASE lower(COALESCE(ic.rarity, '')) WHEN 'legendary' THEN 5 WHEN 'legendär' THEN 5 WHEN 'epic' THEN 4 WHEN 'episch' THEN 4 WHEN 'rare' THEN 3 WHEN 'selten' THEN 3 WHEN 'uncommon' THEN 2 WHEN 'common' THEN 1 ELSE 0 END DESC, ic.name ASC",
    }.get(sort_key, "CASE ic.main_category WHEN 'weapon' THEN 1 WHEN 'armor' THEN 2 WHEN 'material' THEN 3 WHEN 'currency' THEN 4 ELSE 9 END, COALESCE(ic.sub_category, ''), ic.name")
    sql = f"""
        SELECT
            ic.id, ic.source, ic.source_url, ic.source_item_id, ic.locale, ic.name, ic.slug,
            ic.main_category, ic.sub_category, ic.rarity, ic.item_level, ic.required_level,
            ic.damage_min, ic.damage_max, ic.defense, ic.stats, ic.abilities, ic.traits,
            ic.image_url, ic.icon_url, ov.image_url AS manual_image_url,
            ic.classification_confidence, ic.raw_data,
            ic.is_active, ic.first_seen_at, ic.last_seen_at, ic.updated_at
        FROM item_catalog ic
        LEFT JOIN item_catalog_image_overrides ov ON ov.source_url = ic.source_url
        {sql_where}
        ORDER BY {order_by}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]



def get_item_by_id(item_id: int) -> Optional[dict[str, Any]]:
    """Lädt genau einen aktiven Katalogeintrag inklusive Bild-Override."""
    ensure_item_catalog_schema()
    sql = """
        SELECT
            ic.id, ic.source, ic.source_url, ic.source_item_id, ic.locale, ic.name, ic.slug,
            ic.main_category, ic.sub_category, ic.rarity, ic.item_level, ic.required_level,
            ic.damage_min, ic.damage_max, ic.defense, ic.stats, ic.abilities, ic.traits,
            ic.image_url, ic.icon_url, ov.image_url AS manual_image_url,
            ic.classification_confidence, ic.raw_data,
            ic.is_active, ic.first_seen_at, ic.last_seen_at, ic.updated_at
        FROM item_catalog ic
        LEFT JOIN item_catalog_image_overrides ov ON ov.source_url = ic.source_url
        WHERE ic.id = %s AND ic.is_active = TRUE
        LIMIT 1
    """
    with connect() as conn:
        row = conn.execute(sql, (int(item_id),)).fetchone()
    return dict(row) if row else None


def set_item_image_override(source_url: str, image_url: str, *, actor_id: str = "", actor_name: str = "") -> None:
    """Speichert eine manuelle Bildkorrektur anhand der Questlog-URL.

    Der Override überlebt normale Re-Imports, solange die source_url gleich bleibt.
    """
    ensure_item_catalog_schema()
    source_url = str(source_url or "").strip()
    image_url = str(image_url or "").strip()
    if not source_url:
        raise ValueError("source_url fehlt")
    with connect() as conn:
        if image_url:
            conn.execute(
                """
                INSERT INTO item_catalog_image_overrides (source_url, image_url, updated_by_id, updated_by_name, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (source_url) DO UPDATE SET
                    image_url = EXCLUDED.image_url,
                    updated_by_id = EXCLUDED.updated_by_id,
                    updated_by_name = EXCLUDED.updated_by_name,
                    updated_at = now()
                """,
                (source_url, image_url, actor_id, actor_name),
            )
        else:
            conn.execute("DELETE FROM item_catalog_image_overrides WHERE source_url = %s", (source_url,))
        conn.commit()


def item_source_url_by_id(item_id: int) -> str:
    ensure_item_catalog_schema()
    with connect() as conn:
        row = conn.execute("SELECT source_url FROM item_catalog WHERE id = %s", (int(item_id),)).fetchone()
    return str((row or {}).get("source_url") or "").strip()

def catalog_stats() -> dict[str, Any]:
    ensure_item_catalog_schema()
    with connect() as conn:
        by_category = conn.execute(
            """
            SELECT main_category, COALESCE(sub_category, '') AS sub_category, COUNT(*)::int AS count
            FROM item_catalog
            WHERE is_active = TRUE
            GROUP BY main_category, COALESCE(sub_category, '')
            ORDER BY main_category, sub_category
            """
        ).fetchall()
        totals = conn.execute(
            """
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE classification_confidence = 'low')::int AS low_confidence,
                MAX(updated_at) AS last_update
            FROM item_catalog
            WHERE is_active = TRUE
            """
        ).fetchone() or {}
    return {
        "total": int(totals.get("total") or 0),
        "low_confidence": int(totals.get("low_confidence") or 0),
        "last_update": totals.get("last_update"),
        "by_category": [dict(r) for r in by_category],
    }
