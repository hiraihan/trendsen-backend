"""Native SQL for the ``sources`` table."""

from __future__ import annotations

from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from trendsentinel.db.rows import SourceRow

__all__ = [
    "get_source_by_id",
    "get_source_by_key",
    "list_sources",
    "set_source_active",
]

_COLUMNS = """
    id, key, name, integration, base_url, is_active, config, created_at, updated_at,
    source_type
"""


async def list_sources(
    conn: AsyncConnection[DictRow], *, active_only: bool = False
) -> list[SourceRow]:
    """Return all sources, optionally only the active ones."""
    query = f"SELECT {_COLUMNS} FROM sources"
    params: dict[str, Any] = {}
    if active_only:
        query += " WHERE is_active = true"
    query += " ORDER BY key"

    async with conn.cursor() as cur:
        await cur.execute(query, params)
        return [SourceRow.from_row(row) for row in await cur.fetchall()]


async def get_source_by_key(conn: AsyncConnection[DictRow], key: str) -> SourceRow | None:
    """Return a single source by its stable key, or ``None``."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM sources WHERE key = %(key)s",
            {"key": key},
        )
        row = await cur.fetchone()

    return SourceRow.from_row(row) if row else None


async def get_source_by_id(conn: AsyncConnection[DictRow], source_id: int) -> SourceRow | None:
    """Return a single source by primary key, or ``None``.

    Used when executing a queued run: the run row stores ``source_id``, not a key.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM sources WHERE id = %(id)s",
            {"id": source_id},
        )
        row = await cur.fetchone()

    return SourceRow.from_row(row) if row else None


async def set_source_active(
    conn: AsyncConnection[DictRow], *, key: str, is_active: bool
) -> SourceRow | None:
    """Toggle a source's collection flag; returns the updated row or ``None``."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE sources
            SET is_active = %(is_active)s, updated_at = now()
            WHERE key = %(key)s
            RETURNING {_COLUMNS}
            """,
            {"is_active": is_active, "key": key},
        )
        row = await cur.fetchone()

    return SourceRow.from_row(row) if row else None
