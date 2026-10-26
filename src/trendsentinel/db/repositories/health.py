"""Native SQL for liveness probes."""

from __future__ import annotations

from psycopg import AsyncConnection
from psycopg.rows import DictRow

__all__ = ["check_database"]


async def check_database(conn: AsyncConnection[DictRow]) -> bool:
    """Return whether the database answers a trivial query."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT 1 AS ok")
        row = await cur.fetchone()

    return bool(row and row["ok"] == 1)
