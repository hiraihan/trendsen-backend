"""Native SQL for ``item_snapshots``.

Snapshots hold only the metrics a source actually exposes, at the moment they were measured.
They are kept apart from ``collected_items`` because a metric such as a score or a comment
count changes over time, while the item itself does not.
"""

from __future__ import annotations

import json
from datetime import datetime

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from trendsentinel.db.rows import SnapshotRow

__all__ = [
    "delete_snapshots_older_than",
    "latest_snapshot",
    "latest_snapshots_for_items",
    "list_snapshots",
    "record_snapshot",
]

_COLUMNS = "id, item_id, captured_at, metrics"


async def record_snapshot(
    conn: AsyncConnection[DictRow],
    item_id: int,
    metrics: dict[str, float],
    captured_at: datetime,
) -> bool:
    """Store a metric snapshot. Returns whether a row was written.

    A source that exposes no metrics produces no snapshot: writing an empty one would add a
    meaningless row per item per run.
    """
    if not metrics:
        return False

    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO item_snapshots (item_id, captured_at, metrics)
            VALUES (%(item_id)s, %(captured_at)s, %(metrics)s::jsonb)
            ON CONFLICT (item_id, captured_at) DO NOTHING
            RETURNING id
            """,
            {
                "item_id": item_id,
                "captured_at": captured_at,
                "metrics": _serialize(metrics),
            },
        )
        return await cur.fetchone() is not None


async def list_snapshots(
    conn: AsyncConnection[DictRow],
    item_id: int,
    *,
    limit: int = 100,
) -> list[SnapshotRow]:
    """Return an item's snapshots, newest first."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_COLUMNS} FROM item_snapshots
            WHERE item_id = %(item_id)s
            ORDER BY captured_at DESC
            LIMIT %(limit)s
            """,
            {"item_id": item_id, "limit": limit},
        )
        return [SnapshotRow.from_row(row) for row in await cur.fetchall()]


async def latest_snapshot(
    conn: AsyncConnection[DictRow],
    item_id: int,
) -> SnapshotRow | None:
    """Return an item's most recent snapshot, or ``None`` when it has none.

    Separate from :func:`list_snapshots` because the common caller — the item detail
    endpoint — wants exactly one row and would otherwise ask for a list of one.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_COLUMNS} FROM item_snapshots
            WHERE item_id = %(item_id)s
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            {"item_id": item_id},
        )
        row = await cur.fetchone()

    return SnapshotRow.from_row(row) if row is not None else None


async def latest_snapshots_for_items(
    conn: AsyncConnection[DictRow], item_ids: list[int]
) -> dict[int, SnapshotRow]:
    """Return each item's most recent snapshot, keyed by item id.

    One ``DISTINCT ON`` query for the whole page (the same pattern
    ``runs.get_last_runs_per_source`` uses), so the items list can show current metrics
    without an N+1. Items with no snapshots are simply absent from the result.
    """
    if not item_ids:
        return {}

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT DISTINCT ON (item_id) {_COLUMNS}
            FROM item_snapshots
            WHERE item_id = ANY(%(item_ids)s)
            ORDER BY item_id, captured_at DESC
            """,
            {"item_ids": item_ids},
        )
        rows = await cur.fetchall()

    return {row["item_id"]: SnapshotRow.from_row(row) for row in rows}


async def delete_snapshots_older_than(conn: AsyncConnection[DictRow], retention_days: int) -> int:
    """Delete snapshots past the retention window. Returns how many rows were removed.

    Snapshots are the growth driver in this schema, so retention is a configured window.
    The call itself is unconditional; it is the worker's job registration that is opt-in
    (``SNAPSHOT_PURGE_ENABLED``, off by default), because destroying collected data is a
    decision an operator makes rather than a side effect of running the service.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            DELETE FROM item_snapshots
            WHERE captured_at < now() - make_interval(days => %(days)s)
            RETURNING id
            """,
            {"days": retention_days},
        )
        return len(await cur.fetchall())


def _serialize(metrics: dict[str, float]) -> str:
    """Serialize metrics for an explicit ``::jsonb`` cast."""
    return json.dumps(metrics)
