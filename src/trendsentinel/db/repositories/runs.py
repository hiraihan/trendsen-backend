"""Native SQL for ``collection_runs``, plus the two duplicate-work guards.

Two independent guards cooperate here:

* The partial unique index ``collection_runs_one_active_per_source`` (see migration 0001)
  prevents a second active run row for a source, whatever created it.
* ``pg_try_advisory_lock`` prevents two processes from performing the *same work* when the
  index has been cleared by the stale-run reaper but another process is still executing.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation
from psycopg.rows import DictRow

from trendsentinel.db.rows import RunRow, RunStatus

__all__ = [
    "RunInProgressError",
    "claim_pending_run",
    "create_run",
    "finish_run",
    "get_active_runs",
    "get_last_runs_per_source",
    "get_pending_runs",
    "reap_stale_runs",
    "release_advisory_lock",
    "try_advisory_lock",
    "update_run_progress",
]

_COLUMNS = """
    id, source_id, started_at, finished_at, status, trigger,
    items_fetched, items_new, items_updated, error_summary, item_limit
"""

# Arbitrary namespace so this project's locks cannot collide with another application
# sharing the database. The second key is always the source id.
_LOCK_NAMESPACE = 8_431_001


class RunInProgressError(Exception):
    """A source already has an active run.

    Raised from the database's partial unique index rather than from a pre-check, so the
    guarantee holds even when two processes race.
    """

    def __init__(self, source_id: int) -> None:
        super().__init__(f"source {source_id} already has an active collection run")
        self.source_id = source_id


async def create_run(
    conn: AsyncConnection[DictRow],
    source_id: int,
    trigger: str,
    *,
    status: str = RunStatus.RUNNING,
    item_limit: int | None = None,
) -> RunRow:
    """Open a collection run.

    Args:
        source_id: The source being collected.
        trigger: ``scheduler`` or ``manual``.
        status: ``running`` for a run about to execute, ``pending`` for a queued manual run.
        item_limit: A caller-supplied item cap, stored so a queued run keeps it. ``None``
            means "use the source's configured limit".

    Raises:
        RunInProgressError: the source already has a pending or running run.
    """
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                INSERT INTO collection_runs (source_id, status, trigger, item_limit)
                VALUES (%(source_id)s, %(status)s, %(trigger)s, %(item_limit)s)
                RETURNING {_COLUMNS}
                """,
                {
                    "source_id": source_id,
                    "status": status,
                    "trigger": trigger,
                    "item_limit": item_limit,
                },
            )
            row = await cur.fetchone()
    except UniqueViolation as exc:
        raise RunInProgressError(source_id) from exc

    if row is None:  # pragma: no cover - RETURNING always yields a row
        raise RuntimeError("create_run returned no row")

    return RunRow.from_row(row)


async def claim_pending_run(conn: AsyncConnection[DictRow], run_id: int) -> RunRow | None:
    """Move a pending run to running, or return ``None`` if someone else claimed it.

    The conditional ``UPDATE`` makes claiming atomic: only one worker can win.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE collection_runs
            SET status = %(running)s
            WHERE id = %(id)s AND status = %(pending)s
            RETURNING {_COLUMNS}
            """,
            {"id": run_id, "running": RunStatus.RUNNING, "pending": RunStatus.PENDING},
        )
        row = await cur.fetchone()

    return RunRow.from_row(row) if row else None


async def update_run_progress(
    conn: AsyncConnection[DictRow],
    run_id: int,
    *,
    items_fetched: int,
    items_new: int,
    items_updated: int,
) -> None:
    """Publish live counts on a running run so the status view can show progress.

    Only touches non-terminal runs; a finished run's counts are set by
    :func:`finish_run` and must not be clobbered by a late progress write.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE collection_runs
            SET items_fetched = %(items_fetched)s,
                items_new     = %(items_new)s,
                items_updated = %(items_updated)s
            WHERE id = %(id)s AND status NOT IN ('succeeded', 'partial', 'failed')
            """,
            {
                "id": run_id,
                "items_fetched": items_fetched,
                "items_new": items_new,
                "items_updated": items_updated,
            },
        )


async def finish_run(
    conn: AsyncConnection[DictRow],
    run_id: int,
    *,
    status: str,
    items_fetched: int = 0,
    items_new: int = 0,
    items_updated: int = 0,
    error_summary: dict[str, Any] | None = None,
) -> None:
    """Close a run with its outcome and counts."""
    if status not in RunStatus.TERMINAL:
        raise ValueError(f"{status!r} is not a terminal run status")

    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE collection_runs
            SET status        = %(status)s,
                finished_at   = now(),
                items_fetched = %(items_fetched)s,
                items_new     = %(items_new)s,
                items_updated = %(items_updated)s,
                error_summary = %(error_summary)s::jsonb
            WHERE id = %(id)s
            """,
            {
                "id": run_id,
                "status": status,
                "items_fetched": items_fetched,
                "items_new": items_new,
                "items_updated": items_updated,
                "error_summary": _json_or_null(error_summary),
            },
        )


async def try_advisory_lock(conn: AsyncConnection[DictRow], source_id: int) -> bool:
    """Try to take the per-source advisory lock on this connection.

    The lock is session-scoped, so the caller must hold the same connection until
    :func:`release_advisory_lock` runs.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT pg_try_advisory_lock(%(namespace)s, %(source_id)s) AS acquired",
            {"namespace": _LOCK_NAMESPACE, "source_id": source_id},
        )
        row = await cur.fetchone()

    return bool(row and row["acquired"])


async def release_advisory_lock(conn: AsyncConnection[DictRow], source_id: int) -> None:
    """Release the per-source advisory lock held on this connection."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT pg_advisory_unlock(%(namespace)s, %(source_id)s)",
            {"namespace": _LOCK_NAMESPACE, "source_id": source_id},
        )


async def reap_stale_runs(conn: AsyncConnection[DictRow], older_than_minutes: int) -> int:
    """Mark abandoned runs as failed and return how many were reaped.

    Without this, a worker killed mid-run leaves a `running` row whose partial unique index
    entry blocks that source forever: collection would silently stop.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE collection_runs
            SET status        = %(failed)s,
                finished_at   = now(),
                error_summary = jsonb_build_object(
                    'reason', 'reaped as stale',
                    'detected_after_minutes', %(minutes)s
                )
            WHERE status = ANY(%(active)s)
              AND started_at < now() - make_interval(mins => %(minutes)s)
            RETURNING id
            """,
            {
                "failed": RunStatus.FAILED,
                "active": sorted(RunStatus.ACTIVE),
                "minutes": older_than_minutes,
            },
        )
        return len(await cur.fetchall())


async def get_pending_runs(conn: AsyncConnection[DictRow]) -> list[RunRow]:
    """Return runs queued by a manual trigger and not yet picked up."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_COLUMNS} FROM collection_runs
            WHERE status = %(pending)s
            ORDER BY started_at
            """,
            {"pending": RunStatus.PENDING},
        )
        return [RunRow.from_row(row) for row in await cur.fetchall()]


async def get_active_runs(conn: AsyncConnection[DictRow]) -> list[RunRow]:
    """Return runs that are currently pending or running."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT {_COLUMNS} FROM collection_runs
            WHERE status = ANY(%(active)s)
            ORDER BY started_at
            """,
            {"active": sorted(RunStatus.ACTIVE)},
        )
        return [RunRow.from_row(row) for row in await cur.fetchall()]


async def get_last_runs_per_source(
    conn: AsyncConnection[DictRow], *, limit_sources: int = 100
) -> list[RunRow]:
    """Return the most recent run for each source, newest first."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT DISTINCT ON (source_id) {_COLUMNS}
            FROM collection_runs
            ORDER BY source_id, started_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit_sources},
        )
        rows = [RunRow.from_row(row) for row in await cur.fetchall()]

    return sorted(rows, key=lambda run: run.started_at, reverse=True)


def _json_or_null(value: dict[str, Any] | None) -> str | None:
    """Serialize a summary for an explicit ``::jsonb`` cast, preserving ``NULL``."""
    if value is None:
        return None

    return json.dumps(value, default=_json_default)


def _json_default(value: object) -> str:
    """Fallback serializer for values json cannot handle natively."""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
