"""``GET /collect/status`` and ``POST /collect/run``: triggering and observing collection.

A manual trigger does not collect inline by default. It inserts a ``pending`` run row and the
worker executes it, which keeps request latency bounded and means manual and scheduled
collection share exactly one execution path. ``settings.run_manual_inline`` exists for
single-process local development only.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, status

from trendsentinel.api.deps import AdminKey, CollectionServiceDep, DbSession, SettingsDep
from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import RunRow, SourceRow
from trendsentinel.schemas.collect import (
    CollectRunRequest,
    CollectRunResponse,
    CollectStatusResponse,
    RunResultResponse,
    RunSummary,
)
from trendsentinel.services.collection import SKIPPED, CollectionService, RunResult

__all__ = ["router"]

router = APIRouter(tags=["collect"])


def _summary(run: RunRow, source_key: str | None) -> RunSummary:
    """Adapt a run row to the response model."""
    return RunSummary(
        run_id=run.id,
        source_id=run.source_id,
        source_key=source_key,
        status=run.status,
        trigger=run.trigger,
        started_at=run.started_at,
        finished_at=run.finished_at,
        items_fetched=run.items_fetched,
        items_new=run.items_new,
        items_updated=run.items_updated,
        item_limit=run.item_limit,
        error_summary=dict(run.error_summary) if run.error_summary is not None else None,
    )


async def _source_keys(conn: DbSession) -> dict[int, str]:
    """Map source ids to keys for the status view.

    A run whose source was deleted has no key; that is represented as ``None`` rather than by
    dropping the run from the response.
    """
    rows = await sources_repo.list_sources(conn)
    return {row.id: row.key for row in rows}


@router.get(
    "/collect/status",
    response_model=CollectStatusResponse,
    summary="Last run per source, plus every run still active",
)
async def collect_status(conn: DbSession) -> CollectStatusResponse:
    """Report the newest run for each source and everything currently pending or running."""
    keys = await _source_keys(conn)
    last_runs = await runs_repo.get_last_runs_per_source(conn)
    active_runs = await runs_repo.get_active_runs(conn)

    return CollectStatusResponse(
        runs=[_summary(run, keys.get(run.source_id)) for run in last_runs],
        active=[_summary(run, keys.get(run.source_id)) for run in active_runs],
    )


@router.post(
    "/collect/run",
    response_model=CollectRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue a collection run (requires X-API-Key)",
    responses={
        401: {"description": "Missing or invalid X-API-Key"},
        404: {"description": "Unknown source key"},
        409: {"description": "The source already has an active run"},
        422: {"description": "Bad request body"},
    },
)
async def collect_run(
    conn: DbSession,
    service: CollectionServiceDep,
    settings: SettingsDep,
    _admin: AdminKey,
    body: Annotated[CollectRunRequest | None, Body()] = None,
) -> CollectRunResponse:
    """Queue a run for one source, or for every active source when none is named.

    Returns 202 with one entry per source. A single source that already has an active run is a
    409: the caller asked for that source specifically and must learn that it was not queued.
    When every active source is requested, an already-running source is reported per entry
    instead, because rejecting the whole request because one source is busy would be wrong.
    """
    request = body or CollectRunRequest()

    if request.source_key is not None:
        source = await sources_repo.get_source_by_key(conn, request.source_key)
        if source is None:
            raise ApiError(
                ErrorCode.NOT_FOUND,
                f"Unknown source {request.source_key!r}",
                status_code=404,
            )
        result = await _run_one(service, source, request.limit, settings.run_manual_inline)
        if result.skipped:
            raise ApiError(
                ErrorCode.CONFLICT,
                f"Source {source.key!r} already has an active run",
                status_code=409,
                details={"source_key": source.key, "run_id": result.run_id, "status": SKIPPED},
            )
        return CollectRunResponse(runs=[_to_result(result)])

    sources = await sources_repo.list_sources(conn, active_only=True)
    results = [
        await _run_one(service, source, request.limit, settings.run_manual_inline)
        for source in sources
    ]

    return CollectRunResponse(runs=[_to_result(result) for result in results])


async def _run_one(
    service: CollectionService,
    source: SourceRow,
    item_limit: int | None,
    inline: bool,
) -> RunResult:
    """Queue a run, or execute it inline when ``run_manual_inline`` is enabled."""
    if inline:
        return await service.collect_source(source, "manual", item_limit=item_limit)

    return await service.enqueue_source(source, "manual", item_limit=item_limit)


def _to_result(result: RunResult) -> RunResultResponse:
    """Adapt a service result to the response model."""
    return RunResultResponse(
        run_id=result.run_id,
        source_key=result.source_key,
        status=result.status,
    )
