"""``GET /sources``: the configured sources, with secrets removed.

A source's ``config`` is stored as ``jsonb`` and may carry credentials for a future source,
so it is sanitized by key name before it is serialized. The raw value never leaves this
module.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from trendsentinel.api.deps import AdminKey, DbSession
from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import SourceRow
from trendsentinel.schemas.sources import (
    SourceListResponse,
    SourceResponse,
    SourceUpdateRequest,
    sanitize_config,
)

__all__ = ["router"]

router = APIRouter(tags=["sources"])


@router.get(
    "/sources",
    response_model=SourceListResponse,
    summary="List configured sources",
)
async def list_sources(
    conn: DbSession,
    active_only: bool = Query(default=False, description="Return only active sources."),
) -> SourceListResponse:
    """Return every source, optionally filtered to the active ones."""
    rows = await sources_repo.list_sources(conn, active_only=active_only)
    return SourceListResponse(sources=[_to_response(row) for row in rows])


@router.patch(
    "/sources/{key}",
    response_model=SourceResponse,
    summary="Set whether a source is collected",
)
async def set_source_active(
    conn: DbSession,
    key: str,
    body: SourceUpdateRequest,
    _admin: AdminKey,
) -> SourceResponse:
    """Enable or pause collection for one source (requires X-API-Key)."""
    row = await sources_repo.set_source_active(
        conn, key=key, is_active=body.is_active
    )
    if row is None:
        raise ApiError(
            ErrorCode.NOT_FOUND,
            f"Unknown source {key!r}",
            status_code=404,
        )
    return _to_response(row)


def _to_response(row: SourceRow) -> SourceResponse:
    """Adapt a database row, sanitizing ``config`` on the way out."""
    return SourceResponse(
        id=row.id,
        key=row.key,
        name=row.name,
        integration=row.integration,
        base_url=row.base_url,
        is_active=row.is_active,
        config=sanitize_config(dict(row.config)) or {},
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
