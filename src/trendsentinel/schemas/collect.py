"""Request and response models for the collection endpoints."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from trendsentinel.schemas.common import ErrorBody

__all__ = [
    "CollectRunRequest",
    "CollectRunResponse",
    "CollectStatusResponse",
    "RunResultResponse",
    "RunSummary",
]

#: ``GET /items`` allows up to 100 rows. The collect body's limit is a *write* clamp, so it is
#: capped by ``settings.max_items_per_run`` and validated again in the service. Bounding it
#: here too keeps the rejection at the edge rather than after a run row has been created.
MIN_COLLECT_LIMIT = 1
MAX_COLLECT_LIMIT = 1000


class RunSummary(BaseModel):
    """One run in ``GET /collect/status``."""

    run_id: int
    source_id: int
    source_key: str | None = Field(
        default=None, description="Null when the run's source row has been deleted."
    )
    status: str = Field(
        description="`pending`, `running`, `succeeded`, `partial`, or `failed`."
    )
    trigger: str = Field(description="`scheduler` or `manual`.")
    started_at: datetime
    finished_at: datetime | None
    items_fetched: int
    items_new: int
    items_updated: int
    item_limit: int | None = Field(
        default=None,
        description="A per-run cap requested by a caller, overriding the source's config.",
    )
    error_summary: dict[str, object] | None = Field(
        default=None,
        description=(
            "Present only for a run that had errors: `{errors: [...], counts: {...}}`. "
            "Contains no stack traces or connection details."
        ),
    )


class CollectStatusResponse(BaseModel):
    """``GET /collect/status``: the newest run per source, plus anything still active."""

    runs: list[RunSummary] = Field(
        description="The most recent run for each source, newest first."
    )
    active: list[RunSummary] = Field(
        default_factory=list,
        description="Every run currently `pending` or `running`, oldest first.",
    )


class CollectRunRequest(BaseModel):
    """Body of ``POST /collect/run``."""

    model_config = ConfigDict(extra="forbid")

    source_key: str | None = Field(
        default=None,
        description=(
            "Collect only this source. Omit or null to queue every active source."
        ),
    )
    limit: int | None = Field(
        default=None,
        ge=MIN_COLLECT_LIMIT,
        le=MAX_COLLECT_LIMIT,
        description=(
            "Item cap for this run only, overriding the source's configured limit. The "
            "global `MAX_ITEMS_PER_RUN` cap still applies."
        ),
    )


class RunResultResponse(BaseModel):
    """One entry of ``POST /collect/run``'s response."""

    run_id: int | None = Field(
        default=None, description="Null when nothing was queued for this source."
    )
    source_key: str
    status: str = Field(
        description=(
            "`pending` for a newly queued run, `running` if executed inline, or `skipped` "
            "when the source already had an active run."
        )
    )


class CollectRunResponse(BaseModel):
    """``POST /collect/run``: 202 with one entry per requested source."""

    runs: list[RunResultResponse]


class ConflictDetails(BaseModel):
    """`details` for a 409 `run_in_progress` error."""

    source_key: str
    run_id: int | None = None
    status: str | None = None


class CollectErrorResponse(BaseModel):
    """Documented error shape for the collect endpoints."""

    error: ErrorBody
