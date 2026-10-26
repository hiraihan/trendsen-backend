"""Response models for health, collected items, item pages and metric snapshots.

These mirror the database rows, minus anything a client has no business seeing. Source
models live in :mod:`trendsentinel.schemas.sources`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "HealthResponse",
    "ItemResponse",
    "ItemSnapshots",
    "ItemsPage",
    "SnapshotResponse",
]


class HealthResponse(BaseModel):
    """``GET /health``."""

    status: str = Field(description="`ok` when the database answers, otherwise `degraded`.")
    database: str = Field(description="`ok` or `unreachable`.")
    version: str = Field(description="Application version.")


class ItemResponse(BaseModel):
    """One row of ``GET /items`` or the body of ``GET /items/{id}``."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    source_id: int
    external_id: str = Field(description="The item's identity at its source.")
    url: str | None
    title: str | None
    text: str | None = Field(
        default=None, description="Summary text only. Never a full article body."
    )
    author: str | None
    published_at: datetime | None = Field(
        default=None, description="Publication time at the source. Distinct from `captured_at`."
    )
    metadata: dict[str, Any]
    captured_at: datetime = Field(description="First capture time. Immutable once written.")
    last_seen_at: datetime | None = Field(
        default=None, description="Most recent capture time. Advances on every re-run."
    )
    metrics: dict[str, float] | None = Field(
        default=None,
        description=(
            "Metrics from the item's newest snapshot, e.g. a `score` of 42.0; null when the "
            "item has none (an RSS item has no metrics)."
        ),
    )
    viral: bool = Field(
        default=False,
        description=(
            "True when the item falls in the requested viral quantile band. Only meaningful "
            "on a viral-filtered request; always false on the unfiltered list."
        ),
    )
    percentile: float | None = Field(
        default=None,
        description=(
            "The item's percentile within its day's population (0..100), present only on a "
            "viral-filtered request."
        ),
    )


class ItemsPage(BaseModel):
    """``GET /items``: a page of items plus the cursor for the next one."""

    items: list[ItemResponse]
    total: int = Field(
        description="Number of items matching the current filters, before pagination."
    )
    next_cursor: str | None = Field(
        default=None,
        description=(
            "Opaque token for the next page. Pass it back as `cursor`; null means the last "
            "page. Do not construct or modify it."
        ),
    )


class SnapshotResponse(BaseModel):
    """One row of ``GET /items/{id}/snapshots``: one measurement of one item."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    item_id: int
    captured_at: datetime = Field(description="When the metrics were measured at the source.")
    metrics: dict[str, float] = Field(
        description="Numbers the source exposed at that moment, e.g. a `score` of 42.0."
    )


class ItemSnapshots(BaseModel):
    """``GET /items/{id}/snapshots``: an item's snapshots, newest first."""

    snapshots: list[SnapshotResponse]
