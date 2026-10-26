"""``GET /items``, ``GET /items/{id}`` and ``GET /items/{id}/snapshots``: reading stored data.

Pagination is keyset-based on ``(published_at, id)``. The cursor is opaque to clients: it is
produced here and decoded here, and a malformed one is a 422 rather than a 500.

Metric history lives in ``item_snapshots`` and is read through three routes: the snapshot
list, the item detail, and the items list — the page carries each item's newest snapshot's
metrics so a card can show them without a second request.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Path, Query

from trendsentinel.api.deps import DbSession, SettingsDep
from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.collectors.base import SourceConfig, SourceRef
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemRow, SnapshotRow, SourceRow
from trendsentinel.schemas.items import (
    ItemResponse,
    ItemSnapshots,
    ItemsPage,
    SnapshotResponse,
)
from trendsentinel.services import ranking, viral

__all__ = ["require_aware", "router"]

router = APIRouter(tags=["items"])

MIN_LIMIT = 1
MAX_LIMIT = 100
DEFAULT_LIMIT = 20

MIN_SNAPSHOT_LIMIT = 1
MAX_SNAPSHOT_LIMIT = 500
DEFAULT_SNAPSHOT_LIMIT = 100


def _aware(value: datetime | None, name: str) -> datetime | None:
    """Require a timezone-aware bound, since stored times are UTC.

    A naive value would silently be interpreted in the database's local time zone, so it is
    rejected with a 422 rather than guessed at.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            f"`{name}` must include a time zone (for example `2026-10-21T00:00:00Z`)",
            status_code=422,
        )
    return value.astimezone(UTC)


def require_aware(value: datetime | None, name: str) -> datetime | None:
    """Public alias of :func:`_aware`, so sibling routes share one rule.

    The export endpoint honours the same ``since``/``until`` semantics as the list (they
    are documented as "the same filters"), so it must reject a naive bound the same way.
    A naive value would otherwise be cast by the database in its session time zone and
    silently select the wrong rows.
    """
    return _aware(value, name)


def _decode_cursor(token: str | None) -> items_repo.Cursor | None:
    """Decode the opaque cursor, turning a malformed token into a 422."""
    if not token:
        return None
    try:
        return items_repo.decode_cursor(token)
    except ValueError as exc:
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            "Invalid cursor",
            status_code=422,
            details=str(exc),
        ) from exc


@router.get(
    "/items",
    response_model=ItemsPage,
    summary="List collected items, newest publication first",
    responses={
        404: {"description": "Unknown source filter"},
        422: {"description": "Bad parameters"},
    },
)
async def list_items(
    conn: DbSession,
    settings: SettingsDep,
    source: Annotated[
        str | None, Query(description="Filter by source key, e.g. `hackernews`.")
    ] = None,
    limit: Annotated[int, Query(ge=MIN_LIMIT, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: Annotated[
        str | None, Query(description="Opaque token from a previous response's `next_cursor`.")
    ] = None,
    since: Annotated[
        datetime | None, Query(description="Only items published at or after this instant.")
    ] = None,
    until: Annotated[
        datetime | None, Query(description="Only items published at or before this instant.")
    ] = None,
    q: Annotated[
        str | None,
        Query(description="Case-insensitive match on `title` only. Not a search feature."),
    ] = None,
    keywords: Annotated[
        str | None,
        Query(description="Comma-separated keywords selecting the population for viral ranking."),
    ] = None,
    quantile_low: Annotated[
        int | None,
        Query(
            ge=0,
            le=99,
            description="Lower percentile band bound for the viral filter (default P50).",
        ),
    ] = None,
    quantile_high: Annotated[
        int | None,
        Query(
            ge=1,
            le=100,
            description="Upper percentile band bound for the viral filter (default P95).",
        ),
    ] = None,
) -> ItemsPage:
    """Return one page of items and the cursor for the next."""
    source_row = None
    source_id: int | None = None
    if source:
        source_row = await sources_repo.get_source_by_key(conn, source)
        if source_row is None:
            raise ApiError(
                ErrorCode.NOT_FOUND,
                f"Unknown source {source!r}",
                status_code=404,
            )
        source_id = source_row.id

    viral_active = (
        keywords is not None or quantile_low is not None or quantile_high is not None
    )
    if viral_active:
        return await _list_viral(
            conn,
            settings,
            source_row=source_row,
            source_id=source_id,
            limit=limit,
            cursor=cursor,
            since=since,
            until=until,
            query=q,
            keywords=keywords,
            quantile_low=quantile_low,
            quantile_high=quantile_high,
        )

    rows = await items_repo.list_items(
        conn,
        source_id=source_id,
        limit=limit + 1,
        cursor=_decode_cursor(cursor),
        since=_aware(since, "since"),
        until=_aware(until, "until"),
        query=q,
    )

    # Fetch one extra row to know whether another page exists, then drop it.
    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = _next_cursor(page) if has_more and page else None

    # The total counts the whole filtered set, independent of which page is shown.
    total = await items_repo.count_items(
        conn,
        source_id=source_id,
        since=_aware(since, "since"),
        until=_aware(until, "until"),
        query=q,
    )

    # One batched lookup — not one query per row — so a page of cards can show each
    # item's current metrics without an N+1.
    snapshots = await snapshots_repo.latest_snapshots_for_items(
        conn, [row.id for row in page]
    )

    return ItemsPage(
        items=[
            _to_response(
                row,
                metrics=_narrow_metrics(snapshots[row.id]) if row.id in snapshots else None,
            )
            for row in page
        ],
        total=total,
        next_cursor=next_cursor,
    )


async def _list_viral(
    conn: DbSession,
    settings: SettingsDep,
    *,
    source_row: SourceRow | None,
    source_id: int | None,
    limit: int,
    cursor: str | None,
    since: datetime | None,
    until: datetime | None,
    query: str | None,
    keywords: str | None,
    quantile_low: int | None,
    quantile_high: int | None,
) -> ItemsPage:
    """The viral-filtered list path: load the whole population, rank, paginate.

    The band is computed per source from the global defaults, the source's config overrides,
    and the explicit query parameters. Keyword selection defaults to the source's configured
    keywords and is overridden by the query parameter when given.
    """
    if source_row is None:
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            "The viral filter requires a source",
            status_code=422,
        )

    low = quantile_low if quantile_low is not None else settings.viral_quantile_low
    high = quantile_high if quantile_high is not None else settings.viral_quantile_high
    if not (0 <= low < high <= 100):
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            "quantile_low must be less than quantile_high",
            status_code=422,
        )

    merged = dict(source_row.config)
    if quantile_low is not None:
        merged["viral_quantile_low"] = quantile_low
    if quantile_high is not None:
        merged["viral_quantile_high"] = quantile_high
    band = ranking.resolve_band(source_row.collector_key, merged, settings)

    if keywords is not None:
        kws = tuple(k.strip().lower() for k in keywords.split(",") if k.strip())
    else:
        cfg = SourceConfig.from_ref(
            SourceRef(base_url=source_row.base_url or "", config=dict(source_row.config)),
            default_limit=30,
            default_interval=30,
        )
        kws = cfg.keywords

    rows = await items_repo.list_with_metrics(
        conn,
        source_id=source_id,
        since=_aware(since, "since"),
        until=_aware(until, "until"),
        query=query,
    )
    viral_ids = viral.classify_viral(rows, band, kws)

    ranked_all = [r for r in rows if r.item.id in viral_ids]
    # The total is the size of the whole viral-filtered set, not just the shown page.
    total = len(ranked_all)
    ranked = _after_cursor(ranked_all, _decode_cursor(cursor))

    has_more = len(ranked) > limit
    page = ranked[:limit]
    next_cursor = _next_cursor([r.item for r in page]) if has_more and page else None

    return ItemsPage(
        items=[
            _to_response(
                r.item,
                metrics=dict(r.metrics) if r.metrics else None,
                viral=True,
                percentile=viral_ids[r.item.id],
            )
            for r in page
        ],
        total=total,
        next_cursor=next_cursor,
    )


def _after_cursor(
    rows: Sequence[items_repo.ItemWithMetrics],
    cursor: items_repo.Cursor | None,
) -> list[items_repo.ItemWithMetrics]:
    """Keep only rows strictly after ``cursor``, mirroring the SQL keyset comparison."""
    if cursor is None:
        return list(rows)
    out: list[items_repo.ItemWithMetrics] = []
    for r in rows:
        pub = r.item.published_at
        if cursor.published_at is None:
            if pub is None and r.item.id < cursor.id:
                out.append(r)
        else:
            if pub is None or (pub, r.item.id) < (cursor.published_at, cursor.id):
                out.append(r)
    return out


@router.get(
    "/items/{item_id}",
    response_model=ItemResponse,
    summary="Fetch one collected item",
    responses={404: {"description": "Item not found"}, 422: {"description": "Bad id"}},
)
async def get_item(
    conn: DbSession,
    item_id: Annotated[int, Path(ge=1, description="The item's database id.")],
) -> ItemResponse:
    """Return a single item, or a 404 envelope when it does not exist."""
    row = await items_repo.get_item(conn, item_id)
    if row is None:
        raise ApiError(ErrorCode.NOT_FOUND, "Item not found", status_code=404)

    snapshot = await snapshots_repo.latest_snapshot(conn, item_id)
    return _to_response(row, metrics=_narrow_metrics(snapshot) if snapshot else None)


@router.get(
    "/items/{item_id}/snapshots",
    response_model=ItemSnapshots,
    summary="List one item's metric snapshots, newest first",
    responses={404: {"description": "Item not found"}, 422: {"description": "Bad id or limit"}},
)
async def list_item_snapshots(
    conn: DbSession,
    item_id: Annotated[int, Path(ge=1, description="The item's database id.")],
    limit: Annotated[
        int,
        Query(
            ge=MIN_SNAPSHOT_LIMIT,
            le=MAX_SNAPSHOT_LIMIT,
            description=(
                f"Maximum number of snapshots to return, newest first. "
                f"Between {MIN_SNAPSHOT_LIMIT} and {MAX_SNAPSHOT_LIMIT}."
            ),
        ),
    ] = DEFAULT_SNAPSHOT_LIMIT,
) -> ItemSnapshots:
    """Return an item's snapshots, or a 404 envelope when the item does not exist."""
    if await items_repo.get_item(conn, item_id) is None:
        raise ApiError(ErrorCode.NOT_FOUND, "Item not found", status_code=404)

    rows = await snapshots_repo.list_snapshots(conn, item_id, limit=limit)
    return ItemSnapshots(
        snapshots=[
            SnapshotResponse(
                id=row.id,
                item_id=row.item_id,
                captured_at=row.captured_at,
                metrics=_narrow_metrics(row),
            )
            for row in rows
        ]
    )


def _next_cursor(page: list[ItemRow]) -> str:
    """Build the cursor pointing just past the last row of a page."""
    last = page[-1]
    return items_repo.encode_cursor(
        items_repo.Cursor(id=last.id, published_at=last.published_at)
    )


def _to_response(
    row: ItemRow,
    *,
    metrics: dict[str, float] | None = None,
    viral: bool = False,
    percentile: float | None = None,
) -> ItemResponse:
    """Adapt a database row to the response model.

    ``metrics`` is passed in rather than looked up here: the list route shares this helper and
    must not issue one snapshot query per row. ``viral``/``percentile`` are only populated on
    the viral-filtered path; the unfiltered list leaves them at their defaults.
    """
    return ItemResponse(
        id=row.id,
        source_id=row.source_id,
        external_id=row.external_id,
        url=row.url,
        title=row.title,
        text=row.text,
        author=row.author,
        published_at=row.published_at,
        metadata=dict(row.metadata),
        captured_at=row.captured_at,
        last_seen_at=row.last_seen_at,
        metrics=metrics,
        viral=viral,
        percentile=percentile,
    )


def _narrow_metrics(snapshot: SnapshotRow) -> dict[str, float]:
    """Narrow a snapshot's stored metrics to the numbers a client may see.

    ``metrics`` is a ``jsonb`` column, so nothing at the database level guarantees every value
    is a number. Coercing blindly would turn one malformed stored row into a 500 for every
    reader of that item; dropping the value instead degrades to the most a client can safely be
    shown. The writers only ever store floats, so this is a guard, not a conversion path.
    """
    narrowed: dict[str, float] = {}
    for key, value in snapshot.metrics.items():
        if isinstance(value, bool):
            # ``bool`` is an ``int`` in Python; a flag is not a metric.
            continue
        if isinstance(value, int | float):
            narrowed[key] = float(value)
    return narrowed
