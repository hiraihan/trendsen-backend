"""``GET /export/items.csv``: the collected dataset as CSV for offline analysis.

This is a data-collection project first, so the export is a first-class endpoint, not an
afterthought: the same filters as ``GET /items`` (source, since, until, title query) select
the rows, and the response streams row-by-row so a full export never builds one giant
string in memory.

The columns keep the two time axes the spec (§5) refuses to conflate: ``published_at_utc``
is when the post went up at the source, ``captured_at_utc`` is when this project stored it.
Both are emitted as ISO-8601 UTC so a spreadsheet or pandas reads them unambiguously.

Bounding the export: a hard ``max_rows`` cap (default and ceiling below) keeps an
unfiltered export of a very large dataset from hammering the database for minutes; the
``X-Total-Matching`` header reports how many rows matched, so a truncated export is
visible rather than silent.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query, Response
from fastapi.responses import StreamingResponse

from trendsentinel.api.deps import DbSession, SettingsDep
from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.api.routes import items as items_route
from trendsentinel.collectors.base import SourceConfig, SourceRef
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.repositories.items import ExportFilters
from trendsentinel.db.rows import SourceRow
from trendsentinel.services import ranking, viral

__all__ = ["router"]

router = APIRouter(tags=["export"])

DEFAULT_MAX_ROWS = 10_000
MAX_MAX_ROWS = 100_000

_CSV_FIELDS = [
    "item_id",
    "source_key",
    "external_id",
    "url",
    "title",
    "text",
    "author",
    "published_at_utc",
    "captured_at_utc",
    "last_seen_at_utc",
    "metrics_latest_json",
    "metadata_json",
    # Source & collection provenance (joined from `sources`).
    "source_name",
    "source_type",
    "source_url",
    "collection_method",
    # Content metadata collected alongside identity.
    "language",
    "content_type",
    "author_url",
    "thumbnail_url",
]

_CSV_FIELDS_VIRAL = [*_CSV_FIELDS, "viral", "percentile"]


def _iso_utc(value: datetime | None) -> str:
    """Render a stored instant as ISO-8601 UTC, or an empty cell when absent."""
    if value is None:
        return ""
    if value.tzinfo is None:
        # Stored values are timezone-aware by schema; a naive one would be a bug.
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _json_cell(value: Any) -> str:
    """Serialize a jsonb value for one CSV cell; an absent value stays an empty cell."""
    if value is None:
        return ""
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return ""


def _csv_row(row: Mapping[str, Any]) -> list[Any]:
    """One stored row, verbatim, in the documented column order."""
    metrics = row["metrics_latest"]
    return [
        row["id"],
        row["source_key"],
        row["external_id"],
        row["url"] or "",
        row["title"] or "",
        row["text"] or "",
        row["author"] or "",
        _iso_utc(row["published_at"]),
        _iso_utc(row["captured_at"]),
        _iso_utc(row["last_seen_at"]),
        _json_cell(metrics),
        _json_cell(row["metadata"]),
        row["source_name"] or "",
        row["source_type"] or "",
        row["source_url"] or "",
        row["collection_method"] or "",
        row["language"] or "",
        row["content_type"] or "",
        row["author_url"] or "",
        row["thumbnail_url"] or "",
    ]


def _csv_row_viral(
    row: items_repo.ItemWithMetrics,
    percentile: float,
    source_meta: Mapping[str, Any],
) -> list[Any]:
    """One viral-ranked row, with the ranking fields appended."""
    item = row.item
    return [
        item.id,
        "",
        item.external_id,
        item.url or "",
        item.title or "",
        item.text or "",
        item.author or "",
        _iso_utc(item.published_at),
        _iso_utc(item.captured_at),
        _iso_utc(item.last_seen_at),
        _json_cell(dict(row.metrics)),
        _json_cell(dict(item.metadata)),
        source_meta["name"],
        source_meta["type"],
        source_meta["url"],
        source_meta["method"],
        item.language or "",
        item.content_type or "",
        item.author_url or "",
        item.thumbnail_url or "",
        "true",
        f"{percentile:.2f}",
    ]


@router.get(
    "/export/items.csv/plan",
    summary="Report how many rows an export would contain",
    responses={
        200: {"description": "Row count for the requested filters"},
        404: {"description": "Unknown source"},
        422: {"description": "Bad parameters"},
    },
)
async def plan_items_csv(
    conn: DbSession,
    settings: SettingsDep,
    source: Annotated[
        str | None, Query(description="Filter by source key, e.g. `hackernews`.")
    ] = None,
    since: Annotated[
        datetime | None, Query(description="Only items published at or after this instant.")
    ] = None,
    until: Annotated[
        datetime | None, Query(description="Only items published at or before this instant.")
    ] = None,
    q: Annotated[
        str | None, Query(description="Case-insensitive match on `title` only.")
    ] = None,
    keywords: Annotated[
        str | None,
        Query(description="Comma-separated keywords selecting the population for viral ranking."),
    ] = None,
    quantile_low: Annotated[
        int | None,
        Query(ge=0, le=99, description="Lower percentile band bound for the viral filter."),
    ] = None,
    quantile_high: Annotated[
        int | None,
        Query(ge=1, le=100, description="Upper percentile band bound for the viral filter."),
    ] = None,
    max_rows: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_MAX_ROWS,
            description=f"The ceiling a download with this filter set would use. "
            f"Between 1 and {MAX_MAX_ROWS}. Default {DEFAULT_MAX_ROWS}.",
        ),
    ] = DEFAULT_MAX_ROWS,
) -> dict[str, int | bool]:
    """Report the row count for a set of export filters, without streaming the data.

    A download is a browser navigation, so the response headers of the CSV itself are
    never shown to the operator — a truncated export would be silent. This endpoint lets
    the dialog say "1 234 rows match, your ceiling is 10 000" *before* the download, so
    the truncation is visible rather than discovered later in a spreadsheet.
    """
    viral_active = (
        keywords is not None or quantile_low is not None or quantile_high is not None
    )
    if viral_active:
        ranked, _, _ = await _viral_selection(
            conn,
            settings,
            source=source,
            since=since,
            until=until,
            q=q,
            keywords=keywords,
            quantile_low=quantile_low,
            quantile_high=quantile_high,
        )
        return {"total_matching": len(ranked), "max_rows": max_rows}

    source_id = await _source_id_for(conn, source)
    filters = ExportFilters(
        source_id=source_id,
        since=items_route.require_aware(since, "since"),
        until=items_route.require_aware(until, "until"),
        query=q,
    )
    # The count is exact (header semantics) regardless of the ceiling; the ceiling is
    # echoed so the caller compares the same numbers a download would apply.
    total = await items_repo.count_export_rows(conn, filters)
    return {"total_matching": total, "max_rows": max_rows}


async def _source_id_for(conn: Any, key: str | None) -> int | None:
    """Resolve a source key to its id, or 404 when the key is unknown."""
    if key is None:
        return None
    row = await sources_repo.get_source_by_key(conn, key)
    if row is None:
        raise ApiError(ErrorCode.NOT_FOUND, f"Unknown source {key!r}", status_code=404)
    return row.id


async def _viral_selection(
    conn: Any,
    settings: SettingsDep,
    *,
    source: str | None,
    since: datetime | None,
    until: datetime | None,
    q: str | None,
    keywords: str | None,
    quantile_low: int | None,
    quantile_high: int | None,
) -> tuple[list[items_repo.ItemWithMetrics], dict[int, float], SourceRow]:
    """Load and rank the viral population, returning viral items and their percentiles."""
    if source is None:
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            "The viral filter requires a source",
            status_code=422,
        )
    source_row = await sources_repo.get_source_by_key(conn, source)
    if source_row is None:
        raise ApiError(ErrorCode.NOT_FOUND, f"Unknown source {source!r}", status_code=404)

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
        source_id=source_row.id,
        since=items_route.require_aware(since, "since"),
        until=items_route.require_aware(until, "until"),
        query=q,
    )
    viral_ids = viral.classify_viral(rows, band, kws)
    ranked = [r for r in rows if r.item.id in viral_ids]
    return ranked, viral_ids, source_row


@router.get(
    "/export/items.csv",
    summary="Export collected items as CSV",
    responses={
        200: {"description": "CSV stream"},
        404: {"description": "Unknown source"},
        422: {"description": "Bad parameters"},
    },
)
async def export_items_csv(
    conn: DbSession,
    settings: SettingsDep,
    source: Annotated[
        str | None, Query(description="Filter by source key, e.g. `hackernews`.")
    ] = None,
    since: Annotated[
        datetime | None, Query(description="Only items published at or after this instant.")
    ] = None,
    until: Annotated[
        datetime | None, Query(description="Only items published at or before this instant.")
    ] = None,
    q: Annotated[
        str | None, Query(description="Case-insensitive match on `title` only.")
    ] = None,
    keywords: Annotated[
        str | None,
        Query(description="Comma-separated keywords selecting the population for viral ranking."),
    ] = None,
    quantile_low: Annotated[
        int | None,
        Query(ge=0, le=99, description="Lower percentile band bound for the viral filter."),
    ] = None,
    quantile_high: Annotated[
        int | None,
        Query(ge=1, le=100, description="Upper percentile band bound for the viral filter."),
    ] = None,
    max_rows: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_MAX_ROWS,
            description=f"Row ceiling, between 1 and {MAX_MAX_ROWS}. Default {DEFAULT_MAX_ROWS}.",
        ),
    ] = DEFAULT_MAX_ROWS,
) -> Response:
    """Stream the filtered items as CSV, newest publication first."""
    viral_active = (
        keywords is not None or quantile_low is not None or quantile_high is not None
    )
    if viral_active:
        ranked, viral_ids, source_row = await _viral_selection(
            conn,
            settings,
            source=source,
            since=since,
            until=until,
            q=q,
            keywords=keywords,
            quantile_low=quantile_low,
            quantile_high=quantile_high,
        )
        return _viral_csv_response(
            ranked, viral_ids, source_row, max_rows=max_rows
        )

    source_id = await _source_id_for(conn, source)

    filters = ExportFilters(
        source_id=source_id,
        since=items_route.require_aware(since, "since"),
        until=items_route.require_aware(until, "until"),
        query=q,
    )
    total = await items_repo.count_export_rows(conn, filters)

    def _header() -> str:
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\n").writerow(_CSV_FIELDS)
        return buf.getvalue()

    async def _body() -> AsyncIterator[str]:
        yield _header()
        async for row in items_repo.iterate_export_rows(
            conn, filters, max_rows=max_rows
        ):
            buf = io.StringIO()
            csv.writer(buf, lineterminator="\n").writerow(_csv_row(row))
            yield buf.getvalue()

    return StreamingResponse(
        _body(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="trendsentinel-items.csv"',
            "X-Total-Matching": str(total),
            "X-Max-Rows": str(max_rows),
            "Cache-Control": "no-store",
        },
    )


def _viral_csv_response(
    ranked: list[items_repo.ItemWithMetrics],
    viral_ids: dict[int, float],
    source_row: SourceRow,
    *,
    max_rows: int,
) -> Response:
    """Stream the viral-ranked population as CSV (in memory; the cohort is small)."""
    total = len(ranked)
    source_meta = {
        "name": source_row.name or "",
        "type": source_row.source_type or "",
        "url": source_row.base_url or "",
        "method": source_row.integration or "",
    }

    def _header() -> str:
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\n").writerow(_CSV_FIELDS_VIRAL)
        return buf.getvalue()

    async def _body() -> AsyncIterator[str]:
        yield _header()
        for row in ranked[:max_rows]:
            buf = io.StringIO()
            csv.writer(buf, lineterminator="\n").writerow(
                _csv_row_viral(row, percentile=viral_ids[row.item.id], source_meta=source_meta)
            )
            yield buf.getvalue()

    return StreamingResponse(
        _body(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="trendsentinel-items.csv"',
            "X-Total-Matching": str(total),
            "X-Max-Rows": str(max_rows),
            "Cache-Control": "no-store",
        },
    )
