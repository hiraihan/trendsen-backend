"""Native SQL for ``collected_items`` and ``item_snapshots``.

The upsert here is what makes collection idempotent. Re-running a collector must never
produce a duplicate row, and a later, thinner payload must never erase data an earlier
payload already stored.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from trendsentinel.db.rows import ItemRow, ItemUpsert

__all__ = [
    "Cursor",
    "ExportFilters",
    "ItemWithMetrics",
    "count_export_rows",
    "decode_cursor",
    "encode_cursor",
    "get_item",
    "iterate_export_rows",
    "list_items",
    "list_with_metrics",
    "upsert_item",
]

_COLUMNS = """
    id, source_id, external_id, url, title, text, author,
    published_at, metadata, captured_at, last_seen_at,
    language, content_type, author_url, thumbnail_url
"""

# `metadata` merges with `||` rather than replacing, and nullable columns keep their
# previously stored value when the new payload omits them.
_UPSERT = """
    INSERT INTO collected_items (
        source_id, external_id, url, title, text, author,
        published_at, metadata, captured_at, last_seen_at,
        language, content_type, author_url, thumbnail_url, content_fingerprint
    )
    VALUES (
        %(source_id)s, %(external_id)s, %(url)s, %(title)s, %(text)s, %(author)s,
        %(published_at)s, %(metadata)s, %(captured_at)s, %(captured_at)s,
        %(language)s, %(content_type)s, %(author_url)s, %(thumbnail_url)s,
        %(fingerprint)s
    )
    ON CONFLICT (source_id, external_id) DO UPDATE SET
        url          = COALESCE(EXCLUDED.url, collected_items.url),
        title        = COALESCE(EXCLUDED.title, collected_items.title),
        text         = COALESCE(EXCLUDED.text, collected_items.text),
        author       = COALESCE(EXCLUDED.author, collected_items.author),
        published_at = COALESCE(EXCLUDED.published_at, collected_items.published_at),
        metadata     = collected_items.metadata || EXCLUDED.metadata,
        language     = COALESCE(EXCLUDED.language, collected_items.language),
        content_type = COALESCE(EXCLUDED.content_type, collected_items.content_type),
        author_url   = COALESCE(EXCLUDED.author_url, collected_items.author_url),
        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, collected_items.thumbnail_url),
        content_fingerprint =
            COALESCE(EXCLUDED.content_fingerprint, collected_items.content_fingerprint),
        last_seen_at = EXCLUDED.last_seen_at
    RETURNING id, (xmax = 0) AS inserted
"""


class Cursor:
    """Opaque keyset pagination position: the last row's ``(published_at, id)``."""

    __slots__ = ("id", "published_at")

    def __init__(self, id: int, published_at: datetime | None) -> None:
        self.id = id
        self.published_at = published_at


def encode_cursor(cursor: Cursor) -> str:
    """Encode a position as an opaque token.

    ``published_at`` is nullable, so a sentinel ``-`` marks the null group. Clients must not
    construct these tokens themselves; the encoding is an implementation detail.
    """
    timestamp = "-" if cursor.published_at is None else cursor.published_at.isoformat()
    raw = f"{timestamp}|{cursor.id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(token: str) -> Cursor:
    """Decode a token produced by :func:`encode_cursor`.

    Raises:
        ValueError: the token is malformed. Callers surface this as a 422.
    """
    padded = token + "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("invalid cursor") from exc

    timestamp, separator, identifier = raw.rpartition("|")
    if not separator:
        raise ValueError("invalid cursor")

    try:
        row_id = int(identifier)
    except ValueError as exc:
        raise ValueError("invalid cursor") from exc

    published_at: datetime | None = None
    if timestamp != "-":
        try:
            published_at = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise ValueError("invalid cursor") from exc

    return Cursor(id=row_id, published_at=published_at)


async def upsert_item(conn: AsyncConnection[DictRow], item: ItemUpsert) -> tuple[int, bool]:
    """Store one collected item, returning ``(row_id, was_inserted)``.

    ``was_inserted`` is ``False`` when an existing row was updated, which is how the
    collection service counts new versus updated items.
    """
    async with conn.cursor() as cur:
        await cur.execute(_UPSERT, item.as_params())
        row = await cur.fetchone()

    if row is None:  # pragma: no cover - RETURNING always yields a row
        raise RuntimeError("upsert returned no row")

    return int(row["id"]), bool(row["inserted"])


async def get_item(conn: AsyncConnection[DictRow], item_id: int) -> ItemRow | None:
    """Return a single item by primary key."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM collected_items WHERE id = %(id)s",
            {"id": item_id},
        )
        row = await cur.fetchone()

    return ItemRow.from_row(row) if row else None


def _filter_conditions(
    *,
    source_id: int | None,
    since: datetime | None,
    until: datetime | None,
    query: str | None,
    alias: str = "",
) -> tuple[list[str], dict[str, Any]]:
    """The shared filter conditions, with their parameters.

    One place builds these four conditions so the list and the export cannot drift
    apart: an exported dataset must be the same set of rows the list serves, only
    without pagination. ``alias`` is the table alias to qualify the column names
    with (``"ci."`` for the export query, which joins sources and snapshots).
    """
    prefix = f"{alias}." if alias else ""
    conditions: list[str] = []
    params: dict[str, Any] = {}

    if source_id is not None:
        conditions.append(f"{prefix}source_id = %(source_id)s")
        params["source_id"] = source_id

    if since is not None:
        conditions.append(f"{prefix}published_at >= %(since)s")
        params["since"] = since

    if until is not None:
        conditions.append(f"{prefix}published_at <= %(until)s")
        params["until"] = until

    if query:
        # A plain case-insensitive title match. This is not a search feature.
        conditions.append(f"{prefix}title ILIKE %(query)s")
        params["query"] = f"%{query}%"

    return conditions, params


async def count_items(
    conn: AsyncConnection[DictRow],
    *,
    source_id: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    query: str | None = None,
) -> int:
    """Count every matching item for the same filters as ``list_items``.

    No cursor: the total is a property of the whole filtered set, independent of which
    page the user is on. Shares the filter builder so the count can never drift from the
    list.
    """
    conditions, params = _filter_conditions(
        source_id=source_id, since=since, until=until, query=query
    )
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT COUNT(*) FROM collected_items {where}"

    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        row = await cur.fetchone()
        return int(row["count"]) if row else 0


async def list_items(
    conn: AsyncConnection[DictRow],
    *,
    source_id: int | None = None,
    limit: int = 20,
    cursor: Cursor | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    query: str | None = None,
) -> list[ItemRow]:
    """Return a page of items, newest publication first.

    Keyset pagination is used instead of ``OFFSET`` because the worker inserts rows
    continuously; an offset page can skip or repeat rows while the underlying data changes.
    Items with no publication time sort last, so they remain reachable.
    """
    conditions, filter_params = _filter_conditions(
        source_id=source_id, since=since, until=until, query=query
    )
    params: dict[str, Any] = {"limit": limit, **filter_params}

    if cursor is not None:
        if cursor.published_at is None:
            # Continue within the trailing null-publication group.
            conditions.append("(published_at IS NULL AND id < %(cursor_id)s)")
            params["cursor_id"] = cursor.id
        else:
            conditions.append(
                """
                (
                    (published_at IS NOT NULL
                     AND (published_at, id) < (%(cursor_ts)s, %(cursor_id)s))
                    OR published_at IS NULL
                )
                """
            )
            params["cursor_ts"] = cursor.published_at
            params["cursor_id"] = cursor.id

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT {_COLUMNS}
        FROM collected_items
        {where}
        ORDER BY published_at DESC NULLS LAST, id DESC
        LIMIT %(limit)s
    """

    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        return [ItemRow.from_row(row) for row in await cur.fetchall()]


@dataclass(frozen=True, slots=True)
class ItemWithMetrics:
    """An item with its newest snapshot's metrics attached."""

    item: ItemRow
    metrics: Mapping[str, Any]


async def list_with_metrics(
    conn: AsyncConnection[DictRow],
    *,
    source_id: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    query: str | None = None,
) -> list[ItemWithMetrics]:
    """Return every matching item with its latest metrics, newest publication first.

    Unbounded by design: the viral filter (service layer) needs the whole per-day per-source
    population to compute percentiles, so pagination is applied after ranking, not here.
    """
    conditions, filter_params = _filter_conditions(
        source_id=source_id, since=since, until=until, query=query, alias="ci"
    )
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT {_COLUMNS}, snap.metrics AS metrics_latest
        FROM collected_items ci
        LEFT JOIN LATERAL (
            SELECT metrics
            FROM item_snapshots s
            WHERE s.item_id = ci.id
            ORDER BY s.captured_at DESC
            LIMIT 1
        ) snap ON true
        {where}
        ORDER BY ci.published_at DESC NULLS LAST, ci.id DESC
    """

    async with conn.cursor() as cur:
        await cur.execute(sql, filter_params)
        rows = await cur.fetchall()

    return [
        ItemWithMetrics(item=ItemRow.from_row(row), metrics=dict(row["metrics_latest"] or {}))
        for row in rows
    ]


# --------------------------------------------------------------------------- csv export


@dataclass(frozen=True, slots=True)
class ExportFilters:
    """The subset of the list filters the CSV export honors.

    Deliberately the same semantics as :func:`list_items` — an exported dataset must be
    explainable as "the same rows the list serves", minus pagination.
    """

    source_id: int | None = None
    since: datetime | None = None
    until: datetime | None = None
    query: str | None = None

    def _parts(self) -> tuple[str, dict[str, object]]:
        """The WHERE clause and parameters every export query shares."""
        conditions, params = _filter_conditions(
            source_id=self.source_id,
            since=self.since,
            until=self.until,
            query=self.query,
            alias="ci",
        )
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return where, params


#: One LATERAL join picks each item's newest snapshot in the same pass — no second
#: query, no N+1 (the same shape the items-list route achieves with DISTINCT ON).
#:
#: The ``LIMIT`` is the row ceiling applied by the database, not by the caller: an
#: unbounded export would have the server sort and join every matching row before the
#: client could stop reading it.
_EXPORT_SQL = """
    SELECT
        ci.id, ci.source_id, ci.external_id, ci.url, ci.title, ci.text, ci.author,
        ci.published_at, ci.metadata, ci.captured_at, ci.last_seen_at,
        ci.language, ci.content_type, ci.author_url, ci.thumbnail_url,
        s.key AS source_key,
        s.name AS source_name,
        s.source_type AS source_type,
        s.base_url AS source_url,
        s.integration AS collection_method,
        snap.metrics AS metrics_latest
    FROM collected_items ci
    JOIN sources s ON s.id = ci.source_id
    LEFT JOIN LATERAL (
        SELECT metrics FROM item_snapshots
        WHERE item_id = ci.id
        ORDER BY captured_at DESC
        LIMIT 1
    ) snap ON true
    {where}
    ORDER BY ci.published_at DESC NULLS LAST, ci.id DESC
    LIMIT %(max_rows)s
"""


async def count_export_rows(
    conn: AsyncConnection[DictRow], filters: ExportFilters
) -> int:
    """How many rows the export filters select — the truncation signal's numerator.

    The count is exact on purpose: the header says how many rows the filters matched, so
    an operator can tell "you downloaded all of them" from "there were 250 000 and you
    got 10 000". It is a plain count over the same WHERE clause — no snapshot join — so
    it costs one index scan, not the full export query. The ceiling applies to
    ``iterate_export_rows`` only; this count ignores it by design.
    """
    where, params = filters._parts()
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT count(*) AS count FROM collected_items ci {where}", params
        )
        row = await cur.fetchone()

    if row is None:  # pragma: no cover - count always yields a row
        return 0
    return int(row["count"])


async def iterate_export_rows(
    conn: AsyncConnection[DictRow],
    filters: ExportFilters,
    *,
    max_rows: int,
) -> AsyncIterator[Mapping[str, Any]]:
    """Stream export rows, newest publication first, through a server-side cursor.

    A named psycopg cursor keeps the transfer incremental on the database side too:
    rows arrive in chunks instead of the whole result landing in memory at once, and the
    ``max_rows`` ceiling bounds the query itself (``LIMIT`` in the SQL) as well as the
    number of rows handed to the caller.
    """
    where, params = filters._parts()
    sql = _EXPORT_SQL.format(where=where)
    params = {**params, "max_rows": max_rows}

    async with conn.cursor(name="items_export") as cur:
        await cur.execute(sql, params)
        while chunk := await cur.fetchmany(500):
            for row in chunk:
                yield row
