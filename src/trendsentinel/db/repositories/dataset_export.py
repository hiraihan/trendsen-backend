"""Native SQL backing the CSV dataset exports.

Per the project's layering rule (``tests/unit/test_layering.py``), SQL only lives here,
in ``db/repositories``. The export surface in ``dataset_export.py`` calls these and
serializes the rows to CSV.
"""

# Dense SQL strings exceed the 100-column line budget; E501 is relaxed for this module.
# ruff: noqa: E501

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow

__all__ = [
    "iter_entities_rows",
    "iter_items_rows",
    "iter_snapshot_rows",
    "iter_source_rows",
    "iter_topic_assignment_rows",
    "iter_topic_observation_rows",
    "iter_topic_rows",
    "iter_training_label_rows",
    "quality_counts",
]

Connection = AsyncConnection[DictRow]

_ITEMS_SQL = """
    SELECT
        ci.id, s.key AS source_key, ci.external_id,
        ci.url, ci.canonical_url, ci.content_fingerprint,
        ci.title, ci.text, ci.author,
        ci.published_at, ci.metadata, ci.captured_at, ci.last_seen_at,
        ci.language, ci.language_confidence, ci.content_type, ci.content_status,
        ci.category, ci.subcategory, ci.author_url, ci.thumbnail_url,
        s.name AS source_name, s.source_type, s.base_url AS source_url,
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
    ORDER BY ci.published_at DESC NULLS LAST, ci.id DESC
"""

_SNAPSHOTS_SQL = """
    SELECT
        si.id AS snapshot_id,
        ci.id AS item_id,
        s.key AS source_key,
        ci.external_id,
        si.captured_at AS captured_at,
        m.key AS metric_name,
        m.value::float8 AS metric_value,
        s.integration AS integration,
        s.config AS config
    FROM item_snapshots si
    JOIN collected_items ci ON ci.id = si.item_id
    JOIN sources s ON s.id = ci.source_id
    CROSS JOIN LATERAL jsonb_each(si.metrics) AS m(key, value)
    ORDER BY ci.id, si.captured_at, m.key
"""

_SOURCES_SQL = """
    SELECT
        s.id, s.key, s.name, s.integration, s.source_type, s.base_url,
        s.is_active, s.config, s.schema_version, s.supported_content_types,
        s.rate_limit_policy, s.created_at, s.updated_at,
        run.finished_at AS last_run_finished_at,
        run.status AS last_run_status,
        COALESCE(obs.keys, ARRAY[]::text[]) AS observed_metric_keys
    FROM sources s
    LEFT JOIN LATERAL (
        SELECT status, finished_at
        FROM collection_runs
        WHERE source_id = s.id AND status IN ('succeeded', 'partial', 'failed')
        ORDER BY started_at DESC
        LIMIT 1
    ) run ON true
    LEFT JOIN LATERAL (
        SELECT array_agg(DISTINCT m.key ORDER BY m.key) AS keys
        FROM collected_items ci
        JOIN item_snapshots si ON si.item_id = ci.id
        CROSS JOIN LATERAL jsonb_each(si.metrics) AS m(key, value)
        WHERE ci.source_id = s.id
    ) obs ON true
    ORDER BY s.key
"""

_TOPICS_SQL = """
    SELECT
        id, canonical_name, description, category, aliases, keywords,
        first_observed_at, last_observed_at, identification_method, version,
        created_at
    FROM topics
    ORDER BY canonical_name
"""

_TOPIC_ASSIGNMENTS_SQL = """
    SELECT
        id, item_id, topic_id, assigned_at, method, confidence, rule_version
    FROM item_topic_assignments
    ORDER BY item_id, topic_id, assigned_at
"""

_TOPIC_OBSERVATIONS_SQL = """
    SELECT
        id, topic_id, observation_window_start, observation_window_end,
        content_count, unique_source_count, engagement_aggregates,
        activity_growth, historical_baseline, anomaly_indicator, created_at
    FROM topic_observations
    ORDER BY topic_id, observation_window_end
"""

_ENTITIES_SQL = """
    SELECT
        id, item_id, entity_type, canonical_name, aliases,
        extraction_method, confidence, extraction_version, created_at
    FROM item_entities
    ORDER BY item_id, canonical_name
"""

_TRAINING_LABELS_SQL = """
    SELECT
        id, item_id, target_value, prediction_horizon,
        label_definition, label_version, generation_method, generated_at
    FROM training_labels
    ORDER BY item_id, id
"""


async def iter_rows(
    conn: Connection, sql: str, params: Mapping[str, Any] | None = None
) -> AsyncIterator[Mapping[str, Any]]:
    """Yield query rows one at a time through a server-side cursor."""
    async with conn.cursor() as cur:
        await cur.execute(sql, params)
        async for row in cur:
            yield row


async def iter_items_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per item, newest publication first."""
    async for row in iter_rows(conn, _ITEMS_SQL):
        yield row


async def iter_snapshot_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per (item, snapshot, metric), chronological."""
    async for row in iter_rows(conn, _SNAPSHOTS_SQL):
        yield row


async def iter_source_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per source, ordered by key."""
    async for row in iter_rows(conn, _SOURCES_SQL):
        yield row


async def iter_topic_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per topic."""
    async for row in iter_rows(conn, _TOPICS_SQL):
        yield row


async def iter_topic_assignment_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per topic assignment."""
    async for row in iter_rows(conn, _TOPIC_ASSIGNMENTS_SQL):
        yield row


async def iter_topic_observation_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per topic observation window."""
    async for row in iter_rows(conn, _TOPIC_OBSERVATIONS_SQL):
        yield row


async def iter_entities_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per extracted entity."""
    async for row in iter_rows(conn, _ENTITIES_SQL):
        yield row


async def iter_training_label_rows(conn: Connection) -> AsyncIterator[Mapping[str, Any]]:
    """Yield one mapping per training label."""
    async for row in iter_rows(conn, _TRAINING_LABELS_SQL):
        yield row


async def _scalar(conn: Connection, sql: str) -> int:
    """Run a scalar count/aggregate query and return its first value."""
    async with conn.cursor() as cur:
        await cur.execute(sql)
        row = await cur.fetchone()
    if row is None:
        return 0
    return int(next(iter(row.values())))


async def quality_counts(conn: Connection) -> dict[str, int | float]:
    """Return every data-quality count over the actual data, in one pass.

    Keys are stable and consumed by ``dataset_export.analyze_quality``. Keys ending in
    ``_total`` are the per-dataset denominators used for ``total_rows``; every other key is
    an affected-rows count used for ``affected_rows``.
    """
    counts: dict[str, int | float] = {}

    counts["items_total"] = await _scalar(conn, "SELECT count(*) AS n FROM collected_items")
    counts["items_dup"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n FROM (
            SELECT 1 FROM collected_items
            GROUP BY source_id, external_id
            HAVING count(*) > 1
        ) d
        """,
    )
    counts["items_no_title"] = await _scalar(conn, "SELECT count(*) AS n FROM collected_items WHERE title IS NULL")
    counts["items_no_text"] = await _scalar(conn, "SELECT count(*) AS n FROM collected_items WHERE text IS NULL")
    counts["items_no_pub"] = await _scalar(conn, "SELECT count(*) AS n FROM collected_items WHERE published_at IS NULL")
    counts["items_null_url"] = await _scalar(conn, "SELECT count(*) AS n FROM collected_items WHERE url IS NULL")
    counts["items_malformed_url"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM collected_items
        WHERE url IS NOT NULL AND url NOT ILIKE 'http%'
        """,
    )
    counts["items_no_fingerprint"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM collected_items
        WHERE content_fingerprint IS NULL AND (title IS NOT NULL OR text IS NOT NULL)
        """,
    )
    counts["items_with_category"] = await _scalar(
        conn, "SELECT count(*) AS n FROM collected_items WHERE category IS NOT NULL"
    )
    counts["items_early_cap"] = await _scalar(
        conn,
        "SELECT count(*) AS n FROM collected_items WHERE published_at IS NOT NULL AND captured_at < published_at",
    )
    counts["items_with_metrics"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n FROM collected_items ci
        WHERE EXISTS (SELECT 1 FROM item_snapshots si WHERE si.item_id = ci.id)
        """,
    )

    counts["snap_total"] = await _scalar(conn, "SELECT count(*) AS n FROM item_snapshots")
    counts["snap_items"] = await _scalar(conn, "SELECT count(DISTINCT item_id) AS n FROM item_snapshots")
    counts["snap_min"] = await _scalar(
        conn,
        """
        SELECT COALESCE(min(c), 0) AS n FROM (
            SELECT item_id, count(*) AS c FROM item_snapshots GROUP BY item_id
        ) g
        """,
    )
    counts["snap_max"] = await _scalar(
        conn,
        """
        SELECT COALESCE(max(c), 0) AS n FROM (
            SELECT item_id, count(*) AS c FROM item_snapshots GROUP BY item_id
        ) g
        """,
    )
    counts["snap_avg"] = await _scalar(
        conn,
        """
        SELECT COALESCE(round(avg(c)::numeric, 2), 0) AS n FROM (
            SELECT item_id, count(*) AS c FROM item_snapshots GROUP BY item_id
        ) g
        """,
    )
    counts["snap_negative"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM item_snapshots si
        CROSS JOIN LATERAL jsonb_each(si.metrics) AS m(key, value)
        WHERE m.value::float8 < 0
        """,
    )
    counts["snap_zero"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM (
            SELECT si.id
            FROM item_snapshots si
            CROSS JOIN LATERAL jsonb_each(si.metrics) AS m(key, value)
            GROUP BY si.id
            HAVING bool_and(m.value::float8 = 0)
        ) z
        """,
    )
    counts["snap_orphan"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM item_snapshots si
        LEFT JOIN collected_items ci ON ci.id = si.item_id
        WHERE ci.id IS NULL
        """,
    )

    counts["src_total"] = await _scalar(conn, "SELECT count(*) AS n FROM sources")
    counts["src_active"] = await _scalar(conn, "SELECT count(*) AS n FROM sources WHERE is_active")
    counts["src_failed_runs"] = await _scalar(
        conn,
        """
        SELECT count(DISTINCT source_id) AS n
        FROM collection_runs
        WHERE status = 'failed'
        """,
    )
    counts["src_unclass"] = await _scalar(conn, "SELECT count(*) AS n FROM sources WHERE source_type IS NULL")
    counts["src_no_snap"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM sources s
        WHERE NOT EXISTS (
            SELECT 1 FROM collected_items ci
            JOIN item_snapshots si ON si.item_id = ci.id
            WHERE ci.source_id = s.id
        )
        """,
    )

    counts["rel_items_no_snap"] = await _scalar(
        conn,
        """
        SELECT count(*) AS n
        FROM collected_items ci
        WHERE NOT EXISTS (SELECT 1 FROM item_snapshots si WHERE si.item_id = ci.id)
        """,
    )

    counts["topics_total"] = await _scalar(conn, "SELECT count(*) AS n FROM topics")
    counts["entity_total"] = await _scalar(conn, "SELECT count(*) AS n FROM item_entities")
    counts["label_total"] = await _scalar(conn, "SELECT count(*) AS n FROM training_labels")

    return counts
