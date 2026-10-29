"""Deterministic CSV dataset exports for offline analysis and ML preparation.

These exports are the hand-off from the scraper — a data-collection system — to
downstream Data Science work done in pandas / Colab / Jupyter. PostgreSQL stays the
source of truth; these files are reproducible snapshots of it.

Nine datasets are emitted:

* ``items.csv``                    — canonical content records (parity with ``GET /export/items.csv``)
* ``item_metric_snapshots.csv``    — every engagement observation, long form (one metric per row)
* ``sources.csv``                  — source capabilities, observed metric keys, provenance
* ``data_quality_report.csv``      — automated validation findings (missing values, duplicates,
                                     timestamp anomalies, metric consistency, relationships)
* ``topics.csv``                   — topic registry (empty until a producer lands)
* ``item_topic_assignments.csv``   — item→topic links (empty until a producer lands)
* ``topic_observations.csv``       — per-window topic aggregates (empty until a producer lands)
* ``item_entities.csv``            — extracted entities (empty until a producer lands)
* ``training_labels.csv``          — explicit labels (empty until a producer lands)

The five topic/entity/label datasets are emitted header-only: no producer exists for them
yet, and this project never fabricates data. Their columns are the documented contract an
enrichment pass writes to.

Leakage guard: every engagement snapshot carries ``captured_at`` — when the metric was
observed. Any feature for time T may therefore use only snapshots with ``captured_at <= T``,
never a later snapshot. All timestamps are timezone-aware UTC.

SQL lives in ``db/repositories/dataset_export.py`` (the project's layering rule); this
module only serializes rows to CSV and computes report severities.
"""

# Dense SQL strings and CSV cells exceed the 100-column line budget; E501 is relaxed for
# this whole module rather than sprinkled per line.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from collections.abc import Callable, Coroutine, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories.dataset_export import (
    iter_entities_rows,
    iter_items_rows,
    iter_snapshot_rows,
    iter_source_rows,
    iter_topic_assignment_rows,
    iter_topic_observation_rows,
    iter_topic_rows,
    iter_training_label_rows,
    quality_counts,
)
from trendsentinel.services.ranking import primary_metric

__all__ = [
    "DATASET_WRITERS",
    "DEFAULT_OUT_DIR",
    "export_all",
    "main",
]

DEFAULT_OUT_DIR = "exports"

WriterFn = Callable[[Any, Any], Coroutine[Any, Any, None]]

#: Controlled vocabulary describing what each known metric means and how to read it. The
#: unit/definition are documentation, not observations: they explain a metric key, they do
#: not invent values. This is what makes ``score`` on Hacker News provably different from
#: ``score`` on Reddit — they are not directly comparable.
METRIC_SEMANTICS: dict[str, dict[str, dict[str, str]]] = {
    "hackernews": {
        "score": {"unit": "count", "definition": "points (upvotes) as reported by the HN API"},
        "comments": {"unit": "count", "definition": "comment count as reported by the HN API"},
    },
    "reddit": {
        "score": {"unit": "count", "definition": "net score (upvotes minus downvotes)"},
        "comments": {"unit": "count", "definition": "comment count"},
    },
    "x": {
        "likes": {"unit": "count", "definition": "favorite/like count"},
        "reposts": {"unit": "count", "definition": "repost/retweet count"},
        "replies": {"unit": "count", "definition": "reply count"},
        "views": {"unit": "count", "definition": "impression/view count"},
    },
    "threads": {
        "likes": {"unit": "count", "definition": "like count"},
        "reposts": {"unit": "count", "definition": "repost count"},
        "replies": {"unit": "count", "definition": "reply count"},
        "views": {"unit": "count", "definition": "view count"},
    },
    "wikimedia": {
        "views": {
            "unit": "count",
            "definition": (
                "pageviews measured in the observation's day window, as reported by "
                "the Wikimedia Pageviews API (all-access, user-agent)"
            ),
        },
    },
}

_FALLBACK_METRIC: dict[str, str] = {
    "unit": "count",
    "definition": "raw value as reported by the source",
}


def metric_semantics(collector_key: str, metric_name: str) -> dict[str, str]:
    """Return unit+definition for a metric, falling back to a documented default."""
    return METRIC_SEMANTICS.get(collector_key, {}).get(metric_name, _FALLBACK_METRIC)


def _iso_utc(value: datetime | None) -> str:
    """Render a stored instant as ISO-8601 UTC, or an empty cell when absent."""
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _json_cell(value: Any) -> str:
    """Serialize a jsonb value for one CSV cell; an absent value stays an empty cell."""
    if value is None:
        return ""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return ""


def _float_cell(value: Any) -> str:
    """Render a nullable float, stripping trailing zeros and blanks for ``None``."""
    if value is None:
        return ""
    try:
        return format(float(value), ".6g")
    except (TypeError, ValueError):
        return ""


def _interval_cell(value: Any) -> str:
    """Render an ``interval`` cell (psycopg gives a ``timedelta``)."""
    return "" if value is None else str(value)


def _collector_key(integration: str | None, config: Mapping[str, Any]) -> str:
    """Resolve the collector key for a source, defaulting to its integration."""
    key = config.get("collector_key")
    resolved = key if isinstance(key, str) and key else (integration or "")
    return resolved


# --------------------------------------------------------------------------- items

ITEMS_FIELDS = [
    "item_id",
    "source_key",
    "external_id",
    "url",
    "canonical_url",
    "content_fingerprint",
    "title",
    "text",
    "author",
    "published_at_utc",
    "captured_at_utc",
    "last_seen_at_utc",
    "metrics_latest_json",
    "metadata_json",
    "source_name",
    "source_type",
    "source_url",
    "collection_method",
    "language",
    "language_confidence",
    "content_format",
    "category",
    "subcategory",
    "content_status",
    "author_url",
    "thumbnail_url",
]


async def write_items_csv(conn: Any, fh: Any) -> None:
    """Write ``items.csv``: one row per item, newest publication first."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(ITEMS_FIELDS)
    async for row in iter_items_rows(conn):
        writer.writerow(
            [
                row["id"],
                row["source_key"],
                row["external_id"],
                row["url"] or "",
                row["canonical_url"] or "",
                row["content_fingerprint"] or "",
                row["title"] or "",
                row["text"] or "",
                row["author"] or "",
                _iso_utc(row["published_at"]),
                _iso_utc(row["captured_at"]),
                _iso_utc(row["last_seen_at"]),
                _json_cell(row["metrics_latest"]),
                _json_cell(row["metadata"]),
                row["source_name"] or "",
                row["source_type"] or "",
                row["source_url"] or "",
                row["collection_method"] or "",
                row["language"] or "",
                _float_cell(row["language_confidence"]),
                row["content_type"] or "",
                row["category"] or "",
                row["subcategory"] or "",
                row["content_status"] or "collected",
                row["author_url"] or "",
                row["thumbnail_url"] or "",
            ]
        )


# ----------------------------------------------------------- item_metric_snapshots

SNAPSHOTS_FIELDS = [
    "snapshot_id",
    "item_id",
    "source_key",
    "external_id",
    "captured_at_utc",
    "metric_name",
    "metric_value",
    "metric_unit",
    "metric_definition",
]


async def write_snapshots_csv(conn: Any, fh: Any) -> None:
    """Write ``item_metric_snapshots.csv``: one row per (item, snapshot, metric)."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(SNAPSHOTS_FIELDS)
    async for row in iter_snapshot_rows(conn):
        key = _collector_key(row.get("integration"), row.get("config") or {})
        semantics = metric_semantics(key, row["metric_name"])
        writer.writerow(
            [
                row["snapshot_id"],
                row["item_id"],
                row["source_key"],
                row["external_id"],
                _iso_utc(row["captured_at"]),
                row["metric_name"],
                row["metric_value"],
                semantics["unit"],
                semantics["definition"],
            ]
        )


# --------------------------------------------------------------------------- sources

SOURCES_FIELDS = [
    "source_id",
    "source_key",
    "source_name",
    "integration",
    "source_type",
    "base_url",
    "is_active",
    "collector_key",
    "ranking_metric",
    "keywords",
    "interval_minutes",
    "limit",
    "supported_content_types",
    "supported_metrics",
    "metric_semantics",
    "rate_limit_policy",
    "observed_metric_keys",
    "schema_version",
    "last_successful_collection_at_utc",
    "last_collection_status",
    "created_at_utc",
    "updated_at_utc",
]


def _config_str(config: Mapping[str, Any], name: str) -> str | None:
    """Read a string config value, tolerating absent or non-string values."""
    value = config.get(name)
    return value if isinstance(value, str) and value else None


def _config_int(config: Mapping[str, Any], name: str) -> int | None:
    """Read an integer config value, returning ``None`` when absent/malformed."""
    value = config.get(name)
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) and value > 0 else None


def _keywords(config: Mapping[str, Any]) -> str:
    """Normalize config keywords (list or comma string) to a comma-joined string."""
    raw = config.get("keywords")
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, list):
        parts = [str(part).strip() for part in raw if str(part).strip()]
    else:
        parts = []
    return ",".join(parts)


async def write_sources_csv(conn: Any, fh: Any) -> None:
    """Write ``sources.csv``: capabilities, provenance, and observed metric keys."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(SOURCES_FIELDS)
    async for row in iter_source_rows(conn):
        config = dict(row["config"] or {})
        collector_key = _collector_key(row["integration"], config)
        interval = _config_int(config, "interval_minutes")
        limit = _config_int(config, "limit")
        known_metrics = METRIC_SEMANTICS.get(collector_key, {})
        supported_metrics = ",".join(sorted(known_metrics))
        metric_semantics_cell = _json_cell(
            {name: sem["definition"] for name, sem in sorted(known_metrics.items())}
        )
        last_run_status = row.get("last_run_status")
        last_finished = row.get("last_run_finished_at")
        successful_at = (
            _iso_utc(last_finished)
            if last_finished and last_run_status == "succeeded"
            else ""
        )
        writer.writerow(
            [
                row["id"],
                row["key"],
                row["name"],
                row["integration"],
                row["source_type"] or "",
                row["base_url"] or "",
                "true" if row["is_active"] else "false",
                collector_key,
                primary_metric(collector_key, config) or "",
                _keywords(config),
                interval if interval is not None else "",
                limit if limit is not None else "",
                ",".join(row.get("supported_content_types") or []),
                supported_metrics,
                metric_semantics_cell,
                _json_cell(row.get("rate_limit_policy")),
                ",".join(row["observed_metric_keys"]),
                row.get("schema_version") if row.get("schema_version") is not None else "",
                successful_at,
                last_run_status or "",
                _iso_utc(row["created_at"]),
                _iso_utc(row["updated_at"]),
            ]
        )


# ----------------------------------------------------------------- data_quality_report

QUALITY_FIELDS = [
    "report_id",
    "report_date_utc",
    "dataset",
    "check",
    "severity",
    "status",
    "affected_rows",
    "total_rows",
    "value",
    "detail",
]


def _quality_row(
    stamp: str,
    dataset: str,
    check: str,
    affected: int,
    total: int,
    value: int | float,
    *,
    severity: str,
    status: str,
    detail: str = "",
) -> list[Any]:
    return [stamp, stamp, dataset, check, severity, status, affected, total, value, detail]


async def analyze_quality(conn: Any) -> list[list[Any]]:
    """Compose validation findings from the repository's counts, in a fixed order."""
    c = await quality_counts(conn)
    stamp = _iso_utc(datetime.now(UTC))
    rows: list[list[Any]] = []
    items_total = int(c["items_total"])
    snap_total = int(c["snap_total"])
    src_total = int(c["src_total"])

    rows.append(_quality_row(stamp, "items", "row_count", items_total, items_total, items_total, severity="info", status="ok"))
    rows.append(
        _quality_row(
            stamp, "items", "duplicate_external_id_count", int(c["items_dup"]), items_total,
            int(c["items_dup"]), severity="error" if c["items_dup"] else "info",
            status="fail" if c["items_dup"] else "ok",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "missing_title_count", int(c["items_no_title"]), items_total,
            int(c["items_no_title"]), severity="warning" if c["items_no_title"] else "info",
            status="warn" if c["items_no_title"] else "ok", detail="title is null; sources may omit it",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "missing_text_count", int(c["items_no_text"]), items_total,
            int(c["items_no_text"]), severity="info", status="ok",
            detail="expected for link posts: no body text",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "missing_published_at_count", int(c["items_no_pub"]), items_total,
            int(c["items_no_pub"]), severity="warning" if c["items_no_pub"] else "info",
            status="warn" if c["items_no_pub"] else "ok",
            detail="source did not report a publication time",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "null_url_count", int(c["items_null_url"]), items_total,
            int(c["items_null_url"]), severity="info", status="ok", detail="e.g. Ask HN posts",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "malformed_url_count", int(c["items_malformed_url"]), items_total,
            int(c["items_malformed_url"]), severity="error" if c["items_malformed_url"] else "info",
            status="fail" if c["items_malformed_url"] else "ok",
            detail="url present but does not start with a scheme",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "content_without_fingerprint_count", int(c["items_no_fingerprint"]),
            items_total, int(c["items_no_fingerprint"]), severity="warning" if c["items_no_fingerprint"] else "info",
            status="warn" if c["items_no_fingerprint"] else "ok",
            detail="has title/text but no content fingerprint",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "categorized_count", int(c["items_with_category"]), items_total,
            int(c["items_with_category"]), severity="info", status="ok",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "captured_before_published_count", int(c["items_early_cap"]), items_total,
            int(c["items_early_cap"]), severity="error" if c["items_early_cap"] else "info",
            status="fail" if c["items_early_cap"] else "ok",
            detail="captured_at < published_at: impossible ordering, data anomaly",
        )
    )
    rows.append(
        _quality_row(
            stamp, "items", "rows_with_metrics_count", int(c["items_with_metrics"]), items_total,
            int(c["items_with_metrics"]), severity="info", status="ok",
        )
    )

    rows.append(_quality_row(stamp, "snapshots", "row_count", snap_total, snap_total, snap_total, severity="info", status="ok"))
    rows.append(_quality_row(stamp, "snapshots", "items_with_snapshots_count", int(c["snap_items"]), snap_total, int(c["snap_items"]), severity="info", status="ok"))
    rows.append(_quality_row(stamp, "snapshots", "snapshots_per_item_min", 0, snap_total, c["snap_min"], severity="info", status="ok"))
    rows.append(_quality_row(stamp, "snapshots", "snapshots_per_item_max", 0, snap_total, c["snap_max"], severity="info", status="ok"))
    rows.append(_quality_row(stamp, "snapshots", "snapshots_per_item_avg", 0, snap_total, c["snap_avg"], severity="info", status="ok"))
    rows.append(
        _quality_row(
            stamp, "snapshots", "negative_metric_value_count", int(c["snap_negative"]), snap_total,
            int(c["snap_negative"]), severity="warning" if c["snap_negative"] else "info",
            status="warn" if c["snap_negative"] else "ok",
            detail="a negative engagement metric is anomalous; verify the source report",
        )
    )
    rows.append(
        _quality_row(
            stamp, "snapshots", "zero_metric_snapshot_count", int(c["snap_zero"]), snap_total,
            int(c["snap_zero"]), severity="info", status="ok",
            detail="all-zero snapshot; may indicate an unreported metric, not a fabrication",
        )
    )
    rows.append(
        _quality_row(
            stamp, "snapshots", "orphan_item_count", int(c["snap_orphan"]), snap_total,
            int(c["snap_orphan"]), severity="error" if c["snap_orphan"] else "info",
            status="fail" if c["snap_orphan"] else "ok",
            detail="snapshot points at a missing item (should be impossible under the FK)",
        )
    )

    rows.append(_quality_row(stamp, "sources", "row_count", src_total, src_total, src_total, severity="info", status="ok"))
    rows.append(_quality_row(stamp, "sources", "active_count", int(c["src_active"]), src_total, int(c["src_active"]), severity="info", status="ok"))
    rows.append(
        _quality_row(
            stamp, "sources", "unclassified_source_type_count", int(c["src_unclass"]), src_total,
            int(c["src_unclass"]), severity="info", status="ok", detail="source_type not yet classified",
        )
    )
    rows.append(
        _quality_row(
            stamp, "sources", "sources_with_failed_runs_count", int(c["src_failed_runs"]), src_total,
            int(c["src_failed_runs"]), severity="error" if c["src_failed_runs"] else "info",
            status="fail" if c["src_failed_runs"] else "ok",
            detail="sources whose most recent terminal run failed; check collection_runs.error_summary",
        )
    )
    rows.append(
        _quality_row(
            stamp, "sources", "sources_without_snapshots_count", int(c["src_no_snap"]), src_total,
            int(c["src_no_snap"]), severity="info", status="ok",
            detail="source has collected items but no engagement snapshots yet",
        )
    )

    rows.append(
        _quality_row(
            stamp, "relationships", "items_without_snapshots_count", int(c["rel_items_no_snap"]),
            items_total, int(c["rel_items_no_snap"]), severity="info", status="ok",
            detail="items never seen with a metric snapshot",
        )
    )

    topics_total = int(c["topics_total"])
    rows.append(_quality_row(stamp, "topics", "row_count", topics_total, topics_total, topics_total, severity="info", status="ok"))
    rows.append(_quality_row(stamp, "entities", "row_count", int(c["entity_total"]), int(c["entity_total"]), int(c["entity_total"]), severity="info", status="ok"))
    rows.append(_quality_row(stamp, "labels", "row_count", int(c["label_total"]), int(c["label_total"]), int(c["label_total"]), severity="info", status="ok"))

    return rows


async def write_quality_csv(conn: Any, fh: Any) -> None:
    """Write ``data_quality_report.csv``: one row per validation check."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(QUALITY_FIELDS)
    for row in await analyze_quality(conn):
        writer.writerow(row)


# --------------------------------------------------------------------------- topics

TOPICS_FIELDS = [
    "topic_id",
    "canonical_name",
    "description",
    "category",
    "aliases_json",
    "keywords_json",
    "first_observed_at_utc",
    "last_observed_at_utc",
    "identification_method",
    "version",
    "created_at_utc",
]


async def write_topics_csv(conn: Any, fh: Any) -> None:
    """Write ``topics.csv`` — a registry, currently empty until a producer lands."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(TOPICS_FIELDS)
    async for row in iter_topic_rows(conn):
        writer.writerow(
            [
                row["id"],
                row["canonical_name"],
                row["description"] or "",
                row["category"] or "",
                _json_cell(row["aliases"]),
                _json_cell(row["keywords"]),
                _iso_utc(row["first_observed_at"]),
                _iso_utc(row["last_observed_at"]),
                row["identification_method"] or "",
                row["version"] or "",
                _iso_utc(row["created_at"]),
            ]
        )


# ------------------------------------------------------- item_topic_assignments

ITEM_TOPIC_ASSIGNMENTS_FIELDS = [
    "assignment_id",
    "item_id",
    "topic_id",
    "assigned_at_utc",
    "method",
    "confidence",
    "rule_version",
]


async def write_item_topic_assignments_csv(conn: Any, fh: Any) -> None:
    """Write ``item_topic_assignments.csv`` — empty until a producer lands."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(ITEM_TOPIC_ASSIGNMENTS_FIELDS)
    async for row in iter_topic_assignment_rows(conn):
        writer.writerow(
            [
                row["id"],
                row["item_id"],
                row["topic_id"],
                _iso_utc(row["assigned_at"]),
                row["method"],
                _float_cell(row["confidence"]),
                row["rule_version"] or "",
            ]
        )


# ------------------------------------------------------- topic_observations

TOPIC_OBSERVATIONS_FIELDS = [
    "observation_id",
    "topic_id",
    "observation_window_start_utc",
    "observation_window_end_utc",
    "content_count",
    "unique_source_count",
    "engagement_aggregates_json",
    "activity_growth",
    "historical_baseline",
    "anomaly_indicator",
    "created_at_utc",
]


async def write_topic_observations_csv(conn: Any, fh: Any) -> None:
    """Write ``topic_observations.csv`` — empty until a producer lands."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(TOPIC_OBSERVATIONS_FIELDS)
    async for row in iter_topic_observation_rows(conn):
        anomaly = row.get("anomaly_indicator")
        writer.writerow(
            [
                row["id"],
                row["topic_id"],
                _iso_utc(row["observation_window_start"]),
                _iso_utc(row["observation_window_end"]),
                row["content_count"],
                row["unique_source_count"],
                _json_cell(row["engagement_aggregates"]),
                _float_cell(row["activity_growth"]),
                _float_cell(row["historical_baseline"]),
                "true" if anomaly else ("false" if anomaly is not None else ""),
                _iso_utc(row["created_at"]),
            ]
        )


# ----------------------------------------------------------------- item_entities

ITEM_ENTITIES_FIELDS = [
    "entity_id",
    "item_id",
    "entity_type",
    "canonical_name",
    "aliases_json",
    "extraction_method",
    "confidence",
    "extraction_version",
    "created_at_utc",
]


async def write_item_entities_csv(conn: Any, fh: Any) -> None:
    """Write ``item_entities.csv`` — empty until an extractor lands."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(ITEM_ENTITIES_FIELDS)
    async for row in iter_entities_rows(conn):
        writer.writerow(
            [
                row["id"],
                row["item_id"],
                row["entity_type"],
                row["canonical_name"],
                _json_cell(row["aliases"]),
                row["extraction_method"],
                _float_cell(row["confidence"]),
                row["extraction_version"] or "",
                _iso_utc(row["created_at"]),
            ]
        )


# ------------------------------------------------------------- training_labels

TRAINING_LABELS_FIELDS = [
    "label_id",
    "item_id",
    "target_value_json",
    "prediction_horizon",
    "label_definition",
    "label_version",
    "generation_method",
    "generated_at_utc",
]


async def write_training_labels_csv(conn: Any, fh: Any) -> None:
    """Write ``training_labels.csv`` — empty until a label generator lands."""
    writer = csv.writer(fh, lineterminator="\n")
    writer.writerow(TRAINING_LABELS_FIELDS)
    async for row in iter_training_label_rows(conn):
        writer.writerow(
            [
                row["id"],
                row["item_id"],
                _json_cell(row["target_value"]),
                _interval_cell(row["prediction_horizon"]),
                row["label_definition"],
                row["label_version"],
                row["generation_method"],
                _iso_utc(row["generated_at"]),
            ]
        )


# --------------------------------------------------------------------------- runner

DATASET_WRITERS: dict[str, WriterFn] = {
    "items": write_items_csv,
    "item_metric_snapshots": write_snapshots_csv,
    "sources": write_sources_csv,
    "data_quality_report": write_quality_csv,
    "topics": write_topics_csv,
    "item_topic_assignments": write_item_topic_assignments_csv,
    "topic_observations": write_topic_observations_csv,
    "item_entities": write_item_entities_csv,
    "training_labels": write_training_labels_csv,
}


async def export_all(conn: Any, out_dir: Path) -> list[Path]:
    """Write every dataset to ``out_dir`` and return the created file paths.

    ``out_dir`` must already exist; callers create it (the CLI does, synchronously,
    before entering the event loop) so the async path stays non-blocking.
    """
    paths: list[Path] = []
    for name, writer_fn in DATASET_WRITERS.items():
        path = out_dir / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            await writer_fn(conn, fh)
        paths.append(path)
    return paths


async def _export(out_dir: Path) -> list[Path]:
    settings = Settings()
    settings.assert_runtime_ready()
    database = Database(settings)
    await database.open()
    try:
        async with database.pool.connection() as conn:
            return await export_all(conn, out_dir)
    finally:
        await database.close()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="trendsentinel-dataset-export")
    parser.add_argument(
        "--out-dir",
        default=DEFAULT_OUT_DIR,
        help=f"Directory to write CSVs into (default: {DEFAULT_OUT_DIR!r})",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: ``python -m trendsentinel.dataset_export --out-dir out``."""
    args = _parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        paths = asyncio.run(_export(out_dir))
    except Exception as exc:
        print(f"error: {type(exc).__name__}: {exc}")
        return 2
    for path in paths:
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
