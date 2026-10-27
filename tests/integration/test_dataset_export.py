"""Integration tests for the deterministic CSV dataset exports.

These run against a real PostgreSQL test database (same fixture as the other repository
tests) and assert the emitted datasets — header contract, row ordering, provenance arrows,
and that the quality report reflects actual anomalies.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import pytest

from trendsentinel.dataset_export import (
    ITEMS_FIELDS,
    SNAPSHOTS_FIELDS,
    SOURCES_FIELDS,
    analyze_quality,
    export_all,
    write_item_entities_csv,
    write_item_topic_assignments_csv,
    write_items_csv,
    write_snapshots_csv,
    write_sources_csv,
    write_topic_observations_csv,
    write_topics_csv,
    write_training_labels_csv,
)
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemUpsert

pytestmark = pytest.mark.integration

CAPTURED = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
PUBLISHED = datetime(2026, 10, 21, 13, 0, tzinfo=UTC)


def make_item(
    source_id: int,
    external_id: str,
    *,
    title: str | None = "Title",
    published_at: datetime | None = PUBLISHED,
    text: str | None = "body",
    captured_at: datetime = CAPTURED,
    fingerprint: str | None = None,
) -> ItemUpsert:
    return ItemUpsert(
        source_id=source_id,
        external_id=external_id,
        url=f"https://example.test/{external_id}",
        title=title,
        text=text,
        author="tester",
        published_at=published_at,
        metadata={},
        captured_at=captured_at,
        fingerprint=fingerprint,
    )


async def source_id_for(database: Database, key: str = "hackernews") -> int:
    async with database.pool.connection() as conn:
        source = await sources_repo.get_source_by_key(conn, key)
    assert source is not None, f"source {key!r} is not seeded"
    return source.id


async def test_items_csv_has_the_documented_header_and_newest_first(
    database: Database,
) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        await items_repo.upsert_item(
            conn, make_item(source_id, "a", published_at=PUBLISHED, fingerprint="fp-a")
        )
        await items_repo.upsert_item(
            conn,
            make_item(
                source_id,
                "b",
                published_at=datetime(2026, 10, 20, 9, 0, tzinfo=UTC),
                fingerprint="fp-b",
            ),
        )

        buf = io.StringIO()
        await write_items_csv(conn, buf)

    lines = buf.getvalue().splitlines()
    assert lines[0] == ",".join(ITEMS_FIELDS)
    header = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(header)
    assert len(rows) == 2
    # Newest publication first.
    assert rows[0]["external_id"] == "a"
    assert rows[1]["external_id"] == "b"
    assert rows[0]["source_key"] == "hackernews"
    assert rows[0]["collection_method"] == "api"
    # Universal content fields round-trip.
    assert rows[0]["content_fingerprint"] == "fp-a"
    assert rows[0]["content_status"] == "collected"
    assert rows[0]["canonical_url"] == ""
    assert rows[0]["language_confidence"] == ""
    assert rows[0]["content_format"] == ""


async def test_items_fingerprint_is_stable_across_recollection(database: Database) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_item(source_id, "a", fingerprint="fp"))
        # Re-collecting the same item updates rather than duplicates; the fingerprint stays.
        _, inserted = await items_repo.upsert_item(
            conn, make_item(source_id, "a", fingerprint="fp")
        )
        assert inserted is False
        buf = io.StringIO()
        await write_items_csv(conn, buf)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    assert len(rows) == 1
    assert rows[0]["content_fingerprint"] == "fp"


async def test_snapshots_csv_is_long_form_and_chronological(database: Database) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_item(source_id, "a"))
        await snapshots_repo.record_snapshot(
            conn,
            item_id,
            {"score": 10.0, "comments": 2.0},
            datetime(2026, 10, 21, 12, 0, tzinfo=UTC),
        )
        await snapshots_repo.record_snapshot(
            conn,
            item_id,
            {"score": 20.0},
            datetime(2026, 10, 21, 14, 0, tzinfo=UTC),
        )

        buf = io.StringIO()
        await write_snapshots_csv(conn, buf)

    lines = buf.getvalue().splitlines()
    assert lines[0] == ",".join(SNAPSHOTS_FIELDS)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    assert len(rows) == 3
    # Ordered by captured_at then metric name.
    assert [r["metric_name"] for r in rows] == ["comments", "score", "score"]
    assert rows[2]["metric_value"] == "20.0"
    # Snapshot id and metric semantics are present.
    assert rows[0]["snapshot_id"]
    assert rows[0]["metric_definition"]
    assert rows[0]["metric_unit"] == "count"


async def test_sources_csv_reports_capabilities_and_ranking(database: Database) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_item(source_id, "a"))
        await snapshots_repo.record_snapshot(conn, item_id, {"score": 5.0}, CAPTURED)

        buf = io.StringIO()
        await write_sources_csv(conn, buf)

    lines = buf.getvalue().splitlines()
    assert lines[0] == ",".join(SOURCES_FIELDS)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    hn = next(r for r in rows if r["source_key"] == "hackernews")
    assert hn["observed_metric_keys"] == "score"
    assert hn["ranking_metric"] == "score"
    assert hn["collector_key"] == "hackernews"
    assert hn["integration"] == "api"
    assert hn["supported_content_types"] == "link,self"
    assert hn["supported_metrics"] == "comments,score"
    assert hn["schema_version"] == "1"
    assert hn["last_collection_status"] == ""
    assert hn["metric_semantics"] != ""


async def test_quality_report_flags_a_captured_before_published_anomaly(
    database: Database,
) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_item(source_id, "anomaly"))
        rows = await analyze_quality(conn)

    assert rows, "quality report must contain at least one check row"
    anomaly = next(r for r in rows if r[2] == "items" and r[3] == "captured_before_published_count")
    assert int(anomaly[8]) >= 1
    assert anomaly[5] == "fail"
    # Total rows are reported alongside affected rows.
    assert int(anomaly[7]) >= int(anomaly[6])
    # No duplicate external ids possible under the unique constraint.
    dup = next(r for r in rows if r[2] == "items" and r[3] == "duplicate_external_id_count")
    assert int(dup[8]) == 0


async def test_topic_entity_label_datasets_emit_headers_while_empty(
    database: Database,
) -> None:
    async with database.pool.connection() as conn:
        bufs = {
            "topics": io.StringIO(),
            "assignments": io.StringIO(),
            "observations": io.StringIO(),
            "entities": io.StringIO(),
            "labels": io.StringIO(),
        }
        await write_topics_csv(conn, bufs["topics"])
        await write_item_topic_assignments_csv(conn, bufs["assignments"])
        await write_topic_observations_csv(conn, bufs["observations"])
        await write_item_entities_csv(conn, bufs["entities"])
        await write_training_labels_csv(conn, bufs["labels"])

    # Header-only: no producer fills these yet, and we never fabricate.
    for name, buf in bufs.items():
        lines = buf.getvalue().splitlines()
        assert len(lines) == 1, f"{name} must have no data rows yet"
        assert lines[0]


async def test_export_all_is_deterministic_across_runs(
    database: Database, tmp_path: Path
) -> None:
    source_id = await source_id_for(database)
    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_item(source_id, "a"))
        first_dir = tmp_path / "a"
        second_dir = tmp_path / "b"
        first_dir.mkdir(parents=True)
        second_dir.mkdir(parents=True)
        first_paths = await export_all(conn, first_dir)
        second_paths = await export_all(conn, second_dir)

    for first, second in zip(first_paths, second_paths, strict=True):
        if first.name == "data_quality_report.csv":
            # The report stamps its generation time (report_id and report_date), so it is
            # deterministic except for the first two columns.
            with first.open(newline="", encoding="utf-8") as fa, second.open(
                newline="", encoding="utf-8"
            ) as fb:
                a = list(csv.reader(fa))
                b = list(csv.reader(fb))
            assert [row[2:] for row in a] == [row[2:] for row in b]
        else:
            assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")
