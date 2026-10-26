"""Repository behaviour against a real PostgreSQL database.

These tests exist because the guarantees this project makes are *database* guarantees: a
unique constraint is what makes collection idempotent, and a partial unique index is what makes
a duplicate run impossible. Mocking them would test nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.rows import dict_row

from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemUpsert, RunStatus

pytestmark = pytest.mark.integration

CAPTURED = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


def make_upsert(
    source_id: int,
    external_id: str,
    *,
    title: str | None = None,
    text: str | None = None,
    metadata: dict[str, object] | None = None,
    published_at: datetime | None = None,
) -> ItemUpsert:
    """Build the database-facing item shape with sensible defaults."""
    return ItemUpsert(
        source_id=source_id,
        external_id=external_id,
        url=f"https://example.test/{external_id}",
        title=title,
        text=text,
        author="tester",
        published_at=published_at,
        metadata=metadata or {},
        captured_at=CAPTURED,
    )


async def source_id_for(database: Database, key: str = "hackernews") -> int:
    """Return the seeded source's id, failing loudly if the seed migration did not run."""
    async with database.pool.connection() as conn:
        source = await sources_repo.get_source_by_key(conn, key)

    assert source is not None, f"source {key!r} is not seeded"
    return source.id


async def count_items(database: Database) -> int:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT count(*) AS n FROM collected_items")
        row = await cur.fetchone()

    assert row is not None
    return int(row["n"])


# --------------------------------------------------------------------------- sources


async def test_seeded_sources_are_listed_and_filterable(database: Database) -> None:
    async with database.pool.connection() as conn:
        all_sources = await sources_repo.list_sources(conn)
        active = await sources_repo.list_sources(conn, active_only=True)

    keys = {source.key for source in all_sources}
    assert {"hackernews", "hnrss_frontpage", "github_blog"} <= keys
    assert [s.key for s in all_sources] == sorted(s.key for s in all_sources)

    # Active is a subset (not a copy): reddit_technology is seeded inactive until its
    # credentials are provisioned out of band, so it must not appear as active.
    active_keys = {source.key for source in active}
    assert {"hackernews", "hnrss_frontpage", "github_blog"} <= active_keys
    assert active_keys <= keys
    assert "reddit_technology" not in active_keys


async def test_get_source_by_id_and_by_key_agree(database: Database) -> None:
    async with database.pool.connection() as conn:
        by_key = await sources_repo.get_source_by_key(conn, "github_blog")
        assert by_key is not None
        by_id = await sources_repo.get_source_by_id(conn, by_key.id)

    assert by_id == by_key


async def test_get_source_by_key_returns_none_when_absent(database: Database) -> None:
    async with database.pool.connection() as conn:
        assert await sources_repo.get_source_by_key(conn, "no-such-source") is None


# --------------------------------------------------------------------------- upsert / dedup


async def test_upsert_twice_creates_one_row(database: Database) -> None:
    """The numeric form of the dedup acceptance criterion."""
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        first_id, first_inserted = await items_repo.upsert_item(
            conn, make_upsert(source_id, "1001", title="first")
        )
        second_id, second_inserted = await items_repo.upsert_item(
            conn, make_upsert(source_id, "1001", title="first")
        )

    assert first_inserted is True
    assert second_inserted is False
    assert first_id == second_id
    assert await count_items(database) == 1


async def test_a_later_thinner_payload_does_not_erase_richer_data(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(
            conn,
            make_upsert(
                source_id,
                "1002",
                title="Rich title",
                text="Full body",
                metadata={"a": 1},
                published_at=CAPTURED,
            ),
        )
        # RSS feeds routinely drop fields between fetches.
        await items_repo.upsert_item(
            conn, make_upsert(source_id, "1002", title=None, text=None, metadata={"b": 2})
        )
        stored = await items_repo.get_item(conn, item_id)

    assert stored is not None
    assert stored.title == "Rich title"
    assert stored.text == "Full body"
    assert stored.published_at == CAPTURED
    # metadata merges rather than replacing.
    assert stored.metadata == {"a": 1, "b": 2}


async def test_metadata_merge_keeps_both_sides(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_upsert(source_id, "1003", metadata={"score": 1}))
        await items_repo.upsert_item(conn, make_upsert(source_id, "1003", metadata={"comments": 2}))
        item_id, _ = await items_repo.upsert_item(
            conn, make_upsert(source_id, "1003", metadata={"score": 9})
        )
        stored = await items_repo.get_item(conn, item_id)

    assert stored is not None
    assert stored.metadata == {"score": 9, "comments": 2}


async def test_last_seen_at_advances_while_captured_at_does_not(database: Database) -> None:
    """Publication time is distinct from capture time; first capture is immutable."""
    source_id = await source_id_for(database)

    earlier = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
    later = datetime(2026, 10, 21, 11, 0, tzinfo=UTC)

    async with database.pool.connection() as conn:
        first_id, _ = await items_repo.upsert_item(
            conn, _with_capture(make_upsert(source_id, "1004"), earlier)
        )
        await items_repo.upsert_item(conn, _with_capture(make_upsert(source_id, "1004"), later))
        stored = await items_repo.get_item(conn, first_id)

    assert stored is not None
    assert stored.captured_at == earlier
    assert stored.last_seen_at == later


async def test_the_same_external_id_under_a_different_source_is_a_different_row(
    database: Database,
) -> None:
    """Identity is (source, external_id), not external_id alone."""
    hn = await source_id_for(database, "hackernews")
    github = await source_id_for(database, "github_blog")

    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_upsert(hn, "shared-id"))
        await items_repo.upsert_item(conn, make_upsert(github, "shared-id"))

    assert await count_items(database) == 2


# --------------------------------------------------------------------------- pagination


async def test_list_items_pages_without_skipping_or_repeating(database: Database) -> None:
    """Walk every page with the returned cursors and account for every row exactly once."""
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        for index in range(25):
            await items_repo.upsert_item(
                conn,
                make_upsert(
                    source_id,
                    f"page-{index}",
                    title=f"item {index}",
                    published_at=CAPTURED - timedelta(minutes=index),
                ),
            )

        seen: list[int] = []
        cursor = None
        while True:
            page = await items_repo.list_items(conn, limit=10, cursor=cursor)
            if not page:
                break
            seen.extend(item.id for item in page)
            cursor = items_repo.Cursor(id=page[-1].id, published_at=page[-1].published_at)

    assert len(seen) == 25
    assert len(set(seen)) == 25


async def test_list_items_includes_null_publication_items_at_the_end(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        await items_repo.upsert_item(conn, make_upsert(source_id, "dated", published_at=CAPTURED))
        await items_repo.upsert_item(conn, make_upsert(source_id, "undated"))

        page = await items_repo.list_items(conn, limit=10)

    assert [item.external_id for item in page] == ["dated", "undated"]


async def test_list_items_filters_by_source_since_until_and_query(database: Database) -> None:
    hn = await source_id_for(database, "hackernews")
    github = await source_id_for(database, "github_blog")

    async with database.pool.connection() as conn:
        await items_repo.upsert_item(
            conn,
            make_upsert(hn, "f1", title="Alpha release", published_at=CAPTURED),
        )
        await items_repo.upsert_item(
            conn,
            make_upsert(hn, "f2", title="Beta release", published_at=CAPTURED + timedelta(days=2)),
        )
        await items_repo.upsert_item(conn, make_upsert(github, "f3", title="Alpha elsewhere"))

        by_source = await items_repo.list_items(conn, source_id=github)
        by_query = await items_repo.list_items(conn, query="alpha")
        by_since = await items_repo.list_items(conn, since=CAPTURED + timedelta(days=1))
        by_until = await items_repo.list_items(conn, until=CAPTURED + timedelta(days=1))

    assert [item.external_id for item in by_source] == ["f3"]
    assert sorted(item.external_id for item in by_query) == ["f1", "f3"]
    assert [item.external_id for item in by_since] == ["f2"]
    assert [item.external_id for item in by_until] == ["f1"]


@pytest.mark.parametrize(
    "published_at",
    [datetime(2026, 10, 21, 12, 0, tzinfo=UTC), None],
)
def test_cursor_round_trips(published_at: datetime | None) -> None:
    original = items_repo.Cursor(id=42, published_at=published_at)

    decoded = items_repo.decode_cursor(items_repo.encode_cursor(original))

    assert decoded.id == 42
    assert decoded.published_at == published_at


@pytest.mark.parametrize("token", ["", "not-base64!!", "aGVsbG8", "MQ"])
def test_malformed_cursor_is_rejected(token: str) -> None:
    with pytest.raises(ValueError, match="invalid cursor"):
        items_repo.decode_cursor(token)


async def test_get_item_returns_none_for_a_missing_row(database: Database) -> None:
    async with database.pool.connection() as conn:
        assert await items_repo.get_item(conn, 999_999) is None


# --------------------------------------------------------------------------- snapshots


async def test_snapshots_are_written_and_deduplicated(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_upsert(source_id, "s1"))

        first = await snapshots_repo.record_snapshot(conn, item_id, {"score": 10.0}, CAPTURED)
        again = await snapshots_repo.record_snapshot(conn, item_id, {"score": 11.0}, CAPTURED)
        later = await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 12.0}, CAPTURED + timedelta(hours=1)
        )

        stored = await snapshots_repo.list_snapshots(conn, item_id)

    assert first is True
    assert again is False, "the same (item, captured_at) must not create a second row"
    assert later is True
    assert len(stored) == 2
    # Newest first; the first snapshot's value is untouched by the duplicate insert.
    assert stored[0].metrics == {"score": 12.0}
    assert stored[1].metrics == {"score": 10.0}


async def test_empty_metrics_produce_no_snapshot(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_upsert(source_id, "s2"))
        written = await snapshots_repo.record_snapshot(conn, item_id, {}, CAPTURED)
        stored = await snapshots_repo.list_snapshots(conn, item_id)

    assert written is False
    assert stored == []


async def test_latest_snapshot_returns_the_newest_row_or_none(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_upsert(source_id, "s4"))

        before_any = await snapshots_repo.latest_snapshot(conn, item_id)

        await snapshots_repo.record_snapshot(conn, item_id, {"score": 42.0}, CAPTURED)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 7.0}, CAPTURED + timedelta(hours=1)
        )
        latest = await snapshots_repo.latest_snapshot(conn, item_id)

        second_item_id, _ = await items_repo.upsert_item(conn, make_upsert(source_id, "s5"))
        item_without_snapshots = await snapshots_repo.latest_snapshot(conn, second_item_id)

    assert before_any is None
    assert latest is not None
    assert latest.metrics == {"score": 7.0}
    assert latest.item_id == item_id
    assert item_without_snapshots is None


async def test_snapshot_retention_purge_removes_only_old_rows(database: Database) -> None:
    source_id = await source_id_for(database)
    now = datetime.now(UTC)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, make_upsert(source_id, "s3"))
        await snapshots_repo.record_snapshot(conn, item_id, {"score": 1.0}, now)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 2.0}, now - timedelta(days=400)
        )

        removed = await snapshots_repo.delete_snapshots_older_than(conn, 90)
        remaining = await snapshots_repo.list_snapshots(conn, item_id)

    assert removed == 1
    assert len(remaining) == 1


# --------------------------------------------------------------------------- run guards


async def test_a_second_active_run_for_one_source_is_rejected(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        first = await runs_repo.create_run(conn, source_id, "scheduler")

        with pytest.raises(runs_repo.RunInProgressError) as excinfo:
            await runs_repo.create_run(conn, source_id, "manual")

    assert excinfo.value.source_id == source_id
    assert first.status == RunStatus.RUNNING


async def test_a_pending_run_also_blocks_a_duplicate_trigger(database: Database) -> None:
    """A queued manual run must not be duplicated before the worker picks it up."""
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        await runs_repo.create_run(conn, source_id, "manual", status=RunStatus.PENDING)

        with pytest.raises(runs_repo.RunInProgressError):
            await runs_repo.create_run(conn, source_id, "manual", status=RunStatus.PENDING)


async def test_a_finished_run_frees_the_source(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        run = await runs_repo.create_run(conn, source_id, "scheduler")
        await runs_repo.finish_run(conn, run.id, status=RunStatus.SUCCEEDED, items_new=3)

        second = await runs_repo.create_run(conn, source_id, "scheduler")

    assert second.id != run.id


async def test_different_sources_run_concurrently(database: Database) -> None:
    hn = await source_id_for(database, "hackernews")
    github = await source_id_for(database, "github_blog")

    async with database.pool.connection() as conn:
        await runs_repo.create_run(conn, hn, "scheduler")
        await runs_repo.create_run(conn, github, "scheduler")

        active = await runs_repo.get_active_runs(conn)

    assert {run.source_id for run in active} == {hn, github}


async def test_finish_run_rejects_a_non_terminal_status(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        run = await runs_repo.create_run(conn, source_id, "scheduler")

        with pytest.raises(ValueError, match="not a terminal run status"):
            await runs_repo.finish_run(conn, run.id, status=RunStatus.RUNNING)


async def test_finish_run_records_counts_and_error_summary(database: Database) -> None:
    source_id = await source_id_for(database)
    summary = {"errors": [{"kind": "timeout", "message": "slow"}], "counts": {"fetched": 5}}

    async with database.pool.connection() as conn:
        run = await runs_repo.create_run(conn, source_id, "scheduler")
        await runs_repo.finish_run(
            conn,
            run.id,
            status=RunStatus.PARTIAL,
            items_fetched=5,
            items_new=2,
            items_updated=1,
            error_summary=summary,
        )
        last = await runs_repo.get_last_runs_per_source(conn)

    assert len(last) == 1
    assert last[0].status == RunStatus.PARTIAL
    assert last[0].items_new == 2
    assert last[0].items_updated == 1
    assert last[0].error_summary == summary
    assert last[0].finished_at is not None


async def test_claim_pending_run_is_exclusive(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        pending = await runs_repo.create_run(conn, source_id, "manual", status=RunStatus.PENDING)

        first = await runs_repo.claim_pending_run(conn, pending.id)
        second = await runs_repo.claim_pending_run(conn, pending.id)

    assert first is not None
    assert first.status == RunStatus.RUNNING
    assert second is None, "only one worker may claim a pending run"


async def test_get_pending_runs_lists_only_pending(database: Database) -> None:
    source_id = await source_id_for(database, "hnrss_frontpage")
    other_id = await source_id_for(database, "github_blog")

    async with database.pool.connection() as conn:
        # A different source, so the partial unique index is not involved.
        await runs_repo.create_run(conn, other_id, "scheduler")
        pending = await runs_repo.create_run(conn, source_id, "manual", status=RunStatus.PENDING)

        listed = await runs_repo.get_pending_runs(conn)

    assert [run.id for run in listed] == [pending.id]


async def test_item_limit_is_stored_on_the_run(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        run = await runs_repo.create_run(conn, source_id, "manual", item_limit=7)
        stored = await runs_repo.get_last_runs_per_source(conn)

    assert run.item_limit == 7
    assert stored[0].item_limit == 7


# --------------------------------------------------------------------------- reaping


async def test_stale_runs_are_reaped_and_the_source_becomes_collectable(database: Database) -> None:
    """Without this, a worker killed mid-run blocks its source forever."""
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        stale = await runs_repo.create_run(conn, source_id, "scheduler")
        await _age_run(conn, stale.id, minutes=120)

        reaped = await runs_repo.reap_stale_runs(conn, older_than_minutes=30)
        replacement = await runs_repo.create_run(conn, source_id, "scheduler")
        last = await runs_repo.get_last_runs_per_source(conn)

    assert reaped == 1
    assert replacement.id != stale.id
    assert {run.source_id: run.status for run in last} == {source_id: RunStatus.RUNNING}

    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT status, error_summary FROM collection_runs WHERE id = %(id)s",
            {"id": stale.id},
        )
        row = await cur.fetchone()

    assert row is not None
    assert row["status"] == RunStatus.FAILED
    assert row["error_summary"]["reason"] == "reaped as stale"


async def test_reaping_leaves_recent_runs_alone(database: Database) -> None:
    source_id = await source_id_for(database)

    async with database.pool.connection() as conn:
        await runs_repo.create_run(conn, source_id, "scheduler")
        reaped = await runs_repo.reap_stale_runs(conn, older_than_minutes=30)
        active = await runs_repo.get_active_runs(conn)

    assert reaped == 0
    assert len(active) == 1


# --------------------------------------------------------------------------- advisory lock


async def test_advisory_lock_is_exclusive_per_source(database: Database) -> None:
    """Two concurrent sessions must not both hold the same source's lock.

    The second session is opened directly rather than taken from the pool: this is the only
    test holding two connections at once, and it should not depend on the pool's size.
    """
    async with (
        database.pool.connection() as first,
        await psycopg.AsyncConnection.connect(
            str(database.pool.conninfo), row_factory=dict_row
        ) as second,
    ):
        assert await runs_repo.try_advisory_lock(first, 1234) is True
        assert await runs_repo.try_advisory_lock(second, 1234) is False
        # A different source is unaffected.
        assert await runs_repo.try_advisory_lock(second, 5678) is True

        await runs_repo.release_advisory_lock(first, 1234)
        assert await runs_repo.try_advisory_lock(second, 1234) is True

        await runs_repo.release_advisory_lock(second, 5678)


# --------------------------------------------------------------------------- health


async def test_check_database_reports_a_live_connection(database: Database) -> None:
    from trendsentinel.db.repositories.health import check_database

    async with database.pool.connection() as conn:
        assert await check_database(conn) is True


# --------------------------------------------------------------------------- helpers


def _with_capture(item: ItemUpsert, captured_at: datetime) -> ItemUpsert:
    """Return a copy of an upsert with a different capture time."""
    return ItemUpsert(
        source_id=item.source_id,
        external_id=item.external_id,
        url=item.url,
        title=item.title,
        text=item.text,
        author=item.author,
        published_at=item.published_at,
        metadata=item.metadata,
        captured_at=captured_at,
    )


async def _age_run(conn: object, run_id: int, *, minutes: int) -> None:
    """Backdate a run so the reaper considers it abandoned."""
    from psycopg import AsyncConnection

    assert isinstance(conn, AsyncConnection)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE collection_runs
            SET started_at = now() - make_interval(mins => %(minutes)s)
            WHERE id = %(id)s
            """,
            {"minutes": minutes, "id": run_id},
        )
