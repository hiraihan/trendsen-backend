"""Collection service behaviour against a real database, with HTTP mocked.

The two requirements worth the most here are the numeric dedup proof ("run twice, row count
unchanged") and failure isolation ("one bad source cannot stop the others"). Both are asserted
directly rather than inferred.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    ErrorKind,
    SourceRef,
)
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.registry import get_collector
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import RunStatus, SourceRow
from trendsentinel.services.collection import SKIPPED, CollectionService, RunResult

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "hackernews"
HN_BASE = "https://hacker-news.firebaseio.com/v0"


def public_resolver(_hostname: str) -> list[str]:
    """Deterministic DNS so the SSRF guard never touches the network in tests."""
    return ["93.184.216.34"]


def make_settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


@pytest.fixture
def service_settings(migrated_test_db: str) -> Settings:
    """Settings bound to the migrated test database, with fast retries."""
    return make_settings(database_url=migrated_test_db, http_max_retries=1)


@pytest.fixture
async def service(database: Database, service_settings: Settings) -> CollectionService:
    """A service whose fetcher is stubbed, so no test can reach the network."""
    return CollectionService(
        service_settings,
        database,
        fetcher_factory=lambda settings: HttpFetcher(
            settings,
            policy=RetryPolicy(max_attempts=1, base_delay=0.0),
            resolver=public_resolver,
        ),
    )


async def get_source(database: Database, key: str = "hackernews") -> SourceRow:
    async with database.pool.connection() as conn:
        source = await sources_repo.get_source_by_key(conn, key)

    assert source is not None, f"source {key!r} is not seeded"
    return source


async def item_count(database: Database) -> int:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT count(*) AS n FROM collected_items")
        row = await cur.fetchone()

    assert row is not None
    return int(row["n"])


async def run_row(database: Database, run_id: int) -> Mapping[str, object]:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT status, items_fetched, items_new, items_updated, error_summary, finished_at
            FROM collection_runs WHERE id = %(id)s
            """,
            {"id": run_id},
        )
        row = await cur.fetchone()

    assert row is not None
    return dict(row)


class ScriptedCollector:
    """A collector whose results are decided by the test.

    It records the ``SourceRef`` it was given and the limit it was asked for, so the service's
    clamping and adaptation can be asserted without a network round trip.
    """

    key = "scripted"

    def __init__(self, results: Sequence[CollectionResult | Exception]) -> None:
        self._results = list(results)
        self.seen: list[tuple[SourceRef, int]] = []

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        self.seen.append((source, limit))
        outcome = self._results.pop(0) if self._results else CollectionResult()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def scripted_factory(collector: ScriptedCollector) -> object:
    """A factory returning one prepared collector for every call."""

    def factory(
        settings: Settings,
        fetcher: HttpFetcher,
        integration: str,
        config: Mapping[str, object] | None = None,
    ) -> ScriptedCollector:
        return collector

    return factory


def scripted_service(
    database: Database, settings: Settings, collector: ScriptedCollector
) -> CollectionService:
    """A service bound to a scripted collector, with a stubbed fetcher."""
    return CollectionService(
        settings,
        database,
        collector_factory=scripted_factory(collector),  # type: ignore[arg-type]
        fetcher_factory=lambda s: HttpFetcher(
            s, policy=RetryPolicy(max_attempts=1, base_delay=0.0), resolver=public_resolver
        ),
    )


# --------------------------------------------------------------------------- dedup


@respx.mock
async def test_running_the_same_collection_twice_creates_no_duplicate_rows(
    database: Database, service: CollectionService
) -> None:
    """The acceptance criterion, asserted numerically: rows are stable across two runs."""
    topstories = (FIXTURES / "topstories.json").read_bytes()
    # The real topstories fixture is trusted here; the first three ids are served with detail.
    import json

    ids = json.loads(topstories)[:3]
    respx.get(f"{HN_BASE}/topstories.json").mock(
        return_value=httpx.Response(200, content=b"[%s]" % b", ".join(str(i).encode() for i in ids))
    )
    for item_id in ids:
        payload = {
            "by": "tester",
            "id": item_id,
            "score": 10,
            "descendants": 3,
            "time": 1_767_225_600,
            "title": f"Story {item_id}",
            "type": "story",
            "url": f"https://example.test/{item_id}",
        }
        respx.get(f"{HN_BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json=payload)
        )

    source = await get_source(database)
    first = await service.collect_source(source, "scheduler")
    after_first = await item_count(database)
    second = await service.collect_source(source, "scheduler")
    after_second = await item_count(database)

    assert first.status == RunStatus.SUCCEEDED
    assert first.items_new == 3
    assert first.items_updated == 0

    assert second.status == RunStatus.SUCCEEDED
    assert second.items_new == 0, "a re-run must insert nothing"
    assert second.items_updated == 3, "a re-run must update the existing rows"
    assert after_second == after_first, "row count must be stable across runs"


@respx.mock
async def test_metrics_are_recorded_as_snapshots_not_items(
    database: Database, service: CollectionService
) -> None:
    respx.get(f"{HN_BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1]))
    respx.get(f"{HN_BASE}/item/1.json").mock(
        return_value=httpx.Response(
            200,
            json={
                "by": "tester",
                "id": 1,
                "score": 42,
                "descendants": 7,
                "time": 1_767_225_600,
                "title": "Measured",
                "type": "story",
            },
        )
    )

    source = await get_source(database)
    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED

    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT metrics FROM item_snapshots")
        rows = await cur.fetchall()

    assert [row["metrics"] for row in rows] == [{"score": 42.0, "comments": 7.0}]


# --------------------------------------------------------------------------- status semantics


async def test_no_errors_means_succeeded(database: Database, service_settings: Settings) -> None:
    collector = ScriptedCollector([CollectionResult(items=[CollectedItem(external_id="ok-1")])])
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED
    assert result.items_new == 1
    assert result.error_summary is None


async def test_errors_with_items_means_partial(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [
            CollectionResult(
                items=[CollectedItem(external_id="p-1")],
                errors=[CollectError(ErrorKind.TIMEOUT, "one item timed out")],
            )
        ]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.PARTIAL
    assert result.items_new == 1
    assert result.error_summary is not None
    assert result.error_summary["errors"][0]["kind"] == "timeout"
    assert result.error_summary["counts"]["new"] == 1


async def test_errors_without_items_means_failed(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [CollectionResult(errors=[CollectError(ErrorKind.HTTP_STATUS, "HTTP 500", retryable=True)])]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.FAILED
    assert result.persisted == 0
    assert result.error_summary is not None
    assert result.error_summary["errors"][0]["kind"] == "http_status"


async def test_a_run_with_no_items_and_no_errors_still_succeeds(
    database: Database, service_settings: Settings
) -> None:
    """A quiet feed is not a failure: nothing to store, nothing went wrong."""
    collector = ScriptedCollector([CollectionResult()])
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED
    assert result.persisted == 0


# --------------------------------------------------------------------------- validation gate


async def test_item_with_an_empty_external_id_is_rejected_and_not_persisted(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [
            CollectionResult(
                items=[
                    CollectedItem(external_id="good-1"),
                    CollectedItem(external_id="   "),
                ]
            )
        ]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.PARTIAL, "some items stored, some rejected"
    assert result.items_fetched == 2
    assert result.persisted == 1, "the invalid item must not be stored"
    assert await item_count(database) == 1
    assert result.error_summary is not None
    kinds = [error["kind"] for error in result.error_summary["errors"]]
    assert kinds == [ErrorKind.INVALID_PAYLOAD]
    assert result.error_summary["counts"]["rejected"] == 1


async def test_non_numeric_metrics_are_rejected(
    database: Database, service_settings: Settings
) -> None:
    """A string where a number belongs would corrupt every later snapshot comparison.

    Note that a naive ``published_at`` is *normalized* to UTC rather than rejected (collectors
    deliberately stamp unlabelled feed dates as UTC), so the validation gate is exercised here
    with a defect normalization cannot repair.
    """
    collector = ScriptedCollector(
        [
            CollectionResult(
                items=[CollectedItem(external_id="bad-metric", metrics={"score": "many"})]  # type: ignore[dict-item]
            )
        ]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.FAILED
    assert await item_count(database) == 0
    assert result.error_summary is not None
    assert result.error_summary["counts"]["rejected"] == 1


async def test_a_naive_publication_time_is_normalized_to_utc(
    database: Database, service_settings: Settings
) -> None:
    """Publication and capture times must both be timezone-aware; a naive one is taken as UTC."""
    collector = ScriptedCollector(
        [
            CollectionResult(
                items=[CollectedItem(external_id="naive-1", published_at=datetime(2026, 1, 1))]
            )
        ]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED

    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT published_at FROM collected_items")
        row = await cur.fetchone()

    assert row is not None
    assert row["published_at"] == datetime(2026, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------- failure isolation


async def test_one_failing_source_does_not_stop_the_others(
    database: Database, service_settings: Settings
) -> None:
    """The requirement stated in the spec: a crash in one source must not abort the batch."""
    collector = ScriptedCollector(
        [
            CollectionResult(items=[CollectedItem(external_id="first-ok")]),
            RuntimeError("this collector blew up"),
            CollectionResult(items=[CollectedItem(external_id="third-ok")]),
        ]
    )
    service = scripted_service(database, service_settings, collector)

    sources = [
        await get_source(database, "hackernews"),
        await get_source(database, "hnrss_frontpage"),
        await get_source(database, "github_blog"),
    ]

    results = await service.collect_many(sources, "scheduler")

    assert [result.source_key for result in results] == [s.key for s in sources]
    assert results[0].status == RunStatus.SUCCEEDED
    assert results[1].status == RunStatus.FAILED, "the crashing source is recorded as failed"
    assert results[2].status == RunStatus.SUCCEEDED, "the batch continued"

    assert results[1].error_summary is not None
    assert "RuntimeError" in results[1].error_summary["errors"][0]["message"]

    # Both healthy sources stored their item.
    assert await item_count(database) == 2

    # The failed source's run row was closed, not left `running`.
    crashed = await run_row(database, results[1].run_id or 0)
    assert crashed["status"] == RunStatus.FAILED
    assert crashed["finished_at"] is not None


async def test_collect_many_is_sequential(database: Database, service_settings: Settings) -> None:
    """Order is preserved, which is what makes a per-source failure readable in the logs."""
    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)

    sources = [
        await get_source(database, "hackernews"),
        await get_source(database, "github_blog"),
    ]

    results = await service.collect_many(sources, "scheduler")

    assert [result.source_key for result in results] == ["hackernews", "github_blog"]


async def test_a_source_that_already_has_a_run_is_skipped(
    database: Database, service_settings: Settings
) -> None:
    """A concurrent trigger is normal operation, so it is a skip, not an exception."""
    collector = ScriptedCollector([CollectionResult(items=[CollectedItem(external_id="s1")])])
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    async with database.pool.connection() as conn:
        await runs_repo.create_run(conn, source.id, "scheduler")

    result = await service.collect_source(source, "manual")

    assert result.skipped is True
    assert result.status == SKIPPED
    assert result.run_id is None
    assert result.skipped_reason is not None
    assert collector.seen == [], "the skipped source must not have been collected"


# --------------------------------------------------------------------------- pending runs


async def test_a_pending_run_is_claimed_and_executed_once(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [CollectionResult(items=[CollectedItem(external_id="pending-1")])]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    async with database.pool.connection() as conn:
        pending = await runs_repo.create_run(
            conn, source.id, "manual", status=RunStatus.PENDING, item_limit=3
        )

    result = await service.collect_pending(pending)

    assert result.status == RunStatus.SUCCEEDED
    assert result.items_new == 1
    assert result.run_id == pending.id

    # The stored limit reached the collector.
    assert collector.seen[0][1] == 3

    stored = await run_row(database, pending.id)
    assert stored["status"] == RunStatus.SUCCEEDED
    assert stored["finished_at"] is not None


async def test_claiming_a_pending_run_twice_skips_the_second_attempt(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [CollectionResult(items=[CollectedItem(external_id="claim-1")]), CollectionResult()]
    )
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    async with database.pool.connection() as conn:
        pending = await runs_repo.create_run(conn, source.id, "manual", status=RunStatus.PENDING)

    first = await service.collect_pending(pending)
    second = await service.collect_pending(pending)

    assert first.status == RunStatus.SUCCEEDED
    assert second.skipped is True
    assert second.status == SKIPPED
    assert len(collector.seen) == 1, "the run must be executed exactly once"
    assert await item_count(database) == 1


# --------------------------------------------------------------------------- limits


async def test_the_explicit_limit_is_clamped_to_the_global_cap(
    database: Database, migrated_test_db: str
) -> None:
    settings = make_settings(database_url=migrated_test_db, max_items_per_run=5)
    collector = ScriptedCollector([CollectionResult()])
    service = scripted_service(database, settings, collector)
    source = await get_source(database)

    await service.collect_source(source, "manual", item_limit=500)

    assert collector.seen[0][1] == 5


async def test_the_source_config_limit_is_used_when_no_explicit_limit_is_given(
    database: Database, migrated_test_db: str
) -> None:
    """The seeded hackernews source configures ``limit: 50``."""
    settings = make_settings(database_url=migrated_test_db, max_items_per_run=100)
    collector = ScriptedCollector([CollectionResult()])
    service = scripted_service(database, settings, collector)
    source = await get_source(database)

    await service.collect_source(source, "scheduler")

    assert collector.seen[0][1] == source.limit


async def test_the_global_cap_applies_when_nothing_is_configured(
    database: Database, migrated_test_db: str
) -> None:
    settings = make_settings(database_url=migrated_test_db, max_items_per_run=7)
    collector = ScriptedCollector([CollectionResult()])
    service = scripted_service(database, settings, collector)

    # A source row with no limit in its config.
    bare = SourceRow(
        id=(await get_source(database)).id,
        key="bare",
        name="Bare",
        integration="api",
        base_url="https://example.test",
        is_active=True,
        config={},
        created_at=None,
        updated_at=None,
    )

    await service.collect_source(bare, "scheduler")

    assert collector.seen[0][1] == 7


async def test_a_source_without_a_base_url_fails_cleanly(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)
    existing = await get_source(database)

    bare = SourceRow(
        id=existing.id,
        key="no-url",
        name="No URL",
        integration="rss",
        base_url=None,
        is_active=True,
        config={},
        created_at=None,
        updated_at=None,
    )

    result = await service.collect_source(bare, "scheduler")

    assert result.status == RunStatus.FAILED
    assert collector.seen == []
    assert result.error_summary is not None
    assert "base_url" in result.error_summary["errors"][0]["message"]


# --------------------------------------------------------------------------- reaping


async def test_reaping_is_delegated_and_reported(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    async with database.pool.connection() as conn:
        abandoned = await runs_repo.create_run(conn, source.id, "scheduler")
        await conn.execute(
            """
            UPDATE collection_runs
            SET started_at = now() - make_interval(mins => %(minutes)s)
            WHERE id = %(id)s
            """,
            {"minutes": service_settings.stale_run_minutes + 60, "id": abandoned.id},
        )

    reaped = await service.reap_stale_runs()

    assert reaped == 1

    # The source is collectable again, which is the point of reaping.
    result = await service.collect_source(source, "scheduler")
    assert result.status == RunStatus.SUCCEEDED


# --------------------------------------------------------------------------- trigger validation


async def test_an_invalid_trigger_is_rejected_before_any_work(
    database: Database, service_settings: Settings
) -> None:
    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)
    source = await get_source(database)

    with pytest.raises(ValueError, match="not a valid trigger"):
        await service.collect_source(source, "whenever")

    assert collector.seen == []


# --------------------------------------------------------------------------- fetch errors


@respx.mock
async def test_a_network_failure_is_recorded_on_the_run(
    database: Database, service: CollectionService
) -> None:
    """The real collector turning a 500 into a CollectError, end to end."""
    respx.get(f"{HN_BASE}/topstories.json").mock(return_value=httpx.Response(500))
    source = await get_source(database)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.FAILED
    assert result.error_summary is not None
    assert result.error_summary["errors"][0]["kind"] == ErrorKind.HTTP_STATUS

    stored = await run_row(database, result.run_id or 0)
    assert stored["status"] == RunStatus.FAILED
    assert stored["error_summary"] is not None


@respx.mock
async def test_a_deleted_item_is_skipped_without_failing_the_run(
    database: Database, service: CollectionService
) -> None:
    """Deleted HN items return JSON ``null``; that is normal, not an error."""
    respx.get(f"{HN_BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1, 2]))
    # `json=None` in httpx means "no body", so the literal JSON null is sent explicitly:
    # that is exactly what the live API returns for a deleted item.
    respx.get(f"{HN_BASE}/item/1.json").mock(return_value=httpx.Response(200, content=b"null"))
    respx.get(f"{HN_BASE}/item/2.json").mock(
        return_value=httpx.Response(
            200,
            json={
                "by": "tester",
                "id": 2,
                "time": 1_767_225_600,
                "title": "Alive",
                "type": "story",
            },
        )
    )

    source = await get_source(database)
    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED
    assert result.items_new == 1


# --------------------------------------------------------------------------- run result shape


def test_run_result_reports_skipped_and_persisted_consistently() -> None:
    skipped = RunResult(source_id=1, source_key="k", status=SKIPPED)
    done = RunResult(
        source_id=1, source_key="k", status=RunStatus.SUCCEEDED, items_new=2, items_updated=3
    )

    assert skipped.skipped is True
    assert skipped.persisted == 0
    assert done.skipped is False
    assert done.persisted == 5


def test_captured_at_default_is_utc() -> None:
    item = CollectedItem(external_id="x")

    assert item.captured_at.tzinfo is UTC


# --------------------------------------------------------------------------- no-credential class

async def _insert_source(
    database: Database,
    *,
    key: str,
    integration: str,
    base_url: str | None,
    config: Mapping[str, object],
) -> None:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO sources (key, name, integration, base_url, config, is_active)
            VALUES (%(key)s, %(name)s, %(integration)s, %(base_url)s,
                    CAST(%(config)s AS jsonb), true)
            """,
            {
                "key": key,
                "name": key,
                "integration": integration,
                "base_url": base_url,
                "config": json.dumps(config),
            },
        )

@respx.mock
async def test_public_api_source_collects_when_host_allowlisted(
    database: Database, service_settings: Settings
) -> None:
    """A no-credential JSON endpoint collects normally when its host is allowlisted."""
    config = {"interval_minutes": 60, "allowed_domains": "api.example.com"}
    await _insert_source(
        database,
        key="pubapi",
        integration="public_api",
        base_url="https://api.example.com/items",
        config=config,
    )
    source = await get_source(database, "pubapi")

    payload = {
        "items": [
            {
                "id": "1",
                "url": "https://api.example.com/1",
                "title": "One",
                "metrics": {"score": 5},
            },
            {"id": "2", "url": "https://api.example.com/2", "title": "Two"},
        ]
    }
    respx.get("https://api.example.com/items").mock(return_value=httpx.Response(200, json=payload))

    service = CollectionService(
        service_settings,
        database,
        collector_factory=get_collector,
        fetcher_factory=lambda s: HttpFetcher(
            s, policy=RetryPolicy(max_attempts=1, base_delay=0.0), resolver=public_resolver
        ),
    )

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.SUCCEEDED, f"{result.error_summary}"
    assert result.items_new == 2
    assert await item_count(database) == 2


async def test_no_credential_source_rejected_when_host_not_allowlisted(
    database: Database, service_settings: Settings
) -> None:
    """A no-credential source whose host is not allowlisted fails REJECTED before any fetch."""
    config = {"interval_minutes": 60}
    await _insert_source(
        database,
        key="pubapi-bad",
        integration="public_api",
        base_url="https://notallowed.example/items",
        config=config,
    )
    source = await get_source(database, "pubapi-bad")

    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.FAILED
    assert collector.seen == []  # the collector must never run
    assert result.error_summary is not None
    assert result.error_summary["errors"][0]["kind"] == "rejected"
    assert "allowlist" in result.error_summary["errors"][0]["message"]


async def test_rss_source_rejected_when_host_not_allowlisted(
    database: Database, service_settings: Settings
) -> None:
    """The same gate applies to generic RSS (ADR-0008)."""
    config = {"interval_minutes": 60}
    await _insert_source(
        database,
        key="rss-bad",
        integration="rss",
        base_url="https://notallowed.example/feed.xml",
        config=config,
    )
    source = await get_source(database, "rss-bad")

    collector = ScriptedCollector([])
    service = scripted_service(database, service_settings, collector)

    result = await service.collect_source(source, "scheduler")

    assert result.status == RunStatus.FAILED
    assert collector.seen == []
    assert result.error_summary is not None
    assert result.error_summary["errors"][0]["kind"] == "rejected"


async def test_credential_source_not_gated_by_allowlist(
    database: Database, service_settings: Settings
) -> None:
    """Credential-backed integrations are exempt from the allowlist requirement."""
    config = {"interval_minutes": 60}
    await _insert_source(
        database,
        key="cred-bad",
        integration="api",
        base_url="https://notallowed.example/items",
        config={"collector_key": "x", **config},
    )
    source = await get_source(database, "cred-bad")

    collector = ScriptedCollector([CollectionResult()])
    service = scripted_service(database, service_settings, collector)

    result = await service.collect_source(source, "scheduler")

    # The collector ran; the allowlist gate did not stop it.
    assert collector.seen != []
    assert result.items_fetched == 0
