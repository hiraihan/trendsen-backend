"""Scheduler behaviour against a real database and mocked HTTP.

The properties worth proving here are the ones the spec calls out: the interval floor, the
duplicate-work guard, per-source failure isolation, the pending-run poller, and stale-run
reaping. HTTP is always mocked, so the suite never touches the internet.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import CollectedItem, CollectionResult
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemUpsert, RunStatus, SourceRow
from trendsentinel.services.collection import CollectionService
from trendsentinel.worker.scheduler import SourceScheduler, interval_minutes_for

pytestmark = pytest.mark.integration

HN_BASE = "https://hacker-news.firebaseio.com/v0"


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


class ScriptedCollector:
    """A collector whose results, and failures, the test decides."""

    key = "scripted"

    def __init__(self, results: Sequence[CollectionResult | Exception]) -> None:
        self._results = list(results)
        self.calls = 0

    async def collect(self, source: object, limit: int) -> CollectionResult:
        self.calls += 1
        outcome = self._results.pop(0) if self._results else CollectionResult()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def scripted_service(
    database: Database, settings: Settings, collector: ScriptedCollector
) -> CollectionService:
    def factory(
        settings: Settings,
        fetcher: HttpFetcher,
        integration: str,
        config: Mapping[str, object] | None = None,
    ) -> ScriptedCollector:
        return collector

    return CollectionService(
        settings,
        database,
        collector_factory=factory,
        fetcher_factory=lambda s: HttpFetcher(
            s, policy=RetryPolicy(max_attempts=1, base_delay=0.0), resolver=public_resolver
        ),
    )


@pytest.fixture
def worker_settings(migrated_test_db: str) -> Settings:
    return build_settings(
        database_url=migrated_test_db,
        collect_interval_minutes=30,
        pending_poll_seconds=1,
        stale_run_minutes=30,
    )


@pytest.fixture
async def scheduler(
    database: Database, worker_settings: Settings
) -> AsyncIterator[SourceScheduler]:
    """A scheduler with a real pool, but jobs are invoked directly by each test."""
    service = scripted_service(database, worker_settings, ScriptedCollector([]))
    instance = SourceScheduler(worker_settings, service, database)
    try:
        yield instance
    finally:
        instance.shutdown(wait=False)


async def get_source(database: Database, key: str = "hackernews") -> SourceRow:
    async with database.pool.connection() as conn:
        source = await sources_repo.get_source_by_key(conn, key)

    assert source is not None, f"source {key!r} is not seeded"
    return source


async def set_active(database: Database, key: str, *, active: bool) -> None:
    """Flip a source's ``is_active`` flag.

    The ``database`` fixture snapshots and restores ``sources``, so a test may change the table
    freely without leaking the change into another test.
    """
    async with database.pool.connection() as conn:
        await conn.execute(
            "UPDATE sources SET is_active = %(active)s WHERE key = %(key)s",
            {"key": key, "active": active},
        )


async def run_rows(database: Database) -> list[Mapping[str, object]]:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, source_id, status, trigger, item_limit FROM collection_runs ORDER BY id"
        )
        return [dict(row) for row in await cur.fetchall()]


async def expected_source_job_ids(database: Database) -> set[str]:
    """The ``source:<id>`` job ids the *database* implies, so no test hard-codes a count.

    Seeding or activating a source changes this set; a test asserting against it keeps its
    meaning (one job per active source) instead of breaking on an unrelated data change.
    """
    async with database.pool.connection() as conn:
        active = await sources_repo.list_sources(conn, active_only=True)

    return {f"source:{source.id}" for source in active}


def source_job_ids(job_ids: set[str]) -> set[str]:
    """The ``source:*`` members of a job-id set, separating them from maintenance jobs."""
    return {job_id for job_id in job_ids if job_id.startswith("source:")}


# --------------------------------------------------------------------------- interval floor


def test_interval_uses_the_source_config_when_above_the_floor() -> None:
    settings = build_settings(collect_interval_minutes=30)
    source = _source(interval_minutes=60)

    assert interval_minutes_for(source, settings) == 60


def test_interval_is_clamped_up_to_the_global_floor() -> None:
    """A source asking for 5 minutes cannot make the deployment collect more often than 30."""
    settings = build_settings(collect_interval_minutes=30)
    source = _source(interval_minutes=5)

    assert interval_minutes_for(source, settings) == 30


def test_interval_falls_back_to_the_floor_when_unconfigured() -> None:
    settings = build_settings(collect_interval_minutes=45)

    assert interval_minutes_for(_source(interval_minutes=None), settings) == 45


async def test_load_jobs_registers_one_job_per_active_source_plus_the_maintenance_jobs(
    scheduler: SourceScheduler, database: Database
) -> None:
    registered = await scheduler.load_jobs()

    job_ids = {job.id for job in scheduler.scheduler.get_jobs()}
    expected = await expected_source_job_ids(database)
    assert source_job_ids(job_ids) == expected
    assert registered == len(expected)
    assert "pending-runs" in job_ids
    assert "reload-sources" in job_ids
    assert "snapshot-purge" not in job_ids, "the purge is opt-in"


async def test_load_jobs_drops_a_source_that_became_inactive(
    scheduler: SourceScheduler, database: Database
) -> None:
    await scheduler.load_jobs()
    await set_active(database, "github_blog", active=False)

    registered = await scheduler.load_jobs()

    job_ids = {job.id for job in scheduler.scheduler.get_jobs()}
    expected = await expected_source_job_ids(database)
    assert source_job_ids(job_ids) == expected
    assert registered == len(expected)
    # The maintenance jobs survive the reload, and only the deactivated source is gone.
    assert {"pending-runs", "reload-sources"} <= job_ids
    github = await get_source(database, "github_blog")
    assert f"source:{github.id}" not in job_ids


# --------------------------------------------------------------------------- source reload


async def test_load_jobs_picks_up_a_source_that_became_active(
    scheduler: SourceScheduler, database: Database
) -> None:
    """The documented way to add a source is a database write; no restart may be needed.

    The mirror image of the drop test: ``github_blog`` goes from inactive to active between
    two registrations and the reload that follows must create its job.
    """
    await set_active(database, "github_blog", active=False)
    await scheduler.load_jobs()
    github = await get_source(database, "github_blog")
    assert f"source:{github.id}" not in {job.id for job in scheduler.scheduler.get_jobs()}

    await set_active(database, "github_blog", active=True)

    registered = await scheduler.load_jobs()

    job_ids = {job.id for job in scheduler.scheduler.get_jobs()}
    expected = await expected_source_job_ids(database)
    assert source_job_ids(job_ids) == expected
    assert registered == len(expected)
    assert f"source:{github.id}" in job_ids


async def test_a_second_load_jobs_keeps_the_maintenance_jobs_and_does_not_duplicate(
    database: Database, worker_settings: Settings
) -> None:
    """Reloading must be a replacement, not an accumulation, and must not drop maintenance.

    A reload that removed the poller (or the purge job) would silently stop executing manual
    triggers and stop the nightly delete; both are re-registered idempotently.
    """
    settings = worker_settings.model_copy(update={"snapshot_purge_enabled": True})
    scheduler = SourceScheduler(
        settings,
        scripted_service(database, settings, ScriptedCollector([])),
        database,
    )

    await scheduler.load_jobs()
    first = sorted(job.id for job in scheduler.scheduler.get_jobs())
    await scheduler.load_jobs()
    second = sorted(job.id for job in scheduler.scheduler.get_jobs())

    assert first == second, "the same source set must yield the same job ids"
    assert len(second) == len(set(second)), "reloading must not duplicate jobs"
    expected = await expected_source_job_ids(database)
    # Exactly the source jobs plus the three maintenance jobs; nothing else, no subset.
    assert set(second) == expected | {"pending-runs", "reload-sources", "snapshot-purge"}


# --------------------------------------------------------------------------- source reload
# The reload mechanism: a periodic job re-registers the source jobs from the `sources` table.


async def test_load_jobs_registers_the_reload_job(
    scheduler: SourceScheduler,
) -> None:
    """Registration is kept current by a job, so the job that does it must itself be registered."""
    await scheduler.load_jobs()

    assert "reload-sources" in {job.id for job in scheduler.scheduler.get_jobs()}


async def test_the_reload_job_runs_on_a_started_scheduler_without_raising(
    scheduler: SourceScheduler, database: Database
) -> None:
    """The reload reaches a running scheduler: only ``load_jobs`` runs, never ``start`` again.

    A second ``start()`` would raise ``SchedulerAlreadyRunningError``, so this proves the reload
    path is the safe one, and that the scheduler is still running afterwards.
    """
    await scheduler.load_jobs()
    scheduler.start()
    before = sorted(job.id for job in scheduler.scheduler.get_jobs())

    await scheduler.reload_sources()

    assert scheduler.scheduler.running is True
    assert sorted(job.id for job in scheduler.scheduler.get_jobs()) == before


async def test_a_reload_with_no_database_change_keeps_the_same_jobs(
    scheduler: SourceScheduler,
) -> None:
    await scheduler.load_jobs()
    before = {(job.id, str(job.trigger)) for job in scheduler.scheduler.get_jobs()}

    await scheduler.reload_sources()

    assert {(job.id, str(job.trigger)) for job in scheduler.scheduler.get_jobs()} == before


async def test_a_reload_is_silent_when_nothing_changed_but_reports_a_change(
    scheduler: SourceScheduler, database: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """A line every reload would be noise; one line per actual change is the useful signal."""
    await scheduler.load_jobs()
    caplog.clear()

    with caplog.at_level(logging.INFO, logger="trendsentinel.worker.scheduler"):
        await scheduler.reload_sources()
    assert not [r for r in caplog.records if "sources changed" in r.getMessage()]

    await set_active(database, "github_blog", active=False)
    caplog.clear()

    with caplog.at_level(logging.INFO, logger="trendsentinel.worker.scheduler"):
        await scheduler.reload_sources()

    changed = [r for r in caplog.records if "sources changed" in r.getMessage()]
    assert len(changed) == 1
    assert changed[0].levelno == logging.INFO


async def test_a_reload_applies_an_edited_interval_to_the_existing_job(
    scheduler: SourceScheduler, database: Database
) -> None:
    """Editing ``config.interval_minutes`` is the other half of the ticket.

    ``github_blog`` is seeded at 120 minutes and is re-configured down to 45; after a reload its
    job must carry the new trigger. The scheduler is running here, because that is the state a
    stale job would show up in.
    """
    await scheduler.load_jobs()
    scheduler.start()
    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    assert str(scheduler.scheduler.get_job(job_id).trigger) == "interval[2:00:00]"

    try:
        async with database.pool.connection() as conn:
            await conn.execute(
                "UPDATE sources SET config = %(config)s::jsonb WHERE key = %(key)s",
                {
                    "key": "github_blog",
                    "config": '{"collector_key": "rss", "interval_minutes": 45}',
                },
            )

        await scheduler.reload_sources()

        assert str(scheduler.scheduler.get_job(job_id).trigger) == "interval[0:45:00]"
    finally:
        scheduler.shutdown(wait=False)


async def test_a_reload_never_raises_when_the_database_is_gone(
    worker_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    """A transient outage must not kill the timer that would recover from it."""
    # A dedicated pool, closed before the reload: the shared fixture's pool must stay usable so
    # its own teardown can still restore the sources table.
    database = Database(worker_settings)
    await database.open()
    scheduler = SourceScheduler(
        worker_settings,
        scripted_service(database, worker_settings, ScriptedCollector([])),
        database,
    )
    await scheduler.load_jobs()
    await database.close()
    caplog.clear()

    with caplog.at_level(logging.ERROR, logger="trendsentinel.worker.scheduler"):
        await scheduler.reload_sources()  # must not raise

    assert [r for r in caplog.records if "could not reload sources" in r.getMessage()]


async def test_the_running_scheduler_picks_up_a_new_source_on_its_own(
    database: Database, worker_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    """The whole point of the ticket, with no test calling the reload by hand.

    The reload interval is shortened to a second; the scheduler is started for real, a source is
    activated in the database, and the job must appear without a restart or a second ``start()``.
    """
    settings = worker_settings.model_copy(
        update={"source_reload_seconds": 1, "snapshot_purge_enabled": False}
    )
    scheduler = SourceScheduler(
        settings, scripted_service(database, settings, ScriptedCollector([])), database
    )
    # Deactivate before registering, so no job exists for it at startup. The scripted collector
    # has no queued results, so even an unexpected tick would be cheap.
    await set_active(database, "github_blog", active=False)
    await scheduler.load_jobs()
    scheduler.start()
    github = await get_source(database, "github_blog")
    assert f"source:{github.id}" not in {job.id for job in scheduler.scheduler.get_jobs()}

    try:
        caplog.clear()
        await set_active(database, "github_blog", active=True)
        with caplog.at_level(logging.INFO, logger="trendsentinel.worker.scheduler"):
            # Two reload periods plus slack: one tick is enough, the second covers a missed one.
            await _wait_until(
                lambda: f"source:{github.id}" in {j.id for j in scheduler.scheduler.get_jobs()},
                seconds=3.0,
            )
    finally:
        scheduler.shutdown(wait=False)

    assert f"source:{github.id}" in {j.id for j in scheduler.scheduler.get_jobs()}
    assert [r.getMessage() for r in caplog.records].count("sources changed") == 1


async def _wait_until(predicate: Callable[[], bool], *, seconds: float) -> None:
    """Poll a predicate, allowing the scheduler's own event loop to run its jobs."""
    deadline = asyncio.get_running_loop().time() + seconds
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("the scheduler did not reload within the timeout")
        await asyncio.sleep(0.05)


# --------------------------------------------------------------------------- retention purge


async def test_load_jobs_registers_the_purge_job_when_it_is_enabled(
    database: Database, worker_settings: Settings
) -> None:
    """Opted-in deployments get a daily purge; nothing else about registration changes."""
    settings = worker_settings.model_copy(update={"snapshot_purge_enabled": True})
    scheduler = SourceScheduler(
        settings,
        scripted_service(database, settings, ScriptedCollector([])),
        database,
    )

    await scheduler.load_jobs()

    assert "snapshot-purge" in {job.id for job in scheduler.scheduler.get_jobs()}


async def test_load_jobs_does_not_register_the_purge_job_by_default(
    scheduler: SourceScheduler,
) -> None:
    """Destroying collected data is opt-in: the default deployment never schedules a delete."""
    await scheduler.load_jobs()

    assert "snapshot-purge" not in {job.id for job in scheduler.scheduler.get_jobs()}


async def test_purge_snapshots_removes_only_rows_past_the_window(
    database: Database, worker_settings: Settings
) -> None:
    """One snapshot inside the 90-day window survives; one 400 days old does not."""
    settings = worker_settings.model_copy(update={"snapshot_purge_enabled": True})
    scheduler = SourceScheduler(
        settings,
        scripted_service(database, settings, ScriptedCollector([])),
        database,
    )
    source = await get_source(database)
    now = datetime.now(UTC)

    async with database.pool.connection() as conn:
        item_id, _ = await items_repo.upsert_item(conn, _upsert(source.id))
        await snapshots_repo.record_snapshot(conn, item_id, {"score": 1.0}, now)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 2.0}, now - timedelta(days=400)
        )

    removed = await scheduler.purge_snapshots()

    async with database.pool.connection() as conn:
        remaining = await snapshots_repo.list_snapshots(conn, item_id)

    assert removed == 1
    assert [snapshot.metrics for snapshot in remaining] == [{"score": 1.0}]


# --------------------------------------------------------------------------- concurrency guard


async def test_two_concurrent_runs_of_one_source_do_not_duplicate_work(
    database: Database, worker_settings: Settings
) -> None:
    """The guard is the database's; this test drives two jobs at the same source at once.

    Exactly one run executes; the other is skipped. Every attempt that did create a run row is
    still closed, so no row is left active.
    """
    collector = ScriptedCollector(
        [CollectionResult(items=[CollectedItem(external_id="only-once")])]
    )
    service = scripted_service(database, worker_settings, collector)
    scheduler = SourceScheduler(worker_settings, service, database)
    source = await get_source(database)

    results = await asyncio.gather(scheduler.run_source(source.id), scheduler.run_source(source.id))

    statuses = sorted(r.status for r in results if r is not None)
    assert RunStatus.SUCCEEDED in statuses
    assert statuses.count(RunStatus.SUCCEEDED) == 1, "only one run may execute"
    assert collector.calls == 1

    rows = await run_rows(database)
    assert len(rows) == 1
    assert rows[0]["status"] == RunStatus.SUCCEEDED


async def test_sequential_scheduled_runs_each_get_their_own_row(
    database: Database, worker_settings: Settings
) -> None:
    collector = ScriptedCollector([CollectionResult(items=[CollectedItem(external_id="a")])])
    service = scripted_service(database, worker_settings, collector)
    scheduler = SourceScheduler(worker_settings, service, database)
    source = await get_source(database)

    first = await scheduler.run_source(source.id)
    second = await scheduler.run_source(source.id)

    assert first is not None and first.status == RunStatus.SUCCEEDED
    # The first run closed its row, so the second legitimately runs; both persist one item.
    assert second is not None
    assert collector.calls == 2

    rows = await run_rows(database)
    assert len(rows) == 2
    assert all(row["status"] == RunStatus.SUCCEEDED for row in rows)


# --------------------------------------------------------------------------- failure isolation


async def test_a_crashing_source_does_not_stop_another_source(
    database: Database, worker_settings: Settings
) -> None:
    """One failing source must never abort the others; the scheduler job never raises.

    Dispatch is by ``integration``: the ``api`` source crashes, the ``rss`` source succeeds.
    Both runs are recorded, and the healthy one is unaffected.
    """

    def factory(
        settings: Settings,
        fetcher: HttpFetcher,
        integration: str,
        config: Mapping[str, object] | None = None,
    ) -> ScriptedCollector:
        if integration == "api":
            return ScriptedCollector([RuntimeError("collector exploded")])
        return ScriptedCollector([CollectionResult(items=[CollectedItem(external_id="ok")])])

    service = CollectionService(
        worker_settings,
        database,
        collector_factory=factory,
        fetcher_factory=lambda s: HttpFetcher(
            s, policy=RetryPolicy(max_attempts=1, base_delay=0.0), resolver=public_resolver
        ),
    )
    scheduler = SourceScheduler(worker_settings, service, database)

    crashing = await get_source(database, "hackernews")
    healthy = await get_source(database, "hnrss_frontpage")

    crashed = await scheduler.run_source(crashing.id)
    survived = await scheduler.run_source(healthy.id)

    assert crashed is not None and crashed.status == RunStatus.FAILED
    assert survived is not None and survived.status == RunStatus.SUCCEEDED, "the batch continued"

    rows = {row["source_id"]: row["status"] for row in await run_rows(database)}
    assert rows[crashing.id] == RunStatus.FAILED
    assert rows[healthy.id] == RunStatus.SUCCEEDED


async def test_a_source_with_a_deleted_row_is_skipped_quietly(
    scheduler: SourceScheduler,
) -> None:
    assert await scheduler.run_source(999_999) is None


async def test_run_source_reports_a_failure_without_raising(
    database: Database, worker_settings: Settings
) -> None:
    collector = ScriptedCollector([RuntimeError("boom")])
    service = scripted_service(database, worker_settings, collector)
    scheduler = SourceScheduler(worker_settings, service, database)
    source = await get_source(database)

    result = await scheduler.run_source(source.id)

    assert result is not None
    assert result.status == RunStatus.FAILED
    rows = await run_rows(database)
    assert rows[0]["status"] == RunStatus.FAILED


# --------------------------------------------------------------------------- pending poller


async def test_poller_executes_a_pending_run_exactly_once(
    database: Database, worker_settings: Settings
) -> None:
    collector = ScriptedCollector(
        [CollectionResult(items=[CollectedItem(external_id="pending-1")])]
    )
    service = scripted_service(database, worker_settings, collector)
    scheduler = SourceScheduler(worker_settings, service, database)
    source = await get_source(database)

    # What POST /collect/run does: create a pending row, no execution.
    async with database.pool.connection() as conn:
        pending = await runs_repo.create_run(
            conn, source.id, "manual", status=RunStatus.PENDING, item_limit=2
        )

    executed = await scheduler.poll_pending_runs()
    again = await scheduler.poll_pending_runs()

    assert [r.run_id for r in executed] == [pending.id]
    assert executed[0].status == RunStatus.SUCCEEDED
    assert again == [], "the run must not be claimed a second time"
    assert collector.calls == 1

    rows = await run_rows(database)
    assert rows[0]["status"] == RunStatus.SUCCEEDED
    assert rows[0]["item_limit"] == 2


async def test_poller_reaps_a_stale_run_and_frees_the_source(
    database: Database, worker_settings: Settings
) -> None:
    """The failure mode that would otherwise silently stop collection forever."""
    collector = ScriptedCollector([CollectionResult(items=[CollectedItem(external_id="after")])])
    service = scripted_service(database, worker_settings, collector)
    scheduler = SourceScheduler(worker_settings, service, database)
    source = await get_source(database)

    async with database.pool.connection() as conn:
        stale = await runs_repo.create_run(conn, source.id, "scheduler")
        await conn.execute(
            """
            UPDATE collection_runs
            SET started_at = now() - make_interval(mins => %(minutes)s)
            WHERE id = %(id)s
            """,
            {"minutes": worker_settings.stale_run_minutes + 30, "id": stale.id},
        )

    await scheduler.poll_pending_runs()

    rows = await run_rows(database)
    assert rows[0]["status"] == RunStatus.FAILED

    # With the stale row gone, the source is collectable again.
    result = await scheduler.run_source(source.id)
    assert result is not None and result.status == RunStatus.SUCCEEDED


async def test_shutdown_is_safe_when_never_started(scheduler: SourceScheduler) -> None:
    scheduler.shutdown(wait=False)  # must not raise


# --------------------------------------------------------------------------- process lifecycle
#
# No database needed: these cover `worker/__main__.py` itself — the signal wait that every
# deploy exercises, and the `python -m` entrypoint. They live here with the rest of the worker
# coverage rather than next to the collector tests.


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
async def test_wait_for_stop_returns_on_a_real_signal_and_removes_its_handlers(
    signum: signal.Signals, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shutdown path an operator hits on every `docker compose stop`.

    `loop.add_signal_handler` only works on the main thread, so this must not move to a worker
    thread. The handler is installed by the task before the signal is sent — sending a SIGTERM
    with nothing waiting on it would terminate the pytest process.

    Removal is observed at both layers that matter: the process-level disposition goes back to
    the default, and `remove_signal_handler` reports that it actually found (and cancelled)
    the loop handle the worker installed. A leaked handler would break every later test — it
    would swallow the next `SIGINT` of the session.

    Known limit: deleting the `remove_signal_handler` loop in `_wait_for_stop`'s `finally`
    still passes, because the loop's own teardown removes handlers when it closes. What this
    pins is the operator-visible contract (the wait ends, the process signal disposition is
    default again), not that the loop's private table was emptied by that exact statement.
    """
    from trendsentinel.worker import __main__ as worker_main

    loop = asyncio.get_running_loop()
    added: list[int] = []
    original_add = loop.add_signal_handler

    def recording_add(sig: int, callback: Callable[..., object], *args: object) -> None:
        original_add(sig, callback, *args)
        added.append(sig)

    monkeypatch.setattr(loop, "add_signal_handler", recording_add)

    task = asyncio.create_task(worker_main._wait_for_stop(None))
    try:
        # Give the task a chance to install its handlers before the signal arrives.
        await asyncio.sleep(0.05)
        assert signum in added, "the handler for this signal was never installed"

        os.kill(os.getpid(), signum)

        await asyncio.wait_for(task, timeout=5)
    finally:
        if not task.done():  # pragma: no cover - only on failure
            task.cancel()
        # Never leak a real handler into the rest of the session.
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)

    assert task.exception() is None
    assert signal.getsignal(signum) in (signal.SIG_DFL, signal.default_int_handler)


def test_main_completes_when_the_worker_stops(
    migrated_test_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`python -m trendsentinel.worker` exits cleanly when it is told to stop.

    A consequence test: real settings -> `run()` -> real `_wait_for_stop(None)` -> real
    `SIGTERM` (sent from a timer thread, so nothing is left awaiting a signal that never
    comes) -> `main()` returns. Every layer is the production one except `get_settings`, so
    the operator's `.env` cannot leak in.

    The only other seam is the event-loop driver: `main()` passes `stop=None`, so the
    production `asyncio.run` would serve a real pool until a signal arrives. The stub starts
    the timer, then defers to the real `asyncio.run`, which keeps `main()`'s teardown worth
    testing.
    """
    from trendsentinel.worker import __main__ as worker_main

    settings = build_settings(database_url=migrated_test_db)
    reached_wait: list[asyncio.Event | None] = []
    # Captured before the patch: a wrapper that reaches back through the module would call
    # itself (the patch replaces the module global).
    real_wait_for_stop = worker_main._wait_for_stop
    real_run = asyncio.run

    async def recording_wait(stop: asyncio.Event | None) -> None:
        reached_wait.append(stop)
        await real_wait_for_stop(stop)

    def signalling_driver(coro: Coroutine[object, object, None]) -> None:
        def send_stop() -> None:
            os.kill(os.getpid(), signal.SIGTERM)

        timer = threading.Timer(0.1, send_stop)
        timer.start()
        try:
            real_run(coro)
        finally:
            timer.cancel()

    monkeypatch.setattr(worker_main, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_main, "_wait_for_stop", recording_wait)
    monkeypatch.setattr(asyncio, "run", signalling_driver)

    worker_main.main()  # must not raise

    assert reached_wait == [None], "main() ran the worker to the signal wait and stopped"


async def test_worker_run_closes_its_pool_before_shutdown_completes(
    migrated_test_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shutdown half of the documented start-up order runs in a causal sequence.

    The other lifecycle test proves open -> reap -> schedule -> close eventually happen; this
    proves the *order*: the scheduler is stopped first (it is the pool's last user), then the
    pool is closed, and only then does `run()` log that it is done. Events rather than call
    counts, because the sequence is the behaviour.

    `WorkerDatabase.open`/`close` are wrapped, not stubbed: the real pool opens against the
    test database. Only `scheduler.shutdown` is stubbed, because with `stop` already set the
    scheduler has not started yet, and a real shutdown refuses that; the replaced method is a
    bare `pass`, so it could not fail silently either way.
    """
    from trendsentinel.db.pool import Database as WorkerDatabase
    from trendsentinel.worker import __main__ as worker_main
    from trendsentinel.worker.scheduler import SourceScheduler

    order: list[str] = []
    original_open = WorkerDatabase.open
    original_close = WorkerDatabase.close

    async def recording_open(self: WorkerDatabase) -> None:
        await original_open(self)
        order.append("open")

    async def recording_close(self: WorkerDatabase) -> None:
        await original_close(self)
        order.append("close")

    def recording_scheduler_shutdown(self: SourceScheduler, *, wait: bool = True) -> None:
        order.append("scheduler stopped")

    # A recorder on the module's own logger: the observability boundary, not a collaborator.
    class Recorder:
        def info(self, message: str, **_: object) -> None:
            order.append(message)

        def warning(self, message: str, **_: object) -> None:  # pragma: no cover
            order.append(message)

        def exception(self, message: str, **_: object) -> None:  # pragma: no cover
            order.append(message)

    stop = asyncio.Event()
    stop.set()

    monkeypatch.setattr(WorkerDatabase, "open", recording_open)
    monkeypatch.setattr(WorkerDatabase, "close", recording_close)
    monkeypatch.setattr(SourceScheduler, "shutdown", recording_scheduler_shutdown)
    monkeypatch.setattr(worker_main, "logger", Recorder())

    await worker_main.run(build_settings(database_url=migrated_test_db), stop=stop)

    assert order[0] == "open"
    assert "worker started" in order, "the worker did start, not just fail early"
    # Shutdown: scheduler first (it is the last user of the pool), then the pool is closed.
    assert order[-3:] == ["scheduler stopped", "worker stopped", "close"]


# --------------------------------------------------------------------------- worker lifecycle


async def test_worker_run_opens_reaps_schedules_and_shuts_down(
    database: Database, worker_settings: Settings
) -> None:
    """The whole worker lifecycle, driven by an injected stop event instead of signals.

    This is the closest automated equivalent of `docker compose up` then `SIGTERM`: it proves
    startup order (open → reap → schedule) and that shutdown closes the pool.
    """
    import asyncio

    from trendsentinel.worker import __main__ as worker_main

    # A stale row from a "previous worker" that would otherwise block its source forever.
    source = await get_source(database)
    async with database.pool.connection() as conn:
        stale = await runs_repo.create_run(conn, source.id, "scheduler")
        await conn.execute(
            """
            UPDATE collection_runs
            SET started_at = now() - make_interval(mins => %(minutes)s)
            WHERE id = %(id)s
            """,
            {"minutes": worker_settings.stale_run_minutes + 10, "id": stale.id},
        )

    stop = asyncio.Event()
    stop.set()  # shut down as soon as startup completes

    await worker_main.run(worker_settings, stop=stop)

    rows = await run_rows(database)
    assert rows[0]["status"] == RunStatus.FAILED, "the stale run was reaped at startup"


# --------------------------------------------------------------------------- live-collector path


@respx.mock
async def test_the_scheduled_job_runs_a_real_collector_end_to_end(
    database: Database, worker_settings: Settings
) -> None:
    """A real HN collector through a real fetcher and the real service, on a created job."""
    respx.get(f"{HN_BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1]))
    respx.get(f"{HN_BASE}/item/1.json").mock(
        return_value=httpx.Response(
            200,
            json={
                "by": "tester",
                "id": 1,
                "score": 5,
                "descendants": 2,
                "time": 1_767_225_600,
                "title": "Scheduled story",
                "type": "story",
                "url": "https://example.test/1",
            },
        )
    )

    service = CollectionService(
        worker_settings,
        database,
        fetcher_factory=lambda s: HttpFetcher(
            s, policy=RetryPolicy(max_attempts=1, base_delay=0.0), resolver=public_resolver
        ),
    )
    scheduler = SourceScheduler(worker_settings, service, database)
    await scheduler.load_jobs()
    source = await get_source(database)

    result = await scheduler.run_source(source.id)

    assert result is not None
    assert result.status == RunStatus.SUCCEEDED
    assert result.items_new == 1

    # Re-running updates rather than duplicating: the numeric dedup proof through the job path.
    again = await scheduler.run_source(source.id)
    assert again is not None
    assert again.items_new == 0
    assert again.items_updated == 1


# --------------------------------------------------------------------------- helpers


def _upsert(source_id: int, external_id: str = "purge-1") -> ItemUpsert:
    """A minimal item row, enough to own a snapshot."""
    return ItemUpsert(
        source_id=source_id,
        external_id=external_id,
        url=f"https://example.test/{external_id}",
        title=None,
        text=None,
        author=None,
        published_at=None,
        metadata={},
        captured_at=datetime(2026, 10, 21, 12, 0, tzinfo=UTC),
    )


def _source(interval_minutes: int | None) -> SourceRow:
    config = {} if interval_minutes is None else {"interval_minutes": interval_minutes}
    return SourceRow(
        id=1,
        key="k",
        name="K",
        integration="rss",
        base_url="https://example.test/feed",
        is_active=True,
        config=config,
        created_at=None,
        updated_at=None,
    )
