"""A reload must not disturb the schedule of an unchanged source job.

Regression tests for the bug that made scheduled collection never run: ``load_jobs`` used to
remove and re-add every ``source:*`` job on each reload, and ``add_job`` seeds ``next_run_time``
from now plus the interval. With a reload period shorter than a source interval — which is both
the default and the point of the reload — the next fire time was pushed forward on every reload
and arrived never.

Why these tests look the way they do: the reload suite in ``test_scheduler.py`` compares job ids
and trigger strings. Both survive a remove/re-add untouched, so they could not see the bug. The
observable difference is the job *identity* (a remove/re-add yields a different ``Job`` object)
and the *instants between which* the next fire time sits (a re-add shifts it by the reload
duration, which is microseconds fast and a whole interval slow — so the window, not the exact
value, is the honest assertion).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.worker.scheduler import SourceScheduler

from .test_scheduler import ScriptedCollector, get_source, scripted_service, worker_settings

pytestmark = pytest.mark.integration

#: Re-exported so this module shares the scheduler suite's settings fixture.
__all__ = ["worker_settings"]


@pytest.fixture
async def fast_scheduler(
    database: Database, worker_settings: Settings
) -> AsyncIterator[SourceScheduler]:
    """A scheduler over the real pool; no jobs run unless a test starts it."""
    service = scripted_service(database, worker_settings, ScriptedCollector([]))
    instance = SourceScheduler(worker_settings, service, database)
    try:
        yield instance
    finally:
        instance.shutdown(wait=False)


async def test_load_jobs_reuses_an_unchanged_source_job(
    fast_scheduler: SourceScheduler, database: Database
) -> None:
    """An unchanged source keeps the same job object, so nothing about it was reset.

    Identity is the assertion that matters: a remove-and-re-add would produce a different
    ``Job``. ``next_run_time`` is deliberately not compared exactly — APScheduler's own loop
    recomputes it on every tick, so a microsecond-level drift is normal and says nothing about
    whether the job survived.
    """
    await fast_scheduler.load_jobs()
    fast_scheduler.start()

    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    before = fast_scheduler.scheduler.get_job(job_id)

    await fast_scheduler.load_jobs()

    after = fast_scheduler.scheduler.get_job(job_id)
    assert after is before, "an unchanged job must not be removed and re-added"


async def test_a_reload_does_not_postpone_a_source_job(
    fast_scheduler: SourceScheduler, database: Database
) -> None:
    """A reload leaves the next fire time in the same place, not one interval later.

    A remove-and-re-add would set the next fire to roughly *now plus the interval* — a whole
    interval away. The reload is fast, so allowing a couple of seconds of scheduler jitter still
    distinguishes "untouched" from "rescheduled" by three orders of magnitude.
    """
    await fast_scheduler.load_jobs()
    fast_scheduler.start()

    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    before = fast_scheduler.scheduler.get_job(job_id).next_run_time

    await fast_scheduler.reload_sources()

    after = fast_scheduler.scheduler.get_job(job_id).next_run_time
    assert after is not None
    assert after - before < timedelta(seconds=5), (
        f"the next fire moved from {before} to {after} — the job was rescheduled"
    )


async def test_repeated_reloads_do_not_walk_the_schedule_forward(
    fast_scheduler: SourceScheduler, database: Database
) -> None:
    """Many reloads in a row must not accumulate drift in a source's next fire time.

    This is the deterministic form of the bug. With the old remove-and-re-add, every reload
    added the reload duration to ``next_run_time``; twenty reloads compounded it, and with a
    reload period shorter than the interval the job was postponed forever. Here the accumulated
    drift over twenty reloads must stay inside scheduler jitter.

    A test that waits for a real 30-minute job to fire is not practical, so this measures the
    thing that decides whether it fires: the next fire time.
    """
    await fast_scheduler.load_jobs()
    fast_scheduler.start()

    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    before = fast_scheduler.scheduler.get_job(job_id).next_run_time

    for _ in range(20):
        await fast_scheduler.reload_sources()

    after = fast_scheduler.scheduler.get_job(job_id).next_run_time
    assert after is not None
    assert after - before < timedelta(seconds=5), (
        f"twenty reloads moved the next fire from {before} to {after}"
    )


async def test_load_jobs_keeps_a_short_interval_job_on_its_own_schedule(
    fast_scheduler: SourceScheduler, database: Database
) -> None:
    """A reload leaves a job alone when its trigger already matches the table.

    ``load_jobs`` derives the desired trigger from ``sources``, so a source whose configured
    interval is unchanged must not be touched. This pins that the comparison — not a blanket
    remove-and-re-add — is what decides.
    """
    await fast_scheduler.load_jobs()
    fast_scheduler.start()

    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    before = fast_scheduler.scheduler.get_job(job_id)

    await fast_scheduler.load_jobs()

    assert fast_scheduler.scheduler.get_job(job_id) is before


async def test_an_edited_interval_is_applied_by_the_next_reload(
    fast_scheduler: SourceScheduler, database: Database
) -> None:
    """Changing ``config.interval_minutes`` in the database reaches the job, with no restart."""
    await fast_scheduler.load_jobs()

    github = await get_source(database, "github_blog")
    job_id = f"source:{github.id}"
    before = str(fast_scheduler.scheduler.get_job(job_id).trigger)

    async with database.pool.connection() as conn:
        await conn.execute(
            """
            UPDATE sources
            SET config = jsonb_set(config, '{interval_minutes}', '45'::jsonb)
            WHERE key = %(key)s
            """,
            {"key": "github_blog"},
        )

    await fast_scheduler.reload_sources()

    after = str(fast_scheduler.scheduler.get_job(job_id).trigger)
    assert after != before
    assert "0:45:00" in after, f"expected a 45-minute interval, got {after!r}"
