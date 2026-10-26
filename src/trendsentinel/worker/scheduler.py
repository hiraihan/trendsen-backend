"""The scheduler worker.

Collection runs here and never in the API process: scaling the API cannot then duplicate a
job, because the API has no scheduler at all. Coordination uses only the database — a partial
unique index on active runs plus a per-source advisory lock — so no broker is needed at this
scale.

A job list that is built once at startup goes stale: activating a source or editing its
``config.interval_minutes`` in the database would then take effect only after a worker restart,
which is exactly the step an operator adding a source does not know to perform. So a reload job
re-reads ``sources`` every ``settings.source_reload_seconds`` and re-registers the source jobs.
It is a timer rather than ``SIGHUP`` because it needs no signal plumbing, behaves identically in
Docker and in a development shell, and heals itself when the table changes for any reason.

Four kinds of job are registered:

* one interval job per active source, for periodic collection;
* one poller job, which picks up ``pending`` runs that ``POST /collect/run`` queued and also
  reaps stale rows on every tick;
* one reload job, which keeps the source jobs in step with the ``sources`` table;
* a daily snapshot retention purge, registered only when ``settings.snapshot_purge_enabled`` is
  true. The service runs correctly without it; it exists so ``item_snapshots`` does not grow
  without bound, and it is opt-in because deleting collected data is an operator decision
  rather than a side effect of running the service.

The maintenance jobs (poller, reload, purge) are registered once and never removed: only
``source:*`` jobs belong to ``load_jobs``, so a reload cannot delete the reloader that is
executing it. That is what keeps reloads idempotent instead of oscillating.

The class is deliberately startable from a test without touching signal handlers: the job
methods are ordinary coroutines, so a test can await one directly and assert the guard.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Sequence
from typing import Any, Final

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from trendsentinel.collectors import loader as _collectors  # noqa: F401 - registers adapters
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import RunRow, SourceRow
from trendsentinel.logging import get_logger
from trendsentinel.services.collection import CollectionService, RunResult

__all__ = ["SourceScheduler", "interval_minutes_for"]

logger = get_logger(__name__)

#: How long APScheduler waits before considering a job trigger "missed". A run takes minutes;
#: anything shorter would queue catch-up runs after a worker restart.
_MISFIRE_GRACE_SECONDS: Final[int] = 300

#: Job ids are derived from the source so a reload replaces rather than duplicates a job.
_SOURCE_JOB_PREFIX: Final[str] = "source:"
_POLL_JOB_ID: Final[str] = "pending-runs"
_PURGE_JOB_ID: Final[str] = "snapshot-purge"
_RELOAD_JOB_ID: Final[str] = "reload-sources"

#: Hour (UTC) the retention purge runs at. 03:00 UTC is the low point of the collection
#: schedule: the busiest sources are US and EU daytime feeds, so a delete that holds row locks
#: on ``item_snapshots`` is least likely to compete with a collection run here. Hard-coded
#: rather than configurable because a wrong hour costs a little lock contention, not
#: correctness, and one more knob an operator can mistype is not worth that.
_PURGE_HOUR_UTC: Final[int] = 3

#: Grace period for the daily purge. Much longer than a source's: the purge is a single bulk
#: ``DELETE`` that must not be skipped just because a deploy or a busy collection run delayed
#: it by a few minutes, and nothing about it is time-critical to the second. A whole day of
#: lateness is also harmless — the next run catches up, since the window is absolute.
_PURGE_MISFIRE_GRACE_SECONDS: Final[int] = 6 * 60 * 60


def _interval_trigger(minutes: int) -> IntervalTrigger:
    """Build the interval trigger for a source job."""
    return IntervalTrigger(minutes=minutes)


def interval_minutes_for(source: SourceRow, settings: Settings) -> int:
    """Return the collection interval for a source, in minutes.

    The source's own ``interval_minutes`` decides; the global ``collect_interval_minutes`` is a
    floor, so a misconfigured source cannot be collected more aggressively than the operator
    allowed for the whole deployment.
    """
    configured = source.interval_minutes or settings.collect_interval_minutes
    return max(configured, settings.collect_interval_minutes)


def _source_job_ids(scheduler: AsyncIOScheduler) -> set[str]:
    """The ids of the source-owned jobs currently registered.

    Used to decide whether a reload changed anything. The maintenance jobs are excluded on
    purpose: they are re-registered on every reload, and counting them would make every reload
    look like a change.
    """
    return {
        job.id for job in scheduler.get_jobs() if job.id.startswith(_SOURCE_JOB_PREFIX)
    }


#: The shape of every scheduler job here: a bound coroutine method with no arguments.
_JobFunc = Callable[[], Coroutine[Any, Any, object]]


class SourceScheduler:
    """Owns the APScheduler instance and the jobs it runs."""

    def __init__(
        self,
        settings: Settings,
        service: CollectionService,
        database: Database,
    ) -> None:
        self._settings = settings
        self._service = service
        self._database = database
        self._scheduler = AsyncIOScheduler(timezone="UTC")

    @property
    def scheduler(self) -> AsyncIOScheduler:
        """The underlying APScheduler instance, for tests and for shutdown."""
        return self._scheduler

    async def load_jobs(self) -> int:
        """Bring the registered source jobs in step with the ``sources`` table.

        Idempotent by construction. A source job is only added, rescheduled or removed when it
        actually differs from the table, so a repeated call on a running scheduler is a no-op
        for unchanged sources and never calls ``start()``.

        **The reschedule-only-when-changed rule is load-bearing, not an optimisation.**
        ``add_job`` seeds ``next_run_time`` from now plus the interval, so re-adding an
        unchanged job on every reload would push its next fire forward every
        ``source_reload_seconds`` — and a job whose interval is longer than the reload period
        would then never fire at all. Comparing the desired trigger to the registered one is
        what keeps periodic collection actually periodic.

        Returns how many source jobs are registered.
        """
        sources = await self._active_sources()

        wanted: dict[int, SourceRow] = {source.id: source for source in sources}
        existing = {
            job.args[0]: job
            for job in self._scheduler.get_jobs()
            if job.id.startswith(_SOURCE_JOB_PREFIX) and job.args
        }

        for source_id, job in existing.items():
            if source_id not in wanted:
                job.remove()

        for source in sources:
            trigger = _interval_trigger(interval_minutes_for(source, self._settings))
            job = existing.get(source.id)
            if job is None:
                self._scheduler.add_job(
                    self.run_source,
                    trigger,
                    args=[source.id],
                    id=f"{_SOURCE_JOB_PREFIX}{source.id}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=_MISFIRE_GRACE_SECONDS,
                    replace_existing=True,
                    name=f"collect {source.key}",
                )
            elif str(job.trigger) != trigger:
                # The interval changed. ``reschedule`` keeps the job's id and run counters and
                # recomputes the next fire time from the new trigger, which is what we want;
                # a remove/re-add would do the same but churn the job store for nothing.
                job.reschedule(trigger)

        self._register_maintenance_jobs()

        logger.info(
            "scheduler jobs registered",
            extra={
                "sources": len(sources),
                "poll_seconds": self._settings.pending_poll_seconds,
                "reload_seconds": self._settings.source_reload_seconds,
                "snapshot_purge_enabled": self._settings.snapshot_purge_enabled,
            },
        )
        return len(sources)

    def start(self) -> None:
        """Start the scheduler."""
        self._scheduler.start()
        logger.info("scheduler started")

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop the scheduler, optionally waiting for running jobs to finish."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("scheduler stopped")

    # -- jobs -----------------------------------------------------------------------

    async def run_source(self, source_id: int) -> RunResult | None:
        """Collect one source by id. This is a scheduler job, so it never raises.

        A missing source (deleted between registration and execution) is logged and skipped.
        The service already isolates a collector failure into a failed run row.
        """
        source = await self._get_source(source_id)
        if source is None:
            logger.warning("scheduled source no longer exists", extra={"source_id": source_id})
            return None

        if not source.is_active:
            logger.info("skipping inactive source", extra={"source": source.key})
            return None

        try:
            result = await self._service.collect_source(source, "scheduler")
        except asyncio.CancelledError:
            # Shutdown must propagate so the process can stop promptly.
            raise
        except Exception:
            # The service records its own failures; reaching here means the guard itself
            # failed. One source must never take the scheduler down with it.
            logger.exception("scheduler job crashed", extra={"source": source.key})
            return None

        logger.info(
            "scheduled collection finished",
            extra={"source": result.source_key, "status": result.status},
        )
        return result

    async def poll_pending_runs(self) -> list[RunResult]:
        """Execute runs queued by ``POST /collect/run`` and reap stale rows.

        Both happen on the same tick: the poller is the one place that already runs on a timer,
        so it is where abandoned runs are noticed without adding another job.
        """
        await self.reap_stale_runs()

        results: list[RunResult] = []
        for run in await self._pending_runs():
            try:
                results.append(await self._service.collect_pending(run))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("pending run crashed", extra={"run_id": run.id})

        if results:
            logger.info("pending runs executed", extra={"count": len(results)})

        return results

    async def reap_stale_runs(self) -> int:
        """Close runs abandoned by a dead worker."""
        try:
            return await self._service.reap_stale_runs()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("could not reap stale runs")
            return 0

    async def purge_snapshots(self) -> int:
        """Delete metric snapshots past ``snapshot_retention_days``. Returns rows removed.

        A scheduler job, so it never raises: a failed purge is logged and the next daily run
        retries, because the window is absolute (``captured_at < now() - window``) and a missed
        night is simply caught up. The job is only registered when the operator enabled it.
        """
        try:
            removed = await self._purge_expired_snapshots()
        except asyncio.CancelledError:
            # Shutdown must propagate so the process can stop promptly.
            raise
        except Exception:
            logger.exception("could not purge old snapshots")
            return 0

        logger.info(
            "snapshot retention purge finished",
            extra={"removed": removed, "retention_days": self._settings.snapshot_retention_days},
        )
        return removed

    async def reload_sources(self) -> int:
        """Re-register the source jobs from the ``sources`` table. Returns the new count.

        A scheduler job, so it never raises: a worker that lost the database must retry on the
        next tick instead of dying, and the reload is the thing that would recover it. It only
        re-runs :meth:`load_jobs` — never ``start()``, which would raise
        ``SchedulerAlreadyRunningError`` on a running scheduler.

        Logs one line **only when the set of registered source jobs actually changed**, so a
        deployment where nothing happened stays quiet instead of emitting a line a minute.
        """
        try:
            previous = _source_job_ids(self._scheduler)
            registered = await self.load_jobs()
            current = _source_job_ids(self._scheduler)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("could not reload sources")
            return 0

        if current != previous:
            logger.info(
                "sources changed",
                extra={
                    "sources": registered,
                    "added": sorted(current - previous),
                    "removed": sorted(previous - current),
                },
            )
        return registered

    # -- internals ------------------------------------------------------------------

    def _register_maintenance_jobs(self) -> None:
        """Register the jobs that do not depend on the source list, idempotently.

        Called on every load and reload, so a repeat call must be a no-op that keeps the same
        id and trigger. Note that ``replace_existing=True`` only takes effect once the scheduler
        is running: while it is stopped, ``add_job`` appends to ``_pending_jobs`` without
        de-duplicating ids, and the same job would then run twice on every tick. Removing by id
        first is what makes the call idempotent in both states.
        """
        self._replace_job(
            self.poll_pending_runs,
            "interval",
            trigger_args={"seconds": self._settings.pending_poll_seconds},
            id=_POLL_JOB_ID,
            name="pending runs",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=self._settings.pending_poll_seconds,
        )

        # A reload is a cheap read, so its misfire grace is the same order of magnitude as the
        # poller's: a few minutes late is harmless, whereas a short grace would make APScheduler
        # skip the reload entirely after a busy collection run held the event loop.
        self._replace_job(
            self.reload_sources,
            "interval",
            trigger_args={"seconds": self._settings.source_reload_seconds},
            id=_RELOAD_JOB_ID,
            name="reload sources",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(self._settings.source_reload_seconds, _MISFIRE_GRACE_SECONDS),
        )

        if self._settings.snapshot_purge_enabled:
            self._replace_job(
                self.purge_snapshots,
                "cron",
                trigger_args={"hour": _PURGE_HOUR_UTC},
                id=_PURGE_JOB_ID,
                name="snapshot retention purge",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_PURGE_MISFIRE_GRACE_SECONDS,
            )

    def _replace_job(
        self,
        func: _JobFunc,
        trigger: str,
        *,
        trigger_args: dict[str, Any],
        id: str,
        name: str,
        **job_options: Any,
    ) -> None:
        """Register a maintenance job so that a repeat call reuses rather than duplicates it.

        A job that already exists is left alone when its trigger is unchanged. That matters for
        the reload job, which calls this via ``load_jobs`` *while it is executing*: removing and
        re-adding it would reset its own ``next_run_time`` every cycle, so its period would drift
        to "reload duration + interval" instead of the configured one.
        """
        existing = self._scheduler.get_job(id)
        if existing is not None:
            # Build the trigger the scheduler would build, then compare rendered forms: both
            # sides come from APScheduler's own renderer, so the check cannot drift from what
            # the scheduler actually stores.
            desired = self._scheduler._create_trigger(trigger, dict(trigger_args))
            if str(existing.trigger) == str(desired):
                return
            self._scheduler.remove_job(id)

        self._scheduler.add_job(
            func,
            trigger,
            id=id,
            name=name,
            replace_existing=True,
            **job_options,
            **trigger_args,
        )

    async def _active_sources(self) -> Sequence[SourceRow]:
        async with self._database.pool.connection() as conn:
            return await sources_repo.list_sources(conn, active_only=True)

    async def _get_source(self, source_id: int) -> SourceRow | None:
        async with self._database.pool.connection() as conn:
            return await sources_repo.get_source_by_id(conn, source_id)

    async def _pending_runs(self) -> list[RunRow]:
        async with self._database.pool.connection() as conn:
            return await runs_repo.get_pending_runs(conn)

    async def _purge_expired_snapshots(self) -> int:
        async with self._database.pool.connection() as conn:
            return await snapshots_repo.delete_snapshots_older_than(
                conn, self._settings.snapshot_retention_days
            )


def build_scheduler(
    settings: Settings,
    service: CollectionService,
    database: Database,
) -> SourceScheduler:
    """Construct the scheduler without registering jobs.

    Registration is a separate, awaitable step so a caller can decide when (and how often)
    the source list is read.
    """
    return SourceScheduler(settings, service, database)
