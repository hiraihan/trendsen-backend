"""Worker entrypoint: ``python -m trendsentinel.worker``.

The worker owns the scheduler. It is a separate process from the API on purpose (see
``docs/architecture.md``): scaling the API replicas cannot then duplicate a collection job.

Startup order matters and is fixed:

1. configure logging, so every later step is observable;
2. open the pool, so the database is known to be reachable before scheduling begins;
3. reap stale runs, so a source abandoned by a previous, killed worker becomes collectable
   again before the first tick;
4. register jobs and start;
5. run until ``SIGTERM``/``SIGINT``, then shut the scheduler and the pool down gracefully.
"""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from trendsentinel.config import Settings, get_settings
from trendsentinel.db.pool import Database
from trendsentinel.logging import configure_logging, get_logger
from trendsentinel.services.collection import build_collection_service
from trendsentinel.worker.scheduler import build_scheduler

__all__ = ["main", "run"]

logger = get_logger(__name__)


async def run(settings: Settings, *, stop: asyncio.Event | None = None) -> None:
    """Run the worker until ``stop`` is set.

    ``stop`` is injected so a test can drive the whole lifecycle (open, reap, schedule, shut
    down) without signals.
    """
    configure_logging(settings)
    settings.assert_runtime_ready()

    database = Database(settings)
    await database.open()

    service = build_collection_service(settings, database)
    scheduler = build_scheduler(settings, service, database)

    try:
        # Reap before the first tick: a run left `running` by a killed worker would otherwise
        # block its source forever, because the partial unique index counts it as active.
        await scheduler.reap_stale_runs()

        await scheduler.load_jobs()
        scheduler.start()

        logger.info("worker started", extra={"pid": _pid()})
        await _wait_for_stop(stop)
    finally:
        # Shutdown is best-effort: it must not raise and hide the reason the worker is
        # stopping, and it must always close the pool. `database.close()` goes last, after
        # the scheduler has stopped using it, so nothing touches a closed pool.
        with suppress(Exception):
            scheduler.shutdown(wait=True)
        logger.info("worker stopped")
        await database.close()


async def _wait_for_stop(stop: asyncio.Event | None) -> None:
    """Wait for a shutdown signal, or for an injected event to be set."""
    if stop is not None:
        await stop.wait()
        return

    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()

    def request_stop(signum: int) -> None:
        logger.info("shutdown requested", extra={"signal": signal.Signals(signum).name})
        stopping.set()

    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_stop, int(sig))
            installed.append(sig)
        except NotImplementedError:  # pragma: no cover - not reachable on Linux
            signal.signal(sig, lambda _s, _f: request_stop(_s))

    try:
        await stopping.wait()
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def _pid() -> int:
    import os

    return os.getpid()


def main() -> None:
    """Blocking entrypoint used by ``python -m trendsentinel.worker``."""
    settings = get_settings()
    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:  # pragma: no cover - the signal handler normally handles this
        logger.info("worker interrupted")


if __name__ == "__main__":  # pragma: no cover
    main()
