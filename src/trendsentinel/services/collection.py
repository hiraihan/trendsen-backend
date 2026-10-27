"""Collection orchestration: the one place a run is executed and recorded.

Everything below this module is transport (``collectors``) or storage (``db``). This is the
only module that combines them, which is why both requirements that sound hardest — per-source
failure isolation and idempotent persistence — are implemented here, once.

The lifecycle a run goes through:

    create/claim run row  ->  advisory lock  ->  collect  ->  validate  ->  persist
                          ->  finish run (succeeded | partial | failed)  ->  release lock

Two guards against duplicated work are held at the same time, because they defend different
failure modes:

* the partial unique index on active runs stops a second run *row* for a source;
* ``pg_try_advisory_lock`` stops two processes performing the same *work* after a stale row
  has been reaped from under a still-running process.

A run that cannot start is reported as :data:`SKIPPED`, not as an error: "someone is already
collecting this source" is normal operation, not a failure.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol
from urllib.parse import urlsplit

# Importing the loader is what registers the adapters. It happens here, and not only in the
# API and worker entry points, so that constructing a service is enough to make the registry
# usable: an otherwise empty registry fails late, at the first collection.
from trendsentinel import __version__
from trendsentinel.collectors import loader as _collectors  # noqa: F401 - registers adapters
from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    Collector,
    ErrorKind,
    SourceRef,
    parse_domains,
)
from trendsentinel.collectors.http import HttpFetcher
from trendsentinel.collectors.registry import get_collector, resolve_key
from trendsentinel.collectors.ssrf import host_is_allowlisted
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemUpsert, RunRow, RunStatus, SourceRow
from trendsentinel.logging import get_logger

#: Integrations that are collectable without credentials (ADR-0008). For these the host
#: must be in an allowlist before any request is made; a non-allowlisted host fails the run
#: with ``REJECTED`` at collection time.
NO_CREDENTIAL_KEYS = frozenset({"rss", "public_api"})

__all__ = [
    "SKIPPED",
    "CollectionService",
    "RunResult",
    "build_collection_service",
]

logger = get_logger(__name__)

#: Not a database status: a run that never started. See the module docstring.
SKIPPED: Final[str] = "skipped"

#: The two triggers the database accepts, kept here so a bad one fails before the insert.
VALID_TRIGGERS: Final[frozenset[str]] = frozenset({"scheduler", "manual"})

#: Collapses any run of whitespace to a single space for fingerprint normalization.
_WS_RE: Final[re.Pattern[str]] = re.compile(r"\s+")


def content_fingerprint(item: CollectedItem) -> str | None:
    """Return a stable sha-256 over normalized content, or ``None`` when there is none.

    The input is the lower-cased, whitespace-collapsed concatenation of title and text.
    It deliberately excludes the source-specific ``external_id`` and ``url`` so the same
    article found on two sources fingerprints identically — enabling cross-source
    similarity/dedup without a dedicated NLP pass. It is a content digest, not a primary
    key: collisions are merely unlikely, so it must never be used as identity.
    """
    blob = "\n".join(part for part in (item.title, item.text) if part)
    normalized = blob.strip().lower()
    if not normalized:
        return None
    return hashlib.sha256(_WS_RE.sub(" ", normalized).encode("utf-8")).hexdigest()


class CollectorFactory(Protocol):
    """Builds the adapter for a source. Injected so tests need no registry."""

    def __call__(
        self,
        settings: Settings,
        fetcher: HttpFetcher,
        integration: str,
        config: Mapping[str, object] | None = None,
    ) -> Collector: ...


class FetcherFactory(Protocol):
    """Builds the run's HTTP fetcher. Injected so a test can supply its own transport."""

    def __call__(self, settings: Settings) -> HttpFetcher: ...


@dataclass(frozen=True, slots=True)
class RunResult:
    """The outcome of one attempted run.

    ``status`` is a :class:`RunStatus` value, or :data:`SKIPPED` when the work never started.
    """

    source_id: int
    source_key: str
    status: str
    run_id: int | None = None
    items_fetched: int = 0
    items_new: int = 0
    items_updated: int = 0
    error_summary: Mapping[str, Any] | None = None
    skipped_reason: str | None = None

    @property
    def skipped(self) -> bool:
        """Whether the run was skipped rather than executed."""
        return self.status == SKIPPED

    @property
    def persisted(self) -> int:
        """How many rows were written (new plus updated)."""
        return self.items_new + self.items_updated


class CollectionService:
    """Runs collectors and records what happened.

    The service owns the HTTP fetcher for a run: collectors receive one and never create or
    close their own, so a run uses a single bounded client.
    """

    def __init__(
        self,
        settings: Settings,
        database: Database,
        *,
        collector_factory: CollectorFactory = get_collector,
        fetcher_factory: FetcherFactory | None = None,
    ) -> None:
        self._settings = settings
        self._database = database
        self._collector_factory = collector_factory
        self._fetcher_factory = fetcher_factory or _default_fetcher

    #: How often `_persist` publishes live progress to the run row.
    _PROGRESS_EVERY = 5

    # -- public API -------------------------------------------------------------

    async def enqueue_source(
        self,
        source: SourceRow,
        trigger: str,
        *,
        item_limit: int | None = None,
    ) -> RunResult:
        """Create a run row without collecting anything.

        This is what ``POST /collect/run`` uses by default. The worker's poller executes the
        queued run, so the HTTP request never waits on a network round trip to a third party,
        and manual and scheduled collection share one execution path.

        Returns a skipped result when the source already has an active run.
        """
        self._assert_trigger(trigger)

        try:
            async with self._database.pool.connection() as conn:
                run = await runs_repo.create_run(
                    conn,
                    source.id,
                    trigger,
                    status=RunStatus.PENDING,
                    item_limit=item_limit,
                )
        except runs_repo.RunInProgressError:
            logger.info(
                "not queueing: source already has an active run",
                extra={"source": source.key, "trigger": trigger},
            )
            return self._skipped(source, "an active run already exists for this source")

        return RunResult(
            source_id=source.id,
            source_key=source.key,
            status=run.status,
            run_id=run.id,
        )

    async def collect_source(
        self,
        source: SourceRow,
        trigger: str,
        *,
        item_limit: int | None = None,
    ) -> RunResult:
        """Collect one source, creating its run row first.

        Returns a skipped result when the source already has an active run. Never raises for
        a source-level problem: a failure is recorded on the run instead.
        """
        self._assert_trigger(trigger)

        try:
            async with self._database.pool.connection() as conn:
                run = await runs_repo.create_run(
                    conn,
                    source.id,
                    trigger,
                    status=RunStatus.RUNNING,
                    item_limit=item_limit,
                )
        except runs_repo.RunInProgressError:
            logger.info(
                "skipping source with an active run",
                extra={"source": source.key, "trigger": trigger},
            )
            return self._skipped(source, "an active run already exists for this source")

        return await self._execute(run, source, trigger, item_limit)

    async def collect_pending(self, run: RunRow) -> RunResult:
        """Execute a run that ``POST /collect/run`` queued.

        Claiming is a conditional update, so if two workers see the same pending row exactly
        one proceeds; the other returns a skip.
        """
        async with self._database.pool.connection() as conn:
            claimed = await runs_repo.claim_pending_run(conn, run.id)

        if claimed is None:
            return RunResult(
                source_id=run.source_id,
                source_key="",
                status=SKIPPED,
                run_id=run.id,
                skipped_reason="the pending run was already claimed",
            )

        source = await self._get_source(claimed.source_id)
        if source is None:
            await self._finish(
                claimed.id,
                RunStatus.FAILED,
                error_summary=_internal_summary("the run's source no longer exists"),
            )
            return RunResult(
                source_id=claimed.source_id,
                source_key="",
                status=RunStatus.FAILED,
                run_id=claimed.id,
                error_summary=_internal_summary("the run's source no longer exists"),
            )

        return await self._execute(claimed, source, claimed.trigger, claimed.item_limit)

    async def collect_many(
        self,
        sources: Sequence[SourceRow],
        trigger: str,
        *,
        item_limit: int | None = None,
    ) -> list[RunResult]:
        """Collect several sources sequentially, isolating every failure.

        Sequential on purpose: three sources minutes apart do not justify concurrency, and
        the requirement is isolation, not throughput. Each source is wrapped in its own
        guard, so a crash — even one that escapes the run lifecycle — cannot stop the others.
        """
        results: list[RunResult] = []

        for source in sources:
            try:
                results.append(await self.collect_source(source, trigger, item_limit=item_limit))
            except Exception as exc:
                # Only reachable for a failure outside the run lifecycle (a database fault
                # while opening or closing the run). The loop must still continue.
                logger.exception("collection crashed for source", extra={"source": source.key})
                summary = _internal_summary(f"{type(exc).__name__}: {exc}")
                results.append(
                    RunResult(
                        source_id=source.id,
                        source_key=source.key,
                        status=RunStatus.FAILED,
                        error_summary=summary,
                    )
                )

        return results

    async def reap_stale_runs(self) -> int:
        """Close runs abandoned by a dead worker. Returns how many were reaped."""
        async with self._database.pool.connection() as conn:
            reaped = await runs_repo.reap_stale_runs(conn, self._settings.stale_run_minutes)

        if reaped:
            logger.warning(
                "reaped stale runs",
                extra={"count": reaped, "older_than_minutes": self._settings.stale_run_minutes},
            )

        return reaped

    # -- run execution ----------------------------------------------------------

    async def _execute(
        self,
        run: RunRow,
        source: SourceRow,
        trigger: str,
        item_limit: int | None,
    ) -> RunResult:
        """Hold the advisory lock, run the collector, and always close the run row."""
        limit = self._effective_limit(source, item_limit)

        async with self._database.pool.connection() as lock_conn:
            if not await runs_repo.try_advisory_lock(lock_conn, source.id):
                # The index allowed the row but another process already holds the work.
                locked_elsewhere = _internal_summary(
                    "another process holds the advisory lock for this source"
                )
                await self._finish(run.id, RunStatus.FAILED, error_summary=locked_elsewhere)
                logger.warning(
                    "advisory lock unavailable; not collecting",
                    extra={"source": source.key, "run_id": run.id},
                )
                return RunResult(
                    source_id=source.id,
                    source_key=source.key,
                    status=RunStatus.FAILED,
                    run_id=run.id,
                    error_summary=locked_elsewhere,
                )

            # End the implicit transaction while keeping the session-scoped lock, so this
            # connection sits idle rather than holding a snapshot for the whole run.
            await lock_conn.commit()

            try:
                return await self._collect(run, source, trigger, limit)
            except Exception as exc:
                logger.exception(
                    "run failed unexpectedly",
                    extra={"source": source.key, "run_id": run.id},
                )
                crashed = _internal_summary(f"{type(exc).__name__}: {exc}")
                await self._finish(run.id, RunStatus.FAILED, error_summary=crashed)
                return RunResult(
                    source_id=source.id,
                    source_key=source.key,
                    status=RunStatus.FAILED,
                    run_id=run.id,
                    error_summary=crashed,
                )
            finally:
                await runs_repo.release_advisory_lock(lock_conn, source.id)

    async def _collect(
        self,
        run: RunRow,
        source: SourceRow,
        trigger: str,
        limit: int,
    ) -> RunResult:
        """Fetch, validate, persist and close the run."""
        if not (source.base_url or "").strip():
            missing_url = _error_summary(
                [CollectError(ErrorKind.REJECTED, "source has no base_url")], {}
            )
            await self._finish(run.id, RunStatus.FAILED, error_summary=missing_url)
            return RunResult(
                source_id=source.id,
                source_key=source.key,
                status=RunStatus.FAILED,
                run_id=run.id,
                error_summary=missing_url,
            )

        blocked = self._allowlist_violation(source)
        if blocked is not None:
            await self._finish(run.id, RunStatus.FAILED, error_summary=blocked)
            return RunResult(
                source_id=source.id,
                source_key=source.key,
                status=RunStatus.FAILED,
                run_id=run.id,
                error_summary=blocked,
            )

        async with self._fetcher_factory(self._settings) as fetcher:
            collector = self._collector_factory(
                self._settings, fetcher, source.integration, source.config
            )
            result = await collector.collect(_source_ref(source), limit)
            fetched = len(result.items)
            errors: list[CollectError] = list(result.errors)
            accepted, rejects = validate_items(result.items)
            errors.extend(rejects)

            persisted = await self._persist(source, accepted, run_id=run.id)
            errors.extend(persisted.errors)

        counts = {
            "fetched": fetched,
            "new": persisted.new,
            "updated": persisted.updated,
            "rejected": len(rejects),
            "failed_to_store": len(persisted.errors),
        }
        status = _status_for(errors, persisted.total)
        summary: dict[str, Any] | None = _error_summary(errors, counts) if errors else None

        await self._finish(
            run.id,
            status,
            items_fetched=fetched,
            items_new=persisted.new,
            items_updated=persisted.updated,
            error_summary=summary,
        )
        logger.info(
            "collection finished",
            extra={
                "source": source.key,
                "run_id": run.id,
                "trigger": trigger,
                "status": status,
                **counts,
            },
        )

        return RunResult(
            source_id=source.id,
            source_key=source.key,
            status=status,
            run_id=run.id,
            items_fetched=fetched,
            items_new=persisted.new,
            items_updated=persisted.updated,
            error_summary=summary,
        )

    def _allowlist_violation(self, source: SourceRow) -> dict[str, Any] | None:
        """Return a REJECTED summary when a no-credential source's host is not allowlisted.

        No-credential sources (ADR-0008) must have their ``base_url`` host in the source's
        ``config.allowed_domains`` or the global ``allowed_html_domains_list`` before any
        request is made. Credential-backed integrations are exempt: their URLs come from the
        database under the vendor's own domain.
        """
        key = resolve_key(source.integration, source.config)
        if key not in NO_CREDENTIAL_KEYS:
            return None

        host = urlsplit((source.base_url or "").strip()).hostname
        domains = (
            *parse_domains(source.config or {}),
            *self._settings.allowed_html_domains_list,
        )
        # The allowlist is mandatory for this class: an empty allowlist must not silently
        # allow every public host.
        if host and domains and host_is_allowlisted(host.lower().rstrip("."), domains):
            return None

        return _error_summary(
            [
                CollectError(
                    ErrorKind.REJECTED,
                    "source host is not in the domain allowlist",
                    detail=(
                        f"add {host!r} to config.allowed_domains or "
                        "allowed_html_domains"
                    ),
                )
            ],
            {},
        )

    async def _persist(
        self,
        source: SourceRow,
        items: Sequence[CollectedItem],
        *,
        run_id: int,
    ) -> _PersistOutcome:
        """Store items, one transaction each, so one bad row cannot discard the batch.

        Counts are published live on the run row so the status view reports progress while a
        long batch is still writing. Writes are throttled to every ``_PROGRESS_EVERY`` items;
        the final, authoritative numbers are set by ``_finish``.
        """
        outcome = _PersistOutcome()

        async with self._database.pool.connection() as conn:
            for index, item in enumerate(items, start=1):
                try:
                    async with conn.transaction():
                        item_id, inserted = await items_repo.upsert_item(
                            conn, _to_upsert(source.id, item)
                        )
                        if item.metrics:
                            await snapshots_repo.record_snapshot(
                                conn, item_id, dict(item.metrics), item.captured_at
                            )
                except Exception as exc:
                    logger.warning(
                        "could not store item",
                        extra={"source": source.key, "external_id": item.external_id},
                        exc_info=True,
                    )
                    outcome.errors.append(
                        CollectError(
                            ErrorKind.INVALID_PAYLOAD,
                            f"could not store item {item.external_id!r}",
                            detail=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    if conn.closed:
                        # The connection is gone; further items would fail identically.
                        break
                    continue

                if inserted:
                    outcome.new += 1
                else:
                    outcome.updated += 1

                if index % self._PROGRESS_EVERY == 0:
                    await runs_repo.update_run_progress(
                        conn,
                        run_id,
                        items_fetched=index,
                        items_new=outcome.new,
                        items_updated=outcome.updated,
                    )

        return outcome

    # -- internals ---------------------------------------------------------------

    async def _finish(
        self,
        run_id: int,
        status: str,
        *,
        items_fetched: int = 0,
        items_new: int = 0,
        items_updated: int = 0,
        error_summary: Mapping[str, Any] | None = None,
    ) -> None:
        """Close a run row, tolerating a database that has become unreachable.

        A failure to record the outcome must not mask the original failure, so it is logged
        and swallowed: the stale-run reaper will close the row later.
        """
        try:
            async with self._database.pool.connection() as conn:
                await runs_repo.finish_run(
                    conn,
                    run_id,
                    status=status,
                    items_fetched=items_fetched,
                    items_new=items_new,
                    items_updated=items_updated,
                    error_summary=dict(error_summary) if error_summary is not None else None,
                )
        except Exception:
            logger.exception("could not record run outcome", extra={"run_id": run_id})

    async def _get_source(self, source_id: int) -> SourceRow | None:
        async with self._database.pool.connection() as conn:
            return await sources_repo.get_source_by_id(conn, source_id)

    def _effective_limit(self, source: SourceRow, item_limit: int | None) -> int:
        """Resolve the item cap: explicit request, then source config, then the global cap.

        The global ``max_items_per_run`` clamps every path, so a caller cannot widen what a
        run is allowed to store.
        """
        ceiling = self._settings.max_items_per_run
        for candidate in (item_limit, source.limit):
            if isinstance(candidate, int) and not isinstance(candidate, bool) and candidate > 0:
                return max(1, min(candidate, ceiling))

        return ceiling

    def _skipped(self, source: SourceRow, reason: str) -> RunResult:
        return RunResult(
            source_id=source.id,
            source_key=source.key,
            status=SKIPPED,
            skipped_reason=reason,
        )

    def _assert_trigger(self, trigger: str) -> None:
        if trigger not in VALID_TRIGGERS:
            raise ValueError(
                f"{trigger!r} is not a valid trigger; expected one of {sorted(VALID_TRIGGERS)}"
            )


@dataclass(slots=True)
class _PersistOutcome:
    """Counts and per-item failures from writing a batch."""

    new: int = 0
    updated: int = 0
    errors: list[CollectError] = field(default_factory=list)

    @property
    def total(self) -> int:
        """How many rows were written in total."""
        return self.new + self.updated


def validate_items(
    items: Sequence[CollectedItem],
) -> tuple[list[CollectedItem], list[CollectError]]:
    """Split items into storable ones and rejects.

    This is the validation gate: an item that cannot be stored is counted and reported, never
    persisted. Normalization runs first, so what is validated is what will be written.
    """
    accepted: list[CollectedItem] = []
    rejects: list[CollectError] = []

    for item in items:
        try:
            normalized = item.normalized()
            normalized.validate()
        except ValueError as exc:
            rejects.append(
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"item {item.external_id!r} failed validation",
                    detail=str(exc),
                )
            )
            continue

        accepted.append(normalized)

    return accepted, rejects


def _status_for(errors: Sequence[CollectError], persisted: int) -> str:
    """Map errors and stored rows onto a run status.

    ``partial`` is the expected case for real feeds: a run that stored items *and* hit some
    failures is not a broken run.
    """
    if not errors:
        return RunStatus.SUCCEEDED

    return RunStatus.PARTIAL if persisted > 0 else RunStatus.FAILED


def _error_summary(errors: Sequence[CollectError], counts: Mapping[str, int]) -> dict[str, Any]:
    """Build ``collection_runs.error_summary`` for a run that had at least one error."""
    return {"errors": [error.as_dict() for error in errors], "counts": dict(counts)}


def _internal_summary(message: str) -> dict[str, Any]:
    """Summary for a failure this module raised, not one a collector reported."""
    return {
        "errors": [{"kind": "internal_error", "message": message, "retryable": False}],
        "counts": {},
    }


def _source_ref(source: SourceRow) -> SourceRef:
    """Adapt a database row to the small view a collector is allowed to see."""
    return SourceRef(
        base_url=(source.base_url or "").strip(),
        key=source.key,
        config=source.config,
    )


def _to_upsert(source_id: int, item: CollectedItem) -> ItemUpsert:
    """Adapt a normalized item to the database-facing shape.

    This function is the only meeting point of the collector DTO and the database rows; the
    database layer must not depend on the collector layer.
    """
    metadata = dict(item.metadata)
    # Record which scraper version produced this row, so a later schema/parser change
    # stays auditable per record without a dedicated column.
    metadata.setdefault("collector_version", __version__)

    return ItemUpsert(
        source_id=source_id,
        external_id=item.external_id,
        url=item.url,
        title=item.title,
        text=item.text,
        author=item.author,
        published_at=item.published_at,
        metadata=metadata,
        captured_at=item.captured_at,
        language=item.language,
        content_type=item.content_type,
        author_url=item.author_url,
        thumbnail_url=item.thumbnail_url,
        fingerprint=content_fingerprint(item),
    )


def _default_fetcher(settings: Settings) -> HttpFetcher:
    """The production fetcher: one bounded, SSRF-guarded client per run."""
    return HttpFetcher(settings)


def build_collection_service(settings: Settings, database: Database) -> CollectionService:
    """Build the production collection service.

    A single constructor used by both the API (which only enqueues) and the worker (which
    executes), so the two processes cannot drift apart. Tests build the service directly to
    inject scripted collectors.
    """
    return CollectionService(settings, database)
