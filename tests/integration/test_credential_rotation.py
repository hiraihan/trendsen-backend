"""Rotation and error-record hygiene for a credentialed source, end to end.

ADR-0003/ADR-0004 put credentials in ``sources.config`` and the adapters read them from the
``SourceRef`` built per run, so rotating a credential is an ``UPDATE … SET config`` — no
restart, no redeploy. That claim is only worth something if it is proven against the real
service, the real registry and a real database row, which is what this file does:

* a rotated bearer is picked up on the very next run (the request carries the new value);
* a refused run's ``collection_runs.error_summary`` never stores the credential it used,
  even though the platform answered with a body that echoes it.

The HTTP layer is ``respx``-mocked and the X fixtures are the same unverified recorded shapes
the unit suite uses; the credential flow, not the payload, is what this file underwrites.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import cast

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.registry import get_collector
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import RunStatus, SourceRow
from trendsentinel.services.collection import CollectionService

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
X_BASE = "https://api.twitter.com"
X_SEARCH = f"{X_BASE}/2/tweets/search/recent"
X_SOURCE_KEY = "x_trendsentinel"

#: Distinctive, so a leak is unmistakable; the first is refused, the second is accepted.
TOKEN_BEFORE_ROTATION = "PROBE-X-BEARER-BEFORE-ROTATION-9c4d"
TOKEN_AFTER_ROTATION = "PROBE-X-BEARER-AFTER-ROTATION-9c4d"


def public_resolver(_hostname: str) -> list[str]:
    """Deterministic DNS so the SSRF guard never touches the network in tests."""
    return ["93.184.216.34"]


@pytest.fixture
async def settings(migrated_test_db: str) -> Settings:
    """Production-shaped settings: default retry budget, bound to the migrated test DB."""
    return build_settings(database_url=migrated_test_db)


@pytest.fixture
async def service(database: Database, settings: Settings) -> AsyncIterator[CollectionService]:
    """The service as production builds it: real registry, pinned DNS, ``respx`` owns HTTP."""
    service = CollectionService(
        settings,
        database,
        collector_factory=get_collector,
        fetcher_factory=lambda s: HttpFetcher(
            s,
            policy=RetryPolicy(max_attempts=s.http_max_retries, base_delay=0.0),
            resolver=public_resolver,
        ),
    )
    yield service


async def set_x_token(database: Database, token: str) -> None:
    """What an operator's rotation looks like: an UPDATE on ``sources.config``."""
    async with database.pool.connection() as conn:
        await conn.execute(
            """
            UPDATE sources
            SET is_active = true, config = config || %(config)s::jsonb
            WHERE key = %(key)s
            """,
            {"config": json.dumps({"x_bearer_token": token}), "key": X_SOURCE_KEY},
        )


async def active_x_source(database: Database) -> SourceRow:
    """The source row as the scheduler would read it on the next cycle."""
    async with database.pool.connection() as conn:
        rows = await sources_repo.list_sources(conn, active_only=True)
    [row] = [candidate for candidate in rows if candidate.key == X_SOURCE_KEY]
    return row


async def error_summary_of(database: Database, run_id: int) -> Mapping[str, object] | None:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT error_summary FROM collection_runs WHERE id = %(id)s", {"id": run_id}
        )
        row = await cur.fetchone()
    assert row is not None
    return cast(Mapping[str, object] | None, row["error_summary"])


def page_two() -> httpx.Response:
    """A one-page response, so a run makes exactly one search request."""
    return httpx.Response(200, content=(FIXTURES / "x" / "recent_search_page2.json").read_bytes())


@respx.mock
async def test_a_rotated_bearer_is_used_on_the_next_run_without_a_restart(
    database: Database, service: CollectionService
) -> None:
    """Rotation is a config UPDATE; the adapter must not have bound the credential earlier."""
    route = respx.get(url__startswith=X_SEARCH).mock(
        side_effect=[httpx.Response(401), page_two()]
    )

    await set_x_token(database, TOKEN_BEFORE_ROTATION)
    first = await service.collect_many([await active_x_source(database)], "scheduler")
    await set_x_token(database, TOKEN_AFTER_ROTATION)
    second = await service.collect_many([await active_x_source(database)], "scheduler")

    assert first[0].status == RunStatus.FAILED
    assert second[0].status == RunStatus.SUCCEEDED
    assert route.calls[0].request.headers["authorization"] == f"Bearer {TOKEN_BEFORE_ROTATION}"
    assert route.calls[1].request.headers["authorization"] == f"Bearer {TOKEN_AFTER_ROTATION}"


@respx.mock
async def test_the_recorded_run_never_stores_the_credential_it_used(
    database: Database, service: CollectionService
) -> None:
    """The platform quotes the bearer back in its error envelope; the record stays clean."""
    respx.get(url__startswith=X_SEARCH).mock(
        return_value=httpx.Response(
            200, json={"errors": [{"message": f"unauthorized for token {TOKEN_BEFORE_ROTATION}"}]}
        )
    )

    await set_x_token(database, TOKEN_BEFORE_ROTATION)
    results = await service.collect_many([await active_x_source(database)], "scheduler")
    run_id = results[0].run_id
    assert run_id is not None

    summary = await error_summary_of(database, run_id)
    assert summary is not None, "a failed run must record why it failed"
    rendered = json.dumps(summary)
    assert TOKEN_BEFORE_ROTATION not in rendered
    assert "***REDACTED***" in rendered, "the platform's message survives, the credential does not"

    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT config FROM sources WHERE key = %(key)s", {"key": X_SOURCE_KEY})
        row = await cur.fetchone()
    assert row is not None
    # The credential itself is still in the database, as ADR-0003 intends: only the record is clean.
    assert TOKEN_BEFORE_ROTATION in json.dumps(row["config"])
