"""Multi-source isolation with the *real* registry adapters, not scripted ones.

The unit suites build each adapter directly and the scripted isolation test in
``test_collection_service.py`` feeds the service a fake, so neither proves what T25 asks:
that a misbehaving *authenticated* adapter — its credentials rejected at the token
exchange — is confined to its own run while the real Hacker News and RSS adapters keep
working in the same batch.

The failure exercised here is Reddit with bad script-app credentials: the token endpoint
answers 401. Two properties are pinned:

1. isolation — the other three sources run to completion and store their items;
2. discipline — a refused credential is never retried: the token endpoint is hit exactly
   once even though the fetcher's retry budget is the production default.

The fixtures are the same unverified recorded shapes the per-adapter suites use; the identity
of the sources, not their content, is what this file underwrites.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors import loader  # noqa: F401 - importing registers the adapters
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.reddit import TOKEN_URL
from trendsentinel.collectors.registry import get_collector
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import RunStatus, SourceRow
from trendsentinel.services.collection import CollectionService

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
HN_BASE = "https://hacker-news.firebaseio.com/v0"
HNRSS_URL = "https://hnrss.org/frontpage"
GITHUB_FEED_URL = "https://github.blog/feed/"
REDDIT_TOKEN_URL = TOKEN_URL

PRODUCTION_RETRY_BUDGET = 3


def public_resolver(_hostname: str) -> list[str]:
    """Deterministic DNS so the SSRF guard never touches the network in tests."""
    return ["93.184.216.34"]


@pytest.fixture
def settings(migrated_test_db: str) -> Settings:
    """Production-shaped settings: default retry budget, bound to the migrated test DB."""
    return build_settings(
        database_url=migrated_test_db,
        http_max_retries=PRODUCTION_RETRY_BUDGET,
    )


def real_service(database: Database, settings: Settings) -> CollectionService:
    """The service as production builds it: real registry, real fetcher policy.

    Only the DNS resolver is pinned, so the suite never resolves a real hostname while
    ``respx`` owns the HTTP layer.
    """
    return CollectionService(
        settings,
        database,
        collector_factory=get_collector,
        fetcher_factory=lambda s: HttpFetcher(
            s,
            policy=RetryPolicy(max_attempts=s.http_max_retries, base_delay=0.0),
            resolver=public_resolver,
        ),
    )


@pytest.fixture
async def service(database: Database, settings: Settings) -> AsyncIterator[CollectionService]:
    yield real_service(database, settings)


async def activate_reddit_with_bad_credentials(database: Database) -> None:
    """Activate the seeded-but-inactive Reddit source carrying credentials that will not work."""
    async with database.pool.connection() as conn:
        await conn.execute(
            """
            UPDATE sources
            SET is_active = true,
                config = config || %(config)s::jsonb
            WHERE key = 'reddit_technology'
            """,
            {
                "config": json.dumps(
                    {
                        "reddit_client_id": "bad-client-id",
                        "reddit_client_secret": "bad-client-secret",
                        "reddit_user_agent": "trendsentinel:uji-isolasi:1.0 (by /u/trendsentinel)",
                    }
                )
            },
        )


async def active_sources(database: Database) -> list[SourceRow]:
    async with database.pool.connection() as conn:
        sources = await sources_repo.list_sources(conn, active_only=True)

    assert len(sources) >= 4, "the batch needs at least four active sources"
    return sources


async def item_counts_per_source(database: Database) -> dict[str, int]:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT s.key AS key, count(ci.id) AS n
            FROM collected_items ci
            JOIN sources s ON s.id = ci.source_id
            GROUP BY s.key
            """
        )
        rows = await cur.fetchall()

    return {str(row["key"]): int(row["n"]) for row in rows}


async def run_row(database: Database, run_id: int) -> Mapping[str, object]:
    async with database.pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT status, items_fetched, items_new, error_summary, finished_at"
            " FROM collection_runs WHERE id = %(id)s",
            {"id": run_id},
        )
        row = await cur.fetchone()

    assert row is not None
    return dict(row)


def _hn_story(item_id: int) -> dict[str, Any]:
    return {
        "by": "tester",
        "id": item_id,
        "score": 10,
        "descendants": 3,
        "time": 1_767_225_600,
        "title": f"Story {item_id}",
        "type": "story",
        "url": f"https://example.test/{item_id}",
    }


def _feed(name: str) -> httpx.Response:
    """A recorded RSS body, served as the response for a feed route."""
    return httpx.Response(200, content=(FIXTURES / "rss" / name).read_bytes())


def mock_healthy_sources() -> None:
    """Serve every healthy source's endpoint, so only Reddit's credentials are in question."""
    respx.get(f"{HN_BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[301, 302]))
    for item_id in (301, 302):
        respx.get(f"{HN_BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json=_hn_story(item_id))
        )
    respx.get(HNRSS_URL).mock(return_value=_feed("hnrss_frontpage.xml"))
    respx.get(GITHUB_FEED_URL).mock(return_value=_feed("github_blog.xml"))


@respx.mock
async def test_a_rejected_credential_is_confined_to_its_own_run(
    database: Database, service: CollectionService
) -> None:
    """Four sources, one with refused credentials: the other three still collect."""
    mock_healthy_sources()
    token = respx.post(REDDIT_TOKEN_URL).mock(return_value=httpx.Response(401))

    await activate_reddit_with_bad_credentials(database)
    results = await service.collect_many(await active_sources(database), "scheduler")
    status_by_key = {result.source_key: result.status for result in results}

    assert status_by_key["hackernews"] == RunStatus.SUCCEEDED
    assert status_by_key["hnrss_frontpage"] == RunStatus.SUCCEEDED
    assert status_by_key["github_blog"] == RunStatus.SUCCEEDED
    assert status_by_key["reddit_technology"] == RunStatus.FAILED
    assert token.call_count == 1

    counts = await item_counts_per_source(database)
    assert counts["hackernews"] >= 1
    assert counts["hnrss_frontpage"] >= 1
    assert counts["github_blog"] >= 1
    assert counts.get("reddit_technology", 0) == 0, "a refused run stores nothing"


@respx.mock
async def test_the_refused_run_carries_a_non_retryable_rejection(
    database: Database, service: CollectionService
) -> None:
    """The run row must name the failure precisely enough to act on."""
    mock_healthy_sources()
    respx.post(REDDIT_TOKEN_URL).mock(return_value=httpx.Response(401))

    await activate_reddit_with_bad_credentials(database)
    sources = await active_sources(database)
    results = await service.collect_many(sources, "scheduler")

    rejected = next(result for result in results if result.source_key == "reddit_technology")
    assert rejected.run_id is not None
    summary = rejected.error_summary
    assert summary is not None
    [error] = summary["errors"]
    assert error["kind"] == "rejected"
    assert error["retryable"] is False
    assert summary["counts"]["new"] == 0

    row = await run_row(database, rejected.run_id)
    assert row["status"] == RunStatus.FAILED
    assert row["finished_at"] is not None

    healthy = [result for result in results if result.source_key != "reddit_technology"]
    assert all(result.error_summary is None for result in healthy)


@respx.mock
async def test_a_refused_credential_is_not_retried(
    database: Database, settings: Settings, service: CollectionService
) -> None:
    """A credential refusal consumes no retry budget: exchange once, fail, stop."""
    mock_healthy_sources()
    token = respx.post(REDDIT_TOKEN_URL).mock(return_value=httpx.Response(401))

    await activate_reddit_with_bad_credentials(database)
    await service.collect_many(await active_sources(database), "scheduler")

    assert token.call_count == 1, "a refused exchange must not burn the retry budget"
