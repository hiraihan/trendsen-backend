"""The conformance suite: every registered collector observes the collector contract.

Each registered collector key must have a case here. A new adapter that registers itself in
``collectors/loader.py`` without a case fails these tests with its key named in the message —
so the suite grows with the registry instead of silently narrowing.

Fixtures are the verbatim captures the per-adapter test files use; no hand-built payloads.

Enforced contract per case (details in ``docs/adr/0002-metric-names.md``):

* the run succeeds and never exceeds its limit;
* identity is present, unique and whitespace-stable;
* absent optional fields stay ``None`` — nothing is zero-filled or fabricated;
* ``published_at`` (when present) is timezone-aware;
* ``metrics`` keys come from the shared vocabulary and every value is numeric.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import CollectedItem, CollectionResult, SourceRef
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.loader import (  # noqa: F401 - importing registers the adapters
    hackernews,
    reddit,
    rss,
    threads,
    wikimedia,
    x,
)
from trendsentinel.collectors.registry import available_keys, get_collector

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

#: The shared metrics vocabulary (ADR-0002). A collector must not emit anything outside it.
METRIC_VOCABULARY = frozenset({"score", "comments", "likes", "reposts", "replies", "views"})

HN_BASE = "https://hacker-news.firebaseio.com/v0"
HNRSS_URL = "https://hnrss.org/frontpage"
REDDIT_BASE = "https://oauth.reddit.com/r/technology"
REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
X_BASE = "https://api.twitter.com"
X_SEARCH_PATH = "/2/tweets/search/recent"
THREADS_BASE = "https://graph.threads.net/v1.0"


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


# --------------------------------------------------------------------------- cases


@dataclass(frozen=True, slots=True)
class ConformanceCase:
    """One registered collector, driven through the registry with canned transport."""

    key: str
    integration: str
    config: dict[str, Any]
    limit: int
    base_url: str
    setup_routes: Callable[[], None]

    @property
    def source(self) -> SourceRef:
        return SourceRef(base_url=self.base_url, key=self.key, config=self.config)


def _hn_item_route(base: str) -> None:
    """Serve the captured item fixtures; ids without a fixture are deleted items (null)."""

    def item_response(request: httpx.Request) -> httpx.Response:
        raw_id = request.url.path.removesuffix(".json").split("/")[-1]
        path = FIXTURES / "hackernews" / f"item_{raw_id}.json"
        content = path.read_bytes() if path.exists() else b"null"
        return httpx.Response(200, content=content)

    respx.get(url__regex=re.compile(re.escape(base) + r"/item/\d+\.json$")).mock(
        side_effect=item_response
    )


def _hn_setup() -> None:
    respx.get(f"{HN_BASE}/topstories.json").mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "hackernews" / "topstories.json").read_bytes()
        )
    )
    _hn_item_route(HN_BASE)


def _rss_setup() -> None:
    respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "rss" / "hnrss_frontpage.xml").read_bytes()
        )
    )


def _reddit_setup() -> None:
    respx.post(REDDIT_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "conformance-placeholder-token", "expires_in": 3600}
        )
    )
    respx.get(url__startswith=f"{REDDIT_BASE}/new").mock(
        side_effect=[
            httpx.Response(200, content=(FIXTURES / "reddit" / "listing_page1.json").read_bytes()),
            httpx.Response(200, content=(FIXTURES / "reddit" / "listing_page2.json").read_bytes()),
        ]
    )


def _x_setup() -> None:
    respx.get(url__startswith=f"{X_BASE}{X_SEARCH_PATH}").mock(
        side_effect=[
            httpx.Response(200, content=(FIXTURES / "x" / "recent_search_page1.json").read_bytes()),
            httpx.Response(200, content=(FIXTURES / "x" / "recent_search_page2.json").read_bytes()),
        ]
    )


def _threads_setup() -> None:
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "threads" / "me_profile.json").read_bytes()
        )
    )
    respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        side_effect=[
            httpx.Response(200, content=(FIXTURES / "threads" / "threads_page1.json").read_bytes()),
            httpx.Response(200, content=(FIXTURES / "threads" / "threads_page2.json").read_bytes()),
        ]
    )

PUBLIC_API_URL = "https://api.example.com/items"

def _public_api_setup() -> None:
    respx.get(PUBLIC_API_URL).mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "public_api" / "items.json").read_bytes()
        )
    )

WIKIMEDIA_BASE = "https://wikimedia.org/api/rest_v1/"

def _wikimedia_setup() -> None:
    respx.get(url__startswith=f"{WIKIMEDIA_BASE}metrics/pageviews/per-article/").mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "wikimedia" / "pageviews_js.json").read_bytes()
        )
    )


#: One entry per registered collector key. A new adapter adds its row here.
CASES: dict[str, ConformanceCase] = {
    "hackernews": ConformanceCase(
        key="hackernews",
        integration="api",
        config={"collector_key": "hackernews"},
        limit=5,
        base_url=HN_BASE,
        setup_routes=_hn_setup,
    ),
    "rss": ConformanceCase(
        key="rss",
        integration="rss",
        config={"collector_key": "rss"},
        limit=30,
        base_url=HNRSS_URL,
        setup_routes=_rss_setup,
    ),
    "reddit": ConformanceCase(
        key="reddit",
        integration="api",
        # Placeholder credentials, distinct from test_reddit.py's, so the module-level token
        # cache cannot carry a token between the two files.
        config={
            "collector_key": "reddit",
            "reddit_client_id": "conformance-client-id",
            "reddit_client_secret": "conformance-client-secret",
            "reddit_user_agent": "trendsentinel:conformance:1.0 (by /u/trendsentinel)",
        },
        limit=4,
        base_url=REDDIT_BASE,
        setup_routes=_reddit_setup,
    ),
    "x": ConformanceCase(
        key="x",
        integration="api",
        config={
            "collector_key": "x",
            "x_bearer_token": "conformance-placeholder-token",
            "query": "trendsentinel",
        },
        limit=3,
        base_url=X_BASE,
        setup_routes=_x_setup,
    ),
    "threads": ConformanceCase(
        key="threads",
        integration="api",
        config={
            "collector_key": "threads",
            "threads_access_token": "conformance-placeholder-token",
        },
        limit=3,
        base_url=THREADS_BASE,
        setup_routes=_threads_setup,
    ),
    "public_api": ConformanceCase(
        key="public_api",
        integration="public_api",
        config={
            "collector_key": "public_api",
            "allowed_domains": "api.example.com",
        },
        limit=3,
        base_url=PUBLIC_API_URL,
        setup_routes=_public_api_setup,
    ),
    "wikimedia": ConformanceCase(
        key="wikimedia",
        integration="api",
        config={
            "collector_key": "wikimedia",
            "articles": ["JavaScript"],
            "days": 7,
        },
        limit=2,
        base_url=WIKIMEDIA_BASE,
        setup_routes=_wikimedia_setup,
    ),
}

#: The production registry as of collection time: before any test has run, so test doubles
#: that other test files register during their execution cannot pollute this suite.
_PRODUCTION_KEYS: frozenset[str] = available_keys()


# --------------------------------------------------------------------------- coverage


def test_every_registered_collector_has_a_conformance_case() -> None:
    """A collector that registers itself without a case fails here, with its key named."""

    missing = sorted(_PRODUCTION_KEYS - CASES.keys())
    extra = sorted(CASES.keys() - _PRODUCTION_KEYS)

    assert not missing, f"register a conformance case for: {', '.join(missing)}"
    assert not extra, f"conformance cases for unregistered collectors: {', '.join(extra)}"


# --------------------------------------------------------------------------- the suite


@pytest.mark.parametrize("case", CASES.values(), ids=sorted(CASES))
@respx.mock
async def test_collector_conforms(case: ConformanceCase) -> None:
    case.setup_routes()
    settings = build_settings()

    async with HttpFetcher(
        settings,
        policy=RetryPolicy(max_attempts=settings.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    ) as fetcher:
        collector = get_collector(settings, fetcher, case.integration, case.config)
        assert collector.key == case.key
        result = await collector.collect(case.source, case.limit)

    assert_conforms(result, limit=case.limit)


def assert_conforms(result: CollectionResult, *, limit: int) -> None:
    """The shared invariants every collector must hold on its happy path."""
    assert result.ok, [f"{error.kind}: {error.message}" for error in result.errors]
    assert result.items, "a healthy run collects at least one item"
    assert len(result.items) <= limit

    external_ids = [item.external_id for item in result.items]
    assert all(item_id.strip() == item_id and item_id for item_id in external_ids)
    # Identity is unique within a run *at a capture instant*. Time-series sources (e.g.
    # Wikimedia pageviews) emit several observations of one article across distinct windows,
    # so the meaningful key is (external_id, captured_at) — same article at the same instant
    # would still be a duplicate bug.
    identity_pairs = [(item.external_id, item.captured_at) for item in result.items]
    assert len(set(identity_pairs)) == len(identity_pairs), (
        "identity+capture instant must be unique within a run"
    )

    for item in result.items:
        _assert_item_conforms(item)


def _assert_item_conforms(item: CollectedItem) -> None:
    normalized = item.normalized()

    normalized.validate()  # raises on an unusable item
    assert normalized.external_id

    if normalized.published_at is not None:
        assert normalized.published_at.tzinfo is not None

    # Absent stays absent: no zero-filling, no fabricated fields.
    for name in ("url", "title", "text", "author"):
        value = getattr(normalized, name)
        assert value is None or value.strip()

    if normalized.metrics is not None:
        unexpected = set(normalized.metrics) - METRIC_VOCABULARY
        assert not unexpected, f"{normalized.external_id} emitted unknown metrics: {unexpected}"
        assert all(
            isinstance(value, int | float) and not isinstance(value, bool)
            for value in normalized.metrics.values()
        )
