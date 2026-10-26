"""Tests for the X (Twitter) collector.

The fixture in ``tests/fixtures/x/`` follows the documented shape of X API v2's recent-search
response (``data[]`` plus ``includes.users`` and ``meta.next_token``). It was **not** captured
from the live API: reading the recent-search endpoint requires a paid X API read tier, which
this project has not been provisioned with. The shape is therefore unverified against the live
service until the first credentialed run; ``docs/tickets/T22-x-collector.md`` keeps that
recapture as the ticket's release condition. What these tests pin down is the mapping we
control: identity, absence, and metric names.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import CollectionResult, ErrorKind, SourceRef
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.x import XCollector, normalize_item
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "x"

X_BASE = "https://api.twitter.com"
SEARCH_PATH = "/2/tweets/search/recent"
PLACEHOLDER_TOKEN = "placeholder-app-only-token"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def page(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def first_page() -> dict[str, Any]:
    return cast("dict[str, Any]", load("recent_search_page1.json"))


@pytest.fixture
def usernames() -> dict[str, str]:
    users: list[dict[str, Any]] = load("recent_search_page1.json")["includes"]["users"]
    return {cast("str", user["id"]): cast("str", user["username"]) for user in users}


# --------------------------------------------------------------------------- identity


def test_identity_is_the_post_id(first_page: dict[str, Any]) -> None:
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.external_id == "1760000000000000001"


def test_a_non_string_id_is_rejected(first_page: dict[str, Any]) -> None:
    """X ids exceed the JS-safe range, so they must stay strings; a number is not one."""
    payload = {**first_page["data"][0], "id": 1760000000000000001}

    assert normalize_item(payload, usernames={}) is None


def test_a_missing_id_is_rejected(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "id": None}

    assert normalize_item(payload, usernames={}) is None


def test_a_blank_id_is_rejected(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "id": "   "}

    assert normalize_item(payload, usernames={}) is None


# --------------------------------------------------------------------------- fields


def test_the_text_is_kept_verbatim(first_page: dict[str, Any]) -> None:
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.text == "Shipping the trend detection rewrite today."


def test_an_empty_text_stays_absent(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "text": ""}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.text is None


def test_the_author_comes_from_the_includes_users(
    first_page: dict[str, Any], usernames: dict[str, str]
) -> None:
    item = normalize_item(first_page["data"][0], usernames=usernames)

    assert item is not None
    assert item.author == "trend_sentinel"


def test_an_unresolved_author_stays_absent(first_page: dict[str, Any]) -> None:
    """A post whose author is not in ``includes`` has no name we can report."""
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.author is None


def test_the_url_is_derived_only_when_the_username_resolved(
    first_page: dict[str, Any], usernames: dict[str, str]
) -> None:
    with_author = normalize_item(first_page["data"][0], usernames=usernames)
    without_author = normalize_item(first_page["data"][0], usernames={})

    assert with_author is not None
    assert with_author.url == "https://x.com/trend_sentinel/status/1760000000000000001"
    assert without_author is not None
    assert without_author.url is None


def test_published_at_is_parsed_from_iso8601(first_page: dict[str, Any]) -> None:
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.published_at is not None
    assert item.published_at.tzinfo is not None
    assert item.published_at.year == 2026
    assert item.published_at.month == 10
    assert item.published_at.day == 23


def test_a_missing_timestamp_leaves_published_at_absent(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "created_at": None}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.published_at is None


def test_an_unparsable_timestamp_leaves_published_at_absent(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "created_at": "yesterday"}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.published_at is None


# --------------------------------------------------------------------------- metrics


def test_metrics_use_the_shared_vocabulary(first_page: dict[str, Any]) -> None:
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.metrics == {
        "likes": 40.0,
        "reposts": 3.0,
        "replies": 2.0,
        "views": 5123.0,
    }


def test_a_zero_metric_is_kept_not_dropped(first_page: dict[str, Any]) -> None:
    """Zero is a real value; dropping it would hide a brand-new post."""
    item = normalize_item(first_page["data"][1], usernames={})

    assert item is not None
    assert item.metrics == {"likes": 0.0, "reposts": 0.0, "replies": 0.0}


def test_an_absent_metric_stays_absent(first_page: dict[str, Any]) -> None:
    """``impression_count`` is only returned for some tiers; it must not be zero-filled."""
    payload = {**first_page["data"][0], "public_metrics": {"like_count": 7}}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.metrics == {"likes": 7.0}


def test_missing_public_metrics_leave_metrics_absent(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0]}
    payload.pop("public_metrics")

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.metrics is None


def test_a_null_public_metrics_leaves_metrics_and_quote_count_absent(
    first_page: dict[str, Any],
) -> None:
    payload = {**first_page["data"][0], "public_metrics": None}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.metrics is None
    assert item.metadata["quote_count"] is None


def test_a_malformed_includes_block_leaves_the_author_absent(first_page: dict[str, Any]) -> None:
    payload = {**first_page["data"][0], "includes": []}

    item = normalize_item(payload, usernames={})

    assert item is not None
    assert item.author is None
    assert item.url is None


# --------------------------------------------------------------------------- metadata


def test_metadata_keeps_the_quote_count_and_the_author_id(first_page: dict[str, Any]) -> None:
    item = normalize_item(first_page["data"][0], usernames={})

    assert item is not None
    assert item.metadata["quote_count"] == 1
    assert item.metadata["author_id"] == "1769999999999999666"


# --------------------------------------------------------------------------- collection


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


def make_fetcher(settings: Settings | None = None) -> HttpFetcher:
    resolved = settings or build_settings()
    return HttpFetcher(
        resolved,
        policy=RetryPolicy(max_attempts=resolved.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    )


def make_source(**overrides: Any) -> SourceRef:
    config: dict[str, Any] = {
        "collector_key": "x",
        "x_bearer_token": PLACEHOLDER_TOKEN,
        "query": "trendsentinel OR scraping",
        **overrides,
    }
    return SourceRef(base_url=X_BASE, key="x_trendsentinel", config=config)


async def collect(
    source: SourceRef, limit: int = 10, settings: Settings | None = None
) -> CollectionResult:
    """Run the collector through a shared fetcher, exactly as the service layer does."""
    async with make_fetcher(settings) as fetcher:
        return await XCollector(settings or build_settings(), fetcher).collect(source, limit)


@respx.mock
async def test_collect_follows_the_next_token_until_it_runs_out() -> None:
    search = respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        side_effect=[
            httpx.Response(200, content=page("recent_search_page1.json")),
            httpx.Response(200, content=page("recent_search_page2.json")),
        ]
    )

    result = await collect(make_source())

    assert result.ok
    assert [item.external_id for item in result.items] == [
        "1760000000000000001",
        "1760000000000000002",
        "1760000000000000003",
    ]
    assert search.call_count == 2
    # The second request must carry the cursor the first response announced.
    assert "pagination-token-page-2" in str(search.calls.last.request.url)


@respx.mock
async def test_the_limit_is_respected() -> None:
    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, content=page("recent_search_page1.json"))
    )

    result = await collect(make_source(), limit=1)

    assert len(result.items) == 1
    assert result.items[0].external_id == "1760000000000000001"


@respx.mock
async def test_collect_sends_the_bearer_token_and_the_query() -> None:
    search = respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, content=page("recent_search_page2.json"))
    )

    await collect(make_source())

    request = search.calls.last.request
    assert request.headers["authorization"] == f"Bearer {PLACEHOLDER_TOKEN}"
    assert "query=trendsentinel" in str(request.url)
    assert "expansions=author_id" in str(request.url)


@respx.mock
async def test_collect_rejects_without_a_bearer_token_and_sends_nothing() -> None:
    """Failing loudly beats being rate-limited by the platform later."""
    search = respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, content=page("recent_search_page2.json"))
    )

    result = await collect(make_source(x_bearer_token=""))

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert "x_bearer_token" in result.errors[0].message
    assert search.call_count == 0


@respx.mock
async def test_collect_rejects_without_a_query_and_sends_nothing() -> None:
    search = respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, content=page("recent_search_page2.json"))
    )

    result = await collect(make_source(query=""))

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert "query" in result.errors[0].message
    assert search.call_count == 0


@respx.mock
async def test_a_401_is_rejected_as_non_retryable() -> None:
    """A revoked app-only token cannot be refreshed: the operator must issue a new one."""
    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(return_value=httpx.Response(401))

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert not result.errors[0].retryable


@respx.mock
async def test_a_403_is_rejected_as_non_retryable() -> None:
    """403 from X means the tier is not entitled to read; retrying cannot fix that."""
    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(return_value=httpx.Response(403))

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert not result.errors[0].retryable


@respx.mock
async def test_a_rate_limit_is_retryable() -> None:
    """``retry-after: 0`` keeps the suite fast; the header itself is honoured by the HTTP
    layer, proven in ``test_http.py``."""
    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(429, headers={"retry-after": "0"})
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.RATE_LIMITED
    assert result.errors[0].retryable


@respx.mock
async def test_a_payload_that_is_not_a_search_response_is_invalid() -> None:
    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, json={"errors": [{"message": "bad query"}]})
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_a_post_without_an_identity_is_reported_but_does_not_abort_the_run() -> None:
    payload = cast("dict[str, Any]", load("recent_search_page1.json"))
    payload["data"].append({"id": None, "text": "broken", "author_id": "1"})
    payload["meta"].pop("next_token")

    respx.get(url__startswith=f"{X_BASE}{SEARCH_PATH}").mock(
        return_value=httpx.Response(200, json=payload)
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD
    assert [item.external_id for item in result.items] == [
        "1760000000000000001",
        "1760000000000000002",
    ]
