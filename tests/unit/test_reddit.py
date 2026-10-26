"""Tests for the Reddit collector.

The listing fixture in ``tests/fixtures/reddit/`` follows the documented shape of Reddit's
OAuth listing response (``kind``/``data.children[].data``). It was **not** captured from the
live API: Reddit's public JSON endpoint is unreachable from this build machine, and no
script-app credentials exist yet. The shape is therefore unverified against the live service
until the first credentialed run; ``docs/tickets/T23-reddit-collector.md`` keeps that
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
from trendsentinel.collectors.reddit import (
    TOKEN_URL,
    RedditCollector,
    clear_token_cache,
    normalize_item,
)
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "reddit"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def link_post() -> dict[str, Any]:
    """The second child of the fixture: a link post with an empty body and a deleted author."""
    children: list[dict[str, Any]] = load("listing_page1.json")["data"]["children"]
    return cast("dict[str, Any]", children[1]["data"])


@pytest.fixture
def text_post() -> dict[str, Any]:
    children: list[dict[str, Any]] = load("listing_page1.json")["data"]["children"]
    return cast("dict[str, Any]", children[0]["data"])


# --------------------------------------------------------------------------- identity


def test_identity_is_the_fullname_not_the_bare_id(text_post: dict[str, Any]) -> None:
    """``t3_`` prefixed fullname: a bare id would collide with comments and other kinds."""
    item = normalize_item(text_post)

    assert item is not None
    assert item.external_id == "t3_abc123"


def test_identity_falls_back_to_the_fullname_field(text_post: dict[str, Any]) -> None:
    payload = {**text_post, "id": None}

    item = normalize_item(payload)

    assert item is not None
    assert item.external_id == "t3_abc123"


def test_a_post_without_any_identity_is_rejected(text_post: dict[str, Any]) -> None:
    payload = {**text_post, "id": None, "name": None}

    assert normalize_item(payload) is None


def test_a_blank_identity_is_rejected(text_post: dict[str, Any]) -> None:
    payload = {**text_post, "id": "", "name": "  "}

    assert normalize_item(payload) is None


# --------------------------------------------------------------------------- fields


def test_a_text_post_maps_title_body_and_author(text_post: dict[str, Any]) -> None:
    item = normalize_item(text_post)

    assert item is not None
    assert item.title == "Some post title"
    assert item.text == "A body paragraph.\n\nA second one."
    assert item.author == "someuser"


def test_an_empty_body_stays_absent(link_post: dict[str, Any]) -> None:
    """A link post has no body; storing an empty string would be a different thing."""
    item = normalize_item(link_post)

    assert item is not None
    assert item.text is None


def test_a_deleted_author_is_kept_verbatim(link_post: dict[str, Any]) -> None:
    """Reddit reports a removed author as ``[deleted]``; that is a fact, not an absence."""
    item = normalize_item(link_post)

    assert item is not None
    assert item.author == "[deleted]"


def test_the_permalink_becomes_an_absolute_url(text_post: dict[str, Any]) -> None:
    item = normalize_item(text_post)

    assert item is not None
    assert item.url == "https://www.reddit.com/r/technology/comments/abc123/some_post_title/"


def test_published_at_comes_from_created_utc(text_post: dict[str, Any]) -> None:
    item = normalize_item(text_post)

    assert item is not None
    assert item.published_at is not None
    assert item.published_at.tzinfo is not None
    assert item.published_at.timestamp() == 1761213600.0


def test_a_missing_timestamp_leaves_published_at_absent(text_post: dict[str, Any]) -> None:
    payload = {**text_post, "created_utc": None}

    item = normalize_item(payload)

    assert item is not None
    assert item.published_at is None


# --------------------------------------------------------------------------- metrics


def test_score_and_comments_become_metrics(text_post: dict[str, Any]) -> None:
    item = normalize_item(text_post)

    assert item is not None
    assert item.metrics == {"score": 42.0, "comments": 7.0}


def test_a_zero_score_is_kept_not_dropped(link_post: dict[str, Any]) -> None:
    """Zero is a real value; dropping it would hide a brand-new post."""
    item = normalize_item(link_post)

    assert item is not None
    assert item.metrics == {"score": 0.0, "comments": 0.0}


def test_non_numeric_metrics_are_ignored(text_post: dict[str, Any]) -> None:
    payload = {**text_post, "score": "many", "num_comments": True}

    item = normalize_item(payload)

    assert item is not None
    assert item.metrics is None


# --------------------------------------------------------------------------- metadata


def test_metadata_keeps_the_platform_specific_numbers(text_post: dict[str, Any]) -> None:
    """``upvote_ratio`` is not a metric in the shared vocabulary, so it lives in metadata."""
    item = normalize_item(text_post)

    assert item is not None
    assert item.metadata["subreddit"] == "technology"
    assert item.metadata["upvote_ratio"] == 0.95
    assert item.metadata["stickied"] is False


# --------------------------------------------------------------------------- thumbnail


def test_thumbnail_extracted_from_preview(link_post: dict[str, Any]) -> None:
    """A post with ``preview`` carries its source image URL as ``thumbnail_url``."""
    payload = {
        **link_post,
        "preview": {
            "images": [
                {
                    "source": {"url": "https://preview.redd.it/cover.jpg"},
                    "resolutions": [{"url": "https://preview.redd.it/small.jpg"}],
                }
            ]
        },
    }
    item = normalize_item(payload)

    assert item is not None
    assert item.metadata["thumbnail_url"] == "https://preview.redd.it/cover.jpg"


def test_thumbnail_absent_when_the_post_has_no_preview(text_post: dict[str, Any]) -> None:
    """Extracted, never fabricated: a text post keeps ``None``, not a placeholder."""
    item = normalize_item(text_post)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


def test_thumbnail_refuses_a_non_https_preview(link_post: dict[str, Any]) -> None:
    payload = {
        **link_post,
        "preview": {"images": [{"source": {"url": "http://preview.redd.it/x.jpg"}}]},
    }
    item = normalize_item(payload)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


# --------------------------------------------------------------------------- collection

LISTING_BASE = "https://oauth.reddit.com/r/technology"
LISTING_PATTERN = f"{LISTING_BASE}/new"
PLACEHOLDER_UA = "trendsentinel:tests:1.0 (by /u/trendsentinel)"


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
        "collector_key": "reddit",
        "reddit_client_id": "placeholder-client-id",
        "reddit_client_secret": "placeholder-client-secret",
        "reddit_user_agent": PLACEHOLDER_UA,
        **overrides,
    }
    return SourceRef(base_url=LISTING_BASE, key="reddit_technology", config=config)


async def collect(
    source: SourceRef, limit: int = 10, settings: Settings | None = None
) -> CollectionResult:
    """Run the collector through a shared fetcher, exactly as the service layer does."""
    async with make_fetcher(settings) as fetcher:
        return await RedditCollector(settings or build_settings(), fetcher).collect(source, limit)


def listing(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def token_response(token: str = "placeholder-access-token") -> httpx.Response:
    return httpx.Response(200, json={"access_token": token, "expires_in": 3600})


@pytest.fixture(autouse=True)
def isolated_token_cache() -> None:
    """The in-process token cache is module state; no test may inherit a token."""
    clear_token_cache()


@respx.mock
async def test_collect_exchanges_the_token_once_and_reuses_it_across_pages() -> None:
    token_route = respx.post(TOKEN_URL).mock(return_value=token_response())
    listing_route = respx.get(url__startswith=LISTING_PATTERN).mock(
        side_effect=[
            httpx.Response(200, content=listing("listing_page1.json")),
            httpx.Response(200, content=listing("listing_page2.json")),
        ]
    )

    result = await collect(make_source())

    assert result.ok
    assert [item.external_id for item in result.items] == ["t3_abc123", "t3_def456", "t3_ghi789"]
    # One exchange for the whole run, not one per page.
    assert token_route.call_count == 1
    assert listing_route.call_count == 2

    for call in listing_route.calls:
        assert call.request.headers["authorization"] == "Bearer placeholder-access-token"
        assert call.request.headers["user-agent"] == PLACEHOLDER_UA


@respx.mock
async def test_collect_sends_the_descriptive_user_agent_to_the_token_endpoint() -> None:
    token_route = respx.post(TOKEN_URL).mock(return_value=token_response())
    respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(200, content=listing("listing_page2.json"))
    )

    await collect(make_source())

    assert token_route.calls.last.request.headers["user-agent"] == PLACEHOLDER_UA
    assert token_route.calls.last.request.headers["authorization"].startswith("Basic ")


@respx.mock
async def test_collect_rejects_without_a_user_agent_and_sends_nothing() -> None:
    """Reddit refuses anonymous clients; failing here is better than being blocked silently."""
    token_route = respx.post(TOKEN_URL).mock(return_value=token_response())
    listing_route = respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(200, content=listing("listing_page2.json"))
    )

    result = await collect(make_source(reddit_user_agent=""))

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert "reddit_user_agent" in result.errors[0].message
    assert token_route.call_count == 0
    assert listing_route.call_count == 0


@respx.mock
async def test_collect_refreshes_the_token_once_after_a_401() -> None:
    token_route = respx.post(TOKEN_URL).mock(
        side_effect=[token_response("first-token"), token_response("refreshed-token")]
    )
    listing_route = respx.get(url__startswith=LISTING_PATTERN).mock(
        side_effect=[
            httpx.Response(401),
            httpx.Response(200, content=listing("listing_page2.json")),
        ]
    )

    result = await collect(make_source())

    assert result.ok
    assert [item.external_id for item in result.items] == ["t3_ghi789"]
    assert token_route.call_count == 2
    assert listing_route.call_count == 2
    assert listing_route.calls.last.request.headers["authorization"] == "Bearer refreshed-token"


@respx.mock
async def test_collect_rejects_after_a_second_401_without_a_third_attempt() -> None:
    token_route = respx.post(TOKEN_URL).mock(
        side_effect=[token_response("first-token"), token_response("refreshed-token")]
    )
    listing_route = respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(401)
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert not result.errors[0].retryable
    assert token_route.call_count == 2
    assert listing_route.call_count == 2


@respx.mock
async def test_collect_rejects_when_the_token_exchange_itself_is_refused() -> None:
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(401))
    listing_route = respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(200, content=listing("listing_page2.json"))
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert not result.errors[0].retryable
    assert listing_route.call_count == 0


@respx.mock
async def test_the_limit_is_respected_across_pages() -> None:
    respx.post(TOKEN_URL).mock(return_value=token_response())
    respx.get(url__startswith=LISTING_PATTERN).mock(
        side_effect=[
            httpx.Response(200, content=listing("listing_page1.json")),
            httpx.Response(200, content=listing("listing_page2.json")),
        ]
    )

    result = await collect(make_source(), limit=1)

    assert len(result.items) == 1
    assert result.items[0].external_id == "t3_abc123"


@respx.mock
async def test_the_token_is_reused_across_runs_while_it_is_valid() -> None:
    token_route = respx.post(TOKEN_URL).mock(return_value=token_response())
    respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(200, content=listing("listing_page2.json"))
    )

    first = await collect(make_source())
    second = await collect(make_source())

    assert first.ok and second.ok
    assert token_route.call_count == 1


@respx.mock
async def test_a_listing_that_is_not_a_listing_is_an_invalid_payload() -> None:
    respx.post(TOKEN_URL).mock(return_value=token_response())
    respx.get(url__startswith=LISTING_PATTERN).mock(
        return_value=httpx.Response(200, json={"unexpected": True})
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_non_post_children_are_skipped_without_errors() -> None:
    """A listing can carry comment/ad children (``kind != 't3'``); they are not failures."""
    respx.post(TOKEN_URL).mock(return_value=token_response())
    payload = load("listing_page2.json")
    payload["data"]["children"].append({"kind": "t1", "data": {"id": "comment1"}})
    respx.get(url__startswith=LISTING_PATTERN).mock(return_value=httpx.Response(200, json=payload))

    result = await collect(make_source())

    assert result.ok
    assert [item.external_id for item in result.items] == ["t3_ghi789"]
