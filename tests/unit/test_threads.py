"""Tests for the Threads collector.

The fixtures in ``tests/fixtures/threads/`` follow the documented shape of the Threads Graph
API. They were **not** captured from the live API: the app-scoped permissions this collector
needs are granted only after Meta App Review, which has not run. The shape is therefore
unverified against the live service until the first credentialed run; the review result and
that recapture are the exit conditions tracked in ``docs/tickets/T24-threads-collector.md``.

What these tests pin down is what we control: identity, absence (a post may have no title,
no link and no metrics), metric names, and the fact that the access token never leaks into a
URL that gets logged or into ``paging.next``'s embedded credential.
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
from trendsentinel.collectors.threads import ThreadsCollector, normalize_item
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "threads"

THREADS_BASE = "https://graph.threads.net/v1.0"
PLACEHOLDER_TOKEN = "placeholder-long-lived-token"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def page(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def first_page() -> dict[str, Any]:
    return cast("dict[str, Any]", load("threads_page1.json"))


@pytest.fixture
def first_post() -> dict[str, Any]:
    children: list[dict[str, Any]] = load("threads_page1.json")["data"]
    return children[0]


@pytest.fixture
def media_post() -> dict[str, Any]:
    """The second child: an image post with no permalink and no body link."""
    children: list[dict[str, Any]] = load("threads_page1.json")["data"]
    return children[1]


PROFILE_USERNAME = "trendsentinel_ops"


# --------------------------------------------------------------------------- identity


def test_identity_is_the_post_id(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.external_id == "17923800000000001"


def test_a_missing_id_is_rejected(first_post: dict[str, Any]) -> None:
    payload = {**first_post, "id": None}

    assert normalize_item(payload, PROFILE_USERNAME) is None


def test_a_non_string_id_is_rejected(first_post: dict[str, Any]) -> None:
    payload = {**first_post, "id": 17923800000000001}

    assert normalize_item(payload, PROFILE_USERNAME) is None


# --------------------------------------------------------------------------- fields


def test_the_body_becomes_the_text(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.text == "First post, text only."


def test_a_post_has_no_title(first_post: dict[str, Any]) -> None:
    """Threads posts carry no title; never fabricate one from the first line."""
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.title is None


def test_the_author_is_the_profile_username(first_post: dict[str, Any]) -> None:
    """``me/threads`` is always the profile's own posts, so one username covers the run."""
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.author == "trendsentinel_ops"


def test_author_url_is_derived_from_the_profile_username(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.author_url == "https://www.threads.net/@trendsentinel_ops"


def test_the_permalink_becomes_the_url(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.url == "https://www.threads.net/@trendsentinel_ops/post/first"


def test_a_media_post_without_a_permalink_stays_absent(media_post: dict[str, Any]) -> None:
    item = normalize_item(media_post, PROFILE_USERNAME)

    assert item is not None
    assert item.url is None
    assert item.text == "Second post, media only, no permalink yet."


def test_published_at_is_parsed_from_the_iso_timestamp(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.published_at is not None
    assert item.published_at.tzinfo is not None
    assert item.published_at.year == 2026
    assert item.published_at.day == 23


def test_a_missing_timestamp_leaves_published_at_absent(first_post: dict[str, Any]) -> None:
    payload = {**first_post, "timestamp": None}

    item = normalize_item(payload, PROFILE_USERNAME)

    assert item is not None
    assert item.published_at is None


# --------------------------------------------------------------------------- metrics


def test_present_metrics_use_the_shared_vocabulary(first_post: dict[str, Any]) -> None:
    payload = {**first_post, "likes": 12, "replies": 3, "views": 480}

    item = normalize_item(payload, PROFILE_USERNAME)

    assert item is not None
    assert item.metrics == {"likes": 12.0, "replies": 3.0, "views": 480.0}


def test_absent_metrics_stay_absent(first_post: dict[str, Any]) -> None:
    """Metrics are only returned when the fields are requested and granted; never zero-fill."""
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.metrics is None


def test_metadata_keeps_the_media_type(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.metadata["media_type"] == "TEXT_POST"


# --------------------------------------------------------------------------- thumbnail


def test_thumbnail_extracted_from_media_url(first_post: dict[str, Any]) -> None:
    """An image post carries ``media_url``; it lands in metadata as the thumbnail."""
    payload = {**first_post, "media_type": "IMAGE", "media_url": "https://cdn.instagram.com/x.jpg"}

    item = normalize_item(payload, PROFILE_USERNAME)

    assert item is not None
    assert item.metadata["thumbnail_url"] == "https://cdn.instagram.com/x.jpg"


def test_thumbnail_absent_for_a_text_post(first_post: dict[str, Any]) -> None:
    item = normalize_item(first_post, PROFILE_USERNAME)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


def test_thumbnail_refuses_a_non_https_media_url(first_post: dict[str, Any]) -> None:
    payload = {**first_post, "media_url": "http://cdn.instagram.com/x.jpg"}

    item = normalize_item(payload, PROFILE_USERNAME)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


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
        "collector_key": "threads",
        "threads_access_token": PLACEHOLDER_TOKEN,
        **overrides,
    }
    return SourceRef(base_url=THREADS_BASE, key="threads_profile", config=config)


async def collect(
    source: SourceRef, limit: int = 10, settings: Settings | None = None
) -> CollectionResult:
    """Run the collector through a shared fetcher, exactly as the service layer does."""
    async with make_fetcher(settings) as fetcher:
        return await ThreadsCollector(settings or build_settings(), fetcher).collect(source, limit)


@respx.mock
async def test_collect_reads_the_profile_and_both_pages() -> None:
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, content=page("me_profile.json"))
    )
    threads = respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        side_effect=[
            httpx.Response(200, content=page("threads_page1.json")),
            httpx.Response(200, content=page("threads_page2.json")),
        ]
    )

    result = await collect(make_source())

    assert result.ok
    assert [item.external_id for item in result.items] == [
        "17923800000000001",
        "17923800000000002",
        "17923800000000003",
    ]
    assert all(item.author == PROFILE_USERNAME for item in result.items)
    assert threads.call_count == 2
    # The cursor of page one must reach page two.
    assert "after=pagination-cursor-page-2" in str(threads.calls.last.request.url)


@respx.mock
async def test_the_access_token_stays_out_of_the_requested_url() -> None:
    """``paging.next`` embeds the token; rebuild the URL instead of following it verbatim."""
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, content=page("me_profile.json"))
    )
    threads = respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        side_effect=[
            httpx.Response(200, content=page("threads_page1.json")),
            httpx.Response(200, content=page("threads_page2.json")),
        ]
    )

    await collect(make_source())

    for call in threads.calls:
        assert "access_token" not in str(call.request.url)
        assert PLACEHOLDER_TOKEN not in str(call.request.url)


@respx.mock
async def test_the_limit_is_respected() -> None:
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, content=page("me_profile.json"))
    )
    respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        return_value=httpx.Response(200, content=page("threads_page1.json"))
    )

    result = await collect(make_source(), limit=1)

    assert len(result.items) == 1
    assert result.items[0].external_id == "17923800000000001"


@respx.mock
async def test_collect_rejects_without_a_token_and_sends_nothing() -> None:
    me = respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, content=page("me_profile.json"))
    )
    threads = respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        return_value=httpx.Response(200, content=page("threads_page2.json"))
    )

    result = await collect(make_source(threads_access_token=""))

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert "threads_access_token" in result.errors[0].message
    assert me.call_count == 0
    assert threads.call_count == 0


@respx.mock
async def test_an_expired_token_says_the_token_needs_rotation() -> None:
    """A long-lived token is rotated by hand (ADR-0004 case 3); the message must say so."""
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(return_value=httpx.Response(401))

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.REJECTED
    assert not result.errors[0].retryable
    assert "rotat" in result.errors[0].message


@respx.mock
async def test_a_profile_that_is_not_a_profile_is_invalid() -> None:
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_a_post_without_an_identity_is_reported_but_does_not_abort_the_run() -> None:
    payload = cast("dict[str, Any]", load("threads_page1.json"))
    payload["data"].append({"id": None, "text": "broken"})
    payload["paging"] = {}

    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(200, content=page("me_profile.json"))
    )
    respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        return_value=httpx.Response(200, json=payload)
    )

    result = await collect(make_source())

    assert not result.ok
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD
    assert [item.external_id for item in result.items] == [
        "17923800000000001",
        "17923800000000002",
    ]
