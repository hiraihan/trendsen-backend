"""Tests for the Hacker News collector.

Fixtures in ``tests/fixtures/hackernews/`` are captured verbatim from the live API, so these
tests encode the real response shapes, including the fields HN genuinely omits.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    ErrorKind,
    SourceRef,
)
from trendsentinel.collectors.hackernews import (
    HackerNewsCollector,
    comments_url_for,
    normalize_item,
)
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "hackernews"
BASE = "https://hacker-news.firebaseio.com/v0"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def topstories() -> list[int]:
    return cast(list[int], load("topstories.json"))


@pytest.fixture
def story_payload() -> dict[str, Any]:
    return cast(dict[str, Any], load("item_50175185.json"))


@pytest.fixture
def ask_payload() -> dict[str, Any]:
    return cast(dict[str, Any], load("item_ask_no_url.json"))


def make_settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


def make_fetcher(settings: Settings | None = None) -> HttpFetcher:
    """A fetcher with a stubbed resolver and no backoff, so tests stay offline and fast."""
    resolved = settings or make_settings()
    return HttpFetcher(
        resolved,
        policy=RetryPolicy(max_attempts=resolved.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    )


async def collect(settings: Settings, source_ref: SourceRef, limit: int) -> CollectionResult:
    """Run the collector through a shared fetcher, exactly as the service layer does."""
    async with make_fetcher(settings) as fetcher:
        collector = HackerNewsCollector(settings, fetcher)
        return await collector.collect(source_ref, limit)


def source(base: str = BASE) -> SourceRef:
    return SourceRef(base_url=base, key="hackernews", config={"collector_key": "hackernews"})


# --------------------------------------------------------------------------- normalization


def test_normalize_a_link_post(story_payload: dict[str, Any]) -> None:
    item = normalized(story_payload)

    assert item.external_id == "50175185"
    assert item.title == "In Hilbert space, all things are quantumly possible"
    assert item.url == (
        "https://www.quantamagazine.org/in-hilbert-space-all-things-are-quantumly-possible-20260826/"
    )
    assert item.author == "unprovable"
    # A link post has no body text in the API.
    assert item.text is None
    assert item.content_type == "link"
    assert item.author_url == "https://news.ycombinator.com/user?id=unprovable"


def test_published_at_is_utc_and_matches_the_epoch(story_payload: dict[str, Any]) -> None:
    item = normalized(story_payload)

    assert item.published_at is not None
    assert item.published_at.tzinfo is not None
    assert item.published_at.utcoffset() == timedelta_zero()
    assert item.published_at == datetime.fromtimestamp(story_payload["time"], tz=UTC)


def test_metrics_capture_score_and_comment_count(story_payload: dict[str, Any]) -> None:
    item = normalized(story_payload)

    assert item.metrics is not None
    assert item.metrics["score"] == story_payload["score"]
    assert item.metrics["comments"] == story_payload["descendants"]


def test_metadata_records_type_and_derived_comments_url(story_payload: dict[str, Any]) -> None:
    item = normalized(story_payload)

    assert item.metadata["type"] == "story"
    assert item.metadata["hackernews_id"] == 50175185
    assert item.metadata["comments_url"] == "https://news.ycombinator.com/item?id=50175185"


def test_ask_hn_has_no_url_but_does_have_text(ask_payload: dict[str, Any]) -> None:
    """Verified live: an Ask HN item is type `story`, has `text`, and never has `url`."""
    item = normalized(ask_payload)

    assert ask_payload["type"] == "story"
    assert "url" not in ask_payload
    assert item.url is None
    assert item.text is not None
    assert item.text
    assert item.content_type == "self"


def test_missing_url_becomes_none_not_an_empty_string() -> None:
    item = normalized({"id": 1, "type": "story", "title": "t", "by": "a"})

    assert item.url is None
    assert item.title == "t"


def test_missing_optional_fields_stay_none() -> None:
    item = normalized({"id": 7, "type": "story"})

    assert item.url is None
    assert item.title is None
    assert item.text is None
    assert item.author is None
    assert item.published_at is None
    assert item.metrics is None
    assert item.content_type is None
    assert item.author_url is None
    assert item.language is None
    assert item.thumbnail_url is None


def test_payload_without_an_id_has_no_identity() -> None:
    assert normalize_item({"type": "story", "title": "orphan"}) is None


@pytest.mark.parametrize("time_value", [0, -5, None, "yesterday", True])
def test_unusable_timestamps_become_none(time_value: object) -> None:
    item = normalize_item({"id": 1, "type": "story", "time": time_value})

    assert item is not None
    assert item.published_at is None


def test_metrics_keep_only_present_values() -> None:
    with_score = normalized({"id": 1, "type": "story", "score": 10})
    with_comments = normalized({"id": 2, "type": "story", "descendants": 4})
    with_neither = normalized({"id": 3, "type": "story"})

    assert with_score.metrics == {"score": 10}
    assert with_comments.metrics == {"comments": 4}
    assert with_neither.metrics is None


def test_comments_url_is_derived_from_the_id() -> None:
    assert comments_url_for(42) == "https://news.ycombinator.com/item?id=42"
    assert comments_url_for("42") == "https://news.ycombinator.com/item?id=42"


def timedelta_zero() -> timedelta:
    """The zero offset a UTC datetime is expected to report."""
    return timedelta(0)


def normalized(payload: dict[str, Any]) -> CollectedItem:
    """Normalize a payload, asserting it produced an item."""
    item = normalize_item(payload)
    assert item is not None
    return item.normalized()


# --------------------------------------------------------------------------- collection


@respx.mock
async def test_collect_returns_normalized_items(
    topstories: list[int], story_payload: dict[str, Any]
) -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=topstories[:3]))
    for item_id in topstories[:3]:
        respx.get(f"{BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(
                200, json={"id": item_id, "type": "story", "title": f"t{item_id}"}
            )
        )

    result = await collect(make_settings(), source(), limit=3)

    assert result.ok
    assert len(result.items) == 3
    assert [item.external_id for item in result.items] == [str(i) for i in topstories[:3]]


@respx.mock
async def test_limit_bounds_the_number_of_items_fetched(topstories: list[int]) -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=topstories))
    for item_id in topstories[:2]:
        respx.get(f"{BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json={"id": item_id, "type": "story", "title": "t"})
        )

    result = await collect(make_settings(), source(), limit=2)

    assert len(result.items) == 2


@respx.mock
async def test_limit_is_clamped_to_max_items_per_run(topstories: list[int]) -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=topstories))
    for item_id in topstories[:2]:
        respx.get(f"{BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json={"id": item_id, "type": "story", "title": "t"})
        )

    # Ask for far more than the configured ceiling; the ceiling must win.
    result = await collect(make_settings(max_items_per_run=2), source(), limit=50)

    assert len(result.items) == 2


@respx.mock
async def test_zero_limit_fetches_nothing() -> None:
    route = respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1]))

    result = await collect(make_settings(), source(), limit=0)

    assert result.items == []
    assert result.errors == []
    assert route.call_count == 0


@respx.mock
async def test_deleted_item_is_skipped_silently() -> None:
    """A JSON `null` is a deleted item: expected, and not worth reporting as an error."""
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1, 2]))
    # `resp.json()` yields an empty body, so the literal `null` body is returned explicitly.
    respx.get(f"{BASE}/item/1.json").mock(
        return_value=httpx.Response(
            200, content=b"null", headers={"content-type": "application/json"}
        )
    )
    respx.get(f"{BASE}/item/2.json").mock(
        return_value=httpx.Response(200, json={"id": 2, "type": "story", "title": "kept"})
    )

    result = await collect(make_settings(), source(), limit=2)

    assert result.errors == []
    assert [item.external_id for item in result.items] == ["2"]


@respx.mock
async def test_poll_and_job_items_are_skipped() -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1, 2]))
    respx.get(f"{BASE}/item/1.json").mock(
        return_value=httpx.Response(200, json={"id": 1, "type": "poll", "title": "poll"})
    )
    respx.get(f"{BASE}/item/2.json").mock(
        return_value=httpx.Response(200, json={"id": 2, "type": "job", "title": "job"})
    )

    result = await collect(make_settings(), source(), limit=2)

    assert result.items == []
    assert result.errors == []


@respx.mock
async def test_one_failed_item_does_not_lose_the_others() -> None:
    """Failure isolation at the item level: the run is partial, not empty."""
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1, 2, 3]))
    respx.get(f"{BASE}/item/1.json").mock(
        return_value=httpx.Response(200, json={"id": 1, "type": "story", "title": "a"})
    )
    respx.get(f"{BASE}/item/2.json").mock(return_value=httpx.Response(404))
    respx.get(f"{BASE}/item/3.json").mock(
        return_value=httpx.Response(200, json={"id": 3, "type": "story", "title": "c"})
    )

    result = await collect(make_settings(), source(), limit=3)

    assert [item.external_id for item in result.items] == ["1", "3"]
    assert len(result.errors) == 1
    assert result.errors[0].kind is ErrorKind.HTTP_STATUS


@respx.mock
async def test_topstories_failure_returns_an_error_and_no_items() -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(503))

    result = await collect(make_settings(), source(), limit=5)

    assert result.items == []
    assert len(result.errors) == 1
    assert result.errors[0].retryable is True


@respx.mock
async def test_topstories_returning_a_non_array_is_invalid_payload() -> None:
    respx.get(f"{BASE}/topstories.json").mock(
        return_value=httpx.Response(200, json={"error": "not a list"})
    )

    result = await collect(make_settings(), source(), limit=5)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_item_returning_a_non_object_is_invalid_payload() -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[1]))
    respx.get(f"{BASE}/item/1.json").mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    result = await collect(make_settings(), source(), limit=1)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_non_integer_ids_in_the_list_are_ignored() -> None:
    respx.get(f"{BASE}/topstories.json").mock(
        return_value=httpx.Response(200, json=[1, "two", None, True, 2])
    )
    respx.get(f"{BASE}/item/1.json").mock(
        return_value=httpx.Response(200, json={"id": 1, "type": "story", "title": "a"})
    )
    respx.get(f"{BASE}/item/2.json").mock(
        return_value=httpx.Response(200, json={"id": 2, "type": "story", "title": "b"})
    )

    result = await collect(make_settings(), source(), limit=10)

    assert [item.external_id for item in result.items] == ["1", "2"]


@respx.mock
async def test_trailing_slash_in_base_url_is_tolerated() -> None:
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[]))

    result = await collect(make_settings(), source(f"{BASE}/"), limit=1)

    assert result.items == []


@respx.mock
async def test_collector_uses_a_database_configured_base_url() -> None:
    """A database-configured base URL lets a mirror be swapped in without code changes."""
    mirror = "https://hn-mirror.example.test/v0"
    respx.get(f"{mirror}/topstories.json").mock(return_value=httpx.Response(200, json=[]))

    result = await collect(make_settings(), source(mirror), limit=1)

    assert result.ok


@respx.mock
async def test_concurrency_is_bounded_by_settings() -> None:
    ids = list(range(1, 21))
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=ids))
    for item_id in ids:
        respx.get(f"{BASE}/item/{item_id}.json").mock(
            return_value=httpx.Response(200, json={"id": item_id, "type": "story", "title": "t"})
        )

    # A concurrency of 1 must still complete; the bound cannot deadlock the run.
    result = await collect(
        make_settings(max_concurrency=1, max_items_per_run=20), source(), limit=20
    )

    assert len(result.items) == 20


@respx.mock
async def test_error_kind_is_timeout_when_the_source_times_out() -> None:
    respx.get(f"{BASE}/topstories.json").mock(side_effect=httpx.ReadTimeout("slow"))

    result = await collect(make_settings(http_max_retries=1), source(), limit=1)

    assert result.errors[0].kind is ErrorKind.TIMEOUT


def test_collector_declares_its_key() -> None:
    assert HackerNewsCollector(make_settings(), make_fetcher()).key == "hackernews"


@respx.mock
async def test_an_injected_fetcher_is_not_closed() -> None:
    """The service layer owns the fetcher's lifetime; a collector must not close it."""
    respx.get(f"{BASE}/topstories.json").mock(return_value=httpx.Response(200, json=[]))

    fetcher = make_fetcher()
    async with fetcher:
        collector = HackerNewsCollector(make_settings(), fetcher)
        await collector.collect(source(), limit=1)

        assert fetcher._client is not None
        assert not fetcher._client.is_closed


def test_error_serialization_is_json_safe() -> None:
    error = CollectError(ErrorKind.HTTP_STATUS, "boom", retryable=True)

    assert json.dumps(error.as_dict())
