"""Tests for the Wikimedia pageviews collector.

Pageviews are time-series observations: each ``(article, day)`` window becomes its own
``CollectedItem`` whose ``captured_at`` is the day-window start, so the per-item snapshot
rows deduplicate on ``(item_id, captured_at)`` and a re-run of the same window is
idempotent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import CollectionResult, ErrorKind, SourceRef
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.wikimedia import (
    WikimediaCollector,
    normalize_payload,
    parse_wikimedia_timestamp,
)
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "wikimedia"
BASE_URL = "https://wikimedia.org/api/rest_v1/"


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


def public_resolver(_hostname: str) -> list[str]:
    return ["208.80.154.224"]


def make_fetcher(settings: Settings | None = None) -> HttpFetcher:
    resolved = settings or make_settings()
    return HttpFetcher(
        resolved,
        allowed_domains=["wikimedia.org"],
        policy=RetryPolicy(max_attempts=resolved.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    )


def source(**config: Any) -> SourceRef:
    return SourceRef(
        base_url=BASE_URL,
        key="wikimedia",
        config={"collector_key": "wikimedia", **config},
    )


async def collect(settings: Settings, src: SourceRef, limit: int) -> CollectionResult:
    async with make_fetcher(settings) as fetcher:
        collector = WikimediaCollector(settings, fetcher)
        return await collector.collect(src, limit)


# --------------------------------------------------------------------------- parsing


def test_parse_wikimedia_timestamp_parses_utc_midnight() -> None:
    assert parse_wikimedia_timestamp("2026102000") == datetime(
        2026, 10, 20, tzinfo=UTC
    )


def test_parse_wikimedia_timestamp_rejects_malformed_values() -> None:
    for bad in ("", "2026102", "2026130100", "not-a-date", 2026102000):
        assert parse_wikimedia_timestamp(bad) is None


def test_normalize_payload_makes_one_observation_per_day() -> None:
    payload = {
        "items": [
            {
                "project": "en.wikipedia",
                "article": "JavaScript",
                "granularity": "daily",
                "timestamp": "2026102000",
                "access": "all-access",
                "agent": "user",
                "views": 1524,
            },
            {
                "project": "en.wikipedia",
                "article": "JavaScript",
                "granularity": "daily",
                "timestamp": "2026102100",
                "access": "all-access",
                "agent": "user",
                "views": 1868,
            },
        ]
    }

    result = normalize_payload(payload, source_url=BASE_URL)

    assert result.ok
    assert len(result.items) == 2

    first = result.items[0]
    # The article identity is the canonical article URL, shared across days.
    assert first.external_id == "https://en.wikipedia.org/wiki/JavaScript"
    assert first.url == first.external_id
    assert first.title == "JavaScript"
    assert first.content_type == "page"
    assert first.published_at is None
    assert first.captured_at == datetime(2026, 10, 20, tzinfo=UTC)
    assert first.metrics == {"views": 1524.0}

    second = result.items[1]
    assert second.external_id == first.external_id
    assert second.captured_at == datetime(2026, 10, 21, tzinfo=UTC)


def test_normalize_payload_records_window_metadata() -> None:
    result = normalize_payload(
        {
            "items": [
                {
                    "project": "en.wikipedia",
                    "article": "Python (programming language)",
                    "timestamp": "2026102000",
                    "access": "all-access",
                    "agent": "user",
                    "views": 100,
                }
            ]
        },
        source_url=BASE_URL,
    )

    item = result.items[0]
    assert item.external_id == (
        "https://en.wikipedia.org/wiki/Python_(programming_language)"
    )
    meta = item.metadata
    assert meta["article"] == "Python (programming language)"
    assert meta["window_start"] == "2026-10-20T00:00:00+00:00"
    assert meta["window_end"] == "2026-10-21T00:00:00+00:00"


def test_normalize_payload_reports_bad_observations() -> None:
    result = normalize_payload(
        {
            "items": [
                {"project": "en.wikipedia", "timestamp": "2026102000", "views": 1},
                {"article": "X", "timestamp": "not-a-date", "views": 1},
                {"article": "Y", "timestamp": "2026102000", "views": "nan"},
                "not an object",
            ]
        },
        source_url=BASE_URL,
    )

    assert result.items == []
    # Three malformed observations each produce a recorded error.
    assert len(result.errors) == 4


def test_normalize_payload_rejects_missing_items_list() -> None:
    result = normalize_payload({"foo": 1}, source_url=BASE_URL)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


# --------------------------------------------------------------------------- collection


@respx.mock
async def test_collect_fetches_observed_articles() -> None:
    respx.get(f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027").mock(
        return_value=httpx.Response(200, content=fixture_bytes("pageviews_js.json"))
    )

    src = source(articles=["JavaScript"], days=7, project="en.wikipedia")
    result = await collect(make_settings(), src, limit=200)

    assert result.ok
    assert len(result.items) == 2
    # Both observations share the canonical article URL (the item identity).
    assert result.items[0].external_id == "https://en.wikipedia.org/wiki/JavaScript"
    assert result.items[0].captured_at == datetime(2026, 10, 21, tzinfo=UTC)


@respx.mock
async def test_collect_percent_encodes_article_titles() -> None:
    route = respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/"
        "Python_(programming_language)/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(200, content=b'{"items":[]}'))

    src = source(articles=["Python (programming language)"], days=7)
    await collect(make_settings(), src, limit=200)

    assert route.call_count == 1


@respx.mock
async def test_collect_reports_404_no_data_as_error_and_keeps_others() -> None:
    respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(404, content=b'{"detail":"not found"}'))
    respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/Python_(programming_language)/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(200, content=fixture_bytes("pageviews_js.json")))

    src = source(articles=["JavaScript", "Python (programming language)"], days=7)
    result = await collect(make_settings(), src, limit=200)

    # The 404 (no data) is recorded as an error; the other article still yields items.
    assert len(result.items) == 2
    assert len(result.errors) == 1
    assert result.errors[0].kind is ErrorKind.HTTP_STATUS

    # A 404 is not retryable.
    assert result.errors[0].retryable is False


@respx.mock
async def test_http_failure_is_reported_as_retryable() -> None:
    respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(503))

    src = source(articles=["JavaScript"], days=7)
    result = await collect(make_settings(http_max_retries=1), src, limit=200)

    assert result.items == []
    assert len(result.errors) == 1
    assert result.errors[0].retryable is True


@respx.mock
async def test_rate_limit_is_reported() -> None:
    respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(429))

    src = source(articles=["JavaScript"], days=7)
    result = await collect(make_settings(http_max_retries=1), src, limit=200)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.RATE_LIMITED
    assert result.errors[0].retryable is True


@respx.mock
async def test_invalid_json_response_is_an_invalid_payload() -> None:
    respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(200, content=b"<html>not json</html>"))

    src = source(articles=["JavaScript"], days=7)
    result = await collect(make_settings(), src, limit=200)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_no_articles_configured_is_reported() -> None:
    src = source()

    result = await collect(make_settings(), src, limit=200)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD
    assert "articles" in result.errors[0].message


@respx.mock
async def test_zero_limit_fetches_nothing() -> None:
    route = respx.get(
        f"{BASE_URL}metrics/pageviews/per-article/en.wikipedia/all-access/user/JavaScript/daily/20261021/20261027"
    ).mock(return_value=httpx.Response(200, content=fixture_bytes("pageviews_js.json")))

    src = source(articles=["JavaScript"], days=7)
    result = await collect(make_settings(), src, limit=0)

    assert result.items == []
    assert route.call_count == 0


def test_source_without_base_url_is_rejected() -> None:
    src = SourceRef(base_url="   ", key="wikimedia", config={"collector_key": "wikimedia"})

    # Unit-level: exercise collect via a direct call with a fetcher context.
    import asyncio

    async def run() -> CollectionResult:
        async with make_fetcher() as fetcher:
            collector = WikimediaCollector(make_settings(), fetcher)
            return await collector.collect(src, 10)

    result = asyncio.run(run())

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.REJECTED


def test_collector_declares_its_key() -> None:
    collector = WikimediaCollector(make_settings(), make_fetcher())

    assert collector.key == "wikimedia"
