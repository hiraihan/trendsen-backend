"""Tests for the RSS/Atom collector.

Fixtures in ``tests/fixtures/rss/`` are captured verbatim from the live feeds, so these tests
encode the real document shapes. The most important assertion in this file is the identity
one: for ``hnrss.org`` the ``guid`` is the Hacker News item, while ``<link>`` is the external
article. Getting that backwards would silently duplicate items on every run.
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
from trendsentinel.collectors.rss import (
    RssCollector,
    normalize_entry,
    parse_hnrss_metrics,
    strip_html,
)
from trendsentinel.config import Settings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "rss"
HNRSS_URL = "https://hnrss.org/frontpage"
GITHUB_URL = "https://github.blog/feed/"

#: The live hnrss feed's first entry, captured 2026-10-21.
HNRSS_GUID = "https://news.ycombinator.com/item?id=50175185"
HNRSS_ARTICLE = (
    "https://www.quantamagazine.org/in-hilbert-space-all-things-are-quantumly-possible-20260826/"
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


def make_fetcher(settings: Settings | None = None) -> HttpFetcher:
    resolved = settings or make_settings()
    return HttpFetcher(
        resolved,
        policy=RetryPolicy(max_attempts=resolved.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    )


async def collect(settings: Settings, url: str, limit: int, **config: Any) -> CollectionResult:
    """Run the RSS collector through a shared fetcher, exactly as the service layer does."""
    async with make_fetcher(settings) as fetcher:
        collector = RssCollector(settings, fetcher)
        source = SourceRef(base_url=url, key="rss", config={"collector_key": "rss", **config})
        return await collector.collect(source, limit)


def parsed_entries(name: str) -> list[dict[str, Any]]:
    """Return the feedparser entries for a fixture, so normalization can be tested directly."""
    import feedparser

    return list(feedparser.parse(fixture_bytes(name)).entries)


# --------------------------------------------------------------------------- html helpers


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        ("<p>hello <b>world</b></p>", "hello world"),
        ("<p>Article URL: <a href='x'>link</a></p>", "Article URL: link"),
        ("plain text", "plain text"),
        ("", ""),
        ("<p>a</p>\n<p>b</p>", "a b"),
        ("<p>  spaced   out  </p>", "spaced out"),
    ],
)
def test_strip_html(html: str, expected: str) -> None:
    assert strip_html(html) == expected


def test_strip_html_collapses_runs_of_whitespace() -> None:
    assert strip_html("<p>a</p>\n\n\n<p>b</p>") == "a b"


# --------------------------------------------------------------------------- hn metrics


def test_hnrss_metrics_are_parsed_from_the_description() -> None:
    description = (
        "<p>Article URL: <a href='x'>x</a></p>"
        "<p>Comments URL: <a href='y'>y</a></p>"
        "<p>Points: 605</p>"
        "<p># Comments: 308</p>"
    )

    assert parse_hnrss_metrics(description) == {"score": 605.0, "comments": 308.0}


def test_hnrss_metrics_are_absent_when_the_description_has_none() -> None:
    assert parse_hnrss_metrics("<p>Some feed with no numbers</p>") is None


@pytest.mark.parametrize(
    "description",
    [
        "<p>Points: 10</p>",  # no comment count
        "<p># Comments: 3</p>",  # no score
        "",
    ],
)
def test_a_partial_metric_parse_yields_nothing(description: str) -> None:
    """A wrong metric is worse than a missing one, so both numbers are required."""
    assert parse_hnrss_metrics(description) is None


def test_metric_parsing_tolerates_formatting_variation() -> None:
    assert parse_hnrss_metrics("<p>points: 12</p><p>#comments: 4</p>") == {
        "score": 12.0,
        "comments": 4.0,
    }


# --------------------------------------------------------------------------- identity


def test_hnrss_identity_is_the_guid_not_the_article_link() -> None:
    """The single most consequential assertion in this file."""
    literal = [e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID]
    assert literal, "fixture no longer contains the captured entry"

    item = normalize_entry(literal[0], max_text_chars=2000)

    assert item is not None
    assert item.external_id == HNRSS_GUID
    assert item.external_id != HNRSS_ARTICLE
    # The article URL is still stored, as the item's url.
    assert item.url == HNRSS_ARTICLE


def test_github_identity_is_the_permalink_guid() -> None:
    entry = parsed_entries("github_blog.xml")[0]

    item = normalize_entry(entry, max_text_chars=2000)

    assert item is not None
    assert item.external_id == entry["guid"]
    assert item.external_id.startswith("https://github.blog/")


def test_identity_falls_back_to_the_link_when_no_guid_or_id() -> None:
    entry = {
        "title": "link only",
        "link": "https://partial.example.test/link-only",
        "summary": "x",
    }

    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.external_id == "https://partial.example.test/link-only"


def test_identity_falls_back_to_the_discussion_url_before_the_link() -> None:
    """When a feed exposes `comments` but no guid, the discussion URL is the stabler identity."""
    entry = {
        "title": "t",
        "link": "https://evil.example.test/article",
        "comments": "https://news.ycombinator.com/item?id=999",
    }

    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.external_id == "https://news.ycombinator.com/item?id=999"


@pytest.mark.parametrize(
    "entry",
    [
        {"title": "no identity at all", "summary": "x"},
        {"title": "blank guid", "guid": "   "},
        {"title": "blank link", "link": ""},
        {"summary": "nothing usable"},
    ],
)
def test_entries_without_identity_are_rejected(entry: dict[str, Any]) -> None:
    """An unidentifiable entry could not be deduplicated, so it must not be stored."""
    assert normalize_entry(entry, max_text_chars=100) is None


# --------------------------------------------------------------------------- normalization


def test_github_entry_uses_content_encoded_for_text() -> None:
    entry = parsed_entries("github_blog.xml")[0]

    item = normalize_entry(entry, max_text_chars=2000)

    assert item is not None
    assert item.text is not None
    # content:encoded is long HTML; the stored text is the stripped, truncated version.
    assert len(item.text) <= 2001
    assert "<p" not in item.text


def test_hnrss_entry_metadata_and_metrics() -> None:
    entry = next(e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID)

    item = normalize_entry(entry, max_text_chars=2000, feed_title="Hacker News: Front Page")

    assert item is not None
    assert item.metadata["feed_title"] == "Hacker News: Front Page"
    # The raw description is retained so a derived metric stays auditable.
    assert item.metadata["raw_description"] is not None
    assert "Points:" in item.metadata["raw_description"]
    assert item.metrics is not None
    assert set(item.metrics) == {"score", "comments"}


def test_hnrss_stored_text_drops_the_boilerplate() -> None:
    """The field list is metadata, not content: the stored text carries prose only.

    An hnrss description is ``Article URL / Comments URL / Points / # Comments`` — the
    same numbers the metrics vocabulary already stores — so the excerpt would repeat
    the metric chips as a sentence.
    """
    entry = next(
        e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID
    )

    item = normalize_entry(entry, max_text_chars=2000)

    assert item is not None
    if item.text is not None:
        assert "Article URL:" not in item.text
        assert "Comments URL:" not in item.text
        assert "Points:" not in item.text


def test_non_hnrss_text_passes_through_untouched() -> None:
    """A feed entry without the hnrss field list is never rewritten."""
    prose = "A perfectly ordinary sentence about databases. Points of view differ."
    entry = {"guid": "urn:prose", "title": "t", "summary": prose}

    item = normalize_entry(entry, max_text_chars=500)

    assert item is not None
    assert item.text is not None
    assert "Points of view" in item.text


def test_hnrss_text_keeps_prose_before_the_field_list() -> None:
    """Ask-HN style text leads with real prose; only the field list is dropped."""
    entry = {
        "guid": "urn:ask",
        "title": "t",
        "summary": (
            "Ask HN: what are you working on? "
            "Article URL: https://x.test/a Points: 5 # Comments: 2"
        ),
    }

    item = normalize_entry(entry, max_text_chars=500)

    assert item is not None
    assert item.text == "Ask HN: what are you working on?"


def test_hnrss_text_drops_a_leading_bare_article_url() -> None:
    """Some entries lead with the bare article URL before the ``Comments URL:`` field."""
    entry = {
        "guid": "urn:bare",
        "title": "t",
        "summary": (
            "https://hackios-web.vercel.app/en Comments URL: "
            "https://news.ycombinator.com/item?id=50196718 Points: 442 # Comments: 15"
        ),
    }

    item = normalize_entry(entry, max_text_chars=500)

    assert item is not None
    assert item.text is None


def test_author_is_captured() -> None:
    entry = next(e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID)

    item = normalize_entry(entry, max_text_chars=2000)

    assert item is not None
    assert item.author == "unprovable"


def test_published_at_is_utc_aware() -> None:
    entry = next(e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID)

    item = normalize_entry(entry, max_text_chars=2000)

    assert item is not None
    assert item.published_at is not None
    assert item.published_at.tzinfo is not None
    assert item.published_at == datetime(2026, 10, 21, 2, 4, 15, tzinfo=UTC)


def test_missing_publication_date_leaves_the_field_none() -> None:
    entry = {"guid": "urn:1", "title": "no date", "summary": "x"}

    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.published_at is None


def test_updated_parsed_is_used_when_published_is_missing() -> None:
    import time

    entry = {
        "guid": "urn:2",
        "title": "updated only",
        "summary": "x",
        "updated_parsed": time.struct_time((2026, 5, 1, 6, 30, 0, 0, 0, 0)),
    }

    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.published_at == datetime(2026, 5, 1, 6, 30, tzinfo=UTC)


def test_text_is_truncated_to_the_configured_length() -> None:
    entry = {"guid": "urn:3", "title": "long", "summary": "word " * 1000}

    item = normalize_entry(entry, max_text_chars=50)

    assert item is not None
    assert item.text is not None
    assert len(item.text) <= 51
    assert item.text.endswith("…")


def test_string_summary_is_used_when_content_is_absent() -> None:
    entry = {"guid": "urn:4", "title": "t", "summary": "<p>from summary</p>"}

    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.text == "from summary"


def test_feed_title_is_null_when_not_provided() -> None:
    item = normalize_entry({"guid": "urn:5", "title": "t"}, max_text_chars=100)

    assert item is not None
    assert item.metadata["feed_title"] is None


# --------------------------------------------------------------------------- thumbnail


def test_thumbnail_extracted_from_entry_html() -> None:
    """A feed entry whose HTML carries an <img> exposes its first https src."""
    entry = {
        "guid": "urn:img",
        "title": "with image",
        "summary": '<p>lead</p><img src="https://github.blog/cover.png"><p>tail</p>',
    }
    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.metadata["thumbnail_url"] == "https://github.blog/cover.png"


def test_thumbnail_null_when_entry_has_no_image() -> None:
    """No fabricated placeholder: an imageless entry keeps ``None``."""
    item = normalize_entry({"guid": "urn:5", "title": "t"}, max_text_chars=100)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


def test_thumbnail_refuses_non_https_img() -> None:
    entry = {
        "guid": "urn:img-http",
        "title": "insecure image",
        "summary": '<img src="http://github.blog/cover.png">',
    }
    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.metadata["thumbnail_url"] is None


def test_rss_entry_classifies_as_article_with_optional_metadata() -> None:
    entry = {
        "guid": "urn:9",
        "title": "t",
        "link": "https://example.test/1",
        "author": "a",
        "language": "en-us",
    }
    item = normalize_entry(entry, max_text_chars=100)

    assert item is not None
    assert item.content_type == "article"
    assert item.language == "en-us"
    # RSS authors carry no profile URL.
    assert item.author_url is None
    assert item.thumbnail_url is None


def test_feed_language_is_used_when_entry_declares_none() -> None:
    entry = {"guid": "urn:10", "title": "t"}

    item = normalize_entry(entry, max_text_chars=100, feed_language="en")
    assert item is not None
    assert item.language == "en"

    no_lang = normalize_entry(entry, max_text_chars=100)
    assert no_lang is not None
    assert no_lang.language is None


# --------------------------------------------------------------------------- collection


@respx.mock
async def test_collect_hnrss_feed() -> None:
    respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("hnrss_frontpage.xml"))
    )

    result = await collect(make_settings(), HNRSS_URL, limit=30, min_request_delay_ms=0)

    assert result.ok
    assert len(result.items) == 20
    assert all(item.external_id for item in result.items)
    # Every hnrss identity is a Hacker News item, never an external article URL.
    assert all("news.ycombinator.com/item?id=" in item.external_id for item in result.items)


@respx.mock
async def test_collect_github_feed() -> None:
    respx.get(GITHUB_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("github_blog.xml"))
    )

    result = await collect(make_settings(), GITHUB_URL, limit=30, min_request_delay_ms=0)

    assert result.ok
    assert len(result.items) == 3
    assert all(item.author for item in result.items)


@respx.mock
async def test_limit_is_respected() -> None:
    respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("hnrss_frontpage.xml"))
    )

    result = await collect(make_settings(), HNRSS_URL, limit=5, min_request_delay_ms=0)

    assert len(result.items) == 5


@respx.mock
async def test_limit_is_clamped_to_max_items_per_run() -> None:
    respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("hnrss_frontpage.xml"))
    )

    result = await collect(
        make_settings(max_items_per_run=3), HNRSS_URL, limit=100, min_request_delay_ms=0
    )

    assert len(result.items) == 3


@respx.mock
async def test_entries_without_identity_are_reported_and_others_kept() -> None:
    respx.get("https://partial.example.test/feed.xml").mock(
        return_value=httpx.Response(200, content=fixture_bytes("partial_identity.xml"))
    )

    result = await collect(
        make_settings(), "https://partial.example.test/feed.xml", limit=10, min_request_delay_ms=0
    )

    ids = {item.external_id for item in result.items}
    assert "urn:partial:1" in ids
    assert "https://partial.example.test/link-only" in ids
    assert "urn:partial:no-date" in ids

    # The single unidentifiable entry is reported rather than stored.
    assert len(result.errors) == 1
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD
    assert "no usable identity" in result.errors[0].message


@respx.mock
async def test_malformed_feed_with_no_entries_is_an_invalid_payload() -> None:
    respx.get("https://broken.example.test/feed.xml").mock(
        return_value=httpx.Response(200, content=fixture_bytes("bozo.xml"))
    )

    result = await collect(
        make_settings(), "https://broken.example.test/feed.xml", limit=10, min_request_delay_ms=0
    )

    assert result.items == []
    assert len(result.errors) == 1
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD


@respx.mock
async def test_valid_feed_with_no_entries_is_reported() -> None:
    empty = b'<?xml version="1.0"?><rss version="2.0"><channel><title>Empty</title></channel></rss>'
    respx.get("https://empty.example.test/feed.xml").mock(
        return_value=httpx.Response(200, content=empty)
    )

    result = await collect(
        make_settings(), "https://empty.example.test/feed.xml", limit=10, min_request_delay_ms=0
    )

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.INVALID_PAYLOAD
    assert "no entries" in result.errors[0].message


@respx.mock
async def test_http_failure_is_reported() -> None:
    respx.get(HNRSS_URL).mock(return_value=httpx.Response(503))

    result = await collect(
        make_settings(http_max_retries=1), HNRSS_URL, limit=10, min_request_delay_ms=0
    )

    assert result.items == []
    assert len(result.errors) == 1
    assert result.errors[0].retryable is True


@respx.mock
async def test_zero_limit_fetches_nothing() -> None:
    route = respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("hnrss_frontpage.xml"))
    )

    result = await collect(make_settings(), HNRSS_URL, limit=0, min_request_delay_ms=0)

    assert result.items == []
    assert route.call_count == 0


@respx.mock
async def test_a_source_without_a_base_url_is_rejected() -> None:
    result = await collect(make_settings(), "   ", limit=10, min_request_delay_ms=0)

    assert result.items == []
    assert result.errors[0].kind is ErrorKind.REJECTED


@respx.mock
async def test_request_delay_is_honoured() -> None:
    """Third-party feeds get a gentler cadence; the delay is configuration, not hardcoded."""
    import time

    respx.get(HNRSS_URL).mock(
        return_value=httpx.Response(200, content=fixture_bytes("hnrss_frontpage.xml"))
    )

    started = time.monotonic()
    await collect(make_settings(), HNRSS_URL, limit=1, min_request_delay_ms=150)
    elapsed = time.monotonic() - started

    assert elapsed >= 0.15


def test_collector_declares_its_key() -> None:
    collector = RssCollector(make_settings(), make_fetcher())

    assert collector.key == "rss"


def test_identity_schemes_are_scoped_per_source() -> None:
    """Two feeds may legitimately use different identity schemes for the same content.

    The database's unique constraint is `(source_id, external_id)`, so scoping is by source
    and a collision between feeds is impossible by construction.
    """
    hnrss_entry = next(
        e for e in parsed_entries("hnrss_frontpage.xml") if e.get("guid") == HNRSS_GUID
    )
    github_entry = parsed_entries("github_blog.xml")[0]

    a = normalize_entry(hnrss_entry, max_text_chars=100)
    b = normalize_entry(github_entry, max_text_chars=100)

    assert a is not None
    assert b is not None
    assert a.external_id != b.external_id
