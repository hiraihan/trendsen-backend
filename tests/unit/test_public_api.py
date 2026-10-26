"""Unit tests for the public (no-credential) JSON adapter and its allowlist gate."""

from __future__ import annotations

from datetime import UTC, datetime

from trendsentinel.collectors.public_api import normalize_item, parse_items
from trendsentinel.collectors.ssrf import host_is_allowlisted

ISO = "2026-10-01T12:00:00Z"


# --------------------------------------------------------------------------- payload shape

def test_parse_items_accepts_list() -> None:
    items = parse_items([{"id": "1"}, {"id": "2"}])
    assert [i["id"] for i in items] == ["1", "2"]


def test_parse_items_accepts_object_under_conventional_key() -> None:
    items = parse_items({"items": [{"id": "1"}], "meta": {"page": 1}})
    assert [i["id"] for i in items] == ["1"]


def test_parse_items_treats_single_object_as_one_item() -> None:
    items = parse_items({"id": "1", "title": "Only"})
    assert len(items) == 1
    assert items[0]["id"] == "1"


def test_parse_items_ignores_non_mapping_entries() -> None:
    items = parse_items([{"id": "1"}, "junk", None])
    assert len(items) == 1


def test_parse_items_returns_empty_for_garbage() -> None:
    assert parse_items("nope") == []
    assert parse_items(42) == []
    assert parse_items(None) == []


# --------------------------------------------------------------------------- normalization

def test_normalize_item_prefers_id_and_carries_fields() -> None:
    item = normalize_item(
        {
            "id": "a1",
            "url": "https://x.test/1",
            "title": "T",
            "text": "body",
            "author": "alice",
            "published_at": ISO,
            "metrics": {"score": 10},
        },
        base_url="https://x.test",
        max_text_chars=100,
    )
    assert item is not None
    assert item.external_id == "a1"
    assert item.url == "https://x.test/1"
    assert item.title == "T"
    assert item.text == "body"
    assert item.author == "alice"
    assert item.published_at == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert item.metrics == {"score": 10.0}


def test_normalize_item_falls_back_to_url_title_text_for_identity() -> None:
    assert (
        normalize_item({"url": "https://x.test"}, base_url="https://x.test", max_text_chars=10)
        is not None
    )
    assert (
        normalize_item({"title": "Only title"}, base_url="https://x.test", max_text_chars=10)
        is not None
    )
    assert (
        normalize_item({"text": "Only body"}, base_url="https://x.test", max_text_chars=10)
        is not None
    )


def test_normalize_item_returns_none_when_no_identity() -> None:
    assert normalize_item({"author": "anon"}, base_url="https://x.test", max_text_chars=10) is None


def test_normalize_item_truncates_text() -> None:
    item = normalize_item(
        {"id": "1", "text": "x" * 50},
        base_url="https://x.test",
        max_text_chars=10,
    )
    assert item is not None
    assert len(item.text or "") <= 11
    assert (item.text or "").endswith("…")


def test_normalize_item_drops_non_numeric_metrics() -> None:
    item = normalize_item(
        {"id": "1", "metrics": {"score": 5, "bad": "nope", "flag": True}},
        base_url="https://x.test",
        max_text_chars=10,
    )
    assert item is not None
    assert item.metrics == {"score": 5.0}


def test_normalize_item_naive_datetime_is_treated_as_utc() -> None:
    item = normalize_item(
        {"id": "1", "published_at": "2026-10-01T12:00:00"},
        base_url="https://x.test",
        max_text_chars=10,
    )
    assert item is not None
    assert item.published_at == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- allowlist gate

def test_host_is_allowlisted_exact_and_subdomain() -> None:
    assert host_is_allowlisted("example.com", ["example.com"])
    assert host_is_allowlisted("blog.example.com", ["example.com"])
    assert host_is_allowlisted("api.example.com", ["example.com"])


def test_host_is_allowlisted_rejects_non_matching() -> None:
    assert not host_is_allowlisted("evil-example.com", ["example.com"])
    assert not host_is_allowlisted("notexample.com", ["example.com"])
    assert not host_is_allowlisted("example.org", ["example.com"])


def test_host_is_allowlisted_empty_list_allows_everything() -> None:
    assert host_is_allowlisted("anything.test", [])


def test_host_is_allowlisted_ignores_case_and_trailing_dot() -> None:
    assert host_is_allowlisted("EXAMPLE.COM.", ["example.com"])
