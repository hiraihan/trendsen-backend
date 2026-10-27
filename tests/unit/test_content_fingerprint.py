"""Unit tests for the content fingerprint digest (services/collection.py).

The fingerprint is the cross-source similarity/dedup mechanism, so this test pins the
normalization contract: same content must hash identically regardless of source or
external_id, and items with no content must yield ``None`` (never a default hash).
"""

from __future__ import annotations

from datetime import UTC, datetime

from trendsentinel.collectors.base import CollectedItem
from trendsentinel.services.collection import content_fingerprint

PUBLISHED = datetime(2026, 10, 21, 13, 0, tzinfo=UTC)


def _item(title: str | None, text: str | None, external_id: str = "e1") -> CollectedItem:
    return CollectedItem(
        external_id=external_id,
        url="https://example.test/1",
        title=title,
        text=text,
        author="tester",
        published_at=PUBLISHED,
        metadata={},
        captured_at=PUBLISHED,
    )


def test_same_content_hashes_identically_across_sources() -> None:
    # Different source/external_id, identical normalized content -> same fingerprint,
    # regardless of case or whitespace.
    a = _item("Hello  World", "Body  text", external_id="hn-1")
    b = _item("hello world", "body text", external_id="reddit-2")
    assert content_fingerprint(a) == content_fingerprint(b)


def test_fingerprint_is_stable_across_calls() -> None:
    item = _item("Some Title", "Some body")
    assert content_fingerprint(item) == content_fingerprint(item)


def test_no_content_yields_none() -> None:
    assert content_fingerprint(_item(None, None)) is None


def test_title_only_still_fingerprints() -> None:
    # Ask-style posts carry title but no body; these still get a fingerprint.
    fp = content_fingerprint(_item("Ask HN: what is this", None))
    assert fp is not None
    assert len(fp) == 64
