"""RSS/Atom collector.

One adapter covers both formats, because feeds migrate and the format is detected from the
parsed document rather than from the URL.

Identity resolution is the part most likely to be wrong, so it is explicit and tested. For
``hnrss.org`` the ``guid`` is ``https://news.ycombinator.com/item?id={ID}`` while ``<link>``
points at the *external article*. Using ``link`` as the identity would be incorrect.

Verified against the live feeds on 2026-10-21:

* ``hnrss.org/frontpage`` - RSS 2.0, ``guid`` with ``isPermaLink="false"``, ``dc:creator``,
  ``pubDate``, and a ``description`` containing ``Points:``/``# Comments:``.
* ``github.blog/feed/`` - RSS 2.0 with ``content:encoded``, ``dc:creator``, permalink
  ``guid``.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import feedparser
from bs4 import BeautifulSoup

from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    ErrorKind,
    SourceRef,
)
from trendsentinel.collectors.http import HttpFetcher
from trendsentinel.collectors.registry import register
from trendsentinel.collectors.thumbnail import THUMBNAIL_KEY, thumbnail_from_html
from trendsentinel.config import Settings
from trendsentinel.logging import get_logger

__all__ = ["RssCollector", "normalize_entry", "parse_hnrss_metrics", "strip_html"]

logger = get_logger(__name__)

#: hnrss.org renders these two numbers into the item description as HTML.
_POINTS_RE = re.compile(r"Points:\s*(\d+)", re.IGNORECASE)
_COMMENTS_RE = re.compile(r"#\s*Comments:\s*(\d+)", re.IGNORECASE)

#: Matches the Hacker News item id inside a discussion URL.
_HN_ITEM_RE = re.compile(r"[?&]id=(\d+)")


def strip_html(value: str) -> str:
    """Return the visible text of an HTML fragment, with runs of whitespace collapsed."""
    if not value:
        return ""

    text = BeautifulSoup(value, "html.parser").get_text(separator=" ")
    return " ".join(text.split())


def parse_hnrss_metrics(description: str) -> dict[str, float] | None:
    """Extract ``score`` and ``comments`` from an hnrss description.

    Returns ``None`` unless *both* numbers are present: a partial parse is more likely to be
    misread markup than a genuine metric, and a wrong metric is worse than none.
    """
    if not description:
        return None

    text = strip_html(description)
    points = _POINTS_RE.search(text)
    comments = _COMMENTS_RE.search(text)

    if not points or not comments:
        return None

    return {"score": float(points.group(1)), "comments": float(comments.group(1))}


def normalize_entry(
    entry: Mapping[str, Any],
    *,
    max_text_chars: int,
    feed_title: str | None = None,
    feed_language: str | None = None,
) -> CollectedItem | None:
    """Convert one parsed feed entry into the internal shape.

    A pure function, so it is unit-testable against recorded feeds with no network access.

    Returns:
        The normalized item, or ``None`` when the entry has no usable identity. An entry
        that cannot be identified could not be deduplicated, so storing it would create a
        duplicate on every run.
    """
    external_id = _identity_of(entry)
    if external_id is None:
        return None

    description = _raw_description(entry)
    summary = entry.get("summary")
    text_source = description or (summary if isinstance(summary, str) else "")

    stripped_text = strip_html(text_source)
    return CollectedItem(
        external_id=external_id,
        url=_first_non_empty(entry.get("link")),
        title=_first_non_empty(entry.get("title")),
        # An hnrss description is pure boilerplate (article/comments URLs, the same
        # numbers the metrics already carry), so the stored text drops it and keeps
        # whatever real prose followed — usually none, which is an honest absence.
        text=_truncate(_drop_hnrss_boilerplate(stripped_text), max_text_chars) or None,
        author=_first_non_empty(entry.get("author")),
        published_at=_entry_datetime(entry),
        # A feed's own <language> declaration is the only non-guessed source of language;
        # an entry that declares none keeps null. RSS entries are articles/posts by type.
        language=(_first_non_empty(entry.get("language"))) or (
            feed_language if isinstance(feed_language, str) else None
        ),
        content_type="article",
        thumbnail_url=thumbnail_from_html(text_source),
        metadata={
            "feed_title": feed_title,
            # Kept verbatim so a later mis-parse of the derived metrics stays auditable.
            "raw_description": description or None,
            # The feed's own <img>, when the entry HTML carries one — extracted, not
            # fabricated, and https-only (the shared helper's rule).
            THUMBNAIL_KEY: thumbnail_from_html(text_source),
            # Publisher-assigned tags/categories (RSS <category>, Atom <category>).
            # Where a feed declares none, the key stays absent.
            "tags": _entry_tags(entry),
        },
        metrics=parse_hnrss_metrics(description),
    )


# hnrss renders its field list into the description: ``Article URL: <url>``,
# ``Comments URL: <url>``, ``Points: <n>``, ``# Comments: <n>`` — sometimes led by a bare
# article URL. Each field is removed by its own anchored pattern; prose inside the same
# description survives, and text without these markers is never touched.
_HNRSS_ARTICLE_URL_RE = re.compile(r"Article URL:\s*https?://\S+", re.IGNORECASE)
_HNRSS_COMMENTS_URL_RE = re.compile(r"Comments URL:\s*https?://\S+", re.IGNORECASE)
_HNRSS_POINTS_FIELD_RE = re.compile(r"Points:\s*\d+", re.IGNORECASE)
_HNRSS_COMMENTS_FIELD_RE = re.compile(r"#\s*Comments:\s*\d+", re.IGNORECASE)
_HNRSS_LEADING_URL_RE = re.compile(
    r"^https?://\S+(?=\s*(?:Article URL:|Comments URL:|Points:|#\s*Comments:)|\s*$)",
    re.IGNORECASE,
)

#: The gate markers, matched the same way the patterns they guard are: case-insensitively.
_HNRSS_POINTS_MARKER_RE = re.compile(r"Points:\s*\d+", re.IGNORECASE)
_HNRSS_COMMENTS_URL_MARKER_RE = re.compile(
    r"Comments URL:\s*https?://", re.IGNORECASE
)


def _drop_hnrss_boilerplate(text: str) -> str:
    """Remove the hnrss field list from stripped text, keeping any prose.

    Only bites when the text actually looks like an hnrss field list (a ``Points:``/
    ``Comments URL:`` marker); a normal feed entry never contains those, so it passes
    through untouched. The gate mirrors ``parse_hnrss_metrics``, which already treats
    ``Points:`` in a feed description as an hnrss marker — case-insensitively, like the
    patterns it guards.
    """
    if not (_HNRSS_POINTS_MARKER_RE.search(text) or _HNRSS_COMMENTS_URL_MARKER_RE.search(text)):
        return text

    cleaned = _HNRSS_ARTICLE_URL_RE.sub("", text)
    cleaned = _HNRSS_COMMENTS_URL_RE.sub("", cleaned)
    cleaned = _HNRSS_POINTS_FIELD_RE.sub("", cleaned)
    cleaned = _HNRSS_COMMENTS_FIELD_RE.sub("", cleaned)
    cleaned = _HNRSS_LEADING_URL_RE.sub("", cleaned)
    return cleaned.strip()


def _identity_of(entry: Mapping[str, Any]) -> str | None:
    """Resolve an entry's identity, preferring the most stable field available.

    Order: ``guid``/``id`` (a publisher-assigned, stable identifier), then a Hacker News item
    id found in the ``comments`` or ``link`` URL, then ``link`` itself. Anything else is
    unusable, and an unusable identity means the entry must be skipped rather than stored.
    """
    for key in ("guid", "id"):
        candidate = _first_non_empty(entry.get(key))
        if candidate:
            return candidate

    # The discussion URL is a more stable identity than the article it points at, and it is
    # checked first for exactly that reason.
    for key in ("comments", "link"):
        candidate = _first_non_empty(entry.get(key))
        if not candidate:
            continue

        match = _HN_ITEM_RE.search(candidate)
        if match:
            return f"https://news.ycombinator.com/item?id={match.group(1)}"

    # No discussion identity anywhere: the article URL is the last usable option.
    return _first_non_empty(entry.get("link"))


def _raw_description(entry: Mapping[str, Any]) -> str:
    """Return an entry's richest HTML body, preferring ``content:encoded``.

    feedparser exposes ``content:encoded`` as ``entry["content"]``; ``summary`` is the
    fallback for feeds that only publish a description.
    """
    content = entry.get("content")
    if isinstance(content, list):
        for part in content:
            if isinstance(part, Mapping):
                value = part.get("value")
                if isinstance(value, str) and value:
                    return value

    summary = entry.get("summary")
    return summary if isinstance(summary, str) else ""


def _entry_datetime(entry: Mapping[str, Any]) -> datetime | None:
    """Return the publication time as a UTC datetime, falling back to the update time.

    ``published_parsed`` is absent in feeds that omit ``pubDate``; the entry is still stored,
    with a null publication time.
    """
    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed is None:
            continue
        try:
            # feedparser hands back a time.struct_time; take the first six fields and stamp
            # them as UTC, since feed dates carry no offset of their own.
            year, month, day, hour, minute, second = parsed[:6]
            return datetime(year, month, day, hour, minute, second, tzinfo=UTC)
        except (TypeError, ValueError):
            continue

    return None


def _first_non_empty(value: Any) -> str | None:
    """Return a trimmed non-empty string, or ``None``."""
    if not isinstance(value, str):
        return None

    stripped = value.strip()
    return stripped or None


def _entry_tags(entry: Mapping[str, Any]) -> list[str] | None:
    """Return the entry's publisher-assigned tags/categories, or ``None`` when absent.

    feedparser exposes both RSS `<category>` and Atom `<category>` as ``entry.tags``
    (a list of mappings carrying a ``term``); some feeds also surface ``categories``.
    Tags are deduplicated in document order — a tag repeated by the publisher is still
    one tag.
    """
    found: list[str] = []
    for key in ("tags", "categories"):
        for tag in (entry.get(key) or []):
            if isinstance(tag, Mapping):
                term = tag.get("term")
            else:
                term = tag
            if isinstance(term, str):
                clean = term.strip()
                if clean and clean not in found:
                    found.append(clean)
    return found or None


def _truncate(value: str, max_chars: int) -> str:
    """Limit stored text length, appending an ellipsis marker when cut."""
    if len(value) <= max_chars:
        return value

    return value[:max_chars].rstrip() + "…"


class RssCollector:
    """Collects entries from a single RSS or Atom feed."""

    key = "rss"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch and normalize up to ``limit`` entries from the feed."""
        url = source.base_url.strip()
        if not url:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        # A gentler cadence for volunteer-run third-party feeds.
        delay_ms = source.config_int("min_request_delay_ms", 0)
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000)

        result = await self._fetcher.get_text(url)
        if not result.ok or result.response is None:
            return CollectionResult(errors=[result.error or _unknown_error(url)])

        return _parse_feed(
            result.response.content,
            url=url,
            limit=effective_limit,
            max_text_chars=self._settings.max_text_chars,
        )


def _parse_feed(content: bytes, *, url: str, limit: int, max_text_chars: int) -> CollectionResult:
    """Parse a feed document and normalize its entries.

    Bytes are parsed rather than text so the document's own encoding declaration is honoured.
    """
    parsed = feedparser.parse(content)

    entries = list(parsed.entries or [])

    if parsed.get("bozo") and not entries:
        # Markup that yielded nothing at all is a fetch we cannot use. A `bozo` flag with
        # usable entries is common with real-world feeds and is tolerated.
        return CollectionResult(
            errors=[
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"feed at {url} could not be parsed",
                    detail=str(parsed.get("bozo_exception")),
                )
            ]
        )

    if not entries:
        return CollectionResult(
            errors=[CollectError(ErrorKind.INVALID_PAYLOAD, f"feed at {url} contains no entries")]
        )

    feed_title = parsed.feed.get("title") if parsed.get("feed") else None
    feed_language = parsed.feed.get("language") if parsed.get("feed") else None

    items: list[CollectedItem] = []
    errors: list[CollectError] = []

    for entry in entries[:limit]:
        item = normalize_entry(
            entry,
            max_text_chars=max_text_chars,
            feed_title=feed_title,
            feed_language=feed_language,
        )
        if item is None:
            errors.append(
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"entry in {url} has no usable identity",
                    detail="no guid, id, link or discussion URL",
                )
            )
            continue

        try:
            item = item.normalized()
            item.validate()
        except ValueError as exc:
            errors.append(
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"entry in {url} failed validation",
                    detail=str(exc),
                )
            )
            continue

        items.append(item)

    return CollectionResult(items=items, errors=errors)


def _unknown_error(url: str) -> CollectError:
    """Fallback for the impossible case of a failed fetch with no recorded error."""
    return CollectError(ErrorKind.NETWORK, f"could not fetch {url}", retryable=True)


@register("rss")
def _build(settings: Settings, fetcher: HttpFetcher) -> RssCollector:
    """Registry factory."""
    return RssCollector(settings, fetcher)
