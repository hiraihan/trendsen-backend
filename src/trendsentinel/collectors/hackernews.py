"""Hacker News collector, using the official public Firebase API.

Verified against the live API on 2026-10-21. Facts the implementation depends on, each
confirmed rather than assumed:

* ``/v0/topstories.json`` returns an array of integer item ids.
* ``/v0/item/{id}.json`` returns ``by``, ``descendants``, ``id``, ``kids``, ``score``,
  ``time`` (unix seconds), ``title``, ``type`` and usually ``url``.
* A story may have **no ``url``** (Ask HN posts) and **no ``text``** (link posts), so both
  are optional here.
* A ``poll`` and its parts are not what this collector stores; only ``story`` is kept.
* Deleted or dead items return JSON ``null``.
* ``score`` and ``descendants`` change over time, so they are emitted as metrics rather than
  as item columns.

No API key is required and none is used.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    ErrorKind,
    SourceRef,
)
from trendsentinel.collectors.http import HttpFetcher
from trendsentinel.collectors.registry import register
from trendsentinel.config import Settings
from trendsentinel.logging import get_logger

__all__ = ["HackerNewsCollector", "comments_url_for", "normalize_item", "user_url_for"]

logger = get_logger(__name__)

#: Item types this collector stores. Polls, jobs and comments are out of scope.
STORED_TYPES = frozenset({"story"})


def comments_url_for(item_id: int | str) -> str:
    """Return the discussion URL for an item.

    The API does not return this field; it is derived from the item id, which is documented
    behaviour rather than enrichment.
    """
    return f"https://news.ycombinator.com/item?id={item_id}"


def user_url_for(author: str | None) -> str | None:
    """Return the profile URL for an author, or ``None`` when none is named."""
    if not author or not author.strip():
        return None
    return f"https://news.ycombinator.com/user?id={author.strip()}"


def normalize_item(payload: Mapping[str, Any]) -> CollectedItem | None:
    """Convert one API item into the internal shape.

    A pure function: no network, no clock beyond the capture timestamp, so it is directly
    unit-testable against recorded payloads.

    Returns:
        The normalized item, or ``None`` when the payload has no usable identity (a deleted
        item is a JSON ``null``, which the caller filters out before reaching here).
    """
    raw_id = payload.get("id")
    if raw_id is None:
        return None

    external_id = str(raw_id).strip()
    if not external_id:
        return None

    title = payload.get("title")
    text = payload.get("text")
    url = payload.get("url")
    author = payload.get("by")

    return CollectedItem(
        external_id=external_id,
        url=url if isinstance(url, str) and url else None,
        title=title if isinstance(title, str) and title else None,
        text=text if isinstance(text, str) and text else None,
        author=author if isinstance(author, str) and author else None,
        published_at=_published_at(payload.get("time")),
        # A story with an external URL is a link post; one without one is a self post.
        # Detected from the payload, not guessed.
        content_type=(
            "link"
            if isinstance(url, str) and url
            else ("self" if isinstance(text, str) and text else None)
        ),
        author_url=user_url_for(author),
        metadata={
            "type": payload.get("type"),
            "hackernews_id": raw_id,
            "comments_url": comments_url_for(external_id),
        },
        metrics=_metrics(payload),
    )


def _published_at(value: Any) -> datetime | None:
    """Convert the API's unix-seconds timestamp to a UTC datetime."""
    if not isinstance(value, int | float) or isinstance(value, bool) or value <= 0:
        return None

    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _metrics(payload: Mapping[str, Any]) -> dict[str, float] | None:
    """Extract the mutable measurements the API exposes, keeping only what is present."""
    metrics: dict[str, float] = {}

    score = payload.get("score")
    if isinstance(score, int | float) and not isinstance(score, bool):
        metrics["score"] = score

    descendants = payload.get("descendants")
    if isinstance(descendants, int | float) and not isinstance(descendants, bool):
        metrics["comments"] = descendants

    return metrics or None


class HackerNewsCollector:
    """Collects front-page stories from the official Hacker News API."""

    key = "hackernews"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch up to ``limit`` front-page stories.

        A failure on one item is recorded and does not prevent the remaining items from being
        returned, so a single flaky item cannot empty a run.
        """
        base = source.base_url.rstrip("/")
        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        fetcher = self._fetcher

        result = await fetcher.get_json(f"{base}/topstories.json")
        if not result.ok or result.response is None:
            return CollectionResult(errors=[result.error or _unknown_error(base)])

        try:
            payload = result.response.json()
        except ValueError as exc:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        "topstories response is not a JSON array",
                        detail=str(exc),
                    )
                ]
            )

        if not isinstance(payload, list):
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        "topstories response is not a JSON array",
                    )
                ]
            )

        item_ids = [
            int(raw_id)
            for raw_id in payload[:effective_limit]
            if isinstance(raw_id, int) and not isinstance(raw_id, bool)
        ]

        return await self._fetch_items(fetcher, base, item_ids)

    async def _fetch_items(
        self, fetcher: HttpFetcher, base: str, item_ids: list[int]
    ) -> CollectionResult:
        """Fetch item detail concurrently, under a bounded semaphore."""
        semaphore = asyncio.Semaphore(self._settings.max_concurrency)

        async def fetch_one(item_id: int) -> CollectionResult:
            async with semaphore:
                return await self._fetch_item(fetcher, base, item_id)

        results = await asyncio.gather(*(fetch_one(item_id) for item_id in item_ids))

        combined = CollectionResult()
        for result in results:
            combined = combined.merge(result)

        return combined

    async def _fetch_item(self, fetcher: HttpFetcher, base: str, item_id: int) -> CollectionResult:
        """Fetch and normalize one item."""
        url = f"{base}/item/{item_id}.json"
        result = await fetcher.get_json(url)

        if not result.ok or result.response is None:
            error = result.error or _unknown_error(url)
            logger.debug("item fetch failed", extra={"item_id": item_id, "kind": str(error.kind)})
            return CollectionResult(errors=[error])

        try:
            payload = result.response.json()
        except ValueError as exc:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"item {item_id} is not valid JSON",
                        detail=str(exc),
                    )
                ]
            )

        if payload is None:
            # A deleted or dead item. Expected, and not an error worth reporting.
            logger.debug("skipping unavailable item", extra={"item_id": item_id})
            return CollectionResult()

        if not isinstance(payload, Mapping):
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"item {item_id} is not a JSON object",
                    )
                ]
            )

        if payload.get("type") not in STORED_TYPES:
            logger.debug(
                "skipping item of uninteresting type",
                extra={"item_id": item_id, "type": payload.get("type")},
            )
            return CollectionResult()

        item = normalize_item(payload)
        if item is None:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"item {item_id} has no usable identity",
                    )
                ]
            )

        try:
            item = item.normalized()
            item.validate()
        except ValueError as exc:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"item {item_id} failed validation",
                        detail=str(exc),
                    )
                ]
            )

        return CollectionResult(items=[item])


def _unknown_error(url: str) -> CollectError:
    """Fallback for the impossible case of a failed fetch with no recorded error."""
    return CollectError(ErrorKind.NETWORK, f"could not fetch {url}", retryable=True)


@register("hackernews")
def _build(settings: Settings, fetcher: HttpFetcher) -> HackerNewsCollector:
    """Registry factory."""
    return HackerNewsCollector(settings, fetcher)
