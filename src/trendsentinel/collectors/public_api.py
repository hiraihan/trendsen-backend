"""Public, unauthenticated JSON/API collector.

``integration="public_api"`` covers public endpoints that return JSON without credentials —
the "no-credential" class (ADR-0008). It stays thin: fetch a JSON document, normalize a
documented shape into ``CollectedItem``s, and hand back the result. There is no per-source
adapter; a source is one row in ``sources``.

The response shape is deliberately simple and explicit:

.. code-block:: json

    {
      "items": [
        {
          "id": "1",
          "url": "https://example.test/1",
          "title": "A title",
          "text": "Body text",
          "author": "author",
          "published_at": "2026-10-01T12:00:00Z",
          "metrics": {"score": 10}
        }
      ]
    }

An outer object whose ``items`` key holds the list, a bare JSON array, or an object that is
itself one item are all accepted. Unknown keys are ignored; a missing ``id`` and ``url``
falls back to ``title`` then to ``text`` so an item is never dropped for a missing identity
when another stable key exists.

Metrics, when present, must be numeric — enforced by ``CollectedItem.validate``.
"""

from __future__ import annotations

import json
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
from trendsentinel.collectors.thumbnail import thumbnail_from_mapping
from trendsentinel.config import Settings

__all__ = ["PublicApiCollector", "normalize_item", "parse_items"]

_ITEM_KEYS = ("items", "data", "results")
_IDENTITY_KEYS = ("id", "external_id", "guid", "url")


def parse_items(payload: Any) -> list[Mapping[str, Any]]:
    """Return the list of item maps from a JSON payload.

    Accepts a list, an object carrying a list under a conventional key, or a single item
    object.
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in _ITEM_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
        return [payload]
    return []


def normalize_item(
    raw: Mapping[str, Any],
    *,
    base_url: str,
    max_text_chars: int,
) -> CollectedItem | None:
    """Convert one payload item into the internal shape.

    Returns ``None`` when the item has no usable identity (no ``id``/``url``/``title``/
    ``text``), so it cannot be deduplicated.
    """
    external_id = _first_string(raw.get("id"), raw.get("external_id"), raw.get("guid"))
    url = _first_string(raw.get("url"))
    title = _first_string(raw.get("title"))
    text = _first_string(raw.get("text"), raw.get("content"))

    identity = external_id or url or title or text
    if identity is None:
        return None

    text = text or ""
    if len(text) > max_text_chars:
        text = text[:max_text_chars].rstrip() + "…"
    text = text or None

    metrics = _metric_map(raw.get("metrics"))

    return CollectedItem(
        external_id=external_id or url or title or (identity or "")[:200],
        url=url,
        title=title,
        text=text,
        author=_first_string(raw.get("author"), raw.get("creator")),
        published_at=_parse_datetime(raw.get("published_at"), raw.get("date")),
        language=_first_string(raw.get("language")),
        content_type=_first_string(raw.get("content_type")),
        author_url=_first_string(raw.get("author_url")),
        thumbnail_url=thumbnail_from_mapping(raw.get("thumbnail_url")),
        metadata={"api_base": base_url},
        metrics=metrics,
    )


def _metric_map(value: Any) -> Mapping[str, float] | None:
    if not isinstance(value, Mapping):
        return None
    result: dict[str, float] = {}
    for key, metric in value.items():
        if isinstance(metric, int | float) and not isinstance(metric, bool):
            result[key] = float(metric)
    return result or None


def _parse_datetime(*values: Any) -> datetime | None:
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _first_string(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


class PublicApiCollector:
    """Collects items from a public JSON endpoint."""

    key = "public_api"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        url = source.base_url.strip()
        if not url:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        result = await self._fetcher.get_json(url)
        if not result.ok or result.response is None:
            return CollectionResult(
                errors=[
                    result.error
                    or CollectError(ErrorKind.NETWORK, f"could not fetch {url}")
                ]
            )

        try:
            payload = result.response.json()
        except json.JSONDecodeError as exc:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"response from {url} is not valid JSON",
                        detail=str(exc),
                    )
                ]
            )

        raw_items = parse_items(payload)[:effective_limit]

        items: list[CollectedItem] = []
        errors: list[CollectError] = []
        for raw in raw_items:
            item = normalize_item(
                raw,
                base_url=url,
                max_text_chars=self._settings.max_text_chars,
            )
            if item is None:
                errors.append(
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        "item has no usable identity",
                        detail="no id, url, title or text",
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
                        "item failed validation",
                        detail=str(exc),
                    )
                )
                continue
            items.append(item)

        if not items and not errors:
            errors.append(
                CollectError(ErrorKind.INVALID_PAYLOAD, f"endpoint at {url} returned no items")
            )

        return CollectionResult(items=items, errors=errors)


@register("public_api")
def _build(settings: Settings, fetcher: HttpFetcher) -> PublicApiCollector:
    """Registry factory."""
    return PublicApiCollector(settings, fetcher)
