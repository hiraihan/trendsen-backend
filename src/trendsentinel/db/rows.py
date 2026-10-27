"""Typed row shapes returned by repositories.

Repositories return plain mappings from psycopg's ``dict_row`` factory. These dataclasses
give callers a typed view without introducing an ORM or a query builder.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Self

__all__ = [
    "ItemRow",
    "ItemUpsert",
    "RunRow",
    "RunStatus",
    "SnapshotRow",
    "SourceRow",
]


def _as_datetime(value: Any) -> datetime | None:
    """Pass through psycopg's ``datetime``, tolerating a missing value."""
    return value if isinstance(value, datetime) else None


@dataclass(frozen=True, slots=True)
class SourceRow:
    """A row from ``sources``."""

    id: int
    key: str
    name: str
    integration: str
    base_url: str | None
    is_active: bool
    config: Mapping[str, Any]
    created_at: datetime | None
    updated_at: datetime | None
    source_type: str | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Self:
        return cls(
            id=row["id"],
            key=row["key"],
            name=row["name"],
            integration=row["integration"],
            base_url=row["base_url"],
            is_active=row["is_active"],
            config=dict(row["config"] or {}),
            created_at=_as_datetime(row.get("created_at")),
            updated_at=_as_datetime(row.get("updated_at")),
            source_type=row.get("source_type"),
        )

    @property
    def collector_key(self) -> str:
        """Which collector handles this source, defaulting to the integration type."""
        key = self.config.get("collector_key")
        return key if isinstance(key, str) and key else self.integration

    @property
    def limit(self) -> int | None:
        """Per-source item cap, when configured."""
        limit = self.config.get("limit")
        return limit if isinstance(limit, int) and limit > 0 else None

    @property
    def interval_minutes(self) -> int | None:
        """Per-source collection interval, when configured."""
        interval = self.config.get("interval_minutes")
        return interval if isinstance(interval, int) and interval > 0 else None


@dataclass(frozen=True, slots=True)
class ItemRow:
    """A row from ``collected_items``."""

    id: int
    source_id: int
    external_id: str
    url: str | None
    title: str | None
    text: str | None
    author: str | None
    published_at: datetime | None
    metadata: Mapping[str, Any]
    captured_at: datetime
    last_seen_at: datetime | None
    language: str | None = None
    content_type: str | None = None
    author_url: str | None = None
    thumbnail_url: str | None = None
    canonical_url: str | None = None
    content_fingerprint: str | None = None
    category: str | None = None
    subcategory: str | None = None
    content_status: str | None = None
    language_confidence: float | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Self:
        captured_at = _as_datetime(row.get("captured_at"))
        if captured_at is None:
            raise ValueError("collected_items.captured_at is NOT NULL but was missing")

        return cls(
            id=row["id"],
            source_id=row["source_id"],
            external_id=row["external_id"],
            url=row["url"],
            title=row["title"],
            text=row["text"],
            author=row["author"],
            published_at=_as_datetime(row.get("published_at")),
            metadata=dict(row["metadata"] or {}),
            captured_at=captured_at,
            last_seen_at=_as_datetime(row.get("last_seen_at")),
            language=row.get("language"),
            content_type=row.get("content_type"),
            author_url=row.get("author_url"),
            thumbnail_url=row.get("thumbnail_url"),
            canonical_url=row.get("canonical_url"),
            content_fingerprint=row.get("content_fingerprint"),
            category=row.get("category"),
            subcategory=row.get("subcategory"),
            content_status=row.get("content_status"),
            language_confidence=row.get("language_confidence"),
        )


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    """A row from ``item_snapshots``."""

    id: int
    item_id: int
    captured_at: datetime
    metrics: Mapping[str, Any]

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Self:
        captured_at = _as_datetime(row.get("captured_at"))
        if captured_at is None:
            raise ValueError("item_snapshots.captured_at is NOT NULL but was missing")

        return cls(
            id=row["id"],
            item_id=row["item_id"],
            captured_at=captured_at,
            metrics=dict(row["metrics"] or {}),
        )


@dataclass(frozen=True, slots=True)
class RunRow:
    """A row from ``collection_runs``."""

    id: int
    source_id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    trigger: str
    items_fetched: int
    items_new: int
    items_updated: int
    error_summary: Mapping[str, Any] | None
    item_limit: int | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Self:
        started_at = _as_datetime(row.get("started_at"))
        if started_at is None:
            raise ValueError("collection_runs.started_at is NOT NULL but was missing")

        summary = row.get("error_summary")
        limit = row.get("item_limit")
        return cls(
            id=row["id"],
            source_id=row["source_id"],
            started_at=started_at,
            finished_at=_as_datetime(row.get("finished_at")),
            status=row["status"],
            trigger=row["trigger"],
            items_fetched=row["items_fetched"],
            items_new=row["items_new"],
            items_updated=row["items_updated"],
            error_summary=dict(summary) if isinstance(summary, Mapping) else None,
            item_limit=limit if isinstance(limit, int) and not isinstance(limit, bool) else None,
        )


@dataclass(frozen=True, slots=True)
class ItemUpsert:
    """The database-facing shape of one collected item.

    Deliberately separate from the collectors' DTO: the database layer must not depend on
    the collector layer. :meth:`from_collected_item` is the single place the two meet.
    """

    source_id: int
    external_id: str
    url: str | None
    title: str | None
    text: str | None
    author: str | None
    published_at: datetime | None
    metadata: Mapping[str, Any]
    captured_at: datetime
    language: str | None = None
    content_type: str | None = None
    author_url: str | None = None
    thumbnail_url: str | None = None
    fingerprint: str | None = None

    def as_params(self) -> dict[str, Any]:
        """Return bound-parameter values matching the repository's SQL placeholders."""
        return {
            "source_id": self.source_id,
            "external_id": self.external_id,
            "url": self.url,
            "title": self.title,
            "text": self.text,
            "author": self.author,
            "published_at": self.published_at,
            "metadata": json.dumps(dict(self.metadata)),
            "captured_at": self.captured_at,
            "language": self.language,
            "content_type": self.content_type,
            "author_url": self.author_url,
            "thumbnail_url": self.thumbnail_url,
            "fingerprint": self.fingerprint,
        }


class RunStatus:
    """Values allowed by the ``collection_runs.status`` check constraint.

    Kept as plain string constants rather than an enum so a row can be passed straight back
    to SQL without a conversion step.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"

    #: Statuses that occupy a source's single active-run slot.
    ACTIVE: frozenset[str] = frozenset({PENDING, RUNNING})

    #: Statuses considered finished.
    TERMINAL: frozenset[str] = frozenset({SUCCEEDED, FAILED, PARTIAL})
