"""The collector contract.

Every source adapter implements :class:`Collector` and returns :class:`CollectionResult`.
Collectors depend only on this module, ``http`` and ``ssrf`` — never on the database, the
services or the API.

The DTO is deliberately permissive. Fields are optional because real sources omit them:
Hacker News stories frequently carry no ``text``, and Ask HN posts carry no ``url``. A
missing value stays ``None`` and is never fabricated.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "CollectError",
    "CollectedItem",
    "CollectionResult",
    "Collector",
    "ErrorKind",
    "SourceConfig",
    "SourceRef",
    "UnknownCollectorError",
    "parse_domains",
]


class ErrorKind(StrEnum):
    """Classification of a collection failure.

    The distinction matters operationally: only ``TIMEOUT``, ``RATE_LIMITED`` and
    ``HTTP_STATUS`` with a retryable status are worth retrying.
    """

    TIMEOUT = "timeout"
    HTTP_STATUS = "http_status"
    RATE_LIMITED = "rate_limited"
    INVALID_PAYLOAD = "invalid_payload"
    NETWORK = "network"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class CollectError:
    """A failure that occurred during one collection run."""

    kind: ErrorKind
    message: str
    retryable: bool = False
    detail: str | None = None
    #: HTTP status when the failure came from a response. Adapters use this to react to a
    #: specific status (401 means "refresh the credential") instead of matching strings.
    status_code: int | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable form for ``collection_runs.error_summary``."""
        return {
            "kind": str(self.kind),
            "message": self.message,
            "retryable": self.retryable,
            **({"detail": self.detail} if self.detail else {}),
            **({"status_code": self.status_code} if self.status_code is not None else {}),
        }


@dataclass(frozen=True, slots=True)
class CollectedItem:
    """One normalized item, in the shape the rest of the application speaks.

    ``external_id`` is the only required field: it is the item's identity at the source and
    therefore the key deduplication depends on.
    """

    external_id: str
    url: str | None = None
    title: str | None = None
    text: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, float] | None = None
    captured_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    #: Optional content/collection metadata. Sources do not provide these uniformly; a
    #: ``None`` stays ``None`` and is never fabricated from a guess.
    language: str | None = None
    content_type: str | None = None
    author_url: str | None = None
    thumbnail_url: str | None = None

    def validate(self) -> None:
        """Raise :class:`ValueError` when the item cannot be stored.

        Only identity is mandatory. An item with no usable identity could not be
        deduplicated, so it must be rejected rather than stored repeatedly.
        """
        if not self.external_id or not self.external_id.strip():
            raise ValueError("external_id is empty")

        if self.published_at is not None and self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")

        for name, value in (("metrics", self.metrics),):
            if value is not None:
                for key, metric in value.items():
                    if not isinstance(metric, int | float) or isinstance(metric, bool):
                        raise ValueError(f"{name}[{key!r}] must be numeric")

    def normalized(self) -> CollectedItem:
        """Return a copy with whitespace-trimmed text fields and a UTC-aware timestamp.

        Normalization happens here, once, so no collector has to remember to do it.
        """
        return replace(
            self,
            external_id=self.external_id.strip(),
            url=_clean(self.url),
            title=_clean(self.title),
            text=_clean(self.text),
            author=_clean(self.author),
            published_at=_as_utc(self.published_at),
            captured_at=_as_utc(self.captured_at) or datetime.now(UTC),
            language=_clean(self.language),
            content_type=_clean(self.content_type),
            author_url=_clean(self.author_url),
            thumbnail_url=_clean(self.thumbnail_url),
        )


@dataclass(frozen=True, slots=True)
class CollectionResult:
    """What a collector returns: items it managed to normalize, plus its failures."""

    items: list[CollectedItem] = field(default_factory=list)
    errors: list[CollectError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether the run produced no failures at all."""
        return not self.errors

    def merge(self, other: CollectionResult) -> CollectionResult:
        """Combine two results, preserving order."""
        return CollectionResult(
            items=[*self.items, *other.items], errors=[*self.errors, *other.errors]
        )


@dataclass(frozen=True, slots=True)
class SourceRef:
    """The minimal view of a source a collector needs to fetch data.

    Keeping this small is what lets collectors be tested without a database row: the service
    layer builds one from a ``SourceRow``.
    """

    base_url: str
    key: str = ""
    config: Mapping[str, Any] = field(default_factory=dict)

    def config_int(self, name: str, default: int) -> int:
        """Read an integer config value, falling back when absent or malformed."""
        value = self.config.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) else default

    def config_str(self, name: str, default: str | None = None) -> str | None:
        """Read a string config value, falling back when absent or malformed."""
        value = self.config.get(name)
        return value if isinstance(value, str) and value else default


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Typed view over ``sources.config`` shared by collectors."""

    limit: int
    interval_minutes: int
    min_request_delay_ms: int = 0
    keywords: tuple[str, ...] = ()
    viral_quantile_low: int | None = None
    viral_quantile_high: int | None = None
    allowed_domains: tuple[str, ...] = ()
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_ref(cls, source: SourceRef, default_limit: int, default_interval: int) -> SourceConfig:
        known = {
            "limit",
            "interval_minutes",
            "min_request_delay_ms",
            "collector_key",
            "keywords",
            "viral_quantile_low",
            "viral_quantile_high",
            "allowed_domains",
        }
        return cls(
            limit=source.config_int("limit", default_limit),
            interval_minutes=source.config_int("interval_minutes", default_interval),
            min_request_delay_ms=source.config_int("min_request_delay_ms", 0),
            keywords=_parse_keywords(source.config),
            viral_quantile_low=_parse_quantile(source.config, "viral_quantile_low"),
            viral_quantile_high=_parse_quantile(source.config, "viral_quantile_high"),
            allowed_domains=_parse_domains(source.config),
            extra={k: v for k, v in source.config.items() if k not in known},
        )


def _parse_keywords(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Read ``keywords`` as a list or a comma-separated string, lowered and deduped."""
    raw = config.get("keywords")
    if isinstance(raw, str):
        parts: Sequence[Any] = raw.split(",")
    elif isinstance(raw, (list, tuple)):
        parts = raw
    else:
        return ()
    seen: list[str] = []
    for part in parts:
        if isinstance(part, str):
            clean = part.strip().lower()
            if clean and clean not in seen:
                seen.append(clean)
    return tuple(seen)


def _parse_quantile(config: Mapping[str, Any], name: str) -> int | None:
    """Read an optional quantile override, rejecting out-of-range values."""
    value = config.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    if not 0 <= value <= 100:
        return None
    return value


def parse_domains(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Read ``allowed_domains`` from a config mapping (list or comma-separated)."""
    return _parse_domains(config)


def _parse_domains(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Read ``allowed_domains`` as a list or a comma-separated string."""
    raw = config.get("allowed_domains")
    if isinstance(raw, str):
        parts: Sequence[Any] = raw.split(",")
    elif isinstance(raw, (list, tuple)):
        parts = raw
    else:
        return ()
    result: list[str] = []
    for part in parts:
        if isinstance(part, str):
            clean = part.strip().lower().rstrip(".")
            if clean and clean not in result:
                result.append(clean)
    return tuple(result)


@runtime_checkable
class Collector(Protocol):
    """A source adapter.

    Implementations must never raise for an expected network condition: they convert
    failures into :class:`CollectError` values so one bad item cannot abort a run.
    """

    key: str

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch up to ``limit`` items from ``source``."""
        ...


class UnknownCollectorError(Exception):
    """No collector is registered for a source's integration or ``collector_key``."""

    def __init__(self, key: str) -> None:
        super().__init__(f"no collector registered for {key!r}")
        self.key = key


def _clean(value: str | None) -> str | None:
    """Trim a value, mapping empty results to ``None`` rather than an empty string."""
    if value is None:
        return None

    stripped = value.strip()
    return stripped or None


def _as_utc(value: datetime | None) -> datetime | None:
    """Return a UTC-aware datetime, treating a naive value as UTC."""
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)
