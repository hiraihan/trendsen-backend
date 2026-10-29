"""Wikimedia Pageviews API collector.

``integration="api"`` with ``collector_key="wikimedia"`` collects pageview counts from the
Wikimedia REST API (``wikimedia.org/api/rest_v1/metrics/pageviews/per-article/...``). No
credentials are required, so the source is a no-credential source (ADR-0008) and its host
must be domain-allowlisted before any request is made.

Pageviews are *time-series observations*, not engagement metrics like a score or a vote. A
pageview count belongs to a measurement window (a day), not to an item value. So each
observation is modeled as its own ``CollectedItem`` whose ``captured_at`` is the start of its
day window, and the metric ``views`` is that day's count. All observations for one article
share the same ``external_id`` (the canonical article URL), and the per-item snapshot rows
deduplicate on ``(item_id, captured_at)`` — so a re-run of the same window updates nothing
and inserts nothing.

Article discovery (which articles to track) is deliberately separate from pageview
collection: the articles are configured on the source, and this collector only fetches the
counts for them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

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

__all__ = ["WikimediaCollector", "normalize_payload", "parse_wikimedia_timestamp"]

DEFAULT_PROJECT = "en.wikipedia"
DEFAULT_ACCESS = "all-access"
DEFAULT_AGENT = "user"
DEFAULT_GRANULARITY = "daily"
DEFAULT_DAYS = 7


@dataclass(frozen=True, slots=True)
class _WikimediaConfig:
    project: str
    access: str
    agent: str
    granularity: str
    articles: tuple[str, ...]
    days: int


def parse_wikimedia_timestamp(value: str) -> datetime | None:
    """Return the UTC midnight of a ``YYYYMMDD00`` pageview timestamp, or ``None``.

    Daily pageviews carry a timestamp of the form ``2026102000`` — ``YYYYMMDD`` plus a
    fixed ``00`` hour. Anything that does not fit that shape yields ``None`` so a caller
    can reject the observation rather than mis-read it.
    """
    if not isinstance(value, str) or len(value) < 8:
        return None
    try:
        return datetime(
            int(value[0:4]), int(value[4:6]), int(value[6:8]), tzinfo=UTC
        )
    except ValueError:
        return None


def _canonical_article_url(project: str, article: str) -> str:
    """Return a Wikipedia canonical article URL for ``project`` and ``article``.

    ``project`` is the API form (e.g. ``en.wikipedia``); the article host appends the
    ``.org`` TLD. Spaces become underscores (the canonical form in wiki URLs); the rest is
    URL-encoded.
    """
    return f"https://{project}.org/wiki/{quote(article.strip().replace(' ', '_'), safe='()')}"


def normalize_payload(payload: Any, *, source_url: str) -> CollectionResult:
    """Convert a pageviews response into collected observations.

    Each observation is one ``CollectedItem`` for one ``(article, day)`` pair, with
    ``captured_at`` set to the day-window start and ``metrics={"views": N}``. Observations
    that cannot be parsed (bad timestamp, non-numeric views) are reported as errors and
    never silently dropped.
    """
    if not isinstance(payload, Mapping):
        return CollectionResult(
            errors=[
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"pageviews response from {source_url} is not an object",
                )
            ]
        )

    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        return CollectionResult(
            errors=[
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"pageviews response from {source_url} has no items list",
                )
            ]
        )

    items: list[CollectedItem] = []
    errors: list[CollectError] = []

    for raw in raw_items:
        if not isinstance(raw, Mapping):
            errors.append(
                CollectError(ErrorKind.INVALID_PAYLOAD, "pageviews item is not an object")
            )
            continue

        article = raw.get("article")
        if not isinstance(article, str) or not article.strip():
            errors.append(
                CollectError(ErrorKind.INVALID_PAYLOAD, "pageviews item has no article")
            )
            continue

        project = raw.get("project") or DEFAULT_PROJECT
        if not isinstance(project, str) or not project.strip():
            project = DEFAULT_PROJECT

        captured_at = parse_wikimedia_timestamp(raw.get("timestamp"))
        if captured_at is None:
            errors.append(
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"pageviews item for {article!r} has an unusable timestamp",
                )
            )
            continue

        views = raw.get("views")
        if not isinstance(views, int) or isinstance(views, bool):
            errors.append(
                CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"pageviews item for {article!r} has a non-numeric views value",
                )
            )
            continue

        canonical = _canonical_article_url(project, article)
        window_end = captured_at + timedelta(days=1)
        items.append(
            CollectedItem(
                external_id=canonical,
                url=canonical,
                title=article.strip(),
                content_type="page",
                captured_at=captured_at,
                metrics={"views": float(views)},
                metadata={
                    "project": project,
                    "access": raw.get("access") or DEFAULT_ACCESS,
                    "agent": raw.get("agent") or DEFAULT_AGENT,
                    "granularity": raw.get("granularity") or DEFAULT_GRANULARITY,
                    "article": article.strip(),
                    "window_start": captured_at.isoformat(),
                    "window_end": window_end.isoformat(),
                },
            )
        )

    return CollectionResult(items=items, errors=errors)


def _wikimedia_config(config: Mapping[str, Any]) -> _WikimediaConfig:
    articles_raw = config.get("articles")
    if isinstance(articles_raw, str):
        articles = tuple(
            a.strip() for a in articles_raw.split(",") if a.strip() and not a.strip().isspace()
        )
    elif isinstance(articles_raw, (list, tuple)):
        articles = tuple(a.strip() for a in articles_raw if isinstance(a, str) and a.strip())
    else:
        articles = ()

    days = config.get("days")
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        days = DEFAULT_DAYS

    return _WikimediaConfig(
        project=(config.get("project") or DEFAULT_PROJECT).strip(),
        access=(config.get("access") or DEFAULT_ACCESS).strip(),
        agent=(config.get("agent") or DEFAULT_AGENT).strip(),
        granularity=(config.get("granularity") or DEFAULT_GRANULARITY).strip(),
        articles=articles,
        days=days,
    )


class WikimediaCollector:
    """Collects pageview observations for a configured set of articles."""

    key = "wikimedia"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        base = source.base_url.strip()
        if not base:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        cfg = _wikimedia_config(source.config)
        if not cfg.articles:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        "wikimedia source has no articles configured",
                    )
                ]
            )

        today = datetime.now(UTC).date()
        start = (today - timedelta(days=cfg.days - 1)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")

        items: list[CollectedItem] = []
        errors: list[CollectError] = []

        for article in cfg.articles:
            url = (
                f"{base}metrics/pageviews/per-article/{cfg.project}/{cfg.access}/{cfg.agent}/"
                f"{quote(article.strip().replace(' ', '_'))}/{cfg.granularity}/{start}/{end}"
            )
            result = await self._fetcher.get_json(url)
            if not result.ok or result.response is None:
                errors.append(
                    result.error
                    or CollectError(ErrorKind.NETWORK, f"could not fetch {url}")
                )
                continue

            try:
                payload = result.response.json()
            except Exception as exc:  # json decode — handled by get_json; guard anyway
                errors.append(
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"response from {url} is not valid JSON",
                        detail=str(exc),
                    )
                )
                continue

            parsed = normalize_payload(payload, source_url=url)
            items.extend(parsed.items)
            errors.extend(parsed.errors)

            if len(items) >= effective_limit:
                break

        return CollectionResult(items=items[:effective_limit], errors=errors)


@register("wikimedia")
def _build(settings: Settings, fetcher: HttpFetcher) -> WikimediaCollector:
    """Registry factory."""
    return WikimediaCollector(settings, fetcher)
