"""Reddit collector, OAuth2 ``client_credentials`` (a read-only script app).

Reddit does not offer key-free read access: a script-type Reddit app provides a client id
and secret, exchanged for a short-lived access token (about an hour) using HTTP Basic auth.
Shape verified against platform documentation 2026-10-23; the first credentialed run against
the live service is a release condition tracked in ``docs/tickets/T23-reddit-collector.md``
(Reddit's public JSON endpoint is unreachable from this build machine, so the fixtures
encode the documented shape, not a captured response).

Facts the implementation depends on:

* ``POST /api/v1/access_token`` grants ``grant_type=client_credentials`` under HTTP Basic
  ``client_id:client_secret`` → ``{"access_token", "expires_in"}``.
* data requests go to ``https://oauth.reddit.com`` with ``Authorization: Bearer`` and a
  **descriptive User-Agent** (platform requirement; the identity of the script app).
* ``{base}/new?limit=…&after=…`` returns a Listing: ``data.children[]`` of
  ``{"kind": "t3", "data": {...}}`` post objects and ``data.after`` for pagination.
* ``author`` is ``"[deleted]"`` for removed accounts — a fact worth keeping, not an absence.
* mutable numbers (``score``, ``num_comments``, ``upvote_ratio``) change over time, so emit
  them as metrics/metadata, never as item columns.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    ErrorKind,
    SourceRef,
)
from trendsentinel.collectors.http import FetchResult, HttpFetcher
from trendsentinel.collectors.registry import register
from trendsentinel.collectors.thumbnail import THUMBNAIL_KEY, thumbnail_from_mapping
from trendsentinel.config import Settings
from trendsentinel.logging import get_logger

__all__ = ["RedditCollector", "clear_token_cache", "normalize_item"]

logger = get_logger(__name__)

TOKEN_URL = "https://www.reddit.com/api/v1/access_token"

#: Reddit caps a listing page at 100 posts; a page above that is clamped by the platform.
MAX_PAGE_SIZE = 100

TOKEN_CREDENTIAL_KEYS = ("reddit_client_id", "reddit_client_secret", "reddit_user_agent")


def clear_token_cache() -> None:
    """Empty the in-process token cache. Tests use this to isolate runs from each other."""
    _TOKEN_CACHE.clear()


# --------------------------------------------------------------------------- normalize


def _first_string(value: Any) -> str | None:
    """Return a non-empty trimmed string, or ``None``."""
    if isinstance(value, str):
        return value.strip() or None
    return None


def normalize_item(payload: Mapping[str, Any]) -> CollectedItem | None:
    """Convert one ``t3`` post into the internal shape.

    A pure function: no network, no clock beyond the capture timestamp, so it is directly
    unit-testable against recorded payloads.

    Returns:
        The normalized item, or ``None`` when the payload has no usable identity.
    """
    external_id = _identity_of(payload)
    if external_id is None:
        return None

    permalink = _first_string(payload.get("permalink"))
    url = f"https://www.reddit.com{permalink}" if permalink else None

    return CollectedItem(
        external_id=external_id,
        url=url,
        title=_first_string(payload.get("title")),
        text=_first_string(payload.get("selftext")),
        author=_first_string(payload.get("author")),
        published_at=_published_at(payload.get("created_utc")),
        metadata=_metadata(payload),
        metrics=_metrics(payload),
    )


def _identity_of(payload: Mapping[str, Any]) -> str | None:
    """Prefer the bare post id as ``t3_<id>``; the ``name`` fullname is the fallback.

    Anything else (a stripped-down crosspost, an ad payload) has no stable identity here and
    must be skipped rather than guessed at.
    """
    raw_id = _first_string(payload.get("id"))
    if raw_id:
        return f"t3_{raw_id}"

    name = _first_string(payload.get("name"))
    if name:
        return name

    return None


def _published_at(value: Any) -> datetime | None:
    """``created_utc`` is unix seconds, UTC by definition."""
    if not isinstance(value, int | float) or isinstance(value, bool) or value <= 0:
        return None

    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _metrics(payload: Mapping[str, Any]) -> dict[str, float] | None:
    """Extract the mutable measurements the listing reports, keeping only what is present."""
    metrics: dict[str, float] = {}

    score = payload.get("score")
    if isinstance(score, int | float) and not isinstance(score, bool):
        metrics["score"] = float(score)

    comments = payload.get("num_comments")
    if isinstance(comments, int | float) and not isinstance(comments, bool):
        metrics["comments"] = float(comments)

    return metrics or None


def _metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Platform-specific context that does not fit the shared metric vocabulary."""
    return {
        "subreddit": _first_string(payload.get("subreddit")),
        "upvote_ratio": (
            payload.get("upvote_ratio")
            if isinstance(payload.get("upvote_ratio"), int | float)
            and not isinstance(payload.get("upvote_ratio"), bool)
            else None
        ),
        "stickied": payload.get("stickied") if isinstance(payload.get("stickied"), bool) else None,
        # `preview` nests the image under images[].source.url; the shared scanner walks
        # it so a nesting change between API versions is not this adapter's problem.
        THUMBNAIL_KEY: thumbnail_from_mapping(payload.get("preview")),
    }


# --------------------------------------------------------------------------- collector


#: Per-process token cache under ADR-0004: one exchange per credential pair, reused across
#: runs. Keyed on a digest of the credentials so the client secret never becomes a dict key.
_TOKEN_CACHE: dict[str, tuple[str, float]] = {}


def _cache_key(client_id: str, client_secret: str) -> str:
    return hashlib.sha256(f"{client_id}\x00{client_secret}".encode()).hexdigest()


class RedditCollector:
    """Collects the newest posts of configured subreddits."""

    key = "reddit"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch and normalize up to ``limit`` posts, paginating on ``after``."""
        missing = [name for name in TOKEN_CREDENTIAL_KEYS if not source.config_str(name)]
        if missing:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.REJECTED,
                        f"missing Reddit config key(s) in source config: {', '.join(missing)}",
                        retryable=False,
                    )
                ]
            )

        base = source.base_url.strip().rstrip("/")
        if not base:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        client_id = source.config_str("reddit_client_id", "")
        client_secret = source.config_str("reddit_client_secret", "")
        user_agent = source.config_str("reddit_user_agent", "")
        assert client_id and client_secret and user_agent  # guaranteed above; narrows types

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        delay_ms = source.config_int("min_request_delay_ms", 0)

        result = await self._token(client_id, client_secret, user_agent, force=False)
        if result.token is None:
            return CollectionResult(errors=[result.error or _token_unknown_error()])

        token = result.token
        page_limit = min(effective_limit, MAX_PAGE_SIZE)
        after: str | None = None
        collected: list[CollectedItem] = []
        errors: list[CollectError] = []

        while len(collected) < effective_limit:
            if after is not None and delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

            url = f"{base}/new?limit={page_limit}" + (f"&after={after}" if after else "")
            outcome = await self._listing(url, client_id, client_secret, user_agent, token)

            if outcome.token_rejected:
                # A refreshed token was refused too: the credential itself is bad.
                return CollectionResult(
                    errors=outcome.errors
                    or [
                        CollectError(
                            ErrorKind.REJECTED,
                            "reddit rejected the access token after refresh",
                            retryable=False,
                        )
                    ]
                )
            if outcome.error is not None:
                return CollectionResult(items=collected, errors=[*errors, outcome.error])
            assert outcome.pages is not None

            for child in outcome.pages:
                item = normalize_item(child)
                if item is None:
                    errors.append(
                        CollectError(
                            ErrorKind.INVALID_PAYLOAD,
                            f"post in {url} has no usable identity",
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
                            "post failed validation",
                            detail=str(exc),
                        )
                    )
                    continue

                collected.append(item)
                if len(collected) >= effective_limit:
                    break

            after = outcome.next_after
            if not after:
                break

        return CollectionResult(items=collected, errors=errors)

    async def _listing(
        self,
        url: str,
        client_id: str,
        client_secret: str,
        user_agent: str,
        token: str,
    ) -> _ListingOutcome:
        """Fetch one listing page, with one token refresh on a 401 (ADR-0004)."""
        result = await self._get(url, user_agent, token)
        error = result.error

        if error is not None and error.status_code == 401:
            refreshed = await self._token(client_id, client_secret, user_agent, force=True)
            if refreshed.token is None:
                return _ListingOutcome(
                    errors=[refreshed.error or _token_unknown_error()],
                    token_rejected=True,
                )

            result = await self._get(url, user_agent, refreshed.token)
            if result.error is not None and result.error.status_code == 401:
                return _ListingOutcome(token_rejected=True)
            error = result.error

        if error is not None:
            return _ListingOutcome(errors=[error])
        assert result.response is not None

        try:
            payload = result.response.json()
        except ValueError as exc:
            return _ListingOutcome(
                errors=[
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"listing from {url} is not valid JSON",
                        detail=str(exc),
                    )
                ]
            )

        return _from_payload(payload, url)

    async def _get(self, url: str, user_agent: str, token: str) -> FetchResult:
        headers = {"Authorization": f"Bearer {token}", "User-Agent": user_agent}
        return await self._fetcher.get_json(url, headers=headers)

    async def _token(
        self, client_id: str, client_secret: str, user_agent: str, *, force: bool
    ) -> _TokenOutcome:
        """Exchange credentials for a token, reusing the cached one until it expires."""
        key = _cache_key(client_id, client_secret)

        if not force:
            cached = _TOKEN_CACHE.get(key)
            if cached and cached[1] > time.monotonic():
                return _TokenOutcome(cached[0])

        basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        result = await self._fetcher.post_form(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {basic}", "User-Agent": user_agent},
        )
        if not result.ok or result.response is None:
            error = result.error or _token_unknown_error()
            if error.kind is ErrorKind.HTTP_STATUS and error.status_code == 401:
                return _TokenOutcome(
                    error=CollectError(
                        ErrorKind.REJECTED,
                        "reddit rejected the app credentials at token exchange",
                        retryable=False,
                        status_code=401,
                    )
                )
            return _TokenOutcome(error=error)

        try:
            payload = result.response.json()
        except ValueError as exc:
            return _TokenOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    "token response is not valid JSON",
                    detail=str(exc),
                )
            )

        token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(token, str) or not token.strip():
            return _TokenOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    "token response carries no usable access_token",
                )
            )

        expires_in = payload.get("expires_in")
        expires = (
            expires_in
            if isinstance(expires_in, int | float) and not isinstance(expires_in, bool)
            else 3600.0
        )
        # A small safety floor: never cache a token that would be used dead-on-arrival.
        _TOKEN_CACHE[key] = (token, time.monotonic() + min(max(expires, 60.0), 3600.0))
        return _TokenOutcome(token)


# --------------------------------------------------------------------------- outcomes


@dataclass(frozen=True, slots=True)
class _TokenOutcome:
    token: str | None = None
    error: CollectError | None = None


@dataclass(frozen=True, slots=True)
class _ListingOutcome:
    pages: list[Mapping[str, Any]] | None = None
    next_after: str | None = None
    errors: list[CollectError] | None = None
    #: True when the token itself was refused even after one refresh (ADR-0004).
    token_rejected: bool = False

    @property
    def error(self) -> CollectError | None:
        return self.errors[0] if self.errors else None


def _from_payload(payload: Any, url: str) -> _ListingOutcome:
    """Extract the ``t3`` post payloads and the pagination cursor from a listing."""
    if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
        return _ListingOutcome(
            errors=[CollectError(ErrorKind.INVALID_PAYLOAD, f"listing from {url} is not a Listing")]
        )

    data = payload["data"]
    children = data.get("children")
    if not isinstance(children, list):
        return _ListingOutcome(
            errors=[CollectError(ErrorKind.INVALID_PAYLOAD, f"listing from {url} has no children")]
        )

    posts: list[Mapping[str, Any]] = []
    for child in children:
        if not isinstance(child, Mapping):
            continue
        if child.get("kind") != "t3":
            logger.debug("skipping child of uninteresting kind", extra={"kind": child.get("kind")})
            continue
        inner = child.get("data")
        if isinstance(inner, Mapping):
            posts.append(dict(inner))

    after = data.get("after")
    return _ListingOutcome(
        pages=posts,
        next_after=after.strip() if isinstance(after, str) and after.strip() else None,
    )


def _token_unknown_error() -> CollectError:
    return CollectError(ErrorKind.NETWORK, "could not exchange reddit credentials", retryable=True)


@register("reddit")
def _build(settings: Settings, fetcher: HttpFetcher) -> RedditCollector:
    """Registry factory."""
    return RedditCollector(settings, fetcher)
