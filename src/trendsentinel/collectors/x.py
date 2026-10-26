"""X (Twitter) collector — API v2 recent search, app-only bearer token.

There is no token exchange: the app-only bearer is minted in the developer portal and stored
as ``config['x_bearer_token']`` (ADR-0003, ADR-0004 case 1). The mapping is verified against
platform documentation 2026-10-23; the fixtures in ``tests/fixtures/x/`` follow that
documented shape and are **not** verbatim captures, because reading recent search requires a
paid X API tier this project is not provisioned with. Re-capturing them from a credentialed
run is the release condition tracked in ``docs/tickets/T22-x-collector.md``.

Facts the implementation depends on:

* ``GET /2/tweets/search/recent`` on ``https://api.twitter.com`` with ``Authorization:
  Bearer <app-only>``, ``query`` (max 512 chars), ``max_results`` clamped to **10..100**,
  and ``next_token`` for pagination.
* ``expansions=author_id&user.fields=username`` supplies ``includes.users[]``; without it a
  post carries only ``author_id`` and no name.
* ``data[]`` of ``{id, text, author_id, created_at, public_metrics}``; post ids are strings
  and must stay strings (they exceed the JS-safe integer range; X documents them as strings).
* ``created_at`` is ISO-8601 with milliseconds and a ``Z`` suffix.
* 401 = invalid/revoked bearer, 403 = tier not entitled to read, 429 = rate limit with
  ``retry-after``. None of these are fixed by retrying within a run.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, TypeGuard
from urllib.parse import urlencode

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
from trendsentinel.logging import redact

__all__ = ["XCollector", "normalize_item"]

#: The documented ``max_results`` bounds of the recent-search endpoint.
MIN_RESULTS = 10
MAX_RESULTS = 100

SEARCH_PATH = "/2/tweets/search/recent"

#: Fields we request but do not parse directly; they exist to make the mapping possible.
_EXPANSIONS = "author_id"
_TWEET_FIELDS = "created_at,public_metrics,author_id"
_USER_FIELDS = "username"

X_CREDENTIAL_KEYS = ("x_bearer_token",)


# --------------------------------------------------------------------------- normalize


def _first_string(value: Any) -> str | None:
    """Return a non-empty trimmed string, or ``None``."""
    if isinstance(value, str):
        return value.strip() or None
    return None


def _is_number(value: Any) -> TypeGuard[int | float]:
    return isinstance(value, int | float) and not isinstance(value, bool)


def normalize_item(
    payload: Mapping[str, Any], usernames: Mapping[str, str]
) -> CollectedItem | None:
    """Convert one v2 post into the internal shape.

    Pure and clock-free, so it is directly unit-testable against recorded payloads.

    Args:
        payload: One element of ``data[]``.
        usernames: ``author_id`` → username, built from ``includes.users``. An author that
            is absent from the mapping is reported as no author rather than as an id.

    Returns:
        The normalized item, or ``None`` when the payload has no usable identity.
    """
    external_id = _identity_of(payload)
    if external_id is None:
        return None

    author_id = _first_string(payload.get("author_id"))
    username = usernames.get(author_id) if author_id else None

    return CollectedItem(
        external_id=external_id,
        # The URL is only honest when the username resolved; X ids are not public paths.
        url=f"https://x.com/{username}/status/{external_id}" if username else None,
        title=None,  # a post has no title; never fabricated
        text=_first_string(payload.get("text")),
        author=username,
        published_at=_published_at(payload.get("created_at")),
        metadata=_metadata(payload, author_id),
        metrics=_metrics(payload),
    )


def _identity_of(payload: Mapping[str, Any]) -> str | None:
    """The post id, as a string — X ids are documented as strings, never numbers."""
    raw_id = payload.get("id")
    if not isinstance(raw_id, str):
        return None
    return raw_id.strip() or None


def _published_at(value: Any) -> datetime | None:
    """``created_at`` is ISO-8601 with a ``Z`` suffix, e.g. ``2026-10-23T12:00:00.000Z``."""
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None

    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _metrics(payload: Mapping[str, Any]) -> dict[str, float] | None:
    """Extract the public metrics, keeping only the ones the response actually carried."""
    raw = payload.get("public_metrics")
    if not isinstance(raw, Mapping):
        return None

    # X metric name → our shared vocabulary (ADR-0002). impression_count is only present
    # on entitled tiers, so a missing one must stay missing rather than become views=0.
    pairs = (
        ("likes", "like_count"),
        ("reposts", "retweet_count"),
        ("replies", "reply_count"),
        ("views", "impression_count"),
    )
    metrics = {name: float(raw[source]) for name, source in pairs if _is_number(raw.get(source))}
    return metrics or None


def _metadata(payload: Mapping[str, Any], author_id: str | None) -> dict[str, Any]:
    """Platform-specific context that does not fit the shared metric vocabulary."""
    raw_metrics = payload.get("public_metrics")
    quote_count = raw_metrics.get("quote_count") if isinstance(raw_metrics, Mapping) else None
    return {
        "quote_count": float(quote_count) if _is_number(quote_count) else None,
        "author_id": author_id,
    }


# --------------------------------------------------------------------------- collector


@dataclass(frozen=True, slots=True)
class _PageOutcome:
    posts: list[Mapping[str, Any]] = field(default_factory=list)
    usernames: Mapping[str, str] = field(default_factory=dict)
    next_token: str | None = None
    error: CollectError | None = None


class XCollector:
    """Collects recent posts matching a configured query."""

    key = "x"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch and normalize up to ``limit`` posts, paginating on ``next_token``."""
        missing = [name for name in X_CREDENTIAL_KEYS if not source.config_str(name)]
        query = source.config_str("query")
        if not query:
            missing.append("query")
        if missing:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.REJECTED,
                        f"missing X config key(s) in source config: {', '.join(missing)}",
                        retryable=False,
                    )
                ]
            )
        assert query  # guaranteed above; narrows the type

        base = source.base_url.strip().rstrip("/")
        if not base:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        token = source.config_str("x_bearer_token", "")
        assert token  # guaranteed above; narrows the type

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        collected: list[CollectedItem] = []
        errors: list[CollectError] = []
        seen_tokens: set[str] = set()
        next_token: str | None = None

        while len(collected) < effective_limit:
            # The endpoint rejects max_results outside 10..100, so ask for the window
            # we are allowed and simply stop reading when the limit is reached.
            page_size = max(MIN_RESULTS, min(effective_limit - len(collected), MAX_RESULTS))
            page = await self._page(base, query, token, page_size, next_token)

            if page.error is not None:
                return CollectionResult(items=collected, errors=[*errors, page.error])

            for post in page.posts:
                item = normalize_item(post, page.usernames)
                if item is None:
                    errors.append(
                        CollectError(
                            ErrorKind.INVALID_PAYLOAD,
                            f"post in {base}{SEARCH_PATH} has no usable identity",
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

            if not page.next_token:
                break
            if page.next_token in seen_tokens:
                errors.append(
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"X repeated next_token {page.next_token!r}; stopping pagination",
                    )
                )
                break
            seen_tokens.add(page.next_token)
            next_token = page.next_token

        return CollectionResult(items=collected, errors=errors)

    async def _page(
        self,
        base: str,
        query: str,
        token: str,
        max_results: int,
        next_token: str | None,
    ) -> _PageOutcome:
        """Fetch one page of recent search."""
        params: dict[str, str] = {
            "query": query,
            "max_results": str(max_results),
            "expansions": _EXPANSIONS,
            "tweet.fields": _TWEET_FIELDS,
            "user.fields": _USER_FIELDS,
        }
        if next_token:
            params["next_token"] = next_token

        url = f"{base}{SEARCH_PATH}?{urlencode(params)}"
        result = await self._fetcher.get_json(url, headers={"Authorization": f"Bearer {token}"})

        error = result.error
        if error is not None:
            if error.status_code in (401, 403):
                # 401 = revoked bearer, 403 = tier not entitled: both need a human.
                return _PageOutcome(
                    error=CollectError(
                        ErrorKind.REJECTED,
                        f"X refused the request (HTTP {error.status_code}): check the "
                        "bearer token and the account's API tier",
                        retryable=False,
                        status_code=error.status_code,
                    )
                )
            return _PageOutcome(error=error)

        assert result.response is not None
        try:
            payload = result.response.json()
        except ValueError as exc:
            return _PageOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"response from {base}{SEARCH_PATH} is not valid JSON",
                    detail=str(exc),
                )
            )

        return _from_payload(payload, secrets=(token,))


# --------------------------------------------------------------------------- outcomes


def _error_detail(raw_errors: Any, secrets: tuple[str, ...] = ()) -> str | None:
    """Surface the platform's own error message when the response carries one.

    The message is remote-controlled, so any credential value echoed back in it is redacted
    before it can reach ``collection_runs.error_summary``.
    """
    if not isinstance(raw_errors, list) or not raw_errors:
        return None
    first = raw_errors[0]
    if isinstance(first, Mapping):
        message = _first_string(first.get("message")) or _first_string(first.get("detail"))
        if message:
            return redact(message, secrets)
    return None


def _from_payload(payload: Any, secrets: tuple[str, ...] = ()) -> _PageOutcome:
    """Extract posts, usernames and the pagination cursor from a search response."""
    if not isinstance(payload, Mapping):
        return _PageOutcome(
            error=CollectError(ErrorKind.INVALID_PAYLOAD, "search response is not an object")
        )

    raw_data = payload.get("data")
    if "data" not in payload:
        # A valid search response always carries ``data`` (an empty list when nothing
        # matched). Its absence means the platform returned an error envelope instead.
        detail = _error_detail(payload.get("errors"), secrets)
        return _PageOutcome(
            error=CollectError(
                ErrorKind.INVALID_PAYLOAD,
                "search response carries no 'data'",
                detail=detail,
            )
        )
    if not isinstance(raw_data, list):
        return _PageOutcome(
            error=CollectError(ErrorKind.INVALID_PAYLOAD, "search response 'data' is not a list")
        )

    meta = payload.get("meta")
    if meta is not None and not isinstance(meta, Mapping):
        return _PageOutcome(
            error=CollectError(ErrorKind.INVALID_PAYLOAD, "search response 'meta' is not an object")
        )

    posts: list[Mapping[str, Any]] = [dict(post) for post in raw_data if isinstance(post, Mapping)]
    if len(posts) != len(raw_data):
        return _PageOutcome(
            error=CollectError(
                ErrorKind.INVALID_PAYLOAD, "search response contains a non-object post"
            )
        )

    includes = payload.get("includes")
    raw_users: Any = includes.get("users") if isinstance(includes, Mapping) else None
    if raw_users is None:
        raw_users = []
    usernames: dict[str, str] = {}
    if isinstance(raw_users, list):
        for user in raw_users:
            if not isinstance(user, Mapping):
                continue
            user_id = _first_string(user.get("id"))
            username = _first_string(user.get("username"))
            if user_id and username:
                usernames[user_id] = username

    token = _first_string(meta.get("next_token")) if isinstance(meta, Mapping) else None

    return _PageOutcome(posts=posts, usernames=usernames, next_token=token)


@register("x")
def _build(settings: Settings, fetcher: HttpFetcher) -> XCollector:
    """Registry factory."""
    return XCollector(settings, fetcher)
