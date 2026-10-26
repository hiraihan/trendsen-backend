"""Threads collector — Graph API ``me/threads``, read-only.

Threads needs a long-lived user access token minted by hand in the Meta developer console;
there is no in-worker refresh (ADR-0004 case 3), so an expired token is a *rotation* task for
the operator, not something a retry can fix. The app-scoped permissions this endpoint needs
are granted only after Meta App Review, so the fixtures in ``tests/fixtures/threads/`` follow
the documented shape and are **not** verbatim captures. Re-capturing them and one live run are
the exit conditions tracked in ``docs/tickets/T24-threads-collector.md``.

Facts the implementation depends on:

* ``GET /me?fields=username`` names the profile being read. ``me/threads`` is always the
  profile's own posts, so one username covers a run.
* ``GET /me/threads?fields=…&limit=…&after=…`` returns ``data[]`` posts. A present
  ``paging.next`` means another page exists; the ``after`` cursor for it is read from
  ``paging.cursors.after``. With neither, the profile is exhausted.
* A post may carry no ``permalink`` and no title — image-only posts are normal. Title and URL
  stay ``None``; the caption (when present) is the ``text``.
* ``timestamp`` is ISO-8601 with a ``+0000`` offset.
* ``paging.next`` embeds the access token. Following it verbatim would copy a credential into
  request logs, so the adapter reads only the ``after`` cursor and rebuilds the URL. The token
  travels in an ``Authorization: Bearer`` header, never in the query string.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
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
from trendsentinel.collectors.thumbnail import THUMBNAIL_KEY, thumbnail_from_mapping
from trendsentinel.config import Settings

__all__ = ["ThreadsCollector", "normalize_item"]

#: Fields requested for each post. Metrics are requested because they are cheap here and
#: absent fields stay absent in the mapping. ``media_url`` feeds the SPA's thumbnail;
#: a media-only post carries the image without any text.
POST_FIELDS = "id,text,timestamp,permalink,media_type,media_url,likes,replies,views"
PROFILE_FIELDS = "id,username"

TOKEN_CONFIG_KEY = "threads_access_token"

#: The API caps a page at 100 posts.
MAX_PAGE_SIZE = 100

ROTATION_HINT = "rotate threads_access_token in sources.config and re-activate the source"


# --------------------------------------------------------------------------- normalize


def _first_string(value: Any) -> str | None:
    """Return a non-empty trimmed string, or ``None``."""
    if isinstance(value, str):
        return value.strip() or None
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def normalize_item(payload: Mapping[str, Any], author: str | None) -> CollectedItem | None:
    """Convert one Threads post into the internal shape.

    Pure and clock-free. ``author`` is the profile username resolved once per run.

    Returns:
        The normalized item, or ``None`` when the payload has no usable identity.
    """
    raw_id = payload.get("id")
    if not isinstance(raw_id, str) or not raw_id.strip():
        return None

    return CollectedItem(
        external_id=raw_id.strip(),
        # A media-only post has no permalink; a title is not part of a Threads post at all.
        url=_first_string(payload.get("permalink")),
        title=None,
        text=_first_string(payload.get("text")),
        author=author,
        published_at=_published_at(payload.get("timestamp")),
        metadata=_metadata(payload),
        metrics=_metrics(payload),
    )


def _published_at(value: Any) -> datetime | None:
    """``timestamp`` is ISO-8601 with a basic-offset suffix, e.g. ``+0000``."""
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None

    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _metrics(payload: Mapping[str, Any]) -> dict[str, float] | None:
    """Likes, replies and views are only present when the fields were granted."""
    metrics = {
        name: float(payload[name])
        for name in ("likes", "replies", "views")
        if _is_number(payload.get(name))
    }
    return metrics or None


def _metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Platform-specific context that does not fit the shared metric vocabulary."""
    return {
        "media_type": _first_string(payload.get("media_type")),
        # Image posts carry the media URL at the top level; the shared scanner keeps
        # this honest (https-only, no fabrication) whatever nesting the API uses.
        THUMBNAIL_KEY: thumbnail_from_mapping(payload.get("media_url")),
    }


# --------------------------------------------------------------------------- collector


class ThreadsCollector:
    """Collects a profile's recent posts."""

    key = "threads"

    def __init__(self, settings: Settings, fetcher: HttpFetcher) -> None:
        self._settings = settings
        self._fetcher = fetcher

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        """Fetch and normalize up to ``limit`` posts, paginating on the ``after`` cursor."""
        token = source.config_str(TOKEN_CONFIG_KEY)
        if not token:
            return CollectionResult(
                errors=[
                    CollectError(
                        ErrorKind.REJECTED,
                        f"missing Threads config key in source config: {TOKEN_CONFIG_KEY}",
                        retryable=False,
                    )
                ]
            )

        base = source.base_url.strip().rstrip("/")
        if not base:
            return CollectionResult(
                errors=[CollectError(ErrorKind.REJECTED, "source has no base_url")]
            )

        effective_limit = max(0, min(limit, self._settings.max_items_per_run))
        if effective_limit == 0:
            return CollectionResult()

        profile = await self._profile(base, token)
        if profile.error is not None:
            return CollectionResult(errors=[profile.error])

        collected: list[CollectedItem] = []
        errors: list[CollectError] = []
        seen_cursors: set[str] = set()
        after: str | None = None

        while len(collected) < effective_limit:
            page_size = min(effective_limit - len(collected), MAX_PAGE_SIZE)
            page = await self._page(base, token, page_size, after)

            if page.error is not None:
                return CollectionResult(items=collected, errors=[*errors, page.error])

            for post in page.posts:
                item = normalize_item(post, profile.username)
                if item is None:
                    errors.append(
                        CollectError(
                            ErrorKind.INVALID_PAYLOAD,
                            f"post in {base}/me/threads has no usable identity",
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

            if not page.has_next:
                break
            if page.next_after is None:
                break
            if page.next_after in seen_cursors:
                errors.append(
                    CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"Threads repeated cursor {page.next_after!r}; stopping pagination",
                    )
                )
                break
            seen_cursors.add(page.next_after)
            after = page.next_after

        return CollectionResult(items=collected, errors=errors)

    async def _profile(self, base: str, token: str) -> _ProfileOutcome:
        """Resolve the profile's username once per run."""
        url = f"{base}/me?{urlencode({'fields': PROFILE_FIELDS})}"
        result = await self._fetcher.get_json(url, headers=_auth_headers(token))

        if result.error is not None:
            return _ProfileOutcome(error=_classified(result.error.status_code, result.error))
        assert result.response is not None

        try:
            payload = result.response.json()
        except ValueError as exc:
            return _ProfileOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"profile response from {base}/me is not valid JSON",
                    detail=str(exc),
                )
            )

        username = _first_string(payload.get("username")) if isinstance(payload, Mapping) else None
        if username is None:
            return _ProfileOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"profile response from {base}/me carries no username",
                )
            )

        return _ProfileOutcome(username=username)

    async def _page(self, base: str, token: str, limit: int, after: str | None) -> _PageOutcome:
        """Fetch one page of the profile's posts."""
        params: dict[str, str] = {"fields": POST_FIELDS, "limit": str(limit)}
        if after:
            params["after"] = after

        url = f"{base}/me/threads?{urlencode(params)}"
        result = await self._fetcher.get_json(url, headers=_auth_headers(token))

        if result.error is not None:
            return _PageOutcome(error=_classified(result.error.status_code, result.error))
        assert result.response is not None

        try:
            payload = result.response.json()
        except ValueError as exc:
            return _PageOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"response from {base}/me/threads is not valid JSON",
                    detail=str(exc),
                )
            )

        return _from_payload(payload)


# --------------------------------------------------------------------------- outcomes


@dataclass(frozen=True, slots=True)
class _ProfileOutcome:
    username: str | None = None
    error: CollectError | None = None


@dataclass(frozen=True, slots=True)
class _PageOutcome:
    posts: list[Mapping[str, Any]] = field(default_factory=list)
    has_next: bool = False
    next_after: str | None = None
    error: CollectError | None = None


def _auth_headers(token: str) -> dict[str, str]:
    """Send the token as a bearer header, never as a query parameter."""
    return {"Authorization": f"Bearer {token}"}


def _classified(status_code: int | None, error: CollectError) -> CollectError:
    """Turn an auth failure into the actionable message the operator needs."""
    if status_code in (401, 403):
        return CollectError(
            ErrorKind.REJECTED,
            f"Threads refused the token (HTTP {status_code}): {ROTATION_HINT}",
            retryable=False,
            status_code=status_code,
        )
    return error


def _from_payload(payload: Any) -> _PageOutcome:
    """Extract the posts and the pagination cursor from a threads response."""
    if not isinstance(payload, Mapping):
        return _PageOutcome(
            error=CollectError(ErrorKind.INVALID_PAYLOAD, "threads response is not an object")
        )

    raw_data = payload.get("data")
    if not isinstance(raw_data, list):
        return _PageOutcome(
            error=CollectError(ErrorKind.INVALID_PAYLOAD, "threads response 'data' is not a list")
        )

    posts: list[Mapping[str, Any]] = []
    for post in raw_data:
        if not isinstance(post, Mapping):
            return _PageOutcome(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD, "threads response contains a non-object post"
                )
            )
        posts.append(dict(post))

    has_next, next_after = _pagination(payload.get("paging"))
    return _PageOutcome(posts=posts, has_next=has_next, next_after=next_after)


def _pagination(paging: Any) -> tuple[bool, str | None]:
    """Decide whether another page exists, and with which cursor.

    ``paging.next`` (when present) means "there is another page", but its URL embeds the
    access token, so only the ``after`` cursor is read out of it and the URL itself is never
    followed.
    """
    if not isinstance(paging, Mapping) or not _first_string(paging.get("next")):
        return False, None

    cursors = paging.get("cursors")
    if isinstance(cursors, Mapping):
        after = _first_string(cursors.get("after"))
        if after:
            return True, after

    return True, None


@register("threads")
def _build(settings: Settings, fetcher: HttpFetcher) -> ThreadsCollector:
    """Registry factory."""
    return ThreadsCollector(settings, fetcher)
