"""Bounded HTTP access for collectors.

Every outbound request in this application goes through here, so timeouts, a response size
cap, a sensible User-Agent, retry policy and SSRF validation are applied uniformly and
cannot be forgotten at an individual call site.

Expected network conditions are reported as :class:`CollectError` values rather than raised:
one unreachable item must never abort a collection run.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final

import httpx

from trendsentinel.collectors import ssrf
from trendsentinel.collectors.base import CollectError, ErrorKind
from trendsentinel.collectors.ssrf import UrlNotAllowedError, assert_url_allowed
from trendsentinel.config import Settings
from trendsentinel.logging import get_logger

__all__ = [
    "HttpFetcher",
    "HttpResponse",
    "RetryPolicy",
    "build_client",
]

logger = get_logger(__name__)

#: Statuses worth trying again: transient server-side conditions.
RETRYABLE_STATUSES: Final[frozenset[int]] = frozenset({408, 425, 429, 500, 502, 503, 504})

#: Exceptions that indicate a transient transport problem.
RETRYABLE_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)


class ResponseTooLargeError(httpx.RequestError):
    """The response body exceeded the configured size cap."""


class RetryPolicy:
    """Attempt budget and backoff schedule for one fetcher."""

    def __init__(self, max_attempts: int, base_delay: float = 0.5, max_delay: float = 30.0) -> None:
        self.max_attempts = max(1, max_attempts)
        self.base_delay = base_delay
        self.max_delay = max_delay

    def delay_for(self, attempt: int, retry_after: float | None = None) -> float:
        """Return how long to wait before the given attempt.

        An explicit ``Retry-After`` always wins: the server is stating what it wants, and
        overriding it would be both impolite and likely to earn a longer ban.
        """
        if retry_after is not None:
            return min(max(retry_after, 0.0), self.max_delay)

        # Exponential backoff with full jitter, so parallel collectors do not retry in lockstep.
        ceiling = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        return random.uniform(0, ceiling)


@dataclass(slots=True)
class HttpResponse:
    """A successfully fetched response."""

    url: str
    status_code: int
    content: bytes
    content_type: str

    @property
    def text(self) -> str:
        """Decode the body using the charset declared in the content type."""
        return self.content.decode(_charset_of(self.content_type), errors="replace")

    def json(self) -> Any:
        """Parse the body as JSON.

        Raises:
            json.JSONDecodeError: The body is not valid JSON.
        """
        return json.loads(self.content)


@dataclass(slots=True)
class FetchResult:
    """The outcome of a fetch: a response or an error, never both."""

    response: HttpResponse | None = None
    error: CollectError | None = None

    @property
    def ok(self) -> bool:
        """Whether the fetch produced a usable response."""
        return self.response is not None and self.error is None


def _charset_of(content_type: str) -> str:
    """Extract a charset from a content type, defaulting to UTF-8."""
    for part in content_type.split(";")[1:]:
        name, separator, value = part.strip().partition("=")
        if separator and name.strip().lower() == "charset":
            candidate = value.strip().strip('"').strip("'")
            if candidate:
                return candidate

    return "utf-8"


def _retry_after_seconds(headers: httpx.Headers) -> float | None:
    """Parse ``Retry-After``, in either delta-seconds or HTTP-date form."""
    raw = headers.get("retry-after")
    if not raw:
        return None

    try:
        return max(0.0, float(raw))
    except ValueError:
        pass

    try:
        target = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None

    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)

    return max(0.0, (target - datetime.now(UTC)).total_seconds())


def build_client(
    settings: Settings,
    allowed_domains: Iterable[str] = (),
    *,
    resolver: ssrf.Resolver | None = None,
) -> httpx.AsyncClient:
    """Build a client with explicit timeouts and the SSRF guard attached.

    The guard is installed as a request event hook, so it runs for the initial request and
    for every redirect hop, and no call site can forget it.
    """
    domains = tuple(allowed_domains)
    timeout = httpx.Timeout(settings.http_timeout_seconds)

    async def guard(request: httpx.Request) -> None:
        assert_url_allowed(str(request.url), domains, resolver=resolver)

    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        max_redirects=3,
        headers={"User-Agent": settings.user_agent, "Accept": "*/*"},
        event_hooks={"request": [guard]},
        limits=httpx.Limits(max_connections=settings.max_concurrency + 2),
    )


class HttpFetcher:
    """A bounded, SSRF-guarded fetcher.

    Usage::

        async with HttpFetcher(settings) as fetcher:
            result = await fetcher.get_json("https://api.test/items.json")
            if result.ok:
                payload = result.response.json()
    """

    def __init__(
        self,
        settings: Settings,
        *,
        allowed_domains: Iterable[str] = (),
        client: httpx.AsyncClient | None = None,
        policy: RetryPolicy | None = None,
        resolver: ssrf.Resolver | None = None,
    ) -> None:
        self._settings = settings
        self._allowed_domains = tuple(allowed_domains)
        self._client = client
        self._owns_client = client is None
        self._policy = policy or RetryPolicy(max_attempts=settings.http_max_retries)
        self._resolver = resolver

    async def __aenter__(self) -> HttpFetcher:
        if self._client is None:
            self._client = build_client(
                self._settings, self._allowed_domains, resolver=self._resolver
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the client, but only if this fetcher created it."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_text(self, url: str, *, headers: Mapping[str, str] | None = None) -> FetchResult:
        """Fetch a URL, returning its body as text."""
        return await self._fetch_retry_loop(url, headers=headers)

    async def get_json(self, url: str, *, headers: Mapping[str, str] | None = None) -> FetchResult:
        """Fetch a URL and require a JSON body.

        The content type is not trusted, because APIs frequently mislabel it; a parse
        failure is reported as an invalid payload instead.
        """
        result = await self._fetch_retry_loop(url, headers=headers)
        if not result.ok or result.response is None:
            return result

        try:
            result.response.json()
        except json.JSONDecodeError as exc:
            return FetchResult(
                error=CollectError(
                    ErrorKind.INVALID_PAYLOAD,
                    f"response from {url} is not valid JSON",
                    detail=str(exc),
                )
            )

        return result

    async def fetch(self, url: str) -> FetchResult:
        """Fetch a URL, applying the size cap and the retry policy."""
        return await self._fetch_retry_loop(url)

    async def post_form(
        self,
        url: str,
        data: Mapping[str, str],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> FetchResult:
        """POST a urlencoded body, for token exchanges.

        Bounds are identical to GET with one conservative difference: a received HTTP status
        is never retried, so a failing credential endpoint is requested exactly once per
        attempt budget instead of amplifying a rejection.
        """
        return await self._fetch_retry_loop(
            url,
            method="POST",
            form=data,
            headers=headers,
            retry_http_status=False,
        )

    async def _fetch_retry_loop(
        self,
        url: str,
        *,
        method: str = "GET",
        form: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        retry_http_status: bool = True,
    ) -> FetchResult:
        """Perform the request, applying the size cap and the retry policy."""
        if self._client is None:
            raise RuntimeError("HttpFetcher must be used as an async context manager")

        error: CollectError | None = None
        retry_after: float | None = None

        for attempt in range(1, self._policy.max_attempts + 1):
            retry_after = None
            try:
                response = await self._request_once(
                    url, method=method, form=form, headers=headers
                )
            except UrlNotAllowedError as exc:
                # A blocked URL cannot be made acceptable by retrying.
                return FetchResult(
                    error=CollectError(ErrorKind.REJECTED, str(exc), detail=exc.reason)
                )
            except ResponseTooLargeError as exc:
                return FetchResult(
                    error=CollectError(
                        ErrorKind.INVALID_PAYLOAD,
                        f"response from {url} exceeded the size limit",
                        retryable=False,
                        detail=str(exc),
                    )
                )
            except RETRYABLE_EXCEPTIONS as exc:
                error = CollectError(
                    ErrorKind.TIMEOUT
                    if isinstance(exc, httpx.TimeoutException)
                    else ErrorKind.NETWORK,
                    f"{type(exc).__name__} fetching {url}",
                    retryable=True,
                    detail=str(exc) or None,
                )
            except httpx.HTTPError as exc:
                error = CollectError(
                    ErrorKind.NETWORK,
                    f"{type(exc).__name__} fetching {url}",
                    retryable=False,
                    detail=str(exc) or None,
                )
            else:
                outcome, error = self._classify(url, response)
                if outcome is not None:
                    return FetchResult(response=outcome)
                retry_after = response.retry_after

            if error is None or not error.retryable:
                break
            if not retry_http_status and error.kind is ErrorKind.HTTP_STATUS:
                break
            if attempt == self._policy.max_attempts:
                break

            delay = self._policy.delay_for(attempt, retry_after)
            logger.debug(
                "retrying request",
                extra={"url": url, "attempt": attempt, "delay_s": round(delay, 2)},
            )
            await asyncio.sleep(delay)

        assert error is not None  # the loop only exits with an error assigned
        return FetchResult(error=error)

    # -- internals ---------------------------------------------------------------

    async def _request_once(
        self,
        url: str,
        *,
        method: str = "GET",
        form: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> _RawResponse:
        """Perform one streaming request, aborting once the size cap is exceeded."""
        assert self._client is not None

        async with self._client.stream(method, url, data=form, headers=headers) as response:
            content = await _read_capped(response, self._settings.max_response_bytes)

            return _RawResponse(
                response=HttpResponse(
                    url=str(response.url),
                    status_code=response.status_code,
                    content=content,
                    content_type=response.headers.get("content-type", "application/octet-stream"),
                ),
                retry_after=_retry_after_seconds(response.headers),
            )

    def _classify(
        self, url: str, raw: _RawResponse
    ) -> tuple[HttpResponse | None, CollectError | None]:
        """Map a raw response to either a success or a classified error."""
        status = raw.response.status_code

        if status == 429:
            return None, CollectError(
                ErrorKind.RATE_LIMITED,
                f"rate limited by {url} (HTTP 429)",
                retryable=True,
                detail=(
                    f"retry-after={raw.retry_after:g}s" if raw.retry_after is not None else None
                ),
                status_code=status,
            )

        if status in RETRYABLE_STATUSES:
            return None, CollectError(
                ErrorKind.HTTP_STATUS,
                f"HTTP {status} from {url}",
                retryable=True,
                status_code=status,
            )

        if status == 403:
            # A source refusing us is a policy outcome, not a transient fault.
            return None, CollectError(
                ErrorKind.REJECTED,
                f"HTTP 403 from {url}: the source refused the request",
                retryable=False,
                status_code=status,
            )

        if status >= 400:
            return None, CollectError(
                ErrorKind.HTTP_STATUS,
                f"HTTP {status} from {url}",
                retryable=False,
                status_code=status,
            )

        if status >= 300:
            return None, CollectError(
                ErrorKind.HTTP_STATUS,
                f"unexpected redirect (HTTP {status}) from {url}",
                retryable=False,
                status_code=status,
            )

        return raw.response, None


@dataclass(slots=True)
class _RawResponse:
    """A response plus the metadata the classifier needs but a caller does not."""

    response: HttpResponse
    retry_after: float | None = field(default=None)


async def _read_capped(response: httpx.Response, max_bytes: int) -> bytes:
    """Read a body, raising once it exceeds ``max_bytes``.

    The cap is enforced while streaming because ``Content-Length`` is often absent or wrong,
    and because the point is to avoid buffering an unbounded body at all.
    """
    chunks: list[bytes] = []
    total = 0

    async for chunk in response.aiter_bytes():
        total += len(chunk)
        if total > max_bytes:
            raise ResponseTooLargeError(
                f"body exceeded {max_bytes} bytes",
                request=response.request,
            )
        chunks.append(chunk)

    return b"".join(chunks)
