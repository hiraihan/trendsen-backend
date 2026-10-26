"""Tests for the bounded HTTP layer.

All transport is mocked with ``respx``: no test in this file touches the network.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import ErrorKind
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy, build_client
from trendsentinel.config import Settings

URL = "https://feeds.example.test/feed.xml"
API_URL = "https://api.example.test/items.json"


#: A resolver stub keeps the SSRF guard deterministic and DNS-free in tests.
def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


def make_settings(**overrides: object) -> Settings:
    """Settings with retries enabled and no backoff delay, so tests stay fast."""
    return build_settings(**overrides)


def make_fetcher(
    settings: Settings | None = None,
    *,
    client: httpx.AsyncClient | None = None,
    policy: RetryPolicy | None = None,
    allowed_domains: tuple[str, ...] = (),
) -> HttpFetcher:
    """Build a fetcher with a zero-delay policy and a stubbed resolver."""
    resolved = settings or make_settings()
    return HttpFetcher(
        resolved,
        allowed_domains=allowed_domains,
        client=client,
        policy=policy or RetryPolicy(max_attempts=resolved.http_max_retries, base_delay=0.0),
        resolver=public_resolver,
    )


# --------------------------------------------------------------------------- happy path


@respx.mock
async def test_successful_text_fetch() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, text="<rss/>"))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_text(URL)

    assert result.ok
    assert result.response is not None
    assert result.response.status_code == 200
    assert result.response.text == "<rss/>"


@respx.mock
async def test_successful_json_fetch() -> None:
    respx.get(API_URL).mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL)

    assert result.ok
    assert result.response is not None
    assert result.response.json() == [1, 2, 3]


@respx.mock
async def test_json_endpoint_returning_non_json_is_an_invalid_payload() -> None:
    respx.get(API_URL).mock(return_value=httpx.Response(200, text="<html>not json</html>"))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.INVALID_PAYLOAD
    assert result.error.retryable is False


@respx.mock
async def test_charset_from_content_type_is_honoured() -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(
            200,
            content="café".encode("latin-1"),
            headers={"content-type": "text/xml; charset=latin-1"},
        )
    )

    async with make_fetcher() as fetcher:
        result = await fetcher.get_text(URL)

    assert result.ok
    assert result.response is not None
    assert result.response.text == "café"


@respx.mock
async def test_user_agent_is_sent() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, text="ok"))
    settings = make_settings(user_agent_contact="https://example.test/contact")

    async with make_fetcher(settings) as fetcher:
        await fetcher.get_text(URL)

    sent = route.calls.last.request.headers["user-agent"]
    assert sent.startswith("TrendSentinel/")
    assert "https://example.test/contact" in sent


# --------------------------------------------------------------------------- retries


@respx.mock
async def test_timeout_is_retried_and_reported_as_a_timeout() -> None:
    route = respx.get(URL).mock(side_effect=httpx.ReadTimeout("timed out"))
    settings = make_settings(http_max_retries=3)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.TIMEOUT
    assert result.error.retryable is True
    assert route.call_count == 3


@respx.mock
async def test_connect_error_is_retried_as_a_network_error() -> None:
    route = respx.get(URL).mock(side_effect=httpx.ConnectError("connection refused"))
    settings = make_settings(http_max_retries=2)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.NETWORK
    assert result.error.retryable is True
    assert route.call_count == 2


@respx.mock
async def test_retry_that_eventually_succeeds() -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, text="recovered"),
        ]
    )
    settings = make_settings(http_max_retries=3)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert result.ok
    assert result.response is not None
    assert result.response.text == "recovered"
    assert route.call_count == 2


@respx.mock
async def test_rate_limit_is_retried_and_classified() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(429, headers={"retry-after": "1"}))
    settings = make_settings(http_max_retries=2)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.RATE_LIMITED
    assert result.error.retryable is True
    assert "retry-after=1s" in (result.error.detail or "")
    assert route.call_count == 2


@pytest.mark.parametrize("status", [500, 502, 503, 504, 408, 425])
@respx.mock
async def test_retryable_server_statuses(status: int) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(status))
    settings = make_settings(http_max_retries=2)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.HTTP_STATUS
    assert result.error.retryable is True
    assert route.call_count == 2


@pytest.mark.parametrize("status", [400, 401, 404, 410, 422, 418])
@respx.mock
async def test_non_retryable_client_errors_fail_fast(status: int) -> None:
    """A 4xx other than 429 and 408 is a request the server will never accept."""
    route = respx.get(URL).mock(return_value=httpx.Response(status))
    settings = make_settings(http_max_retries=3)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.retryable is False
    assert route.call_count == 1


@respx.mock
async def test_403_is_classified_as_rejected_and_not_retried() -> None:
    """A source refusing the request is a policy outcome, not a transient fault."""
    route = respx.get(URL).mock(return_value=httpx.Response(403))
    settings = make_settings(http_max_retries=3)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.REJECTED
    assert result.error.retryable is False
    assert route.call_count == 1


@respx.mock
async def test_retry_budget_is_respected_exactly() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(503))
    settings = make_settings(http_max_retries=1)

    async with make_fetcher(settings) as fetcher:
        await fetcher.get_text(URL)

    assert route.call_count == 1


# --------------------------------------------------------------------------- size cap


@respx.mock
async def test_oversized_response_is_aborted() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"x" * 5000))
    settings = make_settings(max_response_bytes=1000)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.INVALID_PAYLOAD
    assert result.error.retryable is False


@respx.mock
async def test_oversized_response_is_not_retried() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=b"x" * 5000))
    settings = make_settings(max_response_bytes=1000, http_max_retries=3)

    async with make_fetcher(settings) as fetcher:
        await fetcher.get_text(URL)

    assert route.call_count == 1


@respx.mock
async def test_response_within_the_cap_is_accepted() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"x" * 1000))
    settings = make_settings(max_response_bytes=1000)

    async with make_fetcher(settings) as fetcher:
        result = await fetcher.get_text(URL)

    assert result.ok


# --------------------------------------------------------------------------- SSRF wiring


@respx.mock
async def test_blocked_url_is_rejected_without_any_request() -> None:
    """The guard must fire before transport, not after."""
    route = respx.get("http://169.254.169.254/latest/meta-data/").mock(
        return_value=httpx.Response(200, text="secret")
    )

    async with make_fetcher() as fetcher:
        result = await fetcher.get_text("http://169.254.169.254/latest/meta-data/")

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.REJECTED
    assert route.call_count == 0


@respx.mock
async def test_guard_fires_on_redirect_hops() -> None:
    """A public URL redirecting to an internal one must be stopped."""
    respx.get("https://public.example.test/start").mock(
        return_value=httpx.Response(302, headers={"location": "http://127.0.0.1/secret"})
    )
    internal = respx.get("http://127.0.0.1/secret").mock(
        return_value=httpx.Response(200, text="internal")
    )

    fetcher = HttpFetcher(
        make_settings(),
        policy=RetryPolicy(max_attempts=1, base_delay=0.0),
        resolver=public_resolver,
    )

    async with fetcher:
        result = await fetcher.get_text("https://public.example.test/start")

    assert not result.ok
    # The request to localhost must never have been made.
    assert internal.call_count == 0


# --------------------------------------------------------------------------- client / policy


def test_build_client_applies_timeouts_and_limits() -> None:
    settings = make_settings(http_timeout_seconds=7, max_concurrency=4)

    client = build_client(settings)

    assert client.timeout.connect == 7
    assert client.timeout.read == 7
    assert client.max_redirects == 3


def test_retry_policy_uses_retry_after_when_present() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay=10.0, max_delay=30.0)

    assert policy.delay_for(1, retry_after=2.0) == 2.0
    # The server's instruction is capped, not ignored.
    assert policy.delay_for(1, retry_after=9999.0) == 30.0


def test_retry_policy_backs_off_exponentially_with_jitter() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=1.0, max_delay=100.0)

    # Full jitter means the delay is somewhere in [0, 2^(attempt-1)].
    for attempt in (1, 2, 3, 4):
        delay = policy.delay_for(attempt)
        assert 0.0 <= delay <= 2.0 ** (attempt - 1)


def test_retry_policy_honours_the_maximum_delay() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=5.0)

    assert 0.0 <= policy.delay_for(9) <= 5.0


def test_retry_policy_never_allows_zero_attempts() -> None:
    assert RetryPolicy(max_attempts=0).max_attempts == 1


async def test_fetcher_requires_a_context_manager() -> None:
    fetcher = make_fetcher()

    with pytest.raises(RuntimeError, match="context manager"):
        await fetcher.get_text(URL)


async def test_fetcher_does_not_close_an_injected_client() -> None:
    """A caller-supplied client belongs to the caller."""
    injected = httpx.AsyncClient()
    fetcher = make_fetcher(client=injected)

    async with fetcher:
        assert fetcher._client is injected

    assert not injected.is_closed
    await injected.aclose()


async def test_fetcher_closes_a_client_it_created() -> None:
    fetcher = make_fetcher()

    async with fetcher:
        created = fetcher._client
        assert created is not None

    assert created.is_closed


@respx.mock
async def test_json_error_detail_is_captured() -> None:
    respx.get(API_URL).mock(return_value=httpx.Response(200, content=b"{invalid json"))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL)

    assert result.error is not None
    assert result.error.detail is not None
    assert "invalid" in result.error.detail.lower() or "expecting" in result.error.detail.lower()


@respx.mock
async def test_content_type_is_recorded() -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(
            200, content=json.dumps({"a": 1}), headers={"content-type": "application/json"}
        )
    )

    async with make_fetcher() as fetcher:
        result = await fetcher.get_text(URL)

    assert result.response is not None
    assert result.response.content_type == "application/json"


# --------------------------------------------------------------------------- auth headers


@respx.mock
async def test_get_json_forwards_request_headers() -> None:
    route = respx.get(API_URL).mock(return_value=httpx.Response(200, json={"ok": True}))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL, headers={"Authorization": "Bearer test-token"})

    assert result.ok
    assert route.calls.last.request.headers["authorization"] == "Bearer test-token"


@respx.mock
async def test_request_headers_merge_over_client_defaults() -> None:
    """A caller-supplied header wins, and the configured UA survives unless overridden."""
    route = respx.get(URL).mock(return_value=httpx.Response(200, text="<rss/>"))

    async with make_fetcher() as fetcher:
        await fetcher.get_text(URL, headers={"Accept": "application/rss+xml"})

    request_headers = route.calls.last.request.headers
    assert request_headers["accept"] == "application/rss+xml"
    assert "trendsentinel" in request_headers["user-agent"].lower()


# --------------------------------------------------------------------------- post form


TOKEN_URL = "https://auth.example.test/api/v1/access_token"


@respx.mock
async def test_post_form_sends_an_urlencoded_body_and_headers() -> None:
    route = respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "x", "expires_in": 3600})
    )

    async with make_fetcher() as fetcher:
        result = await fetcher.post_form(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": "platform:app:1.0 (by /u/tester)"},
        )

    assert result.ok
    assert result.response is not None
    assert result.response.json()["access_token"] == "x"

    request = route.calls.last.request
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    assert "grant_type=client_credentials" in request.content.decode()
    assert request.headers["user-agent"] == "platform:app:1.0 (by /u/tester)"


@respx.mock
async def test_post_form_http_error_is_classified_once_without_retry() -> None:
    """A failed token exchange must not be hammered: one request, one error, no retry."""
    route = respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(401, json={"message": "Unauthorized"})
    )

    async with make_fetcher(
        make_settings(http_max_retries=3)
    ) as fetcher:
        result = await fetcher.post_form(TOKEN_URL, data={"grant_type": "client_credentials"})

    assert not result.ok
    assert result.error is not None
    assert result.error.kind is ErrorKind.HTTP_STATUS
    assert not result.error.retryable
    assert route.call_count == 1


@respx.mock
async def test_post_form_transport_error_is_retryable() -> None:
    """Connect failures stay retryable for POST too, so one blip does not fail a run."""
    route = respx.post(TOKEN_URL).mock(side_effect=httpx.ConnectError("boom"))

    async with make_fetcher() as fetcher:
        result = await fetcher.post_form(TOKEN_URL, data={"grant_type": "client_credentials"})

    assert result.error is not None
    assert result.error.kind is ErrorKind.NETWORK
    assert result.error.retryable
    assert route.call_count == 3  # settings default: exhausted budget


@respx.mock
async def test_http_status_errors_carry_the_status_code() -> None:
    """Adapters need the status itself (401 means "refresh the token"), not just a message."""
    respx.get(API_URL).mock(return_value=httpx.Response(401))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL)

    assert result.error is not None
    assert result.error.status_code == 401


@respx.mock
async def test_transport_errors_have_no_status_code() -> None:
    respx.get(API_URL).mock(side_effect=httpx.ConnectError("boom"))

    async with make_fetcher() as fetcher:
        result = await fetcher.get_json(API_URL)

    assert result.error is not None
    assert result.error.status_code is None
