"""Credential-leak review (T26): every surface a credential can leave through is checked.

ADR-0003 puts each per-source credential in ``sources.config`` and relies on
``sanitize_config`` to drop it before a source is serialized. That promise only holds while
the scrubber knows the key names, so the key list here is read from the adapters themselves
(``X_CREDENTIAL_KEYS``, ``TOKEN_CREDENTIAL_KEYS``, ``TOKEN_CONFIG_KEY``): an adapter that
declares a credential the scrubber does not drop fails this file instead of shipping it.

The surfaces follow the checklist in ``docs/tickets/T26-credential-leak-review.md``:

* scrubbing — ``sanitize_config`` drops every declared credential key, keeps the benign ones,
  and the ADR-0003 key names stay in agreement with what the adapters declare;
* ``collector_key`` — scrubbed by the ``key`` marker, the accepted cost recorded in ADR-0001;
* error payloads — the exact ``CollectError.as_dict()`` shape ``collection_runs`` stores never
  carries a credential, not even when the platform echoes one back or embeds it in a paging
  URL;
* logs — a refused run at DEBUG emits no credential and never prints a request header;
* rotation — a rotated credential takes effect on the next run from the per-run
  ``SourceRef``, and a rotated Reddit secret invalidates the cached token;
* hygiene — no file in the repo carries a credential literal that does not announce itself as
  a placeholder.
"""

from __future__ import annotations

import ast
import base64
import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import httpx
import pytest
import respx

from tests.helpers import build_settings
from trendsentinel.collectors.base import CollectionResult, ErrorKind, SourceRef
from trendsentinel.collectors.http import HttpFetcher, RetryPolicy
from trendsentinel.collectors.reddit import (
    TOKEN_CREDENTIAL_KEYS,
    TOKEN_URL,
    RedditCollector,
    clear_token_cache,
)
from trendsentinel.collectors.threads import TOKEN_CONFIG_KEY, ThreadsCollector
from trendsentinel.collectors.x import X_CREDENTIAL_KEYS, XCollector
from trendsentinel.config import Settings
from trendsentinel.logging import REDACTED
from trendsentinel.schemas.sources import sanitize_config

#: The credential keys the authenticated adapters declare. Read from the adapters so a new
#: credential cannot be added without this file noticing.
CREDENTIAL_KEYS: tuple[str, ...] = (
    *X_CREDENTIAL_KEYS,
    *TOKEN_CREDENTIAL_KEYS,
    TOKEN_CONFIG_KEY,
)

#: Probe values. Distinctive on purpose: a leak is unmistakable and no code path produces one
#: by accident. The ``PROBE`` marker doubles as the placeholder marker of the hygiene tests.
PROBE: dict[str, str] = {
    "x_bearer_token": "PROBE-X-BEARER-7f3c1a",
    "reddit_client_id": "PROBE-REDDIT-CLIENT-ID-7f3c1a",
    "reddit_client_secret": "PROBE-REDDIT-CLIENT-SECRET-7f3c1a",
    "reddit_user_agent": "trendsentinel:probe:1.0 (by /u/probe)",
    "threads_access_token": "PROBE-THREADS-TOKEN-7f3c1a",
}

#: A rotated credential, as an operator's ``UPDATE sources SET config = …`` would install it.
ROTATED: dict[str, str] = {
    "x_bearer_token": "PROBE-X-BEARER-ROTATED-7f3c1a",
    "reddit_client_secret": "PROBE-REDDIT-CLIENT-SECRET-ROTATED-7f3c1a",
    "threads_access_token": "PROBE-THREADS-TOKEN-ROTATED-7f3c1a",
}

#: Operator-facing configuration. Every one of these must survive scrubbing.
BENIGN: dict[str, Any] = {"limit": 25, "interval_minutes": 45, "query": "trendsentinel"}

X_BASE = "https://api.twitter.com"
X_SEARCH = f"{X_BASE}/2/tweets/search/recent"
REDDIT_BASE = "https://oauth.reddit.com/r/technology"
REDDIT_LISTING = f"{REDDIT_BASE}/new"
THREADS_BASE = "https://graph.threads.net/v1.0"

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
ADRS = ROOT / "docs" / "adr"


class _Adapter(Protocol):
    """The contract the service layer relies on (``collectors/base.py``)."""

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult: ...


type Factory = Callable[[Settings, HttpFetcher], _Adapter]


@pytest.fixture(autouse=True)
def isolated_token_cache() -> None:
    """The in-process token cache is module state; no test may inherit a token."""
    clear_token_cache()


def public_resolver(_hostname: str) -> list[str]:
    return ["93.184.216.34"]


async def run(factory: Factory, source: SourceRef, limit: int = 10) -> CollectionResult:
    """Drive one adapter through a real fetcher, exactly as the service layer does."""
    settings = build_settings()
    policy = RetryPolicy(max_attempts=settings.http_max_retries, base_delay=0.0)
    async with HttpFetcher(settings, policy=policy, resolver=public_resolver) as fetcher:
        return await factory(settings, fetcher).collect(source, limit)


def fixture(name: str) -> httpx.Response:
    return httpx.Response(200, content=(FIXTURES / name).read_bytes())


def errors_payload(result: CollectionResult) -> str:
    """The payload ``_error_summary`` stores in ``collection_runs.error_summary``."""
    return json.dumps([error.as_dict() for error in result.errors])


def x_source(**overrides: Any) -> SourceRef:
    config: dict[str, Any] = {
        "collector_key": "x",
        "x_bearer_token": PROBE["x_bearer_token"],
        "query": "trendsentinel",
        **overrides,
    }
    return SourceRef(base_url=X_BASE, key="x_trendsentinel", config=config)


def reddit_source(**overrides: Any) -> SourceRef:
    config: dict[str, Any] = {
        "collector_key": "reddit",
        "reddit_client_id": PROBE["reddit_client_id"],
        "reddit_client_secret": PROBE["reddit_client_secret"],
        "reddit_user_agent": PROBE["reddit_user_agent"],
        **overrides,
    }
    return SourceRef(base_url=REDDIT_BASE, key="reddit_technology", config=config)


def threads_source(**overrides: Any) -> SourceRef:
    config: dict[str, Any] = {
        "collector_key": "threads",
        "threads_access_token": PROBE["threads_access_token"],
        **overrides,
    }
    return SourceRef(base_url=THREADS_BASE, key="threads_profile", config=config)


# --------------------------------------------------------------------------- scrubbing


def test_the_probe_covers_every_declared_credential_key() -> None:
    """A new credential key fails here until it is declared and probed by name."""
    assert sorted(CREDENTIAL_KEYS) == sorted(PROBE)


def test_every_collector_module_declares_its_credentials_to_this_suite() -> None:
    """The convention is enforced, not just documented: a new adapter cannot slip past.

    Every module under ``collectors/`` that declares credentials does so as a module-level
    ``*_CREDENTIAL_KEYS`` tuple or a ``TOKEN_CONFIG_KEY`` string (see
    ``docs/adding-a-collector.md`` Case 3). This parses those declarations and requires each
    to appear in :data:`CREDENTIAL_KEYS`, so an adapter that adds a credential without adding
    it here fails with the module's own key name.
    """
    discovered: set[str] = set()
    for path in sorted((ROOT / "src" / "trendsentinel" / "collectors").glob("*.py")):
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            target: ast.expr | None = None
            value_node: ast.expr | None = None
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                value_node = node.value
            elif isinstance(node, ast.AnnAssign):
                target = node.target
                value_node = node.value
            if not isinstance(target, ast.Name) or value_node is None:
                continue
            if not (target.id.endswith("_CREDENTIAL_KEYS") or target.id == "TOKEN_CONFIG_KEY"):
                continue
            try:
                value = ast.literal_eval(value_node)
            except ValueError:  # computed at runtime; nothing static to check
                continue
            if isinstance(value, str):
                discovered.add(value)
            elif isinstance(value, (tuple, list)):
                discovered.update(entry for entry in value if isinstance(entry, str))

    assert discovered == set(CREDENTIAL_KEYS), (
        "a collector declares credentials this suite does not probe; add its constant to "
        "CREDENTIAL_KEYS and PROBE in this file (and the key to an ADR)"
    )


@pytest.mark.parametrize("key", sorted(PROBE))
def test_sanitize_config_drops_a_declared_credential_key(key: str) -> None:
    """ADR-0003's contract: ``GET /sources`` may never serialize a credential."""
    config: dict[str, Any] = {**BENIGN, key: PROBE[key]}

    scrubbed = sanitize_config(config)

    assert scrubbed is not None
    assert key not in scrubbed
    assert PROBE[key] not in json.dumps(scrubbed)
    assert scrubbed == BENIGN


def test_a_config_carrying_every_credential_returns_only_the_benign_keys() -> None:
    """The review's own scenario: five credentials, three benign keys, one source."""
    config: dict[str, Any] = {**BENIGN, **PROBE}

    assert sanitize_config(config) == BENIGN


def test_collector_key_is_scrubbed_and_that_cost_is_recorded() -> None:
    """ADR-0001 accepted that the ``key`` marker hides the routing key too."""
    assert sanitize_config({"collector_key": "reddit", **BENIGN}) == BENIGN


def test_sanitize_config_does_not_mutate_the_stored_config() -> None:
    """It returns a copy: the row in the database keeps its credential."""
    config: dict[str, Any] = {**BENIGN, **PROBE}

    sanitize_config(config)

    assert PROBE["threads_access_token"] in config.values()


def test_every_credential_key_is_documented_in_the_adrs() -> None:
    """Docs ↔ code: a renamed or added key must be recorded in an ADR in the same change."""
    adr_text = "\n".join(path.read_text(encoding="utf-8") for path in sorted(ADRS.glob("*.md")))
    for key in CREDENTIAL_KEYS:
        assert f"`{key}`" in adr_text, f"{key} is not documented in docs/adr/"


# --------------------------------------------------------------------------- error payloads


@respx.mock
async def test_an_x_401_never_reaches_the_recorded_error_payload() -> None:
    """The bearer travels in the Authorization header and appears nowhere else."""
    route = respx.get(url__startswith=X_SEARCH).mock(return_value=httpx.Response(401))

    result = await run(XCollector, x_source())

    assert route.calls[0].request.headers["authorization"] == f"Bearer {PROBE['x_bearer_token']}"
    assert PROBE["x_bearer_token"] not in str(route.calls[0].request.url)
    [error] = result.errors
    assert error.kind is ErrorKind.REJECTED and error.status_code == 401
    assert PROBE["x_bearer_token"] not in errors_payload(result)


@respx.mock
async def test_a_platform_message_echoing_the_bearer_is_redacted() -> None:
    """X surfaces its own message as ``detail``; an echoed credential is not recorded."""
    respx.get(url__startswith=X_SEARCH).mock(
        return_value=httpx.Response(
            200, json={"errors": [{"message": f"invalid bearer token {PROBE['x_bearer_token']}"}]}
        )
    )

    result = await run(XCollector, x_source())

    error = result.errors[0]
    assert error.kind is ErrorKind.INVALID_PAYLOAD
    assert PROBE["x_bearer_token"] not in errors_payload(result)
    assert REDACTED in (error.detail or "")


@respx.mock
async def test_a_reddit_token_refusal_never_reaches_the_recorded_error_payload() -> None:
    """A 401 whose body echoes the credentials back is not stored, nor is the Basic blob."""
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(
            401, json={"error": "invalid_client", "echo": PROBE["reddit_client_secret"]}
        )
    )

    result = await run(RedditCollector, reddit_source())

    [error] = result.errors
    assert error.kind is ErrorKind.REJECTED and not error.retryable
    payload = errors_payload(result)
    for key in TOKEN_CREDENTIAL_KEYS:
        assert PROBE[key] not in payload
    assert _basic(f"Basic {_basic_blob()}") not in payload


@respx.mock
async def test_a_threads_401_never_reaches_the_recorded_error_payload() -> None:
    """The rotation hint names the config key; the token value itself stays out."""
    route = respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        return_value=httpx.Response(401)
    )

    result = await run(ThreadsCollector, threads_source())

    assert route.calls[0].request.headers["authorization"] == (
        f"Bearer {PROBE['threads_access_token']}"
    )
    assert PROBE["threads_access_token"] not in str(route.calls[0].request.url)
    [error] = result.errors
    assert error.kind is ErrorKind.REJECTED and error.status_code == 401
    assert PROBE["threads_access_token"] not in errors_payload(result)


@respx.mock
async def test_a_paging_url_embedding_the_token_never_reaches_an_error() -> None:
    """Meta's ``paging.next`` embeds the access token; only the cursor is used."""
    page1 = json.loads((FIXTURES / "threads" / "threads_page1.json").read_text(encoding="utf-8"))
    page1["paging"]["next"] = (
        f"{THREADS_BASE}/me/threads?after=pagination-cursor-page-2"
        f"&access_token={PROBE['threads_access_token']}"
    )
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(return_value=fixture("threads/me_profile.json"))
    page_route = respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        side_effect=[httpx.Response(200, json=page1), httpx.Response(404)]
    )

    result = await run(ThreadsCollector, threads_source())

    assert result.errors, "the failing page must be reported"
    assert PROBE["threads_access_token"] not in errors_payload(result)
    second_url = str(page_route.calls[1].request.url)
    assert "after=pagination-cursor-page-2" in second_url
    assert "access_token" not in second_url


# --------------------------------------------------------------------------- logs


@respx.mock
async def test_no_log_record_carries_a_credential(caplog: pytest.LogCaptureFixture) -> None:
    """Credentials travel in headers; the request path logs URLs, attempts and delays."""
    respx.get(url__startswith=X_SEARCH).mock(return_value=httpx.Response(401))
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(401))
    respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(return_value=httpx.Response(401))

    with caplog.at_level(logging.DEBUG):
        await run(XCollector, x_source())
        await run(RedditCollector, reddit_source())
        await run(ThreadsCollector, threads_source())

    for record in caplog.records:
        rendered = f"{record.name}: {record.getMessage()} | {record.__dict__!r}"
        for value in PROBE.values():
            assert value not in rendered, f"{record.name} logged a credential"
        assert "authorization" not in rendered.lower(), f"{record.name} logged a request header"


# --------------------------------------------------------------------------- rotation


@respx.mock
async def test_a_rotated_x_bearer_token_is_used_on_the_next_run() -> None:
    """The credential is read from the per-run ``SourceRef``, never bound at import time."""
    route = respx.get(url__startswith=X_SEARCH).mock(
        side_effect=[
            httpx.Response(401),
            fixture("x/recent_search_page2.json"),
        ]
    )

    refused = await run(XCollector, x_source())
    rotated = await run(XCollector, x_source(**{"x_bearer_token": ROTATED["x_bearer_token"]}))

    assert not refused.ok
    assert rotated.ok
    assert route.calls[0].request.headers["authorization"] == f"Bearer {PROBE['x_bearer_token']}"
    assert route.calls[1].request.headers["authorization"] == (
        f"Bearer {ROTATED['x_bearer_token']}"
    )


@respx.mock
async def test_a_rotated_threads_token_is_used_on_the_next_run() -> None:
    """Same guarantee for the long-lived token, which operators rotate quarterly."""
    profile_route = respx.get(url__startswith=f"{THREADS_BASE}/me?").mock(
        side_effect=[
            httpx.Response(401),
            fixture("threads/me_profile.json"),
        ]
    )
    respx.get(url__startswith=f"{THREADS_BASE}/me/threads").mock(
        return_value=fixture("threads/threads_page2.json")
    )

    refused = await run(ThreadsCollector, threads_source())
    rotated = await run(
        ThreadsCollector,
        threads_source(**{"threads_access_token": ROTATED["threads_access_token"]}),
    )

    assert not refused.ok
    assert rotated.ok
    assert profile_route.calls[0].request.headers["authorization"] == (
        f"Bearer {PROBE['threads_access_token']}"
    )
    assert profile_route.calls[1].request.headers["authorization"] == (
        f"Bearer {ROTATED['threads_access_token']}"
    )


@respx.mock
async def test_a_rotated_reddit_secret_invalidates_the_cached_token() -> None:
    """The token cache is keyed by (client id, secret); rotation cannot reuse its token."""
    token_route = respx.post(TOKEN_URL).mock(
        side_effect=[
            httpx.Response(
                200, json={"access_token": "probe-token-for-old-secret", "expires_in": 3600}
            ),
            httpx.Response(
                200, json={"access_token": "probe-token-for-rotated-secret", "expires_in": 3600}
            ),
        ]
    )
    listing_route = respx.get(url__startswith=REDDIT_LISTING).mock(
        return_value=fixture("reddit/listing_page2.json")
    )

    first = await run(RedditCollector, reddit_source())
    rotated_secret = ROTATED["reddit_client_secret"]
    second = await run(RedditCollector, reddit_source(**{"reddit_client_secret": rotated_secret}))

    assert first.ok and second.ok
    assert token_route.call_count == 2, "a rotated secret must force a fresh exchange"
    assert _basic(token_route.calls[0].request.headers["authorization"]) == (
        f"{PROBE['reddit_client_id']}:{PROBE['reddit_client_secret']}"
    )
    assert _basic(token_route.calls[1].request.headers["authorization"]).endswith(rotated_secret)
    assert listing_route.calls[0].request.headers["authorization"] == (
        "Bearer probe-token-for-old-secret"
    )
    assert listing_route.calls[1].request.headers["authorization"] == (
        "Bearer probe-token-for-rotated-secret"
    )


def _basic(header_value: str) -> str:
    """Decode a ``Basic`` Authorization header value back to its credentials."""
    scheme, _, blob = header_value.partition(" ")
    assert scheme.lower() == "basic"
    return base64.b64decode(blob).decode()


def _basic_blob() -> str:
    raw = f"{PROBE['reddit_client_id']}:{PROBE['reddit_client_secret']}".encode()
    return base64.b64encode(raw).decode()


# --------------------------------------------------------------------------- hygiene


#: Values that announce themselves as unusable. Every credential-shaped literal in the repo
#: must carry one, so a real value can never land here without a test failing.
PLACEHOLDER_MARKER = re.compile(
    r"(?i)placeholder|redacted|conformance|probe|bad-|example|dummy|fake|sample|test"
    r"|trendsentinel|<|\$\{"
)

CREDENTIAL_LITERAL = re.compile(
    r"(?i)\b(?:x_bearer_token|reddit_client_id|reddit_client_secret|reddit_user_agent"
    r"|threads_access_token|access_token|api_key|client_secret|bearer_token)"
    r"['\"]?\s*[:=]\s*['\"]([^'\"]{4,})['\"]"
)

CREDENTIAL_QUERY_VALUE = re.compile(r"(?i)(access_token|api_key|client_secret)=([^\s\"'&<>]{4,})")

SCANNED_SUFFIXES = frozenset({".py", ".md", ".json", ".sql", ".toml"})


def test_no_repo_file_carries_a_credential_literal_that_is_not_a_placeholder() -> None:
    """Checklist 5: a leaked key shows up as a literal; a placeholder announces itself."""
    offenders: list[str] = []
    for name in ("src", "migrations", "tests", "docs"):
        for path in sorted((ROOT / name).rglob("*")):
            if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for match in CREDENTIAL_LITERAL.finditer(text):
                if not PLACEHOLDER_MARKER.search(match.group(1)):
                    offenders.append(f"{path.relative_to(ROOT)}: {match.group(0)}")
    assert not offenders, "credential-shaped literals must declare themselves placeholders"


def test_fixture_credentials_are_unusable_placeholders() -> None:
    """Fixtures document a credential's *shape*, never a value that could work."""
    offenders: list[str] = []
    for path in sorted(FIXTURES.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in CREDENTIAL_QUERY_VALUE.finditer(text):
            if not PLACEHOLDER_MARKER.search(match.group(2)):
                offenders.append(f"{path.name}: {match.group(0)}")
    assert not offenders
