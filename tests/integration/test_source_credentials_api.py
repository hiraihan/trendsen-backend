"""The credential-provisioning API.

Endpoints: ``GET /sources/credentials`` and ``POST /sources/{key}/credentials``.

The flow these tests pin down is the web setup flow: an operator opens the SPA's Setup page,
enters the admin key once, pastes the Reddit/X/Threads credentials into a form, and the source
activates - no SQL, no shell. The ticket-level live-run gates (T22-T24) are unchanged: they
need real platform accounts.

Every test proves one guarantee the security review (T26) would ask for:

* the status endpoint lists only the adapter-declared keys, with a boolean per key, and is
  admin-key-guarded (ADR-0006 as amended);
* a provisioned value is stored but never returned - no endpoint, no list response;
* unknown keys are refused (allowlist from the adapters, not a free-form config write);
* unknown sources are a 404;
* auth is the same fail-closed admin key as the other write endpoints;
* activation on provision is explicit and reflected by the next ``GET /sources``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI
from psycopg import AsyncConnection

from tests.helpers import build_settings
from trendsentinel.collectors.reddit import TOKEN_CREDENTIAL_KEYS
from trendsentinel.collectors.threads import TOKEN_CONFIG_KEY
from trendsentinel.collectors.x import X_CREDENTIAL_KEYS
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.main import create_app

pytestmark = pytest.mark.integration

ADMIN_KEY = "test-admin-key"

#: The full allowlist, read from the adapters - same source of truth as T26's leak review.
ALL_KEYS: tuple[str, ...] = (*X_CREDENTIAL_KEYS, *TOKEN_CREDENTIAL_KEYS, TOKEN_CONFIG_KEY)

PROBE: dict[str, str] = {
    "reddit_client_id": "PROBE-REDDIT-CLIENT-ID-9d2e7b",
    "reddit_client_secret": "PROBE-REDDIT-CLIENT-SECRET-9d2e7b",
    "reddit_user_agent": "trendsentinel:setup-api-test:1.0 (by /u/probe)",
}

HEADERS = {"X-API-Key": ADMIN_KEY}


@pytest.fixture
def api_settings(migrated_test_db: str) -> Settings:
    return build_settings(database_url=migrated_test_db, admin_api_key=ADMIN_KEY)


@pytest.fixture
def api_app(api_settings: Settings) -> FastAPI:
    """A freshly built app, built with explicit settings."""
    return create_app(api_settings)


@pytest.fixture
async def client(api_app: FastAPI, database: Database) -> AsyncIterator[httpx.AsyncClient]:
    """An ASGI client bound to a freshly built app and the migrated test database."""
    transport = httpx.ASGITransport(app=api_app, raise_app_exceptions=False)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client,
        api_app.router.lifespan_context(api_app),
    ):
        yield http_client


async def _stored_config(database: Database, key: str) -> dict[str, object]:
    """Read the raw stored config straight from the database (not through the API)."""
    async with database.pool.connection() as conn:
        assert isinstance(conn, AsyncConnection)
        async with conn.cursor() as cur:
            await cur.execute("SELECT config FROM sources WHERE key = %(key)s", {"key": key})
            row = await cur.fetchone()
    assert row is not None
    return dict(row["config"])


# --------------------------------------------------------------------------- GET status


async def test_status_lists_every_declared_key_per_source(client: httpx.AsyncClient) -> None:
    """Every adapter-declared key shows up exactly once per matching source."""
    response = await client.get("/sources/credentials", headers=HEADERS)
    assert response.status_code == 200

    body = response.json()
    by_key = {source["source_key"]: source for source in body["sources"]}

    reddit = by_key["reddit_technology"]
    assert sorted(reddit["required_keys"]) == sorted(TOKEN_CREDENTIAL_KEYS)
    assert all(reddit["keys"][name] is False for name in TOKEN_CREDENTIAL_KEYS)

    x_source = by_key["x_trendsentinel"]
    assert sorted(x_source["required_keys"]) == sorted(X_CREDENTIAL_KEYS)

    threads = by_key["threads_profile"]
    assert sorted(threads["required_keys"]) == sorted([TOKEN_CONFIG_KEY])


async def test_status_reflects_a_provisioned_key(
    client: httpx.AsyncClient, database: Database
) -> None:
    """Once a key is stored, the status endpoint reports it as set - the value itself never."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 200

    status_response = await client.get("/sources/credentials", headers=HEADERS)
    reddit = next(
        s for s in status_response.json()["sources"] if s["source_key"] == "reddit_technology"
    )
    assert reddit["keys"]["reddit_client_id"] is True
    assert set(reddit["keys"]) == set(TOKEN_CREDENTIAL_KEYS)
    # The response carries booleans only - never the stored values.
    assert all(isinstance(v, bool) for v in reddit["keys"].values())


async def test_status_without_admin_key_is_401(client: httpx.AsyncClient) -> None:
    """The readiness map is operational information: it sits behind the admin key."""
    response = await client.get("/sources/credentials")
    assert response.status_code == 401


async def test_status_leaks_no_value_even_authorized(client: httpx.AsyncClient) -> None:
    """Authorized or not, the response carries booleans only — never a stored value."""
    response = await client.get("/sources/credentials", headers=HEADERS)
    assert response.status_code == 200
    text = json.dumps(response.json())
    assert "PROBE" not in text


# --------------------------------------------------------------------------- POST provision


async def test_provision_stores_keys_and_reports_sanitized_config(
    client: httpx.AsyncClient, database: Database
) -> None:
    """The values land in ``sources.config`` and are scrubbed from the response."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source_key"] == "reddit_technology"
    assert set(body["configured_keys"]) == set(PROBE)
    assert "PROBE" not in json.dumps(body)

    stored = await _stored_config(database, "reddit_technology")
    for name, value in PROBE.items():
        assert stored[name] == value


async def test_provision_activates_the_source_when_asked(
    client: httpx.AsyncClient, database: Database
) -> None:
    """``activate: true`` flips ``is_active`` in the same transaction."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": True},
        headers=HEADERS,
    )
    assert response.status_code == 200
    assert body_is_active(response) is True

    sources_response = await client.get("/sources")
    reddit = next(s for s in sources_response.json()["sources"] if s["key"] == "reddit_technology")
    assert reddit["is_active"] is True
    # And the public config view still scrubs every provisioned key.
    assert not (set(reddit["config"]) & set(PROBE))


async def test_provision_without_activation_leaves_source_inactive(
    client: httpx.AsyncClient,
) -> None:
    """Credentials can be staged before the source goes live."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 200
    assert body_is_active(response) is False


async def test_provision_preserves_benign_config_keys(
    client: httpx.AsyncClient, database: Database
) -> None:
    """A merge, not a replace: ``limit`` and friends survive the update."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 200

    stored = await _stored_config(database, "reddit_technology")
    assert stored["limit"] == 25
    assert stored["collector_key"] == "reddit"


async def test_provision_rejects_unknown_keys(client: httpx.AsyncClient) -> None:
    """A key no adapter declares is refused: the endpoint is not a free-form config write."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={
            "credentials": {"not_a_credential": "PROBE-not-a-credential-9d2e7b"},
            "activate": False,
        },
        headers=HEADERS,
    )
    assert response.status_code == 422
    stored = json.dumps(response.json())
    assert "PROBE-not-a-credential" not in stored


async def test_provision_rejects_empty_credentials(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": {}, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 422


async def test_provision_rejects_blank_values(client: httpx.AsyncClient) -> None:
    """A whitespace value would store a credential that is not one; refuse it."""
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": {"reddit_client_id": "   "}, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 422


async def test_provision_unknown_source_is_404(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/sources/no_such_source/credentials",
        json={"credentials": PROBE, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 404


async def test_provision_maps_foreign_keys_to_their_own_source(
    client: httpx.AsyncClient, database: Database
) -> None:
    """X keys cannot be planted under the Reddit source and vice versa.

    Each source accepts only the keys its own adapter declares, so a pasted-in-the-wrong-box
    value fails loudly instead of silently arming the wrong adapter.
    """
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": {"x_bearer_token": "PROBE-X-BEARER-9d2e7b"}, "activate": False},
        headers=HEADERS,
    )
    assert response.status_code == 422


async def test_provision_rotates_an_existing_value(
    client: httpx.AsyncClient, database: Database
) -> None:
    """Posting again overwrites: the rotation path of the runbook, now via the web."""
    first = dict(PROBE, reddit_client_secret="PROBE-REDDIT-SECRET-FIRST-9d2e7b")
    second = dict(PROBE, reddit_client_secret="PROBE-REDDIT-SECRET-SECOND-9d2e7b")

    first_response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": first, "activate": False},
        headers=HEADERS,
    )
    second_response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": second, "activate": False},
        headers=HEADERS,
    )
    assert first_response.status_code == 200
    assert second_response.status_code == 200

    stored = await _stored_config(database, "reddit_technology")
    assert stored["reddit_client_secret"] == second["reddit_client_secret"]


# --------------------------------------------------------------------------- auth


async def test_provision_without_admin_key_is_401(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
    )
    assert response.status_code == 401


async def test_provision_with_wrong_admin_key_is_401(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/sources/reddit_technology/credentials",
        json={"credentials": PROBE, "activate": False},
        headers={"X-API-Key": "wrong-key"},
    )
    assert response.status_code == 401


def body_is_active(response: httpx.Response) -> bool:
    """Read ``is_active`` from a provision response body."""
    data: dict[str, object] = response.json()
    return data["is_active"] is True
