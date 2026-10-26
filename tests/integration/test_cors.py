"""The SPA middleware is wired exactly as spec v1.1 §7 requires.

Default is no CORS middleware at all: the allowlist is empty unless the operator lists
origins. Listing origins adds no capability — the admin key on POST /collect/run is
unchanged, and credentials are never allowed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI

from tests.helpers import build_settings
from trendsentinel.main import create_app


@pytest.fixture()
def app_without_cors(migrated_test_db: str) -> FastAPI:
    return create_app(build_settings(database_url=migrated_test_db))


@pytest.fixture()
def app_with_cors(migrated_test_db: str) -> FastAPI:
    return create_app(
        build_settings(
            database_url=migrated_test_db,
            cors_origins="http://localhost:5173",
        )
    )


@pytest.fixture()
async def cors_client(app_with_cors: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app_with_cors, raise_app_exceptions=False)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client,
        app_with_cors.router.lifespan_context(app_with_cors),
    ):
        yield http_client


async def test_no_middleware_by_default(app_without_cors: FastAPI) -> None:
    assert app_without_cors.user_middleware == []


async def test_preflight_allowed_origin_gets_explicit_grant(
    cors_client: httpx.AsyncClient,
) -> None:
    response = await cors_client.options(
        "/items",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"
    # allow_credentials=False: the header must be absent, not "false" — the correct CORS
    # signal for "no credentials" is not sending it at all.
    assert "access-control-allow-credentials" not in response.headers


async def test_preflight_unknown_origin_gets_no_grant(
    cors_client: httpx.AsyncClient,
) -> None:
    response = await cors_client.options(
        "/items",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert "access-control-allow-origin" not in response.headers


async def test_api_key_header_is_preflight_allowed(
    cors_client: httpx.AsyncClient,
) -> None:
    response = await cors_client.options(
        "/collect/run",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "x-api-key",
        },
    )
    assert response.status_code == 200
    assert "x-api-key" in response.headers["access-control-allow-headers"].lower()
