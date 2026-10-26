"""FastAPI dependencies: the database handle and the admin key check."""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request

from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.config import Settings
from trendsentinel.db.pool import Connection, Database
from trendsentinel.services.collection import CollectionService

__all__ = [
    "AdminKey",
    "CollectionServiceDep",
    "DatabaseDep",
    "DbSession",
    "SettingsDep",
    "get_collection_service",
    "get_db",
    "get_settings_dep",
    "require_admin_key",
]


def get_settings_dep(request: Request) -> Settings:
    """Return the settings the app was built with.

    Read from ``app.state`` rather than ``get_settings()`` so a test app built with explicit
    settings is fully self-consistent: nothing in the request path can fall back to the
    process environment.
    """
    settings: Settings = request.app.state.settings
    return settings


def get_db(request: Request) -> Database:
    """Return the process-wide connection pool holder from application state."""
    database: Database = request.app.state.database
    return database


def get_collection_service(request: Request) -> CollectionService:
    """Return the collection service held on application state."""
    service: CollectionService = request.app.state.collection_service
    return service


async def get_connection(request: Request) -> AsyncIterator[Connection]:
    """Yield a pooled connection for the duration of one request.

    Routes depend on this rather than on the pool, so a route function cannot leak a
    connection by forgetting to release one.
    """
    database = get_db(request)
    async with database.pool.connection() as conn:
        yield conn


async def require_admin_key(
    request: Request,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    """Authorize a state-changing request against ``settings.admin_api_key``.

    Fails closed: when no key is configured, every request is rejected. A blank configured key
    must never mean "no authentication required".

    The comparison is constant-time, so a caller cannot learn the key one character at a time
    from response timing.
    """
    settings = get_settings_dep(request)
    expected = settings.admin_api_key.get_secret_value().strip()

    if not expected:
        raise ApiError(
            ErrorCode.UNAUTHORIZED,
            "Authentication is not configured on this server",
            status_code=401,
        )

    supplied = (x_api_key or "").strip()
    if not supplied or not secrets.compare_digest(supplied, expected):
        raise ApiError(
            ErrorCode.UNAUTHORIZED,
            "A valid X-API-Key header is required",
            status_code=401,
        )


#: Convenience aliases so route signatures read as types rather than as ``Depends(...)``.
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
DatabaseDep = Annotated[Database, Depends(get_db)]
DbSession = Annotated[Connection, Depends(get_connection)]
AdminKey = Annotated[None, Depends(require_admin_key)]
CollectionServiceDep = Annotated[CollectionService, Depends(get_collection_service)]
