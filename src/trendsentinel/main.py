"""FastAPI application factory.

The API process is stateless and never schedules collection: the scheduler lives in the
worker process (``trendsentinel.worker``), so adding API replicas cannot duplicate jobs.

``POST /collect/run`` only queues a run row; the worker executes it. The API therefore never
blocks a request on a third-party network round trip.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from trendsentinel import __version__
from trendsentinel.api.errors import register_exception_handlers
from trendsentinel.api.routes import (
    collect,
    export,
    health,
    items,
    source_credentials,
    sources,
)
from trendsentinel.config import Settings, get_settings
from trendsentinel.db.pool import Database
from trendsentinel.logging import configure_logging, get_logger
from trendsentinel.services.collection import build_collection_service

__all__ = ["create_app"]

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the connection pool on startup and close it on shutdown.

    The collection service is built here too, but it holds no connection of its own: it
    acquires one per operation from the same pool.
    """
    settings: Settings = app.state.settings
    database = Database(settings)

    await database.open()
    app.state.database = database
    app.state.collection_service = build_collection_service(settings, database)

    try:
        yield
    finally:
        await database.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application.

    Passing settings explicitly is what makes the app constructible in tests without touching
    the process environment.
    """
    resolved = settings or get_settings()
    configure_logging(resolved)
    resolved.assert_runtime_ready()

    app = FastAPI(
        title="TrendSentinel Scraper",
        version=__version__,
        summary=(
            "Data collection for the TrendSentinel project: collection, normalization, "
            "validation, storage and monitoring."
        ),
        lifespan=lifespan,
    )
    app.state.settings = resolved

    # Explicit origin allowlist for the SPA (spec v1.1). Empty list = no cross-origin
    # request is served. Credentials are never allowed; the admin key travels in a header
    # the operator enters, not in a cookie.
    if resolved.cors_origins_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=resolved.cors_origins_list,
            allow_methods=["GET", "POST", "PATCH"],
            allow_headers=["X-API-Key", "content-type", "accept"],
            allow_credentials=False,
        )

    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(sources.router)
    app.include_router(source_credentials.router)
    app.include_router(items.router)
    app.include_router(collect.router)
    app.include_router(export.router)

    return app


def __getattr__(name: str) -> FastAPI:
    """Build the ASGI app on first access to ``trendsentinel.main:app``.

    Deferring this keeps importing the module free of side effects, so tests can import
    :func:`create_app` and build an app from explicit settings without the process environment
    having to supply a full configuration. Uvicorn's ``trendsentinel.main:app`` target still
    resolves through this hook.
    """
    if name == "app":
        return create_app()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
