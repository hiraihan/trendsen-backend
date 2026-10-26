"""``GET /health``: liveness plus database reachability.

The route deliberately depends on the pool holder rather than on a pooled connection: when
the database is unreachable, acquiring a connection raises, and the response would be a 500
from the catch-all handler instead of the documented 503.
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from trendsentinel import __version__
from trendsentinel.api.deps import DatabaseDep
from trendsentinel.schemas.items import HealthResponse

__all__ = ["router"]

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness and database reachability",
    responses={503: {"description": "The database is unreachable"}},
)
async def health(response: Response, database: DatabaseDep) -> HealthResponse:
    """Report process liveness and whether the database answers a trivial query."""
    reachable = await database.ping()
    if not reachable:
        response.status_code = 503

    return HealthResponse(
        status="ok" if reachable else "degraded",
        database="ok" if reachable else "unreachable",
        version=__version__,
    )
