"""Async connection pool.

The pool is opened on application startup and closed on shutdown. Nothing connects at import
time, so importing a module never requires a reachable database.
"""

from __future__ import annotations

from types import TracebackType

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from trendsentinel.config import Settings
from trendsentinel.db.repositories.health import check_database
from trendsentinel.logging import get_logger

__all__ = ["Database"]

logger = get_logger(__name__)

#: The pool always yields connections configured with the mapping row factory.
Connection = AsyncConnection[DictRow]


class Database:
    """Owns the process-wide connection pool."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: AsyncConnectionPool[Connection] = AsyncConnectionPool(
            conninfo=str(settings.database_url),
            min_size=1,
            max_size=settings.max_concurrency + 2,
            open=False,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )

    @property
    def pool(self) -> AsyncConnectionPool[Connection]:
        """The underlying pool, for repositories to acquire connections from."""
        return self._pool

    async def open(self) -> None:
        """Open the pool. Raises if the database is unreachable."""
        await self._pool.open(wait=True, timeout=self._settings.http_timeout_seconds * 3)
        logger.info("database pool opened")

    async def close(self) -> None:
        """Close the pool and wait for in-flight connections."""
        await self._pool.close()
        logger.info("database pool closed")

    async def ping(self) -> bool:
        """Return whether the database answers a trivial query."""
        try:
            async with self._pool.connection() as conn:
                return await check_database(conn)
        except Exception:
            logger.warning("database ping failed", exc_info=True)
            return False

    async def __aenter__(self) -> Database:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
