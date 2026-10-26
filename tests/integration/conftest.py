"""Integration test fixtures: a real PostgreSQL test database.

Two hard rules from `docs/tickets/T14-tests.md` are enforced here:

1. The test database must be *different* from ``DATABASE_URL``. A suite that truncates tables
   is one configuration mistake away from destroying collected data, so it refuses to start.
2. Migrations are applied for real, with ``alembic upgrade head``. Tests run against the same
   schema production gets, not a hand-built mock of it.

When the test database is unreachable the integration tests skip with an explicit reason
rather than failing: they are meaningless without a database, and a developer running
``pytest`` without one should still get a green unit suite.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import psycopg
import pytest

from tests.helpers import build_settings
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The local defaults that match `docker-compose.yml` (the db port is 55432 on this machine,
#: where a host PostgreSQL already owns 5432).
DEFAULT_TEST_URL = "postgresql://trendsentinel:trendsentinel@127.0.0.1:55432/trendsentinel_test"

#: Application tables, child-first. ``sources`` is deliberately excluded: it is reference
#: data seeded by migration 0002 and tests may rely on it.
#:
#: Spelled as one literal, not built from parts: ``TRUNCATE`` cannot take identifiers as bound
#: parameters, and a literal keeps the T16 "no query built by concatenation" check clean — the
#: rule applies to tests too.
_TRUNCATE_STATEMENT = (
    "TRUNCATE item_snapshots, collection_runs, collected_items RESTART IDENTITY CASCADE"
)


def test_database_url() -> str:
    """Resolve the test database URL from the environment, with a docker-compose default."""
    return os.environ.get("TEST_DATABASE_URL") or DEFAULT_TEST_URL


def _assert_distinct_from_production() -> None:
    """Refuse to run when the test URL points at the configured production database."""
    production = os.environ.get("DATABASE_URL")
    if production and production.strip() == test_database_url():
        raise RuntimeError(
            "TEST_DATABASE_URL must differ from DATABASE_URL: the suite truncates tables."
        )


def _database_is_reachable(url: str) -> tuple[bool, str]:
    """Return whether the test database accepts a connection, plus a reason when it does not."""
    try:
        with psycopg.connect(url, connect_timeout=3):
            return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


@pytest.fixture(scope="session")
def test_db_url() -> str:
    """The test database URL, validated once per session."""
    _assert_distinct_from_production()
    return test_database_url()


@pytest.fixture(scope="session")
def migrated_test_db(test_db_url: str) -> Iterator[str]:
    """Apply migrations to the test database before any integration test runs.

    The round trip `upgrade head` → `downgrade base` → `upgrade head` is performed here, for
    two reasons: it proves the downgrade path works (a ticket requirement), and it guarantees
    the seeded ``sources`` rows are pristine even when a previous run left them mutated.

    Skips the whole integration suite when the database is not reachable, so a machine
    without PostgreSQL still produces a green unit run.
    """
    reachable, reason = _database_is_reachable(test_db_url)
    if not reachable:
        pytest.skip(f"test database {test_db_url!r} is unreachable ({reason})")

    # `ENV_FILE=""` keeps the developer's real `.env` out of the migration run; DATABASE_URL
    # and a blank ADMIN_API_KEY are supplied explicitly so settings validation passes without
    # depending on any file. A blank admin key is valid configuration (it disables the trigger
    # endpoint), and migrations never use it.
    env = {
        **os.environ,
        "ENV_FILE": "",
        "DATABASE_URL": test_db_url,
        "ADMIN_API_KEY": "",
    }

    _run_alembic("upgrade", "head", env)
    _run_alembic("downgrade", "base", env)
    _run_alembic("upgrade", "head", env)

    yield test_db_url


def _run_alembic(command: str, revision: str, env: dict[str, str]) -> None:
    """Run one Alembic command against the test database, failing loudly."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", command, revision],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"alembic {command} {revision} failed against the test database:\n{result.stderr}"
        )


@pytest.fixture
def integration_settings(migrated_test_db: str) -> Settings:
    """Settings pointed at the migrated test database."""
    return build_settings(database_url=migrated_test_db)


@pytest.fixture
async def database(
    integration_settings: Settings, migrated_test_db: str
) -> AsyncIterator[Database]:
    """An open pool against the test database, with the collected data cleared first.

    Truncating rather than rolling back keeps the tests honest about what the repositories
    commit, which is the behaviour under test.

    ``sources`` is *snapshotted* rather than truncated: it is reference data seeded by
    migration 0002 and tests may rely on it, but a test that flips ``is_active`` or patches a
    ``config`` must not leak that change into the next test. Restoring the snapshot gives both
    a stable seed and per-test isolation.
    """
    db = Database(integration_settings)
    await db.open()
    try:
        async with db.pool.connection() as conn:
            await conn.execute(_TRUNCATE_STATEMENT)
            snapshot = await _snapshot_sources(conn)
        yield db
    finally:
        try:
            async with db.pool.connection() as conn:
                await _restore_sources(conn, snapshot)
        finally:
            await db.close()


async def _snapshot_sources(conn: object) -> list[dict[str, object]]:
    """Read the seeded sources so they can be restored after a test."""
    from psycopg import AsyncConnection

    assert isinstance(conn, AsyncConnection)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT key, name, integration, base_url, is_active, config
            FROM sources ORDER BY key
            """
        )
        return [dict(row) for row in await cur.fetchall()]


async def _restore_sources(conn: object, snapshot: list[dict[str, object]]) -> None:
    """Return ``sources`` to its seeded state: drop extras, restore mutated rows."""
    from psycopg import AsyncConnection

    assert isinstance(conn, AsyncConnection)
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM sources WHERE key <> ALL(%(keys)s)",
            {"keys": [row["key"] for row in snapshot]},
        )
        for row in snapshot:
            await cur.execute(
                """
                UPDATE sources
                SET name = %(name)s,
                    integration = %(integration)s,
                    base_url = %(base_url)s,
                    is_active = %(is_active)s,
                    config = %(config)s::jsonb
                WHERE key = %(key)s
                """,
                {
                    "key": row["key"],
                    "name": row["name"],
                    "integration": row["integration"],
                    "base_url": row["base_url"],
                    "is_active": row["is_active"],
                    "config": json.dumps(row["config"]),
                },
            )
