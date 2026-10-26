"""End-to-end viral filter: real app, real test database.

Seeds a source and items with known scores across one day, then asserts the read API
returns exactly the band's members, honours keyword population selection and per-source
overrides, and excludes missing-metric items.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
from fastapi import FastAPI

from tests.helpers import build_settings
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.main import create_app

pytestmark = pytest.mark.integration

ADMIN_KEY = "test-admin-key"
DAY = datetime(2026, 10, 21, tzinfo=UTC)


@pytest.fixture
def api_settings(migrated_test_db: str) -> Settings:
    return build_settings(database_url=migrated_test_db, admin_api_key=ADMIN_KEY)


@pytest.fixture
def api_app(api_settings: Settings) -> FastAPI:
    return create_app(api_settings)


@pytest.fixture
async def client(api_app: FastAPI, database: Database) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=api_app, raise_app_exceptions=False)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client,
        api_app.router.lifespan_context(api_app),
    ):
        yield http_client


async def seed_viral_source(database: Database, config: dict[str, object]) -> int:
    async with database.pool.connection() as conn:
        cur = await conn.execute(
            """
            INSERT INTO sources (key, name, integration, base_url, is_active, config)
            VALUES ('viral_src', 'Viral Src', 'api', 'https://example.test', true, %s::jsonb)
            RETURNING id
            """,
            (json.dumps(config),),
        )
        row = await cur.fetchone()
    assert row is not None
    return int(row["id"])


async def seed_items(database: Database, source_id: int, scores: list[tuple[float, str]]) -> None:
    """Insert items with a latest snapshot carrying a ``score``.

    ``scores`` is a list of ``(score, title)``. Publication times spread across the day so
    ``published_at DESC`` ordering is deterministic.
    """
    async with database.pool.connection() as conn:
        for idx, (score, title) in enumerate(scores):
            published = datetime(2026, 10, 21, 8 + idx, 0, tzinfo=UTC)
            cur = await conn.execute(
                """
                INSERT INTO collected_items (
                    source_id, external_id, url, title, text, author,
                    published_at, metadata, captured_at, last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb, %s, %s)
                RETURNING id
                """,
                (
                    source_id,
                    f"ext-{idx}",
                    f"https://example.test/{idx}",
                    title,
                    None,
                    "tester",
                    published,
                    DAY,
                    DAY,
                ),
            )
            row = await cur.fetchone()
            assert row is not None
            await conn.execute(
                """
                INSERT INTO item_snapshots (item_id, captured_at, metrics)
                VALUES (%s, %s, %s::jsonb)
                """,
                (int(row["id"]), DAY, json.dumps({"score": score})),
            )


async def seed_missing_metric_item(database: Database, source_id: int) -> None:
    async with database.pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO collected_items (
                source_id, external_id, url, title, text, author,
                published_at, metadata, captured_at, last_seen_at
            )
            VALUES (%s, 'ext-missing', 'https://example.test/missing', 'no metrics',
                    NULL, 'tester', %s, '{}'::jsonb, %s, %s)
            """,
            (source_id, datetime(2026, 10, 21, 13, 0, tzinfo=UTC), DAY, DAY),
        )


async def test_viral_band_selects_top_of_day_and_excludes_missing_metric(
    client: httpx.AsyncClient, database: Database
) -> None:
    source_id = await seed_viral_source(database, {"collector_key": "hackernews"})
    await seed_items(database, source_id, [(10.0, "a"), (20.0, "b"), (30.0, "c"), (40.0, "d")])
    await seed_missing_metric_item(database, source_id)

    resp = await client.get(
        "/items",
        params={"source": "viral_src", "quantile_low": 50, "quantile_high": 95},
    )
    assert resp.status_code == 200
    data = resp.json()

    # Day cohort [10, 20, 30, 40] -> percentiles 0, 25, 50, 75: the two top items qualify.
    items = data["items"]
    assert [it["title"] for it in items] == ["d", "c"]
    assert data["total"] == 2
    assert all(it["viral"] for it in items)
    assert items[0]["percentile"] == 75.0
    assert items[1]["percentile"] == 50.0
    assert all(it["title"] != "no metrics" for it in items)


async def test_viral_keywords_narrow_population(
    client: httpx.AsyncClient, database: Database
) -> None:
    source_id = await seed_viral_source(database, {"collector_key": "hackernews"})
    await seed_items(
        database,
        source_id,
        [
            (10.0, "AI cold"),
            (100.0, "AI hot"),
            (500.0, "unrelated big"),
        ],
    )

    resp = await client.get("/items", params={"source": "viral_src", "keywords": "ai"})
    assert resp.status_code == 200
    items = resp.json()["items"]

    # Only the AI population ranks; within it [10, 100] -> only the 100 qualifies.
    assert [it["title"] for it in items] == ["AI hot"]


async def test_viral_source_config_override_widens_band(
    client: httpx.AsyncClient, database: Database
) -> None:
    source_id = await seed_viral_source(
        database,
        {
            "collector_key": "hackernews",
            "viral_quantile_low": 0,
            "viral_quantile_high": 100,
        },
    )
    await seed_items(database, source_id, [(10.0, "a"), (20.0, "b"), (30.0, "c"), (40.0, "d")])

    # An empty ``keywords`` parameter triggers the viral path without overriding the band, so
    # the source's config override (0..100) is what selects the population.
    resp = await client.get("/items", params={"source": "viral_src", "keywords": ""})
    assert resp.status_code == 200
    items = resp.json()["items"]

    assert len(items) == 4
    assert all(it["viral"] for it in items)


async def test_unfiltered_list_has_no_viral_flag(
    client: httpx.AsyncClient, database: Database
) -> None:
    source_id = await seed_viral_source(database, {"collector_key": "hackernews"})
    await seed_items(database, source_id, [(10.0, "a")])

    resp = await client.get("/items", params={"source": "viral_src"})
    assert resp.status_code == 200
    item = resp.json()["items"][0]

    assert item["viral"] is False
    assert item["percentile"] is None


async def test_viral_requires_a_source(client: httpx.AsyncClient) -> None:
    resp = await client.get("/items", params={"quantile_low": 50})

    assert resp.status_code == 422


async def test_viral_rejects_bad_quantile_order(
    client: httpx.AsyncClient, database: Database
) -> None:
    await seed_viral_source(database, {"collector_key": "hackernews"})

    resp = await client.get(
        "/items", params={"source": "viral_src", "quantile_low": 90, "quantile_high": 50}
    )

    assert resp.status_code == 422
