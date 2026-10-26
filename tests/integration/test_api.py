"""API behaviour against the real application and a real test database.

The app is exercised through ``httpx.AsyncClient(ASGITransport)``, so routing, dependency
wiring, validation and the exception handlers are all covered — not just the service layer.
HTTP is never mocked here: the collector path is entered only through the queue, which the
worker (not the API) executes.
"""

from __future__ import annotations

import base64
import csv
import io
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi import FastAPI

from tests.helpers import build_settings
from trendsentinel.collectors.reddit import TOKEN_CREDENTIAL_KEYS
from trendsentinel.collectors.threads import TOKEN_CONFIG_KEY
from trendsentinel.collectors.x import X_CREDENTIAL_KEYS
from trendsentinel.config import Settings
from trendsentinel.db.pool import Database
from trendsentinel.db.repositories import items as items_repo
from trendsentinel.db.repositories import runs as runs_repo
from trendsentinel.db.repositories import snapshots as snapshots_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.db.rows import ItemUpsert
from trendsentinel.main import create_app

pytestmark = pytest.mark.integration

ADMIN_KEY = "test-admin-key"
CAPTURED = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


@pytest.fixture
def api_settings(migrated_test_db: str) -> Settings:
    return build_settings(database_url=migrated_test_db, admin_api_key=ADMIN_KEY)


@pytest.fixture
def api_app(api_settings: Settings) -> FastAPI:
    """A freshly built app, built with explicit settings."""
    return create_app(api_settings)


@pytest.fixture
async def client(api_app: FastAPI, database: Database) -> AsyncIterator[httpx.AsyncClient]:
    """An ASGI client bound to a freshly built app and the migrated test database.

    Depends on the ``database`` fixture purely for its per-test cleanup: the app opens its own
    pool on lifespan, so truncation has to be driven here or state would leak between tests.
    """
    transport = httpx.ASGITransport(app=api_app, raise_app_exceptions=False)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client,
        # Entering the app's lifespan opens the pool the same way uvicorn would.
        api_app.router.lifespan_context(api_app),
    ):
        yield http_client


@pytest.fixture
async def seeded(database: Database) -> Database:
    """Insert a handful of items so the read endpoints have something to return."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        github = await sources_repo.get_source_by_key(conn, "github_blog")
        assert hn is not None and github is not None

        for index in range(5):
            await items_repo.upsert_item(
                conn,
                _item(hn.id, f"hn-{index}", published_at=CAPTURED - timedelta(minutes=index)),
            )
        await items_repo.upsert_item(conn, _item(github.id, "gh-0", published_at=CAPTURED))

    return database


def _item(source_id: int, external_id: str, *, published_at: datetime | None = None) -> ItemUpsert:
    return ItemUpsert(
        source_id=source_id,
        external_id=external_id,
        url=f"https://example.test/{external_id}",
        title=f"Title {external_id}",
        text=None,
        author="tester",
        published_at=published_at,
        metadata={},
        captured_at=CAPTURED,
    )


def auth_headers(key: str = ADMIN_KEY) -> dict[str, str]:
    return {"X-API-Key": key}


# --------------------------------------------------------------------------- health


async def test_health_reports_ok_with_a_live_database(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["version"]


async def test_health_reports_degraded_when_the_database_is_unreachable(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """A broken database must produce the documented 503, not a leaked 500."""
    original_pool = api_app.state.database.pool
    api_app.state.database._pool = _BrokenPool()

    try:
        response = await client.get("/health")
    finally:
        api_app.state.database._pool = original_pool

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["database"] == "unreachable"


class _BrokenPool:
    """A pool whose connection acquisition always fails, for the 503 path."""

    def connection(self) -> object:
        raise RuntimeError("database is unreachable")


# --------------------------------------------------------------------------- sources


async def test_sources_returns_the_seeded_sources(client: httpx.AsyncClient) -> None:
    response = await client.get("/sources")

    assert response.status_code == 200
    keys = {source["key"] for source in response.json()["sources"]}
    assert {"hackernews", "hnrss_frontpage", "github_blog"} <= keys


async def test_sources_active_only_filters(client: httpx.AsyncClient, database: Database) -> None:
    async with database.pool.connection() as conn:
        await conn.execute(
            "UPDATE sources SET is_active = false WHERE key = %(key)s", {"key": "github_blog"}
        )

    response = await client.get("/sources", params={"active_only": "true"})

    assert response.status_code == 200
    keys = {source["key"] for source in response.json()["sources"]}
    assert "github_blog" not in keys


async def test_sources_never_serializes_a_secret_like_config_key(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        await conn.execute(
            "UPDATE sources SET config = config || %(patch)s::jsonb WHERE key = %(key)s",
            {"patch": '{"api_token": "super-secret", "interval_minutes": 15}', "key": "hackernews"},
        )

    response = await client.get("/sources")

    assert response.status_code == 200
    config = next(s["config"] for s in response.json()["sources"] if s["key"] == "hackernews")
    assert "api_token" not in config
    assert "super-secret" not in response.text


#: The credential keys the authenticated adapters declare (ADR-0003, ADR-0005). Read from the
#: adapters so a new credential cannot be added without this test noticing.
CREDENTIAL_KEYS: tuple[str, ...] = (
    *X_CREDENTIAL_KEYS,
    *TOKEN_CREDENTIAL_KEYS,
    TOKEN_CONFIG_KEY,
)


@pytest.mark.parametrize("credential_key", CREDENTIAL_KEYS)
async def test_sources_drops_every_declared_credential_key(
    client: httpx.AsyncClient, database: Database, credential_key: str
) -> None:
    """T26: a source seeded with a credential returns the benign keys and nothing else."""
    probe = f"PROBE-{credential_key}-integration"
    async with database.pool.connection() as conn:
        await conn.execute(
            "UPDATE sources SET config = config || %(patch)s::jsonb WHERE key = %(key)s",
            {
                "patch": json.dumps({credential_key: probe, "interval_minutes": 15}),
                "key": "hackernews",
            },
        )

    response = await client.get("/sources")

    assert response.status_code == 200
    config = next(s["config"] for s in response.json()["sources"] if s["key"] == "hackernews")
    assert credential_key not in config
    assert probe not in response.text
    assert config["interval_minutes"] == 15


async def test_patch_source_active_requires_admin_key(
    client: httpx.AsyncClient,
) -> None:
    response = await client.patch(
        "/sources/hackernews", json={"is_active": False}
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_patch_source_toggles_collection_flag(
    client: httpx.AsyncClient, database: Database
) -> None:
    paused = await client.patch(
        "/sources/hackernews", json={"is_active": False}, headers=auth_headers()
    )
    assert paused.status_code == 200
    assert paused.json()["is_active"] is False

    resumed = await client.patch(
        "/sources/hackernews", json={"is_active": True}, headers=auth_headers()
    )
    assert resumed.status_code == 200
    assert resumed.json()["is_active"] is True


async def test_patch_source_unknown_key_is_404(
    client: httpx.AsyncClient,
) -> None:
    response = await client.patch(
        "/sources/no-such-source", json={"is_active": False}, headers=auth_headers()
    )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


# --------------------------------------------------------------------------- items


async def test_items_default_page_size(client: httpx.AsyncClient, seeded: Database) -> None:
    response = await client.get("/items")

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 6
    assert body["total"] == 6
    assert body["next_cursor"] is None


@pytest.mark.parametrize("limit", [0, 101, -1])
async def test_items_rejects_out_of_range_limits(
    client: httpx.AsyncClient, limit: int
) -> None:
    response = await client.get("/items", params={"limit": limit})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_items_walks_every_page_without_skipping_or_repeating(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    seen: list[int] = []
    cursor: str | None = None

    while True:
        params: dict[str, str | int] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        response = await client.get("/items", params=params)
        assert response.status_code == 200
        body = response.json()
        # The total is the size of the whole filtered set and never changes page to page.
        assert body["total"] == 6
        seen.extend(item["id"] for item in body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break

    assert len(seen) == 6
    assert len(set(seen)) == 6


async def test_items_rejects_a_bad_cursor(client: httpx.AsyncClient) -> None:
    response = await client.get("/items", params={"cursor": "!!!not-base64!!!"})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_items_rejects_a_naive_datetime_bound(client: httpx.AsyncClient) -> None:
    response = await client.get("/items", params={"since": "2026-10-21T00:00:00"})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_items_unknown_source_filter_is_404(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    response = await client.get("/items", params={"source": "no-such-source"})

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_items_filters_by_source_since_and_query(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    by_source = await client.get("/items", params={"source": "github_blog"})
    by_query = await client.get("/items", params={"q": "Title"})
    by_since = await client.get(
        "/items", params={"since": (CAPTURED - timedelta(minutes=1)).isoformat()}
    )

    assert [i["external_id"] for i in by_source.json()["items"]] == ["gh-0"]
    assert len(by_query.json()["items"]) == 6
    # `hn-0` (now), `hn-1` (one minute ago) and `gh-0` (now) fall at or after the bound.
    assert len(by_since.json()["items"]) == 3


async def test_get_item_returns_a_row(client: httpx.AsyncClient, seeded: Database) -> None:
    listing = await client.get("/items", params={"limit": 1})
    item_id = listing.json()["items"][0]["id"]

    response = await client.get(f"/items/{item_id}")

    assert response.status_code == 200
    assert response.json()["id"] == item_id


async def test_get_item_missing_returns_the_error_envelope(client: httpx.AsyncClient) -> None:
    response = await client.get("/items/999999")

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "not_found", "message": "Item not found", "details": None}
    }


async def test_an_item_without_snapshots_returns_an_empty_list(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    """An RSS item has no metrics at all. That is an answer, not an error."""
    listing = await client.get("/items", params={"source": "github_blog"})
    item_id = listing.json()["items"][0]["id"]

    response = await client.get(f"/items/{item_id}/snapshots")

    assert response.status_code == 200
    assert response.json() == {"snapshots": []}


async def test_snapshots_for_a_missing_item_is_404_not_an_empty_list(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/items/999999/snapshots")

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "not_found", "message": "Item not found", "details": None}
    }


@pytest.mark.parametrize("limit", [0, 501, -1])
async def test_snapshots_rejects_an_out_of_range_limit(
    client: httpx.AsyncClient, seeded: Database, limit: int
) -> None:
    listing = await client.get("/items", params={"limit": 1})
    item_id = listing.json()["items"][0]["id"]

    response = await client.get(f"/items/{item_id}/snapshots", params={"limit": limit})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize("item_id", [0, -1])
async def test_snapshots_rejects_a_non_positive_item_id(
    client: httpx.AsyncClient, item_id: int
) -> None:
    response = await client.get(f"/items/{item_id}/snapshots")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_snapshots_honour_the_limit(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(conn, _item(hn.id, "hn-limit"))

        await snapshots_repo.record_snapshot(conn, item_id, {"score": 1.0}, CAPTURED)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 2.0}, CAPTURED + timedelta(hours=1)
        )
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 3.0}, CAPTURED + timedelta(hours=2)
        )

    limited = await client.get(f"/items/{item_id}/snapshots", params={"limit": 2})

    assert limited.status_code == 200
    assert [s["metrics"] for s in limited.json()["snapshots"]] == [{"score": 3.0}, {"score": 2.0}]


async def test_item_detail_carries_the_newest_snapshots_metrics(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(conn, _item(hn.id, "hn-latest"))

        await snapshots_repo.record_snapshot(conn, item_id, {"score": 42.0}, CAPTURED)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 99.0, "descendants": 7.0}, CAPTURED + timedelta(hours=1)
        )

    response = await client.get(f"/items/{item_id}")

    assert response.status_code == 200
    assert response.json()["metrics"] == {"score": 99.0, "descendants": 7.0}


async def test_item_detail_without_snapshots_has_null_metrics(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    listing = await client.get("/items", params={"source": "github_blog"})
    item_id = listing.json()["items"][0]["id"]

    response = await client.get(f"/items/{item_id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


async def test_the_item_page_carries_the_newest_snapshots_metrics(
    client: httpx.AsyncClient, database: Database
) -> None:
    """The page carries each item's newest snapshot metrics — batched, without a detail call.

    An item without snapshots still answers ``null``: absent stays absent.
    """
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(conn, _item(hn.id, "hn-page"))
        await snapshots_repo.record_snapshot(conn, item_id, {"score": 10.0}, CAPTURED)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 42.0}, CAPTURED + timedelta(minutes=5)
        )
        bare = await items_repo.upsert_item(conn, _item(hn.id, "hn-page-bare"))

    page = await client.get("/items")

    by_external = {item["external_id"]: item for item in page.json()["items"]}
    assert by_external["hn-page"]["metrics"] == {"score": 42.0}
    assert by_external["hn-page-bare"]["metrics"] is None
    assert bare[1] is True

# --------------------------------------------------------------------------- csv export


def _csv_rows(response: httpx.Response) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(response.text)))


async def test_export_streams_csv_with_the_documented_header(
    client: httpx.AsyncClient, database: Database
) -> None:
    """The export is the analysis hand-off: the header is a stable contract."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        github = await sources_repo.get_source_by_key(conn, "github_blog")
        assert hn is not None and github is not None
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-hn-1",
                url="https://example.test/exp-hn-1",
                title="Exported HN story",
                text="stored prose",
                author="poster-a",
                published_at=datetime(2026, 10, 20, 9, 30, tzinfo=UTC),
                metadata={"type": "story"},
                captured_at=CAPTURED,
            ),
        )
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=github.id,
                external_id="exp-gh-1",
                url="https://example.test/exp-gh-1",
                title="Exported GH post",
                text=None,
                author="poster-b",
                published_at=datetime(2026, 10, 21, 8, 0, tzinfo=UTC),
                metadata={"feed_title": "The GitHub Blog"},
                captured_at=CAPTURED + timedelta(hours=1),
            ),
        )

    response = await client.get("/export/items.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers.get("content-disposition", "")
    rows = _csv_rows(response)
    assert set(rows[0]) == {
        "item_id",
        "source_key",
        "external_id",
        "url",
        "title",
        "text",
        "author",
        "published_at_utc",
        "captured_at_utc",
        "last_seen_at_utc",
        "metrics_latest_json",
        "metadata_json",
        "source_name",
        "source_type",
        "source_url",
        "collection_method",
        "language",
        "content_type",
        "author_url",
        "thumbnail_url",
    }
    assert len(rows) >= 2


async def test_export_keeps_published_and_captured_apart(
    client: httpx.AsyncClient, database: Database
) -> None:
    """The two time axes live in separate columns, both ISO-8601 UTC (spec §5)."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-times",
                url="https://example.test/exp-times",
                title="Two clocks",
                text=None,
                author="a",
                published_at=datetime(2026, 10, 20, 9, 30, tzinfo=UTC),
                metadata={},
                captured_at=CAPTURED,
            ),
        )
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 77.0}, CAPTURED + timedelta(minutes=1)
        )

    response = await client.get("/export/items.csv?q=Two clocks")
    rows = _csv_rows(response)

    assert response.status_code == 200
    assert len(rows) == 1
    assert rows[0]["published_at_utc"] == "2026-10-20T09:30:00+00:00"
    assert rows[0]["captured_at_utc"] == "2026-10-21T12:00:00+00:00"
    assert rows[0]["source_key"] == "hackernews"
    assert rows[0]["metrics_latest_json"] == '{"score": 77.0}'


async def test_export_includes_new_content_and_source_metadata(
    client: httpx.AsyncClient, database: Database
) -> None:
    """New optional columns round-trip; source provenance joins from `sources`."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-meta",
                url="https://example.test/exp-meta",
                title="Meta rich",
                text="prose",
                author="poster-a",
                published_at=datetime(2026, 10, 22, 9, 30, tzinfo=UTC),
                metadata={},
                captured_at=CAPTURED,
                language="en",
                content_type="link",
                author_url="https://news.ycombinator.com/user?id=poster-a",
                thumbnail_url="https://example.test/img.png",
            ),
        )

    response = await client.get("/export/items.csv?q=Meta rich")
    rows = _csv_rows(response)

    assert response.status_code == 200
    assert len(rows) == 1
    row = rows[0]
    assert row["language"] == "en"
    assert row["content_type"] == "link"
    assert row["author_url"] == "https://news.ycombinator.com/user?id=poster-a"
    assert row["thumbnail_url"] == "https://example.test/img.png"
    # Source provenance comes from the joined `sources` row.
    assert row["source_name"] == "Hacker News (API)"
    assert row["source_type"] == "forum"
    assert row["collection_method"] == "api"
    assert row["source_url"] == (hn.base_url or "")


async def test_export_respects_the_source_filter_and_404s_unknown_keys(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        github = await sources_repo.get_source_by_key(conn, "github_blog")
        assert github is not None
        await items_repo.upsert_item(conn, _item(github.id, "exp-gh-only"))

    filtered = await client.get("/export/items.csv?source=github_blog")
    assert {row["source_key"] for row in _csv_rows(filtered)} == {"github_blog"}

    unknown = await client.get("/export/items.csv?source=nope")
    assert unknown.status_code == 404


async def test_export_reports_truncation_through_headers(
    client: httpx.AsyncClient, database: Database
) -> None:
    """A capped export says so: total matching vs the ceiling that bound it."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        for index in range(5):
            await items_repo.upsert_item(
                conn,
                _item(hn.id, f"cap-{index}", published_at=CAPTURED - timedelta(minutes=index)),
            )

    response = await client.get("/export/items.csv?max_rows=3")

    rows = _csv_rows(response)
    assert response.headers["x-total-matching"] == "5"
    assert response.headers["x-max-rows"] == "3"
    assert len(rows) == 3


async def test_export_row_ceiling_is_validated(client: httpx.AsyncClient) -> None:
    response = await client.get("/export/items.csv?max_rows=0")

    assert response.status_code == 422


async def test_export_rejects_a_naive_time_bound(
    client: httpx.AsyncClient, database: Database
) -> None:
    """A naive bound is rejected, exactly as the list route rejects it.

    The export honours the same ``since``/``until`` semantics as ``GET /items``
    (docs/api.md). A naive value would otherwise be cast by the database in its session
    time zone and silently select the wrong rows — a wrong dataset is worse than an
    error the operator can see.
    """
    response = await client.get("/export/items.csv?since=2026-10-20T00:00:00")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_export_accepts_an_aware_time_bound(
    client: httpx.AsyncClient, database: Database
) -> None:
    response = await client.get("/export/items.csv?since=2026-10-20T00:00:00Z")

    assert response.status_code == 200


async def test_export_honours_the_title_query(
    client: httpx.AsyncClient, database: Database
) -> None:
    """``q`` is one of the documented export filters."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-q-hit",
                url="https://example.test/exp-q-hit",
                title="Rust in the kernel",
                text=None,
                author="a",
                published_at=CAPTURED,
                metadata={},
                captured_at=CAPTURED,
            ),
        )
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-q-miss",
                url="https://example.test/exp-q-miss",
                title="Something else entirely",
                text=None,
                author="a",
                published_at=CAPTURED,
                metadata={},
                captured_at=CAPTURED,
            ),
        )

    response = await client.get("/export/items.csv?q=kernel")
    rows = _csv_rows(response)

    assert response.status_code == 200
    assert [row["external_id"] for row in rows] == ["exp-q-hit"]


async def test_export_quotes_csv_safe_values(
    client: httpx.AsyncClient, database: Database
) -> None:
    """Commas, quotes and newlines in stored values stay inside one cell."""
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        await items_repo.upsert_item(
            conn,
            ItemUpsert(
                source_id=hn.id,
                external_id="exp-commas",
                url="https://example.test/x",
                title='Title, with "quotes" and\nnewlines',
                text=None,
                author="a",
                published_at=None,
                metadata={},
                captured_at=CAPTURED,
            ),
        )

    response = await client.get("/export/items.csv?q=quotes")
    rows = _csv_rows(response)

    assert response.status_code == 200
    assert len(rows) == 1
    assert rows[0]["title"] == 'Title, with "quotes" and\nnewlines'
    assert rows[0]["published_at_utc"] == ""


# --------------------------------------------------------------------------- item snapshots


async def test_listing_an_items_snapshots_returns_them_newest_first(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(conn, _item(hn.id, "hn-snap"))

        await snapshots_repo.record_snapshot(conn, item_id, {"score": 10.0}, CAPTURED)
        await snapshots_repo.record_snapshot(
            conn, item_id, {"score": 42.0}, CAPTURED + timedelta(hours=1)
        )

    response = await client.get(f"/items/{item_id}/snapshots")

    assert response.status_code == 200
    snapshots = response.json()["snapshots"]
    assert [snapshot["metrics"] for snapshot in snapshots] == [{"score": 42.0}, {"score": 10.0}]
    assert {snapshot["item_id"] for snapshot in snapshots} == {item_id}
    assert all("id" in snapshot and "captured_at" in snapshot for snapshot in snapshots)


async def test_a_non_numeric_metric_is_dropped_rather_than_failing_the_request(
    client: httpx.AsyncClient, database: Database
) -> None:
    """``metrics`` is jsonb, so a stray non-number must not turn into a 500.

    The column has no type constraint, so a hand-written row (or a future collector bug) could
    store a string. The API shows what it can safely show instead of failing every reader of
    that item.
    """
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        assert hn is not None
        item_id, _ = await items_repo.upsert_item(conn, _item(hn.id, "hn-mixed-metrics"))
        await conn.execute(
            """
            INSERT INTO item_snapshots (item_id, captured_at, metrics)
            VALUES (%(item_id)s, %(captured_at)s, %(metrics)s::jsonb)
            """,
            {
                "item_id": item_id,
                "captured_at": CAPTURED,
                "metrics": '{"score": 7.0, "label": "not a number", "flag": true}',
            },
        )

    response = await client.get(f"/items/{item_id}/snapshots")

    assert response.status_code == 200
    assert response.json()["snapshots"][0]["metrics"] == {"score": 7.0}


# --------------------------------------------------------------------------- collect status


async def test_collect_status_is_empty_before_any_run(client: httpx.AsyncClient) -> None:
    response = await client.get("/collect/status")

    assert response.status_code == 200
    assert response.json() == {"runs": [], "active": []}


async def test_collect_status_reports_the_last_run_and_active_runs(
    client: httpx.AsyncClient, database: Database
) -> None:
    async with database.pool.connection() as conn:
        hn = await sources_repo.get_source_by_key(conn, "hackernews")
        github = await sources_repo.get_source_by_key(conn, "github_blog")
        assert hn is not None and github is not None

        finished = await runs_repo.create_run(conn, hn.id, "scheduler")
        await runs_repo.finish_run(conn, finished.id, status="succeeded", items_new=2)
        await runs_repo.create_run(conn, github.id, "manual", status="pending")

    response = await client.get("/collect/status")

    assert response.status_code == 200
    body = response.json()
    by_key = {run["source_key"]: run for run in body["runs"]}
    assert by_key["hackernews"]["status"] == "succeeded"
    assert by_key["hackernews"]["items_new"] == 2
    assert [run["source_key"] for run in body["active"]] == ["github_blog"]
    assert body["active"][0]["status"] == "pending"


# --------------------------------------------------------------------------- collect run auth


async def test_collect_run_without_a_key_is_401(client: httpx.AsyncClient) -> None:
    response = await client.post("/collect/run", json={})

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_collect_run_with_a_wrong_key_is_401(client: httpx.AsyncClient) -> None:
    response = await client.post("/collect/run", json={}, headers=auth_headers("wrong"))

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_collect_run_fails_closed_when_no_key_is_configured(
    migrated_test_db: str,
) -> None:
    settings = build_settings(database_url=migrated_test_db, admin_api_key="")
    app = create_app(settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as http_client, app.router.lifespan_context(app):
        response = await http_client.post(
            "/collect/run", json={"source_key": "hackernews"}, headers=auth_headers("anything")
        )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


# --------------------------------------------------------------------------- collect run behaviour


async def test_collect_run_queues_a_pending_run_for_one_source(
    client: httpx.AsyncClient, database: Database
) -> None:
    response = await client.post(
        "/collect/run",
        json={"source_key": "hackernews", "limit": 5},
        headers=auth_headers(),
    )

    assert response.status_code == 202
    runs = response.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["source_key"] == "hackernews"
    assert runs[0]["status"] == "pending"

    async with database.pool.connection() as conn:
        pending = await runs_repo.get_pending_runs(conn)

    assert [run.id for run in pending] == [runs[0]["run_id"]]
    assert pending[0].item_limit == 5
    assert pending[0].trigger == "manual"


async def test_collect_run_queues_every_active_source_when_unnamed(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post("/collect/run", json={}, headers=auth_headers())

    assert response.status_code == 202
    keys = {run["source_key"] for run in response.json()["runs"]}
    assert {"hackernews", "hnrss_frontpage", "github_blog"} <= keys
    assert all(run["status"] == "pending" for run in response.json()["runs"])


async def test_collect_run_unknown_source_is_404(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/collect/run", json={"source_key": "no-such-source"}, headers=auth_headers()
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_collect_run_second_call_for_a_running_source_is_409(
    client: httpx.AsyncClient, database: Database
) -> None:
    first = await client.post(
        "/collect/run", json={"source_key": "hackernews"}, headers=auth_headers()
    )
    assert first.status_code == 202

    second = await client.post(
        "/collect/run", json={"source_key": "hackernews"}, headers=auth_headers()
    )

    assert second.status_code == 409
    body = second.json()
    assert body["error"]["code"] == "conflict"
    assert body["error"]["details"]["source_key"] == "hackernews"


async def test_collect_run_rejects_an_out_of_range_limit(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/collect/run", json={"source_key": "hackernews", "limit": 0}, headers=auth_headers()
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_collect_run_rejects_an_unknown_body_field(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/collect/run", json={"nope": True}, headers=auth_headers()
    )

    assert response.status_code == 422


async def test_collect_run_never_echoes_the_admin_key(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post("/collect/run", json={}, headers=auth_headers())

    assert ADMIN_KEY not in response.text


# --------------------------------------------------------------------------- envelope / leak guards


async def test_unknown_route_uses_the_error_envelope(client: httpx.AsyncClient) -> None:
    response = await client.get("/does-not-exist")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_no_response_body_contains_the_database_url(
    client: httpx.AsyncClient, api_settings: Settings
) -> None:
    secret = str(api_settings.database_url)

    for path in ("/health", "/sources", "/items", "/collect/status", "/nope"):
        response = await client.get(path)
        assert secret not in response.text


async def test_a_crash_returns_a_generic_500_with_no_stack_trace(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """The catch-all handler must log the detail and return only a generic message."""

    async def explode() -> None:
        raise RuntimeError("kaboom: /etc/secret/dsn")

    api_app.add_api_route("/boom", explode, methods=["GET"])

    response = await client.get("/boom")

    assert response.status_code == 500
    body = response.json()
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["message"] == "Internal server error"
    assert "kaboom" not in response.text
    assert "Traceback" not in response.text


# --------------------------------------------------------------------------- cursor contract


async def test_cursor_is_opaque_but_a_round_trip_still_works(
    client: httpx.AsyncClient, seeded: Database
) -> None:
    first = await client.get("/items", params={"limit": 1})
    cursor = first.json()["next_cursor"]
    assert cursor is not None

    # It really is base64 of `published_at|id`, and it must never appear as a plain number.
    decoded = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    assert "|" in decoded

    second = await client.get("/items", params={"limit": 1, "cursor": cursor})
    assert second.status_code == 200
    assert second.json()["items"][0]["id"] != first.json()["items"][0]["id"]
