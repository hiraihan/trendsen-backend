"""Initial schema: sources, collected_items, collection_runs, item_snapshots.

The constraints created here are what enforce this project's core guarantees:

* ``UNIQUE (source_id, external_id)`` makes collection idempotent, so re-running a
  collector updates rows instead of duplicating them.
* The partial ``UNIQUE (source_id) WHERE status = 'running'`` index allows at most one
  concurrent collection run per source, enforced by the database rather than by convention.
* ``UNIQUE (item_id, captured_at)`` makes metric snapshots idempotent.

Revision ID: 0001
Revises:
Create Date: 2026-10-21

"""

from __future__ import annotations

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE sources (
            id          bigserial PRIMARY KEY,
            key         text        NOT NULL,
            name        text        NOT NULL,
            integration text        NOT NULL,
            base_url    text,
            is_active   boolean     NOT NULL DEFAULT true,
            config      jsonb       NOT NULL DEFAULT '{}'::jsonb,
            created_at  timestamptz NOT NULL DEFAULT now(),
            updated_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT sources_key_unique UNIQUE (key),
            CONSTRAINT sources_integration_valid
                CHECK (integration IN ('api', 'rss', 'html'))
        )
        """
    )

    op.execute(
        """
        CREATE TABLE collected_items (
            id            bigserial   PRIMARY KEY,
            source_id     bigint      NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
            external_id   text        NOT NULL,
            url           text,
            title         text,
            text          text,
            author        text,
            published_at  timestamptz,
            metadata      jsonb       NOT NULL DEFAULT '{}'::jsonb,
            captured_at   timestamptz NOT NULL,
            last_seen_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT collected_items_source_external_unique
                UNIQUE (source_id, external_id),
            CONSTRAINT collected_items_external_id_not_blank
                CHECK (length(btrim(external_id)) > 0)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE collection_runs (
            id            bigserial   PRIMARY KEY,
            source_id     bigint      NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
            started_at    timestamptz NOT NULL DEFAULT now(),
            finished_at   timestamptz,
            status        text        NOT NULL,
            trigger       text        NOT NULL,
            items_fetched integer     NOT NULL DEFAULT 0,
            items_new     integer     NOT NULL DEFAULT 0,
            items_updated integer     NOT NULL DEFAULT 0,
            error_summary jsonb,
            CONSTRAINT collection_runs_status_valid
                CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'partial')),
            CONSTRAINT collection_runs_trigger_valid
                CHECK (trigger IN ('scheduler', 'manual')),
            CONSTRAINT collection_runs_finished_after_started
                CHECK (finished_at IS NULL OR finished_at >= started_at)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE item_snapshots (
            id          bigserial   PRIMARY KEY,
            item_id     bigint      NOT NULL
                REFERENCES collected_items (id) ON DELETE CASCADE,
            captured_at timestamptz NOT NULL,
            metrics     jsonb       NOT NULL,
            CONSTRAINT item_snapshots_item_captured_unique
                UNIQUE (item_id, captured_at)
        )
        """
    )

    # At most one active run per source. This is the authoritative guard against duplicate
    # work; the worker's advisory lock is a complementary guard, not a replacement.
    op.execute(
        """
        CREATE UNIQUE INDEX collection_runs_one_active_per_source
            ON collection_runs (source_id)
            WHERE status IN ('pending', 'running')
        """
    )

    # Keyset pagination for GET /items. `NULLS LAST` is deliberate: not every source
    # provides a publication time, and those items must still be listable.
    op.execute(
        """
        CREATE INDEX collected_items_published_at_id_idx
            ON collected_items (published_at DESC NULLS LAST, id DESC)
        """
    )

    op.execute(
        """
        CREATE INDEX collected_items_source_published_at_idx
            ON collected_items (source_id, published_at DESC NULLS LAST, id DESC)
        """
    )

    op.execute(
        """
        CREATE INDEX collection_runs_source_started_at_idx
            ON collection_runs (source_id, started_at DESC)
        """
    )

    op.execute(
        """
        CREATE INDEX item_snapshots_item_captured_at_idx
            ON item_snapshots (item_id, captured_at DESC)
        """
    )


def downgrade() -> None:
    # Reverse dependency order. Indexes are dropped with their tables.
    op.execute("DROP TABLE IF EXISTS item_snapshots")
    op.execute("DROP TABLE IF EXISTS collection_runs")
    op.execute("DROP TABLE IF EXISTS collected_items")
    op.execute("DROP TABLE IF EXISTS sources")
