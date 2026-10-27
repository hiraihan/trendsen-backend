"""Domain-agnostic schema extension: universal content fields, topic/entity/label tables.

This migration makes the collected schema extensible across content domains without a
per-category redesign, and adds the *tables* the offline topic/entity/label datasets need.

Guiding rules (mirrored in ``docs/data-schema.md``):

* Every new ``collected_items`` column is nullable except the lifecycle state
  ``content_status``: a source that does not provide a value leaves the cell empty, never
  fabricated.
* ``content_fingerprint`` is written at ingestion (``services/collection.py``) — a stable
  sha-256 over normalized content, enabling cross-source similarity/dedup later. It is not
  a primary key.
* ``topics``, ``item_topic_assignments``, ``topic_observations``, ``item_entities`` and
  ``training_labels`` are created empty. No producer fills them yet; emitting them is
  honest (never-fabricate), and their export columns are the contract an enrichment pass
  eventually writes to.

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-27
"""

from __future__ import annotations

from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # -- collected_items: universal content fields ----------------------------------
    op.execute("ALTER TABLE collected_items ADD COLUMN canonical_url text")
    op.execute("ALTER TABLE collected_items ADD COLUMN content_fingerprint text")
    op.execute("ALTER TABLE collected_items ADD COLUMN category text")
    op.execute("ALTER TABLE collected_items ADD COLUMN subcategory text")
    op.execute("ALTER TABLE collected_items ADD COLUMN language_confidence real")
    op.execute(
        """
        ALTER TABLE collected_items
        ADD COLUMN content_status text NOT NULL DEFAULT 'collected'
        """
    )
    op.execute(
        """
        ALTER TABLE collected_items
        ADD CONSTRAINT collected_items_content_status_valid
        CHECK (content_status IN ('collected', 'errored'))
        """
    )

    # Partial index only where a fingerprint exists; helps future cross-source dedup.
    op.execute(
        """
        CREATE INDEX collected_items_fingerprint_idx
            ON collected_items (content_fingerprint)
            WHERE content_fingerprint IS NOT NULL
        """
    )

    # -- sources: capability/versioning columns --------------------------------------
    op.execute("ALTER TABLE sources ADD COLUMN source_category text")
    op.execute(
        "ALTER TABLE sources ADD COLUMN supported_content_types text[] NOT NULL DEFAULT '{}'"
    )
    op.execute("ALTER TABLE sources ADD COLUMN rate_limit_policy jsonb")
    op.execute("ALTER TABLE sources ADD COLUMN schema_version integer NOT NULL DEFAULT 1")

    # Backfill supported_content_types from what the seeded collectors actually handle.
    # Conservative and honest: rss entries are articles; the API-driven forums carry links
    # and/or self-posts; unknown integrations stay empty.
    op.execute("UPDATE sources SET supported_content_types = ARRAY['article'] WHERE integration = 'rss'")
    op.execute("UPDATE sources SET supported_content_types = ARRAY['link','self'] WHERE integration = 'api' AND key IN ('hackernews', 'reddit_technology')")
    op.execute("UPDATE sources SET supported_content_types = ARRAY['link'] WHERE integration = 'api' AND key = 'threads_profile'")

    # -- topics ----------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE topics (
            id               bigserial PRIMARY KEY,
            canonical_name    text NOT NULL,
            description       text,
            category          text,
            aliases          jsonb NOT NULL DEFAULT '[]'::jsonb,
            keywords         jsonb NOT NULL DEFAULT '[]'::jsonb,
            first_observed_at timestamptz,
            last_observed_at  timestamptz,
            identification_method text,
            version          text,
            created_at       timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT topics_canonical_name_unique UNIQUE (canonical_name)
        )
        """
    )

    # -- item_topic_assignments ------------------------------------------------------
    op.execute(
        """
        CREATE TABLE item_topic_assignments (
            id         bigserial PRIMARY KEY,
            item_id    bigint NOT NULL REFERENCES collected_items (id) ON DELETE CASCADE,
            topic_id   bigint NOT NULL REFERENCES topics (id) ON DELETE CASCADE,
            assigned_at timestamptz NOT NULL DEFAULT now(),
            method     text NOT NULL,
            confidence real,
            rule_version text,
            CONSTRAINT item_topic_assignments_unique
                UNIQUE (item_id, topic_id, method, rule_version)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX item_topic_assignments_topic_at_idx
            ON item_topic_assignments (topic_id, assigned_at DESC)
        """
    )

    # -- topic_observations ----------------------------------------------------------
    op.execute(
        """
        CREATE TABLE topic_observations (
            id                   bigserial PRIMARY KEY,
            topic_id             bigint NOT NULL REFERENCES topics (id) ON DELETE CASCADE,
            observation_window_start timestamptz NOT NULL,
            observation_window_end   timestamptz NOT NULL,
            content_count        integer NOT NULL DEFAULT 0,
            unique_source_count   integer NOT NULL DEFAULT 0,
            engagement_aggregates jsonb NOT NULL DEFAULT '{}'::jsonb,
            activity_growth      real,
            historical_baseline  real,
            anomaly_indicator    boolean,
            created_at           timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT topic_observations_window_valid
                CHECK (observation_window_end >= observation_window_start)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX topic_observations_topic_window_end_idx
            ON topic_observations (topic_id, observation_window_end DESC)
        """
    )

    # -- item_entities ----------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE item_entities (
            id             bigserial PRIMARY KEY,
            item_id        bigint NOT NULL REFERENCES collected_items (id) ON DELETE CASCADE,
            entity_type    text NOT NULL,
            canonical_name text NOT NULL,
            aliases       jsonb NOT NULL DEFAULT '[]'::jsonb,
            extraction_method text NOT NULL,
            confidence     real,
            extraction_version text,
            created_at     timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT item_entities_unique
                UNIQUE (item_id, entity_type, canonical_name, extraction_method, extraction_version)
        )
        """
    )
    op.execute("CREATE INDEX item_entities_canonical_name_idx ON item_entities (canonical_name)")

    # -- training_labels --------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE training_labels (
            id            bigserial PRIMARY KEY,
            item_id       bigint NOT NULL REFERENCES collected_items (id) ON DELETE CASCADE,
            target_value  jsonb NOT NULL,
            prediction_horizon interval,
            label_definition text NOT NULL,
            label_version text NOT NULL,
            generation_method text NOT NULL,
            generated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX training_labels_item_idx ON training_labels (item_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS training_labels")
    op.execute("DROP TABLE IF EXISTS item_entities")
    op.execute("DROP TABLE IF EXISTS topic_observations")
    op.execute("DROP TABLE IF EXISTS item_topic_assignments")
    op.execute("DROP TABLE IF EXISTS topics")

    op.execute("ALTER TABLE sources DROP COLUMN IF EXISTS schema_version")
    op.execute("ALTER TABLE sources DROP COLUMN IF EXISTS rate_limit_policy")
    op.execute("ALTER TABLE sources DROP COLUMN IF EXISTS supported_content_types")
    op.execute("ALTER TABLE sources DROP COLUMN IF EXISTS source_category")

    op.execute("DROP INDEX IF EXISTS collected_items_fingerprint_idx")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS content_status")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS language_confidence")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS subcategory")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS category")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS content_fingerprint")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS canonical_url")
