"""Add ``source_type`` and content/collection metadata columns.

* ``sources.source_type`` classifies a source's *kind* (blog, forum, social, news). It
  is independent of how it is collected — the existing ``integration`` column remains
  the collection method.
* ``collected_items`` gains ``language``, ``content_type``, ``author_url`` and
  ``thumbnail_url``. Each is nullable: sources do not provide them uniformly, and a
  null cell means "not provided" rather than a fabricated value (spec: never guess).
* Existing rows are backfilled only where derivable without guessing: ``content_type``
  from whether a row carries an external URL or body text, and ``source_type`` from a
  known-key mapping for the seeded sources.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-26
"""

from __future__ import annotations

from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | None = None
depends_on: str | None = None

#: Key -> classification for every source the seed migrations create.
_SOURCE_TYPES = {
    "github_blog": "blog",
    "hackernews": "forum",
    "hnrss_frontpage": "forum",
    "reddit_technology": "forum",
    "threads_profile": "social",
    "x_rsshub": "social",
    "x_trendsentinel": "social",
    "example_public_api": "news",
    "example_rss": "news",
}


def upgrade() -> None:
    op.execute("ALTER TABLE sources ADD COLUMN source_type text")
    op.execute("ALTER TABLE collected_items ADD COLUMN language text")
    op.execute("ALTER TABLE collected_items ADD COLUMN content_type text")
    op.execute("ALTER TABLE collected_items ADD COLUMN author_url text")
    op.execute("ALTER TABLE collected_items ADD COLUMN thumbnail_url text")

    # Backfill content_type where derivable without guessing.
    op.execute(
        """
        UPDATE collected_items
        SET content_type = CASE
            WHEN url IS NOT NULL AND length(btrim(url)) > 0 THEN 'link'
            WHEN text IS NOT NULL AND length(btrim(text)) > 0 THEN 'self'
            ELSE NULL
        END
        """
    )

    # Backfill source_type for the seeded sources. Keys/kinds are hardcoded constants.
    for key, kind in _SOURCE_TYPES.items():
        op.execute(f"UPDATE sources SET source_type = '{kind}' WHERE key = '{key}'")


def downgrade() -> None:
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS thumbnail_url")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS author_url")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS content_type")
    op.execute("ALTER TABLE collected_items DROP COLUMN IF EXISTS language")
    op.execute("ALTER TABLE sources DROP COLUMN IF EXISTS source_type")
