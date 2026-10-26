"""Add an optional per-run item limit to ``collection_runs``.

``POST /collect/run`` accepts ``{"limit": n}``. With the default configuration the endpoint
only *enqueues* a run, and the worker executes it later from a different process, so the
requested limit has to be stored somewhere durable. Without a column the limit would be
silently dropped on exactly the path that is enabled by default.

The column is nullable: most runs use the source's configured limit.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-21

"""

from __future__ import annotations

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE collection_runs
            ADD COLUMN item_limit integer,
            ADD CONSTRAINT collection_runs_item_limit_positive
                CHECK (item_limit IS NULL OR item_limit > 0)
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE collection_runs
            DROP CONSTRAINT IF EXISTS collection_runs_item_limit_positive,
            DROP COLUMN IF EXISTS item_limit
        """
    )
