"""Seed the X (Twitter) source as inactive, pending credentials.

Reading X API v2 requires an app-only bearer token and a paid read tier. The token belongs
in ``sources.config`` (ADR-0003) and must never live in a migration, so this row is seeded
``is_active = false``; the operator provisions the token with the runbook in
``docs/sources.md`` (ticket T27) and only then activates the source.

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-23

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "x_trendsentinel",
        "name": "X recent search (API)",
        "integration": "api",
        "base_url": "https://api.twitter.com",
        "config": {
            "collector_key": "x",
            "interval_minutes": 30,
            "limit": 50,
            # The search terms themselves are not a secret and are safe to seed.
            "query": "trendsentinel OR trend detection",
        },
    },
)

_INSERT = text(
    """
    INSERT INTO sources (key, name, integration, base_url, config, is_active)
    VALUES (
        :key, :name, :integration, :base_url, CAST(:config AS jsonb), false
    )
    ON CONFLICT (key) DO NOTHING
    """
)


def upgrade() -> None:
    bind = op.get_bind()
    for source in SOURCES:
        bind.execute(
            _INSERT,
            {
                "key": source["key"],
                "name": source["name"],
                "integration": source["integration"],
                "base_url": source["base_url"],
                "config": json.dumps(source["config"]),
            },
        )


def downgrade() -> None:
    # Deletes exactly the seeded keys, leaving any other source rows untouched.
    op.get_bind().execute(
        text("DELETE FROM sources WHERE key = ANY(:keys)"),
        {"keys": [source["key"] for source in SOURCES]},
    )
