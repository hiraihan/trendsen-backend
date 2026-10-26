"""Seed the Reddit source as inactive, pending credentials.

Reddit has no key-free read access: the collector needs a script-app client id, client secret,
and a descriptive User-Agent, all read from ``sources.config`` (see ADR-0003). A migration
must not carry secrets, so this row is seeded ``is_active = false`` and the credentials are
provisioned out of band with the runbook in ``docs/sources.md`` (ticket T27). Activating a
source without credentials would only produce a rejected run every interval.

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-23

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "reddit_technology",
        "name": "Reddit r/technology (API)",
        "integration": "api",
        # The subreddit is part of the base URL; the collector appends ``/new``.
        "base_url": "https://oauth.reddit.com/r/technology",
        "config": {
            "collector_key": "reddit",
            "interval_minutes": 30,
            "limit": 25,
            # Reddit's OAuth allowance is generous but not unlimited; keep paging gentle.
            "min_request_delay_ms": 1000,
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
