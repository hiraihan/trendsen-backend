"""Seed the Threads source as inactive, pending credentials.

Threads needs a long-lived user access token minted by hand in the Meta developer console,
and the ``me/threads`` read permission is granted only after Meta App Review (ADR-0003,
ADR-0004 case 3). A migration must not carry secrets, so this row is seeded
``is_active = false`` and the token is provisioned out of band with the runbook in
``docs/sources.md`` (ticket T27).

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-23

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "threads_profile",
        "name": "Threads profile posts (API)",
        "integration": "api",
        # ``me/threads`` is always the authenticated profile's own posts.
        "base_url": "https://graph.threads.net/v1.0",
        "config": {
            "collector_key": "threads",
            "interval_minutes": 60,
            "limit": 25,
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
