"""Seed the X (via self-hosted RSSHub) source as inactive.

Free X access runs through a self-hosted RSSHub instance instead of the paid X API
(T22). The feed is a public RSS URL, so this source is the no-credential class
(ADR-0008): it carries no secret, but its host **must** be in
``config.allowed_domains`` before it can be activated.

The instance host is operator-specific, so the row seeds with a placeholder host and
``is_active = false``. The operator edits ``base_url`` and ``config.allowed_domains``
to the real RSSHub public hostname, then flips ``is_active`` — the SSRF allowlist
(``services/collection.py``) rejects the run until both match.

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-26

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "x_rsshub",
        "name": "X via self-hosted RSSHub (RSS)",
        "integration": "rss",
        # Placeholder host — set to your public RSSHub hostname before activating.
        "base_url": "https://rsshub.invalid/twitter/user/trendsentinel",
        "config": {
            "interval_minutes": 30,
            "limit": 25,
            # Mandatory for no-credential sources (ADR-0008). Must match the RSSHub host.
            "allowed_domains": "rsshub.invalid",
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
    op.get_bind().execute(
        text("DELETE FROM sources WHERE key = ANY(:keys)"),
        {"keys": [source["key"] for source in SOURCES]},
    )
