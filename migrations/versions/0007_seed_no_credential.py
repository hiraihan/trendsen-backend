"""Seed example no-credential source rows.

Demonstrates the no-credential class (ADR-0008): one row adds a generic RSS feed or a
public JSON endpoint, with the mandatory domain allowlist on each. The rows seed inactive —
an operator flips ``is_active`` once the target is verified — so nothing is fetched until a
human opts in.

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-26

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "example_rss",
        "name": "Example public RSS feed",
        "integration": "rss",
        "base_url": "https://example.com/feed.xml",
        "config": {
            "interval_minutes": 60,
            "limit": 25,
            # Mandatory for no-credential sources (ADR-0008).
            "allowed_domains": "example.com",
        },
    },
    {
        "key": "example_public_api",
        "name": "Example public JSON endpoint",
        "integration": "public_api",
        "base_url": "https://api.example.com/items",
        "config": {
            "interval_minutes": 60,
            "limit": 25,
            "allowed_domains": "api.example.com",
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
    # Extend the integration check to admit the no-credential JSON adapter (ADR-0008).
    op.execute(
        "ALTER TABLE sources DROP CONSTRAINT sources_integration_valid"
    )
    op.execute(
        "ALTER TABLE sources ADD CONSTRAINT sources_integration_valid "
        "CHECK (integration IN ('api', 'rss', 'html', 'public_api'))"
    )

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

    # Backfill allowlists for the two trusted RSS seeds (ADR-0008): the no-credential
    # class now requires an allowlist, so the seeded sources must be brought into
    # compliance.
    for key, domain in (("hnrss_frontpage", "hnrss.org"), ("github_blog", "github.blog")):
        bind.execute(
            text(
                "UPDATE sources SET config = config || CAST(:patch AS jsonb) "
                "WHERE key = :key"
            ),
            {"key": key, "patch": json.dumps({"allowed_domains": domain})},
        )

def downgrade() -> None:
    op.get_bind().execute(
        text("DELETE FROM sources WHERE key = ANY(:keys)"),
        {"keys": [source["key"] for source in SOURCES]},
    )

    op.execute(
        "ALTER TABLE sources DROP CONSTRAINT sources_integration_valid"
    )
    op.execute(
        "ALTER TABLE sources ADD CONSTRAINT sources_integration_valid "
        "CHECK (integration IN ('api', 'rss', 'html'))"
    )
