"""Seed the verified MVP sources.

Config holds non-sensitive values only. `hnrss_frontpage` is a third-party volunteer service
rather than an official API, so it runs at a longer interval with an explicit inter-request
delay and retains its attribution.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-21

"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "hackernews",
        "name": "Hacker News (API)",
        "integration": "api",
        "base_url": "https://hacker-news.firebaseio.com/v0",
        "config": {"collector_key": "hackernews", "interval_minutes": 15, "limit": 50},
    },
    {
        "key": "hnrss_frontpage",
        "name": "Hacker News Front Page (RSS)",
        "integration": "rss",
        "base_url": "https://hnrss.org/frontpage",
        "config": {
            "collector_key": "rss",
            "interval_minutes": 60,
            "limit": 30,
            # Attribution and a gentler request cadence: this is a volunteer-run service.
            "attribution": "hnrss.org",
            "min_request_delay_ms": 1000,
        },
    },
    {
        "key": "github_blog",
        "name": "The GitHub Blog (RSS)",
        "integration": "rss",
        "base_url": "https://github.blog/feed/",
        "config": {"collector_key": "rss", "interval_minutes": 120, "limit": 30},
    },
)

_INSERT = text(
    """
    INSERT INTO sources (key, name, integration, base_url, config, is_active)
    VALUES (
        :key, :name, :integration, :base_url, CAST(:config AS jsonb), true
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
