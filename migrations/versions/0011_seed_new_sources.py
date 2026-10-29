"""Seed credential-free news sources: The Verge, TechCrunch, and Wikimedia pageviews.

All three are no-credential sources (ADR-0008), so every row carries the mandatory domain
allowlist. The RSS rows reuse the existing ``rss`` collector; Wikimedia uses the new
``wikimedia`` collector (``integration='api'`` + ``collector_key='wikimedia'``) and is the
only one of these with configured ``articles`` to track.

Lobsters and Ars Technica are deliberately **not** seeded: Lobsters' ``robots.txt``
disallows the wildcard user agent (``Disallow: /``), and Ars Technica's ``robots.txt`` is
behind an AWS WAF human-verification challenge, so it cannot be verified. Both are excluded
per the project's robots/terms review gate (see ``docs/sources.md``).

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-27
"""

from __future__ import annotations

import json
from typing import Any

from alembic import op
from sqlalchemy import text

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | None = None
depends_on: str | None = None

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "key": "theverge",
        "name": "The Verge",
        "integration": "rss",
        "base_url": "https://www.theverge.com/rss/index.xml",
        "source_type": "news",
        "supported_content_types": ["article"],
        "config": {
            "collector_key": "rss",
            "interval_minutes": 60,
            "limit": 25,
            "allowed_domains": "theverge.com",
        },
    },
    {
        "key": "techcrunch",
        "name": "TechCrunch",
        "integration": "rss",
        "base_url": "https://techcrunch.com/feed/",
        "source_type": "news",
        "supported_content_types": ["article"],
        "config": {
            "collector_key": "rss",
            "interval_minutes": 60,
            "limit": 25,
            "allowed_domains": "techcrunch.com",
        },
    },
    {
        "key": "wikimedia",
        "name": "Wikimedia pageviews",
        "integration": "api",
        "base_url": "https://wikimedia.org/api/rest_v1/",
        "source_type": "news",
        "supported_content_types": ["page"],
        "config": {
            "collector_key": "wikimedia",
            "interval_minutes": 1440,
            "limit": 200,
            "allowed_domains": "wikimedia.org",
            "project": "en.wikipedia",
            "access": "all-access",
            "agent": "user",
            "granularity": "daily",
            "days": 7,
            "articles": [
                "JavaScript",
                "Python (programming language)",
                "Artificial intelligence",
            ],
        },
    },
)

_INSERT = text(
    """
    INSERT INTO sources (
        key, name, integration, base_url, config, is_active, source_type, supported_content_types
    )
    VALUES (
        :key, :name, :integration, :base_url, CAST(:config AS jsonb), false, :source_type,
        CAST(:supported_content_types AS text[])
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
                "source_type": source["source_type"],
                "supported_content_types": source["supported_content_types"],
            },
        )


def downgrade() -> None:
    op.get_bind().execute(
        text("DELETE FROM sources WHERE key = ANY(:keys)"),
        {"keys": [source["key"] for source in SOURCES]},
    )
