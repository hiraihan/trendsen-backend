"""Orchestrate the read-time viral filter over a fetched population.

Kept separate from the repository so the ranking math stays pure and unit-testable, and
separate from the routes so list and export share one classification path.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from trendsentinel.db.repositories.items import ItemWithMetrics
from trendsentinel.services import ranking

__all__ = ["classify_viral"]


def _day(item: ItemWithMetrics) -> date:
    ts = item.item.published_at or item.item.captured_at
    return ts.date()


def classify_viral(
    rows: list[ItemWithMetrics],
    band: ranking.Band | None,
    keywords: Sequence[str],
) -> dict[int, float]:
    """Return ``{item_id: percentile}`` for the viral items in ``rows``.

    Keywords select the population first, and the percentile is computed over that matching
    group (ADR-0007). A source with no keywords ranks over its full per-day population. A
    missing band (no ranking metric) yields nothing.
    """
    if band is None:
        return {}

    by_day: dict[date, list[ItemWithMetrics]] = {}
    for row in rows:
        by_day.setdefault(_day(row), []).append(row)

    viral: dict[int, float] = {}
    for members in by_day.values():
        matching = [
            m
            for m in members
            if ranking.match_keywords(
                f"{m.item.title or ''} {m.item.text or ''}", keywords
            )
        ]
        cohort = [
            value
            for m in matching
            if (value := ranking.metric_value(m.metrics, band.metric)) is not None
        ]
        for member in matching:
            percentile = ranking.rank_item(member.metrics, cohort, band)
            if percentile is not None:
                viral[member.item.id] = percentile
    return viral
