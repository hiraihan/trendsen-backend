"""Viral ranking: percentile math and band resolution.

Pure functions — no database, no I/O — so the whole module is unit-testable against
recorded cohorts. The repository and routes call into these to classify items.

The model (ADR-0007): store everything, filter at read time. A ``Band`` is resolved per
source from the global defaults plus per-source overrides. An item's percentile is its
nearest-rank position within its day's population, and it is "viral" when that percentile
falls inside the band.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from trendsentinel.config import Settings

__all__ = [
    "Band",
    "match_keywords",
    "metric_value",
    "percentile_of",
    "primary_metric",
    "rank_item",
    "resolve_band",
]

#: Default ranking (primary) metric per collector key / integration. A source whose key is
#: absent here has no engagement metric and can never rank as viral.
_INTEGRATION_METRIC: dict[str, str] = {
    "hackernews": "score",
    "reddit": "score",
    "x": "likes",
    "threads": "views",
}


@dataclass(frozen=True, slots=True)
class Band:
    """A quantile band: inclusive [low, high] plus the metric it applies to."""

    metric: str
    low: int
    high: int

    def contains(self, percentile: float) -> bool:
        return self.low <= percentile <= self.high


def primary_metric(key: str, config: Mapping[str, object]) -> str | None:
    """Return the ranking metric for a collector ``key``, honouring a config override."""
    override = config.get("ranking_metric")
    if isinstance(override, str) and override.strip():
        return override.strip()
    return _INTEGRATION_METRIC.get(key)


def _quantile_override(config: Mapping[str, object], name: str) -> int | None:
    value = config.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value if 0 <= value <= 100 else None


def _quantile(
    config: Mapping[str, object], name: str, global_value: int
) -> int:
    override = _quantile_override(config, name)
    return override if override is not None else global_value


def resolve_band(
    key: str, config: Mapping[str, object], settings: Settings
) -> Band | None:
    """Resolve the band for a source, or ``None`` when it has no ranking metric."""
    metric = primary_metric(key, config)
    if not metric:
        return None

    low = _quantile(config, "viral_quantile_low", settings.viral_quantile_low)
    high = _quantile(config, "viral_quantile_high", settings.viral_quantile_high)
    if not (0 <= low < high <= 100):
        return None

    return Band(metric=metric, low=low, high=high)


def percentile_of(value: float, cohort: Sequence[float]) -> float:
    """Nearest-rank percentile of ``value`` within ``cohort`` (0..100).

    The percentile is the share of the cohort strictly below the value. The cohort's
    maximum therefore sits at slightly under 100 (e.g. 75 of four items), which keeps the
    top of even a small cohort inside a band whose high bound is below 100 — the ``viral``
    item never falls just outside the band because it is the very maximum. A missing or
    empty cohort yields 0.
    """
    if not cohort:
        return 0.0
    below = sum(1 for v in cohort if v < value)
    return (below / len(cohort)) * 100.0


def match_keywords(text: str, keywords: Sequence[str]) -> bool:
    """Return whether any keyword is a case-insensitive substring of ``text``.

    An empty keyword list matches everything (a source with no keywords ranks over its full
    population, per ADR-0007).
    """
    if not keywords:
        return True
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def metric_value(metrics: Mapping[str, Any] | None, metric: str) -> float | None:
    """Return the item's ranking-metric value, or ``None`` when absent/non-numeric."""
    if not metrics:
        return None
    value = metrics.get(metric)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return float(value)


def rank_item(
    metrics: Mapping[str, Any] | None,
    cohort_values: Sequence[float],
    band: Band,
) -> float | None:
    """Classify one item against its day's cohort.

    Returns the item's percentile when it is inside the band (viral), or ``None``
    when it is outside the band or lacks the ranking metric. Missing-metric items never
    rank (ADR-0007 decision 6).
    """
    value = metric_value(metrics, band.metric)
    if value is None:
        return None
    percentile = percentile_of(value, cohort_values)
    if not band.contains(percentile):
        return None
    return percentile
