"""Unit tests for the viral orchestration (`services/viral.py`)."""

from __future__ import annotations

from datetime import UTC, datetime

from trendsentinel.db.repositories.items import ItemWithMetrics
from trendsentinel.db.rows import ItemRow
from trendsentinel.services.ranking import Band
from trendsentinel.services.viral import classify_viral

DAY1 = datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
DAY2 = datetime(2026, 10, 22, 8, 0, tzinfo=UTC)

BAND = Band(metric="score", low=50, high=95)


def make_row(
    item_id: int,
    title: str = "post",
    *,
    published: datetime | None = DAY1,
) -> ItemRow:
    return ItemRow(
        id=item_id,
        source_id=1,
        external_id=str(item_id),
        url=f"https://example.test/{item_id}",
        title=title,
        text=None,
        author="tester",
        published_at=published,
        metadata={},
        captured_at=DAY1,
        last_seen_at=None,
    )


def mk(
    item_id: int,
    score: float | None,
    title: str = "post",
    *,
    published: datetime = DAY1,
) -> ItemWithMetrics:
    metrics: dict[str, float] = {}
    if score is not None:
        metrics["score"] = score
    return ItemWithMetrics(item=make_row(item_id, title, published=published), metrics=metrics)


def ids(result: dict[int, float]) -> set[int]:
    return set(result)


# -- band membership ------------------------------------------------------------

def test_no_band_returns_empty() -> None:
    rows = [mk(1, 50.0)]
    assert classify_viral(rows, None, ()) == {}


def test_band_membership_within_cohort() -> None:
    rows = [mk(1, 10.0), mk(2, 20.0), mk(3, 30.0), mk(4, 40.0)]
    result = classify_viral(rows, BAND, ())

    assert ids(result) == {3, 4}
    assert 0 < result[4] <= 100


def test_missing_metric_never_viral() -> None:
    rows = [mk(1, None), mk(2, 30.0), mk(3, 40.0)]
    result = classify_viral(rows, BAND, ())

    # 2 (score 30) is at percentile 0 and is excluded; only 3 (score 40) is viral.
    assert ids(result) == {3}


# -- keywords --------------------------------------------------------------------

def test_keywords_narrow_population_and_cohort() -> None:
    # AI cohort: [10, 100] -> 100 is viral, 10 is not. The non-AI post scores 500 but is
    # outside the keyword population, so it is excluded entirely.
    rows = [
        mk(1, 10.0, title="AI post"),
        mk(2, 100.0, title="AI big"),
        mk(3, 500.0, title="unrelated"),
    ]
    result = classify_viral(rows, BAND, ("ai",))

    assert ids(result) == {2}


def test_empty_keywords_rank_full_population() -> None:
    rows = [mk(1, 10.0), mk(2, 20.0), mk(3, 30.0)]
    result = classify_viral(rows, BAND, ())

    assert ids(result) == {3}


# -- cohorts ---------------------------------------------------------------------

def test_days_are_separate_cohorts() -> None:
    # Each day cohort [10, 100]: only the 100 in each day is viral.
    rows = [
        mk(1, 10.0, published=DAY1),
        mk(2, 100.0, published=DAY1),
        mk(3, 10.0, published=DAY2),
        mk(4, 100.0, published=DAY2),
    ]
    result = classify_viral(rows, BAND, ())

    assert ids(result) == {2, 4}


def test_single_item_cohort_ranks_viral() -> None:
    rows = [mk(1, 50.0)]
    # A lone item is at percentile 0, outside P50-P95; only a band reaching 0 includes it.
    result = classify_viral(rows, Band(metric="score", low=0, high=100), ())

    assert ids(result) == {1}
