"""Unit tests for the viral-ranking service (`services/ranking.py`)."""

from __future__ import annotations

import pytest

from tests.helpers import build_settings
from trendsentinel.config import Settings
from trendsentinel.services.ranking import (
    Band,
    match_keywords,
    metric_value,
    percentile_of,
    primary_metric,
    resolve_band,
)


def settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


# -- resolve_band ---------------------------------------------------------------


def test_resolve_band_defaults_for_known_metric() -> None:
    band = resolve_band("hackernews", {}, settings())

    assert isinstance(band, Band)
    assert band.metric == "score"
    assert band.low == 50
    assert band.high == 95


@pytest.mark.parametrize(
    "key,metric",
    [("hackernews", "score"), ("reddit", "score"), ("x", "likes"), ("threads", "views")],
)
def test_resolve_band_primary_metric_per_platform(key: str, metric: str) -> None:
    assert primary_metric(key, {}) == metric


def test_resolve_band_none_for_no_metric_source() -> None:
    assert resolve_band("rss", {}, settings()) is None
    assert resolve_band("public_api", {}, settings()) is None


def test_resolve_band_honours_config_override() -> None:
    band = resolve_band(
        "x", {"viral_quantile_low": 10, "viral_quantile_high": 80}, settings()
    )

    assert band is not None
    assert band.low == 10
    assert band.high == 80


def test_resolve_band_ranking_metric_override() -> None:
    band = resolve_band("x", {"ranking_metric": "views"}, settings())

    assert band is not None
    assert band.metric == "views"


def test_resolve_band_invalid_band_returns_none() -> None:
    config = {"viral_quantile_low": 90, "viral_quantile_high": 50}
    assert resolve_band("x", config, settings()) is None


# -- Band.contains --------------------------------------------------------------

def test_band_contains_is_inclusive_of_boundaries() -> None:
    band = Band(metric="likes", low=50, high=95)

    assert band.contains(50)
    assert band.contains(95)
    assert band.contains(75)
    assert not band.contains(49)
    assert not band.contains(96)


# -- percentile_of --------------------------------------------------------------

COHORT = [10.0, 20.0, 30.0, 40.0]


def test_percentile_of_strictly_below_share() -> None:
    assert percentile_of(25.0, COHORT) == 50.0


def test_percentile_of_maximum_stays_below_100() -> None:
    assert percentile_of(40.0, COHORT) == 75.0


def test_percentile_of_minimum_is_0() -> None:
    assert percentile_of(10.0, COHORT) == 0.0


def test_percentile_of_ties_do_not_count_below() -> None:
    assert percentile_of(20.0, COHORT) == 25.0


def test_percentile_of_empty_cohort_is_0() -> None:
    assert percentile_of(10.0, []) == 0.0


def test_percentile_of_single_item_cohort_is_0() -> None:
    assert percentile_of(5.0, [5.0]) == 0.0


def test_percentile_of_value_above_cohort_is_100() -> None:
    assert percentile_of(999.0, COHORT) == 100.0


# -- match_keywords --------------------------------------------------------------

def test_match_keywords_empty_matches_all() -> None:
    assert match_keywords("anything", [])


def test_match_keywords_case_insensitive_substring() -> None:
    assert match_keywords("The AI Revolution", ("ai", "ml"))
    assert not match_keywords("The Cake", ("ai",))


def test_match_keywords_any_keyword_matches() -> None:
    assert match_keywords("ML systems", ("ai", "ml"))


# -- metric_value --------------------------------------------------------------

def test_metric_value_numeric() -> None:
    assert metric_value({"likes": 7}, "likes") == 7.0


def test_metric_value_missing_metric_is_none() -> None:
    assert metric_value({"score": 1}, "likes") is None


def test_metric_value_missing_metrics_is_none() -> None:
    assert metric_value(None, "likes") is None


@pytest.mark.parametrize("bad", ["5", True, None, [1]])
def test_metric_value_non_numeric_is_none(bad: object) -> None:
    assert metric_value({"likes": bad}, "likes") is None
