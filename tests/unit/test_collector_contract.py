"""Tests for the collector contract and registry."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from tests.helpers import build_settings
from trendsentinel.collectors.base import (
    CollectedItem,
    CollectError,
    CollectionResult,
    Collector,
    ErrorKind,
    SourceConfig,
    SourceRef,
    UnknownCollectorError,
)
from trendsentinel.collectors.http import HttpFetcher
from trendsentinel.collectors.registry import available_keys, get_collector, register, resolve_key
from trendsentinel.config import Settings


def make_settings(**overrides: object) -> Settings:
    return build_settings(**overrides)


class FakeCollector:
    """Minimal collector used to prove the protocol is satisfiable."""

    key = "fake"

    async def collect(self, source: SourceRef, limit: int) -> CollectionResult:
        return CollectionResult(items=[CollectedItem(external_id="1")])


def fake_fetcher() -> HttpFetcher:
    """A stand-in for the run's HTTP fetcher; the fake collector never uses it."""
    return HttpFetcher(build_settings(), resolver=lambda _host: ["93.184.216.34"])


# --------------------------------------------------------------------------- contract


def test_collected_item_defaults() -> None:
    item = CollectedItem(external_id="42")

    assert item.url is None
    assert item.title is None
    assert item.text is None
    assert item.author is None
    assert item.published_at is None
    assert item.metadata == {}
    assert item.metrics is None


def test_captured_at_is_timezone_aware_utc() -> None:
    item = CollectedItem(external_id="42")

    assert item.captured_at.tzinfo is not None
    assert item.captured_at.utcoffset() == timedelta(0)


def test_collected_item_is_frozen_and_hashable_by_identity() -> None:
    item = CollectedItem(external_id="42")

    with pytest.raises(Exception):  # noqa: B017 - dataclass frozen raises FrozenInstanceError
        item.title = "changed"  # type: ignore[misc]


def test_fake_collector_satisfies_the_protocol() -> None:
    assert isinstance(FakeCollector(), Collector)


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_validate_rejects_blank_external_id(value: str) -> None:
    with pytest.raises(ValueError, match="external_id is empty"):
        CollectedItem(external_id=value).validate()


def test_validate_accepts_item_with_only_an_identity() -> None:
    # Nothing else is mandatory: sources genuinely omit url, text and author.
    CollectedItem(external_id="42").validate()


def test_validate_rejects_naive_published_at() -> None:
    item = CollectedItem(external_id="42", published_at=datetime(2026, 1, 1))

    with pytest.raises(ValueError, match="timezone-aware"):
        item.validate()


@pytest.mark.parametrize("metrics", [{"score": "many"}, {"score": True}, {"score": None}])
def test_validate_rejects_non_numeric_metrics(metrics: dict[str, Any]) -> None:
    item = CollectedItem(external_id="42", metrics=metrics)

    with pytest.raises(ValueError, match="must be numeric"):
        item.validate()


def test_normalized_trims_whitespace_and_maps_blanks_to_none() -> None:
    item = CollectedItem(
        external_id="  42  ",
        url="  https://example.test/a  ",
        title="   ",
        text="",
        author="  pg ",
    ).normalized()

    assert item.external_id == "42"
    assert item.url == "https://example.test/a"
    # Empty values become None rather than empty strings, so "absent" has one representation.
    assert item.title is None
    assert item.text is None
    assert item.author == "pg"


def test_normalized_converts_offsets_to_utc() -> None:
    published = datetime(2026, 6, 1, 12, 0, tzinfo=timezone(timedelta(hours=7)))

    item = CollectedItem(external_id="42", published_at=published).normalized()

    assert item.published_at == datetime(2026, 6, 1, 5, 0, tzinfo=UTC)


def test_normalization_is_idempotent() -> None:
    once = CollectedItem(external_id=" 42 ", title=" x ").normalized()
    twice = once.normalized()

    assert once == twice


# --------------------------------------------------------------------------- results


def test_result_ok_flag_reflects_errors() -> None:
    assert CollectionResult(items=[CollectedItem(external_id="1")]).ok is True
    assert CollectionResult(errors=[CollectError(ErrorKind.TIMEOUT, "timed out")]).ok is False


def test_result_merge_preserves_order() -> None:
    first = CollectionResult(items=[CollectedItem(external_id="1")])
    second = CollectionResult(
        items=[CollectedItem(external_id="2")], errors=[CollectError(ErrorKind.NETWORK, "x")]
    )

    merged = first.merge(second)

    assert [item.external_id for item in merged.items] == ["1", "2"]
    assert len(merged.errors) == 1


def test_collect_error_serializes_for_the_run_summary() -> None:
    error = CollectError(ErrorKind.RATE_LIMITED, "429", retryable=True, detail="Retry-After: 5")

    payload = error.as_dict()

    assert payload["kind"] == "rate_limited"
    assert payload["message"] == "429"
    assert payload["retryable"] is True
    assert payload["detail"] == "Retry-After: 5"


def test_collect_error_omits_absent_detail() -> None:
    assert "detail" not in CollectError(ErrorKind.NETWORK, "x").as_dict()


def test_collect_error_serializes_the_status_code_when_present() -> None:
    payload = CollectError(
        ErrorKind.HTTP_STATUS, "HTTP 401", status_code=401
    ).as_dict()

    assert payload["status_code"] == 401


def test_collect_error_omits_an_absent_status_code() -> None:
    assert "status_code" not in CollectError(ErrorKind.NETWORK, "x").as_dict()


# --------------------------------------------------------------------------- source refs


def test_source_ref_config_accessors_fall_back_on_bad_types() -> None:
    source = SourceRef(
        base_url="https://example.test",
        key="demo",
        config={"limit": "many", "interval_minutes": 15, "attribution": "example"},
    )

    assert source.config_int("limit", 30) == 30
    assert source.config_int("interval_minutes", 30) == 15
    assert source.config_str("attribution") == "example"
    assert source.config_str("missing", "fallback") == "fallback"


def test_source_ref_rejects_bool_as_int() -> None:
    # `True` is an int in Python; a config typo must not silently become a count of 1.
    source = SourceRef(base_url="https://example.test", config={"limit": True})

    assert source.config_int("limit", 30) == 30


def test_source_config_splits_known_from_extra_keys() -> None:
    source = SourceRef(
        base_url="https://example.test",
        config={
            "collector_key": "rss",
            "limit": 10,
            "interval_minutes": 60,
            "min_request_delay_ms": 1000,
            "attribution": "hnrss.org",
        },
    )

    config = SourceConfig.from_ref(source, default_limit=30, default_interval=30)

    assert config.limit == 10
    assert config.interval_minutes == 60
    assert config.min_request_delay_ms == 1000
    assert config.extra == {"attribution": "hnrss.org"}


def test_source_config_parses_keywords_from_a_comma_string() -> None:
    source = SourceRef(base_url="https://example.test", config={"keywords": "AI,  ml ,LLM"})

    config = SourceConfig.from_ref(source, default_limit=30, default_interval=30)

    assert config.keywords == ("ai", "ml", "llm")


def test_source_config_parses_keywords_from_a_list_and_dedupes() -> None:
    source = SourceRef(base_url="https://example.test", config={"keywords": ["AI", "ai", "NLP"]})

    config = SourceConfig.from_ref(source, default_limit=30, default_interval=30)

    assert config.keywords == ("ai", "nlp")


def test_source_config_defaults_keywords_to_empty() -> None:
    config = SourceConfig.from_ref(SourceRef(base_url="https://example.test"), 30, 30)

    assert config.keywords == ()


def test_source_config_parses_quantile_overrides() -> None:
    source = SourceRef(
        base_url="https://example.test",
        config={"viral_quantile_low": 40, "viral_quantile_high": 90},
    )

    config = SourceConfig.from_ref(source, 30, 30)

    assert config.viral_quantile_low == 40
    assert config.viral_quantile_high == 90


@pytest.mark.parametrize("value", ["low", 101, -1, True, 40.5])
def test_source_config_ignores_malformed_quantile_overrides(value: object) -> None:
    config = SourceConfig.from_ref(
        SourceRef(base_url="https://example.test", config={"viral_quantile_low": value}), 30, 30
    )

    assert config.viral_quantile_low is None


def test_source_config_parses_allowed_domains() -> None:
    source = SourceRef(
        base_url="https://example.test",
        config={"allowed_domains": "Example.com, blog.sub.org, "},
    )

    config = SourceConfig.from_ref(source, 30, 30)

    assert config.allowed_domains == ("example.com", "blog.sub.org")


# --------------------------------------------------------------------------- registry


def test_registry_returns_the_registered_collector() -> None:
    @register("unit-test-collector")
    def factory(settings: Settings, fetcher: HttpFetcher) -> FakeCollector:
        return FakeCollector()

    collector = get_collector(
        make_settings(), fake_fetcher(), "api", {"collector_key": "unit-test-collector"}
    )

    assert isinstance(collector, FakeCollector)
    assert "unit-test-collector" in available_keys()


@pytest.mark.parametrize(
    ("integration", "config", "expected"),
    [
        ("rss", None, "rss"),
        ("rss", {}, "rss"),
        ("rss", {"collector_key": "custom"}, "custom"),
        ("rss", {"collector_key": "  "}, "rss"),
        ("api", {"collector_key": 123}, "api"),
    ],
)
def test_resolve_key_prefers_a_usable_collector_key(
    integration: str, config: dict[str, Any] | None, expected: str
) -> None:
    assert resolve_key(integration, config) == expected


def test_get_collector_raises_for_an_unknown_key() -> None:
    with pytest.raises(UnknownCollectorError) as excinfo:
        get_collector(make_settings(), fake_fetcher(), "html", {"collector_key": "does-not-exist"})

    assert excinfo.value.key == "does-not-exist"


def test_register_rejects_an_empty_key() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        register("   ")


def test_register_rejects_a_duplicate_key() -> None:
    @register("duplicate-check")
    def first(settings: Settings, fetcher: HttpFetcher) -> FakeCollector:
        return FakeCollector()

    with pytest.raises(ValueError, match="already registered"):

        @register("duplicate-check")
        def second(settings: Settings, fetcher: HttpFetcher) -> FakeCollector:  # pragma: no cover
            return FakeCollector()
