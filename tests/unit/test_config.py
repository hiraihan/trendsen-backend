"""Tests for central configuration."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tests.helpers import BASE_ENV, build_settings
from trendsentinel.config import Settings, get_settings

_REQUIRED = BASE_ENV


def make_settings(**overrides: object) -> Settings:
    """Build settings from explicit values, ignoring any ``.env`` on disk."""
    return build_settings(**overrides)


def test_defaults() -> None:
    settings = make_settings()

    assert settings.log_level == "INFO"
    assert settings.log_format == "json"
    assert settings.http_timeout_seconds == 10.0
    assert settings.http_max_retries == 3
    assert settings.max_items_per_run == 100
    assert settings.collect_interval_minutes == 30
    assert settings.stale_run_minutes == 30
    assert settings.run_manual_inline is False
    assert settings.max_text_chars == 2000
    assert settings.source_reload_seconds == 60
    assert settings.snapshot_purge_enabled is False


def test_settings_are_frozen() -> None:
    settings = make_settings()
    with pytest.raises(ValidationError):
        settings.log_level = "DEBUG"  # type: ignore[misc]


@pytest.mark.parametrize("missing", sorted(_REQUIRED))
def test_missing_required_field_fails(missing: str) -> None:
    values = {k: v for k, v in _REQUIRED.items() if k != missing}

    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None, **values)  # type: ignore[arg-type]

    assert missing in str(excinfo.value)


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", _REQUIRED["database_url"])
    monkeypatch.setenv("ADMIN_API_KEY", _REQUIRED["admin_api_key"])
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("MAX_ITEMS_PER_RUN", "7")
    monkeypatch.setenv("RUN_MANUAL_INLINE", "true")

    get_settings.cache_clear()
    try:
        settings = get_settings()
    finally:
        get_settings.cache_clear()

    assert settings.log_level == "DEBUG"
    assert settings.max_items_per_run == 7
    assert settings.run_manual_inline is True


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", _REQUIRED["database_url"])
    monkeypatch.setenv("ADMIN_API_KEY", _REQUIRED["admin_api_key"])

    get_settings.cache_clear()
    try:
        assert get_settings() is get_settings()
    finally:
        get_settings.cache_clear()


def test_snapshot_purge_is_off_by_default_and_can_be_enabled_by_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The purge destroys snapshots, so the only way to arm it is an explicit opt-in."""
    assert make_settings().snapshot_purge_enabled is False

    monkeypatch.setenv("DATABASE_URL", _REQUIRED["database_url"])
    monkeypatch.setenv("ADMIN_API_KEY", _REQUIRED["admin_api_key"])
    monkeypatch.setenv("SNAPSHOT_PURGE_ENABLED", "true")

    get_settings.cache_clear()
    try:
        settings = get_settings()
    finally:
        get_settings.cache_clear()

    assert settings.snapshot_purge_enabled is True


def test_bounds_are_enforced() -> None:
    with pytest.raises(ValidationError):
        make_settings(max_items_per_run=0)
    with pytest.raises(ValidationError):
        make_settings(http_max_retries=99)
    with pytest.raises(ValidationError):
        make_settings(log_format="xml")
    with pytest.raises(ValidationError):
        make_settings(source_reload_seconds=0)


def test_source_reload_interval_can_be_set_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The reload cadence is deployment-tunable, so a slow source list can be polled less."""
    monkeypatch.setenv("DATABASE_URL", _REQUIRED["database_url"])
    monkeypatch.setenv("ADMIN_API_KEY", _REQUIRED["admin_api_key"])
    monkeypatch.setenv("SOURCE_RELOAD_SECONDS", "300")

    get_settings.cache_clear()
    try:
        settings = get_settings()
    finally:
        get_settings.cache_clear()

    assert settings.source_reload_seconds == 300


def test_assert_runtime_ready_passes_for_valid_settings() -> None:
    make_settings().assert_runtime_ready()


def test_admin_key_blank_is_allowed_but_reported_as_unconfigured() -> None:
    settings = make_settings(admin_api_key="   ")

    assert settings.admin_api_key_configured is False
    # A blank key disables the trigger endpoint; it is a valid deployment, not an error.
    settings.assert_runtime_ready()


def test_admin_key_is_not_exposed_by_repr() -> None:
    settings = make_settings(admin_api_key="super-secret-value")

    assert "super-secret-value" not in repr(settings)
    assert settings.admin_api_key_configured is True


def test_allowed_html_domains_parsing() -> None:
    assert make_settings().allowed_html_domains_list == []

    settings = make_settings(allowed_html_domains=" Example.COM , , hnrss.org ")
    assert settings.allowed_html_domains_list == ["example.com", "hnrss.org"]


def test_user_agent_includes_version_and_optional_contact() -> None:
    assert make_settings().user_agent.startswith("TrendSentinel/")

    with_contact = make_settings(user_agent_contact="https://example.test/contact")
    assert "https://example.test/contact" in with_contact.user_agent
