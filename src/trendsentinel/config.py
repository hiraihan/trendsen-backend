"""Central configuration.

Every setting the application reads lives here. Modules import :func:`get_settings`
rather than touching :mod:`os.environ`, so configuration has exactly one source of truth.
"""

from __future__ import annotations

import functools
import os
from typing import Literal

from pydantic import Field, PostgresDsn, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings"]

# The environment file to load. Overridable so tests can pin a deterministic value and
# cannot accidentally pick up a developer's real `.env`.
_ENV_FILE: str | None = os.environ.get("ENV_FILE", ".env") or None


class Settings(BaseSettings):
    """Application settings, loaded from the environment and an optional ``.env`` file."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    # -- required ---------------------------------------------------------------

    database_url: PostgresDsn = Field(
        description="PostgreSQL connection string used by the API and the worker.",
    )
    admin_api_key: SecretStr = Field(
        description=(
            "Protects POST /collect/run, sent as the X-API-Key header. "
            "When blank, the trigger endpoint is disabled and fails closed."
        ),
    )

    # -- logging ----------------------------------------------------------------

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    # -- outbound HTTP ----------------------------------------------------------

    http_timeout_seconds: float = Field(default=10.0, gt=0)
    http_max_retries: int = Field(default=3, ge=0, le=10)
    max_response_bytes: int = Field(default=5_000_000, gt=0)
    max_concurrency: int = Field(default=10, ge=1, le=100)
    max_items_per_run: int = Field(default=100, ge=1, le=1000)
    max_text_chars: int = Field(default=2000, ge=1)
    user_agent_contact: str = ""

    # -- scheduler --------------------------------------------------------------

    collect_interval_minutes: int = Field(default=30, ge=1)
    pending_poll_seconds: int = Field(default=10, ge=1)
    stale_run_minutes: int = Field(default=30, ge=1)
    # How often the worker re-reads ``sources`` to pick up a source that was activated (or had
    # its ``interval_minutes`` edited) in the database. A floor of 1 second keeps a mistyped
    # value from turning the reload into a busy loop against the database.
    source_reload_seconds: int = Field(default=60, ge=1)
    run_manual_inline: bool = False

    # -- viral ranking ----------------------------------------------------------

    # Global quantile band defaults for the "medium→viral" filter. Per-source overrides
    # live in ``sources.config`` (``viral_quantile_low/high``).
    viral_quantile_low: int = Field(default=50, ge=0, le=99)
    viral_quantile_high: int = Field(default=95, ge=1, le=100)

    # -- retention --------------------------------------------------------------

    snapshot_retention_days: int = Field(default=90, ge=1)
    # Off by default: deleting collected data is an operator decision, never a side effect of
    # running the service. See the Retention section of docs/runbook.md.
    snapshot_purge_enabled: bool = False

    # -- security ---------------------------------------------------------------

    allowed_html_domains: str = ""

    # -- CORS (serves the SPA, spec v1.1 §7) --------------------------------------

    # Comma-separated origin allowlist. Empty by default: no cross-origin request is
    # allowed unless the operator lists one. Grants no extra capability — the admin key
    # on POST /collect/run is unaffected.
    cors_origins: str = ""

    # -- tests ------------------------------------------------------------------

    test_database_url: str | None = None

    # -- derived ----------------------------------------------------------------

    @computed_field  # type: ignore[prop-decorator]
    @property
    def allowed_html_domains_list(self) -> list[str]:
        """Parsed allowlist. An empty list means no HTML source may run."""
        return [d.strip().lower() for d in self.allowed_html_domains.split(",") if d.strip()]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def cors_origins_list(self) -> list[str]:
        """Parsed CORS origin allowlist. An empty list allows no cross-origin request."""
        return [o.strip().rstrip("/") for o in self.cors_origins.split(",") if o.strip()]

    @property
    def user_agent(self) -> str:
        """Descriptive User-Agent. Several feeds reject requests without one."""
        from trendsentinel import __version__

        base = f"TrendSentinel/{__version__}"
        return f"{base} (+{self.user_agent_contact})" if self.user_agent_contact else base

    @property
    def admin_api_key_configured(self) -> bool:
        """Whether the trigger endpoint can be enabled at all."""
        return bool(self.admin_api_key.get_secret_value().strip())

    # -- startup validation -----------------------------------------------------

    def assert_runtime_ready(self) -> None:
        """Fail fast on configuration the process cannot run without.

        Only genuinely mandatory configuration raises. A blank ``ADMIN_API_KEY`` is not an
        error: it disables ``POST /collect/run``, which is a valid deployment.
        """
        if not str(self.database_url).strip():
            raise RuntimeError(
                "DATABASE_URL is not configured. Copy .env.example to .env and set it."
            )


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()
