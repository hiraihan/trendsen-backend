"""Test helpers shared across the suite.

Settings are always built from explicit values so a test never depends on a developer's
environment or on a ``.env`` file.
"""

from __future__ import annotations

from trendsentinel.config import Settings

#: Values used to construct a valid, complete configuration in tests.
BASE_ENV: dict[str, str] = {
    "database_url": "postgresql://trendsentinel:trendsentinel@localhost:5432/trendsentinel_test",
    "admin_api_key": "test-admin-key",
}


def build_settings(**overrides: object) -> Settings:
    """Build a ``Settings`` instance from explicit values, ignoring any env file."""
    return Settings(_env_file=None, **{**BASE_ENV, **overrides})  # type: ignore[arg-type]
