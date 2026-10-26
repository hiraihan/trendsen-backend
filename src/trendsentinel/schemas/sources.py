"""Response models for source collections.

`SourceResponse.config` is deliberately not the raw database value: it is passed through
:func:`sanitize_config` (defined here) before it leaves the process.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "SourceListResponse",
    "SourceResponse",
    "SourceUpdateRequest",
    "sanitize_config",
]

#: Substrings that mark a config key as credential-like. Matched case-insensitively. The
#: two OAuth identifiers were added by ADR-0005 (`docs/adr/0005-scrubbing-correction.md`),
#: which supersedes part of ADR-0003: a client id is half of the Reddit Basic-auth pair, and
#: the Reddit user agent is issued per app, so a rotated one identifies the app as much as a
#: token does.
_SECRET_MARKERS = (
    "key",
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "auth",
    "cookie",
    "session",
    "bearer",
    "signature",
    "private",
    "cert",
    "client_id",
    "user_agent",
)

#: Substrings for values that could hide an entire credential blob under an innocuous name.
_REDACTED = "***REDACTED***"


def sanitize_config(config: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return a copy of a source config with credential-like keys removed.

    The check is by key name, both because the schema stores ``jsonb`` (so a value could be
    anything) and because a name is the only signal available before decoding. Values under a
    suspicious name are dropped entirely rather than replaced: returning a redaction marker
    would still confirm the key exists.
    """
    if config is None:
        return None

    return {
        name: value
        for name, value in config.items()
        if not _looks_secret(name)
    }


def _looks_secret(name: str) -> bool:
    """Return whether a config key name suggests a credential."""
    lowered = name.lower()
    return any(marker in lowered for marker in _SECRET_MARKERS)


class SourceResponse(BaseModel):
    """One configured source.

    ``config`` is the sanitized view, never the stored value.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    key: str = Field(description="Stable identifier used by `source=` and `source_key`.")
    name: str
    integration: str = Field(description="Collector family: `api`, `rss` or `html`.")
    base_url: str | None
    is_active: bool
    config: dict[str, Any] = Field(
        description="Non-sensitive source configuration. Credential-like keys are removed."
    )
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SourceListResponse(BaseModel):
    """``GET /sources``."""

    sources: list[SourceResponse]


class SourceUpdateRequest(BaseModel):
    """``PATCH /sources/{key}``: the collection flag to set."""

    is_active: bool
