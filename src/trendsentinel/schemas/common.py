"""Shared response envelopes.

Every successful list response and every error response has one stable shape, so a client
writes one parser. These models are the API's published contract; changing a field name is a
breaking change.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["ErrorBody", "ErrorResponse", "MessageResponse", "PageMeta"]


class ErrorBody(BaseModel):
    """The error detail inside :class:`ErrorResponse`."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(description="Stable, machine-readable error code. Branch on this.")
    message: str = Field(description="Human-readable summary. Safe to display.")
    details: Any = Field(
        default=None,
        description="Optional structured context, such as per-field validation failures.",
    )


class ErrorResponse(BaseModel):
    """Every non-2xx response body, without exception."""

    error: ErrorBody


class MessageResponse(BaseModel):
    """A short acknowledgement."""

    message: str


class PageMeta(BaseModel):
    """Pagination position for a keyset-paginated list."""

    next_cursor: str | None = Field(
        default=None,
        description=(
            "Opaque token for the next page. Pass it back as `cursor`. Null means this is "
            "the last page. Do not construct or modify it."
        ),
    )
