"""Request and response models for the credential-provisioning endpoints.

The models carry key *names* and booleans only. A credential value enters through
``SourceCredentialsRequest`` on its way to storage and never appears in any response model:
the status view reports ``bool`` per key, and the provision response reports which key names
were configured, not what was stored.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "SourceCredentialStatus",
    "SourceCredentialStatusList",
    "SourceCredentialsRequest",
    "SourceCredentialsResponse",
]


class SourceCredentialsRequest(BaseModel):
    """Body of ``POST /sources/{key}/credentials``."""

    model_config = ConfigDict(extra="forbid")

    credentials: dict[str, str] = Field(
        min_length=1,
        description=(
            "Credential keys to store, exactly as the source's adapter declares them. "
            "Unknown keys are refused; the accepted names are on the Setup screen and in "
            "docs/sources.md."
        ),
    )
    activate: bool = Field(
        default=True,
        description=(
            "Also activate the source so the worker starts collecting from it. A request "
            "without activation only stages the values (rotation without going live)."
        ),
    )

    @field_validator("credentials")
    @classmethod
    def _values_must_not_be_blank(cls, value: dict[str, str]) -> dict[str, str]:
        """A whitespace value would store a credential that is not one."""
        blank = sorted(name for name, raw in value.items() if not raw.strip())
        if blank:
            raise ValueError(f"credential values must not be blank: {', '.join(blank)}")
        return value


class SourceCredentialsResponse(BaseModel):
    """``POST /sources/{key}/credentials``: what was stored, as names only."""

    source_key: str
    configured_keys: list[str] = Field(
        description="The credential key names stored by this request, sorted."
    )
    is_active: bool = Field(description="The source's activation state after the request.")


class SourceCredentialStatus(BaseModel):
    """One source's credential state in ``GET /sources/credentials``."""

    source_key: str
    name: str
    collector_key: str
    required_keys: list[str] = Field(
        description="Every credential key the source's adapter declares, sorted."
    )
    keys: dict[str, bool] = Field(
        description=(
            "Per declared key, whether a value is stored. Never the value itself: "
            "a credential cannot be read back through the API, only replaced."
        )
    )


class SourceCredentialStatusList(BaseModel):
    """``GET /sources/credentials``."""

    sources: list[SourceCredentialStatus]
