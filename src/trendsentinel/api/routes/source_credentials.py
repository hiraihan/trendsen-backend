"""``GET /sources/credentials`` and ``POST /sources/{key}/credentials``: web-based setup.

Why this exists: the multi-source batch (T22-T24) made collection from Reddit, X and Threads
possible, but provisioning their credentials was SQL-only (the ``sources.md`` runbook). This
router is the same operation as that runbook, expressed as an authenticated endpoint so the
operator can do it from the SPA's Setup screen - no shell, no shell history, no psql.

Security posture, in the order the security review (T26) would ask:

* **Write auth** - the provision endpoint depends on the same fail-closed ``AdminKey`` as
  ``POST /collect/run``. No key configured means every request is rejected.
* **Allowlist** - only key names declared by an adapter (``*_CREDENTIAL_KEYS`` /
  ``TOKEN_CONFIG_KEY``) are accepted, and each only for the source whose adapter declares it.
  The endpoint is not a free-form config write; an unknown name is a 422, and a valid name
  aimed at the wrong source is a 422 too.
* **Status auth** - ``GET /sources/credentials`` requires the same admin key (ADR-0006 as
  amended: the response is booleans, but the readiness map itself is operational
  information — and requiring the key here is what makes the SPA's unlock an honest
  *verification* before the first write).
* **No readback** - credentials are write-only over HTTP. The status endpoint answers
  "is this key set?" with a boolean; neither endpoint ever returns a stored value, and the
  ``sanitize_config`` scrubber (ADR-0005) keeps them out of ``GET /sources`` regardless.
* **No logging** - request bodies here are never logged; the API logs route names and
  statuses, not payloads.

This module is API-layer. It imports the adapters' credential-key constants (the same
constants the T26 leak review reads), but never builds a collector, opens a fetcher, or
touches the network: collectors stay on the collection path.
"""

from __future__ import annotations

from fastapi import APIRouter

from trendsentinel.api.deps import AdminKey, DbSession
from trendsentinel.api.errors import ApiError, ErrorCode
from trendsentinel.collectors.reddit import TOKEN_CREDENTIAL_KEYS
from trendsentinel.collectors.registry import resolve_key
from trendsentinel.collectors.threads import TOKEN_CONFIG_KEY
from trendsentinel.collectors.x import X_CREDENTIAL_KEYS
from trendsentinel.db.repositories import source_credentials as credentials_repo
from trendsentinel.db.repositories import sources as sources_repo
from trendsentinel.schemas.source_credentials import (
    SourceCredentialsRequest,
    SourceCredentialsResponse,
    SourceCredentialStatus,
    SourceCredentialStatusList,
)

__all__ = ["router"]

router = APIRouter(tags=["sources"])


def _credential_keys_for(collector_key: str) -> tuple[str, ...]:
    """Return the credential keys the named adapter declares, or ``()``.

    Read from the adapter modules at call time - the same constants the T26 leak review
    imports - so a new credential key cannot be provisioned before its adapter and its
    review exist.
    """
    declared: dict[str, tuple[str, ...]] = {
        "x": X_CREDENTIAL_KEYS,
        "reddit": TOKEN_CREDENTIAL_KEYS,
        "threads": (TOKEN_CONFIG_KEY,),
    }
    return declared.get(collector_key, ())


@router.get(
    "/sources/credentials",
    response_model=SourceCredentialStatusList,
    summary="Per-source credential status (X-API-Key required; booleans, never values)",
    responses={401: {"description": "Missing or invalid X-API-Key"}},
)
async def credential_status(conn: DbSession, _admin: AdminKey) -> SourceCredentialStatusList:
    """Report, for every credentialed source, which declared keys have values.

    The response is booleans by construction, so it cannot leak a credential value —
    but the readiness map is operational information, so the endpoint sits behind the
    same admin key as the write it precedes (ADR-0006, amended). That also gives the
    Setup screen a real key check before its first save.
    """
    rows = await sources_repo.list_sources(conn)
    sources: list[SourceCredentialStatus] = []
    for row in rows:
        collector_key = resolve_key(row.integration, row.config)
        declared = _credential_keys_for(collector_key)
        if not declared:
            continue

        present = await credentials_repo.get_configured_credential_keys(conn, row.key, declared)
        sources.append(
            SourceCredentialStatus(
                source_key=row.key,
                name=row.name,
                collector_key=collector_key,
                required_keys=sorted(declared),
                keys={name: bool(present.get(name)) for name in sorted(declared)},
            )
        )

    return SourceCredentialStatusList(sources=sources)


@router.post(
    "/sources/{source_key}/credentials",
    response_model=SourceCredentialsResponse,
    summary="Store credentials for one source (requires X-API-Key); optionally activate it",
    responses={
        401: {"description": "Missing or invalid X-API-Key"},
        404: {"description": "Unknown source key"},
        422: {"description": "A key is not declared by this source's adapter, or a value is blank"},
    },
)
async def provision_credentials(
    source_key: str,
    body: SourceCredentialsRequest,
    conn: DbSession,
    _admin: AdminKey,
) -> SourceCredentialsResponse:
    """Store the given credential keys on the source's config; optionally activate it.

    The source's own adapter decides which keys are legal here: a key is accepted only when
    that adapter declares it, so a value pasted into the wrong form cannot silently arm the
    wrong collector.
    """
    source = await sources_repo.get_source_by_key(conn, source_key)
    if source is None:
        raise ApiError(
            ErrorCode.NOT_FOUND,
            f"Unknown source {source_key!r}",
            status_code=404,
        )

    collector_key = resolve_key(source.integration, source.config)
    declared = _credential_keys_for(collector_key)
    unknown = sorted(set(body.credentials) - set(declared))
    if unknown:
        raise ApiError(
            ErrorCode.VALIDATION_ERROR,
            (f"Source {source.key!r} accepts credential keys {sorted(declared)}; got {unknown}."),
            status_code=422,
            details={"unknown_keys": unknown, "accepted_keys": sorted(declared)},
        )

    updated = await credentials_repo.merge_source_config(
        conn, source.key, dict(body.credentials), activate=body.activate
    )
    return SourceCredentialsResponse(
        source_key=updated.key,
        configured_keys=sorted(body.credentials),
        is_active=updated.is_active,
    )
