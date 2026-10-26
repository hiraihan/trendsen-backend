"""Native SQL for storing and inspecting per-source credentials.

Credentials live inside ``sources.config`` (ADR-0003) under key names the adapters declare
(``X_CREDENTIAL_KEYS``, ``TOKEN_CREDENTIAL_KEYS``, ``TOKEN_CONFIG_KEY``). The write path is a
jsonb merge - ``||`` - so an operator provisioning credentials never clobbers the benign
configuration (``limit``, ``interval_minutes``, …) already on the row.

The status read needs to know whether a key is present, not what it holds, so it selects
``jsonb_exists`` - the value never crosses into a result set it does not have to.
"""

from __future__ import annotations

import json
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from trendsentinel.db.rows import SourceRow

__all__ = ["get_configured_credential_keys", "merge_source_config"]

_COLUMNS = """
    id, key, name, integration, base_url, is_active, config, created_at, updated_at
"""


async def merge_source_config(
    conn: AsyncConnection[DictRow],
    source_key: str,
    patch: dict[str, Any],
    *,
    activate: bool | None = None,
) -> SourceRow:
    """Merge ``patch`` into ``sources.config`` and return the updated row.

    Args:
        conn: Pooled connection from the request scope; the update commits with it.
        source_key: The source to update.
        patch: The keys to set. Existing keys not named here are preserved.
        activate: When not ``None``, also set ``is_active`` to it.

    Raises:
        KeyError: No source carries ``source_key``.
    """
    set_clauses = ["config = config || %(patch)s::jsonb", "updated_at = now()"]
    params: dict[str, Any] = {"key": source_key, "patch": json.dumps(patch)}
    if activate is not None:
        set_clauses.append("is_active = %(activate)s")
        params["activate"] = activate

    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE sources
            SET {", ".join(set_clauses)}
            WHERE key = %(key)s
            RETURNING {_COLUMNS}
            """,
            params,
        )
        row = await cur.fetchone()

    if row is None:
        raise KeyError(source_key)
    return SourceRow.from_row(row)


async def get_configured_credential_keys(
    conn: AsyncConnection[DictRow], source_key: str, names: tuple[str, ...]
) -> dict[str, bool]:
    """Report which of ``names`` have a value, without reading the values."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT %(_names)s::text[] AS names,
                   ARRAY(SELECT jsonb_exists(config, name)
                         FROM unnest(%(_names)s::text[]) AS name) AS present
            FROM sources WHERE key = %(key)s
            """,
            {"key": source_key, "_names": list(names)},
        )
        row = await cur.fetchone()

    if row is None:
        raise KeyError(source_key)
    return dict(zip(row["names"], row["present"], strict=True))
