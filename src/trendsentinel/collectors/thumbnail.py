"""Thumbnail extraction, shared by the adapters that carry one.

Why a shared module: the three adapters that can carry an image (Reddit ``preview``,
RSS ``<img>``, Threads ``media_url``) extract it in different shapes but must answer
the same way on the safety rules. One module, one set of rules:

* A thumbnail is **extracted, never fabricated** — a source without one keeps ``None``
  and the SPA renders its placeholder. Hacker News and X provide no image in the
  payloads this project reads; their items simply have no ``thumbnail_url``.
* **https only.** A mixed-content ``http://`` image would be blocked by browsers on an
  https deployment anyway; storing it would be storing a value the SPA cannot show.
* **No data:, blob:, or javascript: URLs** — those bypass the scheme check and can
  smuggle content past it.
* No host allowlist: the thumbnail comes from the payload the source's own collector
  already fetched and parsed, under the same SSRF rules as the rest of the run. The
  browser fetches it directly from the platform's CDN (Reddit and Threads serve
  thumbnails from dedicated media hosts); the SPA renders it with `no-referrer` so the
  deployment's own origin is not disclosed to the CDN.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

__all__ = ["THUMBNAIL_KEY", "thumbnail_from_html", "thumbnail_from_mapping"]

#: The metadata key every adapter writes the extracted image URL under.
THUMBNAIL_KEY = "thumbnail_url"

_MAX_URL_LENGTH = 2048

_IMG_SRC_RE = re.compile(
    r"""<img\b[^>]*?\bsrc\s*=\s*(?:"(?P<dq>[^"]*)"|'(?P<sq>[^']*)')""",
    re.IGNORECASE,
)


def _usable(value: Any) -> str | None:
    """Return the value as a usable thumbnail URL, or ``None``."""
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate or len(candidate) > _MAX_URL_LENGTH:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or not parsed.netloc:
        return None
    return candidate


def thumbnail_from_mapping(value: Any, _depth: int = 0) -> str | None:
    """Return the first usable https image URL inside a payload-shaped object.

    Scans mappings and sequences recursively so the exact nesting — which platforms
    shuffle between API versions — is not this function's problem. The first hit in
    depth-first order wins; payloads carry at most one meaningful preview anyway.
    """
    if _depth > 6:
        return None
    if isinstance(value, Mapping):
        for nested in value.values():
            found = thumbnail_from_mapping(nested, _depth + 1)
            if found is not None:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for nested in value:
            found = thumbnail_from_mapping(nested, _depth + 1)
            if found is not None:
                return found
        return None
    return _usable(value)


def thumbnail_from_html(html: str | None) -> str | None:
    """Return the first usable https ``<img src>`` in an HTML fragment, or ``None``."""
    if not html:
        return None
    for match in _IMG_SRC_RE.finditer(html):
        found = _usable(match.group("dq") or match.group("sq"))
        if found is not None:
            return found
    return None
