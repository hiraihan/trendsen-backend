"""Explicit collector registration.

Importing this module is what makes the registered collectors available. It is done in one
obvious place rather than as a side effect of importing the registry, so a linter or a test
can tell exactly which adapters exist.

To add a source: write the module, add one import below, seed a ``sources`` row. No existing
collector changes.
"""

from __future__ import annotations

from trendsentinel.collectors import (
    hackernews,
    public_api,
    reddit,
    rss,
    threads,
    wikimedia,
    x,
)

__all__ = ["hackernews", "public_api", "reddit", "rss", "threads", "wikimedia", "x"]
