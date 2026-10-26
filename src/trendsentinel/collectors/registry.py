"""Collector registry.

A source is routed to a collector by ``config.collector_key`` when present, otherwise by its
``integration`` type. Adding a source is therefore a two-file change: a new module in this
package and a row in ``sources``. No existing collector is touched.

Factories receive the process settings, because every collector needs the HTTP timeout,
concurrency bound, size cap and User-Agent, and none of them should reach for a global.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from trendsentinel.collectors.base import UnknownCollectorError
from trendsentinel.config import Settings

if TYPE_CHECKING:
    from trendsentinel.collectors.base import Collector
    from trendsentinel.collectors.http import HttpFetcher

__all__ = [
    "available_keys",
    "get_collector",
    "register",
    "resolve_key",
]

#: A collector factory: given the process settings and a fetcher to use, returns a collector.
#: The fetcher is injected rather than created inside the collector so that one bounded HTTP
#: client is shared across a whole collection run, and so collectors are testable without DNS.
Factory = Callable[[Settings, "HttpFetcher"], "Collector"]

_FACTORIES: dict[str, Factory] = {}


def register(key: str) -> Callable[[Factory], Factory]:
    """Register a collector factory under ``key``.

    ``key`` is a ``collector_key`` value from a source's config, or an ``integration`` type.
    """
    if not key or not key.strip():
        raise ValueError("collector key must not be empty")

    def decorator(factory: Factory) -> Factory:
        if key in _FACTORIES:
            raise ValueError(f"collector {key!r} is already registered")
        _FACTORIES[key] = factory
        return factory

    return decorator


def available_keys() -> frozenset[str]:
    """Return the registered collector keys."""
    return frozenset(_FACTORIES)


def resolve_key(integration: str, config: Mapping[str, object] | None = None) -> str:
    """Return the collector key for a source.

    An explicit ``collector_key`` wins, so several sources sharing an integration can use
    different adapters; otherwise the integration type is used.
    """
    configured = (config or {}).get("collector_key")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()

    return integration


def get_collector(
    settings: Settings,
    fetcher: HttpFetcher,
    integration: str,
    config: Mapping[str, object] | None = None,
) -> Collector:
    """Build the collector for a source.

    Args:
        settings: Process settings, for limits and concurrency bounds.
        fetcher: The run's shared, bounded HTTP fetcher.
        integration: The source's ``integration`` value.
        config: The source's ``config``, used to resolve a ``collector_key``.

    Raises:
        UnknownCollectorError: Nothing is registered for the resolved key. The caller records
            this as a failed run rather than letting it abort a whole batch.
    """
    key = resolve_key(integration, config)
    factory = _FACTORIES.get(key)
    if factory is None:
        raise UnknownCollectorError(key)

    return factory(settings, fetcher)
