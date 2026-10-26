"""Shared test configuration.

Guarantees the suite never depends on a developer's machine state: ``ENV_FILE`` is pinned to
a non-existent path before ``trendsentinel.config`` is imported, so a real ``.env`` cannot
leak values into a test run.
"""

from __future__ import annotations

import logging
import os

# Must happen before the first import of trendsentinel.config, which reads ENV_FILE at
# module import time to configure pydantic-settings.
os.environ.setdefault("ENV_FILE", "")

from collections.abc import Iterator

import pytest

from tests.helpers import build_settings
from trendsentinel import logging as trendsentinel_logging
from trendsentinel.config import Settings


@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    """Undo ``configure_logging``'s process-wide side effects after every test.

    ``configure_logging`` installs a log-record factory and a root handler. Both are global, so
    a test that builds an app (or runs the worker) would otherwise leave its own admin key
    scrubbing records for every later test — and the reverse, a leaked factory that redacts
    strings a later test was asserting on.

    This lives in ``conftest.py`` rather than next to the logging tests for that reason: the
    leak is caused by *any* test that configures logging, not only by the ones about logging.
    """
    yield

    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, trendsentinel_logging._OWNED_HANDLER_ATTR, False):
            root.removeHandler(handler)
        trendsentinel_logging.remove_owned_filters(handler)

    if logging.getLogRecordFactory() is trendsentinel_logging._installed_factory:
        logging.setLogRecordFactory(logging.LogRecord)


@pytest.fixture
def settings() -> Settings:
    """A complete, valid settings object with no file or environment input."""
    return build_settings()
