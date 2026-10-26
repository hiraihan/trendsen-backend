"""Structured logging.

Both the API and the worker call :func:`configure_logging` once at startup. Logs go to
stdout only; nothing here decides where they are shipped.

Secret values are scrubbed as each record is created, so no handler -- ours, a host's, or one
attached to a descendant logger -- ever sees an unredacted record. The rendered line is
scrubbed once more as a safety net.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from datetime import UTC, datetime
from typing import Any

from trendsentinel.config import Settings

__all__ = ["configure_logging", "get_logger", "redact", "remove_owned_filters"]

REDACTED = "***REDACTED***"

# Marks the handler this module installs, so reconfiguration removes only our own handler
# and never a handler installed by an embedding host (pytest, a supervisor, a notebook).
_OWNED_HANDLER_ATTR = "_trendsentinel_owned"

# Marks the redacting filter this module attaches, so it can be detached again. A filter left
# on a host handler (pytest's capture handler, a supervisor's) would keep scrubbing strings with
# a key that no longer applies.
_OWNED_FILTER_ATTR = "_trendsentinel_owned_filter"

# Attributes present on every LogRecord; anything else is a caller-supplied `extra`.
_RESERVED_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


def redact(text: str, secrets: Iterable[str]) -> str:
    """Replace every occurrence of a known secret in ``text``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def _secret_values(settings: Settings) -> tuple[str, ...]:
    """Collect raw secret strings that must never appear in a log line."""
    candidates = [settings.admin_api_key.get_secret_value()]
    return tuple(value for value in candidates if value and value.strip())


class RedactingFilter(logging.Filter):
    """Scrubs secrets from a record's message, arguments and extra fields."""

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self._secrets: tuple[str, ...] = tuple(s for s in secrets if s)

    def _scrub_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return redact(value, self._secrets)
        if isinstance(value, Mapping):
            return {key: self._scrub_value(item) for key, item in value.items()}
        if isinstance(value, list | tuple):
            return type(value)(self._scrub_value(item) for item in value)
        return value

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True

        if isinstance(record.msg, str):
            record.msg = redact(record.msg, self._secrets)

        if record.args:
            if isinstance(record.args, Mapping):
                record.args = self._scrub_value(dict(record.args))
            else:
                record.args = tuple(self._scrub_value(arg) for arg in record.args)

        fields = cast_mutable(record.__dict__)
        for key, value in fields.items():
            if key in _RESERVED_ATTRS:
                continue
            fields[key] = self._scrub_value(value)

        if record.exc_text:
            record.exc_text = redact(record.exc_text, self._secrets)

        return True


def cast_mutable(mapping: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """Narrow a mapping's type for in-place mutation of a record's ``__dict__``."""
    return mapping  # type: ignore[return-value]


class JsonFormatter(logging.Formatter):
    """One JSON object per line."""

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extra = {key: value for key, value in record.__dict__.items() if key not in _RESERVED_ATTRS}
        if extra:
            payload["extra"] = extra

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        rendered = json.dumps(payload, default=str, ensure_ascii=False)
        return redact(rendered, self._secrets)


class ConsoleFormatter(logging.Formatter):
    """Human-readable output for local development."""

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__(fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        self._secrets = tuple(s for s in secrets if s)

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)

        extra = {key: value for key, value in record.__dict__.items() if key not in _RESERVED_ATTRS}
        if extra:
            rendered = f"{rendered} {extra}"

        return redact(rendered, self._secrets)


# The currently installed record factory, and the factory that was installed before ours.
# Kept so reconfiguration replaces our wrapper instead of stacking another one on top, and so
# a host that installed its own factory keeps it. (``logging.LogRecordFactory`` exists only as
# a type alias in typeshed, not at runtime, so the callable shape is spelled out here.)
_installed_factory: Callable[..., logging.LogRecord] | None = None
_previous_factory: Callable[..., logging.LogRecord] | None = None


def _add_redacting_filter_last(handler: logging.Handler, secrets: tuple[str, ...]) -> None:
    """Ensure ``handler`` scrubs records, without ever stacking a second redactor.

    The installed filter is tagged so a later call can find and remove it. Without that tag a
    host handler (pytest's capture handler, a supervisor's) keeps our redactor forever, and a
    later test or process would find its own strings silently masked by a key that is long gone.
    """
    for log_filter in list(handler.filters):
        if isinstance(log_filter, RedactingFilter):
            handler.removeFilter(log_filter)
    log_filter = RedactingFilter(secrets)
    setattr(log_filter, _OWNED_FILTER_ATTR, True)
    handler.addFilter(log_filter)


def remove_owned_filters(handler: logging.Handler) -> None:
    """Detach every redactor this module installed on ``handler``."""
    for log_filter in list(handler.filters):
        if getattr(log_filter, _OWNED_FILTER_ATTR, False):
            handler.removeFilter(log_filter)


def _install_record_factory(secrets: tuple[str, ...]) -> None:
    """Scrub every record as it is created, whatever logger emitted it.

    ``Logger.callHandlers`` never consults an ancestor's filters, so a filter on the root logger
    leaves records emitted by descendant loggers untouched; and a handler attached to a
    descendant runs before the record ever reaches the root. Installing a log-record factory --
    the hook every ``LogRecord`` is built through, whatever logger or ``dictConfig`` emitted it
    -- is the only seam that scrubs the record before *any* handler can see it.
    """
    global _installed_factory, _previous_factory

    if logging.getLogRecordFactory() is not _installed_factory:
        _previous_factory = logging.getLogRecordFactory()
    previous = _previous_factory
    redacting = RedactingFilter(secrets)

    def record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        if previous is None:
            record = logging.LogRecord(*args, **kwargs)
        else:
            record = previous(*args, **kwargs)
        redacting.filter(record)
        # The traceback is the one field a filter cannot reach: ``Handler.handle`` filters
        # before formatting, so the first formatter would memoize a raw ``exc_text``. Rendering
        # it here, once and scrubbed, means every handler -- whichever order they were added in
        # -- sees redacted text and reuses it instead of re-rendering ``exc_info``.
        if record.exc_info is not None:
            record.exc_text = redact(
                "".join(traceback.format_exception(*record.exc_info)), secrets
            )
        return record

    _installed_factory = record_factory
    logging.setLogRecordFactory(record_factory)


def configure_logging(settings: Settings) -> None:
    """Install the root logging handler. Idempotent.

    Only a handler installed by a previous call to this function is replaced, so hosts that
    attach their own handlers (pytest's ``caplog``, a process supervisor) keep working.

    Redaction happens in two layers:

    * a log-record factory scrubs every record as it is created. This is the seam that makes
      the guarantee true: ``Logger.callHandlers`` never consults an ancestor's filters, so a
      filter on the root logger would leave records from ``trendsentinel.*`` loggers untouched,
      and a handler attached to a descendant logger would see the record first regardless;
    * every root handler gets the filter too, as defence in depth, so our own handler stays
      correct for any record that did not come through the factory.

    Both are idempotent: the factory replaces the previous wrapper instead of stacking, and the
    handler filter replaces any filter this module already put there.
    """
    secrets = _secret_values(settings)

    _install_record_factory(secrets)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter(secrets) if settings.log_format == "json" else ConsoleFormatter(secrets)
    )
    setattr(handler, _OWNED_HANDLER_ATTR, True)
    # Defence in depth: the record factory has already scrubbed this record. The filter keeps
    # our own handler correct even if a record reaches it from somewhere the factory did not.
    _add_redacting_filter_last(handler, secrets)

    root = logging.getLogger()
    for handler_to_remove in list(root.handlers):
        if getattr(handler_to_remove, _OWNED_HANDLER_ATTR, False):
            root.removeHandler(handler_to_remove)
            handler_to_remove.filters.clear()

    # A host handler added *before* us would otherwise render records unscrubbed. Detach any
    # filter we installed on it at a previous call first, so a reconfiguration does not leave
    # a stale redactor behind.
    for host_handler in root.handlers:
        remove_owned_filters(host_handler)
        _add_redacting_filter_last(host_handler, secrets)

    root.addHandler(handler)
    root.setLevel(settings.log_level)

    # uvicorn installs its own handlers; let the root configuration win so the output
    # format is consistent across the API and the worker.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True


def get_logger(name: str) -> logging.Logger:
    """Return the logger for ``name``. Every module logs through this."""
    return logging.getLogger(name)
