"""Tests for structured logging and secret redaction.

Redaction is asserted against captured **stdout**, not against pytest's ``caplog``: the
handler installed by :func:`configure_logging` is what ships to production, and pytest
attaches capture handlers of its own that no application code controls. Testing stdout
therefore exercises the real path an operator's log collector would see.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any

import pytest

from tests.helpers import build_settings
from trendsentinel.config import Settings
from trendsentinel.logging import (
    _OWNED_HANDLER_ATTR,
    REDACTED,
    RedactingFilter,
    configure_logging,
    get_logger,
    redact,
    remove_owned_filters,
)

SECRET = "super-secret-key-value"

SOURCE = "hackernews"


def make_settings(**overrides: object) -> Settings:
    return build_settings(admin_api_key=SECRET, **overrides)


def json_lines(out: str) -> list[dict[str, Any]]:
    """Parse the JSON log lines from captured stdout."""
    return [json.loads(line) for line in out.splitlines() if line.strip().startswith("{")]


def make_record(
    logger: logging.Logger,
    msg: object,
    args: tuple[Any, ...] | dict[str, Any] = (),
    *,
    level: int = logging.INFO,
    extra: dict[str, Any] | None = None,
) -> logging.LogRecord:
    """Build a record without the redaction :func:`configure_logging` installs.

    ``logger.makeRecord`` routes through the global log-record factory, which already scrubs.
    These tests exercise ``RedactingFilter`` itself, so the record must arrive unscrubbed.
    ``extra`` is applied the way ``Logger.makeRecord`` applies it, since ``LogRecord.__init__``
    does not do so.
    """
    record = logging.LogRecord(
        logger.name,
        level,
        __file__,
        1,
        msg,
        args,
        None,
        None,
        None,
    )
    for key, value in (extra or {}).items():
        record.__dict__[key] = value
    return record


def current_filter(logger: logging.Logger) -> RedactingFilter:
    """The single redacting filter installed on ``logger`` by these tests."""
    filters = [f for f in logger.filters if isinstance(f, RedactingFilter)]
    assert len(filters) == 1
    return filters[0]


def test_json_format_is_one_object_per_line(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="json"))

    get_logger("test").info("collected", extra={"source": SOURCE, "n": 3})

    payloads = json_lines(capsys.readouterr().out)
    assert len(payloads) == 1

    payload = payloads[0]
    assert payload["message"] == "collected"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "test"
    assert payload["extra"]["source"] == SOURCE
    assert payload["extra"]["n"] == 3
    assert "time" in payload


def test_console_format_is_human_readable(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="console"))

    get_logger("test").warning("something", extra={"source": SOURCE})

    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "something" in out
    assert SOURCE in out
    assert not json_lines(out)


def test_redact_replaces_all_occurrences() -> None:
    assert redact(f"key={SECRET} again {SECRET}", [SECRET]) == f"key={REDACTED} again {REDACTED}"
    assert redact("nothing to do", []) == "nothing to do"


@pytest.mark.parametrize("log_format", ["json", "console"])
def test_secret_in_message_is_redacted(log_format: str, capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format=log_format))

    get_logger("test").info("using key %s", SECRET)

    out = capsys.readouterr().out
    assert SECRET not in out
    assert REDACTED in out


@pytest.mark.parametrize("log_format", ["json", "console"])
def test_secret_in_extra_field_is_redacted(
    log_format: str, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(make_settings(log_format=log_format))

    get_logger("test").info("auth", extra={"header": f"X-API-Key: {SECRET}"})

    out = capsys.readouterr().out
    assert SECRET not in out
    assert REDACTED in out


@pytest.mark.parametrize("log_format", ["json", "console"])
def test_secret_in_exception_traceback_is_redacted(
    log_format: str, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(make_settings(log_format=log_format))

    def raise_with_secret() -> None:
        raise ValueError(f"bad credentials: {SECRET}")

    try:
        raise_with_secret()
    except ValueError:
        get_logger("test").exception("request failed")

    out = capsys.readouterr().out
    assert SECRET not in out
    assert "request failed" in out
    assert "bad credentials" in out


def test_nested_structures_are_scrubbed(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="json"))

    get_logger("test").info(
        "nested",
        extra={"payload": {"headers": {"X-API-Key": SECRET}, "items": [SECRET]}},
    )

    out = capsys.readouterr().out
    assert SECRET not in out

    payload = json_lines(out)[0]
    assert payload["extra"]["payload"]["headers"]["X-API-Key"] == REDACTED
    assert payload["extra"]["payload"]["items"] == [REDACTED]


def test_configure_logging_replaces_only_its_own_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(make_settings(log_format="json"))
    configure_logging(make_settings(log_format="json"))
    configure_logging(make_settings(log_format="json"))

    owned = [
        handler
        for handler in logging.getLogger().handlers
        if getattr(handler, _OWNED_HANDLER_ATTR, False)
    ]
    assert len(owned) == 1


def test_reconfiguration_does_not_duplicate_output(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="json"))
    configure_logging(make_settings(log_format="json"))

    get_logger("test").info("once")

    out = capsys.readouterr().out
    assert out.count("once") == 1


def test_log_level_is_applied(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="json", log_level="WARNING"))

    get_logger("test").info("suppressed")
    get_logger("test").warning("emitted")

    out = capsys.readouterr().out
    assert "suppressed" not in out
    assert "emitted" in out


def test_uvicorn_loggers_propagate_to_root() -> None:
    configure_logging(make_settings(log_format="json"))

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        assert logger.handlers == []
        assert logger.propagate is True


def test_secret_from_child_logger_never_reaches_a_host_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A record must be scrubbed before *any* handler sees it, not just ours.

    pytest attaches its own capture handlers to the root logger, and a filter on the root
    logger is not consulted for records emitted by descendant loggers. Those handlers are
    detached here so the assertion is about our record, not about pytest's plumbing.
    """
    configure_logging(make_settings(log_format="json"))

    root = logging.getLogger()
    foreign_handlers = [h for h in root.handlers if not getattr(h, _OWNED_HANDLER_ATTR, False)]
    for handler in foreign_handlers:
        root.removeHandler(handler)

    # A plain handler, other than ours, writing to a sink we can read.
    host_output = io.StringIO()
    host_handler = logging.StreamHandler(host_output)
    root.addHandler(host_handler)
    try:
        get_logger("trendsentinel.worker").warning("key %s", SECRET)
    finally:
        root.removeHandler(host_handler)
        for handler in foreign_handlers:
            root.addHandler(handler)

    assert SECRET not in host_output.getvalue()
    assert REDACTED in host_output.getvalue()

    ours_output = capsys.readouterr().out
    assert SECRET not in ours_output
    assert REDACTED in ours_output


@pytest.mark.parametrize("log_format", ["json", "console"])
def test_exception_from_child_logger_never_reaches_a_host_handler(
    log_format: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The traceback caching on ``record.exc_text`` must hold the scrubbed text, not the raw one."""
    configure_logging(make_settings(log_format=log_format))

    class RecordingHandler(logging.StreamHandler[io.StringIO]):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.seen: list[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.seen.append(record)
            super().emit(record)

    root = logging.getLogger()
    added_after = RecordingHandler(io.StringIO())
    added_before = RecordingHandler(io.StringIO())
    root.addHandler(added_after)
    root.handlers.insert(0, added_before)
    try:
        try:
            raise ValueError(f"bad credentials: {SECRET}")
        except ValueError:
            get_logger("trendsentinel.worker").exception("request failed")
        after_output = added_after.stream.getvalue()
        before_output = added_before.stream.getvalue()
    finally:
        root.removeHandler(added_after)
        root.removeHandler(added_before)

    assert SECRET not in before_output
    assert SECRET not in after_output
    assert added_after.seen[0].exc_text is not None
    assert SECRET not in added_after.seen[0].exc_text
    assert "bad credentials" in added_after.seen[0].exc_text

    ours_output = capsys.readouterr().out
    assert SECRET not in ours_output
    assert "bad credentials" in ours_output


def scrub(
    msg: object,
    args: tuple[Any, ...] | dict[str, Any] = (),
    *,
    secrets: tuple[str, ...] = (SECRET,),
    level: int = logging.INFO,
    extra: dict[str, Any] | None = None,
) -> logging.LogRecord:
    """Run a freshly built record through ``RedactingFilter`` at its public seam."""
    logger = logging.getLogger("redaction.scrub")
    logger.addFilter(RedactingFilter(secrets))
    try:
        record = make_record(logger, msg, args, level=level, extra=extra)
        assert current_filter(logger).filter(record) is True
    finally:
        logger.filters.clear()
    return record


def test_filter_with_no_secrets_leaves_the_record_untouched() -> None:
    record = scrub("key %s", (SECRET,), secrets=(), extra={"header": f"X-API-Key: {SECRET}"})

    assert record.msg == "key %s"
    assert record.args == (SECRET,)
    assert record.__dict__["header"] == f"X-API-Key: {SECRET}"
    assert SECRET in record.getMessage()


def test_filter_scrubs_the_message_template_and_tuple_arguments() -> None:
    record = scrub("key %s", (SECRET,))

    assert record.msg == "key %s"
    assert record.args == (REDACTED,)
    assert record.getMessage() == f"key {REDACTED}"


def test_filter_scrubs_a_message_template_with_no_arguments() -> None:
    record = scrub(f"key={SECRET}")

    assert record.msg == f"key={REDACTED}"


def test_filter_scrubs_mapping_arguments() -> None:
    record = scrub("key %(key)s", {"key": SECRET, "count": 2})

    assert record.args == {"key": REDACTED, "count": 2}
    assert record.getMessage() == f"key {REDACTED}"


def test_filter_scrubs_extra_fields_recursively_keeping_container_types() -> None:
    record = scrub(
        "nested",
        extra={"payload": {"list": ["a", SECRET], "tuple": ("b", SECRET)}},
    )

    payload = record.__dict__["payload"]
    assert payload == {"list": ["a", REDACTED], "tuple": ("b", REDACTED)}
    # The concrete container types survive the rebuild.
    assert isinstance(payload["list"], list)
    assert isinstance(payload["tuple"], tuple)


def test_filter_leaves_non_string_values_unchanged() -> None:
    record = scrub(
        "scalars",
        extra={"count": 7, "flag": True, "missing": None, "ratio": 0.5},
    )

    assert record.__dict__["count"] == 7
    assert record.__dict__["flag"] is True
    assert record.__dict__["missing"] is None
    assert record.__dict__["ratio"] == 0.5


def test_filter_scrubs_cached_exception_text() -> None:
    record = make_record(logging.getLogger("redaction.exc"), "failed", level=logging.ERROR)
    record.exc_text = f"Traceback ...\nValueError: bad credentials: {SECRET}"

    assert RedactingFilter([SECRET]).filter(record) is True

    assert record.exc_text == f"Traceback ...\nValueError: bad credentials: {REDACTED}"


def test_filter_never_drops_a_record() -> None:
    redacting = RedactingFilter([SECRET])
    logger = logging.getLogger("redaction.never_drops")
    records = [
        make_record(logger, "plain"),
        make_record(logger, SECRET),
        # A non-string message object is left alone but still allowed through.
        make_record(logger, {"k": SECRET}),
    ]

    assert [redacting.filter(record) for record in records] == [True, True, True]
    assert records[0].msg == "plain"
    assert records[1].msg == REDACTED
    assert records[2].msg == {"k": SECRET}


def test_reconfiguration_does_not_stack_redacting_filters(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(make_settings(log_format="json"))
    configure_logging(make_settings(log_format="json"))

    owned = [
        handler
        for handler in logging.getLogger().handlers
        if getattr(handler, _OWNED_HANDLER_ATTR, False)
    ]
    assert len(owned) == 1


def test_a_host_handler_renders_scrubbed_output() -> None:
    """A handler the host owns renders scrubbed output, because it is filtered too.

    Tagging matters here: this filter is installed on a handler this module does not own, so
    it must be removable by :func:`remove_owned_filters` — otherwise a host (pytest's capture
    handler, a supervisor) keeps scrubbing with a key that is long gone.
    """
    host_stream = io.StringIO()
    host = logging.StreamHandler(host_stream)
    host.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.addHandler(host)
    try:
        configure_logging(make_settings())
        configure_logging(make_settings())

        logging.getLogger("trendsentinel.worker").warning("token=%s", SECRET)

        assert SECRET not in host_stream.getvalue()
        assert REDACTED in host_stream.getvalue()
        assert len([f for f in host.filters if isinstance(f, RedactingFilter)]) == 1

        remove_owned_filters(host)
        assert not [f for f in host.filters if isinstance(f, RedactingFilter)]
    finally:
        root.removeHandler(host)


def test_a_host_handler_on_root_is_scrubbed_too(capsys: pytest.CaptureFixture[str]) -> None:
    """A handler installed before ``configure_logging`` must not see the raw record either."""
    host_handler = logging.StreamHandler(io.StringIO())
    root = logging.getLogger()
    root.addHandler(host_handler)
    try:
        configure_logging(make_settings(log_format="json"))
        get_logger("trendsentinel.worker").warning("key %s", SECRET)
        host_output = host_handler.stream.getvalue()
    finally:
        root.removeHandler(host_handler)

    assert SECRET not in host_output
    assert REDACTED in host_output


def test_stack_info_is_rendered_and_keeps_the_secret_out_of_the_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(make_settings(log_format="json"))

    get_logger("test").warning("with stack", stack_info=True)

    out = capsys.readouterr().out
    assert SECRET not in out
    payload = json_lines(out)[0]
    assert payload["message"] == "with stack"
    assert "Stack (most recent call last)" in payload["stack"]


def test_secret_never_reaches_a_handler_attached_to_a_descendant_logger(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A handler on a child logger runs before any root handler, and root filters never see it.

    Only scrubbing at record creation covers this case; a handler filter cannot, because the
    child's handler emits before the record reaches the root at all.
    """
    configure_logging(make_settings(log_format="json"))

    worker = get_logger("trendsentinel.worker")
    sink = io.StringIO()
    child_handler = logging.StreamHandler(sink)
    worker.addHandler(child_handler)
    try:
        worker.warning("key %s", SECRET)
    finally:
        worker.removeHandler(child_handler)

    assert SECRET not in sink.getvalue()
    assert REDACTED in sink.getvalue()
