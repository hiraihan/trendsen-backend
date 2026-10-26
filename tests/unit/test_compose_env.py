"""The compose stack must forward every documented setting.

Compose does not pass the host environment into a container. Each service gets exactly the
variables named under its ``environment:`` key, so a setting that is documented in
``.env.example`` but missing from the ``x-app-env`` anchor silently keeps its in-code default
inside the containers, whatever the operator put in ``.env``.

That is not hypothetical: ``SNAPSHOT_PURGE_ENABLED=true`` reached the worker as ``false`` and
the daily purge was never registered, while the documentation said turning it on was a matter of
setting the variable. These tests make the anchor and the example file move together.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"

#: Test-only, and deliberately not forwarded into a running stack.
_NOT_FORWARDED = {"TEST_DATABASE_URL", "ENV_FILE"}

_ANCHOR = re.compile(r"^x-app-env:\s*&app-env\s*$", re.MULTILINE)
_FORWARDED = re.compile(r"^\s+(?P<key>[A-Z][A-Z0-9_]*):\s*\$\{(?P=key):-(?P<default>[^}]*)\}\s*$")
_DOCUMENTED = re.compile(r"^(?P<key>[A-Z][A-Z0-9_]*)=(?P<value>.*)$")
#: The rsshub service forwards its own variables, separate from the app anchor.
_RSSHUB_SERVICE = re.compile(r"^  rsshub:\s*$", re.MULTILINE)


def _env_block(text: str, header: re.Pattern[str]) -> dict[str, str]:
    """Return the env variables of a compose block as ``{variable: default}``."""
    match = header.search(text)
    assert match is not None
    block: list[str] = []
    for line in text[match.end() :].splitlines():
        if line and not line.startswith(" "):
            break
        block.append(line)
    return {
        found.group("key"): found.group("default")
        for found in (_FORWARDED.match(line) for line in block)
        if found is not None
    }


def _forwarded() -> dict[str, str]:
    """Return every env variable forwarded to any service, as ``{variable: default}``."""
    text = COMPOSE.read_text(encoding="utf-8")
    return {**_env_block(text, _ANCHOR), **_env_block(text, _RSSHUB_SERVICE)}


def _documented() -> dict[str, str]:
    """Return ``.env.example`` as ``{variable: value}``, comments stripped."""
    documented: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("#"):
            continue
        match = _DOCUMENTED.match(line)
        if match is None:
            continue
        documented[match.group("key")] = match.group("value").split("#", 1)[0].strip()
    return documented


def test_every_documented_setting_reaches_the_containers() -> None:
    """A variable an operator can set in ``.env`` must actually arrive at the services."""
    missing = set(_documented()) - _NOT_FORWARDED - set(_forwarded())

    assert not missing, (
        f"documented in .env.example but not forwarded by compose: {sorted(missing)}"
    )


def test_forwarded_defaults_match_the_documented_defaults() -> None:
    """An omitted variable must fall back to the value the documentation promises."""
    documented = _documented()
    drifted = {
        key: (value, documented[key])
        for key, value in _forwarded().items()
        if key in documented and value != documented[key]
    }

    assert not drifted, f"compose default differs from .env.example (compose, example): {drifted}"


def test_the_anchor_does_not_forward_the_test_database() -> None:
    """The suite's database must never follow the stack into a deployment."""
    assert "TEST_DATABASE_URL" not in _forwarded()


@pytest.mark.parametrize("key", ["SNAPSHOT_PURGE_ENABLED", "SOURCE_RELOAD_SECONDS"])
def test_the_settings_this_batch_added_are_forwarded(key: str) -> None:
    """Named explicitly, so a rename cannot pass by removing the variable altogether."""
    assert key in _forwarded()
