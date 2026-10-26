"""The dependency direction between layers is a rule, so it is tested.

`docs/architecture.md` states that dependencies point strictly downward:

    api -> services -> db
    collectors -> (http, ssrf, base only)

A violation is otherwise invisible until it becomes a circular import or an untestable
module, so it is asserted here by inspecting the source text rather than by convention.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "trendsentinel"

#: For each layer, the module prefixes it may not import.
FORBIDDEN_IMPORTS: dict[str, tuple[str, ...]] = {
    "collectors": ("trendsentinel.db", "trendsentinel.services", "trendsentinel.api"),
    "db": ("trendsentinel.collectors", "trendsentinel.services", "trendsentinel.api"),
    "services": ("trendsentinel.api",),
    "schemas": ("trendsentinel.db", "trendsentinel.services", "trendsentinel.collectors"),
    "worker": ("trendsentinel.api",),
}


def python_files(layer: str) -> list[Path]:
    """Return every module file belonging to ``layer``."""
    return sorted((SRC / layer).rglob("*.py"))


def imported_names(path: Path) -> set[str]:
    """Return the dotted module names imported by a file, including relative imports."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package_parts = path.relative_to(SRC).parts[:-1]

    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                # Resolve a relative import against this file's package.
                prefix = package_parts[: len(package_parts) - node.level + 1]
                base = ".".join(("trendsentinel", *prefix))
                names.add(f"{base}.{node.module}" if node.module else base)
            elif node.module:
                names.add(node.module)

    return names


def test_every_layer_has_modules() -> None:
    for layer in FORBIDDEN_IMPORTS:
        assert python_files(layer), f"layer {layer!r} has no modules to check"


@pytest.mark.parametrize("layer", sorted(FORBIDDEN_IMPORTS))
def test_layer_does_not_import_forbidden_modules(layer: str) -> None:
    forbidden = FORBIDDEN_IMPORTS[layer]
    violations: list[str] = []

    for path in python_files(layer):
        for name in imported_names(path):
            if any(name == bad or name.startswith(f"{bad}.") for bad in forbidden):
                violations.append(f"{path.relative_to(SRC)} imports {name}")

    assert not violations, f"{layer} must not import from {forbidden}: " + "; ".join(violations)


def test_collectors_have_no_database_dependency_at_all() -> None:
    """The collector layer must be testable with mocked HTTP and nothing else."""
    offenders: list[str] = []

    for path in python_files("collectors"):
        for name in imported_names(path):
            if name.startswith(("trendsentinel.db", "psycopg")):
                offenders.append(f"{path.name} imports {name}")

    assert not offenders, "; ".join(offenders)


def test_sql_lives_only_in_repositories() -> None:
    """No module outside `db/repositories` may execute SQL of its own."""
    repositories = SRC / "db" / "repositories"
    keywords = ("INSERT INTO", "SELECT ", "UPDATE ", "DELETE FROM", "CREATE TABLE")
    offenders: list[str] = []

    for path in SRC.rglob("*.py"):
        if path.is_relative_to(repositories):
            continue
        text = path.read_text(encoding="utf-8")
        for keyword in keywords:
            if keyword in text:
                offenders.append(f"{path.relative_to(SRC)} contains {keyword!r}")
                break

    assert not offenders, "SQL belongs in db/repositories: " + "; ".join(offenders)
