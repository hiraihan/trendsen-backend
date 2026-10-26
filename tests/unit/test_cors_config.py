"""CORS origin allowlist (spec v1.1 §7).

Empty by default: no cross-origin request is served unless the operator lists origins.
Listing origins adds no capability — the admin key on POST /collect/run is unchanged.
"""

from __future__ import annotations

from tests.helpers import build_settings


def test_cors_origins_default_is_empty() -> None:
    settings = build_settings()
    assert settings.cors_origins == ""
    assert settings.cors_origins_list == []


def test_cors_origins_parsed_and_normalized() -> None:
    settings = build_settings(cors_origins="http://localhost:5173, https://spa.example.com/,")
    assert settings.cors_origins_list == ["http://localhost:5173", "https://spa.example.com"]


def test_cors_origins_blank_entries_dropped() -> None:
    settings = build_settings(cors_origins=" , ,")
    assert settings.cors_origins_list == []
