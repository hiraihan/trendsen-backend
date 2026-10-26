"""The shared thumbnail extraction rules.

Every adapter that writes ``metadata.thumbnail_url`` goes through these two functions,
so the safety rules are proven once, here: https-only, no synthetic URLs, no unlimited
scan depth, and the HTML path reads the first ``<img src>`` only.
"""

from __future__ import annotations

from typing import Any

import pytest

from trendsentinel.collectors.thumbnail import (
    THUMBNAIL_KEY,
    thumbnail_from_html,
    thumbnail_from_mapping,
)

# --------------------------------------------------------------------------- mapping


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # The real Reddit nesting: preview.images[].source.url.
        (
            {"images": [{"source": {"url": "https://cdn.test/a.jpg"}}]},
            "https://cdn.test/a.jpg",
        ),
        # A flat string (the Threads media_url shape).
        ("https://cdn.test/cover.png", "https://cdn.test/cover.png"),
        # Nested lists of candidates: first usable wins.
        (
            {"resolutions": [{"url": None}, {"url": "https://cdn.test/b.webp"}]},
            "https://cdn.test/b.webp",
        ),
    ],
)
def test_mapping_extracts_the_first_usable_https_url(
    value: Any, expected: str
) -> None:
    assert thumbnail_from_mapping(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [],
        "not a url",
        "ftp://cdn.test/a.jpg",
        # http is refused: a mixed-content image is not storable-and-showable.
        "http://cdn.test/a.jpg",
        # data:/blob:/javascript: bypass nothing: the scheme gate refuses them.
        "data:image/png;base64,AAAA",
        "blob:https://cdn.test/id",
        "javascript:alert(1)",
        # A protocol-relative URL has no scheme of its own — refuse.
        "//cdn.test/a.jpg",
        {"too": {"deep": {"to": {"matter": {"and": {"deeper": {"still": {"bottom": "https://cdn.test/deep.jpg"}}}}}}}},
    ],
)
def test_mapping_refuses_non_usable_candidates(value: Any) -> None:
    assert thumbnail_from_mapping(value) is None


def test_mapping_absent_key_is_none() -> None:
    assert thumbnail_from_mapping(None) is None


# --------------------------------------------------------------------------- html


def test_html_reads_the_first_img_src() -> None:
    html = (
        '<p>lead</p><img src="https://cdn.test/first.jpg">'
        '<img src="https://cdn.test/second.jpg">'
    )
    assert thumbnail_from_html(html) == "https://cdn.test/first.jpg"


def test_html_accepts_single_quoted_src() -> None:
    assert thumbnail_from_html("<img src='https://cdn.test/q.jpg'>") == (
        "https://cdn.test/q.jpg"
    )


@pytest.mark.parametrize(
    "html",
    [
        None,
        "",
        "<p>no images here</p>",
        '<img src="http://cdn.test/insecure.jpg">',
        '<img src="data:image/gif;base64,AAAA">',
        '<img alt="no src attribute">',
    ],
)
def test_html_refuses_non_usable_candidates(html: str | None) -> None:
    assert thumbnail_from_html(html) is None


# --------------------------------------------------------------------------- contract


def test_thumbnail_key_is_the_metadata_contract() -> None:
    """The SPA reads this exact key; renaming it would break the card silently."""
    assert THUMBNAIL_KEY == "thumbnail_url"
