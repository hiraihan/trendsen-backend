"""Tests for the SSRF guard.

The rejection table is the core of this file: each entry is an address an attacker would try
to reach through a collector, and every one of them must be refused before a request is made.
"""

from __future__ import annotations

import socket

import pytest

from trendsentinel.collectors.ssrf import UrlNotAllowedError, assert_url_allowed, is_ip_allowed


@pytest.mark.parametrize(
    ("address", "blocked"),
    [
        # Loopback and "this host"
        ("127.0.0.1", True),
        ("127.1.2.3", True),
        ("0.0.0.0", True),
        ("::1", True),
        ("::", True),
        # Private ranges
        ("10.0.0.1", True),
        ("172.16.5.5", True),
        ("192.168.1.1", True),
        ("100.64.0.1", True),
        # Link-local, including the cloud metadata address
        ("169.254.169.254", True),
        ("169.254.0.1", True),
        ("fe80::1", True),
        ("fd00:ec2::254", True),
        # Reserved and special-purpose
        ("224.0.0.1", True),
        ("240.0.0.1", True),
        ("192.0.2.10", True),
        ("198.51.100.10", True),
        ("203.0.113.10", True),
        ("fc00::1", True),
        ("ff02::1", True),
        ("2001:db8::1", True),
        # IPv4-mapped IPv6 must be judged as the IPv4 address it carries
        ("::ffff:127.0.0.1", True),
        ("::ffff:169.254.169.254", True),
        # Public addresses are allowed
        ("93.184.216.34", False),
        ("2606:2800:220:1:248:1893:25c8:1946", False),
    ],
)
def test_is_ip_allowed(address: str, blocked: bool) -> None:
    assert is_ip_allowed(address) is not blocked


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "ftp://example.test/file",
        "gopher://example.test/",
        "data:text/plain;base64,SGVsbG8=",
        "javascript:alert(1)",
    ],
)
def test_non_http_schemes_are_rejected(url: str) -> None:
    with pytest.raises(UrlNotAllowedError, match="scheme"):
        assert_url_allowed(url)


@pytest.mark.parametrize("url", ["", "   ", "not a url at all"])
def test_empty_or_unparseable_urls_are_rejected(url: str) -> None:
    with pytest.raises(UrlNotAllowedError):
        assert_url_allowed(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://user:password@example.test/",
        "https://admin@example.test/",
        "http://user@127.0.0.1/",
    ],
)
def test_credentials_in_url_are_rejected(url: str) -> None:
    with pytest.raises(UrlNotAllowedError, match="credentials"):
        assert_url_allowed(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/",
        "http://127.0.0.1:8000/health",
        "http://10.0.0.5/latest/meta-data/",
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        "http://[::1]/",
        "http://0.0.0.0/",
        "http://192.168.0.1/router",
        "http://172.16.0.1/",
    ],
)
def test_internal_ip_literals_are_rejected(url: str) -> None:
    with pytest.raises(UrlNotAllowedError, match="blocked range"):
        assert_url_allowed(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://metadata/",
        "http://metadata.goog/",
    ],
)
def test_metadata_hostnames_are_rejected(url: str) -> None:
    with pytest.raises(UrlNotAllowedError, match="metadata hostname"):
        assert_url_allowed(url)


#: A resolver that maps every name to one public address, so tests need no real DNS.
PUBLIC = ["93.184.216.34"]


def public_resolver(hostname: str) -> list[str]:
    return list(PUBLIC)


def test_hostname_resolving_to_a_private_address_is_rejected() -> None:
    """A DNS record pointing at internal infrastructure must not be usable."""
    with pytest.raises(UrlNotAllowedError, match="resolves to a blocked address"):
        assert_url_allowed("https://internal.example.test/", resolver=lambda _: ["10.1.2.3"])


def test_mixed_public_and_private_answers_are_rejected() -> None:
    """Every resolved address is checked, not just the first: this defeats DNS rebinding."""
    with pytest.raises(UrlNotAllowedError, match="blocked address"):
        assert_url_allowed(
            "https://rebind.example.test/",
            resolver=lambda _: ["93.184.216.34", "169.254.169.254"],
        )


def test_public_hostname_is_allowed() -> None:
    assert_url_allowed("https://example.test/feed.xml", resolver=public_resolver)


def test_unresolvable_hostname_is_rejected() -> None:
    def failing_resolver(hostname: str) -> list[str]:
        return []

    with pytest.raises(UrlNotAllowedError, match="no addresses"):
        assert_url_allowed("https://does-not-exist.example.test/", resolver=failing_resolver)


def test_gaierror_is_reported_as_a_resolution_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise socket.gaierror(-2, "Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", fail)

    with pytest.raises(UrlNotAllowedError, match="DNS resolution failed"):
        assert_url_allowed("https://does-not-exist.example.test/")


def test_system_resolver_is_used_when_none_is_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))],
    )

    assert_url_allowed("https://example.test/feed.xml")


# --------------------------------------------------------------------------- allowlist


def test_allowlisted_domain_is_permitted() -> None:
    assert_url_allowed("https://hnrss.org/frontpage", ["hnrss.org"], resolver=public_resolver)


def test_subdomain_of_an_allowlisted_domain_is_permitted() -> None:
    assert_url_allowed("https://feeds.example.test/rss", ["example.test"], resolver=public_resolver)


def test_host_outside_the_allowlist_is_rejected() -> None:
    with pytest.raises(UrlNotAllowedError, match="not in the allowlist"):
        assert_url_allowed("https://evil.test/feed", ["hnrss.org"], resolver=public_resolver)


@pytest.mark.parametrize("host", ["evilexample.com", "notexample.com", "example.com.evil.test"])
def test_allowlist_matching_is_on_label_boundaries(host: str) -> None:
    """`evilexample.com` must not satisfy an allowlist entry of `example.com`."""
    with pytest.raises(UrlNotAllowedError, match="not in the allowlist"):
        assert_url_allowed(f"https://{host}/feed", ["example.com"], resolver=public_resolver)


def test_an_empty_allowlist_means_no_allowlist() -> None:
    """Official APIs and RSS sources are configured in the database, not by feed content."""
    assert_url_allowed(
        "https://hacker-news.firebaseio.com/v0/topstories.json", [], resolver=public_resolver
    )
    assert_url_allowed(
        "https://hacker-news.firebaseio.com/v0/topstories.json", resolver=public_resolver
    )


def test_trailing_dot_and_case_do_not_evade_the_allowlist() -> None:
    assert_url_allowed("https://HNRSS.ORG./frontpage", ["hnrss.org"], resolver=public_resolver)
    assert_url_allowed("https://hnrss.org/frontpage", ["  HNRSS.ORG  "], resolver=public_resolver)
