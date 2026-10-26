"""SSRF protection for every outbound request.

Collectors fetch URLs that come from source configuration and, indirectly, from third-party
feed content. This module makes sure a request can never be pointed at the host's own
network, a cloud metadata endpoint, or a domain outside an explicit allowlist.

The checks run per request rather than once at registration, because a redirect or a changed
DNS record would otherwise slip past a single up-front validation.
"""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable, Iterable, Sequence
from urllib.parse import urlsplit

__all__ = [
    "ALLOWED_SCHEMES",
    "BLOCKED_NETWORKS",
    "Resolver",
    "UrlNotAllowedError",
    "assert_url_allowed",
    "host_is_allowlisted",
    "is_ip_allowed",
    "system_resolver",
]

#: A callable that turns a hostname into the addresses it resolves to.
Resolver = Callable[[str], Sequence[str]]

ALLOWED_SCHEMES = frozenset({"http", "https"})

#: Address ranges a request must never reach. Includes the cloud metadata addresses that
#: are the usual target of an SSRF attempt.
BLOCKED_NETWORKS: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "0.0.0.0/8",  # "this host"
        "10.0.0.0/8",  # private
        "100.64.0.0/10",  # carrier-grade NAT
        "127.0.0.0/8",  # loopback
        "169.254.0.0/16",  # link-local, incl. 169.254.169.254 cloud metadata
        "172.16.0.0/12",  # private
        "192.0.0.0/24",  # IETF protocol assignments
        "192.0.2.0/24",  # documentation
        "192.168.0.0/16",  # private
        "198.18.0.0/15",  # benchmarking
        "198.51.100.0/24",  # documentation
        "203.0.113.0/24",  # documentation
        "224.0.0.0/4",  # multicast
        "240.0.0.0/4",  # reserved
        "255.255.255.255/32",  # broadcast
        "::/128",  # unspecified
        "::1/128",  # loopback
        "fc00::/7",  # unique local
        "fe80::/10",  # link-local
        "ff00::/8",  # multicast
        "2001:db8::/32",  # documentation
        "fd00:ec2::254/128",  # AWS metadata over IPv6
    )
)

#: Cloud metadata hostnames reachable by name rather than by raw address.
BLOCKED_HOSTNAMES = frozenset(
    {
        "metadata",
        "metadata.google.internal",
        "metadata.goog",
    }
)


class UrlNotAllowedError(Exception):
    """A URL was rejected before any request was made."""

    def __init__(self, url: str, reason: str) -> None:
        super().__init__(f"URL rejected ({reason}): {url}")
        self.url = url
        self.reason = reason


def is_ip_allowed(address: str) -> bool:
    """Return whether an IP literal is outside every blocked range."""
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False

    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        # An IPv4-mapped IPv6 address must be judged as the IPv4 address it carries,
        # otherwise 127.0.0.1 could be smuggled in as ::ffff:127.0.0.1.
        parsed = parsed.ipv4_mapped

    return not any(parsed in network for network in BLOCKED_NETWORKS)


def assert_url_allowed(
    url: str,
    allowed_domains: Iterable[str] = (),
    *,
    resolver: Resolver | None = None,
) -> None:
    """Validate a URL before it is requested.

    Checks, in order: scheme, absence of embedded credentials, an optional domain
    allowlist, and then every address the hostname resolves to.

    Args:
        url: The absolute URL about to be requested.
        allowed_domains: When non-empty, the hostname must match one of these entries.
            An empty iterable means "no allowlist", which is acceptable for official API and
            RSS sources whose URLs come from the database, never from collected content.
        resolver: How to resolve a hostname. Defaults to the system resolver; tests inject a
            deterministic one so no test depends on real DNS.

    Raises:
        UrlNotAllowedError: The URL is unsafe to request.
    """
    if not url or not url.strip():
        raise UrlNotAllowedError(url, "empty URL")

    parts = urlsplit(url.strip())

    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        raise UrlNotAllowedError(url, f"scheme {parts.scheme!r} is not allowed")

    if parts.username or parts.password:
        # Embedded credentials are both an SSRF vector and a way to smuggle a host.
        raise UrlNotAllowedError(url, "credentials in URL are not allowed")

    hostname = parts.hostname
    if not hostname:
        raise UrlNotAllowedError(url, "missing host")

    if hostname.lower().rstrip(".") in BLOCKED_HOSTNAMES:
        raise UrlNotAllowedError(url, "metadata hostname")

    domains = _normalize_domains(allowed_domains)
    if domains and not _host_matches_any(hostname, domains):
        raise UrlNotAllowedError(url, "host is not in the allowlist")

    # A literal address needs no DNS lookup.
    if _is_literal_address(hostname):
        if not is_ip_allowed(hostname):
            raise UrlNotAllowedError(url, "address is in a blocked range")
        return

    for address in _resolve(hostname, resolver):
        if not is_ip_allowed(address):
            raise UrlNotAllowedError(url, f"host resolves to a blocked address ({address})")


def host_is_allowlisted(hostname: str, allowed_domains: Iterable[str]) -> bool:
    """Return whether ``hostname`` is allowed by a (possibly empty) allowlist.

    An empty allowlist allows everything, matching ``assert_url_allowed``'s semantics.
    """
    domains = _normalize_domains(allowed_domains)
    if not domains:
        return True
    return _host_matches_any(hostname, domains)


def _normalize_domains(allowed_domains: Iterable[str]) -> tuple[str, ...]:
    """Lowercase and strip trailing dots so comparisons are consistent."""
    return tuple(
        domain.strip().lower().rstrip(".")
        for domain in allowed_domains
        if domain and domain.strip()
    )


def _host_matches_any(hostname: str, domains: Sequence[str]) -> bool:
    """Return whether the hostname equals, or is a subdomain of, an allowed domain.

    Matching is done on label boundaries so that ``evilexample.com`` can never satisfy an
    allowlist entry of ``example.com``, which a naive suffix check would accept.
    """
    host = hostname.lower().rstrip(".")
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _is_literal_address(hostname: str) -> bool:
    """Return whether the host is an IP literal rather than a name."""
    candidate = hostname.strip("[]")
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False

    return True


def system_resolver(hostname: str) -> list[str]:
    """Resolve a hostname with the system resolver.

    Every returned address is checked by the caller, not just the first, so a DNS record
    that mixes a public address with a private one cannot be used to reach the private one.

    Raises:
        UrlNotAllowedError: The name does not resolve.
    """
    try:
        infos = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UrlNotAllowedError(hostname, f"DNS resolution failed: {exc.strerror or exc}") from exc

    addresses: list[str] = []
    for info in infos:
        address = info[4][0]
        if isinstance(address, str) and address not in addresses:
            addresses.append(address)

    if not addresses:
        raise UrlNotAllowedError(hostname, "host resolved to no addresses")

    return addresses


def _resolve(hostname: str, resolver: Resolver | None = None) -> Sequence[str]:
    """Resolve a hostname using the injected resolver, or the system one."""
    if resolver is not None:
        addresses = list(resolver(hostname))
        if not addresses:
            raise UrlNotAllowedError(hostname, "host resolved to no addresses")
        return addresses

    return system_resolver(hostname)
