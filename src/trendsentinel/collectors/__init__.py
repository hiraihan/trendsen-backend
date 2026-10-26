"""Source adapters.

Collectors depend only on `base`, `http` and `ssrf`. They must never import from
`db`, `services` or `api`.
"""
