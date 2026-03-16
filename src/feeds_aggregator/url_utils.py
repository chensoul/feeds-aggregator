from __future__ import annotations

from urllib.parse import urlparse


def normalize_http_url(value: str | None) -> str | None:
    if value is None:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return candidate
