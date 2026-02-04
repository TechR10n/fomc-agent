"""Tiny stdlib HTTP helpers (Lambda-friendly).

We keep HTTP logic here so Lambdas and local scripts can share behavior without
pulling in third-party dependencies like `requests`.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any


def fetch_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
) -> bytes:
    """Fetch bytes from a URL with basic retries."""
    if retries < 1:
        retries = 1

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec - url is controlled by caller
                return resp.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (2**attempt))
    assert last_error is not None
    raise last_error


def fetch_text(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    encoding: str = "utf-8",
) -> str:
    """Fetch text from a URL with basic retries."""
    return fetch_bytes(
        url,
        headers=headers,
        timeout=timeout,
        retries=retries,
        backoff_seconds=backoff_seconds,
    ).decode(encoding, errors="replace")


def fetch_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
) -> Any:
    """Fetch JSON from a URL with basic retries (including decode errors)."""
    if retries < 1:
        retries = 1

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            body = fetch_text(
                url,
                headers=headers,
                timeout=timeout,
                retries=1,
            )
            return json.loads(body)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (2**attempt))
    assert last_error is not None
    raise last_error

