"""Tiny stdlib HTTP helpers (Lambda-friendly).

We keep HTTP logic here so Lambdas and local scripts can share behavior without
pulling in third-party dependencies like `requests`.
"""

from __future__ import annotations

import json
import random
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_DEFAULT_RETRYABLE_HTTP_STATUS: set[int] = {429, 500, 502, 503, 504}


def _parse_retry_after_seconds(value: str | None) -> float | None:
    """Parse a Retry-After header into seconds (best effort).

    Many APIs use an integer number of seconds. Some use an HTTP date, but we
    intentionally keep this stdlib-only and minimal; non-numeric values are
    ignored.
    """
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        return None
    if seconds < 0:
        return None
    return seconds


def _sleep_seconds(
    *,
    attempt: int,
    backoff_seconds: float,
    retry_after_seconds: float | None = None,
    max_backoff_seconds: float = 60.0,
) -> None:
    base = max(0.0, float(backoff_seconds)) * (2**max(0, attempt))
    candidate = max(base, retry_after_seconds or 0.0)

    # Add jitter to reduce synchronized retries.
    if candidate > 0:
        candidate *= random.uniform(0.8, 1.2)

    sleep_for = min(max_backoff_seconds, candidate)
    if sleep_for > 0:
        time.sleep(sleep_for)


def _ssl_context() -> ssl.SSLContext:
    """Return an SSL context that works on macOS Python.org installs.

    Some Python builds (notably the Python.org macOS installer) require running
    an "Install Certificates" step to populate the OpenSSL CA bundle path.
    When that hasn't been done, stdlib `urllib` HTTPS calls fail with:
      CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate

    If no system CA bundle is found, fall back to `certifi` (if installed).
    """
    paths = ssl.get_default_verify_paths()
    cafile = paths.openssl_cafile
    capath = paths.openssl_capath
    if (cafile and Path(cafile).exists()) or (capath and Path(capath).exists()):
        return ssl.create_default_context()

    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        # Last resort: use the default context even though verification will
        # likely fail. Callers will see the underlying SSL error.
        return ssl.create_default_context()


def fetch_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    retryable_statuses: set[int] | None = None,
    max_backoff_seconds: float = 60.0,
) -> bytes:
    """Fetch bytes from a URL with basic retries."""
    if retries < 1:
        retries = 1
    retryable = retryable_statuses or _DEFAULT_RETRYABLE_HTTP_STATUS

    last_error: Exception | None = None
    ctx = _ssl_context()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:  # nosec - url is controlled by caller
                return resp.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_error = e
            if attempt < retries - 1:
                retry_after = None
                if isinstance(e, urllib.error.HTTPError):
                    status = int(getattr(e, "code", 0) or 0)
                    if status and status not in retryable:
                        break
                    retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
                _sleep_seconds(
                    attempt=attempt,
                    backoff_seconds=backoff_seconds,
                    retry_after_seconds=retry_after,
                    max_backoff_seconds=max_backoff_seconds,
                )
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
    retryable_statuses: set[int] | None = None,
    max_backoff_seconds: float = 60.0,
) -> str:
    """Fetch text from a URL with basic retries."""
    return fetch_bytes(
        url,
        headers=headers,
        timeout=timeout,
        retries=retries,
        backoff_seconds=backoff_seconds,
        retryable_statuses=retryable_statuses,
        max_backoff_seconds=max_backoff_seconds,
    ).decode(encoding, errors="replace")


def fetch_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: float = 1.0,
    retryable_statuses: set[int] | None = None,
    max_backoff_seconds: float = 60.0,
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
                retry_after = None
                if isinstance(e, urllib.error.HTTPError):
                    status = int(getattr(e, "code", 0) or 0)
                    retryable = retryable_statuses or _DEFAULT_RETRYABLE_HTTP_STATUS
                    if status and status not in retryable:
                        break
                    retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
                _sleep_seconds(
                    attempt=attempt,
                    backoff_seconds=backoff_seconds,
                    retry_after_seconds=retry_after,
                    max_backoff_seconds=max_backoff_seconds,
                )
    assert last_error is not None
    raise last_error
