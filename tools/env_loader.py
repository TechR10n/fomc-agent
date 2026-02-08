#!/usr/bin/env python3
"""Shared env-file loading helpers for local tooling."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _parse_env_file(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        if key:
            data[key] = value
    return data


def load_env_file(path: Path, *, required: bool, override: bool) -> None:
    if not path.exists():
        if required:
            raise SystemExit(f"Env file not found: {path}")
        return

    data = _parse_env_file(path)
    for key, value in data.items():
        if override or key not in os.environ:
            os.environ[key] = value


def require_env_vars(names: list[str]) -> None:
    missing = [name for name in names if not os.environ.get(name, "").strip()]
    if missing:
        raise SystemExit(f"Missing required environment variable(s): {', '.join(missing)}")


def load_shared_env(*, override: bool = False) -> None:
    load_env_file(PROJECT_ROOT / ".env.shared", required=True, override=override)


def load_localstack_env() -> None:
    # Shared vars first, LocalStack-specific vars second.
    load_shared_env(override=False)
    load_env_file(PROJECT_ROOT / ".env.localstack", required=True, override=True)
    require_env_vars(
        [
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "FOMC_BUCKET_PREFIX",
            "FOMC_ANALYTICS_QUEUE_NAME",
            "FOMC_ANALYTICS_DLQ_NAME",
            "FOMC_REMOVAL_POLICY",
            "FOMC_FETCH_INTERVAL_HOURS",
        ]
    )
