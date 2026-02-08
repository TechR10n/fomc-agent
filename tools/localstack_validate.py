#!/usr/bin/env python3
"""Run a full LocalStack validation pass (refresh + asset checks + tests)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"\n==> Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True, env=env)


def _clean_test_env(env: dict[str, str]) -> dict[str, str]:
    """Remove LocalStack endpoint overrides for unit tests.

    Our unit tests use `moto` and should never talk to a real endpoint.
    When running after `load_localstack_env()`, endpoint env vars would route
    boto3 calls to LocalStack and break isolation.
    """
    cleaned = dict(env)
    for key in list(cleaned.keys()):
        if key == "AWS_ENDPOINT_URL" or key.startswith("AWS_ENDPOINT_URL_"):
            cleaned.pop(key, None)
    cleaned.pop("AWS_S3_ADDRESSING_STYLE", None)
    return cleaned


def main() -> None:
    load_localstack_env()
    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_full_refresh.py")])
    _run([sys.executable, str(PROJECT_ROOT / "tools/check_s3_assets.py"), "--strict"])
    _run([sys.executable, "-m", "pytest"], env=_clean_test_env(os.environ))
    print("\n==> LocalStack validation complete.")


if __name__ == "__main__":
    main()
