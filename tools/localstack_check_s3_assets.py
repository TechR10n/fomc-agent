#!/usr/bin/env python3
"""Validate expected LocalStack S3 buckets/keys for the FOMC pipeline.

This is a convenience wrapper around `tools/check_s3_assets.py` that loads
`.env.shared` + `.env.localstack` automatically before running the checks.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> None:
    print(f"\n==> Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    load_localstack_env()
    _run([sys.executable, str(PROJECT_ROOT / "tools/check_s3_assets.py"), "--strict"])
    print("\n==> LocalStack S3 asset check complete.")


if __name__ == "__main__":
    main()
