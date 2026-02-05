#!/usr/bin/env python3
"""Run full LocalStack refresh, then start the CDC live demo."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _run(cmd: list[str]) -> None:
    print(f"\n==> Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    _load_env_file(PROJECT_ROOT / ".env.localstack")

    os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_full_refresh.py")])
    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_cdc_demo_live.py")])


if __name__ == "__main__":
    main()
