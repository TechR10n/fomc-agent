#!/usr/bin/env python3
"""Run full LocalStack refresh, then start the CDC live demo."""

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

    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_full_refresh.py")])
    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_cdc_demo_live.py")])


if __name__ == "__main__":
    main()
