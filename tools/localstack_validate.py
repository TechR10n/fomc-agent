#!/usr/bin/env python3
"""Run a full LocalStack validation pass (refresh + asset checks + tests)."""

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
    _run([sys.executable, str(PROJECT_ROOT / "tools/check_s3_assets.py"), "--strict"])
    _run([sys.executable, "-m", "pytest"])
    print("\n==> LocalStack validation complete.")


if __name__ == "__main__":
    main()
