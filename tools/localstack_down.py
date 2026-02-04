#!/usr/bin/env python3
"""Stop LocalStack via `docker compose down`."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    if not shutil.which("docker"):
        raise SystemExit("`docker` not found on PATH.")

    subprocess.run(["docker", "compose", "down"], cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    main()

