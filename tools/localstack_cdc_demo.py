#!/usr/bin/env python3
"""Run a CDC demo against LocalStack (worker + touch trigger).

Starts the LocalStack worker in long-poll mode, then re-uploads
population.json to trigger S3 -> SQS.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
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


def main() -> None:
    _load_env_file(PROJECT_ROOT / ".env.localstack")

    os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

    worker_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "tools/localstack_worker.py"),
        "--once",
        "--max-messages",
        "25",
        "--wait",
        "10",
    ]
    print(f"==> Starting worker: {' '.join(worker_cmd)}")
    worker = subprocess.Popen(worker_cmd, cwd=PROJECT_ROOT)

    # Give the worker a moment to start its long-poll before triggering.
    time.sleep(0.5)

    touch_cmd = [sys.executable, str(PROJECT_ROOT / "tools/localstack_touch_datausa.py")]
    print(f"==> Triggering CDC: {' '.join(touch_cmd)}")
    subprocess.run(touch_cmd, cwd=PROJECT_ROOT, check=True)

    code = worker.wait()
    if code != 0:
        raise SystemExit(code)

    print("==> CDC demo complete.")


if __name__ == "__main__":
    main()
