#!/usr/bin/env python3
"""Run a CDC demo against LocalStack (worker + touch trigger).

Starts the LocalStack worker in long-poll mode, then re-uploads
population.json to trigger S3 -> SQS.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    load_localstack_env()

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
