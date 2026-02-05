#!/usr/bin/env python3
"""Run a CDC demo against LocalStack and keep the worker running.

Starts the LocalStack worker in long-poll mode, triggers a touch event,
then keeps the worker alive until you stop the run configuration.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PID_FILE = Path("/tmp/fomc-localstack-worker.pid")


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
    ]
    print(f"==> Starting worker: {' '.join(worker_cmd)}")
    worker = subprocess.Popen(worker_cmd, cwd=PROJECT_ROOT)
    PID_FILE.write_text(str(worker.pid))

    def _shutdown(*_args) -> None:
        if worker.poll() is None:
            worker.send_signal(signal.SIGTERM)
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
        if PID_FILE.exists():
            PID_FILE.unlink()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Give the worker a moment to start its long-poll before triggering.
    time.sleep(0.5)

    touch_cmd = [sys.executable, str(PROJECT_ROOT / "tools/localstack_touch_datausa.py")]
    print(f"==> Triggering CDC: {' '.join(touch_cmd)}")
    subprocess.run(touch_cmd, cwd=PROJECT_ROOT, check=True)

    print("==> Worker is running. Stop the run configuration to exit.")
    worker.wait()
    if PID_FILE.exists():
        PID_FILE.unlink()


if __name__ == "__main__":
    main()
