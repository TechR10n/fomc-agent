#!/usr/bin/env python3
"""Stop a running LocalStack worker (started by CDC demo live)."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

PID_FILE = Path("/tmp/fomc-localstack-worker.pid")


def _is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _terminate_pid(pid: int) -> bool:
    if not _is_running(pid):
        return False
    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _is_running(pid):
            return True
        time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)
    return True


def _scan_worker_pids() -> list[int]:
    out = subprocess.check_output(["ps", "-ax", "-o", "pid=,command="], text=True)
    pids: list[int] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        pid_str, cmd = parts
        if "localstack_worker.py" in cmd and "localstack_stop_worker.py" not in cmd:
            try:
                pids.append(int(pid_str))
            except ValueError:
                continue
    return pids


def main() -> None:
    stopped_any = False

    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
        except ValueError:
            pid = -1
        if pid > 0 and _terminate_pid(pid):
            stopped_any = True
        PID_FILE.unlink(missing_ok=True)

    if not stopped_any:
        for pid in _scan_worker_pids():
            if _terminate_pid(pid):
                stopped_any = True

    if stopped_any:
        print("Stopped LocalStack worker.")
    else:
        print("No LocalStack worker process found.")


if __name__ == "__main__":
    main()
