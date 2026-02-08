#!/usr/bin/env python3
"""Start LocalStack via `docker compose up -d` and wait for readiness."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEALTH_URL = "http://localhost:4566/_localstack/health"


def _get_health() -> dict | None:
    try:
        req = urllib.request.Request(HEALTH_URL)
        with urllib.request.urlopen(req, timeout=2) as resp:  # nosec - local URL
            body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def main() -> None:
    if not shutil.which("docker"):
        raise SystemExit("`docker` not found on PATH.")

    load_localstack_env()
    subprocess.run(["docker", "compose", "up", "-d"], cwd=PROJECT_ROOT, check=True)

    deadline = time.time() + 120
    while time.time() < deadline:
        health = _get_health()
        if health:
            print(json.dumps(health, indent=2, default=str))
            return
        time.sleep(1)

    raise SystemExit(
        "LocalStack did not become ready within 120s. "
        "Check Docker logs: `docker logs fomc-localstack`."
    )


if __name__ == "__main__":
    main()
