#!/usr/bin/env python3
"""Start LocalStack via `docker compose up -d` and wait for readiness."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEALTH_URL = "http://localhost:4566/_localstack/health"


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

    _load_env_file(PROJECT_ROOT / ".env.localstack")
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
