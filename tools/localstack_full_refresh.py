#!/usr/bin/env python3
"""Run the full LocalStack refresh pipeline in the correct order.

Steps:
  1) Start LocalStack (docker compose)
  2) Invoke the data-fetcher Lambda locally (writes raw S3)
  3) Parse raw data to processed CSVs
  4) Generate site chart JSON artifacts
  5) Build the BLS change timeline
"""

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

    bls_series = os.environ.get("BLS_SERIES", "pr").strip()
    datausa_datasets = os.environ.get("DATAUSA_DATASETS", "population").strip()

    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_up.py")])
    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_invoke_fetcher.py")])

    parse_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "src/transforms/to_processed.py"),
        "--bls-series",
        bls_series,
        "--datausa-datasets",
        datausa_datasets,
    ]
    _run(parse_cmd)

    _run([sys.executable, str(PROJECT_ROOT / "src/analytics/reports.py")])

    timeline_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "tools/build_bls_timeline.py"),
        "--days",
        "60",
        "--out",
        "site/data/bls_timeline.json",
    ]
    _run(timeline_cmd)

    print("\n==> LocalStack refresh complete.")


if __name__ == "__main__":
    main()
