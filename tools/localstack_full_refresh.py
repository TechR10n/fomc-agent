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

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> None:
    print(f"\n==> Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    load_localstack_env()

    bls_series = os.environ["BLS_SERIES"].strip()
    datausa_datasets = os.environ["DATAUSA_DATASETS"].strip()

    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_up.py")])
    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_invoke_fetcher.py")])

    parse_cmd = [
        sys.executable,
        "-m",
        "src.transforms.to_processed",
        "--bls-series",
        bls_series,
        "--datausa-datasets",
        datausa_datasets,
    ]
    _run(parse_cmd)

    _run([sys.executable, "-m", "src.analytics.reports"])

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
