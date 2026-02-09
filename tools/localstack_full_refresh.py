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

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def _run(cmd: list[str]) -> None:
    print(f"\n==> Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    load_localstack_env()

    bls_series = os.environ["BLS_SERIES"].strip()
    datausa_datasets = os.environ["DATAUSA_DATASETS"].strip()

    started = time.time()

    _run([sys.executable, str(PROJECT_ROOT / "tools/localstack_up.py")])

    # Invoke fetcher in-process so we can reuse results for pipeline_status.json.
    print("\n==> Running: data-fetcher (in-process)")
    from src.lambdas.data_fetcher.handler import handler as fetcher_handler  # noqa: E402

    fetcher_started = time.time()
    fetcher_response = fetcher_handler({}, None)
    fetcher_duration = time.time() - fetcher_started
    fetcher_body = fetcher_response.get("body")
    try:
        sync_results = json.loads(fetcher_body) if isinstance(fetcher_body, str) else (fetcher_body or {})
    except Exception:
        sync_results = {}
    print(
        json.dumps(
            {
                "duration_seconds": round(fetcher_duration, 2),
                "response": {
                    **fetcher_response,
                    "body": sync_results if isinstance(sync_results, dict) else fetcher_body,
                },
            },
            indent=2,
            default=str,
        )
    )

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

    # Write pipeline_status.json (derived from the fetcher results + _sync_state metadata in S3).
    try:
        from src.analytics.reports import export_pipeline_status  # noqa: E402

        total_duration = time.time() - started
        out = export_pipeline_status(sync_results if isinstance(sync_results, dict) else {}, "site/data/pipeline_status.json", total_duration)
        print(f"\n==> Wrote: {out}")
    except Exception as exc:
        print(f"\n==> WARNING: Could not write pipeline_status.json: {exc}")

    print("\n==> LocalStack refresh complete.")


if __name__ == "__main__":
    main()
