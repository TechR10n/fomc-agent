#!/usr/bin/env python3
"""Export a BLS change timeline JSON file for the static site.

Reads `_sync_state/<series>/sync_log.jsonl` in the BLS raw bucket and writes a
compact payload containing only add/update/delete events for the last N days.

The timeline time is driven by the BLS directory listing timestamp
(`source_modified`), not the pipeline run time.

Usage:
  source .env.shared
  source .env.local        # optional AWS local override
  # or: source .env.localstack
  python tools/build_bls_timeline.py --days 60 --out site/data/bls_timeline.json
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.analytics.bls_timeline import export_bls_change_timeline
from src.config import get_bls_bucket, get_bls_series_list


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=get_bls_bucket(), help="BLS raw bucket containing _sync_state/")
    parser.add_argument(
        "--series",
        default=",".join(get_bls_series_list()),
        help="Comma-separated BLS series ids (default: BLS_SERIES env var)",
    )
    parser.add_argument("--days", type=int, default=60, help="Trailing window (days)")
    parser.add_argument("--lookahead-days", type=int, default=14, help="Include upcoming scheduled releases (days)")
    parser.add_argument(
        "--no-schedule",
        action="store_true",
        help="Do not fetch BLS release schedules from bls.gov",
    )
    parser.add_argument("--out", default="site/data/bls_timeline.json", help="Output JSON path")
    args = parser.parse_args()

    series_list = [s.strip() for s in str(args.series).split(",") if s.strip()]
    out_path = Path(args.out)

    path = export_bls_change_timeline(
        out_path=out_path,
        bucket=args.bucket,
        series_list=series_list,
        window_days=args.days,
        lookahead_days=max(0, int(args.lookahead_days)),
        include_release_schedule=not args.no_schedule,
    )
    print(str(path))


if __name__ == "__main__":
    main()
