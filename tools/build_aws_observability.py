#!/usr/bin/env python3
"""Export AWS observability JSON for the static site.

This writes `site/data/aws_observability.json`, which the Timeline dashboard can
render as:
  - CloudWatch metrics (Lambda, SQS, optional S3 storage)
  - Cost Explorer actuals + forecast (predicted vs actual cost)

Usage:
  source .env.localstack   # optional (for LocalStack)
  python tools/build_aws_observability.py --days 30 --forecast-days 30 --out site/data/aws_observability.json
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.analytics.aws_observability import export_aws_observability


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30, help="Trailing window (days)")
    parser.add_argument("--forecast-days", type=int, default=30, help="Cost forecast horizon (days)")
    parser.add_argument("--out", default="site/data/aws_observability.json", help="Output JSON path")
    args = parser.parse_args()

    path = export_aws_observability(
        out_path=Path(args.out),
        window_days=int(args.days),
        forecast_days=max(0, int(args.forecast_days)),
    )
    print(str(path))


if __name__ == "__main__":
    main()

