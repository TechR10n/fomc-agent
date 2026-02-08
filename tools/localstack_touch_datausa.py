#!/usr/bin/env python3
"""Re-upload the existing DataUSA object to trigger S3→SQS locally.

Use this for tight iteration on the analytics processor: change code, then run
this script to generate a fresh S3:ObjectCreated event without re-fetching the
DataUSA API.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_datausa_bucket, get_datausa_key
from src.helpers.aws_client import get_client


def main() -> None:
    load_localstack_env()

    bucket = get_datausa_bucket()
    key = get_datausa_key()

    s3 = get_client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise SystemExit(
            f"Could not read s3://{bucket}/{key}. "
            "Run the fetcher once to seed LocalStack data first.\n\n"
            f"Error: {exc}"
        ) from exc

    body = obj["Body"].read()
    content_type = obj.get("ContentType") or "application/json"
    metadata = dict(obj.get("Metadata") or {})
    metadata["touched_at"] = datetime.now(timezone.utc).isoformat()

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        Metadata=metadata,
    )

    print(f"Touched s3://{bucket}/{key} (re-uploaded to trigger an event).")


if __name__ == "__main__":
    main()
