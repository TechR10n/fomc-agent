#!/usr/bin/env python3
"""Invoke the data-fetcher Lambda handler against LocalStack.

This is the fastest way to validate Lambda behavior without deploying via CDK.
It writes to LocalStack S3 and returns the same JSON shape as the deployed Lambda.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.lambdas.data_fetcher.handler import handler as fetcher_handler


def _ensure_localstack_env() -> None:
    os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def main() -> None:
    _ensure_localstack_env()

    started = time.time()
    response = fetcher_handler({}, None)
    duration_seconds = time.time() - started

    body = response.get("body")
    try:
        parsed = json.loads(body) if isinstance(body, str) else body
        response = {**response, "body": parsed}
    except Exception:
        pass

    print(
        json.dumps(
            {
                "duration_seconds": round(duration_seconds, 2),
                "response": response,
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
