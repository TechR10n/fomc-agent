#!/usr/bin/env python3
"""Poll LocalStack SQS and run the analytics Lambda handler locally.

This simulates the SQS→Lambda event source without deploying anything via CDK.
Keep this running while iterating on `src/lambdas/analytics_processor/handler.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.helpers.aws_client import get_client
from src.lambdas.analytics_processor.handler import handler as analytics_handler


def _ensure_localstack_env() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        raise SystemExit(
            "AWS_ENDPOINT_URL is not set (expected something like http://localhost:4566). "
            "Tip: source `.env.localstack` or use the PyCharm run config."
        )

    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def _get_queue_url(queue_name: str) -> str:
    sqs = get_client("sqs")
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def _handle_message(body: str) -> dict[str, Any]:
    # Lambda (SQS event source) provides record bodies as strings.
    event = {"Records": [{"body": body}]}
    return analytics_handler(event, None)


def process_once(
    *,
    queue_name: str,
    max_messages: int,
    wait_seconds: int,
) -> int:
    sqs = get_client("sqs")
    queue_url = _get_queue_url(queue_name)
    processed = 0

    while processed < max_messages:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break

        msg = messages[0]
        receipt = msg["ReceiptHandle"]
        body = msg.get("Body", "")

        try:
            result = _handle_message(body)
            status = int(result.get("statusCode", 500))
            ok = 200 <= status < 300
        except Exception as exc:
            ok = False
            print(f"[worker] Error processing message: {exc}", file=sys.stderr)

        if ok:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            processed += 1
            print(json.dumps(result, indent=2, default=str))
        else:
            print("[worker] Leaving message in queue for retry.", file=sys.stderr)
            break

    return processed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="fomc-analytics-queue", help="SQS queue name")
    parser.add_argument("--once", action="store_true", help="Process all available messages then exit")
    parser.add_argument("--max-messages", type=int, default=100, help="Max messages to process per run")
    parser.add_argument("--wait", type=int, default=10, help="Long-poll wait seconds (0-20)")
    args = parser.parse_args()

    _ensure_localstack_env()

    if args.once:
        processed = process_once(queue_name=args.queue, max_messages=args.max_messages, wait_seconds=0)
        print(f"[worker] Processed {processed} message(s).")
        return

    print(f"[worker] Polling SQS queue: {args.queue}")
    while True:
        processed = process_once(
            queue_name=args.queue,
            max_messages=1,
            wait_seconds=max(0, min(args.wait, 20)),
        )
        if processed == 0:
            time.sleep(0.25)


if __name__ == "__main__":
    main()

