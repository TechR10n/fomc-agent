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

from env_loader import load_localstack_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEALTH_URL = "http://localhost:4566/_localstack/health"


def _get_health() -> dict | None:
    try:
        req = urllib.request.Request(HEALTH_URL)
        with urllib.request.urlopen(req, timeout=2) as resp:  # nosec - local URL
            body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None


def _ensure_site_bucket() -> None:
    # LocalStack init hook creates the raw/processed buckets, but not the site bucket.
    # Creating it here makes the local validation checks (tools/check_s3_assets.py) pass.
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError
    except Exception:
        # If boto3 isn't installed, skip. Callers will see missing-bucket warnings later.
        return

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "").strip() or None
    region = os.environ.get("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    prefix = os.environ.get("FOMC_BUCKET_PREFIX", "").strip()
    if not prefix:
        return

    addressing_style = os.environ.get("AWS_S3_ADDRESSING_STYLE", "").strip() or "path"
    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        config=Config(s3={"addressing_style": addressing_style}),
    )

    bucket = f"{prefix}-site"
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "")).strip()
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            return

    kwargs = {"Bucket": bucket}
    # Keep behavior AWS-compatible; LocalStack will accept either.
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)


def _require_docker_engine() -> None:
    try:
        subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except subprocess.CalledProcessError:
        ctx = ""
        try:
            ctx = (
                subprocess.run(
                    ["docker", "context", "show"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    check=False,
                )
                .stdout.strip()
            )
        except Exception:
            ctx = ""
        ctx_hint = f" (docker context: {ctx})" if ctx else ""
        raise SystemExit(
            "Docker engine is not reachable" + ctx_hint + ". Start Docker Desktop (macOS) "
            "or your Docker engine, then retry."
        )


def main() -> None:
    if not shutil.which("docker"):
        raise SystemExit("`docker` not found on PATH.")

    _require_docker_engine()
    load_localstack_env()
    subprocess.run(["docker", "compose", "up", "-d"], cwd=PROJECT_ROOT, check=True)

    deadline = time.time() + 120
    while time.time() < deadline:
        health = _get_health()
        if health:
            print(json.dumps(health, indent=2, default=str))
            _ensure_site_bucket()
            return
        time.sleep(1)

    raise SystemExit(
        "LocalStack did not become ready within 120s. "
        "Check Docker logs: `docker logs fomc-localstack`."
    )


if __name__ == "__main__":
    main()
