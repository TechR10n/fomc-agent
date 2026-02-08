#!/usr/bin/env python3
"""Check S3 bucket/key naming and presence for the FOMC pipeline.

This script:
- Verifies expected buckets exist (raw, processed, site).
- Checks key presence for raw/processed data outputs.

Usage (AWS):
  source .env.shared
  source .env.local
  python tools/check_s3_assets.py

Usage (LocalStack):
  source .env.shared
  source .env.localstack
  python tools/check_s3_assets.py
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable
from pathlib import Path

from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (
    get_bls_bucket,
    get_bls_processed_bucket,
    get_bls_series_list,
    get_bucket_prefix,
    get_datausa_bucket,
    get_datausa_datasets,
    get_datausa_key,
    get_datausa_processed_bucket,
)
from src.helpers.aws_client import get_client


def _load_env_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"Env file not found: {path}")
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", "")).strip()


def _bucket_exists(s3, bucket: str) -> tuple[bool, str | None]:
    try:
        s3.head_bucket(Bucket=bucket)
        return True, None
    except ClientError as exc:
        code = _error_code(exc)
        if code in {"404", "NoSuchBucket", "NotFound"}:
            return False, None
        return False, code or "UnknownError"


def _object_exists(s3, *, bucket: str, key: str) -> tuple[bool, str | None]:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True, None
    except ClientError as exc:
        code = _error_code(exc)
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False, None
        return False, code or "UnknownError"


def _list_buckets(s3) -> tuple[list[str], str | None]:
    try:
        buckets = s3.list_buckets().get("Buckets", [])
        return [b["Name"] for b in buckets if "Name" in b], None
    except ClientError as exc:
        return [], _error_code(exc) or "UnknownError"


def _dedupe(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            out.append(item)
            seen.add(item)
    return out


def _expected_bls_raw_keys(series_list: list[str]) -> list[str]:
    return [f"{sid}/{sid}.data.0.Current" for sid in series_list]


def _expected_datausa_raw_keys(dataset_ids: list[str]) -> list[str]:
    keys: list[str] = []
    for dataset_id in dataset_ids:
        if dataset_id == "population":
            keys.append(get_datausa_key())
        else:
            keys.append(f"{dataset_id}.json")
    return keys


def _expected_bls_processed_keys(series_list: list[str]) -> list[str]:
    return [f"{key}.csv" for key in _expected_bls_raw_keys(series_list)]


def _expected_datausa_processed_keys(dataset_ids: list[str]) -> list[str]:
    keys: list[str] = []
    for dataset_id in dataset_ids:
        if dataset_id == "population":
            keys.append("population.csv")
        else:
            keys.append(f"{dataset_id}.csv")
    return keys


def _print_header(title: str) -> None:
    print(f"\n==> {title}")


def _print_ok(message: str) -> None:
    print(f"[OK] {message}")


def _print_warn(message: str) -> None:
    print(f"[WARN] {message}")


def _print_error(message: str) -> None:
    print(f"[ERROR] {message}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file to load (e.g., .env.shared or .env.localstack)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit codes when warnings are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

    if args.env_file:
        _load_env_file(Path(args.env_file))

    s3 = get_client("s3")
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "").strip()
    prefix = get_bucket_prefix()

    _print_header("Environment")
    _print_ok(f"AWS endpoint: {endpoint or 'AWS (default)'}")
    _print_ok(f"Bucket prefix: {prefix}")

    if not endpoint and prefix == "fomc":
        _print_warn("FOMC_BUCKET_PREFIX is default 'fomc' (consider a unique prefix for AWS).")

    expected_buckets = {
        "BLS raw": get_bls_bucket(),
        "DataUSA raw": get_datausa_bucket(),
        "BLS processed": get_bls_processed_bucket(),
        "DataUSA processed": get_datausa_processed_bucket(),
        "Site": f"{prefix}-site",
    }

    expected_bucket_names = set(expected_buckets.values())
    warnings = 0
    errors = 0

    _print_header("Buckets")
    for label, bucket in expected_buckets.items():
        exists, err = _bucket_exists(s3, bucket)
        if err:
            _print_error(f"{label}: s3://{bucket} ({err})")
            errors += 1
            continue
        if not exists:
            _print_warn(f"{label}: missing s3://{bucket}")
            warnings += 1
            continue
        _print_ok(f"{label}: s3://{bucket}")
        if not bucket.startswith(f"{prefix}-"):
            _print_warn(f"{label}: bucket does not start with prefix '{prefix}'")
            warnings += 1

    all_buckets, list_err = _list_buckets(s3)
    if list_err:
        _print_warn(f"Could not list buckets (skipping extra bucket checks): {list_err}")
        warnings += 1
    else:
        extra = sorted(
            b for b in all_buckets if b.startswith(f"{prefix}-") and b not in expected_bucket_names
        )
        if extra:
            _print_warn("Extra buckets with prefix detected:")
            for b in extra:
                print(f"  - s3://{b}")
            warnings += 1

    series_list = get_bls_series_list()
    dataset_ids = get_datausa_datasets()

    raw_bls_keys = _dedupe(_expected_bls_raw_keys(series_list))
    raw_datausa_keys = _dedupe(_expected_datausa_raw_keys(dataset_ids))
    processed_bls_keys = _dedupe(_expected_bls_processed_keys(series_list))
    processed_datausa_keys = _dedupe(_expected_datausa_processed_keys(dataset_ids))

    def check_keys(bucket_label: str, bucket: str, keys: list[str]) -> None:
        nonlocal warnings, errors
        exists, err = _bucket_exists(s3, bucket)
        if err:
            _print_error(f"{bucket_label}: cannot access s3://{bucket} ({err})")
            errors += 1
            return
        if not exists:
            _print_warn(f"{bucket_label}: missing s3://{bucket} (skipping key checks)")
            warnings += 1
            return
        for key in keys:
            ok, obj_err = _object_exists(s3, bucket=bucket, key=key)
            if obj_err:
                _print_error(f"{bucket_label}: s3://{bucket}/{key} ({obj_err})")
                errors += 1
            elif not ok:
                _print_warn(f"{bucket_label}: missing s3://{bucket}/{key}")
                warnings += 1
            else:
                _print_ok(f"{bucket_label}: s3://{bucket}/{key}")

    _print_header("Objects (Raw)")
    check_keys("BLS raw", expected_buckets["BLS raw"], raw_bls_keys)
    check_keys("DataUSA raw", expected_buckets["DataUSA raw"], raw_datausa_keys)

    _print_header("Objects (Processed)")
    check_keys("BLS processed", expected_buckets["BLS processed"], processed_bls_keys)
    check_keys("DataUSA processed", expected_buckets["DataUSA processed"], processed_datausa_keys)

    _print_header("Summary")
    print(f"Warnings: {warnings}")
    print(f"Errors: {errors}")

    if errors:
        return 2
    if args.strict and warnings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
