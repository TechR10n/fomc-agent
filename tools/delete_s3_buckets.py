"""Delete S3 buckets programmatically (empties first).

Safe by default: this script only performs deletions when --yes is provided.
Supports LocalStack via AWS_ENDPOINT_URL.

Examples:
  # Delete legacy "silver" buckets from AWS (derived from FOMC_BUCKET_PREFIX)
  source .env.local
  python tools/delete_s3_buckets.py --legacy-silver --yes

  # Delete explicit buckets (AWS or LocalStack)
  source .env.localstack
  python tools/delete_s3_buckets.py --bucket fomc-bls-silver --bucket fomc-datausa-silver --yes
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable

from botocore.exceptions import ClientError

from src.helpers.aws_client import get_client


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", "")).strip()


def _bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = _error_code(exc)
        if code in {"404", "NoSuchBucket", "NotFound"}:
            return False
        raise


def _iter_unversioned_objects(s3, *, bucket: str) -> Iterable[dict[str, str]]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            if isinstance(key, str) and key:
                yield {"Key": key}


def _iter_versioned_objects(s3, *, bucket: str) -> Iterable[dict[str, str]]:
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket):
        for ver in page.get("Versions", []) or []:
            key = ver.get("Key")
            version_id = ver.get("VersionId")
            if isinstance(key, str) and key and isinstance(version_id, str) and version_id:
                yield {"Key": key, "VersionId": version_id}
        for marker in page.get("DeleteMarkers", []) or []:
            key = marker.get("Key")
            version_id = marker.get("VersionId")
            if isinstance(key, str) and key and isinstance(version_id, str) and version_id:
                yield {"Key": key, "VersionId": version_id}


def _raise_on_delete_errors(resp: dict) -> None:
    errors = resp.get("Errors") or []
    if not errors:
        return
    first = errors[0]
    code = first.get("Code", "Unknown")
    msg = first.get("Message", "Unknown error")
    key = first.get("Key", "")
    raise RuntimeError(f"S3 delete_objects failed: {code} — {msg} (example key: {key})")


def _delete_objects_batched(s3, *, bucket: str, objects: Iterable[dict[str, str]]) -> int:
    batch: list[dict[str, str]] = []
    deleted = 0

    for obj in objects:
        batch.append(obj)
        if len(batch) >= 1000:
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
            _raise_on_delete_errors(resp)
            deleted += len(batch)
            batch = []

    if batch:
        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
        _raise_on_delete_errors(resp)
        deleted += len(batch)

    return deleted


def empty_bucket(s3, *, bucket: str) -> int:
    """Delete all objects from the bucket. Returns number of delete attempts."""
    versioning = s3.get_bucket_versioning(Bucket=bucket).get("Status")
    if versioning in {"Enabled", "Suspended"}:
        return _delete_objects_batched(s3, bucket=bucket, objects=_iter_versioned_objects(s3, bucket=bucket))
    return _delete_objects_batched(s3, bucket=bucket, objects=_iter_unversioned_objects(s3, bucket=bucket))


def delete_bucket(s3, *, bucket: str) -> None:
    s3.delete_bucket(Bucket=bucket)


def _resolve_legacy_silver_buckets(bucket_prefix: str) -> list[str]:
    prefix = bucket_prefix.strip()
    if not prefix:
        raise ValueError("bucket_prefix is empty")
    return [f"{prefix}-bls-silver", f"{prefix}-datausa-silver"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", action="append", default=[], help="S3 bucket to delete (repeatable)")
    parser.add_argument(
        "--legacy-silver",
        action="store_true",
        help="Also delete legacy *-silver buckets derived from FOMC_BUCKET_PREFIX",
    )
    parser.add_argument(
        "--bucket-prefix",
        default=None,
        help="Bucket prefix to use for --legacy-silver (defaults to env FOMC_BUCKET_PREFIX)",
    )
    parser.add_argument(
        "--ignore-missing",
        action="store_true",
        help="Skip buckets that do not exist instead of failing",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete (otherwise prints the plan and exits)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)

    buckets: list[str] = []
    buckets.extend([str(b).strip() for b in (args.bucket or []) if str(b).strip()])

    if args.legacy_silver:
        bucket_prefix = args.bucket_prefix or os.environ.get("FOMC_BUCKET_PREFIX")
        if not bucket_prefix:
            raise SystemExit("--legacy-silver requires --bucket-prefix or env FOMC_BUCKET_PREFIX")
        buckets.extend(_resolve_legacy_silver_buckets(bucket_prefix))

    # Dedupe while keeping order.
    seen: set[str] = set()
    buckets = [b for b in buckets if not (b in seen or seen.add(b))]

    if not buckets:
        raise SystemExit("No buckets provided. Use --bucket and/or --legacy-silver.")

    if not args.yes:
        print("Planned deletions (dry-run):")
        for b in buckets:
            print(f"  - s3://{b}")
        print("\nRe-run with --yes to delete.")
        return 0

    s3 = get_client("s3")

    for bucket in buckets:
        print(f"==> Deleting s3://{bucket}")

        try:
            exists = _bucket_exists(s3, bucket)
        except ClientError as exc:
            code = _error_code(exc)
            raise SystemExit(f"head_bucket failed for {bucket}: {code}") from exc

        if not exists:
            msg = f"Bucket does not exist: {bucket}"
            if args.ignore_missing:
                print(f"    - {msg} (skipping)")
                continue
            raise SystemExit(msg)

        try:
            deleted = empty_bucket(s3, bucket=bucket)
            if deleted:
                print(f"    - Deleted {deleted} object(s)/version(s)")
            delete_bucket(s3, bucket=bucket)
            print("    - Bucket deleted")
        except ClientError as exc:
            code = _error_code(exc)
            raise SystemExit(f"Failed to delete {bucket}: {code}") from exc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

