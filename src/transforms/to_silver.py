"""Parse raw BLS + DataUSA objects into simple "silver" CSVs.

This is intentionally lightweight (stdlib-only) so it can run locally against
LocalStack or AWS without Spark/pandas. It demonstrates separation between:

  1) Raw ingestion (download + land bytes)
  2) Parsing/cleaning (raw → silver)

Usage:
  source .env.localstack
  python -m src.transforms.to_silver
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from typing import Any

from src.config import (
    get_bls_bucket,
    get_bls_key,
    get_bls_silver_bucket,
    get_datausa_bucket,
    get_datausa_key,
    get_datausa_silver_bucket,
)
from src.helpers.aws_client import get_client


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for k, v in row.items():
        if k is None:
            continue
        key = str(k).strip()
        if v is None:
            cleaned[key] = None
        else:
            cleaned[key] = str(v).strip()
    return cleaned


def _write_csv_to_s3(
    *,
    s3,
    bucket: str,
    key: str,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def _build_population_silver_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_rows = payload.get("data", [])
    if not isinstance(raw_rows, list):
        return []

    out: list[dict[str, Any]] = []
    for r in raw_rows:
        if not isinstance(r, dict):
            continue
        cleaned = _clean_row(r)
        out.append({
            "Year": cleaned.get("Year"),
            "Nation": cleaned.get("Nation"),
            "Population": cleaned.get("Population"),
        })
    return out


def _read_tsv_dicts(text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows: list[dict[str, Any]] = []
    for r in reader:
        rows.append(_clean_row(r))
    return rows


def to_silver(
    *,
    bls_raw_bucket: str,
    bls_raw_key: str,
    bls_silver_bucket: str,
    bls_silver_key: str,
    datausa_raw_bucket: str,
    datausa_raw_key: str,
    datausa_silver_bucket: str,
    datausa_silver_key: str,
) -> dict[str, Any]:
    s3 = get_client("s3")

    # DataUSA: JSON → CSV
    pop_obj = s3.get_object(Bucket=datausa_raw_bucket, Key=datausa_raw_key)
    pop_payload = json.loads(pop_obj["Body"].read())
    pop_rows = _build_population_silver_rows(pop_payload)
    _write_csv_to_s3(
        s3=s3,
        bucket=datausa_silver_bucket,
        key=datausa_silver_key,
        rows=pop_rows,
        fieldnames=["Year", "Nation", "Population"],
    )

    # BLS: TSV → CSV
    bls_obj = s3.get_object(Bucket=bls_raw_bucket, Key=bls_raw_key)
    bls_text = bls_obj["Body"].read().decode("utf-8", errors="replace")
    bls_rows = _read_tsv_dicts(bls_text)
    fieldnames = list(bls_rows[0].keys()) if bls_rows else ["series_id", "year", "period", "value"]
    _write_csv_to_s3(
        s3=s3,
        bucket=bls_silver_bucket,
        key=bls_silver_key,
        rows=bls_rows,
        fieldnames=fieldnames,
    )

    return {
        "bls": {
            "raw": f"s3://{bls_raw_bucket}/{bls_raw_key}",
            "silver": f"s3://{bls_silver_bucket}/{bls_silver_key}",
            "rows": len(bls_rows),
            "columns": fieldnames,
        },
        "datausa": {
            "raw": f"s3://{datausa_raw_bucket}/{datausa_raw_key}",
            "silver": f"s3://{datausa_silver_bucket}/{datausa_silver_key}",
            "rows": len(pop_rows),
            "columns": ["Year", "Nation", "Population"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bls-key", default=get_bls_key(), help="Raw BLS S3 key to parse")
    parser.add_argument("--datausa-key", default=get_datausa_key(), help="Raw DataUSA S3 key to parse")
    parser.add_argument(
        "--bls-out-key",
        default=None,
        help="Output key in the BLS silver bucket (default: <bls-key>.csv)",
    )
    parser.add_argument(
        "--datausa-out-key",
        default="population.csv",
        help="Output key in the DataUSA silver bucket",
    )
    args = parser.parse_args()

    bls_key = args.bls_key
    datausa_key = args.datausa_key
    bls_out_key = args.bls_out_key or f"{bls_key}.csv"

    summary = to_silver(
        bls_raw_bucket=get_bls_bucket(),
        bls_raw_key=bls_key,
        bls_silver_bucket=get_bls_silver_bucket(),
        bls_silver_key=bls_out_key,
        datausa_raw_bucket=get_datausa_bucket(),
        datausa_raw_key=datausa_key,
        datausa_silver_bucket=get_datausa_silver_bucket(),
        datausa_silver_key=args.datausa_out_key,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()

