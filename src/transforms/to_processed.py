"""Parse raw BLS + DataUSA objects into simple "processed" CSVs.

This is intentionally lightweight (stdlib-only) so it can run locally against
LocalStack or AWS without Spark/pandas. It demonstrates separation between:

  1) Raw ingestion (download + land bytes)
  2) Parsing/cleaning (raw → processed)

Usage:
  source .env.localstack
  python -m src.transforms.to_processed
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from typing import Any

from botocore.exceptions import ClientError

from src.config import (
    get_bls_bucket,
    get_bls_key,
    get_bls_processed_bucket,
    get_bls_series_list,
    get_datausa_bucket,
    get_datausa_datasets,
    get_datausa_key,
    get_datausa_processed_bucket,
)
from src.helpers.aws_client import get_client


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", "")).strip()


def _is_missing_key(exc: ClientError) -> bool:
    return _error_code(exc) in {"404", "NoSuchKey", "NotFound"}


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


def _build_population_processed_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
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


def _build_datausa_jsonrecords_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_rows = payload.get("data", [])
    if not isinstance(raw_rows, list):
        return []

    out: list[dict[str, Any]] = []
    for r in raw_rows:
        if not isinstance(r, dict):
            continue
        out.append(_clean_row(r))
    return out


def _fieldnames_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    keys: set[str] = set()
    for r in rows:
        keys.update([k for k in r.keys() if isinstance(k, str) and k])
    if not keys:
        return []

    preferred = [
        "Year",
        "Nation",
        "State",
        "County",
        "Metropolitan Area",
        "Citizenship Status",
        "Population",
        "Value",
    ]
    ordered = [k for k in preferred if k in keys]
    for k in sorted(keys):
        if k not in ordered:
            ordered.append(k)
    return ordered


def _read_tsv_dicts(text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows: list[dict[str, Any]] = []
    for r in reader:
        rows.append(_clean_row(r))
    return rows


def to_processed(
    *,
    bls_raw_bucket: str,
    bls_raw_key: str,
    bls_processed_bucket: str,
    bls_processed_key: str,
    datausa_raw_bucket: str,
    datausa_raw_key: str,
    datausa_processed_bucket: str,
    datausa_processed_key: str,
) -> dict[str, Any]:
    s3 = get_client("s3")

    # DataUSA: JSON → CSV
    pop_obj = s3.get_object(Bucket=datausa_raw_bucket, Key=datausa_raw_key)
    pop_payload = json.loads(pop_obj["Body"].read())
    pop_rows = _build_population_processed_rows(pop_payload)
    _write_csv_to_s3(
        s3=s3,
        bucket=datausa_processed_bucket,
        key=datausa_processed_key,
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
        bucket=bls_processed_bucket,
        key=bls_processed_key,
        rows=bls_rows,
        fieldnames=fieldnames,
    )

    return {
        "bls": {
            "raw": f"s3://{bls_raw_bucket}/{bls_raw_key}",
            "processed": f"s3://{bls_processed_bucket}/{bls_processed_key}",
            "rows": len(bls_rows),
            "columns": fieldnames,
        },
        "datausa": {
            "raw": f"s3://{datausa_raw_bucket}/{datausa_raw_key}",
            "processed": f"s3://{datausa_processed_bucket}/{datausa_processed_key}",
            "rows": len(pop_rows),
            "columns": ["Year", "Nation", "Population"],
        },
    }


def to_processed_multi(
    *,
    bls_raw_bucket: str,
    bls_processed_bucket: str,
    datausa_raw_bucket: str,
    datausa_processed_bucket: str,
    bls_keys: list[str],
    datausa_keys: list[tuple[str, str]],
) -> dict[str, Any]:
    """Convert multiple raw objects to processed CSVs.

    Args:
        bls_keys: Raw BLS keys to parse (TSV).
        datausa_keys: Tuples of (dataset_id, raw_key) to parse (JSONRecords).
    """
    s3 = get_client("s3")

    out: dict[str, Any] = {"bls": {}, "datausa": {}, "errors": []}

    # DataUSA: JSON → CSV (generic jsonrecords)
    for dataset_id, raw_key in datausa_keys:
        try:
            obj = s3.get_object(Bucket=datausa_raw_bucket, Key=raw_key)
            payload = json.loads(obj["Body"].read())
        except ClientError as exc:
            if _is_missing_key(exc):
                out["errors"].append({
                    "source": "datausa",
                    "dataset_id": dataset_id,
                    "raw": f"s3://{datausa_raw_bucket}/{raw_key}",
                    "error": "missing_raw_key",
                })
                continue
            raise

        if dataset_id == "population":
            rows = _build_population_processed_rows(payload)
            fieldnames = ["Year", "Nation", "Population"]
            out_key = "population.csv"
        else:
            rows = _build_datausa_jsonrecords_rows(payload)
            fieldnames = _fieldnames_for_rows(rows)
            out_key = f"{dataset_id}.csv"

        _write_csv_to_s3(
            s3=s3,
            bucket=datausa_processed_bucket,
            key=out_key,
            rows=rows,
            fieldnames=fieldnames or [],
        )
        out["datausa"][dataset_id] = {
            "raw": f"s3://{datausa_raw_bucket}/{raw_key}",
            "processed": f"s3://{datausa_processed_bucket}/{out_key}",
            "rows": len(rows),
            "columns": fieldnames,
        }

    # BLS: TSV → CSV
    for raw_key in bls_keys:
        try:
            obj = s3.get_object(Bucket=bls_raw_bucket, Key=raw_key)
            text = obj["Body"].read().decode("utf-8", errors="replace")
        except ClientError as exc:
            if _is_missing_key(exc):
                out["errors"].append({
                    "source": "bls",
                    "raw": f"s3://{bls_raw_bucket}/{raw_key}",
                    "error": "missing_raw_key",
                })
                continue
            raise
        rows = _read_tsv_dicts(text)
        fieldnames = list(rows[0].keys()) if rows else ["series_id", "year", "period", "value"]
        out_key = f"{raw_key}.csv"
        _write_csv_to_s3(
            s3=s3,
            bucket=bls_processed_bucket,
            key=out_key,
            rows=rows,
            fieldnames=fieldnames,
        )
        out["bls"][raw_key] = {
            "raw": f"s3://{bls_raw_bucket}/{raw_key}",
            "processed": f"s3://{bls_processed_bucket}/{out_key}",
            "rows": len(rows),
            "columns": fieldnames,
        }

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bls-key", default=get_bls_key(), help="Raw BLS S3 key to parse")
    parser.add_argument("--datausa-key", default=get_datausa_key(), help="Raw DataUSA S3 key to parse")
    parser.add_argument(
        "--bls-out-key",
        default=None,
        help="Output key in the BLS processed bucket (default: <bls-key>.csv)",
    )
    parser.add_argument(
        "--datausa-out-key",
        default="population.csv",
        help="Output key in the DataUSA processed bucket",
    )
    parser.add_argument(
        "--bls-series",
        default="",
        help="Optional comma-separated BLS series ids to parse (overrides --bls-key)",
    )
    parser.add_argument(
        "--datausa-datasets",
        default="",
        help="Optional comma-separated DataUSA dataset ids to parse (overrides --datausa-key)",
    )
    args = parser.parse_args()

    bls_series = [s.strip() for s in str(args.bls_series).split(",") if s.strip()]
    datausa_datasets = [d.strip() for d in str(args.datausa_datasets).split(",") if d.strip()]

    if bls_series or datausa_datasets:
        # Multi mode.
        bls_keys = [f"{sid}/{sid}.data.0.Current" for sid in (bls_series or get_bls_series_list())]

        # Population can be overridden via DATAUSA_KEY; others follow convention.
        dataset_ids = datausa_datasets or get_datausa_datasets()
        datausa_keys: list[tuple[str, str]] = []
        for dataset_id in dataset_ids:
            if dataset_id == "population":
                datausa_keys.append((dataset_id, get_datausa_key()))
            else:
                datausa_keys.append((dataset_id, f"{dataset_id}.json"))

        summary = to_processed_multi(
            bls_raw_bucket=get_bls_bucket(),
            bls_processed_bucket=get_bls_processed_bucket(),
            datausa_raw_bucket=get_datausa_bucket(),
            datausa_processed_bucket=get_datausa_processed_bucket(),
            bls_keys=bls_keys,
            datausa_keys=datausa_keys,
        )
        print(json.dumps(summary, indent=2, default=str))
        return

    bls_key = args.bls_key
    datausa_key = args.datausa_key
    bls_out_key = args.bls_out_key or f"{bls_key}.csv"

    summary = to_processed(
        bls_raw_bucket=get_bls_bucket(),
        bls_raw_key=bls_key,
        bls_processed_bucket=get_bls_processed_bucket(),
        bls_processed_key=bls_out_key,
        datausa_raw_bucket=get_datausa_bucket(),
        datausa_raw_key=datausa_key,
        datausa_processed_bucket=get_datausa_processed_bucket(),
        datausa_processed_key=args.datausa_out_key,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
