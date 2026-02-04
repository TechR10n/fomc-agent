"""Lambda handler for analytics processing triggered by SQS.

Uses Python stdlib csv/json (no PySpark) for Lambda compatibility.
"""

import csv
import io
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.config import get_bls_bucket, get_bls_key, get_datausa_bucket, get_datausa_key
from src.helpers.aws_client import get_client

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """Process SQS messages triggered by S3 uploads."""
    bls_bucket = get_bls_bucket()
    datausa_bucket = get_datausa_bucket()
    bls_key = get_bls_key()
    pop_key = get_datausa_key()
    join_series_id = os.environ.get("ANALYTICS_SERIES_ID", "PRS30006032")
    join_period = os.environ.get("ANALYTICS_PERIOD", "Q01")

    results = []
    errors = []

    for record in event.get("Records", []):
        try:
            body = json.loads(record.get("body", "{}"))
            # S3 notification format
            s3_records = body.get("Records", [])
            for s3_record in s3_records:
                bucket_name = s3_record.get("s3", {}).get("bucket", {}).get("name", "")
                key = s3_record.get("s3", {}).get("object", {}).get("key", "")
                logger.info(f"Processing S3 event: {bucket_name}/{key}")

            report = run_reports(
                bls_bucket,
                datausa_bucket,
                bls_key=bls_key,
                pop_key=pop_key,
                join_series_id=join_series_id,
                join_period=join_period,
            )
            results.append(report)
            logger.info(f"Report results: {json.dumps(report, default=str)}")
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            errors.append(str(e))

    status = 200 if not errors else 207
    return {
        "statusCode": status,
        "body": json.dumps({"results": results, "errors": errors}, default=str),
    }


def run_reports(
    bls_bucket: str,
    datausa_bucket: str,
    *,
    bls_key: str,
    pop_key: str,
    join_series_id: str,
    join_period: str,
) -> dict:
    """Run all three reports using stdlib csv/json."""
    s3 = get_client("s3")

    # Load population data
    pop_data = load_population(s3, datausa_bucket, pop_key)

    # Load BLS data
    bls_data = load_bls_data(s3, bls_bucket, bls_key)

    return {
        "report_1": report_population_stats(pop_data),
        "report_2": report_best_year(bls_data),
        "report_3": report_series_population(
            bls_data,
            pop_data,
            series_id=join_series_id,
            period=join_period,
        ),
    }


def load_population(s3_client, bucket: str, key: str) -> list[dict]:
    """Load population JSON from S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read())
    return data.get("data", [])


def load_bls_data(s3_client, bucket: str, key: str) -> list[dict]:
    """Load BLS CSV from S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    rows = []
    for row in reader:
        cleaned = {k.strip(): v.strip() if v else v for k, v in row.items()}
        rows.append(cleaned)
    return rows


def report_population_stats(pop_data: list[dict]) -> dict:
    """Report 1: Mean and std dev of US population for 2013-2018."""
    populations = [
        int(r["Population"])
        for r in pop_data
        if 2013 <= int(r.get("Year", 0)) <= 2018
    ]

    if not populations:
        return {"mean": None, "stddev": None}

    n = len(populations)
    mean = sum(populations) / n
    variance = sum((p - mean) ** 2 for p in populations) / (n - 1) if n > 1 else 0
    stddev = variance ** 0.5

    return {
        "report": "Population Statistics (2013-2018)",
        "mean": mean,
        "stddev": stddev,
    }


def report_best_year(bls_data: list[dict]) -> list[dict]:
    """Report 2: Best year per series_id."""
    # Sum values per series_id + year (quarterly only)
    sums = {}
    for row in bls_data:
        period = row.get("period", "")
        if not period.startswith("Q"):
            continue
        series_id = row.get("series_id", "")
        year = row.get("year", "")
        try:
            value = float(row.get("value", 0))
        except (ValueError, TypeError):
            continue

        key = (series_id, year)
        sums[key] = sums.get(key, 0) + value

    # Find best year per series
    best = {}
    for (series_id, year), total in sums.items():
        if series_id not in best or total > best[series_id][1]:
            best[series_id] = (year, total)

    return [
        {"series_id": sid, "year": int(year), "value": round(val, 1)}
        for sid, (year, val) in sorted(best.items())
    ]


def report_series_population(
    bls_data: list[dict],
    pop_data: list[dict],
    *,
    series_id: str = "PRS30006032",
    period: str = "Q01",
) -> list[dict]:
    """Report 3: Join one BLS series with population."""
    pop_by_year = {int(r["Year"]): int(r["Population"]) for r in pop_data}

    results = []
    for row in bls_data:
        if row.get("series_id") != series_id or row.get("period") != period:
            continue
        year = int(row.get("year", 0))
        value = row.get("value", "")
        population = pop_by_year.get(year)
        results.append({
            "series_id": series_id,
            "year": year,
            "period": period,
            "value": value,
            "Population": population,
        })

    return sorted(results, key=lambda r: r["year"])
