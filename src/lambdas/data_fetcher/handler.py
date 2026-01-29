"""Lambda handler for BLS + DataUSA data fetching."""

import json
import os
import sys

# Add project root to path for Lambda deployment
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.data_fetchers.bls_getter import sync_all as sync_bls
from src.data_fetchers.datausa_getter import sync_population_data


def handler(event, context):
    """Lambda entry point: fetch BLS and DataUSA data."""
    bls_bucket = os.environ.get("BLS_BUCKET", "fomc-bls-raw")
    datausa_bucket = os.environ.get("DATAUSA_BUCKET", "fomc-datausa-raw")
    bls_series = os.environ.get("BLS_SERIES", "pr").split(",")

    results = {"bls": None, "datausa": None, "errors": []}

    # Fetch BLS data
    try:
        results["bls"] = sync_bls(series_list=bls_series, bucket=bls_bucket)
    except Exception as e:
        results["errors"].append({"source": "bls", "error": str(e)})

    # Fetch DataUSA data
    try:
        results["datausa"] = sync_population_data(bucket=datausa_bucket)
    except Exception as e:
        results["errors"].append({"source": "datausa", "error": str(e)})

    status = 200 if not results["errors"] else 207

    return {
        "statusCode": status,
        "body": json.dumps(results, default=str),
    }
