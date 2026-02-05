"""Lambda handler for BLS + DataUSA data fetching."""

import json
import os
import sys

# Add project root to path for Lambda deployment
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from src.data_fetchers.bls_getter import sync_all as sync_bls
from src.data_fetchers.datausa_getter import sync_all as sync_datausa
from src.config import get_bls_bucket, get_datausa_bucket, get_bls_series_list


def handler(event, context):
    """Lambda entry point: fetch BLS and DataUSA data."""
    bls_bucket = get_bls_bucket()
    datausa_bucket = get_datausa_bucket()
    bls_series = get_bls_series_list()

    results = {"bls": None, "datausa": None, "errors": []}

    # Fetch BLS data
    try:
        results["bls"] = sync_bls(series_list=bls_series, bucket=bls_bucket)
    except Exception as e:
        results["errors"].append({"source": "bls", "error": str(e)})

    # Fetch DataUSA data
    try:
        results["datausa"] = sync_datausa(bucket=datausa_bucket)
        for err in results["datausa"].get("errors", []):
            results["errors"].append({"source": "datausa", **err})
    except Exception as e:
        results["errors"].append({"source": "datausa", "error": str(e)})

    status = 200 if not results["errors"] else 207

    return {
        "statusCode": status,
        "body": json.dumps(results, default=str),
    }
