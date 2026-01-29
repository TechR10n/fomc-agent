"""DataUSA API fetcher with content-based change detection."""

import hashlib
import json
from datetime import datetime, timezone

import requests

from src.helpers.aws_client import get_client

API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)
BUCKET_NAME = "fomc-datausa-raw"
DATA_KEY = "population.json"


def compute_content_hash(data: dict) -> str:
    """Create deterministic hash of API response content."""
    content = json.dumps(data, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def fetch_population_data(url: str = API_URL, retries: int = 3) -> dict:
    """Fetch population data from DataUSA API with retry logic."""
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            last_error = e
            if attempt < retries - 1:
                import time
                time.sleep(2 ** attempt)
    raise last_error


def needs_update(new_hash: str, s3_metadata: dict) -> bool:
    """Compare content hash with stored hash."""
    stored_hash = s3_metadata.get("content_hash")
    return new_hash != stored_hash


def get_s3_metadata(s3_client, bucket: str, key: str) -> dict:
    """Get metadata for an S3 object, or empty dict if not found."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response.get("Metadata", {})
    except s3_client.exceptions.ClientError:
        return {}


def load_sync_state(s3_client, bucket: str) -> dict:
    """Load the latest sync state from S3."""
    key = "_sync_state/latest_state.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())
    except Exception:
        return {}


def save_sync_state(s3_client, bucket: str, state: dict):
    """Save sync state to S3 atomically."""
    state_key = "_sync_state/latest_state.json"
    temp_key = "_sync_state/_tmp_state.json"

    body = json.dumps(state, indent=2, default=str)
    s3_client.put_object(Bucket=bucket, Key=temp_key, Body=body.encode())
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": temp_key},
        Key=state_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=temp_key)


def append_sync_log(s3_client, bucket: str, entry: dict):
    """Append an entry to the sync log JSONL file."""
    log_key = "_sync_state/sync_log.jsonl"
    existing = ""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=log_key)
        existing = response["Body"].read().decode()
    except Exception:
        pass
    line = json.dumps(entry, default=str) + "\n"
    s3_client.put_object(Bucket=bucket, Key=log_key, Body=(existing + line).encode())


def sync_population_data(bucket: str = BUCKET_NAME) -> dict:
    """Fetch DataUSA population data and sync to S3.

    Returns a summary of the action taken.
    """
    s3 = get_client("s3")
    now = datetime.now(timezone.utc)

    data = fetch_population_data()
    content_hash = compute_content_hash(data)

    metadata = get_s3_metadata(s3, bucket, DATA_KEY)

    if not needs_update(content_hash, metadata):
        log_entry = {
            "timestamp": now.isoformat(),
            "action": "unchanged",
            "content_hash": content_hash,
        }
        append_sync_log(s3, bucket, log_entry)
        return {"action": "unchanged", "content_hash": content_hash}

    # Upload data
    body = json.dumps(data, indent=2)
    s3.put_object(
        Bucket=bucket,
        Key=DATA_KEY,
        Body=body.encode(),
        ContentType="application/json",
        Metadata={"content_hash": content_hash},
    )

    # Compute stats
    records = data.get("data", [])
    record_count = len(records)
    years = [r.get("Year", 0) for r in records if r.get("Year")]
    year_range = [min(years), max(years)] if years else []

    # Update state
    state = {
        "last_sync": now.isoformat(),
        "content_hash": content_hash,
        "record_count": record_count,
        "year_range": year_range,
        "api_url": API_URL,
    }
    save_sync_state(s3, bucket, state)

    log_entry = {
        "timestamp": now.isoformat(),
        "action": "updated",
        "content_hash": content_hash,
        "record_count": record_count,
        "max_year": max(years) if years else None,
    }
    append_sync_log(s3, bucket, log_entry)

    return {"action": "updated", "content_hash": content_hash, "record_count": record_count}


if __name__ == "__main__":
    result = sync_population_data()
    print(json.dumps(result, indent=2))
