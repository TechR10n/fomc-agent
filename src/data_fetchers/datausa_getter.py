"""DataUSA Tesseract ingestion with rate-limit friendly sync state.

This module fetches DataUSA API payloads and lands them as *raw* JSON objects in
S3. Parsing and enrichment are handled in separate steps.

Rate-limit controls:
- Per-dataset minimum sync interval (skip remote fetch entirely)
- Inter-request delay between datasets
- Retries with exponential backoff and best-effort Retry-After support
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from src.config import get_datausa_bucket, get_datausa_datasets, get_datausa_key
from src.helpers.aws_client import get_client
from src.helpers.http_client import fetch_json

DEFAULT_BASE_URL = "https://api.datausa.io/tesseract"
DEFAULT_LOCALE = "en"


@dataclass(frozen=True)
class DataUsaDataset:
    """A small, repeatable DataUSA pull."""

    dataset_id: str
    cube: str
    drilldowns: list[str]
    measures: list[str]
    description: str = ""
    locale: str = DEFAULT_LOCALE
    key: str | None = None
    min_sync_interval_hours: float | None = None

    def build_url(self, *, base_url: str) -> str:
        endpoint = f"{base_url.rstrip('/')}/data.jsonrecords"
        query = {
            "cube": self.cube,
            "drilldowns": ",".join(self.drilldowns),
            "locale": self.locale,
            "measures": ",".join(self.measures),
        }
        return f"{endpoint}?{urlencode(query)}"

    def raw_key(self) -> str:
        if self.key:
            return self.key
        return f"{self.dataset_id}.json"


def _state_key(dataset_id: str) -> str:
    # IMPORTANT: keep sync state out of the S3 ".json" notification filter.
    return f"_sync_state/datausa/{dataset_id}/latest_state.jsonl"


def _temp_state_key(dataset_id: str) -> str:
    return f"_sync_state/datausa/{dataset_id}/_tmp_state.jsonl"


def _log_key(dataset_id: str) -> str:
    return f"_sync_state/datausa/{dataset_id}/sync_log.jsonl"


def compute_content_hash(data: Any) -> str:
    """Create a deterministic hash of API response content."""
    content = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def _load_state(s3_client, bucket: str, dataset_id: str) -> dict[str, Any]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=_state_key(dataset_id))
        return json.loads(response["Body"].read())
    except Exception:
        return {}


def _save_state(s3_client, bucket: str, dataset_id: str, state: dict[str, Any]) -> None:
    body = json.dumps(state, indent=2, default=str) + "\n"
    s3_client.put_object(Bucket=bucket, Key=_temp_state_key(dataset_id), Body=body.encode("utf-8"))
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": _temp_state_key(dataset_id)},
        Key=_state_key(dataset_id),
    )
    s3_client.delete_object(Bucket=bucket, Key=_temp_state_key(dataset_id))


def _append_log(s3_client, bucket: str, dataset_id: str, entry: dict[str, Any]) -> None:
    existing = ""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=_log_key(dataset_id))
        existing = response["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        pass
    line = json.dumps(entry, default=str) + "\n"
    s3_client.put_object(
        Bucket=bucket,
        Key=_log_key(dataset_id),
        Body=(existing + line).encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def _should_skip_fetch(
    *,
    now: datetime,
    state: dict[str, Any],
    min_sync_interval_hours: float | None,
) -> bool:
    if min_sync_interval_hours is None:
        env = os.environ.get("DATAUSA_MIN_SYNC_HOURS")
        if env:
            try:
                min_sync_interval_hours = float(env)
            except ValueError:
                min_sync_interval_hours = None

    if min_sync_interval_hours is None or min_sync_interval_hours <= 0:
        return False

    if os.environ.get("DATAUSA_FORCE_REFRESH", "").strip().lower() in {"1", "true", "yes"}:
        return False

    last_sync = state.get("last_sync")
    if not last_sync or not isinstance(last_sync, str):
        return False

    try:
        prev = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
    except Exception:
        return False

    if prev.tzinfo is None:
        prev = prev.replace(tzinfo=timezone.utc)

    age_seconds = (now - prev.astimezone(timezone.utc)).total_seconds()
    return age_seconds < min_sync_interval_hours * 3600


def _extract_year_range(records: Any) -> list[int]:
    if not isinstance(records, list) or not records:
        return []
    years: list[int] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        y = r.get("Year")
        try:
            y_int = int(y)
        except Exception:
            continue
        years.append(y_int)
    if not years:
        return []
    return [min(years), max(years)]


def _default_base_url() -> str:
    return os.environ.get("DATAUSA_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _default_datasets() -> dict[str, DataUsaDataset]:
    return {
        "population": DataUsaDataset(
            dataset_id="population",
            cube="acs_yg_total_population_1",
            drilldowns=["Year", "Nation"],
            measures=["Population"],
            description="Annual US population (ACS) for Nation by Year.",
            key=get_datausa_key(),
            # ACS-style cubes update infrequently; default to weekly re-checks.
            min_sync_interval_hours=24 * 7,
        ),
        "commute_time": DataUsaDataset(
            dataset_id="commute_time",
            cube="acs_ygt_mean_transportation_time_to_work_1",
            drilldowns=["Year", "Nation"],
            measures=["Mean Transportation Time to Work"],
            description="Mean commute time (minutes) for Nation by Year.",
            key="commute_time.json",
            min_sync_interval_hours=24 * 30,
        ),
        "citizenship": DataUsaDataset(
            dataset_id="citizenship",
            cube="acs_ygc_citizenship_status_1",
            drilldowns=["Year", "Nation", "Citizenship Status"],
            measures=["Population"],
            description="Population by citizenship status for Nation by Year.",
            key="citizenship.json",
            min_sync_interval_hours=24 * 30,
        ),
    }


def sync_dataset(dataset: DataUsaDataset, *, bucket: str | None = None) -> dict[str, Any]:
    """Sync a single DataUSA dataset to S3 raw."""
    if bucket is None:
        bucket = get_datausa_bucket()

    s3 = get_client("s3")
    now = datetime.now(timezone.utc)
    state = _load_state(s3, bucket, dataset.dataset_id)

    if _should_skip_fetch(now=now, state=state, min_sync_interval_hours=dataset.min_sync_interval_hours):
        entry = {
            "timestamp": now.isoformat(),
            "dataset_id": dataset.dataset_id,
            "action": "skipped",
            "reason": "min_sync_interval",
        }
        _append_log(s3, bucket, dataset.dataset_id, entry)
        return {"action": "skipped", "dataset_id": dataset.dataset_id, "key": dataset.raw_key()}

    url = dataset.build_url(base_url=_default_base_url())
    payload = fetch_json(
        url,
        timeout=int(os.environ.get("DATAUSA_TIMEOUT_SECONDS", "60")),
        retries=int(os.environ.get("DATAUSA_RETRIES", "3")),
        backoff_seconds=float(os.environ.get("DATAUSA_BACKOFF_SECONDS", "1")),
    )
    content_hash = compute_content_hash(payload)

    existing_hash = state.get("content_hash")
    if isinstance(existing_hash, str) and existing_hash == content_hash:
        entry = {
            "timestamp": now.isoformat(),
            "dataset_id": dataset.dataset_id,
            "action": "unchanged",
            "content_hash": content_hash,
        }
        _append_log(s3, bucket, dataset.dataset_id, entry)
        return {
            "action": "unchanged",
            "dataset_id": dataset.dataset_id,
            "key": dataset.raw_key(),
            "content_hash": content_hash,
        }

    body = json.dumps(payload, indent=2, default=str) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=dataset.raw_key(),
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={"content_hash": content_hash},
    )

    records = payload.get("data") if isinstance(payload, dict) else None
    record_count = len(records) if isinstance(records, list) else 0
    year_range = _extract_year_range(records)

    new_state = {
        "dataset_id": dataset.dataset_id,
        "last_sync": now.isoformat().replace("+00:00", "Z"),
        "content_hash": content_hash,
        "record_count": record_count,
        "year_range": year_range,
        "api_url": url,
        "cube": dataset.cube,
        "drilldowns": dataset.drilldowns,
        "measures": dataset.measures,
    }
    _save_state(s3, bucket, dataset.dataset_id, new_state)

    entry = {
        "timestamp": now.isoformat(),
        "dataset_id": dataset.dataset_id,
        "action": "updated",
        "content_hash": content_hash,
        "record_count": record_count,
        "year_range": year_range,
    }
    _append_log(s3, bucket, dataset.dataset_id, entry)

    return {
        "action": "updated",
        "dataset_id": dataset.dataset_id,
        "key": dataset.raw_key(),
        "content_hash": content_hash,
        "record_count": record_count,
        "year_range": year_range,
    }


def sync_all(dataset_ids: list[str] | None = None, *, bucket: str | None = None) -> dict[str, Any]:
    """Sync configured DataUSA datasets to S3 raw."""
    if dataset_ids is None:
        dataset_ids = get_datausa_datasets()
    if bucket is None:
        bucket = get_datausa_bucket()

    delay = float(os.environ.get("DATAUSA_DELAY_SECONDS", "1"))

    catalog = _default_datasets()
    results: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []

    for i, dataset_id in enumerate(dataset_ids):
        spec = catalog.get(dataset_id)
        if spec is None:
            errors.append({"dataset_id": dataset_id, "error": "unknown_dataset"})
            continue
        try:
            results[dataset_id] = sync_dataset(spec, bucket=bucket)
        except Exception as exc:
            errors.append({"dataset_id": dataset_id, "error": str(exc)})
        if delay > 0 and i < len(dataset_ids) - 1:
            time.sleep(delay)

    return {"datasets": results, "errors": errors}


def sync_population_data(bucket: str | None = None) -> dict[str, Any]:
    """Backwards-compatible wrapper for syncing just population.json."""
    if bucket is None:
        bucket = get_datausa_bucket()
    spec = _default_datasets()["population"]
    return sync_dataset(spec, bucket=bucket)


if __name__ == "__main__":
    result = sync_all(bucket=get_datausa_bucket())
    print(json.dumps(result, indent=2, default=str))

