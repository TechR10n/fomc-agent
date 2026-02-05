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
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from src.config import get_datausa_bucket, get_datausa_datasets, get_datausa_key
from src.helpers.aws_client import get_client
from src.helpers.http_client import fetch_json

DEFAULT_BASE_URL = "https://api.datausa.io/tesseract"
DEFAULT_LOCALE = "en"

_VALIDATED_CANDIDATES: dict[str, "DataUsaDataset"] = {}
_VALIDATION_ATTEMPTED: set[str] = set()


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
    fallbacks: tuple["DataUsaDataset", ...] = ()

    def build_url(self, *, base_url: str, limit: int | None = None) -> str:
        endpoint = f"{base_url.rstrip('/')}/data.jsonrecords"
        query = {
            "cube": self.cube,
            "drilldowns": ",".join(self.drilldowns),
            "locale": self.locale,
            "measures": ",".join(self.measures),
        }
        if limit is not None and limit > 0:
            query["limit"] = str(limit)
        return f"{endpoint}?{urlencode(query)}"

    def raw_key(self) -> str:
        if self.key:
            return self.key
        return f"{self.dataset_id}.json"

    def candidates(self) -> list["DataUsaDataset"]:
        seen: set[tuple[str, str, str, str, str]] = set()
        out: list[DataUsaDataset] = []
        for spec in (self,) + tuple(self.fallbacks or ()):
            key = (
                spec.cube,
                ",".join(spec.drilldowns),
                ",".join(spec.measures),
                spec.locale,
                spec.raw_key(),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(spec)
        return out


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


def _validation_enabled() -> bool:
    raw = os.environ.get("DATAUSA_VALIDATE_STARTUP", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _candidate_order(dataset: DataUsaDataset) -> list[DataUsaDataset]:
    preferred = _VALIDATED_CANDIDATES.get(dataset.dataset_id)
    if preferred is None:
        return dataset.candidates()
    remaining = [spec for spec in dataset.candidates() if spec != preferred]
    return [preferred] + remaining


def _validate_dataset_candidates(dataset: DataUsaDataset) -> None:
    if not _validation_enabled():
        return
    if dataset.dataset_id in _VALIDATION_ATTEMPTED:
        return
    _VALIDATION_ATTEMPTED.add(dataset.dataset_id)
    if not dataset.fallbacks:
        return

    timeout = int(os.environ.get("DATAUSA_VALIDATE_TIMEOUT_SECONDS", "10"))
    retries = int(os.environ.get("DATAUSA_VALIDATE_RETRIES", "1"))
    backoff_seconds = float(os.environ.get("DATAUSA_VALIDATE_BACKOFF_SECONDS", "0.5"))
    limit_raw = os.environ.get("DATAUSA_VALIDATE_LIMIT", "1")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 1
    if limit <= 0:
        limit = None

    base_url = _default_base_url()

    for spec in dataset.candidates():
        url = spec.build_url(base_url=base_url, limit=limit)
        try:
            fetch_json(
                url,
                headers={"User-Agent": "fomc-agent/1.0 (economic-data-fetcher)"},
                timeout=timeout,
                retries=retries,
                backoff_seconds=backoff_seconds,
            )
            _VALIDATED_CANDIDATES[dataset.dataset_id] = spec
            return
        except urllib.error.HTTPError as exc:
            status = int(getattr(exc, "code", 0) or 0)
            if status in {400, 404}:
                continue
            # Transient or auth errors: skip validation and let the main fetch handle retries.
            return
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            return


def _parse_env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if not raw:
        return list(default)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _default_datasets() -> dict[str, DataUsaDataset]:
    citizenship_cube = os.environ.get("DATAUSA_CITIZENSHIP_CUBE", "acs_ygc_citizenship_status_1")
    citizenship_drilldowns = _parse_env_list(
        "DATAUSA_CITIZENSHIP_DRILLDOWNS",
        ["Year", "Nation", "Citizenship Status"],
    )
    citizenship_measures = _parse_env_list(
        "DATAUSA_CITIZENSHIP_MEASURES",
        ["Population"],
    )

    def _fallbacks_for_citizenship() -> tuple[DataUsaDataset, ...]:
        fallbacks: list[DataUsaDataset] = []

        drilldown_candidates: list[list[str]] = [citizenship_drilldowns]
        if any(d.lower() == "nation" for d in citizenship_drilldowns):
            no_nation = [d for d in citizenship_drilldowns if d.lower() != "nation"]
            drilldown_candidates.append(no_nation)

        if any(d.lower() == "citizenship status" for d in citizenship_drilldowns):
            alt = [
                ("Citizenship" if d.lower() == "citizenship status" else d)
                for d in citizenship_drilldowns
            ]
            drilldown_candidates.append(alt)
            if any(d.lower() == "nation" for d in alt):
                drilldown_candidates.append([d for d in alt if d.lower() != "nation"])

        cube_candidates = [citizenship_cube]
        if "acs_ygc_" in citizenship_cube:
            cube_candidates.append(citizenship_cube.replace("acs_ygc_", "acs_yg_", 1))

        for cube in cube_candidates:
            for drilldowns in drilldown_candidates:
                if cube == citizenship_cube and drilldowns == citizenship_drilldowns:
                    continue
                fallbacks.append(
                    DataUsaDataset(
                        dataset_id="citizenship",
                        cube=cube,
                        drilldowns=drilldowns,
                        measures=citizenship_measures,
                        description="Population by citizenship status (fallback).",
                        key="citizenship.json",
                        min_sync_interval_hours=24 * 30,
                    )
                )

        return tuple(fallbacks)
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
            cube=citizenship_cube,
            drilldowns=citizenship_drilldowns,
            measures=citizenship_measures,
            description="Population by citizenship status for Nation by Year.",
            key="citizenship.json",
            min_sync_interval_hours=24 * 30,
            fallbacks=_fallbacks_for_citizenship(),
        ),
    }


def _fetch_dataset_payload(dataset: DataUsaDataset) -> tuple[DataUsaDataset, Any, str]:
    candidates = _candidate_order(dataset)
    if not candidates:
        raise RuntimeError(f"DataUSA dataset {dataset.dataset_id} has no candidates")

    timeout = int(os.environ.get("DATAUSA_TIMEOUT_SECONDS", "60"))
    retries = int(os.environ.get("DATAUSA_RETRIES", "3"))
    backoff_seconds = float(os.environ.get("DATAUSA_BACKOFF_SECONDS", "1"))
    base_url = _default_base_url()
    last_error: Exception | None = None

    for spec in candidates:
        url = spec.build_url(base_url=base_url)
        try:
            payload = fetch_json(
                url,
                headers={"User-Agent": "fomc-agent/1.0 (economic-data-fetcher)"},
                timeout=timeout,
                retries=retries,
                backoff_seconds=backoff_seconds,
            )
            return spec, payload, url
        except urllib.error.HTTPError as exc:
            last_error = exc
            status = int(getattr(exc, "code", 0) or 0)
            if status in {400, 404}:
                continue
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            break

    summary = "; ".join(
        f"cube={spec.cube} drilldowns={','.join(spec.drilldowns)} measures={','.join(spec.measures)}"
        for spec in candidates
    )
    if last_error is None:
        raise RuntimeError(f"DataUSA fetch failed for {dataset.dataset_id}: no attempts made")
    if isinstance(last_error, urllib.error.HTTPError):
        status = int(getattr(last_error, "code", 0) or 0)
        reason = getattr(last_error, "reason", "")
        raise RuntimeError(
            f"DataUSA fetch failed for {dataset.dataset_id}: HTTP {status} {reason} "
            f"after trying {len(candidates)} candidate specs ({summary})"
        ) from last_error
    raise RuntimeError(
        f"DataUSA fetch failed for {dataset.dataset_id}: {last_error} "
        f"after trying {len(candidates)} candidate specs ({summary})"
    ) from last_error


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

    _validate_dataset_candidates(dataset)

    spec, payload, url = _fetch_dataset_payload(dataset)
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
            "key": spec.raw_key(),
            "content_hash": content_hash,
            "cube": spec.cube,
            "drilldowns": spec.drilldowns,
            "measures": spec.measures,
        }

    body = json.dumps(payload, indent=2, default=str) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=spec.raw_key(),
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
        "cube": spec.cube,
        "drilldowns": spec.drilldowns,
        "measures": spec.measures,
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
        "key": spec.raw_key(),
        "content_hash": content_hash,
        "record_count": record_count,
        "year_range": year_range,
        "cube": spec.cube,
        "drilldowns": spec.drilldowns,
        "measures": spec.measures,
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

    for dataset_id in dataset_ids:
        spec = catalog.get(dataset_id)
        if spec is None:
            continue
        _validate_dataset_candidates(spec)

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
