# Lab 05 — DataUSA Ingestion (API → S3 with Change Detection)

**Timebox:** 45–90 minutes  
**Outcome:** You can fetch population data from the DataUSA API, store it as JSON in S3, and skip re-uploading when content hasn’t changed.

## What you’re doing in this lab

1. Create an S3 bucket for DataUSA raw data (or reuse if you already created it)
2. Implement an API fetcher with retries
3. Compute a deterministic content hash
4. Store JSON in S3 with metadata + sync state/log

## You start with

- Lab 02 completed (Python project)
- Lab 03 completed (AWS profile + `FOMC_BUCKET_PREFIX` set)

## 05.1 Set your environment

```bash
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1
```

## 05.2 Create your S3 bucket for DataUSA raw data

Pick a unique name:

```bash
export DATAUSA_BUCKET="${FOMC_BUCKET_PREFIX}-datausa-raw"
```

Create it:

```bash
aws s3api create-bucket --bucket "$DATAUSA_BUCKET" --region us-east-1 || true
```

## 05.3 Create the DataUSA sync module

Create `src/data_fetchers/datausa_sync.py`:

```bash
cat > src/data_fetchers/datausa_sync.py <<'EOF'
import hashlib
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from src.helpers.aws_client import get_client


API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)


def _content_hash(payload: dict) -> str:
    # Stable hash so identical content always hashes the same
    body = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()[:16]


def _fetch_json(url: str = API_URL, retries: int = 3) -> dict:
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as resp:  # nosec - workshop URL
                body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as e:
            last = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    raise last


def _head_metadata(s3, bucket: str, key: str) -> dict:
    try:
        return s3.head_object(Bucket=bucket, Key=key).get("Metadata", {})
    except Exception:
        return {}


def _load_state(s3, bucket: str) -> dict:
    key = "_sync_state/latest_state.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def _save_state(s3, bucket: str, state: dict) -> None:
    key = "_sync_state/latest_state.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state, indent=2, default=str).encode(),
        ContentType="application/json",
    )


def _append_log(s3, bucket: str, entry: dict) -> None:
    key = "_sync_state/sync_log.jsonl"
    line = json.dumps(entry, default=str) + "\n"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = obj["Body"].read().decode()
    except Exception:
        existing = ""
    s3.put_object(Bucket=bucket, Key=key, Body=(existing + line).encode(), ContentType="text/plain")


def sync_population(bucket: str, key: str = "population.json") -> dict:
    s3 = get_client("s3")
    now = datetime.now(timezone.utc)

    payload = _fetch_json()
    h = _content_hash(payload)

    meta = _head_metadata(s3, bucket, key)
    if meta.get("content_hash") == h:
        _append_log(s3, bucket, {"timestamp": now.isoformat(), "action": "unchanged", "content_hash": h})
        return {"action": "unchanged", "content_hash": h}

    body = json.dumps(payload, indent=2).encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"content_hash": h},
    )

    years = [r.get("Year") for r in payload.get("data", []) if r.get("Year")]
    state = {
        "last_sync": now.isoformat(),
        "content_hash": h,
        "record_count": len(payload.get("data", [])),
        "year_range": [min(years), max(years)] if years else None,
        "api_url": API_URL,
    }
    _save_state(s3, bucket, state)
    _append_log(s3, bucket, {"timestamp": now.isoformat(), "action": "updated", "content_hash": h})

    return {"action": "updated", "content_hash": h, "record_count": state["record_count"]}


if __name__ == "__main__":
    import os

    bucket = os.environ.get("DATAUSA_BUCKET")
    if not bucket:
        raise SystemExit("Set DATAUSA_BUCKET env var first (see Lab 05.2).")
    print(json.dumps(sync_population(bucket), indent=2))
EOF
```

## 05.4 Run the sync (first run)

```bash
python src/data_fetchers/datausa_sync.py | python -m json.tool
```

Expected:
- `action` is `updated`

Verify `population.json` exists:

```bash
aws s3 ls "s3://$DATAUSA_BUCKET/" | grep population.json
```

## 05.5 Verify idempotency (second run)

```bash
python src/data_fetchers/datausa_sync.py | python -m json.tool
```

Expected:
- `action` is `unchanged` (unless the API updated between runs)

## 05.6 Verify sync state/log exist

```bash
aws s3 ls "s3://$DATAUSA_BUCKET/_sync_state/" || true
```

Expected:
- `latest_state.json`
- `sync_log.jsonl`

## UAT Sign‑Off (Instructor)

- [ ] Student created the DataUSA raw bucket successfully
- [ ] `python src/data_fetchers/datausa_sync.py` runs without errors
- [ ] `population.json` exists in the bucket
- [ ] Second run is `unchanged` (idempotent behavior)
- [ ] `_sync_state/latest_state.json` exists and includes `content_hash`

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add a “schema check” that validates `Year` and `Population` fields
- Add a safeguard to keep only the latest N log entries (or rotate logs)
