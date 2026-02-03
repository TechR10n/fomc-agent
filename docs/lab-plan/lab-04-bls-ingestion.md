# Lab 04 — BLS Ingestion (Sync a Public Dataset into S3)

**Timebox:** 60–120 minutes  
**Outcome:** You can sync the BLS “pr” time-series dataset into an S3 bucket (AWS) with idempotency and a sync log.

## What you’re doing in this lab

1. Create an S3 bucket for “raw” BLS files
2. Implement a sync script that:
   - Lists files from BLS
   - Downloads new/updated files
   - Uploads them to S3 under a prefix (e.g. `pr/…`)
   - Writes a sync state file and an append-only sync log
3. Verify idempotency (second run should do almost nothing)

## You start with

- Lab 02 completed (Python project + venv)
- Lab 03 completed (AWS profile + `FOMC_BUCKET_PREFIX` set)

## 04.1 Set your environment

```bash
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1
```

## 04.2 Create your S3 bucket for BLS raw data

S3 bucket names must be globally unique in AWS. Pick a unique name:

```bash
export BLS_BUCKET="${FOMC_BUCKET_PREFIX}-bls-raw"
```

Create bucket:

```bash
aws s3api create-bucket --bucket "$BLS_BUCKET" --region us-east-1
```

Verify:

```bash
aws s3 ls | grep "$BLS_BUCKET" || true
```

## 04.3 Create the BLS sync module

Create the folder:

```bash
mkdir -p src/data_fetchers
touch src/data_fetchers/__init__.py
```

Create `src/data_fetchers/bls_sync.py`:

```bash
cat > src/data_fetchers/bls_sync.py <<'EOF'
"""
Sync BLS time-series directory files into S3.

Key ideas:
- List remote directory contents
- Compare "last modified" timestamps to S3 object metadata
- Upload only when needed (idempotency)
- Keep a small state file and an append-only JSONL log
"""

import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser

import urllib.request

from src.helpers.aws_client import get_client


BLS_BASE_URL = "https://download.bls.gov/pub/time.series"
USER_AGENT = "fomc-agent-lab/1.0 (learning; contact: you@example.com)"


class _BlsDirParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files: list[dict] = []
        self._in_pre = False
        self._pre = []

    def handle_starttag(self, tag, attrs):
        if tag == "pre":
            self._in_pre = True
            self._pre = []

    def handle_endtag(self, tag):
        if tag == "pre":
            self._in_pre = False
            self._parse("".join(self._pre))

    def handle_data(self, data):
        if self._in_pre:
            self._pre.append(data)

    def _parse(self, text: str):
        # Example line format in <pre>:
        # pr.data.0.Current      1/15/2026  8:30 AM       123456
        pattern = re.compile(
            r"(\S+)\s+(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)\s+(\d+|-)"
        )
        for m in pattern.finditer(text):
            filename = m.group(1)
            ts = m.group(2)
            size = 0 if m.group(3) == "-" else int(m.group(3))
            if filename in (".", "..", "[To Parent Directory]"):
                continue
            self.files.append({"filename": filename, "timestamp": ts, "size": size})


def _parse_bls_timestamp(ts: str) -> datetime:
    ts = re.sub(r"\s+", " ", ts.strip())
    return datetime.strptime(ts, "%m/%d/%Y %I:%M %p")


def _list_remote_files(series_id: str) -> list[dict]:
    url = f"{BLS_BASE_URL}/{series_id}/"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:  # nosec - workshop URL
        html = resp.read().decode("utf-8", errors="replace")
    parser = _BlsDirParser()
    parser.feed(html)
    return parser.files


def _download(series_id: str, filename: str) -> bytes:
    url = f"{BLS_BASE_URL}/{series_id}/{filename}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:  # nosec - workshop URL
        return resp.read()


def _head_metadata(s3, bucket: str, key: str) -> dict:
    try:
        return s3.head_object(Bucket=bucket, Key=key).get("Metadata", {})
    except Exception:
        return {}


def _needs_update(source_time: datetime, s3_meta: dict) -> bool:
    prev = s3_meta.get("source_modified")
    if not prev:
        return True
    try:
        return source_time > datetime.fromisoformat(prev)
    except Exception:
        return True


def _load_state(s3, bucket: str, series_id: str) -> dict:
    key = f"_sync_state/{series_id}/latest_state.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"series": series_id, "files": {}}


def _save_state(s3, bucket: str, series_id: str, state: dict) -> None:
    key = f"_sync_state/{series_id}/latest_state.json"
    body = json.dumps(state, indent=2, default=str).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def _append_log(s3, bucket: str, series_id: str, entry: dict) -> None:
    key = f"_sync_state/{series_id}/sync_log.jsonl"
    line = json.dumps(entry, default=str) + "\n"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = obj["Body"].read().decode()
    except Exception:
        existing = ""
    s3.put_object(Bucket=bucket, Key=key, Body=(existing + line).encode(), ContentType="text/plain")


def sync_series(series_id: str, bucket: str) -> dict:
    s3 = get_client("s3")
    now = datetime.now(timezone.utc)

    remote = _list_remote_files(series_id)
    state = _load_state(s3, bucket, series_id)
    known = set(state.get("files", {}).keys())
    source_files = {f["filename"] for f in remote}

    summary = {"added": [], "updated": [], "unchanged": [], "deleted": []}

    for info in remote:
        filename = info["filename"]
        source_time = _parse_bls_timestamp(info["timestamp"])
        key = f"{series_id}/{filename}"

        meta = _head_metadata(s3, bucket, key)
        if not _needs_update(source_time, meta):
            summary["unchanged"].append(filename)
            _append_log(s3, bucket, series_id, {
                "timestamp": now.isoformat(),
                "action": "unchanged",
                "file": filename,
                "source_modified": source_time.isoformat(),
            })
            continue

        data = _download(series_id, filename)
        action = "added" if filename not in known else "updated"
        summary[action].append(filename)

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            Metadata={"source_modified": source_time.isoformat()},
        )

        state.setdefault("files", {})[filename] = {
            "source_modified": source_time.isoformat(),
            "bytes": len(data),
        }

        _append_log(s3, bucket, series_id, {
            "timestamp": now.isoformat(),
            "action": action,
            "file": filename,
            "source_modified": source_time.isoformat(),
            "bytes": len(data),
        })

    # Deletions (best-effort)
    for filename in sorted(known - source_files):
        summary["deleted"].append(filename)
        key = f"{series_id}/{filename}"
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass
        state.get("files", {}).pop(filename, None)
        _append_log(s3, bucket, series_id, {
            "timestamp": now.isoformat(),
            "action": "deleted",
            "file": filename,
        })

    state["series"] = series_id
    state["last_sync"] = now.isoformat()
    _save_state(s3, bucket, series_id, state)

    return summary


if __name__ == "__main__":
    import os
    series = os.environ.get("BLS_SERIES", "pr")
    bucket = os.environ.get("BLS_BUCKET")
    if not bucket:
        raise SystemExit("Set BLS_BUCKET env var first (see Lab 04.2).")
    result = sync_series(series, bucket)
    print(json.dumps(result, indent=2))
EOF
```

## 04.4 Run the sync (first run)

```bash
export BLS_SERIES=pr
python src/data_fetchers/bls_sync.py | head -40
```

Expected:
- `added` includes many files on first run

Verify objects exist:

```bash
aws s3 ls "s3://$BLS_BUCKET/pr/" | head
```

## 04.5 Verify state + log exist

```bash
aws s3 ls "s3://$BLS_BUCKET/_sync_state/pr/" || true
```

Expected:
- `latest_state.json`
- `sync_log.jsonl`

## 04.6 Verify idempotency (second run)

```bash
python src/data_fetchers/bls_sync.py | head -60
```

Expected:
- Mostly `unchanged` (unless upstream changed between runs)

## UAT Sign‑Off (Instructor)

- [ ] Student created the BLS raw bucket successfully
- [ ] `python src/data_fetchers/bls_sync.py` runs without errors
- [ ] BLS files exist under `s3://$BLS_BUCKET/pr/`
- [ ] Sync state/log exist under `s3://$BLS_BUCKET/_sync_state/pr/`
- [ ] Second run is mostly `unchanged` (idempotent behavior)

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add exponential backoff to HTTP downloads
- Add a “dry run” mode that prints what would change without uploading
- Track file sizes and alert if a file shrinks unexpectedly
