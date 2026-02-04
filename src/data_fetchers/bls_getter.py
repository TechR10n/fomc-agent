"""BLS time-series data fetcher with sync state management."""

import fnmatch
import json
import os
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser

from src.config import get_bls_bucket, get_bls_series_list
from src.helpers.aws_client import get_client
from src.helpers.http_client import fetch_bytes, fetch_text

BLS_BASE_URL = "https://download.bls.gov/pub/time.series"
USER_AGENT = "fomc-agent/1.0 (data-pipeline; contact: ryan.hammang@outlook.com)"


def _parse_file_patterns(patterns: str | None, series_id: str) -> list[str] | None:
    """Parse comma-separated glob patterns (supports `{series}` placeholder)."""
    if not patterns:
        return None
    items: list[str] = []
    for raw in patterns.split(","):
        p = raw.strip()
        if not p:
            continue
        items.append(p.replace("{series}", series_id))
    return items or None


def _matches_patterns(filename: str, patterns: list[str] | None) -> bool:
    if not patterns:
        return True
    return any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)


class BLSDirectoryParser(HTMLParser):
    """Parse BLS directory listing HTML to extract filename/timestamp pairs."""

    def __init__(self):
        super().__init__()
        self.files = []
        self._in_pre = False
        self._pre_data = []

    def handle_starttag(self, tag, attrs):
        if tag == "pre":
            self._in_pre = True
            self._pre_data = []

    def handle_endtag(self, tag):
        if tag == "pre":
            self._in_pre = False
            self._parse_pre_content("".join(self._pre_data))

    def handle_data(self, data):
        if self._in_pre:
            self._pre_data.append(data)

    def _parse_pre_content(self, text):
        # BLS format: each line has a link text followed by date and size
        # Pattern: filename    M/D/YYYY  H:MM AM/PM    size
        pattern = re.compile(
            r"(\S+)\s+(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)\s+(\d+|-)"
        )
        for match in pattern.finditer(text):
            filename = match.group(1)
            timestamp_str = match.group(2)
            size_str = match.group(3)
            if filename in (".", "..") or "[" in filename or "]" in filename:
                continue
            size = int(size_str) if size_str != "-" else 0
            self.files.append({
                "filename": filename,
                "timestamp": timestamp_str,
                "size": size,
            })


def parse_bls_timestamp(date_str: str) -> datetime:
    """Parse BLS page timestamp: '1/29/2026  8:30 AM' -> datetime."""
    cleaned = re.sub(r"\s+", " ", date_str.strip())
    return datetime.strptime(cleaned, "%m/%d/%Y %I:%M %p")


def fetch_directory_listing(series_id: str) -> list[dict]:
    """Fetch and parse the BLS directory listing for a series."""
    base_url = os.environ.get("BLS_BASE_URL", BLS_BASE_URL)
    user_agent = os.environ.get("BLS_USER_AGENT", USER_AGENT)
    url = f"{base_url}/{series_id}/"
    html = fetch_text(url, headers={"User-Agent": user_agent}, timeout=30)

    parser = BLSDirectoryParser()
    parser.feed(html)
    return parser.files


def needs_update(source_time: datetime, s3_metadata: dict) -> bool:
    """Compare source timestamp with last sync time stored in S3 metadata."""
    last_sync = s3_metadata.get("source_modified")
    if not last_sync:
        return True
    return source_time > datetime.fromisoformat(last_sync)


def get_s3_metadata(s3_client, bucket: str, key: str) -> dict:
    """Get metadata for an S3 object, or empty dict if not found."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response.get("Metadata", {})
    except s3_client.exceptions.ClientError:
        return {}


def download_file(series_id: str, filename: str) -> bytes:
    """Download a single file from BLS."""
    base_url = os.environ.get("BLS_BASE_URL", BLS_BASE_URL)
    user_agent = os.environ.get("BLS_USER_AGENT", USER_AGENT)
    url = f"{base_url}/{series_id}/{filename}"
    return fetch_bytes(url, headers={"User-Agent": user_agent}, timeout=60)


def upload_to_s3(s3_client, bucket: str, key: str, data: bytes, metadata: dict):
    """Upload data to S3 with metadata."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        Metadata=metadata,
    )


def load_sync_state(s3_client, bucket: str, series_id: str) -> dict:
    """Load the latest sync state from S3."""
    key = f"_sync_state/{series_id}/latest_state.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())
    except Exception:
        return {"series": series_id, "files": {}}


def save_sync_state(s3_client, bucket: str, series_id: str, state: dict):
    """Save sync state to S3 (write to temp key, then copy)."""
    state_key = f"_sync_state/{series_id}/latest_state.json"
    temp_key = f"_sync_state/{series_id}/_tmp_state.json"

    body = json.dumps(state, indent=2, default=str)
    s3_client.put_object(Bucket=bucket, Key=temp_key, Body=body.encode())
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": temp_key},
        Key=state_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=temp_key)


def append_sync_log(s3_client, bucket: str, series_id: str, entry: dict):
    """Append an entry to the sync log JSONL file."""
    log_key = f"_sync_state/{series_id}/sync_log.jsonl"
    existing = ""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=log_key)
        existing = response["Body"].read().decode()
    except Exception:
        pass
    line = json.dumps(entry, default=str) + "\n"
    s3_client.put_object(Bucket=bucket, Key=log_key, Body=(existing + line).encode())


def sync_series(series_id: str, bucket: str | None = None) -> dict:
    """Sync a single BLS series to S3.

    Returns a summary of actions taken.
    """
    if bucket is None:
        bucket = get_bls_bucket()

    file_patterns = _parse_file_patterns(
        os.environ.get("BLS_FILE_PATTERNS"),
        series_id=series_id,
    )

    s3 = get_client("s3")
    now = datetime.now(timezone.utc)

    files = fetch_directory_listing(series_id)
    state = load_sync_state(s3, bucket, series_id)
    source_files = {f["filename"] for f in files}
    known_files = set(state.get("files", {}).keys())

    summary = {"updated": [], "added": [], "unchanged": [], "deleted": []}

    for file_info in files:
        filename = file_info["filename"]
        if not _matches_patterns(filename, file_patterns):
            continue
        source_time = parse_bls_timestamp(file_info["timestamp"])
        s3_key = f"{series_id}/{filename}"
        metadata = get_s3_metadata(s3, bucket, s3_key)

        if not needs_update(source_time, metadata):
            summary["unchanged"].append(filename)
            log_entry = {
                "timestamp": now.isoformat(),
                "file": filename,
                "action": "unchanged",
                "source_modified": source_time.isoformat(),
            }
        else:
            data = download_file(series_id, filename)
            upload_to_s3(s3, bucket, s3_key, data, {
                "source_modified": source_time.isoformat(),
            })

            action = "added" if filename not in known_files else "updated"
            summary[action].append(filename)
            log_entry = {
                "timestamp": now.isoformat(),
                "file": filename,
                "action": action,
                "source_modified": source_time.isoformat(),
                "bytes": len(data),
            }
            state.setdefault("files", {})[filename] = {
                "source_modified": source_time.isoformat(),
                "bytes": len(data),
            }

        append_sync_log(s3, bucket, series_id, log_entry)

    # Detect deleted files
    deleted = known_files - source_files
    for filename in deleted:
        summary["deleted"].append(filename)
        s3_key = f"{series_id}/{filename}"
        try:
            s3.delete_object(Bucket=bucket, Key=s3_key)
        except Exception:
            pass
        state.get("files", {}).pop(filename, None)
        append_sync_log(s3, bucket, series_id, {
            "timestamp": now.isoformat(),
            "file": filename,
            "action": "deleted",
        })

    state["last_sync"] = now.isoformat()
    state["series"] = series_id
    save_sync_state(s3, bucket, series_id, state)

    return summary


def sync_all(series_list: list[str] | None = None, bucket: str | None = None) -> dict:
    """Sync all configured BLS series.

    Set BLS_SERIES_DELAY_SECONDS to pause between series (default: 2).
    Set BLS_FILE_PATTERNS to limit which files are downloaded per series.
    """
    if series_list is None:
        series_list = get_bls_series_list()
    if bucket is None:
        bucket = get_bls_bucket()

    delay = float(os.environ.get("BLS_SERIES_DELAY_SECONDS", "2"))

    results = {}
    for i, series_id in enumerate(series_list):
        results[series_id] = sync_series(series_id, bucket)
        if delay > 0 and i < len(series_list) - 1:
            time.sleep(delay)
    return results


if __name__ == "__main__":
    bucket = get_bls_bucket()
    results = sync_all(bucket=bucket)
    print(json.dumps(results, indent=2))
