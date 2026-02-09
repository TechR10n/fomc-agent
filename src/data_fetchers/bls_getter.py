"""BLS time-series data fetcher with sync state management."""

import fnmatch
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone

from src.config import get_bls_bucket, get_bls_series_list
from src.helpers.aws_client import get_client
from src.helpers.http_client import fetch_bytes, fetch_text, post_json

BLS_BASE_URL = "https://download.bls.gov/pub/time.series"
BLS_API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
USER_AGENT = "fomc-agent/1.0 (data-pipeline; contact: ryan.hammang@outlook.com)"

_DEFAULT_LN_SERIES_IDS = ("LNS14000000", "LNS11300000")


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

    # BLS directory HTML format:
    #   M/D/YYYY  H:MM AM|PM   size  <A HREF="...">filename</A><br>
    pattern = re.compile(
        r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)\s+(\d+|-)\s+<A\s+HREF=\"[^\"]+\">([^<]+)</A>",
        flags=re.IGNORECASE,
    )

    files: list[dict] = []
    for m in pattern.finditer(html or ""):
        date_str = m.group(1)
        time_str = m.group(2)
        size_str = m.group(3)
        filename = (m.group(4) or "").strip()
        if not filename or filename.startswith("[") and filename.endswith("]"):
            continue
        size = int(size_str) if size_str.isdigit() else 0
        files.append({
            "filename": filename,
            "timestamp": f"{date_str} {time_str}",
            "size": size,
        })

    return files


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

def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _parse_env_csv(name: str, default: tuple[str, ...]) -> list[str]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return list(default)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _parse_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _year_chunks(start_year: int, end_year: int, max_years: int) -> list[tuple[int, int]]:
    if max_years <= 0:
        max_years = 20
    if end_year < start_year:
        return []
    out: list[tuple[int, int]] = []
    cur = start_year
    while cur <= end_year:
        chunk_end = min(end_year, cur + max_years - 1)
        out.append((cur, chunk_end))
        cur = chunk_end + 1
    return out


def _bls_api_rows(
    *,
    series_ids: list[str],
    start_year: int,
    end_year: int,
) -> list[dict[str, str]]:
    """Fetch rows from the BLS public API and normalize to the TSV schema we ingest."""
    url = os.environ.get("BLS_API_BASE_URL", BLS_API_BASE_URL).strip() or BLS_API_BASE_URL
    user_agent = os.environ.get("BLS_USER_AGENT", USER_AGENT)
    api_key = os.environ.get("BLS_API_KEY", "").strip()

    payload: dict[str, object] = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        payload["registrationKey"] = api_key

    timeout = _parse_env_int("BLS_API_TIMEOUT_SECONDS", 30)
    retries = _parse_env_int("BLS_API_RETRIES", 3)
    backoff = float(os.environ.get("BLS_API_BACKOFF_SECONDS", "1").strip() or "1")

    resp = post_json(
        url,
        payload,
        headers={"User-Agent": user_agent},
        timeout=timeout,
        retries=retries,
        backoff_seconds=backoff,
    )
    if not isinstance(resp, dict):
        raise RuntimeError(f"BLS API returned non-object response: {type(resp)}")

    status = str(resp.get("status", "")).strip()
    if status != "REQUEST_SUCCEEDED":
        msg = resp.get("message", [])
        raise RuntimeError(f"BLS API request failed: status={status} message={msg}")

    results = resp.get("Results", {})
    series_list = results.get("series", []) if isinstance(results, dict) else []
    if not isinstance(series_list, list):
        series_list = []

    out: list[dict[str, str]] = []
    for series in series_list:
        if not isinstance(series, dict):
            continue
        series_id = str(series.get("seriesID", "")).strip()
        data_points = series.get("data", [])
        if not series_id or not isinstance(data_points, list):
            continue

        for p in data_points:
            if not isinstance(p, dict):
                continue
            year = str(p.get("year", "")).strip()
            period = str(p.get("period", "")).strip()
            value = str(p.get("value", "")).strip()

            if not year or not period:
                continue

            # Footnotes come back as a list of {"code": "..."} objects.
            codes: list[str] = []
            for fn in p.get("footnotes", []) or []:
                if not isinstance(fn, dict):
                    continue
                code = str(fn.get("code", "")).strip()
                if code:
                    codes.append(code)
            footnote_codes = ",".join(codes)

            out.append({
                "series_id": series_id,
                "year": year,
                "period": period,
                "value": value,
                "footnote_codes": footnote_codes,
            })

    return out


def _render_tsv(rows: list[dict[str, str]]) -> bytes:
    header = "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
    lines = [header]
    for r in rows:
        lines.append(
            f"{r.get('series_id','')}\t{r.get('year','')}\t{r.get('period','')}\t{r.get('value','')}\t{r.get('footnote_codes','')}\n"
        )
    return "".join(lines).encode("utf-8")


def _sync_ln_via_api(bucket: str) -> dict:
    """Sync a small LN extract via the BLS API.

    The BLS file mirror does not publish `ln.data.0.Current`; downloading
    `ln.data.1.AllData` is hundreds of MB. For our curated charts we only need a
    couple of LN series, so we materialize a tiny TSV at `ln/ln.data.0.Current`.
    """
    series_id = "ln"
    filename = f"{series_id}.data.0.Current"
    s3_key = f"{series_id}/{filename}"

    now = datetime.now(timezone.utc)
    s3 = get_client("s3")

    ln_source = os.environ.get("BLS_LN_SOURCE", "api").strip().lower()
    if ln_source not in {"api", "bls_api"}:
        # Fall back to file-mirror behavior (caller will use directory listing).
        raise RuntimeError(f"unsupported BLS_LN_SOURCE={ln_source!r} (supported: api)")

    series_ids = _parse_env_csv("BLS_LN_SERIES_IDS", _DEFAULT_LN_SERIES_IDS)
    end_year = _parse_env_int("BLS_LN_END_YEAR", now.year)
    start_year = _parse_env_int("BLS_LN_START_YEAR", 2005)
    # The BLS public API commonly limits unregistered requests to ~10 years of
    # data. If callers don't explicitly configure a chunk size, default to a
    # safe value so we don't silently miss mid-range years.
    max_years_env = os.environ.get("BLS_API_MAX_YEARS_PER_REQUEST", "").strip()
    if max_years_env:
        max_years = _parse_env_int("BLS_API_MAX_YEARS_PER_REQUEST", 10)
    else:
        max_years = 20 if os.environ.get("BLS_API_KEY", "").strip() else 10
    if not os.environ.get("BLS_API_KEY", "").strip() and max_years > 10:
        # Without an API key, BLS may only return ~10 years regardless of the
        # requested range. Clamp chunk size so we don't miss intermediate years.
        max_years = 10

    # Fetch in year chunks to stay within API limits.
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, str]] = []
    for y0, y1 in _year_chunks(start_year, end_year, max_years):
        for r in _bls_api_rows(series_ids=series_ids, start_year=y0, end_year=y1):
            key = (r.get("series_id", ""), r.get("year", ""), r.get("period", ""))
            if not key[0] or not key[1] or not key[2]:
                continue
            if key in seen:
                continue
            seen.add(key)
            rows.append(r)

    # Stable ordering for deterministic hashes.
    def _sort_key(r: dict[str, str]) -> tuple[str, int, str]:
        try:
            y = int(r.get("year", "0") or "0")
        except ValueError:
            y = 0
        return (r.get("series_id", ""), y, r.get("period", ""))

    rows.sort(key=_sort_key)
    body = _render_tsv(rows)
    content_hash = _hash_bytes(body)

    summary = {"updated": [], "added": [], "unchanged": [], "deleted": []}

    # Skip write if unchanged.
    existing = get_s3_metadata(s3, bucket, s3_key)
    if isinstance(existing, dict) and existing.get("content_hash") == content_hash:
        summary["unchanged"].append(filename)
        append_sync_log(s3, bucket, series_id, {
            "timestamp": now.isoformat(),
            "file": filename,
            "action": "unchanged",
            "content_hash": content_hash,
            "source": "bls_api",
        })
        return summary

    state = load_sync_state(s3, bucket, series_id)
    known_files = set(state.get("files", {}).keys())

    upload_to_s3(
        s3,
        bucket,
        s3_key,
        body,
        {
            "content_hash": content_hash,
            "source": "bls_api",
            "fetched_at": now.isoformat(),
        },
    )

    action = "added" if filename not in known_files else "updated"
    summary[action].append(filename)
    append_sync_log(s3, bucket, series_id, {
        "timestamp": now.isoformat(),
        "file": filename,
        "action": action,
        "bytes": len(body),
        "content_hash": content_hash,
        "source": "bls_api",
    })

    state.setdefault("files", {})[filename] = {
        "bytes": len(body),
        "content_hash": content_hash,
        "fetched_at": now.isoformat(),
    }
    state["last_sync"] = now.isoformat()
    state["series"] = series_id
    save_sync_state(s3, bucket, series_id, state)

    return summary


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

    if series_id.strip().lower() == "ln":
        # Special-case LN: use the BLS API to avoid huge file downloads.
        return _sync_ln_via_api(bucket)

    # Default to the one file the rest of the pipeline expects to exist:
    #   s3://<bucket>/<series>/<series>.data.0.Current
    #
    # This keeps local runs fast (BLS directories can contain very large files).
    raw_patterns = os.environ.get("BLS_FILE_PATTERNS")
    if raw_patterns is None:
        raw_patterns = "{series}.data.0.Current"
    file_patterns = _parse_file_patterns(raw_patterns, series_id=series_id)

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
