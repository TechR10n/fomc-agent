"""Build a trailing-window timeline of BLS LABSTAT file changes.

This is driven by the "Last Modified" timestamps shown in each BLS time-series
directory listing (download.bls.gov). The ingestion step records these source
timestamps in `_sync_state/<series>/sync_log.jsonl`.

This module reads those logs and emits a compact JSON payload suitable for the
static site (or any UI) to render a change timeline.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.config import get_bls_bucket, get_bls_series_list
from src.helpers.aws_client import get_client
from src.analytics.bls_release_schedule import load_scheduled_releases

CHANGE_ACTIONS = {"added", "updated", "deleted"}
DEFAULT_BLS_SOURCE_TIMEZONE = "America/New_York"


def _get_tz(name: str | None) -> timezone | ZoneInfo:
    tz_name = (name or "").strip() or "UTC"
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def _parse_iso_datetime(value: str | None, *, default_tz: timezone | ZoneInfo) -> datetime | None:
    if not value or not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    # Python's fromisoformat doesn't accept "Z" (Zulu) suffix.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=default_tz)
    return dt.astimezone(timezone.utc)


def _to_utc_iso(dt: datetime) -> str:
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_bls_change_events_from_s3(s3_client, bucket: str, series_id: str) -> list[dict[str, Any]]:
    """Read `_sync_state/<series>/sync_log.jsonl` and return change-only events."""
    key = f"_sync_state/{series_id}/sync_log.jsonl"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        text = response["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue

        action = rec.get("action")
        if action not in CHANGE_ACTIONS:
            continue

        filename = rec.get("file")
        if not filename:
            continue

        out.append({
            "series": series_id,
            "file": str(filename),
            "action": str(action),
            "source_modified": rec.get("source_modified"),
            "observed_at": rec.get("timestamp"),
            "bytes": rec.get("bytes"),
        })
    return out


def build_bls_change_timeline(
    events: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    window_days: int = 60,
    lookahead_days: int = 0,
) -> dict[str, Any]:
    """Build a UI-friendly timeline payload.

    The timeline event time is `source_modified` (BLS directory timestamp) when
    available; otherwise it falls back to `observed_at` (pipeline run time).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    now_utc = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)

    if window_days <= 0:
        window_days = 60

    start = now_utc - timedelta(days=window_days)
    end = now_utc + timedelta(days=max(0, lookahead_days))

    bls_source_tz = _get_tz(os.environ.get("BLS_SOURCE_TIMEZONE", DEFAULT_BLS_SOURCE_TIMEZONE))

    normalized: list[dict[str, Any]] = []
    for e in events:
        # Source timestamps are derived from BLS directory listings and are shown in ET on bls.gov.
        source_dt = _parse_iso_datetime(e.get("source_modified"), default_tz=bls_source_tz)
        observed_dt = _parse_iso_datetime(e.get("observed_at"), default_tz=timezone.utc)
        event_dt = source_dt or observed_dt
        if event_dt is None:
            continue
        if event_dt < start or event_dt > end:
            continue

        bytes_value = e.get("bytes")
        try:
            bytes_int = int(bytes_value) if bytes_value is not None else None
        except (TypeError, ValueError):
            bytes_int = None

        normalized.append({
            "series": str(e.get("series") or ""),
            "file": str(e.get("file") or ""),
            "action": str(e.get("action") or ""),
            "event_time": _to_utc_iso(event_dt),
            "source_modified": _to_utc_iso(source_dt) if source_dt else None,
            "observed_at": _to_utc_iso(observed_dt) if observed_dt else None,
            "bytes": bytes_int,
        })

    # Stable ordering: newest first, then series/file for ties.
    normalized.sort(key=lambda r: (r.get("series", ""), r.get("file", ""), r.get("action", "")))
    normalized.sort(
        key=lambda r: _parse_iso_datetime(r.get("event_time"), default_tz=timezone.utc)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    return {
        "generated_at": _to_utc_iso(now_utc),
        "window_days": window_days,
        "lookahead_days": max(0, lookahead_days),
        "events": normalized,
    }


def _group_actual_series_times(events: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    """Group actual file change events by series + event_time (UTC ISO)."""
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for e in events:
        action = e.get("action")
        if action not in {"added", "updated"}:
            continue
        series = str(e.get("series") or "")
        if not series:
            continue
        event_time = str(e.get("event_time") or "")
        if not event_time:
            continue
        bucket = out.setdefault(series, {})
        agg = bucket.setdefault(event_time, {"files_changed": 0, "bytes_changed": 0})
        agg["files_changed"] += 1
        b = e.get("bytes")
        if isinstance(b, int) and b >= 0:
            agg["bytes_changed"] += b
    return out


def _match_release(
    *,
    scheduled_time: str,
    series_id: str,
    actual_by_series_time: dict[str, dict[str, dict[str, Any]]],
    early_minutes: int = 15,
    late_hours: int = 24,
) -> dict[str, Any] | None:
    scheduled_dt = _parse_iso_datetime(scheduled_time, default_tz=timezone.utc)
    if scheduled_dt is None:
        return None

    series_times = actual_by_series_time.get(series_id, {})
    if not series_times:
        return {
            "actual_time": None,
            "delay_minutes": None,
            "actual_files_changed": 0,
            "actual_bytes_changed": 0,
        }

    early_margin = timedelta(minutes=max(0, early_minutes))
    late_margin = timedelta(hours=max(1, late_hours))

    candidates: list[tuple[float, bool, str, dict[str, Any]]] = []
    for actual_time, agg in series_times.items():
        actual_dt = _parse_iso_datetime(actual_time, default_tz=timezone.utc)
        if actual_dt is None:
            continue
        delta = actual_dt - scheduled_dt
        if delta < -early_margin or delta > late_margin:
            continue
        seconds = delta.total_seconds()
        candidates.append((abs(seconds), seconds < 0, actual_time, agg))

    if not candidates:
        return {
            "actual_time": None,
            "delay_minutes": None,
            "actual_files_changed": 0,
            "actual_bytes_changed": 0,
        }

    candidates.sort(key=lambda t: (t[0], t[1]))
    best_abs, best_is_early, best_time, best_agg = candidates[0]

    best_dt = _parse_iso_datetime(best_time, default_tz=timezone.utc)
    if best_dt is None:
        return None
    delay_minutes = round((best_dt - scheduled_dt).total_seconds() / 60.0, 1)
    return {
        "actual_time": best_time,
        "delay_minutes": delay_minutes,
        "actual_files_changed": int(best_agg.get("files_changed", 0) or 0),
        "actual_bytes_changed": int(best_agg.get("bytes_changed", 0) or 0),
    }


def build_release_timeline(
    *,
    scheduled: list[dict[str, Any]],
    actual_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Combine expected (scheduled) releases with matched actual updates."""
    actual_by_series_time = _group_actual_series_times(actual_events)
    out: list[dict[str, Any]] = []

    for s in scheduled:
        series = str(s.get("series") or "")
        scheduled_time = s.get("scheduled_time")
        if not series or not scheduled_time:
            continue
        match = _match_release(
            scheduled_time=str(scheduled_time),
            series_id=series,
            actual_by_series_time=actual_by_series_time,
        )
        if match is None:
            continue
        out.append({
            "series": series,
            "release": s.get("release"),
            "url": s.get("url"),
            "scheduled_time": s.get("scheduled_time"),
            "scheduled_time_local": s.get("scheduled_time_local"),
            "time_zone": s.get("time_zone"),
            **match,
        })

    out.sort(key=lambda r: r.get("scheduled_time", ""))
    return out


def export_bls_change_timeline(
    *,
    out_path: str | Path = Path("site/data/bls_timeline.json"),
    bucket: str | None = None,
    series_list: list[str] | None = None,
    window_days: int = 60,
    lookahead_days: int = 0,
    include_release_schedule: bool = True,
    now: datetime | None = None,
) -> Path:
    """Export a `site/data/bls_timeline.json` payload from S3 sync logs."""
    if bucket is None:
        bucket = get_bls_bucket()
    if series_list is None:
        series_list = get_bls_series_list()

    s3 = get_client("s3")
    all_events: list[dict[str, Any]] = []
    for series_id in series_list:
        all_events.extend(load_bls_change_events_from_s3(s3, bucket, series_id))

    payload = build_bls_change_timeline(
        all_events,
        now=now,
        window_days=window_days,
        lookahead_days=lookahead_days,
    )

    # Optionally fetch scheduled release calendar entries and match them to the observed updates.
    if include_release_schedule:
        now_utc = (
            (now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc))
            if now is not None
            else datetime.now(timezone.utc)
        )
        start = now_utc - timedelta(days=window_days)
        end = now_utc + timedelta(days=max(0, lookahead_days))
        schedule_tz = os.environ.get("BLS_SCHEDULE_TIMEZONE") or os.environ.get(
            "BLS_SOURCE_TIMEZONE",
            DEFAULT_BLS_SOURCE_TIMEZONE,
        )
        scheduled = load_scheduled_releases(
            series_list=series_list,
            start=start,
            end=end,
            schedule_tz=schedule_tz,
        )
        payload["scheduled_releases"] = scheduled
        payload["releases"] = build_release_timeline(
            scheduled=scheduled,
            actual_events=payload.get("events", []),
        )

    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path.resolve()


if __name__ == "__main__":
    export_bls_change_timeline()
