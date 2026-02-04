"""Tests for BLS change timeline export."""

import json
from datetime import datetime, timezone

import boto3
from moto import mock_aws

from src.analytics.bls_timeline import (
    build_bls_change_timeline,
    export_bls_change_timeline,
    load_bls_change_events_from_s3,
)


@mock_aws
def test_load_bls_change_events_from_s3_filters_to_changes_only():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    body = "\n".join([
        json.dumps({
            "timestamp": "2026-02-03T04:00:00+00:00",
            "file": "pr.data.0.Current",
            "action": "unchanged",
            "source_modified": "2026-02-01T08:30:00",
        }),
        "not json",
        json.dumps({
            "timestamp": "2026-02-03T04:00:00+00:00",
            "file": "pr.data.0.Current",
            "action": "updated",
            "source_modified": "2026-02-01T08:30:00",
            "bytes": 123,
        }),
        json.dumps({
            "timestamp": "2026-02-03T04:00:00+00:00",
            "file": "",
            "action": "updated",
            "source_modified": "2026-02-01T08:30:00",
        }),
    ]) + "\n"

    s3.put_object(
        Bucket="test-bucket",
        Key="_sync_state/pr/sync_log.jsonl",
        Body=body.encode("utf-8"),
    )

    events = load_bls_change_events_from_s3(s3, "test-bucket", "pr")
    assert len(events) == 1
    assert events[0]["series"] == "pr"
    assert events[0]["action"] == "updated"
    assert events[0]["file"] == "pr.data.0.Current"


def test_build_bls_change_timeline_filters_trailing_window_and_normalizes_times():
    now = datetime(2026, 2, 4, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "series": "pr",
            "file": "pr.data.0.Current",
            "action": "updated",
            "source_modified": "2026-02-01T08:30:00",
            "observed_at": "2026-02-03T04:00:00+00:00",
            "bytes": 1,
        },
        {
            "series": "pr",
            "file": "pr.old",
            "action": "updated",
            "source_modified": "2025-09-01T08:30:00",
            "observed_at": "2026-02-03T04:00:00+00:00",
            "bytes": 1,
        },
        {
            "series": "cu",
            "file": "cu.item",
            "action": "deleted",
            "source_modified": None,
            "observed_at": "2026-01-25T04:00:00Z",
        },
    ]

    payload = build_bls_change_timeline(events, now=now, window_days=60)
    assert payload["generated_at"] == "2026-02-04T00:00:00Z"
    assert payload["window_days"] == 60

    files = [e["file"] for e in payload["events"]]
    assert "pr.data.0.Current" in files
    assert "pr.old" not in files  # filtered out of trailing window

    pr_event = next(e for e in payload["events"] if e["file"] == "pr.data.0.Current")
    # Source timestamps in sync logs are naive and interpreted as ET (America/New_York).
    assert pr_event["event_time"] == "2026-02-01T13:30:00Z"
    assert pr_event["source_modified"] == "2026-02-01T13:30:00Z"
    assert pr_event["observed_at"] == "2026-02-03T04:00:00Z"

    deleted_event = next(e for e in payload["events"] if e["action"] == "deleted")
    assert deleted_event["event_time"] == "2026-01-25T04:00:00Z"
    assert deleted_event["source_modified"] is None


@mock_aws
def test_export_bls_change_timeline_writes_json(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    s3.put_object(
        Bucket="test-bucket",
        Key="_sync_state/pr/sync_log.jsonl",
        Body=(json.dumps({
            "timestamp": "2026-02-03T04:00:00+00:00",
            "file": "pr.data.0.Current",
            "action": "updated",
            "source_modified": "2026-02-01T08:30:00",
            "bytes": 10,
        }) + "\n").encode("utf-8"),
    )

    s3.put_object(
        Bucket="test-bucket",
        Key="_sync_state/cu/sync_log.jsonl",
        Body=(json.dumps({
            "timestamp": "2026-02-03T04:00:00+00:00",
            "file": "cu.data.0.Current",
            "action": "added",
            "source_modified": "2026-02-02T08:30:00",
            "bytes": 20,
        }) + "\n").encode("utf-8"),
    )

    out_path = tmp_path / "bls_timeline.json"
    now = datetime(2026, 2, 4, 0, 0, tzinfo=timezone.utc)
    exported = export_bls_change_timeline(
        out_path=out_path,
        bucket="test-bucket",
        series_list=["pr", "cu"],
        window_days=60,
        include_release_schedule=False,
        now=now,
    )

    payload = json.loads(exported.read_text(encoding="utf-8"))
    assert payload["generated_at"] == "2026-02-04T00:00:00Z"
    assert len(payload["events"]) == 2
    assert {e["series"] for e in payload["events"]} == {"pr", "cu"}
