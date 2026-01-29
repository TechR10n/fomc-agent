"""Tests for bls_getter.py."""

import json
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

import boto3
import pytest
from moto import mock_aws

from src.data_fetchers.bls_getter import (
    BLSDirectoryParser,
    parse_bls_timestamp,
    fetch_directory_listing,
    needs_update,
    get_s3_metadata,
    sync_series,
    sync_all,
    load_sync_state,
    save_sync_state,
    append_sync_log,
)


@pytest.fixture(autouse=True)
def clear_endpoint():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AWS_ENDPOINT_URL", None)
        yield


class TestBLSDirectoryParser:
    def test_parse_directory_listing(self, sample_bls_html):
        """Parses filenames + timestamps from saved HTML snapshot."""
        parser = BLSDirectoryParser()
        parser.feed(sample_bls_html)
        assert len(parser.files) == 3
        assert parser.files[0]["filename"] == "pr.data.0.Current"
        assert parser.files[0]["timestamp"] == "1/15/2026  8:30 AM"
        assert parser.files[0]["size"] == 123456

    def test_parse_directory_listing_format_change(self):
        """Raises no files if HTML format changes (no <pre> tag)."""
        parser = BLSDirectoryParser()
        parser.feed("<html><body><div>No pre tag here</div></body></html>")
        assert parser.files == []


class TestParseBLSTimestamp:
    def test_parse_bls_timestamp_valid(self):
        """'1/29/2026  8:30 AM' → correct datetime."""
        result = parse_bls_timestamp("1/29/2026  8:30 AM")
        assert result == datetime(2026, 1, 29, 8, 30)

    def test_parse_bls_timestamp_pm(self):
        """PM time parses correctly."""
        result = parse_bls_timestamp("12/31/2025  3:45 PM")
        assert result == datetime(2025, 12, 31, 15, 45)

    def test_parse_bls_timestamp_midnight(self):
        """Midnight (12:00 AM) parses correctly."""
        result = parse_bls_timestamp("1/1/2026  12:00 AM")
        assert result == datetime(2026, 1, 1, 0, 0)

    def test_parse_bls_timestamp_extra_whitespace(self):
        """Extra whitespace is handled."""
        result = parse_bls_timestamp("  1/29/2026   8:30 AM  ")
        assert result == datetime(2026, 1, 29, 8, 30)


class TestNeedsUpdate:
    def test_needs_update_never_synced(self):
        """Returns True when no metadata exists."""
        assert needs_update(datetime(2026, 1, 29), {}) is True

    def test_needs_update_source_newer(self):
        """Returns True when source > stored timestamp."""
        source = datetime(2026, 1, 29, 10, 0)
        metadata = {"source_modified": "2026-01-28T08:30:00"}
        assert needs_update(source, metadata) is True

    def test_needs_update_source_same(self):
        """Returns False when timestamps match."""
        source = datetime(2026, 1, 29, 8, 30)
        metadata = {"source_modified": "2026-01-29T08:30:00"}
        assert needs_update(source, metadata) is False

    def test_needs_update_source_older(self):
        """Returns False when source is older."""
        source = datetime(2026, 1, 28, 8, 30)
        metadata = {"source_modified": "2026-01-29T08:30:00"}
        assert needs_update(source, metadata) is False


class TestSyncState:
    @mock_aws
    def test_load_sync_state_empty(self):
        """Returns default state when no state exists."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        state = load_sync_state(s3, "test-bucket", "pr")
        assert state["series"] == "pr"
        assert state["files"] == {}

    @mock_aws
    def test_save_and_load_sync_state(self):
        """State round-trips correctly through S3."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        state = {
            "series": "pr",
            "last_sync": "2026-01-29T10:00:00",
            "files": {"pr.data.0.Current": {"source_modified": "2026-01-29T08:30:00", "bytes": 123}},
        }
        save_sync_state(s3, "test-bucket", "pr", state)
        loaded = load_sync_state(s3, "test-bucket", "pr")
        assert loaded["series"] == "pr"
        assert "pr.data.0.Current" in loaded["files"]

    @mock_aws
    def test_sync_log_append(self):
        """New entries appended to JSONL."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        append_sync_log(s3, "test-bucket", "pr", {"action": "updated", "file": "a.txt"})
        append_sync_log(s3, "test-bucket", "pr", {"action": "unchanged", "file": "b.txt"})

        response = s3.get_object(Bucket="test-bucket", Key="_sync_state/pr/sync_log.jsonl")
        lines = response["Body"].read().decode().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["file"] == "a.txt"
        assert json.loads(lines[1])["file"] == "b.txt"

    @mock_aws
    def test_state_corruption_recovery(self):
        """Handles malformed state gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        s3.put_object(
            Bucket="test-bucket",
            Key="_sync_state/pr/latest_state.json",
            Body=b"not valid json{{{",
        )
        state = load_sync_state(s3, "test-bucket", "pr")
        assert state["series"] == "pr"


class TestFetchDirectoryListing:
    def test_403_without_user_agent(self, requests_mock):
        """Simulates 403 response."""
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            status_code=403,
        )
        with pytest.raises(Exception):
            fetch_directory_listing("pr")

    def test_success_with_user_agent(self, requests_mock, sample_bls_html):
        """Proper User-Agent header gets 200."""
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            text=sample_bls_html,
        )
        files = fetch_directory_listing("pr")
        assert len(files) == 3
        assert files[0]["filename"] == "pr.data.0.Current"


class TestSyncSeries:
    @mock_aws
    def test_sync_new_files(self, requests_mock, sample_bls_html):
        """Syncs new files from BLS to S3."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            text=sample_bls_html,
        )
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current",
            content=b"data0",
        )
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/pr.data.1.AllData",
            content=b"data1",
        )
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/pr.series",
            content=b"series",
        )

        result = sync_series("pr")
        assert len(result["added"]) == 3
        assert len(result["unchanged"]) == 0

    @mock_aws
    def test_skip_unchanged_files(self, requests_mock, sample_bls_html):
        """No re-upload when timestamps match."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        # Pre-populate with matching metadata
        s3.put_object(
            Bucket="fomc-bls-raw",
            Key="pr/pr.data.0.Current",
            Body=b"data0",
            Metadata={"source_modified": "2026-01-15T08:30:00"},
        )
        s3.put_object(
            Bucket="fomc-bls-raw",
            Key="pr/pr.data.1.AllData",
            Body=b"data1",
            Metadata={"source_modified": "2026-01-10T08:30:00"},
        )
        s3.put_object(
            Bucket="fomc-bls-raw",
            Key="pr/pr.series",
            Body=b"series",
            Metadata={"source_modified": "2026-01-15T08:30:00"},
        )

        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            text=sample_bls_html,
        )

        result = sync_series("pr")
        assert len(result["unchanged"]) == 3
        assert len(result["added"]) == 0
        assert len(result["updated"]) == 0


class TestSyncMultipleSeries:
    @mock_aws
    def test_sync_multiple_series(self, requests_mock, sample_bls_html):
        """Syncs multiple series to correct S3 prefixes."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        for series in ["pr", "cu"]:
            requests_mock.get(
                f"https://download.bls.gov/pub/time.series/{series}/",
                text=sample_bls_html,
            )
            for fn in ["pr.data.0.Current", "pr.data.1.AllData", "pr.series"]:
                requests_mock.get(
                    f"https://download.bls.gov/pub/time.series/{series}/{fn}",
                    content=b"data",
                )

        results = sync_all(series_list=["pr", "cu"])
        assert "pr" in results
        assert "cu" in results
