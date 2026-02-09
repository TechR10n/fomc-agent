"""Tests for bls_getter.py."""

import json
import os
import urllib.error
from datetime import datetime
from unittest.mock import patch

import hashlib
import boto3
import pytest
from moto import mock_aws

from src.data_fetchers.bls_getter import (
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
    def test_403_without_user_agent(self):
        """Simulates 403 response."""
        err = urllib.error.HTTPError(url="u", code=403, msg="Forbidden", hdrs=None, fp=None)
        with patch("src.data_fetchers.bls_getter.fetch_text", side_effect=err):
            with pytest.raises(urllib.error.HTTPError):
                fetch_directory_listing("pr")

    def test_success_with_user_agent(self, sample_bls_html):
        """Proper User-Agent header gets 200."""
        with patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html):
            files = fetch_directory_listing("pr")
        assert len(files) == 3
        assert files[0]["filename"] == "pr.data.0.Current"
        assert files[0]["timestamp"].startswith("1/15/2026")
        assert files[0]["size"] == 123456


class TestSyncSeries:
    @mock_aws
    def test_sync_new_files(self, sample_bls_html):
        """Syncs new files from BLS to S3."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        def _fetch_bytes(url: str, **_kwargs):
            if url.endswith("/pr/pr.data.0.Current"):
                return b"data0"
            if url.endswith("/pr/pr.data.1.AllData"):
                return b"data1"
            if url.endswith("/pr/pr.series"):
                return b"series"
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
        ):
            result = sync_series("pr")
        # Default behavior only syncs `{series}.data.0.Current` for speed.
        assert len(result["added"]) == 1
        assert len(result["unchanged"]) == 0

    @mock_aws
    def test_skip_unchanged_files(self, sample_bls_html):
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

        with patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html):
            result = sync_series("pr")
        assert len(result["unchanged"]) == 1
        assert len(result["added"]) == 0
        assert len(result["updated"]) == 0

    @mock_aws
    def test_sync_all_files_when_bls_file_patterns_empty(self, sample_bls_html, monkeypatch):
        """Empty BLS_FILE_PATTERNS syncs all files in the directory listing."""
        monkeypatch.setenv("BLS_FILE_PATTERNS", "")

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        def _fetch_bytes(url: str, **_kwargs):
            if url.endswith("/pr/pr.data.0.Current"):
                return b"data0"
            if url.endswith("/pr/pr.data.1.AllData"):
                return b"data1"
            if url.endswith("/pr/pr.series"):
                return b"series"
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
        ):
            result = sync_series("pr")

        assert len(result["added"]) == 3

    @mock_aws
    def test_sync_ln_series_via_bls_api(self, monkeypatch):
        """LN is fetched via BLS API into a small `ln/ln.data.0.Current` TSV."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        monkeypatch.setenv("BLS_BUCKET", "fomc-bls-raw")
        monkeypatch.setenv("BLS_LN_START_YEAR", "2024")
        monkeypatch.setenv("BLS_LN_END_YEAR", "2024")
        monkeypatch.setenv("BLS_API_MAX_YEARS_PER_REQUEST", "20")

        api_payload = {
            "status": "REQUEST_SUCCEEDED",
            "message": [],
            "Results": {
                "series": [
                    {
                        "seriesID": "LNS14000000",
                        "data": [
                            {"year": "2024", "period": "M01", "value": "3.7", "footnotes": [{}]},
                        ],
                    },
                    {
                        "seriesID": "LNS11300000",
                        "data": [
                            {"year": "2024", "period": "M01", "value": "62.5", "footnotes": [{}]},
                        ],
                    },
                ]
            },
        }

        with patch("src.data_fetchers.bls_getter.post_json", return_value=api_payload):
            result = sync_series("ln")

        assert result["added"] == ["ln.data.0.Current"]
        obj = s3.get_object(Bucket="fomc-bls-raw", Key="ln/ln.data.0.Current")
        body = obj["Body"].read().decode("utf-8")
        assert body.startswith("series_id\tyear\tperiod\tvalue\tfootnote_codes\n")
        assert "LNS14000000\t2024\tM01\t3.7" in body
        assert "LNS11300000\t2024\tM01\t62.5" in body
        assert obj["Metadata"].get("source") == "bls_api"
        assert obj["Metadata"].get("content_hash")

    @mock_aws
    def test_sync_ln_series_chunks_year_ranges_by_default(self, monkeypatch):
        """LN API sync defaults to safe chunking so mid-range years aren't skipped."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        monkeypatch.setenv("BLS_BUCKET", "fomc-bls-raw")
        monkeypatch.setenv("BLS_LN_START_YEAR", "2005")
        monkeypatch.setenv("BLS_LN_END_YEAR", "2026")
        monkeypatch.delenv("BLS_API_MAX_YEARS_PER_REQUEST", raising=False)
        monkeypatch.delenv("BLS_API_KEY", raising=False)

        calls: list[dict] = []

        def _post_json(_url: str, payload: dict, **_kwargs):
            calls.append(payload)
            start = str(payload.get("startyear"))
            series = []
            for sid in payload.get("seriesid", []) or []:
                series.append(
                    {
                        "seriesID": sid,
                        "data": [{"year": start, "period": "M01", "value": "1.0", "footnotes": [{}]}],
                    }
                )
            return {"status": "REQUEST_SUCCEEDED", "message": [], "Results": {"series": series}}

        with patch("src.data_fetchers.bls_getter.post_json", side_effect=_post_json):
            result = sync_series("ln")

        assert result["added"] == ["ln.data.0.Current"]
        assert [(c.get("startyear"), c.get("endyear")) for c in calls] == [
            ("2005", "2014"),
            ("2015", "2024"),
            ("2025", "2026"),
        ]

        body = s3.get_object(Bucket="fomc-bls-raw", Key="ln/ln.data.0.Current")["Body"].read().decode("utf-8")
        # Ensure we got rows from each chunk (year chosen here is the chunk start year).
        assert "\t2005\tM01\t" in body
        assert "\t2015\tM01\t" in body
        assert "\t2025\tM01\t" in body

    @mock_aws
    def test_sync_ln_series_skips_unchanged(self, monkeypatch):
        """LN API sync does not re-upload when content hash matches."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        monkeypatch.setenv("BLS_BUCKET", "fomc-bls-raw")
        monkeypatch.setenv("BLS_LN_START_YEAR", "2024")
        monkeypatch.setenv("BLS_LN_END_YEAR", "2024")
        monkeypatch.setenv("BLS_API_MAX_YEARS_PER_REQUEST", "20")

        api_payload = {
            "status": "REQUEST_SUCCEEDED",
            "message": [],
            "Results": {
                "series": [
                    {
                        "seriesID": "LNS14000000",
                        "data": [
                            {"year": "2024", "period": "M01", "value": "3.7", "footnotes": [{}]},
                        ],
                    },
                ]
            },
        }

        # Render the expected TSV body to compute the same content hash.
        expected_body = (
            "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
            "LNS14000000\t2024\tM01\t3.7\t\n"
        ).encode("utf-8")
        expected_hash = hashlib.sha256(expected_body).hexdigest()[:16]

        s3.put_object(
            Bucket="fomc-bls-raw",
            Key="ln/ln.data.0.Current",
            Body=expected_body,
            Metadata={"content_hash": expected_hash},
        )

        with patch("src.data_fetchers.bls_getter.post_json", return_value=api_payload):
            result = sync_series("ln")

        assert result["unchanged"] == ["ln.data.0.Current"]


class TestSyncMultipleSeries:
    @mock_aws
    def test_sync_multiple_series(self, sample_bls_html):
        """Syncs multiple series to correct S3 prefixes."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")

        def _fetch_text(url: str, **_kwargs):
            if url.endswith("/pr/"):
                return sample_bls_html
            if url.endswith("/cu/"):
                return (
                    sample_bls_html.replace("/pr/", "/cu/")
                    .replace("pr.data.0.Current", "cu.data.0.Current")
                    .replace("pr.data.1.AllData", "cu.data.1.AllItems")
                    .replace("pr.series", "cu.series")
                )
            raise AssertionError(f"Unexpected URL: {url}")

        def _fetch_bytes(url: str, **_kwargs):
            if "/pr/" in url or "/cu/" in url:
                return b"data"
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch("src.data_fetchers.bls_getter.fetch_text", side_effect=_fetch_text),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
        ):
            results = sync_all(series_list=["pr", "cu"])
        assert "pr" in results
        assert "cu" in results

        # Default patterns should have synced the "data.0.Current" file for each series.
        assert s3.get_object(Bucket="fomc-bls-raw", Key="pr/pr.data.0.Current")["Body"].read()
        assert s3.get_object(Bucket="fomc-bls-raw", Key="cu/cu.data.0.Current")["Body"].read()
