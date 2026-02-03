"""Tests for datausa_getter.py."""

import json
from unittest.mock import patch
import urllib.error

import boto3
import pytest
from moto import mock_aws

from src.data_fetchers.datausa_getter import (
    compute_content_hash,
    fetch_population_data,
    needs_update,
    sync_population_data,
    load_sync_state,
    save_sync_state,
    append_sync_log,
)


class TestComputeContentHash:
    def test_deterministic(self, sample_population_data):
        """Same data → same hash."""
        h1 = compute_content_hash(sample_population_data)
        h2 = compute_content_hash(sample_population_data)
        assert h1 == h2

    def test_different_data(self, sample_population_data):
        """Different data → different hash."""
        h1 = compute_content_hash(sample_population_data)
        modified = {**sample_population_data, "extra": "field"}
        h2 = compute_content_hash(modified)
        assert h1 != h2

    def test_hash_length(self, sample_population_data):
        """Hash is 16 characters."""
        h = compute_content_hash(sample_population_data)
        assert len(h) == 16


class TestNeedsUpdate:
    def test_needs_update_hash_changed(self):
        """Returns True when hash differs."""
        assert needs_update("abc123", {"content_hash": "def456"}) is True

    def test_needs_update_hash_same(self):
        """Returns False when hash matches."""
        assert needs_update("abc123", {"content_hash": "abc123"}) is False

    def test_needs_update_no_stored(self):
        """Returns True when no stored hash."""
        assert needs_update("abc123", {}) is True


class TestFetchPopulationData:
    def test_fetch_population_data(self, sample_population_data):
        """Successful API call returns expected JSON."""
        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            data = fetch_population_data()
        assert "data" in data
        assert len(data["data"]) == 8

    def test_api_timeout(self):
        """Handles timeout with retry."""
        with patch("src.data_fetchers.datausa_getter.fetch_json", side_effect=TimeoutError):
            with pytest.raises(TimeoutError):
                fetch_population_data(retries=1)

    def test_api_500_error(self):
        """Handles server error with retry."""
        err = urllib.error.HTTPError(url="u", code=500, msg="ServerError", hdrs=None, fp=None)
        with patch("src.data_fetchers.datausa_getter.fetch_json", side_effect=err):
            with pytest.raises(urllib.error.HTTPError):
                fetch_population_data(retries=1)

    def test_api_malformed_json(self):
        """Handles invalid JSON response."""
        with patch("src.data_fetchers.datausa_getter.fetch_json", side_effect=json.JSONDecodeError("msg", "doc", 0)):
            with pytest.raises(json.JSONDecodeError):
                fetch_population_data(retries=1)


class TestSyncPopulationData:
    @mock_aws
    def test_save_to_s3(self, sample_population_data):
        """JSON saved correctly to S3 bucket."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            result = sync_population_data()
        assert result["action"] == "updated"
        assert result["record_count"] == 8

        # Verify data in S3
        response = s3.get_object(Bucket="fomc-datausa-raw", Key="population.json")
        data = json.loads(response["Body"].read())
        assert len(data["data"]) == 8

    @mock_aws
    def test_skip_unchanged(self, sample_population_data):
        """Skips upload when content hash matches."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        content_hash = compute_content_hash(sample_population_data)
        s3.put_object(
            Bucket="fomc-datausa-raw",
            Key="population.json",
            Body=json.dumps(sample_population_data).encode(),
            Metadata={"content_hash": content_hash},
        )

        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            result = sync_population_data()
        assert result["action"] == "unchanged"


class TestSyncState:
    @mock_aws
    def test_sync_log_append(self):
        """JSONL log updated."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        append_sync_log(s3, "test-bucket", {"action": "updated"})
        append_sync_log(s3, "test-bucket", {"action": "unchanged"})

        response = s3.get_object(Bucket="test-bucket", Key="_sync_state/sync_log.jsonl")
        lines = response["Body"].read().decode().strip().split("\n")
        assert len(lines) == 2

    @mock_aws
    def test_state_snapshot_write(self):
        """latest_state.json with hash, record count, year range."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        state = {
            "content_hash": "abc123",
            "record_count": 8,
            "year_range": [2013, 2020],
        }
        save_sync_state(s3, "test-bucket", state)
        loaded = load_sync_state(s3, "test-bucket")
        assert loaded["content_hash"] == "abc123"
        assert loaded["record_count"] == 8

    @mock_aws
    def test_first_run_no_existing_state(self):
        """Handles missing state file gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

        state = load_sync_state(s3, "test-bucket")
        assert state == {}
