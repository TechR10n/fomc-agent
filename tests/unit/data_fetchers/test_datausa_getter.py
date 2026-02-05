"""Tests for src/data_fetchers/datausa_getter.py."""

from __future__ import annotations

import json
from unittest.mock import patch

import boto3
from moto import mock_aws

from src.data_fetchers.datausa_getter import compute_content_hash, sync_all, sync_population_data


class TestComputeContentHash:
    def test_deterministic(self, sample_population_data):
        h1 = compute_content_hash(sample_population_data)
        h2 = compute_content_hash(sample_population_data)
        assert h1 == h2

    def test_different_data(self, sample_population_data):
        h1 = compute_content_hash(sample_population_data)
        modified = {**sample_population_data, "extra": "field"}
        h2 = compute_content_hash(modified)
        assert h1 != h2

    def test_hash_length(self, sample_population_data):
        assert len(compute_content_hash(sample_population_data)) == 16


class TestSyncPopulationData:
    @mock_aws
    def test_save_to_s3_and_state(self, sample_population_data):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            result = sync_population_data(bucket="fomc-datausa-raw")

        assert result["action"] == "updated"
        assert result["record_count"] == 8

        # Raw object
        obj = s3.get_object(Bucket="fomc-datausa-raw", Key="population.json")
        payload = json.loads(obj["Body"].read())
        assert len(payload["data"]) == 8

        # Sync state (kept out of the *.json notification filter)
        state_obj = s3.get_object(
            Bucket="fomc-datausa-raw",
            Key="_sync_state/datausa/population/latest_state.jsonl",
        )
        state = json.loads(state_obj["Body"].read())
        assert state["dataset_id"] == "population"
        assert state["record_count"] == 8
        assert state["year_range"] == [2013, 2020]

        # Sync log exists
        log_obj = s3.get_object(
            Bucket="fomc-datausa-raw",
            Key="_sync_state/datausa/population/sync_log.jsonl",
        )
        lines = log_obj["Body"].read().decode("utf-8").strip().splitlines()
        assert len(lines) >= 1

    @mock_aws
    def test_force_refresh_unchanged_uses_content_hash(self, sample_population_data):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            first = sync_population_data(bucket="fomc-datausa-raw")
        assert first["action"] == "updated"

        with (
            patch.dict("os.environ", {"DATAUSA_FORCE_REFRESH": "1"}),
            patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data),
        ):
            second = sync_population_data(bucket="fomc-datausa-raw")
        assert second["action"] == "unchanged"


class TestSyncAll:
    @mock_aws
    def test_unknown_dataset_is_reported(self, sample_population_data):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        with patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data):
            result = sync_all(["population", "not-a-real-dataset"], bucket="fomc-datausa-raw")

        assert "datasets" in result
        assert "population" in result["datasets"]
        assert any(e.get("dataset_id") == "not-a-real-dataset" for e in result["errors"])

