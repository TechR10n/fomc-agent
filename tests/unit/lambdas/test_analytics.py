"""Tests for Lambda analytics_processor handler."""

import json
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.lambdas.analytics_processor.handler import (
    handler,
    run_reports,
    load_population,
    load_bls_data,
    report_population_stats,
    report_best_year,
    report_series_population,
)


def _setup_s3_data(sample_population_data, sample_bls_csv):
    """Helper to create S3 buckets with test data."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="fomc-bls-raw")
    s3.create_bucket(Bucket="fomc-datausa-raw")

    s3.put_object(
        Bucket="fomc-datausa-raw",
        Key="population.json",
        Body=json.dumps(sample_population_data).encode(),
    )
    s3.put_object(
        Bucket="fomc-bls-raw",
        Key="pr/pr.data.0.Current",
        Body=sample_bls_csv.encode(),
    )
    return s3


class TestHandler:
    @mock_aws
    def test_handler_processes_sqs_event(self, sample_population_data, sample_bls_csv):
        """Parses SQS event, reads S3 data, logs reports."""
        _setup_s3_data(sample_population_data, sample_bls_csv)

        event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "fomc-datausa-raw"},
                                    "object": {"key": "population.json"},
                                }
                            }
                        ]
                    })
                }
            ]
        }

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
        }):
            result = handler(event, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert len(body["results"]) == 1
        assert len(body["errors"]) == 0

    @mock_aws
    def test_handler_multiple_records(self, sample_population_data, sample_bls_csv):
        """Processes batch of SQS messages."""
        _setup_s3_data(sample_population_data, sample_bls_csv)

        event = {
            "Records": [
                {"body": json.dumps({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}]})},
                {"body": json.dumps({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}]})},
            ]
        }

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
        }):
            result = handler(event, None)

        body = json.loads(result["body"])
        assert len(body["results"]) == 2

    @mock_aws
    def test_handler_invalid_message(self, sample_population_data, sample_bls_csv):
        """Handles malformed SQS record gracefully."""
        _setup_s3_data(sample_population_data, sample_bls_csv)

        event = {"Records": [{"body": "not json"}]}

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
        }):
            result = handler(event, None)

        # Should still return (with errors)
        assert result["statusCode"] == 207

    @mock_aws
    def test_handler_s3_read_error(self):
        """Handles missing S3 objects gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")
        # No data uploaded

        event = {
            "Records": [
                {"body": json.dumps({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}]})}
            ]
        }

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
        }):
            result = handler(event, None)

        assert result["statusCode"] == 207


class TestReportPopulationStats:
    def test_report_1_population_stats(self, sample_population_data):
        """Plain Python mean/stddev matches expected."""
        records = sample_population_data["data"]
        result = report_population_stats(records)
        pops = [311536594, 314107084, 316515021, 318558162, 321004407, 322903030]
        expected_mean = sum(pops) / len(pops)
        assert abs(result["mean"] - expected_mean) < 1


class TestReportBestYear:
    def test_report_2_best_year(self, sample_bls_csv):
        """Plain Python CSV aggregation matches expected."""
        import csv
        import io
        reader = csv.DictReader(io.StringIO(sample_bls_csv), delimiter="\t")
        rows = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

        result = report_best_year(rows)
        by_series = {r["series_id"]: r for r in result}
        assert by_series["PRS30006011"]["year"] == 1996
        assert abs(by_series["PRS30006011"]["value"] - 7.0) < 0.1
        assert by_series["PRS30006012"]["year"] == 2000
        assert abs(by_series["PRS30006012"]["value"] - 8.0) < 0.1


class TestReportSeriesJoin:
    def test_report_3_series_join(self, sample_population_data, sample_bls_csv):
        """Plain Python join matches expected."""
        import csv
        import io
        reader = csv.DictReader(io.StringIO(sample_bls_csv), delimiter="\t")
        bls_rows = [{k.strip(): v.strip() for k, v in row.items()} for row in reader]
        pop_records = sample_population_data["data"]

        result = report_series_population(bls_rows, pop_records)
        r2018 = [r for r in result if r["year"] == 2018]
        assert len(r2018) == 1
        assert r2018[0]["Population"] == 322903030
