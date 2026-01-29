"""Tests for Lambda data_fetcher handler."""

import json
import os
from unittest.mock import patch, MagicMock

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def clear_endpoint():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AWS_ENDPOINT_URL", None)
        yield


class TestLambdaHandler:
    @mock_aws
    def test_lambda_handler_success(self, requests_mock, sample_bls_html, sample_population_data):
        """Handler invokes BLS + DataUSA fetchers."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        # Mock BLS
        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            text=sample_bls_html,
        )
        for fn in ["pr.data.0.Current", "pr.data.1.AllData", "pr.series"]:
            requests_mock.get(
                f"https://download.bls.gov/pub/time.series/pr/{fn}",
                content=b"data",
            )

        # Mock DataUSA
        requests_mock.get(
            "https://honolulu-api.datausa.io/tesseract/data.jsonrecords",
            json=sample_population_data,
        )

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
            "BLS_SERIES": "pr",
        }):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["bls"] is not None
        assert body["datausa"] is not None
        assert len(body["errors"]) == 0

    @mock_aws
    def test_lambda_handler_bls_failure(self, requests_mock, sample_population_data):
        """Handles BLS fetch error gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            status_code=500,
        )
        requests_mock.get(
            "https://honolulu-api.datausa.io/tesseract/data.jsonrecords",
            json=sample_population_data,
        )

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
            "BLS_SERIES": "pr",
        }):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert result["statusCode"] == 207
        body = json.loads(result["body"])
        assert len(body["errors"]) == 1
        assert body["errors"][0]["source"] == "bls"

    @mock_aws
    def test_lambda_handler_datausa_failure(self, requests_mock, sample_bls_html):
        """Handles API error gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        requests_mock.get(
            "https://download.bls.gov/pub/time.series/pr/",
            text=sample_bls_html,
        )
        for fn in ["pr.data.0.Current", "pr.data.1.AllData", "pr.series"]:
            requests_mock.get(
                f"https://download.bls.gov/pub/time.series/pr/{fn}",
                content=b"data",
            )
        requests_mock.get(
            "https://honolulu-api.datausa.io/tesseract/data.jsonrecords",
            status_code=500,
        )

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
            "BLS_SERIES": "pr",
        }):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert result["statusCode"] == 207
        body = json.loads(result["body"])
        assert any(e["source"] == "datausa" for e in body["errors"])

    def test_lambda_environment_variables(self):
        """Reads bucket names from env vars."""
        with patch.dict(os.environ, {
            "BLS_BUCKET": "custom-bls",
            "DATAUSA_BUCKET": "custom-datausa",
            "BLS_SERIES": "pr,cu",
        }):
            assert os.environ["BLS_BUCKET"] == "custom-bls"
            assert os.environ["DATAUSA_BUCKET"] == "custom-datausa"
            assert os.environ["BLS_SERIES"].split(",") == ["pr", "cu"]

    @mock_aws
    def test_lambda_response_format(self, requests_mock, sample_bls_html, sample_population_data):
        """Returns proper status code and body."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        requests_mock.get("https://download.bls.gov/pub/time.series/pr/", text=sample_bls_html)
        for fn in ["pr.data.0.Current", "pr.data.1.AllData", "pr.series"]:
            requests_mock.get(f"https://download.bls.gov/pub/time.series/pr/{fn}", content=b"data")
        requests_mock.get("https://honolulu-api.datausa.io/tesseract/data.jsonrecords", json=sample_population_data)

        with patch.dict(os.environ, {
            "BLS_BUCKET": "fomc-bls-raw",
            "DATAUSA_BUCKET": "fomc-datausa-raw",
            "BLS_SERIES": "pr",
        }):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert "statusCode" in result
        assert "body" in result
        body = json.loads(result["body"])
        assert "bls" in body
        assert "datausa" in body
        assert "errors" in body
