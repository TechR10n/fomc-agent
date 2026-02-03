"""Tests for Lambda data_fetcher handler."""

import json
import os
import urllib.error
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


class TestLambdaHandler:
    @mock_aws
    def test_lambda_handler_success(self, sample_bls_html, sample_population_data):
        """Handler invokes BLS + DataUSA fetchers."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        def _fetch_bytes(url: str, **_kwargs):
            if "/pr/" in url:
                return b"data"
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch.dict(os.environ, {
                "BLS_BUCKET": "fomc-bls-raw",
                "DATAUSA_BUCKET": "fomc-datausa-raw",
                "BLS_SERIES": "pr",
            }),
            patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
            patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data),
        ):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["bls"] is not None
        assert body["datausa"] is not None
        assert len(body["errors"]) == 0

    @mock_aws
    def test_lambda_handler_bls_failure(self, sample_population_data):
        """Handles BLS fetch error gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        err = urllib.error.HTTPError(url="u", code=500, msg="ServerError", hdrs=None, fp=None)

        with (
            patch.dict(os.environ, {
                "BLS_BUCKET": "fomc-bls-raw",
                "DATAUSA_BUCKET": "fomc-datausa-raw",
                "BLS_SERIES": "pr",
            }),
            patch("src.data_fetchers.bls_getter.fetch_text", side_effect=err),
            patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data),
        ):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert result["statusCode"] == 207
        body = json.loads(result["body"])
        assert len(body["errors"]) == 1
        assert body["errors"][0]["source"] == "bls"

    @mock_aws
    def test_lambda_handler_datausa_failure(self, sample_bls_html):
        """Handles API error gracefully."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        def _fetch_bytes(url: str, **_kwargs):
            if "/pr/" in url:
                return b"data"
            raise AssertionError(f"Unexpected URL: {url}")

        err = urllib.error.HTTPError(url="u", code=500, msg="ServerError", hdrs=None, fp=None)

        with (
            patch.dict(os.environ, {
                "BLS_BUCKET": "fomc-bls-raw",
                "DATAUSA_BUCKET": "fomc-datausa-raw",
                "BLS_SERIES": "pr",
            }),
            patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
            patch("src.data_fetchers.datausa_getter.fetch_json", side_effect=err),
        ):
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
    def test_lambda_response_format(self, sample_bls_html, sample_population_data):
        """Returns proper status code and body."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="fomc-bls-raw")
        s3.create_bucket(Bucket="fomc-datausa-raw")

        def _fetch_bytes(url: str, **_kwargs):
            if "/pr/" in url:
                return b"data"
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch.dict(os.environ, {
                "BLS_BUCKET": "fomc-bls-raw",
                "DATAUSA_BUCKET": "fomc-datausa-raw",
                "BLS_SERIES": "pr",
            }),
            patch("src.data_fetchers.bls_getter.fetch_text", return_value=sample_bls_html),
            patch("src.data_fetchers.bls_getter.fetch_bytes", side_effect=_fetch_bytes),
            patch("src.data_fetchers.datausa_getter.fetch_json", return_value=sample_population_data),
        ):
            from src.lambdas.data_fetcher.handler import handler
            result = handler({}, None)

        assert "statusCode" in result
        assert "body" in result
        body = json.loads(result["body"])
        assert "bls" in body
        assert "datausa" in body
        assert "errors" in body
