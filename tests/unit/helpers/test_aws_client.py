"""Tests for aws_client.py."""

import os
from unittest.mock import patch

from botocore.config import Config

from src.helpers.aws_client import get_client, get_resource


class TestGetClient:
    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_defaults_region(self, mock_client):
        with patch.dict(os.environ, {}, clear=True):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["region_name"] == "us-east-1"
        assert kwargs["endpoint_url"] is None

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_uses_region_from_env(self, mock_client):
        with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["region_name"] == "us-west-2"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_uses_aws_region_fallback(self, mock_client):
        with patch.dict(os.environ, {"AWS_REGION": "us-west-1"}, clear=True):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["region_name"] == "us-west-1"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_uses_service_specific_endpoint(self, mock_client):
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT_URL": "http://localhost:4566",
                "AWS_ENDPOINT_URL_SQS": "http://localhost:4567",
            },
            clear=True,
        ):
            get_client("sqs")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["endpoint_url"] == "http://localhost:4567"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_injects_local_credentials_for_local_endpoint(self, mock_client):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}, clear=True):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["aws_access_key_id"] == "test"
        assert kwargs["aws_secret_access_key"] == "test"
        assert kwargs["aws_session_token"] == "test"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_preserves_explicit_local_credentials(self, mock_client):
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT_URL": "http://localhost:4566",
                "AWS_ACCESS_KEY_ID": "abc",
                "AWS_SECRET_ACCESS_KEY": "def",
                "AWS_SESSION_TOKEN": "ghi",
            },
            clear=True,
        ):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert kwargs["aws_access_key_id"] == "abc"
        assert kwargs["aws_secret_access_key"] == "def"
        assert kwargs["aws_session_token"] == "ghi"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_uses_path_style_s3_for_localstack(self, mock_client):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}, clear=True):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert isinstance(kwargs["config"], Config)
        assert kwargs["config"].s3.get("addressing_style") == "path"

    @patch("src.helpers.aws_client.boto3.client")
    def test_get_client_allows_s3_addressing_style_override(self, mock_client):
        with patch.dict(
            os.environ,
            {
                "AWS_ENDPOINT_URL": "http://localhost:4566",
                "AWS_S3_ADDRESSING_STYLE": "virtual",
            },
            clear=True,
        ):
            get_client("s3")

        kwargs = mock_client.call_args.kwargs
        assert isinstance(kwargs["config"], Config)
        assert kwargs["config"].s3.get("addressing_style") == "virtual"


class TestGetResource:
    @patch("src.helpers.aws_client.boto3.resource")
    def test_get_resource_defaults_region(self, mock_resource):
        with patch.dict(os.environ, {}, clear=True):
            get_resource("s3")

        kwargs = mock_resource.call_args.kwargs
        assert kwargs["region_name"] == "us-east-1"
        assert kwargs["endpoint_url"] is None

    @patch("src.helpers.aws_client.boto3.resource")
    def test_get_resource_uses_region_from_env(self, mock_resource):
        with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True):
            get_resource("s3")

        kwargs = mock_resource.call_args.kwargs
        assert kwargs["region_name"] == "us-west-2"

    @patch("src.helpers.aws_client.boto3.resource")
    def test_get_resource_local_endpoint_credentials(self, mock_resource):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}, clear=True):
            get_resource("s3")

        kwargs = mock_resource.call_args.kwargs
        assert kwargs["endpoint_url"] == "http://localhost:4566"
        assert kwargs["aws_access_key_id"] == "test"
        assert kwargs["aws_secret_access_key"] == "test"
