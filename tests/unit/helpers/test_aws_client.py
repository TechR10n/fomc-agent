"""Tests for aws_client.py."""

import os
from unittest.mock import patch

import boto3
from moto import mock_aws

from src.helpers.aws_client import get_client, get_resource, is_localstack


class TestGetClient:
    def test_get_client_without_endpoint_url(self):
        """Returns default AWS client when no endpoint URL is set."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("AWS_ENDPOINT_URL", None)
            client = get_client("s3")
            assert client is not None
            assert client.meta.service_model.service_name == "s3"

    def test_get_client_with_endpoint_url(self):
        """Returns client pointing to LocalStack when endpoint URL is set."""
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}):
            client = get_client("s3")
            assert client is not None
            assert client.meta.endpoint_url == "http://localhost:4566"

    def test_endpoint_url_from_env_var(self):
        """Reads AWS_ENDPOINT_URL from environment."""
        url = "http://localhost.localstack.cloud:4566"
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": url, "AWS_DEFAULT_REGION": "us-east-1"}):
            client = get_client("sqs")
            assert client.meta.endpoint_url == url


class TestGetResource:
    def test_get_resource_without_endpoint(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("AWS_ENDPOINT_URL", None)
            resource = get_resource("s3")
            assert resource is not None

    def test_get_resource_with_endpoint(self):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}):
            resource = get_resource("s3")
            assert resource is not None


class TestIsLocalstack:
    def test_is_localstack_true(self):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost.localstack.cloud:4566"}):
            assert is_localstack() is True

    def test_is_localstack_localhost(self):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://localhost:4566"}):
            assert is_localstack() is True

    def test_is_localstack_false(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("AWS_ENDPOINT_URL", None)
            assert is_localstack() is False
