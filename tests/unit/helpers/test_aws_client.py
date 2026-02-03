"""Tests for aws_client.py."""

import os
from unittest.mock import patch

from src.helpers.aws_client import get_client, get_resource


class TestGetClient:
    def test_get_client_defaults_region(self):
        """Uses default region when AWS_DEFAULT_REGION is unset."""
        with patch.dict(os.environ, {}, clear=True):
            client = get_client("s3")
            assert client is not None
            assert client.meta.service_model.service_name == "s3"
            assert client.meta.region_name == "us-east-1"

    def test_get_client_uses_region_from_env(self):
        with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True):
            client = get_client("s3")
            assert client is not None
            assert client.meta.region_name == "us-west-2"


class TestGetResource:
    def test_get_resource_defaults_region(self):
        with patch.dict(os.environ, {}, clear=True):
            resource = get_resource("s3")
            assert resource is not None
            assert resource.meta.client.meta.region_name == "us-east-1"

    def test_get_resource_uses_region_from_env(self):
        with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True):
            resource = get_resource("s3")
            assert resource is not None
            assert resource.meta.client.meta.region_name == "us-west-2"
