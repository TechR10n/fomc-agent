"""Boto3 client factory with automatic LocalStack/AWS detection."""

import os

import boto3

DEFAULT_REGION = "us-east-1"


def get_client(service: str):
    """Create a boto3 client for the given service.

    Auto-detects LocalStack vs AWS based on AWS_ENDPOINT_URL env var.
    """
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    kwargs = {"region_name": region}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client(service, **kwargs)


def get_resource(service: str):
    """Create a boto3 resource for the given service."""
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    kwargs = {"region_name": region}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.resource(service, **kwargs)


def is_localstack() -> bool:
    """Check if we're targeting LocalStack."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    return "localstack" in endpoint or "localhost" in endpoint
