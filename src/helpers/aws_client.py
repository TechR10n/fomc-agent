"""Boto3 client/resource factory for AWS and LocalStack."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"


def _region() -> str:
    return (
        os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or DEFAULT_REGION
    )


def _service_endpoint(service: str) -> str | None:
    specific_key = f"AWS_ENDPOINT_URL_{service.replace('-', '_').upper()}"
    return os.environ.get(specific_key) or os.environ.get("AWS_ENDPOINT_URL")


def _is_local_endpoint(endpoint: str | None) -> bool:
    if not endpoint:
        return False
    try:
        host = (urlparse(endpoint).hostname or "").lower()
    except Exception:
        return False
    if not host:
        return False
    return host in {"localhost", "127.0.0.1", "::1", "localstack"} or "localstack" in host


def _local_auth_kwargs(endpoint: str | None) -> dict[str, str]:
    if not _is_local_endpoint(endpoint):
        return {}
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        # LocalStack accepts any token value; keep deterministic default.
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN", "test"),
    }


def _service_config(service: str, endpoint: str | None) -> Config | None:
    if service != "s3":
        return None

    addressing = os.environ.get("AWS_S3_ADDRESSING_STYLE", "").strip().lower()
    if not addressing and _is_local_endpoint(endpoint):
        addressing = "path"

    if addressing in {"path", "virtual", "auto"}:
        return Config(s3={"addressing_style": addressing})
    return None


def get_client(service: str):
    """Create a boto3 client for the given service."""
    endpoint = _service_endpoint(service)
    kwargs = {
        "region_name": _region(),
        "endpoint_url": endpoint,
        **_local_auth_kwargs(endpoint),
    }
    config = _service_config(service, endpoint)
    if config is not None:
        kwargs["config"] = config
    return boto3.client(service, **kwargs)


def get_resource(service: str):
    """Create a boto3 resource for the given service."""
    endpoint = _service_endpoint(service)
    kwargs = {
        "region_name": _region(),
        "endpoint_url": endpoint,
        **_local_auth_kwargs(endpoint),
    }
    config = _service_config(service, endpoint)
    if config is not None:
        kwargs["config"] = config
    return boto3.resource(service, **kwargs)
