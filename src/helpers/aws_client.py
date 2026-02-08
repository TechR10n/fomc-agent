"""Boto3 client/resource factory for AWS and LocalStack."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3
from botocore.config import Config


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _region() -> str:
    return _required_env("AWS_DEFAULT_REGION")


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
    kwargs = {
        "aws_access_key_id": _required_env("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": _required_env("AWS_SECRET_ACCESS_KEY"),
    }
    session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()
    if session_token:
        kwargs["aws_session_token"] = session_token
    return kwargs


def _service_config(service: str, endpoint: str | None) -> Config | None:
    if service != "s3":
        return None

    addressing = os.environ.get("AWS_S3_ADDRESSING_STYLE", "").strip().lower()
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
