"""Boto3 client/resource factory for AWS."""

import os

import boto3

DEFAULT_REGION = "us-east-1"


def get_client(service: str):
    """Create a boto3 client for the given service.
    """
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    return boto3.client(service, region_name=region)


def get_resource(service: str):
    """Create a boto3 resource for the given service."""
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    return boto3.resource(service, region_name=region)
