"""Tests for aws_status.py."""

import json
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.helpers.aws_status import (
    check_s3_status,
    check_sqs_status,
    check_lambda_status,
    check_all_status,
)


@pytest.fixture(autouse=True)
def clear_endpoint():
    """Ensure no endpoint URL interferes with moto."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("AWS_ENDPOINT_URL", None)
        yield


@mock_aws
def test_check_s3_status_empty():
    """No buckets returns empty dict."""
    result = check_s3_status()
    assert result == {}


@mock_aws
def test_check_s3_status_with_buckets():
    """Lists buckets and object counts."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="file1.txt", Body=b"hello")
    s3.put_object(Bucket="test-bucket", Key="file2.txt", Body=b"world")

    result = check_s3_status()
    assert "test-bucket" in result
    assert result["test-bucket"]["object_count"] == 2


@mock_aws
def test_check_sqs_status_with_queues():
    """Lists queues and message counts."""
    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.create_queue(QueueName="test-queue")

    result = check_sqs_status()
    assert "test-queue" in result
    assert result["test-queue"]["message_count"] == 0


@mock_aws
def test_check_lambda_status_with_functions():
    """Lists functions and configs."""
    iam = boto3.client("iam", region_name="us-east-1")
    role = iam.create_role(
        RoleName="test-role",
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }),
        Path="/",
    )
    role_arn = role["Role"]["Arn"]

    lam = boto3.client("lambda", region_name="us-east-1")
    lam.create_function(
        FunctionName="test-func",
        Runtime="python3.12",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": b"fake-code"},
        MemorySize=128,
        Timeout=30,
    )

    result = check_lambda_status()
    assert "test-func" in result
    assert result["test-func"]["runtime"] == "python3.12"
    assert result["test-func"]["memory"] == 128
    assert result["test-func"]["timeout"] == 30


@mock_aws
def test_check_all_status():
    """Combined output of all services."""
    result = check_all_status()
    assert "s3" in result
    assert "sqs" in result
    assert "lambda" in result
