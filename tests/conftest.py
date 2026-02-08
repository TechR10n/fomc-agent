"""Shared test fixtures."""

import json
import os

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def default_project_env(monkeypatch):
    """Provide deterministic default env vars for tests."""
    defaults = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "FOMC_BUCKET_PREFIX": "fomc",
        "FOMC_ANALYTICS_QUEUE_NAME": "fomc-analytics-queue",
        "FOMC_ANALYTICS_DLQ_NAME": "fomc-analytics-dlq",
        "FOMC_REMOVAL_POLICY": "retain",
        "FOMC_FETCH_INTERVAL_HOURS": "1",
        "BLS_SERIES": "pr,cu,ce,ln,jt,ci",
        "DATAUSA_DATASETS": "population,commute_time,citizenship",
        "DATAUSA_BASE_URL": "https://api.datausa.io/tesseract",
    }
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    """Create a mocked S3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def sqs_client(aws_credentials):
    """Create a mocked SQS client."""
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def lambda_client(aws_credentials):
    """Create a mocked Lambda client."""
    with mock_aws():
        yield boto3.client("lambda", region_name="us-east-1")


@pytest.fixture
def mock_aws_all(aws_credentials):
    """Mock all AWS services together."""
    with mock_aws():
        yield


@pytest.fixture
def sample_population_data():
    """Sample DataUSA population response."""
    return {
        "data": [
            {"Year": 2013, "Nation": "United States", "Population": 311536594},
            {"Year": 2014, "Nation": "United States", "Population": 314107084},
            {"Year": 2015, "Nation": "United States", "Population": 316515021},
            {"Year": 2016, "Nation": "United States", "Population": 318558162},
            {"Year": 2017, "Nation": "United States", "Population": 321004407},
            {"Year": 2018, "Nation": "United States", "Population": 322903030},
            {"Year": 2019, "Nation": "United States", "Population": 324697795},
            {"Year": 2020, "Nation": "United States", "Population": 326569308},
        ]
    }


@pytest.fixture
def sample_bls_csv():
    """Sample BLS tab-delimited data."""
    return (
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "PRS30006011\t1995\tQ01\t1.0\t\n"
        "PRS30006011\t1995\tQ02\t2.0\t\n"
        "PRS30006011\t1996\tQ01\t3.0\t\n"
        "PRS30006011\t1996\tQ02\t4.0\t\n"
        "PRS30006012\t2000\tQ01\t0.0\t\n"
        "PRS30006012\t2000\tQ02\t8.0\t\n"
        "PRS30006012\t2001\tQ01\t2.0\t\n"
        "PRS30006012\t2001\tQ02\t3.0\t\n"
        "PRS30006032\t2018\tQ01\t1.9\t\n"
        "PRS30006032\t2018\tQ02\t2.1\t\n"
    )


@pytest.fixture
def sample_bls_html():
    """Sample BLS directory listing HTML."""
    return """<html>
<head><title>Directory /pub/time.series/pr/</title></head>
<body>
<pre>
<a href="/pub/time.series/">[To Parent Directory]</a>
<a href="pr.data.0.Current">pr.data.0.Current</a>      1/15/2026  8:30 AM       123456
<a href="pr.data.1.AllData">pr.data.1.AllData</a>      1/10/2026  8:30 AM       789012
<a href="pr.series">pr.series</a>              1/15/2026  8:30 AM        34567
</pre>
</body>
</html>"""
