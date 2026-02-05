"""Tests for additional curated payload builders in src/analytics/reports.py."""

from __future__ import annotations

from pathlib import Path

import boto3
from moto import mock_aws

from src.analytics.reports import build_participation_vs_noncitizen_share, build_unemployment_vs_commute_time


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


@mock_aws
def test_build_unemployment_vs_commute_time_from_raw_s3_fixtures():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bls")
    s3.create_bucket(Bucket="datausa")

    s3.put_object(
        Bucket="bls",
        Key="ln/ln.data.0.Current",
        Body=(FIXTURES / "sample_ln.tsv").read_bytes(),
    )
    s3.put_object(
        Bucket="datausa",
        Key="commute_time.json",
        Body=(FIXTURES / "sample_commute_time.json").read_bytes(),
        ContentType="application/json",
    )

    payload = build_unemployment_vs_commute_time(bls_bucket="bls", datausa_bucket="datausa")
    assert payload["points"]
    assert len(payload["points"]) == 5
    assert payload["points"][0]["year"] == 2019
    assert payload["points"][-1]["year"] == 2023

    # Basic sanity: values are present and numeric-ish.
    assert payload["points"][0]["unemployment_rate"] is not None
    assert payload["points"][0]["mean_commute_minutes"] is not None


@mock_aws
def test_build_participation_vs_noncitizen_share_from_raw_s3_fixtures():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bls")
    s3.create_bucket(Bucket="datausa")

    s3.put_object(
        Bucket="bls",
        Key="ln/ln.data.0.Current",
        Body=(FIXTURES / "sample_ln.tsv").read_bytes(),
    )
    s3.put_object(
        Bucket="datausa",
        Key="citizenship.json",
        Body=(FIXTURES / "sample_citizenship.json").read_bytes(),
        ContentType="application/json",
    )

    payload = build_participation_vs_noncitizen_share(bls_bucket="bls", datausa_bucket="datausa")
    assert payload["points"]
    assert len(payload["points"]) == 5

    first = payload["points"][0]
    assert first["year"] == 2019
    assert first["participation_rate"] is not None
    assert first["noncitizen_share"] is not None

    # 2019: 22.5M / (304.5M + 22.5M) ≈ 6.88%
    assert abs(float(first["noncitizen_share"]) - 6.88) < 0.2
