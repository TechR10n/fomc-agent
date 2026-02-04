"""Runtime configuration helpers shared across scripts and Lambdas."""

from __future__ import annotations

import os

DEFAULT_BUCKET_PREFIX = "fomc"


def get_bucket_prefix() -> str:
    """Get the shared bucket prefix used across this project."""
    return os.environ.get("FOMC_BUCKET_PREFIX", DEFAULT_BUCKET_PREFIX)


def get_bls_bucket() -> str:
    """Get the S3 bucket used for BLS raw files."""
    return os.environ.get("BLS_BUCKET", f"{get_bucket_prefix()}-bls-raw")


def get_datausa_bucket() -> str:
    """Get the S3 bucket used for DataUSA raw files."""
    return os.environ.get("DATAUSA_BUCKET", f"{get_bucket_prefix()}-datausa-raw")


def get_datausa_key() -> str:
    """Get the S3 key used for the DataUSA population JSON."""
    return os.environ.get("DATAUSA_KEY", "population.json")


def get_bls_series_list(default: str = "pr") -> list[str]:
    """Get the BLS series list (comma-separated) for ingestion."""
    raw = os.environ.get("BLS_SERIES", default)
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def bls_data_key(series_id: str, filename: str | None = None) -> str:
    """Build the default S3 key for a BLS file in a given series."""
    if filename is None:
        filename = f"{series_id}.data.0.Current"
    return f"{series_id}/{filename}"


def get_bls_key(default_series: str = "pr") -> str:
    """Get the S3 key used for analytics reads of BLS data."""
    explicit = os.environ.get("BLS_KEY")
    if explicit:
        return explicit

    series = os.environ.get("BLS_ANALYTICS_SERIES")
    if not series:
        series = get_bls_series_list(default=default_series)[0]
    return bls_data_key(series_id=series)

