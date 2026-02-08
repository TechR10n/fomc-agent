"""Runtime configuration helpers shared across scripts and Lambdas."""

from __future__ import annotations

import os


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_bucket_prefix() -> str:
    """Get the shared bucket prefix used across this project."""
    return _required_env("FOMC_BUCKET_PREFIX")


def get_bls_bucket() -> str:
    """Get the S3 bucket used for BLS raw files."""
    explicit = os.environ.get("BLS_BUCKET", "").strip()
    if explicit:
        return explicit
    return f"{get_bucket_prefix()}-bls-raw"


def get_datausa_bucket() -> str:
    """Get the S3 bucket used for DataUSA raw files."""
    explicit = os.environ.get("DATAUSA_BUCKET", "").strip()
    if explicit:
        return explicit
    return f"{get_bucket_prefix()}-datausa-raw"


def get_datausa_key() -> str:
    """Get the S3 key used for the DataUSA population JSON."""
    return os.environ.get("DATAUSA_KEY", "population.json")


def get_analytics_queue_name() -> str:
    """Get the analytics SQS queue name."""
    return _required_env("FOMC_ANALYTICS_QUEUE_NAME")


def get_analytics_dlq_name() -> str:
    """Get the analytics dead-letter queue name."""
    return _required_env("FOMC_ANALYTICS_DLQ_NAME")


def get_datausa_datasets(default: str | None = None) -> list[str]:
    """Get the DataUSA dataset ids to ingest (comma-separated)."""
    raw = os.environ.get("DATAUSA_DATASETS", "").strip()
    if not raw:
        if default is None:
            raise RuntimeError("Missing required environment variable: DATAUSA_DATASETS")
        raw = default
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def get_bls_processed_bucket() -> str:
    """Get the S3 bucket used for BLS parsed/cleaned (processed) data."""
    explicit = os.environ.get("BLS_PROCESSED_BUCKET", "").strip()
    if explicit:
        return explicit
    return f"{get_bucket_prefix()}-bls-processed"


def get_datausa_processed_bucket() -> str:
    """Get the S3 bucket used for DataUSA parsed/cleaned (processed) data."""
    explicit = os.environ.get("DATAUSA_PROCESSED_BUCKET", "").strip()
    if explicit:
        return explicit
    return f"{get_bucket_prefix()}-datausa-processed"


def get_bls_series_list(default: str | None = None) -> list[str]:
    """Get the BLS series list (comma-separated) for ingestion."""
    raw = os.environ.get("BLS_SERIES", "").strip()
    if not raw:
        if default is None:
            raise RuntimeError("Missing required environment variable: BLS_SERIES")
        raw = default
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def bls_data_key(series_id: str, filename: str | None = None) -> str:
    """Build the default S3 key for a BLS file in a given series."""
    if filename is None:
        filename = f"{series_id}.data.0.Current"
    return f"{series_id}/{filename}"


def get_bls_key(default_series: str | None = None) -> str:
    """Get the S3 key used for analytics reads of BLS data."""
    explicit = os.environ.get("BLS_KEY")
    if explicit:
        return explicit

    series = os.environ.get("BLS_ANALYTICS_SERIES")
    if not series:
        series_list = get_bls_series_list(default=default_series)
        if not series_list:
            raise RuntimeError("BLS_SERIES must contain at least one series id")
        series = series_list[0]
    return bls_data_key(series_id=series)
