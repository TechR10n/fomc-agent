"""CDK environment configuration (AWS-only)."""

import os

REQUIRED_KEYS = (
    "AWS_DEFAULT_REGION",
    "FOMC_BUCKET_PREFIX",
    "FOMC_ANALYTICS_QUEUE_NAME",
    "FOMC_ANALYTICS_DLQ_NAME",
    "FOMC_REMOVAL_POLICY",
    "FOMC_FETCH_INTERVAL_HOURS",
    "BLS_SERIES",
    "DATAUSA_DATASETS",
)


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _required_positive_int(name: str) -> int:
    raw = _required(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer, got: {raw}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {raw}")
    return value


def _required_removal_policy() -> str:
    value = _required("FOMC_REMOVAL_POLICY").lower()
    if value not in {"destroy", "retain"}:
        raise ValueError("FOMC_REMOVAL_POLICY must be one of: destroy, retain")
    return value


def _get_csv(name: str) -> list[str]:
    raw = os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def get_env_config() -> dict:
    """Get environment-specific configuration."""
    for key in REQUIRED_KEYS:
        _required(key)

    site_domain = os.environ.get("FOMC_SITE_DOMAIN", "").strip()
    site_aliases = _get_csv("FOMC_SITE_ALIASES")
    if site_domain and site_domain not in site_aliases:
        site_aliases = [site_domain, *site_aliases]

    return {
        "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
        "region": _required("AWS_DEFAULT_REGION"),
        "bucket_prefix": _required("FOMC_BUCKET_PREFIX"),
        "analytics_queue_name": _required("FOMC_ANALYTICS_QUEUE_NAME"),
        "analytics_dlq_name": _required("FOMC_ANALYTICS_DLQ_NAME"),
        "removal_policy": _required_removal_policy(),
        "fetch_interval_hours": _required_positive_int("FOMC_FETCH_INTERVAL_HOURS"),
        "bls_series": _required("BLS_SERIES"),
        "datausa_datasets": _required("DATAUSA_DATASETS"),
        "site_domain": site_domain,
        "site_aliases": site_aliases,
        "site_cert_arn": os.environ.get("FOMC_SITE_CERT_ARN", "").strip(),
    }
