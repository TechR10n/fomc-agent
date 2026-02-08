"""CDK environment configuration (AWS-only)."""

import os

DEFAULT_REGION = "us-east-1"
DEFAULT_FETCH_INTERVAL_HOURS = 1


def _get_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _get_csv(name: str) -> list[str]:
    raw = os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def get_env_config() -> dict:
    """Get environment-specific configuration."""
    removal_policy = os.environ.get("FOMC_REMOVAL_POLICY", "destroy").lower()
    if removal_policy not in ("destroy", "retain"):
        removal_policy = "destroy"

    site_domain = os.environ.get("FOMC_SITE_DOMAIN", "").strip()
    site_aliases = _get_csv("FOMC_SITE_ALIASES")
    if site_domain and site_domain not in site_aliases:
        site_aliases = [site_domain, *site_aliases]

    return {
        "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
        "region": os.environ.get("CDK_DEFAULT_REGION", DEFAULT_REGION),
        "bucket_prefix": os.environ.get("FOMC_BUCKET_PREFIX", "fomc"),
        "analytics_queue_name": os.environ.get("FOMC_ANALYTICS_QUEUE_NAME", "fomc-analytics-queue"),
        "analytics_dlq_name": os.environ.get("FOMC_ANALYTICS_DLQ_NAME", "fomc-analytics-dlq"),
        "removal_policy": removal_policy,
        "fetch_interval_hours": _get_positive_int(
            "FOMC_FETCH_INTERVAL_HOURS",
            DEFAULT_FETCH_INTERVAL_HOURS,
        ),
        "site_domain": site_domain,
        "site_aliases": site_aliases,
        "site_cert_arn": os.environ.get("FOMC_SITE_CERT_ARN", "").strip(),
    }
