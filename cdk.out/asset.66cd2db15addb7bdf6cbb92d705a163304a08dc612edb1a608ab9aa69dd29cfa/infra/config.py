"""CDK environment configuration (AWS-only)."""

import os

DEFAULT_REGION = "us-east-1"

def get_env_config() -> dict:
    """Get environment-specific configuration."""
    removal_policy = os.environ.get("FOMC_REMOVAL_POLICY", "destroy").lower()
    if removal_policy not in ("destroy", "retain"):
        removal_policy = "destroy"
    return {
        "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
        "region": os.environ.get("CDK_DEFAULT_REGION", DEFAULT_REGION),
        "bucket_prefix": os.environ.get("FOMC_BUCKET_PREFIX", "fomc"),
        "removal_policy": removal_policy,
    }
