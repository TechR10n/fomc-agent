"""CDK environment detection and configuration."""

import os


def is_local() -> bool:
    """Check if deploying to LocalStack."""
    return os.environ.get("CDK_LOCAL", "").lower() in ("true", "1")


def get_env_config() -> dict:
    """Get environment-specific configuration."""
    if is_local():
        return {
            "account": "000000000000",
            "region": "us-east-1",
            "bucket_prefix": "fomc",
            "removal_policy": "destroy",
        }
    return {
        "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
        "region": os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
        "bucket_prefix": "fomc",
        "removal_policy": "destroy",
    }
