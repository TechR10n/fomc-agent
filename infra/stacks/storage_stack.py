"""S3 storage stack for FOMC data pipeline."""

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct

from infra.config import get_env_config


class FomcStorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_env_config()
        prefix = config["bucket_prefix"]

        removal = RemovalPolicy.DESTROY if config["removal_policy"] == "destroy" else RemovalPolicy.RETAIN

        bucket_names = [
            f"{prefix}-bls-raw",
            f"{prefix}-datausa-raw",
            f"{prefix}-bls-silver",
            f"{prefix}-datausa-silver",
        ]

        self.buckets = {}
        for name in bucket_names:
            bucket = s3.Bucket(
                self,
                name,
                bucket_name=name,
                removal_policy=removal,
                auto_delete_objects=removal == RemovalPolicy.DESTROY,
            )
            self.buckets[name] = bucket

        self.bls_raw_bucket = self.buckets[f"{prefix}-bls-raw"]
        self.datausa_raw_bucket = self.buckets[f"{prefix}-datausa-raw"]
        self.bls_silver_bucket = self.buckets[f"{prefix}-bls-silver"]
        self.datausa_silver_bucket = self.buckets[f"{prefix}-datausa-silver"]
