"""Static site stack for publishing a simple demo website to S3."""

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3deploy
from constructs import Construct

from infra.config import get_env_config


class FomcSiteStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_env_config()
        prefix = config["bucket_prefix"]
        removal = (
            RemovalPolicy.DESTROY
            if config["removal_policy"] == "destroy"
            else RemovalPolicy.RETAIN
        )

        bucket = s3.Bucket(
            self,
            "SiteBucket",
            bucket_name=f"{prefix}-site",
            website_index_document="index.html",
            website_error_document="index.html",
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=False,
                ignore_public_acls=False,
                block_public_policy=False,
                restrict_public_buckets=False,
            ),
            removal_policy=removal,
            auto_delete_objects=removal == RemovalPolicy.DESTROY,
        )

        s3deploy.BucketDeployment(
            self,
            "DeploySite",
            sources=[s3deploy.Source.asset("site")],
            destination_bucket=bucket,
            prune=True,
        )

        CfnOutput(self, "SiteBucketName", value=bucket.bucket_name)
        CfnOutput(self, "SiteUrl", value=bucket.bucket_website_url)

