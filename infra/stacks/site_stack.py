"""Static site stack for publishing the dashboard via CloudFront + S3."""

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_certificatemanager as acm
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_cloudfront_origins as origins
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

        site_aliases: list[str] = config["site_aliases"]
        site_cert_arn: str = config["site_cert_arn"]
        if site_aliases and not site_cert_arn:
            raise ValueError(
                "FOMC_SITE_CERT_ARN must be set when using "
                "FOMC_SITE_DOMAIN/FOMC_SITE_ALIASES."
            )

        bucket = s3.Bucket(
            self,
            "SiteBucket",
            bucket_name=f"{prefix}-site",
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=removal,
            auto_delete_objects=removal == RemovalPolicy.DESTROY,
            enforce_ssl=True,
        )

        distribution_args: dict = {
            "default_root_object": "index.html",
            "default_behavior": cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin(bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                compress=True,
            ),
            "error_responses": [
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(5),
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(5),
                ),
            ],
        }

        if site_aliases:
            certificate = acm.Certificate.from_certificate_arn(
                self,
                "SiteCertificate",
                site_cert_arn,
            )
            distribution_args["domain_names"] = site_aliases
            distribution_args["certificate"] = certificate

        distribution = cloudfront.Distribution(
            self,
            "SiteDistribution",
            **distribution_args,
        )

        s3deploy.BucketDeployment(
            self,
            "DeploySite",
            sources=[s3deploy.Source.asset("site")],
            destination_bucket=bucket,
            distribution=distribution,
            distribution_paths=["/*"],
            prune=True,
        )

        CfnOutput(self, "SiteBucketName", value=bucket.bucket_name)
        CfnOutput(self, "SiteCloudFrontDomain", value=distribution.distribution_domain_name)
        CfnOutput(self, "SiteUrl", value=f"https://{distribution.distribution_domain_name}")

        if site_aliases:
            CfnOutput(self, "CustomSiteDomains", value=",".join(site_aliases))
            CfnOutput(self, "CustomSiteUrl", value=f"https://{site_aliases[0]}")
