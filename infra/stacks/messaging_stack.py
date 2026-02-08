"""Analytics Lambda stack, consuming from the SQS queue created in the storage stack."""

import os
from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_events
from aws_cdk import triggers
from constructs import Construct

from infra.config import get_env_config
from infra.stacks.storage_stack import FomcStorageStack


class FomcMessagingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        storage: FomcStorageStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_env_config()
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        deployment_id = os.environ.get("FOMC_DEPLOYMENT_ID", "")

        # Analytics processor Lambda
        self.analytics_processor = _lambda.Function(
            self,
            "AnalyticsProcessorFunction",
            function_name="fomc-analytics-processor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.lambdas.analytics_processor.handler.handler",
            code=_lambda.Code.from_asset(
                project_root,
                exclude=[
                    ".venv/*",
                    ".git/*",
                    ".idea/*",
                    ".run/*",
                    "cdk.out/*",
                    "notebooks/*",
                    "tests/*",
                    "docs/*",
                    "site/*",
                    "localstack/*",
                    "tools/*",
                    "volume/*",
                    "__pycache__/*",
                    "*.pyc",
                ],
            ),
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "BLS_BUCKET": storage.bls_raw_bucket.bucket_name,
                "DATAUSA_BUCKET": storage.datausa_raw_bucket.bucket_name,
                "FOMC_DEPLOYMENT_ID": deployment_id,
            },
        )

        # SQS triggers Lambda
        self.analytics_processor.add_event_source(
            lambda_events.SqsEventSource(storage.analytics_queue, batch_size=1)
        )

        # Also run analytics on the configured interval.
        analytics_rule = events.Rule(
            self,
            "AnalyticsScheduleRule",
            schedule=events.Schedule.rate(Duration.hours(config["fetch_interval_hours"])),
        )
        analytics_rule.add_target(targets.LambdaFunction(self.analytics_processor))

        # Grant S3 read permissions
        storage.bls_raw_bucket.grant_read(self.analytics_processor)
        storage.datausa_raw_bucket.grant_read(self.analytics_processor)

        # Trigger once on each deployment (fire-and-forget).
        triggers.Trigger(
            self,
            "AnalyticsDeployTrigger",
            handler=self.analytics_processor,
            invocation_type=triggers.InvocationType.EVENT,
        )
