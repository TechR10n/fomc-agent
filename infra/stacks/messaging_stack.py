"""Analytics Lambda stack, consuming from the SQS queue created in the storage stack."""

from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_events
from constructs import Construct

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

        project_root = str(Path(__file__).resolve().parent.parent.parent)

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
                    "cdk.out/*",
                    "notebooks/*",
                    "tests/*",
                    "docs/*",
                    "site/*",
                    "__pycache__/*",
                    "*.pyc",
                ],
            ),
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "BLS_BUCKET": storage.bls_raw_bucket.bucket_name,
                "DATAUSA_BUCKET": storage.datausa_raw_bucket.bucket_name,
            },
        )

        # SQS triggers Lambda
        self.analytics_processor.add_event_source(
            lambda_events.SqsEventSource(storage.analytics_queue, batch_size=1)
        )

        # Grant S3 read permissions
        storage.bls_raw_bucket.grant_read(self.analytics_processor)
        storage.datausa_raw_bucket.grant_read(self.analytics_processor)
