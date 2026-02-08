"""Lambda compute stack for FOMC data pipeline."""

import os
from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from aws_cdk import triggers
from constructs import Construct

from infra.config import get_env_config
from infra.stacks.storage_stack import FomcStorageStack


class FomcComputeStack(Stack):
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

        self.data_fetcher = _lambda.Function(
            self,
            "DataFetcherFunction",
            function_name="fomc-data-fetcher",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.lambdas.data_fetcher.handler.handler",
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
                "BLS_SERIES": config["bls_series"],
                "DATAUSA_DATASETS": config["datausa_datasets"],
                "DATAUSA_BASE_URL": config["datausa_base_url"],
                "FOMC_DEPLOYMENT_ID": deployment_id,
            },
        )

        # Grant S3 read/write permissions
        storage.bls_raw_bucket.grant_read_write(self.data_fetcher)
        storage.datausa_raw_bucket.grant_read_write(self.data_fetcher)

        # Schedule: run the fetcher on the configured interval.
        rule = events.Rule(
            self,
            "FetchScheduleRule",
            schedule=events.Schedule.rate(Duration.hours(config["fetch_interval_hours"])),
        )
        rule.add_target(targets.LambdaFunction(self.data_fetcher))

        # Trigger once on each deployment (fire-and-forget).
        triggers.Trigger(
            self,
            "DataFetcherDeployTrigger",
            handler=self.data_fetcher,
            invocation_type=triggers.InvocationType.EVENT,
        )
