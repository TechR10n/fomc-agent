"""S3 storage stack for FOMC data pipeline.

Also creates the SQS queue and S3→SQS notification, since the bucket
and its notification must live in the same stack to avoid cyclic
cross-stack references.
"""

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sqs as sqs
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

        # SQS queues — co-located with buckets to avoid cross-stack cycles
        dlq = sqs.Queue(
            self,
            "AnalyticsDLQ",
            queue_name="fomc-analytics-dlq",
            retention_period=Duration.days(14),
        )

        self.analytics_queue = sqs.Queue(
            self,
            "AnalyticsQueue",
            queue_name="fomc-analytics-queue",
            visibility_timeout=Duration.minutes(6),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
        )

        # S3 event notification: JSON uploads to datausa bucket → SQS
        self.datausa_raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(self.analytics_queue),
            s3.NotificationKeyFilter(suffix=".json"),
        )
