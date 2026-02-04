#!/bin/bash
# Pre-create S3 buckets and SQS queues for the FOMC pipeline.
# This runs automatically when LocalStack reaches the READY state.

set -euo pipefail

echo "==> Creating S3 buckets..."
awslocal s3 mb s3://fomc-bls-raw
awslocal s3 mb s3://fomc-datausa-raw
awslocal s3 mb s3://fomc-bls-silver
awslocal s3 mb s3://fomc-datausa-silver

echo "==> Creating SQS queues..."
awslocal sqs create-queue --queue-name fomc-analytics-dlq \
  --attributes '{"MessageRetentionPeriod":"1209600"}'

awslocal sqs create-queue --queue-name fomc-analytics-queue \
  --attributes '{
    "VisibilityTimeout":"360",
    "RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:fomc-analytics-dlq\",\"maxReceiveCount\":\"3\"}"
  }'

echo "==> LocalStack init complete. Resources:"
awslocal s3 ls
awslocal sqs list-queues
