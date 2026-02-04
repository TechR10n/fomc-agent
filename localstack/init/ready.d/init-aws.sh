#!/bin/bash
# Pre-create S3 buckets and SQS queues for the FOMC pipeline.
# This runs automatically when LocalStack reaches the READY state.

set -euo pipefail

REGION="${DEFAULT_REGION:-us-east-1}"

ensure_bucket() {
  local bucket="$1"
  if awslocal s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    echo "==> S3 bucket exists: $bucket"
    return 0
  fi
  echo "==> Creating S3 bucket: $bucket"
  awslocal s3 mb "s3://$bucket" >/dev/null
}

ensure_queue() {
  local name="$1"
  local attributes="$2"

  local url=""
  url="$(awslocal sqs get-queue-url --queue-name "$name" --query QueueUrl --output text 2>/dev/null || true)"
  if [[ -z "$url" || "$url" == "None" ]]; then
    echo "==> Creating SQS queue: $name"
    url="$(awslocal sqs create-queue --queue-name "$name" --attributes "$attributes" --query QueueUrl --output text)"
  else
    echo "==> SQS queue exists: $name"
  fi
  echo "$url"
}

echo "==> Ensuring S3 buckets..."
ensure_bucket "fomc-bls-raw"
ensure_bucket "fomc-datausa-raw"
ensure_bucket "fomc-bls-silver"
ensure_bucket "fomc-datausa-silver"

echo "==> Ensuring SQS queues..."
DLQ_URL="$(ensure_queue "fomc-analytics-dlq" '{"MessageRetentionPeriod":"1209600"}')"
DLQ_ARN="$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)"

QUEUE_URL="$(ensure_queue "fomc-analytics-queue" '{"VisibilityTimeout":"360"}')"
QUEUE_ARN="$(awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)"

echo "==> Configuring SQS redrive policy..."
awslocal sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes \
  VisibilityTimeout="360" \
  RedrivePolicy="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}" \
  >/dev/null

echo "==> Allowing S3 bucket notifications to publish to SQS..."
SOURCE_BUCKET="fomc-datausa-raw"
SOURCE_BUCKET_ARN="arn:aws:s3:::${SOURCE_BUCKET}"

QUEUE_POLICY="$(cat <<EOF | tr -d '\n'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3SendMessage",
      "Effect": "Allow",
      "Principal": { "Service": "s3.amazonaws.com" },
      "Action": "sqs:SendMessage",
      "Resource": "${QUEUE_ARN}",
      "Condition": { "ArnLike": { "aws:SourceArn": "${SOURCE_BUCKET_ARN}" } }
    }
  ]
}
EOF
)"

awslocal sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes Policy="$QUEUE_POLICY" >/dev/null

echo "==> Configuring S3 → SQS notification (suffix: .json)..."
NOTIF_CONFIG="$(cat <<EOF | tr -d '\n'
{
  "QueueConfigurations": [
    {
      "QueueArn": "${QUEUE_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": { "Key": { "FilterRules": [ { "Name": "suffix", "Value": ".json" } ] } }
    }
  ]
}
EOF
)"
awslocal s3api put-bucket-notification-configuration \
  --bucket "$SOURCE_BUCKET" \
  --notification-configuration "$NOTIF_CONFIG" \
  >/dev/null

echo "==> LocalStack init complete. Resources:"
awslocal s3 ls
awslocal sqs list-queues
