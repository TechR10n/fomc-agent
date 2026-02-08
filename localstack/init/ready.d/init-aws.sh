#!/bin/bash
# Pre-create S3 buckets and SQS queues for the FOMC pipeline.
# This runs automatically when LocalStack reaches the READY state.

set -euo pipefail

REGION="${DEFAULT_REGION:-us-east-1}"
BUCKET_PREFIX="${FOMC_BUCKET_PREFIX:-fomc}"
BLS_BUCKET="${BLS_BUCKET:-${BUCKET_PREFIX}-bls-raw}"
DATAUSA_BUCKET="${DATAUSA_BUCKET:-${BUCKET_PREFIX}-datausa-raw}"
BLS_PROCESSED_BUCKET="${BLS_PROCESSED_BUCKET:-${BUCKET_PREFIX}-bls-processed}"
DATAUSA_PROCESSED_BUCKET="${DATAUSA_PROCESSED_BUCKET:-${BUCKET_PREFIX}-datausa-processed}"
ANALYTICS_QUEUE_NAME="${FOMC_ANALYTICS_QUEUE_NAME:-fomc-analytics-queue}"
ANALYTICS_DLQ_NAME="${FOMC_ANALYTICS_DLQ_NAME:-fomc-analytics-dlq}"

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
    echo "==> Creating SQS queue: $name" >&2
    awslocal sqs create-queue --queue-name "$name" --attributes "$attributes" >/dev/null
  else
    echo "==> SQS queue exists: $name" >&2
  fi
  wait_for_queue_url "$name"
}

wait_for_queue_url() {
  local name="$1"
  local attempts="${2:-20}"
  local delay_seconds="${3:-1}"
  local url=""

  for ((i = 1; i <= attempts; i++)); do
    url="$(awslocal sqs get-queue-url --queue-name "$name" --query QueueUrl --output text 2>/dev/null || true)"
    if [[ -n "$url" && "$url" != "None" ]]; then
      echo "$url"
      return 0
    fi

    echo "==> Waiting for SQS queue URL: $name (attempt $i/$attempts)" >&2
    sleep "$delay_seconds"
  done

  echo "ERROR: Queue URL for '$name' did not become available in time." >&2
  return 1
}

get_queue_arn() {
  local name="$1"
  local attempts="${2:-20}"
  local delay_seconds="${3:-1}"
  local url=""
  local arn=""

  for ((i = 1; i <= attempts; i++)); do
    url="$(wait_for_queue_url "$name" 1 0 2>/dev/null || true)"
    if [[ -n "$url" && "$url" != "None" ]]; then
      arn="$(awslocal sqs get-queue-attributes \
        --queue-url "$url" \
        --attribute-names QueueArn \
        --query Attributes.QueueArn \
        --output text \
        2>/dev/null || true)"
      if [[ -n "$arn" && "$arn" != "None" && "$arn" != "null" ]]; then
        echo "$arn"
        return 0
      fi
    fi

    echo "==> Waiting for SQS queue attributes: $name (attempt $i/$attempts)" >&2
    sleep "$delay_seconds"
  done

  echo "ERROR: Queue ARN for '$name' did not become available in time." >&2
  return 1
}

echo "==> Ensuring S3 buckets..."
ensure_bucket "$BLS_BUCKET"
ensure_bucket "$DATAUSA_BUCKET"
ensure_bucket "$BLS_PROCESSED_BUCKET"
ensure_bucket "$DATAUSA_PROCESSED_BUCKET"

echo "==> Ensuring SQS queues..."
DLQ_URL="$(ensure_queue "$ANALYTICS_DLQ_NAME" '{"MessageRetentionPeriod":"1209600"}')"
DLQ_ARN="$(get_queue_arn "$ANALYTICS_DLQ_NAME")"

QUEUE_URL="$(ensure_queue "$ANALYTICS_QUEUE_NAME" '{"VisibilityTimeout":"360"}')"
QUEUE_ARN="$(get_queue_arn "$ANALYTICS_QUEUE_NAME")"

echo "==> Configuring SQS redrive policy..."
REDRIVE_ATTRIBUTES_JSON="$(cat <<EOF | tr -d '\n'
{
  "VisibilityTimeout": "360",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}"
}
EOF
)"
awslocal sqs set-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attributes "$REDRIVE_ATTRIBUTES_JSON" \
  >/dev/null

echo "==> Allowing S3 bucket notifications to publish to SQS..."
SOURCE_BUCKET="$DATAUSA_BUCKET"
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

ESCAPED_QUEUE_POLICY="${QUEUE_POLICY//\"/\\\"}"
POLICY_ATTRIBUTES_JSON="{\"Policy\":\"${ESCAPED_QUEUE_POLICY}\"}"
awslocal sqs set-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attributes "$POLICY_ATTRIBUTES_JSON" \
  >/dev/null

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
echo "    - bucket prefix: ${BUCKET_PREFIX}"
echo "    - bls raw bucket: ${BLS_BUCKET}"
echo "    - datausa raw bucket: ${DATAUSA_BUCKET}"
echo "    - analytics queue: ${ANALYTICS_QUEUE_NAME}"
echo "    - analytics dlq: ${ANALYTICS_DLQ_NAME}"
awslocal s3 ls
awslocal sqs list-queues
