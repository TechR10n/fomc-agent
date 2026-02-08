# AWS Service Tutorials for `fomc-agent`

This guide walks through each AWS service used by this project, what role it plays, and a short hands-on tutorial to verify it end-to-end.

## Before You Start

Assumptions:

- You are in the repo root.
- Dependencies are installed (`uv sync --all-extras`).
- `.env.shared` exists (see `aws_setup.md`).
- `.env.local` exists if you use `AWS_PROFILE` locally.
- You have deployed the stacks at least once.

Load environment variables:

```bash
source .env.shared
source .env.local
```

Useful helper variables:

```bash
export BLS_BUCKET="${FOMC_BUCKET_PREFIX}-bls-raw"
export DATAUSA_BUCKET="${FOMC_BUCKET_PREFIX}-datausa-raw"
export BLS_PROCESSED_BUCKET="${FOMC_BUCKET_PREFIX}-bls-processed"
export DATAUSA_PROCESSED_BUCKET="${FOMC_BUCKET_PREFIX}-datausa-processed"
export SITE_BUCKET="${FOMC_BUCKET_PREFIX}-site"
```

---

## 1) Amazon S3

### How this project uses S3

- Raw data landing zone:
  - `${FOMC_BUCKET_PREFIX}-bls-raw`
  - `${FOMC_BUCKET_PREFIX}-datausa-raw`
- Processed data zone:
  - `${FOMC_BUCKET_PREFIX}-bls-processed`
  - `${FOMC_BUCKET_PREFIX}-datausa-processed`
- Static site bucket:
  - `${FOMC_BUCKET_PREFIX}-site`

Implementation locations:

- `infra/stacks/storage_stack.py`
- `infra/stacks/site_stack.py`
- `src/lambdas/data_fetcher/handler.py`
- `src/transforms/to_processed.py`

### Tutorial

1. Deploy storage and compute stacks:

```bash
python tools/cdk.py deploy FomcStorageStack FomcComputeStack FomcMessagingStack --require-approval never
```

2. Run ingestion once:

```bash
aws lambda invoke --function-name fomc-data-fetcher /tmp/fetcher.json
cat /tmp/fetcher.json
```

3. Verify raw objects were written:

```bash
aws s3 ls "s3://$BLS_BUCKET/pr/" | head
aws s3 ls "s3://$DATAUSA_BUCKET/"
```

4. Run parse-to-processed job and verify output:

```bash
python -m src.transforms.to_processed
aws s3 ls "s3://$BLS_PROCESSED_BUCKET/" | head
aws s3 ls "s3://$DATAUSA_PROCESSED_BUCKET/" | head
```

---

## 2) Amazon SQS

### How this project uses SQS

- Queue: `${FOMC_ANALYTICS_QUEUE_NAME}`
- DLQ: `${FOMC_ANALYTICS_DLQ_NAME}`
- Trigger path: DataUSA raw bucket `.json` uploads -> S3 notification -> SQS message.
- Consumer: analytics Lambda reads SQS messages and runs report generation.

Implementation locations:

- `infra/stacks/storage_stack.py` (queue + DLQ + S3 notification)
- `infra/stacks/messaging_stack.py` (SQS event source for Lambda)

### Tutorial

1. Confirm queues exist:

```bash
aws sqs list-queues --queue-name-prefix "${FOMC_ANALYTICS_QUEUE_NAME%%-*}"
```

2. Upload a JSON file to trigger S3 -> SQS:

```bash
aws s3 cp "s3://$DATAUSA_BUCKET/population.json" /tmp/population.json
aws s3 cp /tmp/population.json "s3://$DATAUSA_BUCKET/population.json"
```

3. Read approximate queue metrics:

```bash
QUEUE_URL="$(aws sqs get-queue-url --queue-name "$FOMC_ANALYTICS_QUEUE_NAME" --query QueueUrl --output text)"
aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

---

## 3) AWS Lambda

### How this project uses Lambda

- `fomc-data-fetcher`: fetches BLS + DataUSA data to raw S3 buckets.
- `fomc-analytics-processor`: triggered by SQS messages from S3 notifications; computes and logs analytics reports.

Implementation locations:

- `infra/stacks/compute_stack.py`
- `infra/stacks/messaging_stack.py`
- `src/lambdas/data_fetcher/handler.py`
- `src/lambdas/analytics_processor/handler.py`

### Tutorial

1. Invoke fetcher Lambda:

```bash
aws lambda invoke --function-name fomc-data-fetcher /tmp/fetcher.json
cat /tmp/fetcher.json
```

2. Tail fetcher logs:

```bash
aws logs tail "/aws/lambda/fomc-data-fetcher" --since 15m
```

3. Trigger analytics Lambda through the normal path (upload JSON):

```bash
aws s3 cp "s3://$DATAUSA_BUCKET/population.json" /tmp/population.json
aws s3 cp /tmp/population.json "s3://$DATAUSA_BUCKET/population.json"
aws logs tail "/aws/lambda/fomc-analytics-processor" --since 15m
```

---

## 4) Amazon EventBridge

### How this project uses EventBridge

- A scheduled rule triggers `fomc-data-fetcher` every `FOMC_FETCH_INTERVAL_HOURS`.

Implementation location:

- `infra/stacks/compute_stack.py`

### Tutorial

1. Find the schedule rule created by the compute stack:

```bash
RULE_NAME="$(aws events list-rules --name-prefix FomcComputeStack-FetchScheduleRule --query 'Rules[0].Name' --output text)"
aws events describe-rule --name "$RULE_NAME" --query '{Name:Name,ScheduleExpression:ScheduleExpression,State:State}'
```

2. Verify Lambda is a target:

```bash
aws events list-targets-by-rule --rule "$RULE_NAME"
```

3. Change interval (example: every 2 hours) in `.env.shared`, then redeploy:

```bash
# Edit .env.shared and set:
# FOMC_FETCH_INTERVAL_HOURS=2
python tools/cdk.py deploy FomcComputeStack --require-approval never
```

---

## 5) AWS Identity and Access Management (IAM)

### How this project uses IAM

- CDK creates execution roles for each Lambda.
- Bucket permissions are granted via `grant_read_write` and `grant_read`.
- GitHub Actions deploys with an assumed IAM role (`AWS_DEPLOY_ROLE_ARN`) via OIDC.

Implementation locations:

- `infra/stacks/compute_stack.py`
- `infra/stacks/messaging_stack.py`
- `.github/workflows/ci-deploy.yml`

### Tutorial

1. Identify Lambda role names:

```bash
aws lambda get-function-configuration --function-name fomc-data-fetcher --query '{FunctionName:FunctionName,Role:Role}'
aws lambda get-function-configuration --function-name fomc-analytics-processor --query '{FunctionName:FunctionName,Role:Role}'
```

2. Inspect inline policies attached to one role:

```bash
ROLE_NAME="$(aws lambda get-function-configuration --function-name fomc-data-fetcher --query 'Role' --output text | awk -F/ '{print $NF}')"
aws iam list-role-policies --role-name "$ROLE_NAME"
```

3. Pull and review one policy document:

```bash
POLICY_NAME="$(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames[0]' --output text)"
aws iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME"
```

---

## 6) Amazon CloudWatch (Logs + Metrics)

### How this project uses CloudWatch

- Lambda logs are written to CloudWatch Logs.
- CloudWatch Metrics are queried by `tools/build_aws_observability.py` to generate dashboard data for Lambda, SQS, and S3 metrics.

Implementation locations:

- `src/analytics/aws_observability.py`
- `tools/build_aws_observability.py`

### Tutorial

1. Tail logs from both Lambdas:

```bash
aws logs tail "/aws/lambda/fomc-data-fetcher" --since 15m
aws logs tail "/aws/lambda/fomc-analytics-processor" --since 15m
```

2. Build observability payload for the site:

```bash
python tools/build_aws_observability.py --days 30 --forecast-days 30 --out site/data/aws_observability.json
```

3. Confirm output includes metric series:

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path("site/data/aws_observability.json")
d = json.loads(p.read_text())
print("metric_series", len(d.get("metrics", {}).get("series", [])))
print("errors", len(d.get("errors", [])))
PY
```

---

## 7) AWS Cost Explorer

### How this project uses Cost Explorer

- The observability export queries:
  - `GetCostAndUsage` (actual cost)
  - `GetCostForecast` (predicted cost)
- Default filter scope is Lambda, SQS, and S3 service charges.

Implementation location:

- `src/analytics/aws_observability.py`

### Tutorial

1. (One-time account setup) Ensure Cost Explorer is enabled in AWS Billing.

2. Optionally scope by tag and service list:

```bash
export FOMC_COST_TAG_KEY=Project
export FOMC_COST_TAG_VALUES=fomc-agent
export FOMC_COST_SERVICES="AWS Lambda,Amazon Simple Queue Service,Amazon Simple Storage Service"
```

3. Generate observability payload and inspect cost section:

```bash
python tools/build_aws_observability.py --days 30 --forecast-days 14 --out site/data/aws_observability.json
python - <<'PY'
import json
from pathlib import Path
d = json.loads(Path("site/data/aws_observability.json").read_text())
print("currency", d.get("cost", {}).get("currency"))
print("cost_dates", len(d.get("cost", {}).get("dates", [])))
print("actual_points", len([v for v in d.get("cost", {}).get("actual", []) if v is not None]))
PY
```

Note: brand-new accounts may show limited or delayed cost data.

---

## 8) Amazon CloudFront

### How this project uses CloudFront

- CloudFront serves the static dashboard from the private site S3 bucket.
- CDK deployment invalidates paths after publishing updated site assets.

Implementation location:

- `infra/stacks/site_stack.py`

### Tutorial

1. Deploy the site stack:

```bash
python tools/cdk.py deploy FomcSiteStack --require-approval never
```

2. Read CloudFront URL from stack outputs:

```bash
aws cloudformation describe-stacks \
  --stack-name FomcSiteStack \
  --query 'Stacks[0].Outputs[?OutputKey==`SiteUrl`].OutputValue' \
  --output text
```

3. Rebuild data + redeploy to see an update roll out:

```bash
python -m src.analytics.reports
python tools/build_bls_timeline.py --days 60 --lookahead-days 14 --out site/data/bls_timeline.json
python tools/build_aws_observability.py --days 30 --forecast-days 30 --out site/data/aws_observability.json
python tools/cdk.py deploy FomcSiteStack --require-approval never
```

---

## 9) AWS Certificate Manager (ACM, optional custom domain)

### How this project uses ACM

- If you set `FOMC_SITE_DOMAIN` or `FOMC_SITE_ALIASES`, the site stack requires `FOMC_SITE_CERT_ARN`.
- The ACM certificate is attached to the CloudFront distribution for HTTPS on custom domains.

Implementation location:

- `infra/stacks/site_stack.py`

### Tutorial

1. Request or import an ACM certificate in `us-east-1` for your domain.
2. Set environment variables:

```bash
export FOMC_SITE_DOMAIN="dashboard.example.com"
export FOMC_SITE_ALIASES="dashboard.example.com"
export FOMC_SITE_CERT_ARN="arn:aws:acm:us-east-1:123456789012:certificate/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

3. Redeploy site stack:

```bash
python tools/cdk.py deploy FomcSiteStack --require-approval never
```

4. Point DNS (for example, Route 53 alias) at the CloudFront distribution domain output by the stack.

---

## 10) AWS CloudFormation (via AWS CDK)

### How this project uses CloudFormation

- CDK (`app.py`) synthesizes and deploys four stacks:
  - `FomcStorageStack`
  - `FomcComputeStack`
  - `FomcMessagingStack`
  - `FomcSiteStack`
- CloudFormation is the deployment engine behind `cdk deploy`.

Implementation locations:

- `app.py`
- `infra/stacks/*.py`
- `tools/cdk.py`

### Tutorial

1. Synthesize templates:

```bash
python tools/cdk.py synth --all
```

2. Review planned changes:

```bash
python tools/cdk.py diff --all
```

3. Deploy all stacks:

```bash
python tools/cdk.py deploy --all --require-approval never
```

4. List stack statuses:

```bash
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE UPDATE_ROLLBACK_COMPLETE \
  --query 'StackSummaries[?starts_with(StackName, `Fomc`)].{Name:StackName,Status:StackStatus}'
```

5. Destroy when finished (optional):

```bash
python tools/cdk.py destroy --all --force
```

---

## Service-to-Project Map (Quick Reference)

| AWS Service | Project usage | Main files |
|---|---|---|
| Amazon S3 | Raw/processed data lake + static site bucket | `infra/stacks/storage_stack.py`, `infra/stacks/site_stack.py` |
| Amazon SQS | Event queue + DLQ for analytics trigger path | `infra/stacks/storage_stack.py`, `infra/stacks/messaging_stack.py` |
| AWS Lambda | Fetcher + analytics compute | `infra/stacks/compute_stack.py`, `infra/stacks/messaging_stack.py` |
| Amazon EventBridge | Scheduled ingestion trigger | `infra/stacks/compute_stack.py` |
| AWS IAM | Lambda execution roles and permissions grants | `infra/stacks/compute_stack.py`, `infra/stacks/messaging_stack.py` |
| Amazon CloudWatch | Logs/metrics powering observability export | `src/analytics/aws_observability.py` |
| AWS Cost Explorer | Actual + forecast cost in dashboard payload | `src/analytics/aws_observability.py` |
| Amazon CloudFront | Global static site delivery | `infra/stacks/site_stack.py` |
| AWS ACM | TLS cert attachment for custom domain | `infra/stacks/site_stack.py` |
| AWS CloudFormation | Stack lifecycle engine behind CDK | `app.py`, `infra/stacks/*.py` |
