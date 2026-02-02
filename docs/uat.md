# FOMC Agent — User Acceptance Testing

This document defines every manual verification step needed to confirm the FOMC data pipeline works correctly in both LocalStack (local) and personal AWS environments.

If you want a guided, lab-by-lab curriculum (recommended order), start with `docs/lab-plan/README.md` and use this document for the deeper verification steps.

## Conventions

- Commands assume you are running from the repo root.
- Commands assume your venv is active (`source .venv/bin/activate`). If it isn’t, replace `python` with `.venv/bin/python`.
- LocalStack commands assume `AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566`.
- If you deploy to personal AWS and change the bucket name prefix, replace `fomc` in bucket names/commands accordingly.

---

## Prerequisites

### Software Requirements

| Tool | Install Command | Verify |
|------|----------------|--------|
| Docker | `brew install --cask docker` | `docker --version` |
| LocalStack | `docker pull localstack/localstack` | `docker run -d --name localstack -p 4566:4566 localstack/localstack` |
| AWS CLI v2 | `brew install awscli` | `aws --version` |
| awslocal wrapper | `pip install awscli-local` | `awslocal --version` |
| Node.js (for CDK) | `brew install node` | `node --version` |
| AWS CDK + cdklocal | `npm install -g aws-cdk-local aws-cdk` | `cdk --version && cdklocal --version` |
| Java 21 (for PySpark) | `brew install openjdk@21` | `java -version` |
| Python 3.12+ | Already installed | `python3 --version` |
| uv | `brew install uv` | `uv --version` |

### Environment Setup

```bash
# 1. Install Python dependencies (includes CDK + dev tooling)
uv sync --all-extras

# 2. Activate the virtualenv (so `python` exists and points at the project interpreter)
source .venv/bin/activate

# 3. Set JAVA_HOME (add to ~/.zshrc for persistence)
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home

# 4. Start LocalStack
docker run -d --name localstack -p 4566:4566 localstack/localstack

# 5. Verify LocalStack is running
curl -s http://localhost.localstack.cloud:4566/_localstack/health | python -m json.tool
# Expected: JSON with "services" showing "s3": "available", "sqs": "available", etc.

# 6. Verify AWS CLI is configured for personal AWS
aws sts get-caller-identity
# Expected: JSON with your Account, UserId, and Arn
```

---

## UAT-1: Unit Test Suite

**Environment**: Local (no AWS or LocalStack needed)

### UAT-1.1: Run full test suite

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
  python -m pytest tests/unit/ -v --cov=src --cov-report=term-missing
```

**Expected result**:
- [ ] All 69 tests pass
- [ ] Overall coverage ≥ 80%
- [ ] No warnings or deprecation errors

### UAT-1.2: Verify coverage by module

Check the coverage output table matches or exceeds these targets:

| Module | Target Coverage |
|--------|----------------|
| `src/helpers/aws_client.py` | 100% |
| `src/helpers/aws_status.py` | ≥ 80% |
| `src/data_fetchers/bls_getter.py` | ≥ 90% |
| `src/data_fetchers/datausa_getter.py` | ≥ 90% |
| `src/analytics/reports.py` | ≥ 50% |
| `src/lambdas/data_fetcher/handler.py` | ≥ 90% |
| `src/lambdas/analytics_processor/handler.py` | ≥ 90% |

- [ ] All modules meet coverage targets

---

## UAT-2: Helpers & Connectivity

### UAT-2.1: LocalStack connectivity

```bash
AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566 \
  python src/helpers/aws_status.py
```

**Expected result**:
```json
{
  "s3": {},
  "sqs": {},
  "lambda": {}
}
```
- [ ] Script runs without errors
- [ ] Output is valid JSON
- [ ] All three service keys present (`s3`, `sqs`, `lambda`)
- [ ] Values are empty dicts (no resources created yet)

### UAT-2.2: AWS connectivity

```bash
unset AWS_ENDPOINT_URL
python src/helpers/aws_status.py
```

**Expected result**:
- [ ] Script runs without errors
- [ ] Output is valid JSON
- [ ] Shows any existing AWS resources (or empty dicts if none)
- [ ] No authentication or region errors

### UAT-2.3: Environment switching

```bash
# With LocalStack
AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566 \
  python -c "from src.helpers.aws_client import is_localstack; print(f'LocalStack: {is_localstack()}')"
# Expected: LocalStack: True

# Without LocalStack
unset AWS_ENDPOINT_URL
python -c "from src.helpers.aws_client import is_localstack; print(f'LocalStack: {is_localstack()}')"
# Expected: LocalStack: False
```

- [ ] `is_localstack()` returns `True` when `AWS_ENDPOINT_URL` is set
- [ ] `is_localstack()` returns `False` when `AWS_ENDPOINT_URL` is unset

---

## UAT-3: CDK Storage Stack

### UAT-3.1: CDK synth validation

```bash
# Synthesize for LocalStack
CDK_LOCAL=true cdk synth FomcStorageStack --quiet 2>&1
echo "Exit code: $?"

# Synthesize for AWS
cdk synth FomcStorageStack --quiet 2>&1
echo "Exit code: $?"
```

- [ ] LocalStack synth exits with code 0
- [ ] AWS synth exits with code 0
- [ ] No errors in output

### UAT-3.2: Deploy to LocalStack

```bash
# Bootstrap LocalStack (first time only)
CDK_LOCAL=true cdklocal bootstrap aws://000000000000/us-east-1

# Deploy storage stack
CDK_LOCAL=true cdklocal deploy FomcStorageStack --require-approval never
```

- [ ] Bootstrap completes without errors
- [ ] Deploy completes without errors

### UAT-3.3: Verify LocalStack buckets

```bash
awslocal s3 ls
```

**Expected output** (4 buckets):
```
20XX-XX-XX XX:XX:XX fomc-bls-raw
20XX-XX-XX XX:XX:XX fomc-bls-silver
20XX-XX-XX XX:XX:XX fomc-datausa-raw
20XX-XX-XX XX:XX:XX fomc-datausa-silver
```

- [ ] `fomc-bls-raw` bucket exists
- [ ] `fomc-bls-silver` bucket exists
- [ ] `fomc-datausa-raw` bucket exists
- [ ] `fomc-datausa-silver` bucket exists

### UAT-3.4: Deploy to personal AWS

> Note: S3 bucket names are globally unique. If `cdk deploy` fails with a bucket-name collision, choose a unique prefix by updating `bucket_prefix` in `infra/config.py` before deploying to AWS.

```bash
# Bootstrap AWS (first time only)
cdk bootstrap

# Deploy
cdk deploy FomcStorageStack --require-approval never
```

- [ ] Bootstrap completes without errors
- [ ] Deploy completes without errors

### UAT-3.5: Verify AWS buckets

```bash
aws s3 ls | grep fomc  # replace "fomc" if you changed the bucket prefix
```

- [ ] All 4 `fomc-*` buckets exist in AWS
- [ ] Bucket names match LocalStack names

### UAT-3.6: Status check confirms buckets

```bash
# LocalStack
AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566 \
  python src/helpers/aws_status.py

# AWS
unset AWS_ENDPOINT_URL
python src/helpers/aws_status.py
```

- [ ] LocalStack status shows 4 buckets with `object_count: 0`
- [ ] AWS status shows 4 buckets with `object_count: 0`

---

## UAT-4: BLS Data Fetcher

### UAT-4.1: Fetch BLS data to LocalStack

```bash
export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
python src/data_fetchers/bls_getter.py
```

**Expected result**:
- [ ] Script runs without errors
- [ ] Output shows JSON summary with `added` files (first run)
- [ ] No `403 Forbidden` errors

### UAT-4.2: Verify BLS files in LocalStack S3

```bash
# List files under pr/ prefix
awslocal s3 ls s3://fomc-bls-raw/pr/

# Check a specific file has content
awslocal s3 cp s3://fomc-bls-raw/pr/pr.data.0.Current - | head -5
```

- [ ] Multiple files listed under `pr/` prefix
- [ ] `pr.data.0.Current` exists and contains tab-delimited data
- [ ] First line is a header row with `series_id`, `year`, `period`, `value`, `footnote_codes`

### UAT-4.3: Verify BLS sync state in LocalStack

```bash
# Check latest state
awslocal s3 cp s3://fomc-bls-raw/_sync_state/pr/latest_state.json - | python -m json.tool

# Check sync log
awslocal s3 cp s3://fomc-bls-raw/_sync_state/pr/sync_log.jsonl -
```

**Expected latest_state.json structure**:
```json
{
  "series": "pr",
  "last_sync": "2026-01-29T...",
  "files": {
    "pr.data.0.Current": {
      "source_modified": "...",
      "bytes": 12345
    }
  }
}
```

- [ ] `latest_state.json` exists and is valid JSON
- [ ] `series` field is `"pr"`
- [ ] `last_sync` is a recent ISO timestamp
- [ ] `files` dict contains entries for each BLS file
- [ ] Each file entry has `source_modified` and `bytes`
- [ ] `sync_log.jsonl` exists
- [ ] Each line in `sync_log.jsonl` is valid JSON
- [ ] Log entries have `timestamp`, `file`, `action` fields

### UAT-4.4: Verify idempotency (re-run skips unchanged)

```bash
export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
python src/data_fetchers/bls_getter.py
```

**Expected result**:
- [ ] Output shows `unchanged` files (not `added` or `updated`)
- [ ] No files re-uploaded
- [ ] Sync log has new entries with `action: "unchanged"`

### UAT-4.5: Fetch BLS data to personal AWS

```bash
unset AWS_ENDPOINT_URL
python src/data_fetchers/bls_getter.py
```

- [ ] Script runs without errors
- [ ] Files uploaded to AWS S3

### UAT-4.6: Verify BLS files in AWS S3

```bash
aws s3 ls s3://fomc-bls-raw/pr/ --human-readable
aws s3 cp s3://fomc-bls-raw/pr/pr.data.0.Current - | head -5
aws s3 cp s3://fomc-bls-raw/_sync_state/pr/latest_state.json - | python -m json.tool
```

- [ ] Files listed with human-readable sizes
- [ ] `pr.data.0.Current` contains valid tab-delimited data
- [ ] `latest_state.json` matches structure from UAT-4.3

---

## UAT-5: DataUSA API Fetcher

### UAT-5.1: Fetch DataUSA data to LocalStack

```bash
export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
python src/data_fetchers/datausa_getter.py
```

**Expected result**:
```json
{
  "action": "updated",
  "content_hash": "...",
  "record_count": 12
}
```

- [ ] Script runs without errors
- [ ] `action` is `"updated"` on first run
- [ ] `record_count` is a positive integer
- [ ] `content_hash` is a 16-character hex string

### UAT-5.2: Verify DataUSA JSON in LocalStack S3

```bash
# Check the data file
awslocal s3 cp s3://fomc-datausa-raw/population.json - | python -m json.tool | head -20

# Check the sync state
awslocal s3 cp s3://fomc-datausa-raw/_sync_state/latest_state.json - | python -m json.tool
```

**Expected population.json structure**:
```json
{
  "data": [
    {
      "Year": 2013,
      "Nation": "United States",
      "Population": 311536594
    },
    ...
  ]
}
```

- [ ] `population.json` exists and is valid JSON
- [ ] `data` array contains records with `Year`, `Nation`, `Population` fields
- [ ] Years span multiple years (at least 2013-2020)
- [ ] Population values are large integers (300M+ range)
- [ ] `latest_state.json` has `content_hash`, `record_count`, `year_range`, `api_url`

### UAT-5.3: Verify DataUSA sync log

```bash
awslocal s3 cp s3://fomc-datausa-raw/_sync_state/sync_log.jsonl -
```

- [ ] `sync_log.jsonl` exists
- [ ] First entry has `action: "updated"` and `content_hash`

### UAT-5.4: Verify idempotency (re-run skips unchanged)

```bash
export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
python src/data_fetchers/datausa_getter.py
```

**Expected result**:
```json
{
  "action": "unchanged",
  "content_hash": "..."
}
```

- [ ] `action` is `"unchanged"` on second run
- [ ] Content hash matches first run
- [ ] Sync log has new entry with `action: "unchanged"`

### UAT-5.5: Fetch DataUSA data to personal AWS

```bash
unset AWS_ENDPOINT_URL
python src/data_fetchers/datausa_getter.py
```

- [ ] Script runs without errors
- [ ] Output shows `action: "updated"`

### UAT-5.6: Verify DataUSA in AWS S3

```bash
aws s3 cp s3://fomc-datausa-raw/population.json - | python -m json.tool | head -20
aws s3 cp s3://fomc-datausa-raw/_sync_state/latest_state.json - | python -m json.tool
```

- [ ] Data matches LocalStack version
- [ ] Sync state written correctly

---

## UAT-6: PySpark Analytics

### UAT-6.1: Run reports against LocalStack data

```bash
export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
python src/analytics/reports.py
```

**Expected result**: JSON output with three reports.

- [ ] Script runs without Spark errors
- [ ] Output is valid JSON

### UAT-6.2: Validate Report 1 — Population Statistics

Check the `report_1_population_stats` section of the output:

```json
{
  "report": "Population Statistics (2013-2018)",
  "mean": 317437383.0,
  "stddev": 4453128.5
}
```

- [ ] `mean` is present and in the 310M-325M range
- [ ] `stddev` is present and in the 3M-6M range
- [ ] Only years 2013-2018 are included (6 data points)

### UAT-6.3: Validate Report 2 — Best Year by Series

Check the `report_2_best_year_by_series` section:

- [ ] Array of objects with `series_id`, `year`, `value`
- [ ] Each `series_id` appears exactly once
- [ ] `year` values are integers
- [ ] `value` represents the maximum yearly sum of quarterly values for that series

### UAT-6.4: Validate Report 3 — Series + Population Join

Check the `report_3_series_population_join` section:

- [ ] Array of objects with `series_id`, `year`, `period`, `value`, `Population`
- [ ] All entries have `series_id = "PRS30006032"` and `period = "Q01"`
- [ ] `Population` is present for years in the DataUSA dataset (2013+)
- [ ] `Population` is `null` for years not in the DataUSA dataset
- [ ] `value` is a decimal number (e.g., `1.9`)

### UAT-6.5: Run reports against personal AWS data

```bash
unset AWS_ENDPOINT_URL
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
python src/analytics/reports.py
```

- [ ] Output matches LocalStack results (same data, same reports)

### UAT-6.6: Jupyter notebook runs end-to-end

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
jupyter notebook notebooks/productivity-analysis.ipynb
```

Run all cells in order:

- [ ] Spark session initializes without errors
- [ ] Data loads from S3 (set `AWS_ENDPOINT_URL` in first cell if using LocalStack)
- [ ] Report 1 output displays mean and stddev
- [ ] Report 2 output displays table of best years
- [ ] Report 3 output displays joined table
- [ ] Sync history charts render (4 charts in a 2x2 grid)
- [ ] Summary table displays data source status
- [ ] Spark session stops cleanly

---

## UAT-7: CDK Compute Stack — Lambda

> Note: The fetcher Lambda imports third-party Python packages (currently `requests`). Ensure the deployed Lambda bundle includes dependencies, otherwise you may see `Runtime.ImportModuleError` at invoke time.

### UAT-7.1: CDK synth validation

```bash
# LocalStack
CDK_LOCAL=true cdk synth FomcComputeStack --quiet 2>&1
echo "Exit code: $?"

# AWS
cdk synth FomcComputeStack --quiet 2>&1
echo "Exit code: $?"
```

- [ ] Both synth commands exit with code 0

### UAT-7.2: Deploy Lambda to LocalStack

```bash
CDK_LOCAL=true cdklocal deploy FomcComputeStack --require-approval never
```

- [ ] Deploy completes without errors

### UAT-7.3: Verify Lambda exists in LocalStack

```bash
awslocal lambda list-functions | python -m json.tool
```

- [ ] `fomc-data-fetcher` function listed
- [ ] Runtime is `python3.12`
- [ ] Timeout is 300 seconds (5 minutes)
- [ ] Memory is 256 MB
- [ ] Environment variables include `BLS_BUCKET`, `DATAUSA_BUCKET`, `BLS_SERIES`

### UAT-7.4: Invoke Lambda in LocalStack

```bash
awslocal lambda invoke \
  --function-name fomc-data-fetcher \
  /tmp/lambda-out.json

cat /tmp/lambda-out.json | python -m json.tool
```

**Expected /tmp/lambda-out.json structure**:
```json
{
  "statusCode": 200,
  "body": "{\"bls\": {...}, \"datausa\": {...}, \"errors\": []}"
}
```

- [ ] Invocation succeeds (no FunctionError)
- [ ] `statusCode` is `200`
- [ ] `body.bls` contains sync results for `pr` series
- [ ] `body.datausa` contains sync result with `action` and `content_hash`
- [ ] `body.errors` is an empty array

### UAT-7.5: Verify Lambda wrote data to S3

```bash
awslocal s3 ls s3://fomc-bls-raw/pr/
awslocal s3 ls s3://fomc-datausa-raw/
```

- [ ] BLS files present after Lambda invocation
- [ ] `population.json` present after Lambda invocation

### UAT-7.6: Deploy Lambda to personal AWS

```bash
cdk deploy FomcComputeStack --require-approval never
```

- [ ] Deploy completes without errors

### UAT-7.7: Invoke Lambda in personal AWS

```bash
aws lambda invoke \
  --function-name fomc-data-fetcher \
  /tmp/aws-lambda-out.json

cat /tmp/aws-lambda-out.json | python -m json.tool
```

- [ ] Invocation succeeds
- [ ] `statusCode` is `200`
- [ ] Data written to AWS S3 buckets

### UAT-7.8: Verify EventBridge schedule exists

```bash
aws events list-rules | python -m json.tool
```

- [ ] A rule exists targeting `fomc-data-fetcher`
- [ ] Schedule expression is `cron(0 9 * * ? *)` (daily at 9 AM UTC)

---

## UAT-8: CDK Messaging Stack — SQS + Analytics Lambda

### UAT-8.1: CDK synth validation

```bash
CDK_LOCAL=true cdk synth FomcMessagingStack --quiet 2>&1
echo "Exit code: $?"

cdk synth FomcMessagingStack --quiet 2>&1
echo "Exit code: $?"
```

- [ ] Both synth commands exit with code 0

### UAT-8.2: Deploy messaging stack to LocalStack

```bash
CDK_LOCAL=true cdklocal deploy FomcMessagingStack --require-approval never
```

- [ ] Deploy completes without errors

### UAT-8.3: Verify SQS queues in LocalStack

```bash
awslocal sqs list-queues | python -m json.tool
```

- [ ] `fomc-analytics-queue` exists
- [ ] `fomc-analytics-dlq` exists (dead letter queue)

### UAT-8.4: Verify analytics Lambda in LocalStack

```bash
awslocal lambda list-functions | python -m json.tool
```

- [ ] `fomc-analytics-processor` function listed
- [ ] Runtime is `python3.12`
- [ ] Environment variables include `BLS_BUCKET`, `DATAUSA_BUCKET`

### UAT-8.5: Test SQS → Lambda manually in LocalStack

```bash
# Get queue URL
QUEUE_URL=$(awslocal sqs list-queues --queue-name-prefix fomc-analytics-queue \
  --query 'QueueUrls[0]' --output text)

# Send a test message simulating an S3 notification
awslocal sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body '{
    "Records": [{
      "s3": {
        "bucket": {"name": "fomc-datausa-raw"},
        "object": {"key": "population.json"}
      }
    }]
  }'
```

- [ ] Message sent successfully (MessageId returned)
- [ ] Analytics Lambda processes the message (check logs below)

### UAT-8.6: Check analytics Lambda logs in LocalStack

```bash
awslocal logs describe-log-groups
awslocal logs tail /aws/lambda/fomc-analytics-processor
```

- [ ] Log group exists
- [ ] Log contains report output (population stats, best year, series join)
- [ ] No error logs

### UAT-8.7: Deploy messaging stack to personal AWS

```bash
cdk deploy FomcMessagingStack --require-approval never
```

- [ ] Deploy completes without errors

### UAT-8.8: Verify SQS and Lambda in personal AWS

```bash
aws sqs list-queues | python -m json.tool
aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `fomc`)]' | python -m json.tool
```

- [ ] Both queues exist in AWS
- [ ] `fomc-analytics-processor` function exists in AWS

### UAT-8.9: Verify S3 → SQS notification in personal AWS

```bash
# Upload a test JSON file to trigger notification
echo '{"test": true}' > /tmp/test-trigger.json
aws s3 cp /tmp/test-trigger.json s3://fomc-datausa-raw/test-trigger.json

# Wait 5-10 seconds, then check analytics Lambda logs
aws logs tail /aws/lambda/fomc-analytics-processor --since 1m --follow
```

- [ ] S3 upload triggers SQS message
- [ ] SQS message triggers analytics Lambda
- [ ] Lambda logs show report execution (may error on test data — that's OK)

```bash
# Clean up test file
aws s3 rm s3://fomc-datausa-raw/test-trigger.json
```

---

## UAT-9: Full Pipeline End-to-End

### UAT-9.1: Deploy all stacks to LocalStack

```bash
CDK_LOCAL=true cdklocal deploy --all --require-approval never
```

- [ ] All 3 stacks deploy without errors

### UAT-9.2: Full pipeline test in LocalStack

```bash
# 1. Invoke data fetcher Lambda
awslocal lambda invoke --function-name fomc-data-fetcher /tmp/pipeline-out.json
cat /tmp/pipeline-out.json | python -m json.tool

# 2. Wait 5-10 seconds for S3 → SQS → Lambda cascade

# 3. Check analytics Lambda logs
awslocal logs tail /aws/lambda/fomc-analytics-processor
```

- [ ] Data fetcher Lambda returns `statusCode: 200`
- [ ] BLS data appears in `fomc-bls-raw` bucket
- [ ] DataUSA data appears in `fomc-datausa-raw` bucket
- [ ] SQS receives notification from `population.json` upload
- [ ] Analytics Lambda triggered by SQS
- [ ] Analytics logs contain Report 1, Report 2, and Report 3 output

### UAT-9.3: Deploy all stacks to personal AWS

```bash
cdk deploy --all --require-approval never
```

- [ ] All 3 stacks deploy without errors

### UAT-9.4: Full pipeline test in personal AWS

```bash
# 1. Invoke data fetcher
aws lambda invoke --function-name fomc-data-fetcher /tmp/aws-pipeline-out.json
cat /tmp/aws-pipeline-out.json | python -m json.tool

# 2. Wait 10-30 seconds for cascade

# 3. Check analytics logs
aws logs tail /aws/lambda/fomc-analytics-processor --since 2m --follow
```

- [ ] Data fetcher returns `statusCode: 200`
- [ ] Analytics Lambda triggered automatically
- [ ] Analytics logs show all 3 reports

### UAT-9.5: Full status check

```bash
# LocalStack
AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566 python src/helpers/aws_status.py

# AWS
unset AWS_ENDPOINT_URL
python src/helpers/aws_status.py
```

**Expected output structure**:
```json
{
  "s3": {
    "fomc-bls-raw": {"object_count": 20},
    "fomc-bls-silver": {"object_count": 0},
    "fomc-datausa-raw": {"object_count": 4},
    "fomc-datausa-silver": {"object_count": 0}
  },
  "sqs": {
    "fomc-analytics-queue": {"queue_url": "...", "message_count": 0},
    "fomc-analytics-dlq": {"queue_url": "...", "message_count": 0}
  },
  "lambda": {
    "fomc-data-fetcher": {"runtime": "python3.12", "memory": 256, "timeout": 300},
    "fomc-analytics-processor": {"runtime": "python3.12", "memory": 256, "timeout": 300}
  }
}
```

- [ ] S3: 4 buckets listed with object counts > 0 for raw buckets
- [ ] SQS: 2 queues listed with 0 pending messages (all processed)
- [ ] SQS: DLQ has 0 messages (no failures)
- [ ] Lambda: 2 functions listed with correct runtime, memory, timeout

---

## UAT-10: Cleanup

### UAT-10.1: Destroy LocalStack resources

```bash
CDK_LOCAL=true cdklocal destroy --all --force
```

- [ ] All stacks destroyed
- [ ] `awslocal s3 ls` returns no fomc buckets

### UAT-10.2: Destroy personal AWS resources (when done testing)

```bash
# Empty buckets first (required before deletion)
aws s3 rm s3://fomc-bls-raw --recursive
aws s3 rm s3://fomc-bls-silver --recursive
aws s3 rm s3://fomc-datausa-raw --recursive
aws s3 rm s3://fomc-datausa-silver --recursive

# Destroy stacks
cdk destroy --all --force
```

- [ ] All stacks destroyed
- [ ] `aws s3 ls | grep fomc` returns nothing
- [ ] `aws lambda list-functions` shows no fomc functions
- [ ] `aws sqs list-queues` shows no fomc queues

### UAT-10.3: Stop LocalStack

```bash
docker stop localstack
docker rm localstack
```

- [ ] Container stopped and removed

---

## UAT Summary Checklist

| UAT | Description | LocalStack | AWS |
|-----|-------------|------------|-----|
| 1 | Unit tests pass | ✅ N/A | ✅ N/A |
| 2 | Helpers & connectivity | ☐ | ☐ |
| 3 | CDK storage stack (4 S3 buckets) | ☐ | ☐ |
| 4 | BLS data fetcher + sync state | ☐ | ☐ |
| 5 | DataUSA API fetcher + sync state | ☐ | ☐ |
| 6 | PySpark analytics (3 reports) | ☐ | ☐ |
| 7 | CDK compute stack (Lambda + EventBridge) | ☐ | ☐ |
| 8 | CDK messaging stack (SQS + analytics Lambda) | ☐ | ☐ |
| 9 | Full pipeline end-to-end | ☐ | ☐ |
| 10 | Cleanup | ☐ | ☐ |
