# FOMC Agent — User Acceptance Testing (AWS)

This document defines manual verification steps needed to confirm the FOMC data pipeline works correctly in AWS.

If you want a guided, lab-by-lab curriculum (recommended order), start with `docs/lab-plan/README.md` and use this document for deeper verification steps.

## Conventions

- Commands assume you are running from the repo root.
- Commands assume your venv is active (`source .venv/bin/activate`). If it isn’t, replace `python` with `.venv/bin/python`.
- Use a dedicated AWS profile for this project via `AWS_PROFILE`.
- S3 bucket names are **globally unique**. This project uses `FOMC_BUCKET_PREFIX` to derive bucket names.

---

## Prerequisites

### Software Requirements

| Tool | Install Command | Verify |
|------|----------------|--------|
| AWS CLI v2 | `brew install awscli` | `aws --version` |
| Node.js (for CDK) | `brew install node` | `node --version` |
| AWS CDK | `npm install -g aws-cdk` | `cdk --version` |
| Python 3.12+ | Already installed | `python3 --version` |
| uv | `brew install uv` | `uv --version` |

### Environment Setup

```bash
# 1) Install Python dependencies (includes CDK + dev tooling)
uv sync --all-extras

# 2) Activate the virtualenv
source .venv/bin/activate

# 3) Choose your AWS profile + region
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1

# 4) Choose a globally-unique bucket prefix
export FOMC_BUCKET_PREFIX="fomc-<yourname>-<yyyymmdd>"

# Optional (workshop convenience; allows `cdk destroy` to delete buckets/objects)
export FOMC_REMOVAL_POLICY=destroy  # alternatives: retain

# 5) Verify AWS auth works
aws sts get-caller-identity
```

---

## UAT-1: Unit Test Suite (Local)

### UAT-1.1: Run full test suite

```bash
python -m pytest tests/unit/ -v --cov=src --cov-report=term-missing
```

**Expected result**:
- [ ] All tests pass
- [ ] Coverage report prints successfully

---

## UAT-2: Helpers & AWS Connectivity

### UAT-2.1: AWS connectivity via status helper

```bash
python src/helpers/aws_status.py
```

**Expected result**:
- [ ] Script runs without auth/region errors
- [ ] Output is valid JSON with keys `s3`, `sqs`, `lambda`

---

## UAT-3: Deploy Infrastructure (AWS CDK)

### UAT-3.1: Synth + bootstrap

```bash
cdk synth
cdk bootstrap
```

- [ ] Both commands exit with code 0

### UAT-3.2: Deploy stacks

```bash
cdk deploy --all --require-approval never
```

- [ ] Deploy completes without errors

### UAT-3.3: Verify resources exist

Derived bucket names:

```bash
export BLS_BUCKET="${FOMC_BUCKET_PREFIX}-bls-raw"
export DATAUSA_BUCKET="${FOMC_BUCKET_PREFIX}-datausa-raw"
```

Verify buckets:

```bash
aws s3 ls | grep "$FOMC_BUCKET_PREFIX" || true
```

Verify Lambdas:

```bash
aws lambda get-function --function-name fomc-data-fetcher >/dev/null
aws lambda get-function --function-name fomc-analytics-processor >/dev/null
```

Verify SQS queue:

```bash
aws sqs list-queues --queue-name-prefix fomc-analytics-queue | python -m json.tool
```

---

## UAT-4: End-to-End Pipeline (Invoke Fetcher → S3 → SQS → Analytics Lambda)

### UAT-4.1: Invoke the fetcher Lambda

```bash
aws lambda invoke --function-name fomc-data-fetcher /tmp/pipeline-out.json
cat /tmp/pipeline-out.json | python -m json.tool
```

- [ ] `statusCode` is `200` (or `207` with partial failures)
- [ ] `errors` array is empty (or contains actionable error messages)

### UAT-4.2: Verify S3 objects exist

```bash
aws s3 ls "s3://$BLS_BUCKET/pr/" | head
aws s3 ls "s3://$DATAUSA_BUCKET/" | grep population.json
```

- [ ] BLS objects exist under `pr/`
- [ ] `population.json` exists in DataUSA bucket

### UAT-4.3: Verify analytics Lambda ran

S3 → SQS → Lambda is asynchronous; give it a minute after the fetcher invocation.

```bash
aws logs tail "/aws/lambda/fomc-analytics-processor" --since 15m
```

- [ ] Logs show processing activity for `population.json`
- [ ] No unhandled exceptions

---

## UAT-5: Pandas Analytics (Local, reading from AWS S3)

```bash
export BLS_BUCKET="${FOMC_BUCKET_PREFIX}-bls-raw"
export DATAUSA_BUCKET="${FOMC_BUCKET_PREFIX}-datausa-raw"

python src/analytics/reports.py | head -80
```

- [ ] Script runs without errors
- [ ] Output is valid JSON with the three reports

---

## UAT-6: Static Website (S3)

Generate the website data file (writes `site/data/timeseries.json`):

```bash
python src/analytics/reports.py >/dev/null
```

Deploy the site stack:

```bash
cdk deploy FomcSiteStack --require-approval never
```

- [ ] CDK output includes `SiteUrl`
- [ ] Opening `SiteUrl` renders a chart

---

## UAT-7: Cleanup

```bash
cdk destroy --all --force
```

- [ ] Stacks delete successfully
- [ ] If `FOMC_REMOVAL_POLICY=retain`, delete buckets manually after verifying nothing important remains
