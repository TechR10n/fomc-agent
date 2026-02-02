# Lab 03 — LocalStack Integration (Recommended)

**Timebox:** 45–75 minutes  
**Outcome:** You can run the same code against “fake AWS” locally (LocalStack), which is safer and cheaper while learning.

## What you’re doing in this lab

1. Run LocalStack
2. Route boto3 and AWS CLI to LocalStack
3. Prove it works by creating S3 buckets locally and listing them

## You start with

- Docker installed and running
- The Python project from Lab 02

## 03.1 Start LocalStack

```bash
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

Verify it’s healthy:

```bash
curl -s http://localhost.localstack.cloud:4566/_localstack/health | python -m json.tool
```

Expected:
- JSON showing services (s3/sqs/lambda) as available

## 03.2 Install `awslocal` (wrapper around AWS CLI)

In your venv:

```bash
python -m pip install awscli-local
awslocal --version
```

## 03.3 Set LocalStack environment variables

Create `.env.local`:

```bash
cat > .env.local <<'EOF'
AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
EOF
```

Load it into your current shell:

```bash
set -a; source .env.local; set +a
echo "$AWS_ENDPOINT_URL"
```

Expected:
- Prints the LocalStack endpoint URL

## 03.4 Prove boto3 routes to LocalStack

```bash
python src/helpers/aws_status.py
```

Expected:
- JSON (may be empty initially)

## 03.5 Create a bucket in LocalStack and list it

```bash
awslocal s3api create-bucket --bucket localstack-demo-bucket
awslocal s3 ls | grep localstack-demo-bucket
```

Expected:
- The bucket appears in the listing

Cleanup (optional):

```bash
awslocal s3api delete-bucket --bucket localstack-demo-bucket
```

## UAT Sign‑Off (Instructor)

- [ ] Student can start LocalStack and hit the health endpoint
- [ ] Student can run `python src/helpers/aws_status.py` with LocalStack env vars
- [ ] Student can create and delete an S3 bucket via `awslocal`
- [ ] Student explains the difference between `AWS_PROFILE` and `AWS_ENDPOINT_URL`

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add a second LocalStack demo: create an SQS queue and list it
- Add a “mode switch” helper script that prints whether you’re using AWS or LocalStack

