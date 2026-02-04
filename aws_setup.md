# AWS Console & PyCharm Setup for fomc-agent

## 1. AWS Console Setup

### 1.1 Create a Dedicated IAM User (or use SSO)

Avoid using the root account. Create an IAM user with programmatic access:

1. Go to **IAM > Users > Create user**
2. Name: `fomc-workshop` (or your preferred name)
3. Attach policies — for this workshop the simplest path is:
    - `AmazonS3FullAccess`
    - `AWSLambda_FullAccess`
    - `AmazonSQSFullAccess`
    - `AmazonEventBridgeFullAccess`
    - `AWSCloudFormationFullAccess`
    - `IAMFullAccess` (needed by CDK to create roles)
4. Create an **Access Key** (CLI use case) and save the credentials

> **Tighter alternative:** Create a custom policy scoped to `fomc-*` resources only. The broad policies above are acceptable for a throwaway workshop account.

### 1.2 Configure the AWS CLI Profile

```bash
aws configure --profile fomc-agent
```

Enter:
- **Access Key ID** and **Secret Access Key** from step 1.1
- **Default region:** `us-east-1`
- **Default output format:** `json`

Verify it works:

```bash
aws sts get-caller-identity --profile fomc-agent
```

You should see your account ID, user ARN, and user ID.

### 1.3 Create `.env.local`

This file is gitignored and holds project-specific environment variables:

```bash
cat > .env.local <<'EOF'
AWS_DEFAULT_REGION=us-east-1
AWS_PROFILE=fomc-agent
FOMC_BUCKET_PREFIX=fomc-yourname-20260204
FOMC_REMOVAL_POLICY=destroy
EOF
```

Replace `yourname-20260204` with something unique to avoid S3 bucket name collisions.

### 1.4 Bootstrap CDK (one-time per account/region)

CDK needs a bootstrap stack to manage assets:

```bash
source .env.local
npx cdk bootstrap aws://$(aws sts get-caller-identity --profile fomc-agent --query Account --output text)/us-east-1 --profile fomc-agent
```

### 1.5 Verify S3 Access

Quick sanity check:

```bash
aws s3 ls --profile fomc-agent
```

No errors means you're good. An empty list is fine if the account is new.

---

## 2. PyCharm Setup

### 2.1 Open the Project

1. **File > Open** and select the `fomc-agent` directory
2. PyCharm should detect `pyproject.toml` automatically

### 2.2 Configure the Python Interpreter

This project uses `uv` for dependency management and requires Python 3.12+.

1. **File > Settings > Project: fomc-agent > Python Interpreter**
   (macOS: **PyCharm > Settings > ...** or `Cmd+,`)
2. Click the gear icon > **Add Interpreter > Add Local Interpreter**
3. Select **Virtualenv Environment > Existing**
4. Point to the existing venv: `<project-root>/.venv/bin/python`

If no `.venv` exists yet, create it first from the terminal:

```bash
uv venv
uv sync --all-extras
```

Then point PyCharm to `.venv/bin/python`.

### 2.3 Mark Source Roots

So PyCharm resolves imports like `from src.config import ...` correctly:

1. Right-click the **project root** folder in the Project pane
2. **Mark Directory as > Sources Root**

Do **not** mark `src/` as a source root — the imports use `src.` as a package prefix.

### 2.4 Configure Environment Variables

For running scripts and tests inside PyCharm:

1. **Run > Edit Configurations > Edit Configuration Templates > Python**
2. Under **Environment variables**, add:
   ```
   AWS_DEFAULT_REGION=us-east-1
   AWS_PROFILE=fomc-agent
   FOMC_BUCKET_PREFIX=fomc-yourname-20260204
   FOMC_REMOVAL_POLICY=destroy
   ```
3. Alternatively, check **EnvFile** plugin and point it to `.env.local`

### 2.5 Run Tests

PyCharm auto-detects pytest from `pyproject.toml` — no manual configuration needed.

- Right-click `tests/` in the Project pane > **Run 'pytest in tests'**
- Or open any test file and click the green gutter icon next to a test function

All unit tests use `moto` to mock AWS, so they run without real credentials.

### 2.6 Useful PyCharm Plugins

| Plugin | Why |
|--------|-----|
| **EnvFile** | Load `.env.local` into run configurations automatically |
| **AWS Toolkit** | Browse S3 buckets, view Lambda functions, read CloudWatch logs |
| **.env files support** | Syntax highlighting for `.env.local` |

### 2.7 CDK and Infrastructure Files

PyCharm should recognize the `infra/` directory as normal Python. If you see unresolved imports for `aws_cdk`:

```bash
uv sync --extra cdk
```

This installs `aws-cdk-lib` and `constructs` into your venv.

---

## 3. LocalStack (Local AWS)

Run the full pipeline locally without touching your AWS account. Requires Docker and a LocalStack Pro account ([app.localstack.cloud](https://app.localstack.cloud/getting-started)).

### 3.1 Set Your Auth Token

Find your token at [app.localstack.cloud](https://app.localstack.cloud/getting-started) and export it:

```bash
export LOCALSTACK_AUTH_TOKEN=ls-xxxxxxxx
```

Add it to your shell profile (`~/.zshrc`) so it persists across sessions.

### 3.2 Start LocalStack

```bash
docker compose up -d
```

This starts the `fomc-localstack` container and automatically creates all S3 buckets and SQS queues via the init hook in `localstack/init/ready.d/init-aws.sh`.

Verify it's ready:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls
```

You should see all four `fomc-*` buckets.

### 3.3 Run the Pipeline Locally

Fetch data into local S3 (uses real BLS and DataUSA APIs, stores in LocalStack S3):

```bash
source .env.localstack
python src/data_fetchers/bls_getter.py
python src/data_fetchers/datausa_getter.py
```

Verify data landed:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://fomc-bls-raw/pr/ --recursive | head
aws --endpoint-url=http://localhost:4566 s3 ls s3://fomc-datausa-raw/
```

Run analytics against local data:

```bash
source .env.localstack
python src/analytics/reports.py
```

### 3.4 PyCharm Run Configurations (One-Click)

The project includes shared run configurations in `.run/` that appear automatically in PyCharm's Run dropdown:

| Configuration | What It Does | Target |
|---------------|-------------|--------|
| **Fetch Data (LocalStack)** | Fetches all BLS series into local S3 | LocalStack |
| **Fetch DataUSA (LocalStack)** | Fetches population data into local S3 | LocalStack |
| **Analytics (LocalStack)** | Runs reports against local S3 data | LocalStack |
| **Fetch Data (AWS)** | Fetches BLS series into real AWS S3 | AWS |
| **Run Tests** | Runs unit tests (moto mocks, no AWS needed) | Neither |
| **Docker Compose Up (LocalStack)** | Starts LocalStack container | Docker |

Each LocalStack config has `AWS_ENDPOINT_URL=http://localhost:4566` pre-set. The AWS config uses your `fomc-agent` profile. No manual env var setup needed — just select a config and click Run.

### 3.5 Stop LocalStack

```bash
docker compose down
```

Add `-v` to also remove persisted data:

```bash
docker compose down -v
```

---

## 4. Quick Verification Checklist

```
[ ] aws sts get-caller-identity --profile fomc-agent       # returns your account
[ ] python3 --version                                      # 3.12+
[ ] uv --version                                           # installed
[ ] .env.local exists with FOMC_BUCKET_PREFIX set
[ ] PyCharm interpreter points to .venv/bin/python
[ ] pytest runs green in PyCharm (right-click tests/)
[ ] docker compose up -d                                   # LocalStack starts
[ ] aws --endpoint-url=http://localhost:4566 s3 ls          # shows fomc-* buckets
[ ] PyCharm shows run configs in the Run dropdown
```

## 4. Common Issues

| Problem                     | Fix                                                                             |
|-----------------------------|---------------------------------------------------------------------------------|
| `NoCredentialProviders`     | Check `AWS_PROFILE` matches your configured profile name                        |
| S3 bucket name conflict     | Bucket names are globally unique — add your name + date to `FOMC_BUCKET_PREFIX` |
| CDK bootstrap error         | Make sure Node.js is installed (`node --version`) and you ran `cdk bootstrap`   |
| PyCharm can't find `boto3`  | Ensure interpreter points to `.venv` and run `uv sync`                          |
| Lambda imports fail locally | Run from project root, not from `src/`                                          |
| LocalStack exits with code 55 | `LOCALSTACK_AUTH_TOKEN` not set — export it before `docker compose up`         |
| LocalStack buckets missing  | Check `docker compose logs` for init hook errors; re-run `docker compose up -d` |
| Run configs not showing     | Reopen the project in PyCharm; configs are in `.run/` and auto-detected         |
