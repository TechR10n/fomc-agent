# AWS Console & PyCharm Setup for fomc-agent

## Service-by-Service Tutorials

For hands-on tutorials for each AWS service used by this project (S3, SQS, Lambda, EventBridge, IAM, CloudWatch, Cost Explorer, CloudFront, ACM, and CloudFormation), see:

- `docs/aws-service-tutorials.md`

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

### 1.3 Update `.env.shared` (Single Source of Truth)

Shared non-secret settings now live in `.env.shared` and are used by:
- LocalStack
- Local AWS/CDK runs
- GitHub Actions variable sync (`./tools/set_repo_vars.sh`)

Set at least:

```bash
# edit .env.shared
AWS_DEFAULT_REGION=us-east-1
FOMC_BUCKET_PREFIX=fomc-yourname-20260204
FOMC_ANALYTICS_QUEUE_NAME=fomc-analytics-queue
FOMC_ANALYTICS_DLQ_NAME=fomc-analytics-dlq
FOMC_REMOVAL_POLICY=destroy
FOMC_FETCH_INTERVAL_HOURS=1
BLS_SERIES=pr,cu,ce,ln,jt,ci
DATAUSA_DATASETS=population,commute_time,citizenship
DATAUSA_BASE_URL=https://api.datausa.io/tesseract
```

Replace `yourname-20260204` with something unique to avoid S3 bucket name collisions.

### 1.4 Create `.env.local` (AWS Local Overrides)

`.env.local` is gitignored and should contain local-only overrides such as your profile:

```bash
cat > .env.local <<'EOF'
AWS_PROFILE=fomc-agent
EOF
```

### 1.5 Bootstrap CDK (one-time per account/region)

CDK needs a bootstrap stack to manage assets:

```bash
source .env.shared
source .env.local
npx cdk bootstrap aws://$(aws sts get-caller-identity --profile fomc-agent --query Account --output text)/us-east-1 --profile fomc-agent
```

### 1.6 Verify S3 Access

Quick sanity check:

```bash
aws s3 ls --profile fomc-agent
```

No errors means you're good. An empty list is fine if the account is new.

### 1.7 Site URL (AWS-managed)

Deploy `FomcSiteStack` and use the `SiteUrl` CloudFormation output to access the site.

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
   AWS_PROFILE=fomc-agent
   ```
3. Alternatively, check **EnvFile** plugin and point it to:
   - `.env.shared` first
   - `.env.local` second (AWS runs) or `.env.localstack` second (LocalStack runs)

### 2.5 Run Tests

PyCharm auto-detects pytest from `pyproject.toml` — no manual configuration needed.

- Right-click `tests/` in the Project pane > **Run 'pytest in tests'**
- Or open any test file and click the green gutter icon next to a test function

All unit tests use `moto` to mock AWS, so they run without real credentials.

### 2.6 Useful PyCharm Plugins

| Plugin | Why |
|--------|-----|
| **EnvFile** | Load `.env.shared` plus `.env.local`/`.env.localstack` automatically |
| **AWS Toolkit** | Browse S3 buckets, view Lambda functions, read CloudWatch logs |
| **.env files support** | Syntax highlighting for `.env.shared` |

### 2.7 CDK and Infrastructure Files

PyCharm should recognize the `infra/` directory as normal Python. If you see unresolved imports for `aws_cdk`:

```bash
uv sync --extra cdk
```

This installs `aws-cdk-lib` and `constructs` into your venv.

---

## 3. LocalStack (Local AWS)

Run the full pipeline locally without touching your AWS account. Requires Docker.

### 3.1 Optional: Set Your Auth Token (Pro only)

If you use LocalStack Pro features, find your token at [app.localstack.cloud](https://app.localstack.cloud/getting-started) and set it in `.env.localstack`:

```bash
# edit .env.localstack
LOCALSTACK_AUTH_TOKEN=ls-xxxxxxxx
```

This repo's `docker-compose.yml` loads `.env.shared` and `.env.localstack` for the LocalStack container, so shared settings and LocalStack overrides stay aligned.

### 3.2 Start LocalStack

```bash
docker compose up -d
```

This starts the `fomc-localstack` container and automatically creates all S3 buckets and SQS queues via the init hook in `localstack/init/ready.d/init-aws.sh`.

Verify it's ready:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls
```

You should see all four buckets derived from `FOMC_BUCKET_PREFIX` in `.env.shared`.

### 3.3 Run the Pipeline Locally

Fetch data into local S3 (uses real BLS and DataUSA APIs, stores in LocalStack S3):

```bash
source .env.shared
source .env.localstack
python -m src.data_fetchers.bls_getter
python -m src.data_fetchers.datausa_getter
```

Verify data landed:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls "s3://${FOMC_BUCKET_PREFIX}-bls-raw/pr/" --recursive | head
aws --endpoint-url=http://localhost:4566 s3 ls "s3://${FOMC_BUCKET_PREFIX}-datausa-raw/"
```

Run analytics against local data:

```bash
source .env.shared
source .env.localstack
python -m src.analytics.reports
```

### 3.4 PyCharm Run Configurations (One-Click)

The project includes shared run configurations in `.run/` that appear automatically in PyCharm's Run dropdown:

| Configuration | What It Does | Target |
|---------------|-------------|--------|
| **LocalStack Up (CLI)** | Starts LocalStack via `docker compose up -d` + waits for health | Docker |
| **LocalStack Down (CLI)** | Stops LocalStack via `docker compose down` | Docker |
| **Docker Compose Up (LocalStack)** | Starts LocalStack container | Docker |
| **Fetch Data (LocalStack)** | Fetches all BLS series into LocalStack S3 | LocalStack |
| **Fetch DataUSA (LocalStack)** | Fetches DataUSA datasets into LocalStack S3 | LocalStack |
| **Invoke Fetcher Lambda (LocalStack)** | Runs the deployed Lambda handler locally (writes to LocalStack S3) | LocalStack |
| **Parse to Processed (LocalStack)** | Converts raw data into processed CSVs in LocalStack S3 | LocalStack |
| **LocalStack Worker (Analytics)** | Polls LocalStack SQS and runs analytics Lambda handler locally | LocalStack |
| **Touch DataUSA (LocalStack)** | Re-uploads `population.json` to trigger S3→SQS (fast re-run) | LocalStack |
| **Analytics (LocalStack)** | Generates `site/data/*.json` charts from LocalStack S3 | LocalStack |
| **Build BLS Timeline (LocalStack)** | Generates `site/data/bls_timeline.json` from LocalStack sync logs | LocalStack |
| **LocalStack Full Refresh** | Runs the full LocalStack pipeline in order (ingest → parse → charts → timeline) | LocalStack |
| **LocalStack CDC Demo** | One-shot CDC demo (worker + touch, exits) | LocalStack |
| **LocalStack CDC Demo (Live)** | Starts worker, triggers CDC, and keeps worker running | LocalStack |
| **LocalStack Worker Stop** | Stops the live LocalStack worker if running | LocalStack |
| **LocalStack Full Refresh + CDC Live** | Full refresh, then start CDC live worker | LocalStack |
| **Fetch Data (AWS)** | Fetches BLS series into real AWS S3 | AWS |
| **Fetch DataUSA (AWS)** | Fetches DataUSA datasets into real AWS S3 | AWS |
| **Parse to Processed (AWS)** | Converts raw data into processed CSVs in AWS S3 | AWS |
| **Analytics (AWS)** | Generates `site/data/*.json` charts from AWS S3 | AWS |
| **Build BLS Timeline (AWS)** | Generates `site/data/bls_timeline.json` from AWS sync logs | AWS |
| **Build AWS Observability (AWS)** | Generates `site/data/aws_observability.json` for the Timeline page | AWS |
| **CDK Diff (AWS)** | Runs `cdk diff --all` (loads `.env.shared` + `.env.local`) | AWS |
| **CDK Deploy (AWS)** | Runs `cdk deploy --all --require-approval never` (loads `.env.shared` + `.env.local`) | AWS |
| **Run Tests** | Runs unit tests (moto mocks, no AWS needed) | Neither |

Each LocalStack config has `AWS_ENDPOINT_URL=http://localhost:4566` pre-set in `.env.localstack`. Run configs include EnvFile entries (if you install the EnvFile plugin) that load `.env.shared` first, then `.env.localstack` or `.env.local`.

#### Chart Refresh Order (so the site renders complete data)

**LocalStack (fast demo):**
1. **LocalStack Up (CLI)** (or **Docker Compose Up (LocalStack)**)
2. **Invoke Fetcher Lambda (LocalStack)** (or **Fetch Data (LocalStack)** + **Fetch DataUSA (LocalStack)**)
3. **Parse to Processed (LocalStack)**
4. **Analytics (LocalStack)**
5. **Build BLS Timeline (LocalStack)**

**AWS (full cloud data + timeline):**
1. **CDK Deploy (AWS)** (run **CDK Diff (AWS)** first if you want a preview)
2. **Fetch Data (AWS)**
3. **Fetch DataUSA (AWS)**
4. **Parse to Processed (AWS)**
5. **Analytics (AWS)**
6. **Build BLS Timeline (AWS)**
7. **Build AWS Observability (AWS)**

#### Recommended iteration cadence (two-speed loop)

**Fast loop (few times per hour):**
1. Start LocalStack once (**LocalStack Up (CLI)**) and leave it running.
2. Run **LocalStack Worker (Analytics)** and leave it running.
3. Change code in `src/lambdas/analytics_processor/handler.py`.
4. Trigger a fresh event without re-fetching APIs (**Touch DataUSA (LocalStack)**).
5. Run **Run Tests** often (fast, no AWS/localstack required).

**Slow loop (few times per day):**
1. Run **CDK Diff (AWS)**.
2. Run **CDK Deploy (AWS)** when ready.

#### LocalStack pre-push gate (recommended)

Before pushing changes, run one full local validation pass:

```bash
source .env.shared
source .env.localstack
python tools/localstack_full_refresh.py
python tools/check_s3_assets.py --env-file .env.localstack --strict
python -m pytest
```

If those pass, push to `main` to trigger the AWS deploy workflow.

### 3.5 Stop LocalStack

```bash
docker compose down
```

Add `-v` to also remove persisted data:

```bash
docker compose down -v
```

---

## 4. GitHub Actions Deploy on `main`

This repo now includes `.github/workflows/ci-deploy.yml`:

- On PRs: runs pytest + `cdk synth --all`.
- On pushes to `main`: runs pytest + synth, then deploys all CDK stacks to AWS.

### 4.1 Required GitHub Repository Secret

- `AWS_DEPLOY_ROLE_ARN`: IAM role ARN that GitHub Actions assumes via OIDC.

### 4.2 Required/Optional GitHub Repository Variables

Use `.env.shared` as the single source of truth for non-secret deploy settings, then sync them to GitHub:

```bash
./tools/set_repo_vars.sh
```

Prerequisite: GitHub CLI installed and authenticated (`gh auth login`).

Optional:

```bash
# preview changes without applying
./tools/set_repo_vars.sh --dry-run
```

The script sets:

- Required:
  - `AWS_DEFAULT_REGION`
  - `FOMC_BUCKET_PREFIX`
  - `FOMC_ANALYTICS_QUEUE_NAME`
  - `FOMC_ANALYTICS_DLQ_NAME`
  - `FOMC_REMOVAL_POLICY`
  - `FOMC_FETCH_INTERVAL_HOURS`
  - `BLS_SERIES`
  - `DATAUSA_DATASETS`
  - `DATAUSA_BASE_URL`
- Optional (set when present, removed when blank):
  - `FOMC_SITE_DOMAIN`
  - `FOMC_SITE_CERT_ARN`
  - `FOMC_SITE_ALIASES`

### 4.3 IAM Role for GitHub OIDC

Create an IAM role trusted by GitHub's OIDC provider with a trust policy scoped to this repo/branch (`main`), then attach permissions needed for CDK deploy (CloudFormation, IAM, Lambda, S3, SQS, EventBridge, CloudFront).

High-level flow:
1. Add GitHub OIDC provider (`token.actions.githubusercontent.com`) in AWS IAM (one-time per account).
2. Create deploy role and trust only this repository's `main` branch.
3. Add that role ARN to the repo secret `AWS_DEPLOY_ROLE_ARN`.

Then every push to `main` deploys via GitHub Actions.

---

## 5. Quick Verification Checklist

```
[ ] aws sts get-caller-identity --profile fomc-agent       # returns your account
[ ] python3 --version                                      # 3.12+
[ ] uv --version                                           # installed
[ ] .env.shared has desired shared values (single source for LocalStack + AWS + GitHub vars)
[ ] .env.local exists (for local AWS profile overrides, e.g. AWS_PROFILE)
[ ] .env.localstack exists (LocalStack endpoint/credentials)
[ ] PyCharm interpreter points to .venv/bin/python
[ ] pytest runs green in PyCharm (right-click tests/)
[ ] docker compose up -d                                   # LocalStack starts
[ ] aws --endpoint-url=http://localhost:4566 s3 ls          # shows prefix-derived buckets
[ ] PyCharm shows run configs in the Run dropdown
[ ] GitHub secret AWS_DEPLOY_ROLE_ARN is set
[ ] ./tools/set_repo_vars.sh                                 # sync non-secret GitHub vars from .env.shared
```

## 6. Common Issues

| Problem                     | Fix                                                                             |
|-----------------------------|---------------------------------------------------------------------------------|
| `NoCredentialProviders`     | Check `AWS_PROFILE` matches your configured profile name                        |
| S3 bucket name conflict     | Bucket names are globally unique — add your name + date to `FOMC_BUCKET_PREFIX` |
| CDK bootstrap error         | Make sure Node.js is installed (`node --version`) and you ran `cdk bootstrap`   |
| PyCharm can't find `boto3`  | Ensure interpreter points to `.venv` and run `uv sync`                          |
| Lambda imports fail locally | Run from project root, not from `src/`                                          |
| LocalStack Pro token missing in PyCharm | Put `LOCALSTACK_AUTH_TOKEN` in `.env.localstack`, then restart LocalStack (`docker compose down && docker compose up -d`) |
| LocalStack buckets missing  | Check `docker compose logs` for init hook errors; re-run `docker compose up -d` |
| Run configs not showing     | Reopen the project in PyCharm; configs are in `.run/` and auto-detected         |
