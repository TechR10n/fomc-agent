# FOMC Agent — Milestone Plan (Tutorial Checklist)

Use this file to stay on track. It’s the **recommended order** of work. For exhaustive verification steps, see `docs/uat.md`.

## Conventions

- Run commands from the repo root.
- Prefer using the project venv:
  - One-time: `uv sync --all-extras`
  - Each new shell: `source .venv/bin/activate`
- If you *don’t* activate the venv, replace `python` with `.venv/bin/python`.
- LocalStack is optional until Milestone 2. Start it only when you’re ready.

## Milestone 0 — Environment Ready

**Goal:** You can run Python + tests locally.

1. Install dependencies (includes CDK + dev tooling):
   ```bash
   uv sync --all-extras
   ```
2. Activate the venv:
   ```bash
   source .venv/bin/activate
   python --version
   ```
3. (Optional, for PySpark analytics) Install Java 21 and set `JAVA_HOME`:
   ```bash
   export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
   java -version
   ```

**Done when:**
- [ ] `python --version` works (from the venv)
- [ ] `java -version` works (if doing PySpark)

## Milestone 1 — Unit Tests Green (Fast Feedback)

**Goal:** Core logic is correct without AWS/LocalStack.

1. Run unit tests:
   ```bash
   python -m pytest tests/unit -v
   ```
2. (Optional) Run with coverage:
   ```bash
   python -m pytest tests/unit -v --cov=src --cov-report=term-missing
   ```

**Done when:**
- [ ] Unit tests pass locally

See also: `docs/uat.md` → UAT-1.

## Milestone 2 — LocalStack Connectivity (No CDK Yet)

**Goal:** Your shell can talk to LocalStack via boto3/AWS CLI.

1. Start LocalStack:
   ```bash
   docker run -d --name localstack -p 4566:4566 localstack/localstack
   ```
2. Load the repo’s LocalStack env vars:
   ```bash
   set -a; source .env.local; set +a
   ```
3. Verify LocalStack health:
   ```bash
   curl -s http://localhost.localstack.cloud:4566/_localstack/health | python -m json.tool
   ```
4. Verify boto3 connectivity (no resources yet is fine):
   ```bash
   python src/helpers/aws_status.py
   ```

**Done when:**
- [ ] LocalStack health shows `s3`, `sqs`, `lambda` as available
- [ ] `src/helpers/aws_status.py` returns valid JSON without errors

See also: `docs/uat.md` → UAT-2.

## Milestone 3 — Storage Stack Deployed (Buckets Exist)

**Goal:** S3 buckets exist in LocalStack (and optionally AWS).

1. Deploy storage stack to LocalStack:
   ```bash
   CDK_LOCAL=true cdklocal bootstrap aws://000000000000/us-east-1
   CDK_LOCAL=true cdklocal deploy FomcStorageStack --require-approval never
   ```
2. Verify buckets:
   ```bash
   awslocal s3 ls
   ```

**Done when:**
- [ ] 4 buckets exist (`*-bls-raw`, `*-bls-silver`, `*-datausa-raw`, `*-datausa-silver`)

See also: `docs/uat.md` → UAT-3.

## Milestone 4 — Ingestion Scripts Working (Raw Data Lands in S3)

**Goal:** The sync scripts write to LocalStack S3 and are idempotent.

1. BLS sync:
   ```bash
   python src/data_fetchers/bls_getter.py
   awslocal s3 ls s3://fomc-bls-raw/pr/ | head
   ```
2. Re-run to confirm idempotency:
   ```bash
   python src/data_fetchers/bls_getter.py
   ```
3. DataUSA sync:
   ```bash
   python src/data_fetchers/datausa_getter.py
   awslocal s3 ls s3://fomc-datausa-raw/ | grep population.json
   ```
4. Re-run to confirm idempotency:
   ```bash
   python src/data_fetchers/datausa_getter.py
   ```

**Done when:**
- [ ] BLS objects exist under `pr/`
- [ ] DataUSA `population.json` exists
- [ ] Re-runs report “unchanged” / skip work when no changes

See also: `docs/uat.md` → UAT-4 and UAT-5.

## Milestone 5 — Analytics Reports Valid (Local PySpark)

**Goal:** The report logic matches the assignment requirements.

1. Run analytics reports against LocalStack data:
   ```bash
   python src/analytics/reports.py
   ```
2. Validate outputs match the expectations in `docs/instructions.md`.

**Done when:**
- [ ] Report 1 returns mean/stddev for years 2013–2018 inclusive
- [ ] Report 2 returns “best year” per series_id
- [ ] Report 3 joins `PRS30006032` Q01 with population when available

See also: `docs/uat.md` → UAT-6.

## Milestone 6 — Serverless Pipeline End-to-End (CDK Compute + Messaging)

**Goal:** Scheduled fetcher → S3 write → SQS notification → analytics processor runs.

1. **Decide Lambda dependency packaging approach** (required before deploy):
   - The fetcher Lambda imports `requests` via `src/data_fetchers/*`.
   - Ensure deployments include third-party deps (either via CDK bundling, a Lambda layer, or by removing the dependency).
2. Deploy compute stack (fetcher Lambda) and validate it writes to S3.
3. Deploy messaging stack (S3→SQS and analytics Lambda), then validate logs show report output.

**Done when:**
- [ ] Fetcher Lambda runs successfully
- [ ] Uploading `population.json` triggers SQS + analytics Lambda
- [ ] Analytics Lambda logs show report output

See also: `docs/uat.md` → UAT-7, UAT-8, UAT-9.

## Milestone 7 — Optional: Personal AWS Validation

**Goal:** Same pipeline works in real AWS.

1. Make S3 bucket names unique (S3 is global).
   - If you keep the default `fomc-*` names, deployment may fail due to collisions.
2. Deploy stacks and rerun the same validations as LocalStack.

See also: `docs/uat.md` → AWS variants of each UAT section.

## Milestone 8 — Cleanup

**Goal:** No leftover resources/costs.

Follow `docs/uat.md` → UAT-10.

