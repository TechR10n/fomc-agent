# Lab 07 — (Optional/Advanced) Automate with AWS CDK

**Timebox:** 90–180 minutes (optional)  
**Outcome:** You can recreate the core AWS resources via infrastructure-as-code (CDK): S3 buckets, scheduled Lambda fetcher, SQS queue, and an analytics Lambda trigger.

Instructor note:
- This lab is intentionally “advanced”. If your goal is the capstone website and you’re short on time, you can skip to Lab 08 and run everything manually.

## What you’re doing in this lab

1. Install CDK Python dependencies
2. Initialize a CDK app (Python)
3. Define stacks:
   - Storage: S3 buckets (raw + website)
   - Compute: EventBridge schedule + fetcher Lambda
   - Messaging: S3 notification → SQS → analytics Lambda
4. Deploy and verify

## You start with

- Labs 01–06 completed (code works locally)
- Node + CDK CLI installed (`cdk --version`)

## 07.1 Add CDK dependencies to `pyproject.toml`

Edit `pyproject.toml` and add an optional dependency group:

```toml
[project.optional-dependencies]
cdk = ["aws-cdk-lib", "constructs"]
```

Then install:

```bash
uv sync --all-extras
source .venv/bin/activate
```

## 07.2 Create the CDK app structure

```bash
mkdir -p infra/stacks
touch infra/__init__.py infra/stacks/__init__.py
```

Create `cdk.json`:

```bash
cat > cdk.json <<'EOF'
{
  "app": "python3 app.py"
}
EOF
```

Create `app.py`:

```bash
cat > app.py <<'EOF'
import aws_cdk as cdk

from infra.stacks.storage_stack import StorageStack
from infra.stacks.compute_stack import ComputeStack
from infra.stacks.messaging_stack import MessagingStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

storage = StorageStack(app, "StorageStack", env=env)
compute = ComputeStack(app, "ComputeStack", storage=storage, env=env)
messaging = MessagingStack(app, "MessagingStack", storage=storage, env=env)

app.synth()
EOF
```

Instructor note:
- For a real project, you’d detect account/region from your AWS profile or environment. For a workshop, context variables are a simple way to control them.

## 07.3 Create the Storage stack

Create `infra/stacks/storage_stack.py`:

```bash
cat > infra/stacks/storage_stack.py <<'EOF'
from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # IMPORTANT: Use a unique prefix in real AWS (S3 names are global).
        prefix = self.node.try_get_context("bucket_prefix") or "fomc-lab"

        removal = RemovalPolicy.DESTROY  # workshop-friendly; change to RETAIN in real projects

        self.bls_raw = s3.Bucket(
            self,
            "BlsRaw",
            bucket_name=f"{prefix}-bls-raw",
            removal_policy=removal,
            auto_delete_objects=True,
        )
        self.datausa_raw = s3.Bucket(
            self,
            "DatausaRaw",
            bucket_name=f"{prefix}-datausa-raw",
            removal_policy=removal,
            auto_delete_objects=True,
        )
        self.website = s3.Bucket(
            self,
            "WebsiteBucket",
            bucket_name=f"{prefix}-website",
            website_index_document="index.html",
            public_read_access=True,
            removal_policy=removal,
            auto_delete_objects=True,
        )
EOF
```

## 07.4 Create the Compute stack (scheduled Lambda)

This is where packaging gets tricky. For a workshop, keep the Lambda minimal or bundle dependencies carefully.

Create `infra/stacks/compute_stack.py`:

```bash
cat > infra/stacks/compute_stack.py <<'EOF'
from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from constructs import Construct

from infra.stacks.storage_stack import StorageStack


class ComputeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage: StorageStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_root = str(Path(__file__).resolve().parents[2])

        self.fetcher = _lambda.Function(
            self,
            "Fetcher",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.lambdas.fetcher.handler",
            code=_lambda.Code.from_asset(project_root),
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "BLS_BUCKET": storage.bls_raw.bucket_name,
                "DATAUSA_BUCKET": storage.datausa_raw.bucket_name,
                "BLS_SERIES": "pr",
            },
        )

        storage.bls_raw.grant_read_write(self.fetcher)
        storage.datausa_raw.grant_read_write(self.fetcher)

        events.Rule(
            self,
            "DailySchedule",
            schedule=events.Schedule.cron(hour="9", minute="0"),
            targets=[targets.LambdaFunction(self.fetcher)],
        )
EOF
```

Now you must create the Lambda handler file referenced above:

```bash
mkdir -p src/lambdas
touch src/lambdas/__init__.py
cat > src/lambdas/fetcher.py <<'EOF'
import json
import os

from src.data_fetchers.bls_sync import sync_series
from src.data_fetchers.datausa_sync import sync_population


def handler(event, context):
    bls_bucket = os.environ["BLS_BUCKET"]
    datausa_bucket = os.environ["DATAUSA_BUCKET"]
    series = os.environ.get("BLS_SERIES", "pr")

    out = {"bls": None, "datausa": None, "errors": []}
    try:
        out["bls"] = sync_series(series, bls_bucket)
    except Exception as e:
        out["errors"].append({"source": "bls", "error": str(e)})
    try:
        out["datausa"] = sync_population(datausa_bucket)
    except Exception as e:
        out["errors"].append({"source": "datausa", "error": str(e)})

    return {"statusCode": 200 if not out["errors"] else 207, "body": json.dumps(out, default=str)}
EOF
```

Instructor note (dependency packaging warning):
- If your Lambda imports `requests` and it is not included in the deployed bundle, invocation will fail.
- Solving packaging cleanly is a separate deep topic; for a workshop, you can:
  - use LocalStack for code execution and keep AWS Lambda as “optional”, OR
  - bundle dependencies using CDK bundling / layers (advanced), OR
  - rewrite HTTP calls using the Python stdlib (`urllib.request`) (no external deps).

## 07.5 Create the Messaging stack (S3 → SQS → analytics)

Create `infra/stacks/messaging_stack.py`:

```bash
cat > infra/stacks/messaging_stack.py <<'EOF'
from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from infra.stacks.storage_stack import StorageStack


class MessagingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage: StorageStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_root = str(Path(__file__).resolve().parents[2])

        dlq = sqs.Queue(self, "AnalyticsDLQ", retention_period=Duration.days(14))
        queue = sqs.Queue(
            self,
            "AnalyticsQueue",
            visibility_timeout=Duration.minutes(6),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=dlq),
        )

        # When a .json file is created in the DataUSA bucket, send an SQS message
        storage.datausa_raw.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(suffix=".json"),
        )

        analytics = _lambda.Function(
            self,
            "Analytics",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="src.lambdas.analytics.handler",
            code=_lambda.Code.from_asset(project_root),
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "BLS_BUCKET": storage.bls_raw.bucket_name,
                "DATAUSA_BUCKET": storage.datausa_raw.bucket_name,
                "WEBSITE_BUCKET": storage.website.bucket_name,
            },
        )

        storage.bls_raw.grant_read(analytics)
        storage.datausa_raw.grant_read(analytics)
        storage.website.grant_read_write(analytics)

        analytics.add_event_source(event_sources.SqsEventSource(queue, batch_size=1))
EOF
```

Create the analytics Lambda handler file:

```bash
cat > src/lambdas/analytics.py <<'EOF'
import json
import logging
import os

from src.analytics.reports import run_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    bls_bucket = os.environ["BLS_BUCKET"]
    datausa_bucket = os.environ["DATAUSA_BUCKET"]

    # Recompute and write site JSON locally, then upload (simplest for a workshop)
    out = run_all(bls_bucket, datausa_bucket, site_json_out="/tmp/timeseries.json")
    logger.info(json.dumps(out, default=str))

    # Optional: upload to the website bucket (advanced; requires boto3 put_object here)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
EOF
```

## 07.6 Deploy (AWS only)

Set CDK context:

```bash
export AWS_PROFILE=fomc-workshop
unset AWS_ENDPOINT_URL

# Choose a globally unique prefix for buckets
export BUCKET_PREFIX="fomc-<yourname>-<yyyymmdd>"
```

Deploy:

```bash
cdk synth -c account="$(aws sts get-caller-identity --query Account --output text)" -c region=us-east-1 -c bucket_prefix="$BUCKET_PREFIX"
cdk bootstrap -c account="$(aws sts get-caller-identity --query Account --output text)" -c region=us-east-1
cdk deploy StorageStack ComputeStack MessagingStack --require-approval never -c account="$(aws sts get-caller-identity --query Account --output text)" -c region=us-east-1 -c bucket_prefix="$BUCKET_PREFIX"
```

## UAT Sign‑Off (Instructor)

- [ ] Student can synth a CDK template successfully
- [ ] Student can deploy the storage stack and see buckets created
- [ ] Student explains what CDK changes (and doesn’t change) about the workflow
- [ ] Student understands bucket name uniqueness and cleanup requirements

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add CloudFront in front of the website bucket (HTTPS)
- Add CDK `BucketDeployment` to automatically upload the `site/` folder
- Implement Lambda dependency bundling so the fetcher can run in AWS without surprises

