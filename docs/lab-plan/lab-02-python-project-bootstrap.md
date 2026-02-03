# Lab 02 — Python Project Bootstrap (From Scratch)

**Timebox:** 60–90 minutes  
**Outcome:** You have a working Python project with tests, and a small helper module that can talk to AWS via boto3.

## What you’re doing in this lab

1. Create a clean Python project structure
2. Use `uv` to manage dependencies and a local `.venv`
3. Implement a tiny AWS client helper (`get_client`) + a status script
4. Write unit tests using `pytest` + `moto`

## You start with

- The folder created in Lab 00
- `python3` and `uv` installed

## 02.1 Create the project skeleton

```bash
mkdir -p src/helpers tests/unit/helpers
touch src/__init__.py src/helpers/__init__.py tests/__init__.py tests/unit/__init__.py tests/unit/helpers/__init__.py
```

## 02.2 Create `pyproject.toml`

This is a minimal dependency set for the course.

```bash
cat > pyproject.toml <<'EOF'
[project]
name = "fomc-agent-lab"
version = "0.0.1"
description = "Workshop project: S3 ingestion + analytics + static site capstone"
requires-python = ">=3.12"
dependencies = [
  "boto3",
  "pandas",
]

[project.optional-dependencies]
dev = [
  "pytest",
  "pytest-cov",
  "moto[s3,sqs,lambda,iam]",
]
EOF
```

## 02.3 Create the virtualenv and install deps

```bash
uv sync --all-extras
source .venv/bin/activate
python --version
```

Expected:
- `python` works (because you activated the venv)

## 02.4 Add a boto3 client factory

Create `src/helpers/aws_client.py`:

```bash
cat > src/helpers/aws_client.py <<'EOF'
import os

import boto3

DEFAULT_REGION = "us-east-1"


def get_client(service: str):
    """
    Create a boto3 client for a service.
    """
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)
    return boto3.client(service, region_name=region)
EOF
```

## 02.5 Add a simple AWS status script

Create `src/helpers/aws_status.py`:

```bash
cat > src/helpers/aws_status.py <<'EOF'
import json

from src.helpers.aws_client import get_client


def check_s3() -> dict:
    s3 = get_client("s3")
    out = {}
    try:
        buckets = s3.list_buckets().get("Buckets", [])
        for b in buckets:
            out[b["Name"]] = {}
    except Exception as e:
        out["_error"] = str(e)
    return out


def check_sqs() -> dict:
    sqs = get_client("sqs")
    out = {}
    try:
        urls = sqs.list_queues().get("QueueUrls", [])
        for url in urls:
            name = url.split("/")[-1]
            out[name] = {"queue_url": url}
    except Exception as e:
        out["_error"] = str(e)
    return out


def check_lambda() -> dict:
    lam = get_client("lambda")
    out = {}
    try:
        fns = lam.list_functions().get("Functions", [])
        for fn in fns:
            out[fn["FunctionName"]] = {"runtime": fn.get("Runtime")}
    except Exception as e:
        out["_error"] = str(e)
    return out


def check_all() -> dict:
    return {"s3": check_s3(), "sqs": check_sqs(), "lambda": check_lambda()}


if __name__ == "__main__":
    print(json.dumps(check_all(), indent=2, default=str))
EOF
```

Run it against real AWS (it should work, but may show empty lists):

```bash
export AWS_PROFILE=fomc-workshop
python src/helpers/aws_status.py
```

Expected:
- JSON output with keys `s3`, `sqs`, `lambda`

## 02.6 Add unit tests for the helper

Create `tests/conftest.py`:

```bash
cat > tests/conftest.py <<'EOF'
import os

import pytest


@pytest.fixture(autouse=True)
def aws_test_env():
    # Ensure tests never accidentally talk to real AWS
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ.pop("AWS_PROFILE", None)
    yield
EOF
```

Create `tests/unit/helpers/test_aws_client.py`:

```bash
cat > tests/unit/helpers/test_aws_client.py <<'EOF'
import os
from unittest.mock import patch

from src.helpers.aws_client import get_client


def test_get_client_defaults():
    with patch.dict(os.environ, {}, clear=True):
        c = get_client("s3")
        assert c.meta.service_model.service_name == "s3"
        assert c.meta.region_name == "us-east-1"


def test_get_client_uses_env_region():
    with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True):
        c = get_client("s3")
        assert c.meta.region_name == "us-west-2"
EOF
```

## 02.7 Run tests

```bash
python -m pytest -q
```

Expected:
- All tests pass

## UAT Sign‑Off (Instructor)

- [ ] Student has a working `.venv` created by `uv`
- [ ] `python src/helpers/aws_status.py` prints JSON
- [ ] `python -m pytest -q` passes
- [ ] Student explains (in words): AWS credential loading (`AWS_PROFILE` vs env vars vs `~/.aws/config`)

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add formatting tooling (ruff/black) and run it
- Expand `aws_status.py` to include object counts in S3 buckets
