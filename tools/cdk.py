#!/usr/bin/env python3
"""Small CDK wrapper that loads env files before invoking the CDK CLI.

Examples:
  python tools/cdk.py diff --all
  python tools/cdk.py deploy --all --require-approval never
  python tools/cdk.py deploy FomcSiteStack --require-approval never
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_env_file(path: Path, *, required: bool, override: bool) -> None:
    if not path.exists():
        if required:
            raise SystemExit(f"Env file not found: {path}")
        return

    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and (override or key not in os.environ):
            os.environ[key] = value


def _require_vars(names: list[str]) -> None:
    missing = [name for name in names if not os.environ.get(name, "").strip()]
    if missing:
        raise SystemExit("Missing required environment variable(s): " + ", ".join(missing))


def _resolve_cdk_command() -> list[str]:
    cdk = shutil.which("cdk")
    if cdk:
        return [cdk]

    npx = shutil.which("npx")
    if npx:
        return [npx, "cdk"]

    raise SystemExit("Could not find `cdk` or `npx` on PATH. Install the AWS CDK CLI first.")


def _ensure_deploy_marker(cdk_args: list[str]) -> None:
    if "deploy" not in cdk_args:
        return
    if os.environ.get("FOMC_DEPLOYMENT_ID"):
        return

    deploy_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.environ["FOMC_DEPLOYMENT_ID"] = deploy_id
    print(f"[cdk] Using FOMC_DEPLOYMENT_ID={deploy_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shared-env-file",
        default=str(PROJECT_ROOT / ".env.shared"),
        help="Shared env file to load (default: .env.shared)",
    )
    parser.add_argument(
        "--env-file",
        default=str(PROJECT_ROOT / ".env.local"),
        help="Optional local env override file (default: .env.local)",
    )
    parser.add_argument("cdk_args", nargs=argparse.REMAINDER, help="Arguments passed to the CDK CLI")
    args = parser.parse_args()

    if not args.cdk_args:
        raise SystemExit("Missing CDK arguments. Example: python tools/cdk.py diff --all")

    _load_env_file(Path(args.shared_env_file), required=True, override=False)
    _load_env_file(Path(args.env_file), required=False, override=True)
    _require_vars(
        [
            "AWS_DEFAULT_REGION",
            "FOMC_BUCKET_PREFIX",
            "FOMC_ANALYTICS_QUEUE_NAME",
            "FOMC_ANALYTICS_DLQ_NAME",
            "FOMC_REMOVAL_POLICY",
            "FOMC_FETCH_INTERVAL_HOURS",
        ]
    )
    _ensure_deploy_marker(args.cdk_args)
    # Ensure the Python interpreter running this wrapper is first on PATH.
    # This keeps CDK app execution (`python3 app.py`) inside the same venv.
    python_bin = str(Path(sys.executable).parent)
    current_path = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{python_bin}{os.pathsep}{current_path}" if current_path else python_bin
    cmd = _resolve_cdk_command() + args.cdk_args

    print(f"[cdk] Running: {shlex.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    main()
