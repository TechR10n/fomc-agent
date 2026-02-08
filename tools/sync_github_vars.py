#!/usr/bin/env python3
"""Sync GitHub Actions repository variables from `.env.shared`.

This keeps shared deploy settings in one place:
- LocalStack and AWS tooling load `.env.shared`.
- GitHub Actions repository variables are updated from the same file.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Required/optional non-secret repository variables used by ci-deploy.yml.
REQUIRED_REPO_VARS = (
    "AWS_DEFAULT_REGION",
    "FOMC_BUCKET_PREFIX",
    "FOMC_ANALYTICS_QUEUE_NAME",
    "FOMC_ANALYTICS_DLQ_NAME",
    "FOMC_REMOVAL_POLICY",
    "FOMC_FETCH_INTERVAL_HOURS",
    "BLS_SERIES",
    "DATAUSA_DATASETS",
    "DATAUSA_BASE_URL",
)
OPTIONAL_REPO_VARS = (
    "FOMC_SITE_DOMAIN",
    "FOMC_SITE_CERT_ARN",
    "FOMC_SITE_ALIASES",
)


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"Env file not found: {path}")

    env: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]

        env[key] = value
    return env


def _resolve_repo_values(env: dict[str, str]) -> dict[str, str]:
    values: dict[str, str] = {}

    for key in REQUIRED_REPO_VARS:
        values[key] = env.get(key, "").strip()

    for key in OPTIONAL_REPO_VARS:
        values[key] = env.get(key, "").strip()

    missing = [key for key in REQUIRED_REPO_VARS if not values.get(key)]
    if missing:
        raise SystemExit(
            "Missing required setting(s) in env file: "
            + ", ".join(missing)
            + "\nUpdate `.env.shared` and retry."
        )

    if values["FOMC_REMOVAL_POLICY"] not in {"destroy", "retain"}:
        raise SystemExit("FOMC_REMOVAL_POLICY must be 'destroy' or 'retain'.")

    interval_raw = values["FOMC_FETCH_INTERVAL_HOURS"]
    if not interval_raw.isdigit() or int(interval_raw) <= 0:
        raise SystemExit("FOMC_FETCH_INTERVAL_HOURS must be a positive integer.")

    return values


def _run(cmd: list[str], *, dry_run: bool) -> None:
    printable = " ".join(cmd)
    if dry_run:
        print(f"[dry-run] {printable}")
        return

    subprocess.run(cmd, check=True)


def _gh_set_variable(name: str, value: str, *, repo: str | None, dry_run: bool) -> None:
    cmd = ["gh", "variable", "set", name, "--body", value]
    if repo:
        cmd.extend(["--repo", repo])
    _run(cmd, dry_run=dry_run)
    print(f"Set {name}")


def _gh_delete_variable(name: str, *, repo: str | None, dry_run: bool) -> None:
    cmd = ["gh", "variable", "delete", name]
    if repo:
        cmd.extend(["--repo", repo])

    if dry_run:
        _run(cmd, dry_run=True)
        print(f"Deleted {name} (if it exists)")
        return

    completed = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if completed.returncode == 0:
        print(f"Deleted {name}")
        return

    # `gh variable delete` returns non-zero when the variable does not exist.
    stderr = (completed.stderr or "").lower()
    # Newer `gh` versions report missing variables as an HTTP 404.
    if "not found" in stderr or "http 404" in stderr or "status 404" in stderr:
        print(f"Skipped delete for {name} (not set)")
        return

    raise subprocess.CalledProcessError(
        completed.returncode,
        cmd,
        output=completed.stdout,
        stderr=completed.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(PROJECT_ROOT / ".env.shared"),
        help="Source env file (default: .env.shared)",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="Optional GitHub repo override (OWNER/REPO). Defaults to current repository context.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    parser.add_argument(
        "--keep-empty-optional",
        action="store_true",
        help="Keep existing optional GitHub vars when value is empty in env file.",
    )
    args = parser.parse_args()

    if not shutil.which("gh"):
        raise SystemExit("`gh` CLI not found on PATH. Install GitHub CLI first.")

    env = _load_env_file(Path(args.env_file))
    repo_values = _resolve_repo_values(env)

    print(f"Using source env file: {args.env_file}")
    if args.repo:
        print(f"Target repository: {args.repo}")

    # Always-set variables.
    for key in REQUIRED_REPO_VARS:
        _gh_set_variable(key, repo_values[key], repo=args.repo, dry_run=args.dry_run)

    # Optional variables are either set or deleted so stale values do not linger.
    for key in OPTIONAL_REPO_VARS:
        value = repo_values[key]
        if value:
            _gh_set_variable(key, value, repo=args.repo, dry_run=args.dry_run)
            continue

        if args.keep_empty_optional:
            print(f"Skipped {key} (empty in env file, leaving existing repo variable untouched)")
            continue

        _gh_delete_variable(key, repo=args.repo, dry_run=args.dry_run)

    print("Repository variable sync complete.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        if stderr:
            print(stderr, file=sys.stderr)
        raise SystemExit(exc.returncode)
