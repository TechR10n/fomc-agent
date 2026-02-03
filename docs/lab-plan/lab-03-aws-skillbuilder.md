# Lab 03 — AWS Skill Builder Alignment (AWS-Only)

**Timebox:** 30–60 minutes  
**Outcome:** You have an AWS profile configured and verified, a naming convention for globally-unique resources, and a Skill Builder checklist aligned to this project’s AWS services.

## What you’re doing in this lab

1. Confirm AWS CLI access (profile + region)
2. Establish workshop-wide environment variables (profile, region, bucket prefix)
3. Use AWS Skill Builder to cover the exact services this project uses

## You start with

- Lab 01 completed (AWS basics + CLI)
- Lab 02 completed (Python project + tests)

## 03.1 Verify AWS identity

Pick (or reuse) your workshop profile name:

```bash
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1
```

Confirm credentials are working:

```bash
aws sts get-caller-identity
```

Expected:
- JSON output with your AWS account and principal ARN

## 03.2 Choose a unique prefix for all buckets (important)

S3 bucket names are **globally unique**. Set a unique prefix once and reuse it:

```bash
export FOMC_BUCKET_PREFIX="fomc-<yourname>-<yyyymmdd>"
```

Optional (workshop convenience; allows `cdk destroy` to delete buckets/objects):

```bash
export FOMC_REMOVAL_POLICY=destroy  # alternatives: retain
```

## 03.3 AWS Skill Builder checklist (search terms)

Use AWS Skill Builder and search for modules/labs matching these topics. Aim to complete as many as you can before (or alongside) Labs 04–08.

- **AWS fundamentals:** IAM, regions, shared responsibility model
- **AWS CLI & credentials:** profiles, credential provider chain, AWS SSO (if applicable)
- **Amazon S3:** buckets/objects, encryption, bucket policies, access control, event notifications
- **AWS Lambda:** Python Lambda basics, permissions (execution role), environment variables, CloudWatch Logs
- **Amazon SQS:** queues, visibility timeout, DLQs, batching
- **Amazon EventBridge:** scheduled rules (cron), targets, retry behavior
- **AWS CDK / CloudFormation:** stacks, synth/deploy, diffs, environments (account/region)
- **Observability:** CloudWatch Logs basics, metrics, alarms (high level)
- **Cost & safety:** budgets, cleanup patterns, least privilege

## UAT Sign‑Off (Instructor)

- [ ] Student can run `aws sts get-caller-identity` successfully
- [ ] Student can explain `AWS_PROFILE` and `AWS_DEFAULT_REGION`
- [ ] Student set `FOMC_BUCKET_PREFIX` and can explain why it must be globally unique
- [ ] Student identifies at least one Skill Builder module per major service used (S3, Lambda, SQS, EventBridge, CDK)

Instructor initials: ________  Date/time: ________
