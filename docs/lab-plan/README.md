# Lab Plans (From Scratch)

These lab plans assume you start from an **empty folder** and build everything step-by-step. Each lab ends with a **UAT sign‑off checklist**. Treat that checklist as the “exit ticket”: if any box is unchecked, you don’t move on.

## What you will build (end state)

- A data pipeline that:
  - Fetches BLS time-series files and stores them in S3
  - Fetches DataUSA population data and stores it in S3
  - Computes a small set of reports and exports a browser‑friendly time series JSON file
- A static website hosted on S3 that loads that JSON and renders a time series graph
- (Optional, advanced) Infrastructure-as-code (AWS CDK) that automates the pipeline

## Lab sequence (8-hour day)

1. `docs/lab-plan/lab-00-workshop-setup.md`
2. `docs/lab-plan/lab-01-aws-basics-and-cli.md`
3. `docs/lab-plan/lab-02-python-project-bootstrap.md`
4. `docs/lab-plan/lab-03-localstack-integration.md` (optional but recommended)
5. `docs/lab-plan/lab-04-bls-ingestion.md`
6. `docs/lab-plan/lab-05-datausa-ingestion.md`
7. `docs/lab-plan/lab-06-analytics-and-export.md`
8. `docs/lab-plan/lab-07-iac-cdk-pipeline.md` (optional/advanced)
9. `docs/lab-plan/lab-08-capstone-s3-static-site.md` (capstone)

## Ground rules (for learning + safety)

- Use a dedicated AWS profile for this workshop (don’t overwrite your default).
- Never commit secrets to git. Avoid putting access keys into project files.
- Prefer LocalStack while you’re learning to reduce AWS costs and risk.
- If you deploy to AWS, clean up (delete stacks/buckets) when you’re done.

