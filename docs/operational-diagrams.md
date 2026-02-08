# Operational PlantUML Diagrams

This document captures the runtime and delivery flows requested for this project.

## 1) Hourly AWS runtime (all AWS services in-path)

- File: `docs/aws-hourly-runtime-sequence.puml`
- Purpose: Show what happens each hour across EventBridge, Lambda, S3, SQS, CloudFront, and CloudWatch.
- Scope:
  - Hourly ingestion trigger.
  - Raw-to-enriched processing.
  - Chart/timeline payload publication into the site bucket.
  - CloudFront invalidation and browser read path.
  - DLQ failure path.

## 2) AWS `allowmixing` component + deployment diagram

- File: `docs/aws-allowmixing-component-deployment.puml`
- Purpose: Show deployment boundaries (account/region/stacks) and runtime flows in one diagram.
- Uses: `allowmixing`
- Scope:
  - Stack-level placement of components.
  - Hourly runtime data movement between components.
  - Observability + DLQ signal flow.

## 3) LocalStack synth + local run flow

- File: `docs/localstack-synth-sequence.puml`
- Purpose: Show how local synth and LocalStack execution fit together for developer iteration.
- Scope:
  - `tools/cdk.py synth --all`
  - LocalStack startup + init hook resource creation.
  - `tools/localstack_full_refresh.py` pipeline.
  - Local site serving on localhost.

## 4) GitHub Actions validation + deploy flow

- File: `docs/github-actions-deploy-sequence.puml`
- Purpose: Show CI validation and `main`-branch AWS deployment behavior.
- Scope:
  - PR/main trigger behavior.
  - Validate job (`pytest` + `cdk synth`).
  - OIDC role assumption in deploy job.
  - CDK deploy into CloudFormation stacks.

## Regenerate SVG outputs

```bash
# from repo root
mkdir -p site/diagrams site/puml
cp docs/*.puml docs/interview/*.puml site/puml/
plantuml -tsvg docs/*.puml -o ../site/diagrams
plantuml -tsvg docs/interview/*.puml -o ../../site/diagrams
```
