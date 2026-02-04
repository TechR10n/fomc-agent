# ADR-004: AWS CDK (Python) as Infrastructure-as-Code Tool

| Field   | Value                |
|---------|----------------------|
| Status  | Accepted             |
| Date    | 2025-01-10           |
| Authors | FOMC Agent Team      |

## Context

This project deploys a serverless data pipeline to AWS (S3, Lambda, SQS, EventBridge). All infrastructure must be defined as code so it is repeatable, reviewable, and disposable. Three options were evaluated:

| Criteria                        | CloudFormation (YAML) | Terraform (HCL) | AWS CDK (Python) |
|---------------------------------|-----------------------|------------------|-------------------|
| Language                        | YAML/JSON             | HCL              | Python            |
| Matches application language    | No                    | No               | Yes               |
| Abstractions / sensible defaults| None — every property | Moderate         | High (L2 constructs) |
| State management                | Managed by AWS        | Remote state file| Managed by AWS (via CloudFormation) |
| Multi-cloud support             | No                    | Yes              | No (AWS only)     |
| Extra toolchain required        | None                  | Terraform CLI    | Node.js + CDK CLI |
| Learning curve for Python devs  | High (YAML verbosity) | Moderate (new DSL)| Low              |

## Decision

Use AWS CDK with Python bindings.

### Reasons

1. **Single-language project.** Application code, tests, infrastructure, and configuration are all Python. Contributors don't need to context-switch to YAML or HCL. IDE features like autocomplete, type checking, and refactoring work across the entire codebase.

2. **L2 constructs reduce boilerplate.** CDK's high-level constructs handle IAM permissions, event wiring, and resource naming with sensible defaults. For example, `bucket.grant_read_write(lambda_fn)` generates the minimal IAM policy automatically — the equivalent CloudFormation is 20+ lines of YAML.

3. **State is managed by AWS.** CDK synthesizes to CloudFormation, so stack state lives in the AWS account with no external state file to manage, lock, or lose. This avoids the Terraform state coordination problem entirely.

4. **This project is AWS-only.** Multi-cloud portability (Terraform's main advantage) is not a requirement. Every service used — S3, Lambda, SQS, EventBridge — is AWS-native with no equivalent abstraction needed.

5. **Workshop accessibility.** Students already know Python. Teaching CDK constructs is faster than teaching CloudFormation YAML syntax or HCL, which lets the workshop focus on pipeline concepts rather than IaC syntax.

## Alternatives Considered

### CloudFormation (YAML/JSON)

Rejected. The verbosity is significant — a single Lambda + EventBridge schedule requires ~80 lines of YAML versus ~15 lines of CDK Python. Error messages reference logical IDs rather than code locations, making debugging harder for newcomers.

### Terraform

Rejected. HCL is a separate language to learn, and Terraform requires managing a remote state backend (S3 + DynamoDB for locking). For a single-account, single-region workshop project, this adds complexity without benefit. Terraform would be the stronger choice if the project needed multi-cloud or multi-provider resources.

### Pulumi

Not evaluated in depth. Similar single-language benefits as CDK, but smaller community, separate state management (Pulumi Cloud or self-hosted), and less AWS-specific documentation. No compelling advantage over CDK for this use case.

## Consequences

### Positive

- Infrastructure code is readable and modifiable by anyone who knows Python.
- `cdk diff` shows planned changes before deploy, similar to `terraform plan`.
- `cdk destroy --all` tears down everything cleanly — important for a workshop where resources must not linger.
- CDK's `grant_*` methods enforce least-privilege IAM without manual policy authoring.

### Negative

- Node.js is required as a runtime dependency (CDK CLI is a Node application), even though no JavaScript is written.
- CDK abstractions can obscure the underlying CloudFormation. Students who later work with raw CloudFormation may find the mapping unfamiliar.
- CDK major version upgrades occasionally introduce breaking changes to construct APIs.

### Accepted Trade-offs

- The Node.js dependency is a one-time install (`brew install node`) and does not affect the Python application.
- For students who need CloudFormation visibility, `cdk synth` outputs the full template for inspection.
