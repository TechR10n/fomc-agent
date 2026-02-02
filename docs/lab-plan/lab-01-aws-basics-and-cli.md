# Lab 01 — AWS Basics + CLI Setup

**Timebox:** 45–75 minutes  
**Outcome:** You can authenticate to AWS from your terminal using a dedicated profile, and you understand the “four fundamentals”: identity, region, permissions, and cleanup.

## What you’re doing in this lab

1. Create (or reuse) a safe AWS identity for the workshop
2. Configure the AWS CLI using a dedicated profile
3. Prove it works by creating + deleting a tiny S3 bucket

## You start with

- AWS account access (console login)
- AWS CLI installed (`aws --version`)

## 01.1 Pick a region for the whole day

Pick one region and stick to it to avoid confusion. Recommended:
- `us-east-1`

Write this in `notes/workshop-log.md`:
- Region: `us-east-1`

## 01.2 Create an IAM user (workshop identity)

In the AWS Console:

1. Open **IAM → Users → Create user**
2. Name: `fomc-workshop-user` (or similar)
3. Enable: **Programmatic access** (access keys)
4. Permissions:
   - For a one‑day workshop, simplest is **AdministratorAccess**
   - Instructor note: This is not least‑privilege; the goal is learning speed. Delete the user after.
5. Finish and create an access key

Important:
- Copy the **Access key ID** and **Secret access key**
- Do **not** paste them into your repo files

## 01.3 Configure the AWS CLI profile (do not use default)

Use a dedicated profile name:
- `fomc-workshop`

```bash
aws configure --profile fomc-workshop
```

Enter:
- AWS Access Key ID: (from IAM)
- AWS Secret Access Key: (from IAM)
- Default region name: `us-east-1`
- Default output format: `json`

## 01.4 Prove your credentials work

```bash
aws sts get-caller-identity --profile fomc-workshop
```

Expected:
- JSON with `Account`, `Arn`, `UserId`

If it fails:
- You likely copied keys wrong or selected the wrong permission option.
- Recreate the access key and re-run `aws configure --profile ...`

## 01.5 Export AWS_PROFILE for the rest of the day (optional but recommended)

This makes every `aws` command automatically use your workshop profile:

```bash
export AWS_PROFILE=fomc-workshop
```

Confirm:

```bash
aws sts get-caller-identity
```

Expected:
- Same output as 01.4 but without the `--profile` flag.

## 01.6 S3 “hello world”: create a bucket, upload a file, delete it

S3 bucket names must be globally unique.

Pick a unique bucket name. Example pattern:
- `fomc-<yourname>-<yyyymmdd>-scratch`

Example:
- `fomc-ryan-20260202-scratch`

Create it (us-east-1 special case):

```bash
export SCRATCH_BUCKET="fomc-<yourname>-<yyyymmdd>-scratch"
aws s3api create-bucket --bucket "$SCRATCH_BUCKET" --region us-east-1
```

Verify:

```bash
aws s3 ls | grep "$SCRATCH_BUCKET"
```

Upload a file:

```bash
echo "hello s3" > /tmp/hello.txt
aws s3 cp /tmp/hello.txt "s3://$SCRATCH_BUCKET/hello.txt"
aws s3 ls "s3://$SCRATCH_BUCKET/"
```

Download it:

```bash
aws s3 cp "s3://$SCRATCH_BUCKET/hello.txt" /tmp/hello-downloaded.txt
cat /tmp/hello-downloaded.txt
```

Delete everything + the bucket:

```bash
aws s3 rm "s3://$SCRATCH_BUCKET/hello.txt"
aws s3api delete-bucket --bucket "$SCRATCH_BUCKET" --region us-east-1
```

Expected:
- No leftover resources

## 01.7 Cost + cleanup habits (write this down)

In your lab notebook, write:

- “If I create it, I must be able to delete it.”
- “S3 buckets must be emptied before deletion.”
- “Lambda + logs can persist costs; delete stacks when done.”

## UAT Sign‑Off (Instructor)

- [ ] Student can run `aws sts get-caller-identity` successfully
- [ ] Student can create and delete an S3 bucket using CLI
- [ ] Student understands: profile vs region vs permissions (explain in 2–3 sentences)
- [ ] Student wrote cleanup rules in the lab notebook

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Create a second AWS profile for “read-only” and compare behavior
- Set up an AWS Budget alarm (Console → Billing → Budgets)

