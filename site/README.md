# Static Site (CloudFront + S3)

This folder is deployed by the CDK `FomcSiteStack` to an S3 bucket named
`${FOMC_BUCKET_PREFIX}-site` and served globally through CloudFront.

## Update the data

Generate a fresh `site/data/timeseries.json` from the latest S3 inputs:

```bash
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1
export FOMC_BUCKET_PREFIX="fomc-yourname-yyyymmdd"

python src/analytics/reports.py
```

## Publish

Deploy (uploads the `site/` folder automatically):

```bash
cdk deploy FomcSiteStack --require-approval never
```

The output includes:
- `SiteUrl` (CloudFront URL)
- `SiteCloudFrontDomain` (target for DNS CNAME)

## Use a GoDaddy-managed domain

Set these env vars before deploying `FomcSiteStack`:

```bash
export FOMC_SITE_DOMAIN="www.example.com"
export FOMC_SITE_CERT_ARN="arn:aws:acm:us-east-1:123456789012:certificate/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

Optional additional aliases (comma-separated):

```bash
export FOMC_SITE_ALIASES="www.example.com,app.example.com"
```

After deploy, create a GoDaddy DNS `CNAME` record for your subdomain that points to
`SiteCloudFrontDomain` (or `GoDaddyCnameTarget` output).
