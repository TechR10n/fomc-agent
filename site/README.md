# Static Site (S3)

This folder is deployed by the CDK `FomcSiteStack` to an S3 bucket named `${FOMC_BUCKET_PREFIX}-site`.

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

The output includes `SiteUrl` (the S3 website endpoint).
