# Static Site (CloudFront + S3)

This folder is deployed by the CDK `FomcSiteStack` to an S3 bucket named
`${FOMC_BUCKET_PREFIX}-site` and served globally through CloudFront.

## Update the data

Generate a fresh `site/data/timeseries.json` from the latest S3 inputs:

```bash
source .env.shared
source .env.local
python src/analytics/reports.py
```

## Publish

Deploy (uploads the `site/` folder automatically):

```bash
python tools/cdk.py deploy FomcSiteStack --require-approval never
```

The output includes:
- `SiteUrl` (CloudFront URL)
- `SiteCloudFrontDomain` (CloudFront host behind `SiteUrl`)
