# Lab 08 — Capstone: Static Website on S3 with a Time Series Graph

**Timebox:** 60–120 minutes  
**Outcome:** A public static webpage hosted in S3 displays a time series chart built from your exported `timeseries.json`.

This is the “demo moment”: you will open a URL in your browser and see the chart.

## What you’re doing in this lab

1. Create a static website (HTML + JS) that fetches `data/timeseries.json`
2. Create/configure an S3 bucket for static hosting
3. Upload site assets to S3
4. Verify the chart renders

## You start with

- Lab 06 completed (you have `site/data/timeseries.json`)
- AWS CLI configured (Lab 01)

Instructor note:
- LocalStack can simulate S3, but “real” public website hosting is easiest in AWS.

## 08.1 Create the website files

### 08.1.1 `site/index.html`

```bash
cat > site/index.html <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>FOMC Agent Lab — Time Series</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }
      .card { max-width: 960px; margin: 0 auto; padding: 1.5rem; border: 1px solid #ddd; border-radius: 12px; }
      h1 { margin-top: 0; }
      canvas { width: 100%; height: 420px; }
      .muted { color: #666; font-size: 0.9rem; }
      code { background: #f6f6f6; padding: 0.15rem 0.35rem; border-radius: 6px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Time Series: Productivity + Population</h1>
      <p class="muted">
        Data source: <code>data/timeseries.json</code> (generated in Lab 06)
      </p>
      <canvas id="chart"></canvas>
      <p class="muted" id="status">Loading…</p>
    </div>

    <!-- Chart.js from CDN for simplicity -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <script src="app.js"></script>
  </body>
</html>
EOF
```

### 08.1.2 `site/app.js`

```bash
cat > site/app.js <<'EOF'
async function loadTimeseries() {
  const status = document.getElementById("status");
  try {
    const resp = await fetch("data/timeseries.json", { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const payload = await resp.json();

    const points = payload.points || [];
    const labels = points.map(p => p.year);
    const population = points.map(p => p.population);
    const blsValue = points.map(p => p.bls_value);

    const ctx = document.getElementById("chart");
    // Two-axis chart because units are very different
    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "US Population",
            data: population,
            yAxisID: "yPop",
            borderColor: "rgb(59, 130, 246)",
            backgroundColor: "rgba(59, 130, 246, 0.15)",
            spanGaps: true,
          },
          {
            label: "PRS30006032 (Q01) value",
            data: blsValue,
            yAxisID: "yBls",
            borderColor: "rgb(16, 185, 129)",
            backgroundColor: "rgba(16, 185, 129, 0.15)",
            spanGaps: true,
          },
        ],
      },
      options: {
        responsive: true,
        interaction: { mode: "index", intersect: false },
        plugins: {
          title: { display: true, text: payload.title || "Time Series" },
        },
        scales: {
          yPop: {
            type: "linear",
            position: "left",
            ticks: { callback: v => v.toLocaleString() },
            title: { display: true, text: "Population" },
          },
          yBls: {
            type: "linear",
            position: "right",
            grid: { drawOnChartArea: false },
            title: { display: true, text: "BLS value" },
          },
        },
      },
    });

    status.textContent = `Loaded ${points.length} points.`;
  } catch (e) {
    status.textContent = `Failed to load: ${e}`;
    throw e;
  }
}

loadTimeseries();
EOF
```

## 08.2 Confirm you have the data file

```bash
ls -la site/data/timeseries.json
cat site/data/timeseries.json | python -m json.tool | head -40
```

Expected:
- File exists and JSON parses

If missing:
- Re-run Lab 06 to regenerate it.

## 08.3 Create an S3 bucket for the website

Pick a globally unique bucket name:

```bash
export AWS_PROFILE=fomc-workshop
export AWS_DEFAULT_REGION=us-east-1
unset AWS_ENDPOINT_URL

export SITE_BUCKET="fomc-<yourname>-site-<yyyymmdd>"
```

Create it (us-east-1):

```bash
aws s3api create-bucket --bucket "$SITE_BUCKET" --region us-east-1
```

## 08.4 Enable static website hosting

```bash
aws s3 website "s3://$SITE_BUCKET/" --index-document index.html --error-document index.html
```

## 08.5 Make the bucket publicly readable (for the website)

Warning:
- This makes the website bucket public. Do not store secrets here.

1) Disable “block public access” (bucket-level):

```bash
aws s3api put-public-access-block \
  --bucket "$SITE_BUCKET" \
  --public-access-block-configuration BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false
```

2) Attach a bucket policy allowing `GetObject`:

```bash
cat > /tmp/site-bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadGetObject",
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::$SITE_BUCKET/*"]
    }
  ]
}
EOF

aws s3api put-bucket-policy --bucket "$SITE_BUCKET" --policy file:///tmp/site-bucket-policy.json
```

## 08.6 Upload the site

```bash
aws s3 sync site/ "s3://$SITE_BUCKET/" --delete
```

Expected:
- Upload succeeds

## 08.7 Open the website URL

S3 website endpoint format depends on region. For `us-east-1`:

```text
http://<bucket-name>.s3-website-us-east-1.amazonaws.com
```

So your URL is:

```text
http://$SITE_BUCKET.s3-website-us-east-1.amazonaws.com
```

Open it in a browser and verify:
- The page loads
- A chart appears
- Status text says “Loaded N points”

## 08.8 Update loop (prove you can change the chart)

1) Re-run analytics to regenerate `site/data/timeseries.json`:

```bash
python src/analytics/reports.py >/tmp/analytics-out.json
```

2) Re-sync the site:

```bash
aws s3 sync site/ "s3://$SITE_BUCKET/" --delete
```

3) Refresh the browser page and confirm it still loads.

## UAT Sign‑Off (Instructor)

- [ ] Student can explain where the website is hosted (S3 website hosting)
- [ ] Website loads publicly in a browser
- [ ] Chart renders from `data/timeseries.json`
- [ ] Student can update the JSON and see the site update after re-upload
- [ ] Student can describe the cleanup steps (delete objects → delete bucket)

Instructor initials: ________  Date/time: ________

## Cleanup (do this!)

Remove all objects then delete the bucket:

```bash
aws s3 rm "s3://$SITE_BUCKET/" --recursive
aws s3api delete-bucket --bucket "$SITE_BUCKET" --region us-east-1
```

## If you finish early (optional extensions)

- Add CloudFront for HTTPS (S3 website endpoints are HTTP-only)
- Add a second chart (population-only) and a dropdown to switch views
- Add a `last_updated` timestamp to the JSON and display it on the page

