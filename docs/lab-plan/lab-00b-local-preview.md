# Lab 00b — Local Site Preview

**Timebox:** 10–15 minutes
**Outcome:** You can preview the static site locally before deploying to S3.

## What you're doing in this lab

- Serve the static site locally with Python's built-in HTTP server
- Verify all pages render: landing page, architecture diagrams, pipeline status, charts, and BLS guide
- Confirm Chart.js charts load with mock data

## You start with

- Lab 00 completed (project bootstrapped, tools installed)

## 00b.1 Serve the site locally

No extra dependencies needed — Python's built-in HTTP server works:

```bash
cd site
python -m http.server 5050
```

The server starts on `http://localhost:5050`.

## 00b.2 Browse the site

Open `http://localhost:5050` in your browser. You should see:

| Page | URL | What to check |
|------|-----|---------------|
| Home | `/` | Project overview, AWS service descriptions, navigation cards |
| Architecture | `/architecture.html` | Three SVG diagrams (pipeline, change detection, CDK stacks) |
| Pipeline | `/pipeline.html` | Pipeline status with series breakdown, progress bars |
| Charts | `/charts.html` | Four dual-axis charts with mock data |
| BLS Guide | `/bls-guide.html` | LABSTAT primer, series table, expansion ideas |

## 00b.3 Verify charts

On the Charts page, confirm:
- Four charts render (Productivity + Population, Great Decoupling, Productivity vs Costs, Manufacturing vs Nonfarm)
- Each chart shows data points and has dual y-axes
- Status text below each chart says "Loaded N points"

## How the site updates

When deployed to AWS, the static site files live in S3. The nightly EventBridge-triggered pipeline:
1. Fetches data from BLS and DataUSA
2. Runs analytics and generates updated JSON files
3. Writes JSON to the site S3 bucket

The HTML/JS/CSS are deployed once (or via `aws s3 sync`). The JSON data files update automatically each night. No rebuild step needed — the browser fetches fresh JSON on each page load.

## UAT Sign-Off (Instructor)

- [ ] `python -m http.server 5050` serves the site from the `site/` directory
- [ ] Home page loads with navigation and AWS service descriptions
- [ ] Architecture page shows SVG diagrams
- [ ] Charts page renders all four charts with mock data
- [ ] Pipeline page loads and displays status from `pipeline_status.json`

Instructor initials: ________  Date/time: ________
