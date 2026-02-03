# Lab 06 — Analytics + Export a Time Series JSON (For the Website)

**Timebox:** 60–120 minutes  
**Outcome:** You can compute the required analytics and export a browser-friendly time series JSON file that your static website can plot.

## What you’re doing in this lab

1. Load BLS data from S3 (tab-delimited file)
2. Load DataUSA population JSON from S3
3. Compute:
   - Population mean/stddev for 2013–2018
   - “Best year” per series_id (max sum of quarterly values)
   - A joined dataset for `PRS30006032` + `Q01` with population
4. Export `site/data/timeseries.json` for the capstone website

## You start with

- Lab 04 and Lab 05 completed (data is in S3)

## 06.1 Create a place for the website assets (we’ll use it later)

```bash
mkdir -p site/data
```

## 06.2 Create an analytics script (pandas-based)

Create `src/analytics/reports.py`:

```bash
mkdir -p src/analytics
touch src/analytics/__init__.py
cat > src/analytics/reports.py <<'EOF'
import io
import json
import os

import pandas as pd

from src.helpers.aws_client import get_client


def _load_bls_dataframe(bucket: str, key: str) -> pd.DataFrame:
    s3 = get_client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8")

    # BLS time-series files are tab-delimited
    df = pd.read_csv(io.StringIO(raw), sep="\t", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # numeric conversions (best-effort)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _load_population_dataframe(bucket: str, key: str) -> pd.DataFrame:
    s3 = get_client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(obj["Body"].read())
    df = pd.DataFrame(payload.get("data", []))
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Population"] = pd.to_numeric(df["Population"], errors="coerce")
    return df


def report_population_stats(pop_df: pd.DataFrame) -> dict:
    subset = pop_df[(pop_df["Year"] >= 2013) & (pop_df["Year"] <= 2018)]
    return {
        "report": "Population Statistics (2013-2018)",
        "mean": float(subset["Population"].mean()) if len(subset) else None,
        "stddev": float(subset["Population"].std(ddof=1)) if len(subset) else None,
        "count": int(len(subset)),
    }


def report_best_year(bls_df: pd.DataFrame) -> pd.DataFrame:
    q = bls_df[bls_df["period"].str.startswith("Q", na=False)].copy()
    q = q.dropna(subset=["series_id", "year", "value"])
    sums = q.groupby(["series_id", "year"], as_index=False)["value"].sum()
    # pick row with max sum per series_id
    idx = sums.groupby("series_id")["value"].idxmax()
    best = sums.loc[idx].sort_values("series_id").reset_index(drop=True)
    best.rename(columns={"value": "total_value"}, inplace=True)
    return best


def report_series_population(bls_df: pd.DataFrame, pop_df: pd.DataFrame) -> pd.DataFrame:
    series = bls_df[
        (bls_df["series_id"] == "PRS30006032")
        & (bls_df["period"] == "Q01")
    ][["series_id", "year", "period", "value"]].copy()
    joined = series.merge(pop_df[["Year", "Population"]], left_on="year", right_on="Year", how="left")
    joined.drop(columns=["Year"], inplace=True)
    return joined.sort_values("year").reset_index(drop=True)


def export_timeseries_json(joined_df: pd.DataFrame, output_path: str) -> None:
    """
    Export an array of points the website can plot.

    Output format:
    {
      "title": "...",
      "points": [
        {"year": 2017, "population": 321004407, "bls_value": 1.5},
        ...
      ]
    }
    """
    points = []
    for _, row in joined_df.iterrows():
        year = int(row["year"]) if pd.notna(row["year"]) else None
        population = int(row["Population"]) if pd.notna(row.get("Population")) else None
        bls_value = float(row["value"]) if pd.notna(row.get("value")) else None
        points.append({"year": year, "population": population, "bls_value": bls_value})

    payload = {
        "title": "PRS30006032 (Q01) and US Population by Year",
        "points": points,
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run_all(
    bls_bucket: str,
    datausa_bucket: str,
    bls_key: str = "pr/pr.data.0.Current",
    population_key: str = "population.json",
    site_json_out: str = "site/data/timeseries.json",
) -> dict:
    bls_df = _load_bls_dataframe(bls_bucket, bls_key)
    pop_df = _load_population_dataframe(datausa_bucket, population_key)

    r1 = report_population_stats(pop_df)
    r2 = report_best_year(bls_df)
    r3 = report_series_population(bls_df, pop_df)

    export_timeseries_json(r3, site_json_out)

    return {
        "report_1": r1,
        "report_2_preview": r2.head(10).to_dict(orient="records"),
        "report_3_preview": r3.head(10).to_dict(orient="records"),
        "exported_site_json": site_json_out,
    }


if __name__ == "__main__":
    bls_bucket = os.environ.get("BLS_BUCKET")
    datausa_bucket = os.environ.get("DATAUSA_BUCKET")
    if not bls_bucket or not datausa_bucket:
        raise SystemExit("Set BLS_BUCKET and DATAUSA_BUCKET env vars first (from Labs 04/05).")

    out = run_all(bls_bucket, datausa_bucket)
    print(json.dumps(out, indent=2, default=str))
EOF
```

## 06.3 Run analytics and export the website JSON

```bash
python src/analytics/reports.py | head -60
```

Expected:
- Output includes `exported_site_json: "site/data/timeseries.json"`

Inspect the file:

```bash
cat site/data/timeseries.json | python -m json.tool | head -80
```

Expected:
- A JSON object with `title` and `points`
- Each point includes `year`, `population`, `bls_value` (some populations may be null if not available)

## 06.4 (Optional) Upload the exported JSON back to S3

You can store the site JSON in your data bucket or your website bucket (later).

For now, upload to the DataUSA raw bucket under `exports/`:

```bash
aws s3 cp site/data/timeseries.json "s3://$DATAUSA_BUCKET/exports/timeseries.json" || true
```

## UAT Sign‑Off (Instructor)

- [ ] Student created `site/data/timeseries.json`
- [ ] JSON is valid and contains a non-empty `points` array
- [ ] Student can explain what is being plotted (what is `bls_value`? what is `population`?)
- [ ] Student can regenerate the JSON on demand by re-running the analytics script

Instructor initials: ________  Date/time: ________

## If you finish early (optional extensions)

- Add a second exported JSON for “population-only” and plot it on a second chart
- Export CSV as well as JSON for easy debugging
- Add a data-quality check: ensure `year` is increasing and unique
