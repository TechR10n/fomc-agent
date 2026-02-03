"""Pandas analytics reports for FOMC data pipeline.

This module is intended for local/offline analysis and for generating artifacts
that can be published (e.g., a static website JSON file). Lambdas should avoid
heavy native dependencies like pandas.
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import get_bls_bucket, get_bls_key, get_datausa_bucket, get_datausa_key
from src.helpers.aws_client import get_client


def _none_if_nan(value: Any):
    if value is None:
        return None
    try:
        # pandas uses numpy.nan for missing values
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def load_population_from_s3(bucket: str | None = None, key: str | None = None) -> pd.DataFrame:
    """Load DataUSA population JSON from S3 into a DataFrame."""
    if bucket is None:
        bucket = get_datausa_bucket()
    if key is None:
        key = get_datausa_key()

    s3 = get_client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(response["Body"].read())

    df = pd.DataFrame(payload.get("data", []))
    if df.empty:
        return pd.DataFrame(columns=["Year", "Nation", "Population"])

    keep = [c for c in ["Year", "Nation", "Population"] if c in df.columns]
    df = df[keep].copy()
    df["Year"] = pd.to_numeric(df.get("Year"), errors="coerce").astype("Int64")
    df["Population"] = pd.to_numeric(df.get("Population"), errors="coerce").astype("Int64")
    if "Nation" in df.columns:
        df["Nation"] = df["Nation"].astype(str).str.strip()

    return df.dropna(subset=["Year", "Population"]).reset_index(drop=True)


def load_bls_from_s3(bucket: str | None = None, key: str | None = None) -> pd.DataFrame:
    """Load BLS tab-delimited file from S3 into a DataFrame."""
    if bucket is None:
        bucket = get_bls_bucket()
    if key is None:
        key = get_bls_key()

    s3 = get_client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(content), sep="\t", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # Cast numeric columns
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def report_population_stats(pop_df: pd.DataFrame) -> dict:
    """Report 1: Mean and std dev of US population for 2013-2018."""
    if pop_df.empty:
        return {"report": "Population Statistics (2013-2018)", "mean": None, "stddev": None}

    df = pop_df.copy()
    df = df[(df["Year"] >= 2013) & (df["Year"] <= 2018)]
    if df.empty:
        return {"report": "Population Statistics (2013-2018)", "mean": None, "stddev": None}

    population = df["Population"].astype(float)
    return {
        "report": "Population Statistics (2013-2018)",
        "mean": float(population.mean()),
        "stddev": float(population.std(ddof=1)),
    }


def report_best_year_by_series(bls_df: pd.DataFrame) -> list[dict]:
    """Report 2: Best year per series_id (year with max sum of quarterly values)."""
    required = {"series_id", "year", "period", "value"}
    if bls_df.empty or not required.issubset(set(bls_df.columns)):
        return []

    df = bls_df.copy()
    df["series_id"] = df["series_id"].astype(str).str.strip()
    df["period"] = df["period"].astype(str).str.strip()
    df = df[df["period"].str.startswith("Q", na=False)]
    df = df.dropna(subset=["series_id", "year", "value"])
    if df.empty:
        return []

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["year", "value"])
    if df.empty:
        return []

    yearly = df.groupby(["series_id", "year"], as_index=False)["value"].sum()
    best = yearly.sort_values(["series_id", "value"], ascending=[True, False]).drop_duplicates("series_id")
    best = best.sort_values("series_id").reset_index(drop=True)
    best["value"] = best["value"].round(1)

    rows: list[dict] = []
    for r in best.to_dict("records"):
        rows.append({
            "series_id": str(r["series_id"]),
            "year": int(r["year"]),
            "value": float(r["value"]),
        })
    return rows


def report_series_population_join(
    bls_df: pd.DataFrame,
    pop_df: pd.DataFrame,
    *,
    series_id: str = "PRS30006032",
    period: str = "Q01",
) -> list[dict]:
    """Report 3: Join BLS series values with population by year."""
    required_bls = {"series_id", "year", "period", "value"}
    required_pop = {"Year", "Population"}
    if bls_df.empty or pop_df.empty:
        return []
    if not required_bls.issubset(set(bls_df.columns)) or not required_pop.issubset(set(pop_df.columns)):
        return []

    bls = bls_df.copy()
    bls["series_id"] = bls["series_id"].astype(str).str.strip()
    bls["period"] = bls["period"].astype(str).str.strip()
    bls["year"] = pd.to_numeric(bls["year"], errors="coerce").astype("Int64")
    bls["value"] = pd.to_numeric(bls["value"], errors="coerce")
    bls = bls.dropna(subset=["year"])
    series_id = series_id.strip()
    period = period.strip()
    bls = bls[(bls["series_id"] == series_id) & (bls["period"] == period)]
    if bls.empty:
        return []

    pop = pop_df.copy()
    pop["Year"] = pd.to_numeric(pop["Year"], errors="coerce").astype("Int64")
    pop["Population"] = pd.to_numeric(pop["Population"], errors="coerce").astype("Int64")
    pop = pop.dropna(subset=["Year"])

    joined = bls.merge(
        pop[["Year", "Population"]].rename(columns={"Year": "year"}),
        on="year",
        how="left",
    )
    joined = joined.sort_values("year").reset_index(drop=True)

    out = []
    for r in joined[["series_id", "year", "period", "value", "Population"]].to_dict("records"):
        out.append({
            "series_id": str(r["series_id"]),
            "year": int(r["year"]),
            "period": str(r["period"]),
            "value": _none_if_nan(r["value"]),
            "Population": _none_if_nan(r["Population"]),
        })
    return out


def build_timeseries_payload(series_rows: list[dict]) -> dict:
    """Convert report 3 rows into a browser-friendly time series JSON payload."""
    title_bits = []
    if series_rows:
        title_bits.append(series_rows[0].get("series_id"))
        title_bits.append(series_rows[0].get("period"))

    points = []
    for r in series_rows:
        points.append({
            "year": r["year"],
            "population": r.get("Population"),
            "bls_value": r.get("value"),
        })
    title_mid = " ".join([b for b in title_bits if b])
    title = f"BLS ({title_mid}) vs US Population" if title_mid else "BLS vs US Population"
    return {"title": title, "points": points}


def export_site_timeseries(series_rows: list[dict], out_path: str | Path) -> Path:
    """Write the static site `timeseries.json` file and return the resolved path."""
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_timeseries_payload(series_rows)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path.resolve()


def run_all_reports(
    bls_bucket: str | None = None,
    bls_key: str | None = None,
    pop_bucket: str | None = None,
    pop_key: str | None = None,
    *,
    site_json_out: str | Path | None = None,
) -> dict:
    """Run all reports and return results."""
    if pop_bucket is None:
        pop_bucket = get_datausa_bucket()
    if pop_key is None:
        pop_key = get_datausa_key()
    if bls_bucket is None:
        bls_bucket = get_bls_bucket()
    if bls_key is None:
        bls_key = get_bls_key()

    pop_df = load_population_from_s3(pop_bucket, pop_key)
    bls_df = load_bls_from_s3(bls_bucket, bls_key)

    report_1 = report_population_stats(pop_df)
    report_2 = report_best_year_by_series(bls_df)
    report_3 = report_series_population_join(bls_df, pop_df)

    out = {
        "report_1_population_stats": report_1,
        "report_2_best_year_by_series": report_2,
        "report_3_series_population_join": report_3,
    }

    if site_json_out is not None:
        out["exported_site_json"] = str(export_site_timeseries(report_3, site_json_out))

    return out


if __name__ == "__main__":
    results = run_all_reports(site_json_out=Path("site/data/timeseries.json"))
    print(json.dumps(results, indent=2, default=str))
