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

from src.config import bls_data_key, get_bls_bucket, get_bls_key, get_datausa_bucket, get_datausa_key
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


def load_datausa_jsonrecords_from_s3(
    *,
    bucket: str,
    key: str,
) -> pd.DataFrame:
    """Load a DataUSA JSONRecords payload (tesseract/data.jsonrecords) into a DataFrame."""
    s3 = get_client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(response["Body"].read())
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df.columns = df.columns.astype(str).str.strip()
    return df

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
    df.columns = df.columns.str.strip()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # Cast numeric columns
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def _annualize_bls_monthly_series(bls_df: pd.DataFrame, *, series_id: str) -> pd.DataFrame:
    required = {"series_id", "year", "period", "value"}
    if bls_df.empty or not required.issubset(set(bls_df.columns)):
        return pd.DataFrame(columns=["year", "value"])

    df = bls_df.copy()
    df["series_id"] = df["series_id"].astype(str).str.strip()
    df = df[df["series_id"] == series_id.strip()]
    if df.empty:
        return pd.DataFrame(columns=["year", "value"])

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["period"] = df["period"].astype(str).str.strip()
    df = df.dropna(subset=["year", "value"])
    if df.empty:
        return pd.DataFrame(columns=["year", "value"])

    # Prefer annual average rows when present.
    m13 = df[df["period"] == "M13"].copy()
    if not m13.empty:
        out = m13.groupby("year", as_index=False)["value"].mean()
        out["year"] = out["year"].astype(int)
        return out.sort_values("year").reset_index(drop=True)

    # Otherwise: average M01..M12.
    df["month"] = pd.to_numeric(df["period"].str[1:], errors="coerce")
    df = df[df["month"].between(1, 12, inclusive="both")]
    if df.empty:
        return pd.DataFrame(columns=["year", "value"])

    out = df.groupby("year", as_index=False)["value"].mean()
    out["year"] = out["year"].astype(int)
    return out.sort_values("year").reset_index(drop=True)


def _infer_numeric_measure_column(df: pd.DataFrame, *, exclude: set[str]) -> str | None:
    if df.empty:
        return None
    candidates = [c for c in df.columns if c not in exclude]
    for c in candidates:
        try:
            s = pd.to_numeric(df[c], errors="coerce")
        except Exception:
            continue
        if s.notna().any():
            return c
    return None


def build_unemployment_vs_commute_time(
    *,
    bls_bucket: str,
    datausa_bucket: str,
    ln_key: str | None = None,
    commute_key: str = "commute_time.json",
    unemployment_series_id: str = "LNS14000000",
) -> dict[str, Any]:
    """Curated dataset: annual unemployment rate vs mean commute time (nation)."""
    if ln_key is None:
        ln_key = bls_data_key("ln", "ln.data.0.Current")

    try:
        ln_df = load_bls_from_s3(bucket=bls_bucket, key=ln_key)
        unemp = _annualize_bls_monthly_series(ln_df, series_id=unemployment_series_id).rename(
            columns={"value": "unemployment_rate"}
        )
    except Exception as exc:
        return {
            "title": "Unemployment vs Commute Time",
            "description": f"Missing BLS LN data ({unemployment_series_id}) — {exc}",
            "y_left": {"label": "Unemployment Rate (%)", "key": "unemployment_rate", "color": "#ef4444"},
            "y_right": {"label": "Mean Commute (minutes)", "key": "mean_commute_minutes", "color": "#3b82f6"},
            "points": [],
        }

    try:
        commute_df = load_datausa_jsonrecords_from_s3(bucket=datausa_bucket, key=commute_key)
        if commute_df.empty:
            raise ValueError("empty commute dataset")
        commute_df["Year"] = pd.to_numeric(commute_df.get("Year"), errors="coerce").astype("Int64")
        measure_col = _infer_numeric_measure_column(commute_df, exclude={"Year", "Nation", "ID Nation", "Slug Nation"})
        if measure_col is None:
            raise ValueError("could not infer commute measure column")
        commute_df["mean_commute_minutes"] = pd.to_numeric(commute_df[measure_col], errors="coerce")
        commute = (
            commute_df.dropna(subset=["Year", "mean_commute_minutes"])
            .groupby("Year", as_index=False)["mean_commute_minutes"]
            .mean()
            .rename(columns={"Year": "year"})
        )
        commute["year"] = commute["year"].astype(int)
    except Exception as exc:
        return {
            "title": "Unemployment vs Commute Time",
            "description": f"Missing DataUSA commute-time data — {exc}",
            "y_left": {"label": "Unemployment Rate (%)", "key": "unemployment_rate", "color": "#ef4444"},
            "y_right": {"label": "Mean Commute (minutes)", "key": "mean_commute_minutes", "color": "#3b82f6"},
            "points": [],
        }

    joined = unemp.merge(commute, on="year", how="inner").sort_values("year").reset_index(drop=True)
    points = []
    for r in joined.to_dict("records"):
        points.append({
            "year": int(r["year"]),
            "unemployment_rate": _none_if_nan(r.get("unemployment_rate")),
            "mean_commute_minutes": _none_if_nan(r.get("mean_commute_minutes")),
        })

    return {
        "title": "Unemployment vs Commute Time (Nation)",
        "description": (
            "Annual unemployment rate (BLS CPS/LN) plotted against mean commuting time (ACS via DataUSA). "
            "Useful for spotting mismatch vs cyclical slack."
        ),
        "y_left": {"label": "Unemployment Rate (%)", "key": "unemployment_rate", "color": "#ef4444"},
        "y_right": {"label": "Mean Commute (minutes)", "key": "mean_commute_minutes", "color": "#3b82f6"},
        "points": points,
    }


def build_participation_vs_noncitizen_share(
    *,
    bls_bucket: str,
    datausa_bucket: str,
    ln_key: str | None = None,
    citizenship_key: str = "citizenship.json",
    participation_series_id: str = "LNS11300000",
) -> dict[str, Any]:
    """Curated dataset: labor force participation vs non-citizen share (nation)."""
    if ln_key is None:
        ln_key = bls_data_key("ln", "ln.data.0.Current")

    try:
        ln_df = load_bls_from_s3(bucket=bls_bucket, key=ln_key)
        part = _annualize_bls_monthly_series(ln_df, series_id=participation_series_id).rename(
            columns={"value": "participation_rate"}
        )
    except Exception as exc:
        return {
            "title": "Participation vs Non-Citizen Share",
            "description": f"Missing BLS LN data ({participation_series_id}) — {exc}",
            "y_left": {"label": "Labor Force Participation (%)", "key": "participation_rate", "color": "#22c55e"},
            "y_right": {"label": "Non-Citizen Share (%)", "key": "noncitizen_share", "color": "#8b5cf6"},
            "points": [],
        }

    try:
        cit_df = load_datausa_jsonrecords_from_s3(bucket=datausa_bucket, key=citizenship_key)
        if cit_df.empty:
            raise ValueError("empty citizenship dataset")

        cit_df.columns = cit_df.columns.astype(str).str.strip()
        year_col = "Year" if "Year" in cit_df.columns else None
        if year_col is None:
            raise ValueError("missing Year column")

        status_col = next((c for c in cit_df.columns if "citizenship" in c.lower()), None)
        if status_col is None:
            raise ValueError("missing citizenship status column")

        pop_col = "Population" if "Population" in cit_df.columns else _infer_numeric_measure_column(
            cit_df,
            exclude={year_col, status_col, "Nation", "ID Nation", "Slug Nation"},
        )
        if pop_col is None:
            raise ValueError("missing population measure column")

        cit_df[year_col] = pd.to_numeric(cit_df[year_col], errors="coerce").astype("Int64")
        cit_df[pop_col] = pd.to_numeric(cit_df[pop_col], errors="coerce")

        total = cit_df.dropna(subset=[year_col, pop_col]).groupby(year_col, as_index=False)[pop_col].sum()
        total = total.rename(columns={year_col: "year", pop_col: "total_population"})

        mask = cit_df[status_col].astype(str).str.contains("not", case=False, na=False)
        noncit = (
            cit_df[mask]
            .dropna(subset=[year_col, pop_col])
            .groupby(year_col, as_index=False)[pop_col]
            .sum()
            .rename(columns={year_col: "year", pop_col: "noncitizen_population"})
        )

        merged = total.merge(noncit, on="year", how="left")
        merged["noncitizen_population"] = merged["noncitizen_population"].fillna(0)
        merged["noncitizen_share"] = (merged["noncitizen_population"] / merged["total_population"]) * 100.0
        merged = merged.dropna(subset=["year"]).copy()
        merged["year"] = merged["year"].astype(int)
        noncit_share = merged[["year", "noncitizen_share"]].copy()
    except Exception as exc:
        return {
            "title": "Participation vs Non-Citizen Share",
            "description": f"Missing DataUSA citizenship data — {exc}",
            "y_left": {"label": "Labor Force Participation (%)", "key": "participation_rate", "color": "#22c55e"},
            "y_right": {"label": "Non-Citizen Share (%)", "key": "noncitizen_share", "color": "#8b5cf6"},
            "points": [],
        }

    joined = part.merge(noncit_share, on="year", how="inner").sort_values("year").reset_index(drop=True)
    points = []
    for r in joined.to_dict("records"):
        points.append({
            "year": int(r["year"]),
            "participation_rate": _none_if_nan(r.get("participation_rate")),
            "noncitizen_share": _none_if_nan(r.get("noncitizen_share")),
        })

    return {
        "title": "Labor Force Participation vs Non-Citizen Share (Nation)",
        "description": (
            "Annual labor force participation rate (BLS CPS/LN) plotted against the share of non-citizens "
            "in the resident population (ACS via DataUSA). Helps frame labor supply changes."
        ),
        "y_left": {"label": "Labor Force Participation (%)", "key": "participation_rate", "color": "#22c55e"},
        "y_right": {"label": "Non-Citizen Share (%)", "key": "noncitizen_share", "color": "#8b5cf6"},
        "points": points,
    }


def export_site_payload(payload: dict[str, Any], out_path: str | Path) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path.resolve()

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


def build_pipeline_status(sync_results: dict, duration_seconds: float = 0.0) -> dict:
    """Build a pipeline_status.json payload from sync results.

    ``sync_results`` is the dict returned by the data-fetcher Lambda (or a
    local equivalent) with shape::

        {
            "bls": {"pr": {"added": [...], "updated": [...], "unchanged": [...], "deleted": [...]}, ...},
            "datausa": {"action": "updated", "content_hash": "...", "record_count": N},
        }
    """
    from datetime import datetime, timezone

    series_list = []
    total_checked = 0
    total_updated = 0
    total_unchanged = 0

    bls = sync_results.get("bls", {})
    for series_id, result in sorted(bls.items()):
        added = result.get("added", [])
        updated = result.get("updated", [])
        unchanged = result.get("unchanged", [])
        deleted = result.get("deleted", [])

        files = []
        for f in added:
            name = f if isinstance(f, str) else f.get("key", f.get("filename", ""))
            files.append({"name": name, "action": "added"})
        for f in updated:
            name = f if isinstance(f, str) else f.get("key", f.get("filename", ""))
            files.append({"name": name, "action": "updated"})
        for f in unchanged:
            name = f if isinstance(f, str) else f.get("key", f.get("filename", ""))
            files.append({"name": name, "action": "unchanged"})
        for f in deleted:
            name = f if isinstance(f, str) else f.get("key", f.get("filename", ""))
            files.append({"name": name, "action": "deleted"})

        n_updated = len(added) + len(updated)
        n_unchanged = len(unchanged)
        n_deleted = len(deleted)
        n_checked = n_updated + n_unchanged + n_deleted

        total_checked += n_checked
        total_updated += n_updated
        total_unchanged += n_unchanged

        series_list.append({
            "id": series_id,
            "name": series_id,
            "url": f"https://download.bls.gov/pub/time.series/{series_id}/",
            "files_checked": n_checked,
            "files_updated": n_updated,
            "files_unchanged": n_unchanged,
            "files_deleted": n_deleted,
            "files": files,
        })

    datausa_raw = sync_results.get("datausa", {})
    datausa = {
        "endpoint": "https://honolulu-api.datausa.io/tesseract/data.jsonrecords",
        "action": datausa_raw.get("action", "unknown"),
        "content_hash": datausa_raw.get("content_hash", ""),
        "record_count": datausa_raw.get("record_count", 0),
    }

    return {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "trigger": "EventBridge scheduled rule (nightly)",
        "duration_seconds": round(duration_seconds, 1),
        "summary": {
            "series_scanned": len(series_list),
            "total_files_checked": total_checked,
            "files_updated": total_updated,
            "files_unchanged": total_unchanged,
            "datausa_status": datausa["action"],
        },
        "series": series_list,
        "datausa": datausa,
    }


def export_pipeline_status(
    sync_results: dict,
    out_path: str | Path,
    duration_seconds: float = 0.0,
) -> Path:
    """Write pipeline_status.json for the static site."""
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_pipeline_status(sync_results, duration_seconds)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path.resolve()


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
    # Local convenience: generate a few demo curated payloads for the static site.
    site_dir = Path("site/data")
    results = run_all_reports(site_json_out=site_dir / "timeseries.json")

    # Additional Fed-style charts (requires DATAUSA_DATASETS + BLS_SERIES to include inputs).
    try:
        unemployment_payload = build_unemployment_vs_commute_time(
            bls_bucket=get_bls_bucket(),
            datausa_bucket=get_datausa_bucket(),
        )
        results["exported_unemployment_vs_commute_time"] = str(
            export_site_payload(unemployment_payload, site_dir / "unemployment_vs_commute_time.json")
        )
    except Exception as exc:
        results["exported_unemployment_vs_commute_time_error"] = str(exc)

    try:
        participation_payload = build_participation_vs_noncitizen_share(
            bls_bucket=get_bls_bucket(),
            datausa_bucket=get_datausa_bucket(),
        )
        results["exported_participation_vs_noncitizen_share"] = str(
            export_site_payload(participation_payload, site_dir / "participation_vs_noncitizen_share.json")
        )
    except Exception as exc:
        results["exported_participation_vs_noncitizen_share_error"] = str(exc)

    print(json.dumps(results, indent=2, default=str))
