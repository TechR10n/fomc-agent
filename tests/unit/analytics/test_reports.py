"""Tests for analytics/reports.py (pandas implementation)."""

import pandas as pd

from src.analytics.reports import (
    report_population_stats,
    report_best_year_by_series,
    report_series_population_join,
    build_timeseries_payload,
)


def test_report_1_population_stats_mean_and_stddev():
    pop_df = pd.DataFrame(
        [
            {"Year": 2013, "Nation": "United States", "Population": 311536594},
            {"Year": 2014, "Nation": "United States", "Population": 314107084},
            {"Year": 2015, "Nation": "United States", "Population": 316515021},
            {"Year": 2016, "Nation": "United States", "Population": 318558162},
            {"Year": 2017, "Nation": "United States", "Population": 321004407},
            {"Year": 2018, "Nation": "United States", "Population": 322903030},
            {"Year": 2019, "Nation": "United States", "Population": 324697795},
            {"Year": 2020, "Nation": "United States", "Population": 326569308},
        ]
    )

    result = report_population_stats(pop_df)
    pops = [311536594, 314107084, 316515021, 318558162, 321004407, 322903030]
    expected_mean = sum(pops) / len(pops)

    assert abs(result["mean"] - expected_mean) < 1
    assert result["stddev"] is not None
    assert result["stddev"] > 0


def test_report_2_best_year_by_series():
    bls_df = pd.DataFrame(
        [
            {"series_id": "PRS30006011", "year": 1995, "period": "Q01", "value": 1.0},
            {"series_id": "PRS30006011", "year": 1995, "period": "Q02", "value": 2.0},
            {"series_id": "PRS30006011", "year": 1996, "period": "Q01", "value": 3.0},
            {"series_id": "PRS30006011", "year": 1996, "period": "Q02", "value": 4.0},
            {"series_id": "PRS30006012", "year": 2000, "period": "Q01", "value": 0.0},
            {"series_id": "PRS30006012", "year": 2000, "period": "Q02", "value": 8.0},
            {"series_id": "PRS30006012", "year": 2001, "period": "Q01", "value": 2.0},
            {"series_id": "PRS30006012", "year": 2001, "period": "Q02", "value": 3.0},
            {"series_id": "PRS30006032", "year": 2018, "period": "Q01", "value": 1.9},
            {"series_id": "PRS30006032", "year": 2018, "period": "Q02", "value": 2.1},
            {"series_id": "PRS30006032", "year": 2017, "period": "Q01", "value": 1.5},
        ]
    )

    result = report_best_year_by_series(bls_df)
    rows = {r["series_id"]: r for r in result}

    assert rows["PRS30006011"]["year"] == 1996
    assert abs(rows["PRS30006011"]["value"] - 7.0) < 0.01

    assert rows["PRS30006012"]["year"] == 2000
    assert abs(rows["PRS30006012"]["value"] - 8.0) < 0.01


def test_report_3_series_population_join_handles_missing_population():
    bls_df = pd.DataFrame(
        [
            {"series_id": "PRS30006032", "year": 2017, "period": "Q01", "value": 1.5},
            {"series_id": "PRS30006032", "year": 2018, "period": "Q01", "value": 1.9},
        ]
    )
    pop_df = pd.DataFrame(
        [{"Year": 2019, "Nation": "United States", "Population": 324697795}]
    )

    result = report_series_population_join(bls_df, pop_df)
    assert len(result) == 2
    assert all(r["Population"] is None for r in result)


def test_report_3_filters_specific_series_and_period_and_trims_whitespace():
    bls_df = pd.DataFrame(
        [
            {"series_id": "  PRS30006032  ", "year": 2018, "period": "Q01", "value": 1.9},
            {"series_id": "PRS30006032", "year": 2018, "period": "Q02", "value": 2.1},
            {"series_id": "PRS30006099", "year": 2018, "period": "Q01", "value": 9.9},
        ]
    )
    pop_df = pd.DataFrame(
        [{"Year": 2018, "Nation": "United States", "Population": 322903030}]
    )

    result = report_series_population_join(bls_df, pop_df)
    assert len(result) == 1
    assert result[0]["series_id"] == "PRS30006032"
    assert result[0]["period"] == "Q01"
    assert result[0]["Population"] == 322903030
    assert abs(result[0]["value"] - 1.9) < 0.01


def test_build_timeseries_payload_shape():
    rows = [
        {"series_id": "PRS30006032", "year": 2017, "period": "Q01", "value": 1.5, "Population": None},
        {"series_id": "PRS30006032", "year": 2018, "period": "Q01", "value": 1.9, "Population": 322903030},
    ]
    payload = build_timeseries_payload(rows)
    assert "title" in payload
    assert "points" in payload
    assert payload["points"][0]["year"] == 2017
    assert payload["points"][1]["bls_value"] == 1.9

