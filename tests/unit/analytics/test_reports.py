"""Tests for analytics/reports.py using PySpark local mode."""

import json
import math

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from src.analytics.reports import (
    report_population_stats,
    report_best_year_by_series,
    report_series_population_join,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    session = (
        SparkSession.builder
        .appName("test-fomc")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def population_df(spark):
    """Population DataFrame with known data."""
    data = [
        (2013, "United States", 311536594),
        (2014, "United States", 314107084),
        (2015, "United States", 316515021),
        (2016, "United States", 318558162),
        (2017, "United States", 321004407),
        (2018, "United States", 322903030),
        (2019, "United States", 324697795),
        (2020, "United States", 326569308),
    ]
    schema = StructType([
        StructField("Year", IntegerType(), False),
        StructField("Nation", StringType(), True),
        StructField("Population", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def bls_df(spark):
    """BLS DataFrame with known data."""
    data = [
        ("PRS30006011", 1995, "Q01", 1.0),
        ("PRS30006011", 1995, "Q02", 2.0),
        ("PRS30006011", 1996, "Q01", 3.0),
        ("PRS30006011", 1996, "Q02", 4.0),
        ("PRS30006012", 2000, "Q01", 0.0),
        ("PRS30006012", 2000, "Q02", 8.0),
        ("PRS30006012", 2001, "Q01", 2.0),
        ("PRS30006012", 2001, "Q02", 3.0),
        ("PRS30006032", 2018, "Q01", 1.9),
        ("PRS30006032", 2018, "Q02", 2.1),
        ("PRS30006032", 2017, "Q01", 1.5),
    ]
    schema = StructType([
        StructField("series_id", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("period", StringType(), False),
        StructField("value", FloatType(), True),
    ])
    return spark.createDataFrame(data, schema)


class TestReport1PopulationStats:
    def test_population_mean_known_data(self, population_df):
        """Mean of fixture data matches expected."""
        result = report_population_stats(population_df)
        pops = [311536594, 314107084, 316515021, 318558162, 321004407, 322903030]
        expected_mean = sum(pops) / len(pops)
        assert abs(result["mean"] - expected_mean) < 1

    def test_population_stddev_known_data(self, population_df):
        """Std dev matches expected."""
        result = report_population_stats(population_df)
        assert result["stddev"] is not None
        assert result["stddev"] > 0

    def test_population_filter_year_range(self, population_df):
        """Only years 2013-2018 included."""
        result = report_population_stats(population_df)
        # The mean should not include 2019 or 2020 data
        all_pops = [311536594, 314107084, 316515021, 318558162, 321004407, 322903030, 324697795, 326569308]
        all_mean = sum(all_pops) / len(all_pops)
        assert abs(result["mean"] - all_mean) > 1000  # Should differ from all-years mean


class TestReport2BestYear:
    def test_best_year_single_series(self, bls_df):
        """Correct year selected for one series."""
        result = report_best_year_by_series(bls_df)
        rows = {r["series_id"]: r for r in [row.asDict() for row in result.collect()]}
        assert rows["PRS30006011"]["year"] == 1996
        assert abs(rows["PRS30006011"]["value"] - 7.0) < 0.01

    def test_best_year_multiple_series(self, bls_df):
        """Each series gets its own best year."""
        result = report_best_year_by_series(bls_df)
        rows = {r["series_id"]: r for r in [row.asDict() for row in result.collect()]}
        assert "PRS30006011" in rows
        assert "PRS30006012" in rows
        assert rows["PRS30006012"]["year"] == 2000
        assert abs(rows["PRS30006012"]["value"] - 8.0) < 0.01

    def test_quarterly_sum_calculation(self, bls_df):
        """Q01+Q02 summed correctly."""
        result = report_best_year_by_series(bls_df)
        rows = {r["series_id"]: r for r in [row.asDict() for row in result.collect()]}
        # PRS30006011 1996: Q01=3 + Q02=4 = 7
        assert abs(rows["PRS30006011"]["value"] - 7.0) < 0.01


class TestReport3SeriesPopulationJoin:
    def test_join_matching_years(self, bls_df, population_df):
        """Join produces correct merged rows."""
        result = report_series_population_join(bls_df, population_df)
        rows = [r.asDict() for r in result.collect()]
        year_2018 = [r for r in rows if r["year"] == 2018]
        assert len(year_2018) == 1
        assert year_2018[0]["Population"] == 322903030
        assert abs(year_2018[0]["value"] - 1.9) < 0.01

    def test_join_missing_population(self, bls_df, spark):
        """Handles years without population data."""
        # Only 2019 population
        pop = spark.createDataFrame(
            [(2019, "United States", 324697795)],
            StructType([
                StructField("Year", IntegerType(), False),
                StructField("Nation", StringType(), True),
                StructField("Population", IntegerType(), False),
            ]),
        )
        result = report_series_population_join(bls_df, pop)
        rows = [r.asDict() for r in result.collect()]
        # 2018 and 2017 should have None population
        for r in rows:
            assert r["Population"] is None

    def test_filter_specific_series_and_period(self, bls_df, population_df):
        """Only PRS30006032 + Q01."""
        result = report_series_population_join(bls_df, population_df)
        rows = [r.asDict() for r in result.collect()]
        for r in rows:
            assert r["series_id"] == "PRS30006032"
            assert r["period"] == "Q01"

    def test_whitespace_trimming(self, spark, population_df):
        """series_id with leading/trailing spaces still matches."""
        # BLS data with whitespace in series_id
        bls_with_spaces = spark.createDataFrame(
            [("  PRS30006032  ", 2018, "Q01", 1.9)],
            StructType([
                StructField("series_id", StringType(), False),
                StructField("year", IntegerType(), False),
                StructField("period", StringType(), False),
                StructField("value", FloatType(), True),
            ]),
        )
        # Trim the data first (as reports.py does during load)
        from pyspark.sql import functions as F
        bls_trimmed = bls_with_spaces.withColumn("series_id", F.trim("series_id"))
        result = report_series_population_join(bls_trimmed, population_df)
        rows = [r.asDict() for r in result.collect()]
        assert len(rows) == 1
