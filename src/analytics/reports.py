"""PySpark analytics reports for FOMC data pipeline."""

import json
import sys
from io import StringIO

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from src.helpers.aws_client import get_client


def get_spark() -> SparkSession:
    """Create or get a SparkSession."""
    return (
        SparkSession.builder
        .appName("FOMC-Analytics")
        .master("local[*]")
        .getOrCreate()
    )


def load_population_from_s3(
    spark: SparkSession, bucket: str = "fomc-datausa-raw", key: str = "population.json"
) -> DataFrame:
    """Load population JSON from S3 into a DataFrame."""
    s3 = get_client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read())

    records = data.get("data", [])
    rows = [
        (int(r["Year"]), r["Nation"], int(r["Population"]))
        for r in records
        if "Year" in r and "Population" in r
    ]

    schema = StructType([
        StructField("Year", IntegerType(), False),
        StructField("Nation", StringType(), True),
        StructField("Population", IntegerType(), False),
    ])
    return spark.createDataFrame(rows, schema)


def load_bls_from_s3(
    spark: SparkSession,
    bucket: str = "fomc-bls-raw",
    key: str = "pr/pr.data.0.Current",
) -> DataFrame:
    """Load BLS CSV from S3 into a DataFrame."""
    s3 = get_client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response["Body"].read().decode("utf-8")

    # Read as text via Spark
    rdd = spark.sparkContext.parallelize(csv_content.splitlines())
    df = spark.read.option("header", "true").option("delimiter", "\t").csv(rdd)

    # Trim whitespace from all string columns
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Cast numeric columns
    df = df.withColumn("year", F.col("year").cast(IntegerType()))
    df = df.withColumn("value", F.col("value").cast(FloatType()))

    return df


def report_population_stats(pop_df: DataFrame) -> dict:
    """Report 1: Mean and std dev of US population for 2013-2018."""
    filtered = pop_df.filter(
        (F.col("Year") >= 2013) & (F.col("Year") <= 2018)
    )
    stats = filtered.agg(
        F.mean("Population").alias("mean"),
        F.stddev("Population").alias("stddev"),
    ).collect()[0]

    return {
        "report": "Population Statistics (2013-2018)",
        "mean": float(stats["mean"]) if stats["mean"] else None,
        "stddev": float(stats["stddev"]) if stats["stddev"] else None,
    }


def report_best_year_by_series(bls_df: DataFrame) -> DataFrame:
    """Report 2: Best year per series_id (year with max sum of values)."""
    # Filter to quarterly data only
    quarterly = bls_df.filter(F.col("period").startswith("Q"))

    # Sum values per series_id + year
    yearly_sums = quarterly.groupBy("series_id", "year").agg(
        F.sum("value").alias("total_value")
    )

    # Find best year per series_id
    from pyspark.sql.window import Window

    w = Window.partitionBy("series_id").orderBy(F.desc("total_value"))
    best = yearly_sums.withColumn("rank", F.row_number().over(w))
    best = best.filter(F.col("rank") == 1).drop("rank")
    best = best.select("series_id", "year", F.col("total_value").alias("value"))

    return best


def report_series_population_join(bls_df: DataFrame, pop_df: DataFrame) -> DataFrame:
    """Report 3: PRS30006032 Q01 values joined with population by year."""
    filtered_bls = bls_df.filter(
        (F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")
    ).select("series_id", "year", "period", "value")

    joined = filtered_bls.join(
        pop_df.select(F.col("Year").alias("year"), "Population"),
        on="year",
        how="left",
    )

    return joined.select("series_id", "year", "period", "value", "Population")


def run_all_reports(
    bls_bucket: str = "fomc-bls-raw",
    bls_key: str = "pr/pr.data.0.Current",
    pop_bucket: str = "fomc-datausa-raw",
    pop_key: str = "population.json",
) -> dict:
    """Run all three reports and return results."""
    spark = get_spark()

    pop_df = load_population_from_s3(spark, pop_bucket, pop_key)
    bls_df = load_bls_from_s3(spark, bls_bucket, bls_key)

    # Report 1
    pop_stats = report_population_stats(pop_df)

    # Report 2
    best_year_df = report_best_year_by_series(bls_df)
    best_year_rows = [row.asDict() for row in best_year_df.collect()]

    # Report 3
    join_df = report_series_population_join(bls_df, pop_df)
    join_rows = [row.asDict() for row in join_df.collect()]

    return {
        "report_1_population_stats": pop_stats,
        "report_2_best_year_by_series": best_year_rows,
        "report_3_series_population_join": join_rows,
    }


if __name__ == "__main__":
    results = run_all_reports()
    print(json.dumps(results, indent=2, default=str))
