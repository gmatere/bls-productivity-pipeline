# Databricks notebook source
# Spark Declarative Pipeline source: Bronze and Silver.
# Attached to pipeline `bls_productivity`, target workspace.rearc.

import dlt
from pyspark.sql import functions as F

VOLUME = "/Volumes/workspace/rearc/raw"


def _tsv(name):
    """BLS ships fixed-width-padded headers inside a tab-separated file, so
    column names arrive as 'series_id      ' and '      value'. Strip them at
    the boundary: the padding is a formatting artifact of the file, not data."""
    df = (spark.read
          .option("sep", "\t")
          .option("header", True)
          .option("inferSchema", False)
          .csv(f"{VOLUME}/{name}"))
    return df.toDF(*[c.strip() for c in df.columns])


# ---------------- BRONZE : land as-is ----------------

@dlt.table(comment="Raw BLS observations, as landed.")
def bronze_pr_data():
    return _tsv("pr.data.1.AllData")


@dlt.table(comment="Series metadata, as landed.")
def bronze_pr_series():
    return _tsv("pr.series")


@dlt.table(comment="Sector lookup.")
def bronze_pr_sector():
    return _tsv("pr.sector")


@dlt.table(comment="Measure lookup.")
def bronze_pr_measure():
    return _tsv("pr.measure")


@dlt.table(comment="Population API response, flattened. Field names normalised "
                   "because Delta rejects spaces in column names.")
def bronze_population():
    raw = spark.read.option("multiline", True).json(f"{VOLUME}/population.json")
    return (raw
            .select(F.explode("data").alias("r"))
            .select(F.col("r.`Nation ID`").alias("nation_id"),
                    F.col("r.Nation").alias("nation"),
                    F.col("r.Year").alias("year"),
                    F.col("r.Population").alias("population")))


# ---------------- SILVER : trim, type, assert ----------------

@dlt.table(comment="Typed observations. series_id trimmed; Q05 flagged, not dropped.")
@dlt.expect_or_fail("series_id_present", "series_id IS NOT NULL AND series_id <> ''")
@dlt.expect_or_fail("year_present",      "year IS NOT NULL")
@dlt.expect_or_fail("period_present",    "period IS NOT NULL")
@dlt.expect_or_fail("value_present",     "value IS NOT NULL")
@dlt.expect("period_is_known",           "period RLIKE '^Q0[1-5]$'")
def silver_observations():
    return (spark.read.table("bronze_pr_data")
            .select(
                F.trim("series_id").alias("series_id"),
                F.col("year").cast("int").alias("year"),
                F.trim("period").alias("period"),
                F.col("value").cast("double").alias("value"),
                F.trim(F.col("footnote_codes")).alias("footnote_codes"),
            )
            .withColumn("is_annual_average", F.col("period") == F.lit("Q05")))


@dlt.table(comment="Series with human-readable labels.")
@dlt.expect_or_fail("series_id_present", "series_id IS NOT NULL")
def silver_series():
    s = spark.read.table("bronze_pr_series").select(
            F.trim("series_id").alias("series_id"),
            F.trim("sector_code").alias("sector_code"),
            F.trim("measure_code").alias("measure_code"))
    sector = spark.read.table("bronze_pr_sector").select(
            F.trim("sector_code").alias("sector_code"),
            F.trim("sector_name").alias("sector_name"))
    measure = spark.read.table("bronze_pr_measure").select(
            F.trim("measure_code").alias("measure_code"),
            F.trim("measure_text").alias("measure_text"))
    return (s.join(sector, "sector_code", "left")
             .join(measure, "measure_code", "left")
             .withColumn("series_label",
                         F.concat_ws(" — ", "measure_text", "sector_name"))
             .select("series_id", "series_label", "sector_name", "measure_text"))


@dlt.table(comment="Annual US population from the Data USA API.")
@dlt.expect_or_fail("year_present", "year IS NOT NULL")
def silver_population():
    return (spark.read.table("bronze_population")
            .select(F.col("year").cast("int").alias("year"),
                    F.col("population").cast("long").alias("population"),
                    "nation"))
