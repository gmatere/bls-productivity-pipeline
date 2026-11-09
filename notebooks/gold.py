import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Q1 — mean and standard deviation of annual US population, 2013-2018 inclusive.
# "Standard deviation" is ambiguous: Spark's stddev() is the sample form, but
# 2013-2018 is the complete set the question names, not a sample from a wider
# population. Both are emitted so the consumer can choose. See PROCESS.md.
@dlt.table(comment="US population mean and standard deviation, 2013-2018 inclusive. "
                   "Both sample and population stddev emitted.")
def gold_population_stats():
    return (spark.read.table("silver_population")
            .filter(F.col("year").between(2013, 2018))
            .agg(F.mean("population").alias("mean_population"),
                 F.stddev_samp("population").alias("stddev_population_sample"),
                 F.stddev_pop("population").alias("stddev_population_population"),
                 F.count("*").alias("years_observed")))


# Q2 — best year per series.
# Q05 is the ANNUAL AVERAGE, not a fifth quarter (see pr.period). Including it
# would add the mean of the four quarters to the four quarters. Excluded.
# Ties broken by earliest year, so the result is deterministic.
@dlt.table(comment="Best year per series: largest sum of quarterly values. Q05 excluded.")
def gold_series_best_year():
    totals = (spark.read.table("silver_observations")
              .filter(~F.col("is_annual_average"))
              .groupBy("series_id", "year")
              .agg(F.sum("value").alias("summed_value")))

    ranked = Window.partitionBy("series_id").orderBy(
        F.desc("summed_value"), F.asc("year"))

    best = (totals.withColumn("rn", F.row_number().over(ranked))
                  .filter(F.col("rn") == 1)
                  .drop("rn"))

    labels = spark.read.table("silver_series").select("series_id", "series_label")
    return (best.join(labels, "series_id", "left")
                .select("series_id", "series_label", "year", "summed_value"))


# Q3 — PRS30006032, Q01, value per year with that year's population where known
@dlt.table(comment="PRS30006032 Q01 values joined to annual population.")
def gold_prs30006032_q01():
    obs = (spark.read.table("silver_observations")
           .filter((F.col("series_id") == "PRS30006032") &
                   (F.col("period") == "Q01"))
           .select("series_id", "year", "period", "value"))
    pop = spark.read.table("silver_population").select("year", "population")
    return obs.join(pop, "year", "left")
