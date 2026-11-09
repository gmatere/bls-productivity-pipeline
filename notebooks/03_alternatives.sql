-- Spark SQL alternatives.
-- PySpark is the primary implementation and builds the Gold tables (gold.py).
-- These are the same three questions expressed in SQL, plus an equivalence
-- check proving the two implementations agree.

-- ---------------------------------------------------------------- Q1
-- Expected: 322069808 | 4158441.040908095 | 3796119.936934378 | 6
SELECT
  avg(population)         AS mean_population,
  stddev_samp(population) AS stddev_population_sample,
  stddev_pop(population)  AS stddev_population_population,
  count(*)                AS years_observed
FROM workspace.rearc.silver_population
WHERE year BETWEEN 2013 AND 2018;


-- ---------------------------------------------------------------- Q2
-- Q05 is the annual average, not a fifth quarter, so it is excluded.
-- Ties broken by earliest year. Expected: 283 rows.
WITH totals AS (
  SELECT series_id, year, sum(value) AS summed_value
  FROM workspace.rearc.silver_observations
  WHERE NOT is_annual_average
  GROUP BY series_id, year
),
ranked AS (
  SELECT *,
         row_number() OVER (PARTITION BY series_id
                            ORDER BY summed_value DESC, year ASC) AS rn
  FROM totals
)
SELECT r.series_id, s.series_label, r.year, r.summed_value
FROM ranked r
LEFT JOIN workspace.rearc.silver_series s USING (series_id)
WHERE r.rn = 1
ORDER BY r.series_id;


-- ---------------------------------------------------------------- Q3
-- Left join: population is available for 11 of the 39 observation years.
-- Expected: 39 rows.
SELECT o.series_id, o.year, o.period, o.value, p.population
FROM workspace.rearc.silver_observations o
LEFT JOIN workspace.rearc.silver_population p USING (year)
WHERE o.series_id = 'PRS30006032' AND o.period = 'Q01'
ORDER BY o.year;


-- ------------------------------------------------- equivalence check
-- The SQL alternative and the PySpark primary must agree. Expected: 0
WITH totals AS (
  SELECT series_id, year, sum(value) AS summed_value
  FROM workspace.rearc.silver_observations
  WHERE NOT is_annual_average
  GROUP BY series_id, year
),
sql_version AS (
  SELECT series_id, year, summed_value
  FROM (SELECT *, row_number() OVER (PARTITION BY series_id
                                     ORDER BY summed_value DESC, year ASC) AS rn
        FROM totals)
  WHERE rn = 1
)
SELECT count(*) AS mismatches
FROM sql_version s
FULL OUTER JOIN workspace.rearc.gold_series_best_year g USING (series_id)
WHERE s.year IS DISTINCT FROM g.year
   OR s.summed_value IS DISTINCT FROM g.summed_value;
