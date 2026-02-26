# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

def generate_orders(seed=42):
  np.random.seed(seed)

  dates = pd.date_range("2016-01-01", "2021-12-31", freq="MS")
  df = pd.DataFrame({"YearMonth": dates})

  df["year"] = df["YearMonth"].dt.year
  df["month"] = df["YearMonth"].dt.month

  growth = 1 + (df["year"]-2016) * 0.08

  noise = np.random.normal(1, 0.02, len(df))

  base_value = 2_800_000

  df["label"] = (base_value * growth * noise).astype("int64")

  df["lag_1_month"] = df["label"].shift(1)
  df["lag_3_month"] = df["label"].shift(3)
  df["rolling_mean_3m"] = df["label"].rolling(3).mean()
  df["rolling_mean_6m"] = df["label"].rolling(6).mean()

  return df

# COMMAND ----------

df = generate_orders()

spark_df = spark.createDataFrame(df)

spark_df.write.mode("overwrite").saveAsTable("adventure_works_catalog.silver.sintetic_data_for_ml")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwsinteticdataml AS

    WITH base_union AS (
         SELECT
        to_date(ReferenceMonth) AS YearMonth,
        MonthlySalesAmount AS label
    FROM adventure_works_catalog.gold.feature_demand_value_monthly_v1

    UNION ALL
    
    SELECT
        YearMonth,
        label
    FROM adventure_works_catalog.silver.sintetic_data_for_ml
),

base_grouped AS (
    SELECT
        YearMonth,
        label
    FROM base_union
)

    SELECT
    to_date(YearMonth) AS YearMonth,
    MONTH(YearMonth) AS month,
    YEAR(YearMonth) AS year,
    CAST(label AS DOUBLE) AS label,

    LAG(label, 1) OVER (
        ORDER BY YearMonth
    )AS lag_1_month,

    LAG(label, 3) OVER (
        ORDER BY YearMonth
    ) AS lag_3_month,

    AVG(label) OVER (
        ORDER BY YearMonth
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS rolling_mean_3m,

    AVG(label) OVER (
        ORDER BY YearMonth
        ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
    ) AS rolling_mean_6m

FROM base_grouped

QUALIFY lag_3_month IS NOT NULL

ORDER BY YearMonth;
""")
