# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwprofitabilitynotprofprod
# MAGIC AS SELECT
# MAGIC     ProductName,
# MAGIC     Category,
# MAGIC     SubCategory,
# MAGIC     Country,
# MAGIC     SUM(Revenue)        AS Revenue,
# MAGIC     SUM(Cost)           AS Cost,
# MAGIC     SUM(Profit)         AS Profit,
# MAGIC     AVG(ProfitMargin) AS AvgProfitMargin
# MAGIC FROM adventure_works_catalog.gold.vwgoliveprofitability
# MAGIC GROUP BY
# MAGIC     ProductName,
# MAGIC     Category,
# MAGIC     SubCategory,
# MAGIC     Country
# MAGIC HAVING AVG(ProfitMargin) < 0
# MAGIC ORDER BY
# MAGIC     AVG(ProfitMargin) ASC
# MAGIC LIMIT 10;
# MAGIC
