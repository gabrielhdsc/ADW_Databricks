# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwprofitabilityrevenuexcost
# MAGIC AS 
# MAGIC WITH max_year AS (
# MAGIC   SELECT MAX(Year) AS max_year
# MAGIC   FROM adventure_works_catalog.gold.vwgoliveprofitability
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ProductName,
# MAGIC     SUM(Revenue) AS TotalRevenue,
# MAGIC     SUM(Cost)    AS TotalCost
# MAGIC FROM adventure_works_catalog.gold.vwgoliveprofitability v
# MAGIC CROSS JOIN max_year m
# MAGIC WHERE v.Year >= m.max_year - 1   -- últimos 2 anos
# MAGIC GROUP BY ProductName
# MAGIC ORDER BY TotalRevenue DESC
# MAGIC LIMIT 10;
# MAGIC
