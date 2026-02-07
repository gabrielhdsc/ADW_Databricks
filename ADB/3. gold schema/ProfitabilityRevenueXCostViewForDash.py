# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwprofitabilityrevenuexcost
# MAGIC AS 
# MAGIC WITH max_year AS (
# MAGIC   SELECT MAX(Year) AS max_year
# MAGIC   FROM adventure_works_catalog.gold.vwgoliveprofitability
# MAGIC )
# MAGIC SELECT
# MAGIC     ProductName,
# MAGIC     CONCAT(
# MAGIC         UPPER(LEFT(ProductName, 1)),                
# MAGIC         REGEXP_EXTRACT(ProductName, '(\\d+)', 1),   
# MAGIC         '-',
# MAGIC         REGEXP_EXTRACT(ProductName, '(\\d+)$', 1)  
# MAGIC     ) AS ProductShortName,
# MAGIC
# MAGIC     SUM(Revenue) AS TotalRevenue,
# MAGIC     SUM(Cost)    AS TotalCost
# MAGIC
# MAGIC FROM adventure_works_catalog.gold.vwgoliveprofitability v
# MAGIC CROSS JOIN max_year m
# MAGIC WHERE v.Year >= m.max_year - 1   
# MAGIC GROUP BY ProductName
# MAGIC ORDER BY TotalRevenue DESC
# MAGIC LIMIT 10;
# MAGIC
