# Databricks notebook source
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwproductgenie AS
# MAGIC SELECT
# MAGIC     sod.OrderID,
# MAGIC     sod.Product_SK,
# MAGIC     p.ProductName,
# MAGIC     p.Category,
# MAGIC     p.SubCategory,
# MAGIC
# MAGIC     d.CalendarYear AS Year,
# MAGIC     d.MonthNumberOfYear AS Month,
# MAGIC
# MAGIC     sod.OrderQty,
# MAGIC     sod.LineTotal AS SalesAmount
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_sales_order_detail sod
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_product p
# MAGIC     ON sod.Product_SK = p.Product_SK
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.fact_sales_order so
# MAGIC     ON sod.OrderID = so.OrderID
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON so.OrderDate_SK = d.DateKey
# MAGIC """)
