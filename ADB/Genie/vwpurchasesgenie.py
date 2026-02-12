# Databricks notebook source
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwpurchasesgenie AS
# MAGIC SELECT
# MAGIC     fp.Product_SK,
# MAGIC     fp.Supplier_SK,
# MAGIC
# MAGIC     d.CalendarYear AS Year,
# MAGIC     d.MonthNumberOfYear AS Month,
# MAGIC
# MAGIC     fp.OrderQty,
# MAGIC     fp.ReceivedQuantity,
# MAGIC     fp.LineTotal,
# MAGIC     fp.TotalDue
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_purchases fp
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON fp.OrderDate_SK = d.DateKey
# MAGIC """)
