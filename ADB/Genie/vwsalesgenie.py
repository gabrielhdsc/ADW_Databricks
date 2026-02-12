# Databricks notebook source
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwsalesgenie AS
# MAGIC SELECT
# MAGIC     so.OrderID,
# MAGIC     so.Customer_SK,
# MAGIC     so.Territory_SK,
# MAGIC     d.FullDate,
# MAGIC     d.CalendarYear AS Year,
# MAGIC     d.MonthNumberOfYear AS Month,
# MAGIC     
# MAGIC     so.SubTotal AS Sales_SubTotal,
# MAGIC     so.TaxAmt AS Sales_Tax,
# MAGIC     so.ShippingCost,
# MAGIC     (so.SubTotal + so.TaxAmt + so.ShippingCost) AS Sales_Total
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_sales_order so
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON so.OrderDate_SK = d.DateKey
# MAGIC """)
    