# Databricks notebook source
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwregiongenie AS
# MAGIC SELECT
# MAGIC     so.OrderID,
# MAGIC     so.Customer_SK,
# MAGIC
# MAGIC     d.FullDate,
# MAGIC     d.CalendarYear AS Year,
# MAGIC     d.MonthNumberOfYear AS Month,
# MAGIC
# MAGIC     t.TerritoryName,
# MAGIC     t.TerritoryGroup,
# MAGIC     t.CountryRegionCode,
# MAGIC
# MAGIC     l.City,
# MAGIC     l.StateProvinceName,
# MAGIC     l.CountryName,
# MAGIC
# MAGIC     so.SubTotal AS ProductRevenue,
# MAGIC     so.TaxAmt AS TaxAmount,
# MAGIC     so.ShippingCost AS ShippingAmount,
# MAGIC     (so.SubTotal + so.TaxAmt + so.ShippingCost) AS TotalRevenue
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_sales_order so
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON so.OrderDate_SK = d.DateKey
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_territory t
# MAGIC     ON so.Territory_SK = t.Territory_SK
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_location l
# MAGIC     ON so.Location_SK = l.Location_SK
# MAGIC """)
