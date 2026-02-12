# Databricks notebook source
# MAGIC spark.sql("""
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwcustomergenie AS
# MAGIC SELECT
# MAGIC     so.OrderID,
# MAGIC     so.Customer_SK,
# MAGIC
# MAGIC     d.FullDate,
# MAGIC     d.CalendarYear AS Year,
# MAGIC     d.MonthNumberOfYear AS Month,
# MAGIC
# MAGIC     c.CustomerID,
# MAGIC     c.CustomerType,
# MAGIC     c.PersonName,
# MAGIC     c.StoreName,
# MAGIC
# MAGIC     so.SubTotal AS ProductRevenue,
# MAGIC     so.TaxAmt AS TaxAmount,
# MAGIC     so.ShippingCost AS ShippingAmount,
# MAGIC     (so.SubTotal + so.TaxAmt + so.ShippingCost) AS TotalRevenue
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_sales_order so
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_customer c
# MAGIC     ON so.Customer_SK = c.Customer_SK
# MAGIC
# MAGIC LEFT JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON so.OrderDate_SK = d.DateKey
# MAGIC """)
