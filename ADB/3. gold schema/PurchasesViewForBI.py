# Databricks notebook source
spark.sql("""
    CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwPurchasesForBI AS
    SELECT
    PurchaseOrderID, 
    CAST(SUM(LineTotal) AS DECIMAL(18,2)) AS Purchases,
    CAST(MAX(ShippingCost) AS DECIMAL(18,2)) + CAST(MAX(TaxAmount) AS DECIMAL(18,2)) AS Variable_Costs,
    CAST(Purchases - Variable_Costs AS DECIMAL (18,2)) AS Fixed_Costs,
    date_format(OrderDate, 'yyyy-MM-dd') AS FullDate
    FROM adventure_works_catalog.silver.fact_purchases
    GROUP BY PurchaseOrderID, FullDate
""")