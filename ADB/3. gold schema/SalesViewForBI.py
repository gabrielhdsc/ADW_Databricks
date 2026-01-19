# Databricks notebook source
    spark.sql("""
CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwSalesForBI AS
SELECT
    s.OrderID,
    CAST(SUM(s.LineTotal) AS DECIMAL(18,2)) AS Sales,
    CAST(MAX(s.ShippingCost) AS DECIMAL(18,2))  AS ShippingCost,
    CAST(MAX(s.TaxAmt) AS DECIMAL(18,2)) AS TaxAmount,
    date_format(d.FullDate, 'yyyy-MM-dd') AS FullDate,
    CONCAT(c.FirstName, ' ', c.LastName) AS Customer
    FROM adventure_works_catalog.silver.fact_sales s
    INNER JOIN adventure_works_catalog.silver.dim_customer c
        ON c.CustomerID = s.CustomerID
    INNER JOIN adventure_works_catalog.silver.dim_date d 
        ON d.DateKey = CAST(date_format(s.OrderDate, 'yyyyMMdd') AS INT)
    GROUP BY s.OrderID, d.FullDate, Customer 
    ORDER BY s.OrderID ASC
""")