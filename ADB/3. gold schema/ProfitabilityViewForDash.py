# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW adventure_works_catalog.gold.vwgoliveprofitability AS
# MAGIC SELECT
# MAGIC     -- Data
# MAGIC     d.FullDate            AS OrderDate,
# MAGIC     d.CalendarYear        AS Year,
# MAGIC     d.MonthNumberOfYear   AS Month,
# MAGIC     d.MonthName           AS MonthName,
# MAGIC
# MAGIC     -- Produto
# MAGIC     p.ProductID,
# MAGIC     p.ProductName,
# MAGIC     p.Category,
# MAGIC     p.SubCategory,
# MAGIC
# MAGIC     -- Localização
# MAGIC     l.CountryName         AS Country,
# MAGIC     l.StateProvinceName   AS StateProvince,
# MAGIC
# MAGIC     -- Moeda
# MAGIC     c.ToCurrencyCode      AS Currency,
# MAGIC
# MAGIC     -- Território
# MAGIC     t.TerritoryName       AS SalesTerritory,
# MAGIC     t.TerritoryGroup      AS TerritoryGroup,
# MAGIC
# MAGIC
# MAGIC     -- Métricas financeiras 
# MAGIC     SUM(s.LineTotal)                              AS Revenue,
# MAGIC     SUM(s.OrderQty * p.StandardCost)             AS Cost,
# MAGIC     SUM(s.LineTotal) - SUM(s.OrderQty * p.StandardCost) AS Profit,
# MAGIC     CASE
# MAGIC         WHEN SUM(s.LineTotal) = 0 THEN 0
# MAGIC         ELSE
# MAGIC             (SUM(s.LineTotal) - SUM(s.OrderQty * p.StandardCost))
# MAGIC             / SUM(s.LineTotal)
# MAGIC     END AS ProfitMargin,
# MAGIC
# MAGIC    -- Indicador da rentabilidade
# MAGIC     CASE
# MAGIC         WHEN (SUM(s.LineTotal) - SUM(s.OrderQty * p.StandardCost)) < 0
# MAGIC             THEN 'Not Profitable'
# MAGIC         ELSE 'Profitable'
# MAGIC     END AS ProfitabilityStatus
# MAGIC
# MAGIC FROM adventure_works_catalog.silver.fact_sales_order_detail s
# MAGIC JOIN (
# MAGIC     SELECT DISTINCT
# MAGIC          OrderID,
# MAGIC          OrderDate_SK,
# MAGIC          Location_SK,
# MAGIC          Currency_SK,
# MAGIC          Territory_SK
# MAGIC     FROM adventure_works_catalog.silver.fact_sales_order
# MAGIC ) o
# MAGIC     ON s.OrderID = o.OrderID
# MAGIC
# MAGIC
# MAGIC JOIN adventure_works_catalog.silver.dim_product p
# MAGIC     ON s.Product_SK = p.Product_SK
# MAGIC JOIN adventure_works_catalog.silver.dim_date d
# MAGIC     ON o.OrderDate_SK = d.DateKey
# MAGIC JOIN adventure_works_catalog.silver.dim_location l
# MAGIC     ON o.Location_SK = l.Location_SK
# MAGIC JOIN adventure_works_catalog.silver.dim_currency c
# MAGIC     ON o.Currency_SK = c.Currency_SK
# MAGIC JOIN adventure_works_catalog.silver.dim_territory t
# MAGIC     ON o.Territory_SK = t.Territory_SK
# MAGIC
# MAGIC  
# MAGIC GROUP BY
# MAGIC     d.FullDate,
# MAGIC     d.CalendarYear,
# MAGIC     d.MonthNumberOfYear,
# MAGIC     d.MonthName,
# MAGIC     p.ProductID,
# MAGIC     p.ProductName,
# MAGIC     p.Category,
# MAGIC     p.SubCategory,
# MAGIC     l.CountryName,
# MAGIC     l.StateProvinceName,
# MAGIC     c.ToCurrencyCode,
# MAGIC     t.TerritoryName,
# MAGIC     t.TerritoryGroup;
# MAGIC
