# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Feito para análises como: "Quais são os produtos que estão crescendo rapidamente em uma região específica?
-- Agregando dados de vendas por produto e região diariamente

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_sales_insights_product_region_daily AS

WITH sales_base AS (
    SELECT
        dt.FullDate AS ReferenceDate,
        d.Product_SK,
        t.Territory_SK,
        t.TerritoryName,
        SUM(d.OrderQty) AS SalesQty,
        SUM(d.LineTotal) AS SalesAmount
    FROM adventure_works_catalog.silver.fact_sales_order_detail d
    JOIN adventure_works_catalog.silver.fact_sales_order h
        ON d.OrderID = h.OrderID
    JOIN adventure_works_catalog.silver.dim_date dt
        ON h.OrderDate_SK = dt.DateKey
    JOIN adventure_works_catalog.silver.dim_territory t
        ON h.Territory_SK = t.Territory_SK
    GROUP BY
        dt.FullDate,
        d.Product_SK,
        t.Territory_SK,
        t.TerritoryName
),

regional_windows AS (
    SELECT
        ReferenceDate,
        Product_SK,
        Territory_SK,
        TerritoryName,
        SalesQty,
        SalesAmount,

        SUM(SalesAmount) OVER (
            PARTITION BY Product_SK, Territory_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
        ) AS SalesAmountRegion30Days
    FROM sales_base
),

product_windows AS (
    SELECT
        ReferenceDate,
        Product_SK,

        SUM(SalesAmount) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
        ) AS SalesAmountProduct30Days
    FROM sales_base
),

combined AS (
    SELECT
        r.ReferenceDate,
        r.Product_SK,
        r.Territory_SK,
        r.TerritoryName,
        r.SalesQty,
        r.SalesAmount,
        r.SalesAmountRegion30Days,
        p.SalesAmountProduct30Days
    FROM regional_windows r
    JOIN product_windows p
        ON r.Product_SK = p.Product_SK
        AND r.ReferenceDate = p.ReferenceDate
)

SELECT
    ReferenceDate,
    Product_SK,
    Territory_SK,
    TerritoryName,
    SalesQty,
    SalesAmount,
    SalesAmountRegion30Days,
    SalesAmountProduct30Days,

    -- participação da região
    CASE
        WHEN SalesAmountProduct30Days = 0 THEN 0
        ELSE SalesAmountRegion30Days / SalesAmountProduct30Days
    END AS RegionShare,

    -- classificação para o LLM
    CASE
        WHEN SalesAmountProduct30Days = 0 THEN 'No Sales'
        WHEN SalesAmountRegion30Days / SalesAmountProduct30Days > 0.4 THEN 'Strong Region'
        WHEN SalesAmountRegion30Days / SalesAmountProduct30Days < 0.1 THEN 'Growth Opportunity'
        ELSE 'Moderate'
    END AS ProductRegionPerformanceCategory

FROM combined;

-- Chave primária:
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_region_daily
ALTER COLUMN ReferenceDate SET NOT NULL;
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_region_daily
ALTER COLUMN Product_SK SET NOT NULL;
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_region_daily
ALTER COLUMN Territory_SK SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_region_daily
ADD CONSTRAINT pk_sales_insights_product_region
PRIMARY KEY (ReferenceDate, Product_SK, Territory_SK);
""")
