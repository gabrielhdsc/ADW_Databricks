# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Feita para análises de compras: estamos comprando menos do que vendemos? Algum produto sem reposição recente? Excesso?
-- Agregando por dia e produto

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_inventory_insights_product_daily AS

WITH sales_daily AS (
    -- Vendas por produto e dia
    SELECT
        Product_SK,
        ReferenceDate,
        DailyQty
    FROM adventure_works_catalog.gold.feature_demand_product_daily_v1),

purchase_daily AS (
    -- Compras por produto e dia (apenas produtos vendáveis)
    SELECT
        p.Product_SK,
        dt.FullDate AS ReferenceDate,
        SUM(p.OrderQty) AS PurchaseQty
    FROM adventure_works_catalog.silver.fact_purchases p
    JOIN adventure_works_catalog.silver.dim_date dt
        ON p.OrderDate_SK = dt.DateKey
    JOIN adventure_works_catalog.silver.dim_product dp
        ON p.Product_SK = dp.Product_SK
    WHERE dp.FinishedGoodsFlag = 1
    GROUP BY
        p.Product_SK,
        dt.FullDate),

base AS (
    -- Junta vendas e compras
    SELECT
        s.Product_SK,
        s.ReferenceDate,
        s.DailyQty AS SalesQty,
        COALESCE(p.PurchaseQty, 0) AS PurchaseQty
    FROM sales_daily s
    LEFT JOIN purchase_daily p
        ON s.Product_SK = p.Product_SK
        AND s.ReferenceDate = p.ReferenceDate),

windows AS (SELECT
        Product_SK,
        ReferenceDate,
        SalesQty,
        PurchaseQty,

        -- Vendas últimos 30 dias
        SUM(SalesQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) 
            AS SalesLast30Days,

        -- Compras últimos 30 dias
        SUM(PurchaseQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) 
            AS PurchasesLast30Days
    FROM base),

recency AS (SELECT*,
        -- última compra
        MAX(CASE WHEN PurchaseQty > 0 THEN ReferenceDate END) 
            OVER (PARTITION BY Product_SK
            ORDER BY ReferenceDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
             AS LastPurchaseDate
    FROM windows)

SELECT
    Product_SK,
    ReferenceDate,
    SalesLast30Days,
    PurchasesLast30Days,

    -- Relação compras vs vendas
    CASE
        WHEN SalesLast30Days = 0 THEN 0
        ELSE PurchasesLast30Days / SalesLast30Days
    END AS PurchaseSalesRatio,

    -- Dias sem compra
    COALESCE(DATEDIFF(ReferenceDate, LastPurchaseDate), 999) AS DaysSinceLastPurchase,

    -- Flags
    CASE
        WHEN PurchasesLast30Days / NULLIF(SalesLast30Days,0) < 0.8 THEN 1
        ELSE 0
    END AS StockoutRiskFlag,

    CASE
        WHEN PurchasesLast30Days / NULLIF(SalesLast30Days,0) > 1.2 THEN 1
        ELSE 0
    END AS OverstockRiskFlag,

    CASE
        WHEN DATEDIFF(ReferenceDate, LastPurchaseDate) >= 30 THEN 1
        ELSE 0
    END AS NoRecentPurchaseFlag
FROM recency;

-- Chave primária
ALTER TABLE adventure_works_catalog.gold.feature_inventory_insights_product_daily
ALTER COLUMN Product_SK SET NOT NULL;
ALTER TABLE adventure_works_catalog.gold.feature_inventory_insights_product_daily
ALTER COLUMN ReferenceDate SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_inventory_insights_product_daily
ADD CONSTRAINT pk_inventory_insights_product_daily
PRIMARY KEY (Product_SK, ReferenceDate);
""")
