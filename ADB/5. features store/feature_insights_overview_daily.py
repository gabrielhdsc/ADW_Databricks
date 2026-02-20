# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Baseada nos totais da empresa: vendas totais, compras totais, crescimento geral, desempenho geral
-- Agregando vendas e compras por dia

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_insights_overview_daily AS

WITH sales_daily AS (
-- Vendas totais por dia
  SELECT
    ReferenceDate,
        SUM(DailyQty) AS TotalSalesQty
    FROM adventure_works_catalog.gold.feature_demand_product_daily_v1
    GROUP BY ReferenceDate),

-- Compras totais por dia separando produtos vendáveis e "materiais"
purchase_daily AS (
    SELECT
        dt.FullDate AS ReferenceDate,
        SUM(p.OrderQty) AS TotalPurchaseQty,
        SUM(
            CASE 
              WHEN dp.FinishedGoodsFlag = 1 THEN p.OrderQty 
              ELSE 0 
            END) AS PurchaseFinishedQty,
        SUM(CASE 
              WHEN dp.FinishedGoodsFlag = 0 THEN p.OrderQty 
              ELSE 0 
            END) AS PurchaseComponentQty

    FROM adventure_works_catalog.silver.fact_purchases p
    JOIN adventure_works_catalog.silver.dim_date dt
        ON p.OrderDate_SK = dt.DateKey
    JOIN adventure_works_catalog.silver.dim_product dp
        ON p.Product_SK = dp.Product_SK
    GROUP BY dt.FullDate),

date_limits AS ( SELECT
        MIN(ReferenceDate) AS MinDate,
        MAX(ReferenceDate) AS MaxDate
    FROM sales_daily),

daily_base AS (SELECT
        d.FullDate AS ReferenceDate,
        COALESCE(s.TotalSalesQty, 0) AS TotalSalesQty,
        COALESCE(p.TotalPurchaseQty, 0) AS TotalPurchaseQty,
        COALESCE(p.PurchaseFinishedQty, 0) AS PurchaseFinishedQty,
        COALESCE(p.PurchaseComponentQty, 0) AS PurchaseComponentQty
    FROM adventure_works_catalog.silver.dim_date d
    CROSS JOIN date_limits l
    LEFT JOIN sales_daily s
        ON d.FullDate = s.ReferenceDate
    LEFT JOIN purchase_daily p
        ON d.FullDate = p.ReferenceDate
    WHERE d.FullDate BETWEEN l.MinDate AND l.MaxDate),

features_with_windows AS (
    SELECT
        ReferenceDate,
        TotalSalesQty,
        TotalPurchaseQty,
        PurchaseFinishedQty,
        PurchaseComponentQty,

        -- vendas últimos 30 dias
        SUM(TotalSalesQty) OVER (
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
        ) AS SalesLast30Days,

        -- compras de produtos finais últimos 30 dias
        SUM(PurchaseFinishedQty) OVER (
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) 
            AS PurchasesFinishedLast30Days
    FROM daily_base),

features_with_lag AS (
    SELECT *, LAG(SalesLast30Days, 30) OVER (
          ORDER BY ReferenceDate) AS SalesPrev30Days
    FROM features_with_windows)

SELECT
    ReferenceDate,
    TotalSalesQty,
    TotalPurchaseQty,
    PurchaseFinishedQty,
    PurchaseComponentQty,
    SalesLast30Days,
    COALESCE(SalesPrev30Days, 0) AS SalesPrev30Days,
    PurchasesFinishedLast30Days,

    -- crescimento geral
    CASE
        WHEN SalesPrev30Days IS NULL OR SalesPrev30Days = 0 THEN 0
        ELSE (SalesLast30Days - SalesPrev30Days) / SalesPrev30Days
    END AS Growth30Days,

    -- Flag para analisar compras de produtos finais em relação a vendas
    -- Se o ratio for 1.0 - comprando exatamente o que vende
    -- Se > 1 - comprando mais do que vende
    -- Se < 1 - comprando menos do que vende
    CASE
        WHEN SalesLast30Days = 0 THEN 0
        ELSE PurchasesFinishedLast30Days / SalesLast30Days
    END AS PurchaseSalesRatio,

    -- Flag para analisar risco de sobreposição de estoque
    CASE
        WHEN (PurchasesFinishedLast30Days / NULLIF(SalesLast30Days,0)) > 1.2 THEN 1
        ELSE 0
    END AS OverstockRiskFlag,

    -- Flag para analisar risco de falta de estoque
    CASE
        WHEN (PurchasesFinishedLast30Days / NULLIF(SalesLast30Days,0)) < 0.8 THEN 1
        ELSE 0
    END AS StockoutRiskFlag

FROM features_with_lag;

-- Chave primária 
ALTER TABLE adventure_works_catalog.gold.feature_insights_overview_daily
ALTER COLUMN ReferenceDate SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_insights_overview_daily
ADD CONSTRAINT pk_feature_insights_overview
PRIMARY KEY (ReferenceDate);
""")
