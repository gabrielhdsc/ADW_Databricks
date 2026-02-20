# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Permite análises como: "Quais são as regiões com maior crescimento nos últimos 30 dias?" ou "Quais são as regiões com maior queda nos últimos 30 dias?"
-- Agregando as vendas por dia e região

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_sales_insights_region_daily AS

-- Agrega as vendas por dia e região
WITH sales_daily AS (
    SELECT
        dt.FullDate AS ReferenceDate,
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
        t.Territory_SK,
        t.TerritoryName),

-- Define o período da análise
date_limits AS (
    SELECT
        MIN(ReferenceDate) AS MinDate,
        MAX(ReferenceDate) AS MaxDate
    FROM sales_daily),

-- Insere dias onde não houveram vendas, para facilitar o cálculo dos períodos
daily_base AS (
    SELECT
        d.FullDate AS ReferenceDate,
        t.Territory_SK,
        t.TerritoryName,
        COALESCE(s.SalesQty, 0) AS SalesQty,
        COALESCE(s.SalesAmount, 0) AS SalesAmount
    FROM adventure_works_catalog.silver.dim_date d
    CROSS JOIN (
        SELECT *
        FROM adventure_works_catalog.silver.dim_territory
        WHERE Territory_SK <> -1
    ) t
    CROSS JOIN date_limits l
    LEFT JOIN sales_daily s
        ON d.FullDate = s.ReferenceDate
        AND t.Territory_SK = s.Territory_SK
    WHERE d.FullDate BETWEEN l.MinDate AND l.MaxDate),

-- Calcula o desempenho nos últimos 30 dias
features_with_windows AS (
  SELECT
      ReferenceDate,
      Territory_SK,
      TerritoryName,
      SalesQty,
      SalesAmount,

-- Volume vendido nos últimos 30 dias
      SUM(SalesQty) OVER (
        PARTITION BY Territory_SK
        ORDER BY ReferenceDate
        RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) AS SalesQtyLast30Days,

-- Quanto a região vendeu nos últimos 30 dias
        SUM(SalesAmount) OVER (
          PARTITION BY Territory_SK
          ORDER BY ReferenceDate
          RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) AS SalesAmountLast30Days
          FROM daily_base),

-- Período anterior ao período atual
features_with_lag AS (
  SELECT *,
    LAG(SalesAmountLast30Days, 30) OVER (
      PARTITION BY Territory_SK
      ORDER BY ReferenceDate) AS SalesAmountPrev30Days
    FROM features_with_windows)

SELECT
    ReferenceDate,
    Territory_SK,
    TerritoryName,
    SalesQty,
    SalesAmount,
    SalesQtyLast30Days,
    SalesAmountLast30Days,
    COALESCE(SalesAmountPrev30Days, 0) AS SalesAmountPrev30Days, -- Transformando NULL em 0

-- Insights
-- Região está crescendo ou diminuindo nos últimos 30 dias
    CASE
        WHEN SalesAmountPrev30Days = 0 THEN 0
        ELSE (SalesAmountLast30Days - SalesAmountPrev30Days) / SalesAmountPrev30Days
    END AS Growth30Days,

-- Aponta se há alta ou queda significativa nos últimos 30 dias
    CASE
        WHEN SalesAmountPrev30Days = 0 THEN 0
        WHEN (SalesAmountLast30Days - SalesAmountPrev30Days) / SalesAmountPrev30Days > 0.1 THEN 1
        WHEN (SalesAmountLast30Days - SalesAmountPrev30Days) / SalesAmountPrev30Days < -0.1 THEN -1
        ELSE 0
    END AS TrendDirection,

-- Região acelerou nos últimos 30 dias
    CASE
        WHEN (SalesAmountLast30Days - SalesAmountPrev30Days) / NULLIF(SalesAmountPrev30Days,0) >= 0.3 THEN 1
        ELSE 0
    END AS HighGrowthFlag,

-- Região com problema de queda nos últimos 30 dias
    CASE
        WHEN (SalesAmountLast30Days - SalesAmountPrev30Days) / NULLIF(SalesAmountPrev30Days,0) <= -0.3 THEN 1
        ELSE 0
    END AS SharpDropFlag
FROM features_with_lag;

-- Chave primária
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_region_daily
ALTER COLUMN ReferenceDate SET NOT NULL;
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_region_daily
ALTER COLUMN Territory_SK SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_region_daily
ADD CONSTRAINT pk_sales_insights_region
PRIMARY KEY (ReferenceDate, Territory_SK);
""")
