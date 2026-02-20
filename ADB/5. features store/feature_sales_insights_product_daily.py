# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Vendas por produto - queda, tendência, produtos parados, top produtos
-- Agregando produto e data

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_sales_insights_product_daily AS

WITH base AS (
SELECT
    Product_SK,
    ReferenceDate,
    DailyQty,
    SalesLast7Days,
    SalesLast30Days,
    DaysSinceLastSale,

    LAG(SalesLast30Days, 30) OVER (
        PARTITION BY Product_SK
        ORDER BY ReferenceDate)
        AS SalesPrev30Days,

    LAG(SalesLast7Days, 7) OVER (
        PARTITION BY Product_SK
        ORDER BY ReferenceDate)
        AS SalesPrev7Days

FROM adventure_works_catalog.gold.feature_demand_product_daily_v1),
growth_calc AS (
    SELECT*,
    CASE 
        WHEN SalesPrev30Days IS NULL OR SalesPrev30Days = 0 THEN 0
        ELSE (SalesLast30Days - SalesPrev30Days) / SalesPrev30Days
    END AS Growth30Days,
    COALESCE(SalesLast7Days - SalesPrev7Days, 0) AS Trend7
FROM base),

ranking_calc AS (
SELECT*,
    RANK() OVER (
        PARTITION BY ReferenceDate
        ORDER BY SalesLast30Days DESC) AS Ranking30Days
FROM growth_calc)

SELECT
    Product_SK,
    ReferenceDate,
    DailyQty,
    SalesLast7Days,
    SalesLast30Days,
    COALESCE(SalesPrev30Days,0) AS SalesPrev30Days,
    Growth30Days,
    Trend7,
    DaysSinceLastSale,
    Ranking30Days,

-- Para identificar se o crescimento nos últimos 30 dias é alto ou baixo
-- Regra: se o crescimento for maior ou igual a 0.5, então HighGrowthFlag = 1
-- se o crescimento for menor ou igual a -0.5, então SharpDropFlag = 1
    CASE 
        WHEN Growth30Days <= -0.5 THEN 1
        ELSE 0
    END AS SharpDropFlag,

-- Para identificar oportunidades de vendas
-- Regra: se o crescimento for maior ou igual a 0.5,
-- então HighGrowthFlag = 1
    CASE 
        WHEN Growth30Days >= 0.5 THEN 1
        ELSE 0
    END AS HighGrowthFlag,

-- Para identificar se há falta de vendas recentes, produtos parados
-- Regra: se o número de dias desde a última venda for maior ou igual a 30,
-- então NoSalesRecentFlag = 1. Se vendeu, então NoSalesRecentFlag = 0
    CASE 
        WHEN DaysSinceLastSale >= 30 THEN 1
        ELSE 0
    END AS NoSalesRecentFlag,

-- Para identificar se o crescimento é positivo, negativo ou neutro
-- Regra: se o crescimento for maior que 0.1, então TrendDirection = 1
-- se o crescimento for menor que -0.1, então TrendDirection = -1
-- se o crescimento for entre -0.1 e 0.1, então TrendDirection = 0
    CASE
        WHEN Growth30Days > 0.1 THEN 1
        WHEN Growth30Days < -0.1 THEN -1
        ELSE 0
    END AS TrendDirection,

-- Para identificar o nível de atividade do produto
-- Regra: se o número de vendas nos últimos 30 dias for 0, então ActivityLevel = 0
-- se o número de vendas nos últimos 30 dias for menor que 10, então ActivityLevel = 1
-- se o número de vendas nos últimos 30 dias for maior ou igual a 10, então ActivityLevel = 2
    CASE
        WHEN SalesLast30Days = 0 THEN 0
        WHEN SalesLast30Days < 10 THEN 1
        ELSE 2
    END AS ActivityLevel

FROM ranking_calc;

-- Chave primária
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_daily
ALTER COLUMN Product_SK SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_daily
ALTER COLUMN ReferenceDate SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_product_daily
ADD CONSTRAINT pk_sales_insights
PRIMARY KEY (Product_SK, ReferenceDate);
""")
