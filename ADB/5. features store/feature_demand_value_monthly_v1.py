# Databricks notebook source
spark.sql("""
-- Feature mensal para o Modelo de Previsão de Demanda - V1
-- Agregando *valor da venda* por mês (nível geral)
-- Janelas: 1mês, 3meses, 6meses, 12meses
-- Permite ao modelo aprender padrões por período

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_demand_value_monthly_v1 AS

WITH monthly_sales AS (
    -- Agrega vendas por mês
    SELECT
    CAST(DATE_TRUNC('MONTH', dt.FullDate) AS DATE) AS ReferenceMonth,
    SUM(d.LineTotal) AS MonthlySalesAmount
    FROM adventure_works_catalog.silver.fact_sales_order_detail d
    JOIN adventure_works_catalog.silver.fact_sales_order h
        ON d.OrderID = h.OrderID
    JOIN adventure_works_catalog.silver.dim_date dt
        ON h.OrderDate_SK = dt.DateKey
    GROUP BY
    CAST(DATE_TRUNC('MONTH', dt.FullDate) AS DATE)),


-- Mantém os meses lineares
-- Primeiro e último mês com venda
month_range AS (
    SELECT
        MIN(ReferenceMonth) AS MinMonth,
        MAX(ReferenceMonth) AS MaxMonth
    FROM monthly_sales),

calendar_months AS (
    -- Gera todos os meses do calendário disponíveis
    SELECT DISTINCT
        CAST(DATE_TRUNC('MONTH', FullDate) AS DATE) AS ReferenceMonth
    FROM adventure_works_catalog.silver.dim_date),

monthly_sales_filled AS (
    -- Garante que exista linha para cada mês, preenchendo com 0 onde não houve venda
    SELECT
        c.ReferenceMonth,
        COALESCE(m.MonthlySalesAmount, 0) AS MonthlySalesAmount
    FROM month_range r
    JOIN calendar_months c
        ON c.ReferenceMonth BETWEEN r.MinMonth AND r.MaxMonth
    LEFT JOIN monthly_sales m
        ON c.ReferenceMonth = m.ReferenceMonth),

features_with_windows AS (
    -- Calcula janelas móveis de soma e média para 3, 6 e 12 meses
    SELECT
        ReferenceMonth,
        MonthlySalesAmount,

        --Janelas
        --Trimestral
        SUM(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
            AS SalesAmountLast3Months,
        
        --Semestral
        SUM(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
            AS SalesAmountLast6Months,
        
        --Anual
        SUM(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) 
            AS SalesAmountLast12Months,

        --Médias
        --Trimestral
        AVG(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
            AS AvgSalesAmountLast3Months,

        --Semestral
        AVG(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
            AS AvgSalesAmountLast6Months,

        --Anual
        AVG(MonthlySalesAmount) OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) 
            AS AvgSalesAmountLast12Months
    FROM monthly_sales_filled),

features_with_recency AS (
    -- Calcula o mês da última venda até o mês corrente
    SELECT *,
        MAX(
            CASE WHEN MonthlySalesAmount > 0 THEN ReferenceMonth END) 
            OVER (
            ORDER BY ReferenceMonth
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS LastSaleMonth
    FROM features_with_windows),

features_with_lags AS (
    -- Calcula lags de 1, 3 e 12 meses para o valor de venda mensal
    SELECT *,
        LAG(MonthlySalesAmount, 1) OVER (
            ORDER BY ReferenceMonth) 
            AS LagAmount1Month,

        LAG(MonthlySalesAmount, 3) OVER (
            ORDER BY ReferenceMonth) 
            AS LagAmount3Month,

        LAG(MonthlySalesAmount, 12) OVER (
            ORDER BY ReferenceMonth) 
            AS LagAmount12Month
    FROM features_with_recency)

SELECT
    ReferenceMonth,
    MonthlySalesAmount,
    SalesAmountLast3Months,
    SalesAmountLast6Months,
    SalesAmountLast12Months,
    AvgSalesAmountLast3Months,
    AvgSalesAmountLast6Months,
    AvgSalesAmountLast12Months,

    -- Calcula meses desde a última venda (ou 999 se nunca vendeu)
    COALESCE(FLOOR(MONTHS_BETWEEN(ReferenceMonth, LastSaleMonth)), 999) AS MonthsSinceLastSaleAmount,
    -- Lags preenchidos com 0 se nulos
    COALESCE(LagAmount1Month, 0) AS LagAmount1Month,
    COALESCE(LagAmount3Month, 0) AS LagAmount3Month,
    COALESCE(LagAmount12Month, 0) AS LagAmount12Month,
    -- Tendência: diferença entre mês atual e 3 meses atrás
    MonthlySalesAmount - COALESCE(LagAmount3Month, 0) AS TrendAmount3Months,
    -- Extrai número do mês e ano
    MONTH(ReferenceMonth) AS MonthNumber,
    YEAR(ReferenceMonth) AS YearNumber
FROM features_with_lags;
""")

spark.sql("""
ALTER TABLE adventure_works_catalog.gold.feature_demand_value_monthly_v1
ALTER COLUMN ReferenceMonth SET NOT NULL
""")

spark.sql("""
ALTER TABLE adventure_works_catalog.gold.feature_demand_value_monthly_v1
ADD CONSTRAINT pk_feature_demand_value_monthly
PRIMARY KEY (ReferenceMonth)
""")
