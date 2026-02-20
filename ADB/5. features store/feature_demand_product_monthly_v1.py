# Databricks notebook source
spark.sql("""
-- Feature mensal para o Modelo de Previsão de Demanda - V1
-- Agregando produtos por mês
-- Janelas: 1mês, 3meses, 6meses, 12meses
-- Permite ao modelo aprender padrões por período
    
CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_demand_product_monthly_v1 AS

WITH monthly_sales AS (
    -- Agrega vendas por produto e mês
    SELECT
        d.Product_SK,
        DATE_TRUNC('MONTH', dt.FullDate) AS ReferenceMonth,
        SUM(d.OrderQty) AS MonthlyQty
    FROM adventure_works_catalog.silver.fact_sales_order_detail d
    JOIN adventure_works_catalog.silver.fact_sales_order h
        ON d.OrderID = h.OrderID
    JOIN adventure_works_catalog.silver.dim_date dt
        ON h.OrderDate_SK = dt.DateKey
    GROUP BY
        d.Product_SK,
        DATE_TRUNC('MONTH', dt.FullDate)),

product_month_range AS (
    -- Garante meses sem venda (zero)
    SELECT
        Product_SK,
        MIN(ReferenceMonth) AS MinMonth,
        MAX(ReferenceMonth) AS MaxMonth
    FROM monthly_sales
    GROUP BY Product_SK),

calendar_months AS (
    SELECT DISTINCT
        DATE_TRUNC('MONTH', FullDate) AS ReferenceMonth
    FROM adventure_works_catalog.silver.dim_date),

monthly_product_sales AS (
    SELECT
        p.Product_SK,
        c.ReferenceMonth,
        COALESCE(m.MonthlyQty, 0) AS MonthlyQty
    FROM product_month_range p
    JOIN calendar_months c
        ON c.ReferenceMonth BETWEEN p.MinMonth AND p.MaxMonth
    LEFT JOIN monthly_sales m
        ON p.Product_SK = m.Product_SK
        AND c.ReferenceMonth = m.ReferenceMonth),

features_with_windows AS (
    SELECT
        Product_SK,
        ReferenceMonth,
        MonthlyQty,

        -- Janelas 
        -- Trimestral
        SUM(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
            AS SalesLast3Months,

        -- Semestral
        SUM(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) 
            AS SalesLast6Months,

        -- Anual
        SUM(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) 
            AS SalesLast12Months,

        -- Média das janelas
        -- Trimestral
        AVG(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
            AS AvgSalesLast3Months,

        -- Semestral
        AVG(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
            AS AvgSalesLast6Months,

        -- Anual
        AVG(MonthlyQty) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) 
            AS AvgSalesLast12Months
    FROM monthly_product_sales),

-- Último mês em que houve venda
features_with_recency AS (
    SELECT *,
        MAX(
            CASE WHEN MonthlyQty > 0 THEN ReferenceMonth END) 
            OVER 
            (PARTITION BY Product_SK
            ORDER BY ReferenceMonth
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS LastSaleMonth
    FROM features_with_windows),

--Lags
features_with_lags AS (
    SELECT *,
    -- Vendas do mês anterior
        LAG(MonthlyQty, 1) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth)
            AS Lag1Month,

    -- Vendas do trimestre anterior
        LAG(MonthlyQty, 3) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth) 
            AS Lag3Month,
    -- Vendas do ano anterior
        LAG(MonthlyQty, 12) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceMonth)
            AS Lag12Month
    FROM features_with_recency)

SELECT
    Product_SK,
    ReferenceMonth,
    MonthlyQty,
    SalesLast3Months,
    SalesLast6Months,
    SalesLast12Months,
    AvgSalesLast3Months,
    AvgSalesLast6Months,
    AvgSalesLast12Months,

    COALESCE(DATEDIFF(ReferenceMonth, LastSaleMonth)/30, 999)
        AS MonthsSinceLastSale,
    COALESCE(Lag1Month, 0) AS Lag1Month,
    COALESCE(Lag3Month, 0) AS Lag3Month,
    COALESCE(Lag12Month, 0) AS Lag12Month,

    -- Tendência mensal
    MonthlyQty - COALESCE(Lag3Month, 0) AS Trend3Months,

    -- Calendário
    MONTH(ReferenceMonth) AS MonthNumber,
    YEAR(ReferenceMonth) AS YearNumber
FROM features_with_lags;

-- Chave Primária
ALTER TABLE adventure_works_catalog.gold.feature_demand_product_monthly_v1
ALTER COLUMN Product_SK SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_demand_product_monthly_v1
ALTER COLUMN ReferenceMonth SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_demand_product_monthly_v1
ADD CONSTRAINT pk_feature_demand_monthly
PRIMARY KEY (Product_SK, ReferenceMonth);
""")
