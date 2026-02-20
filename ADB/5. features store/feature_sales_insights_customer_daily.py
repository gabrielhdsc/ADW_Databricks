# Databricks notebook source
spark.sql("""
-- Feature para o Chat LLM de Insights Acionáveis
-- Permite analisar vendas por cliente, frequencia de compra, valor médio por cliente, ranking
-- Agregando cliente e data

CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_sales_insights_customer_daily AS

WITH sales_daily AS (
    -- vendas por cliente por dia
    SELECT
        h.Customer_SK,
        dt.FullDate AS ReferenceDate,
        SUM(d.OrderQty) AS DailyQty,
        SUM(d.LineTotal) AS DailySalesAmount
    FROM adventure_works_catalog.silver.fact_sales_order_detail d
    JOIN adventure_works_catalog.silver.fact_sales_order h
        ON d.OrderID = h.OrderID
    JOIN adventure_works_catalog.silver.dim_date dt
        ON h.OrderDate_SK = dt.DateKey
    GROUP BY
        h.Customer_SK,
        dt.FullDate),

-- Última data de venda do dataset
global_max AS (
    SELECT MAX(ReferenceDate) AS MaxDate
    FROM sales_daily),

customer_date_range AS (
    SELECT
        c.Customer_SK,
        MIN(c.ReferenceDate) AS MinDate,
        LEAST(
            MAX(c.ReferenceDate) + INTERVAL 365 DAYS,
            g.MaxDate
        ) AS MaxDate
    FROM sales_daily c
    CROSS JOIN global_max g
    GROUP BY c.Customer_SK, g.MaxDate),

-- Uma linha para cada dia, mesmo sem compra
daily_customer_sales AS (
    SELECT
        c.Customer_SK,
        d.FullDate AS ReferenceDate,
        COALESCE(s.DailyQty, 0) AS DailyQty,
        COALESCE(s.DailySalesAmount, 0) AS DailySalesAmount
    FROM customer_date_range c
    JOIN adventure_works_catalog.silver.dim_date d
        ON d.FullDate BETWEEN c.MinDate AND c.MaxDate
    LEFT JOIN sales_daily s
        ON c.Customer_SK = s.Customer_SK
        AND d.FullDate = s.ReferenceDate),


features_with_windows AS (
    SELECT
        Customer_SK,
        ReferenceDate,
        DailyQty,
        DailySalesAmount,

-- Quantidade comprada nos últimos 30 dias
        SUM(DailyQty) OVER (
            PARTITION BY Customer_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
        ) AS SalesQtyLast30Days,

-- Valor gasto nos últimos 30 dias
        SUM(DailySalesAmount) OVER (
            PARTITION BY Customer_SK
            ORDER BY ReferenceDate
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW
        ) AS SalesAmountLast30Days
    FROM daily_customer_sales),

-- Lag para calcular crescimento nos últimos 30 dias
features_with_lag AS (
    SELECT *,
        LAG(SalesQtyLast30Days, 30) OVER (
            PARTITION BY Customer_SK
            ORDER BY ReferenceDate) AS SalesPrev30Days
    FROM features_with_windows),

last_purchase_calc AS (
    SELECT *,
        MAX(CASE WHEN DailyQty > 0 THEN ReferenceDate END)
        OVER (
            PARTITION BY Customer_SK
            ORDER BY ReferenceDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LastPurchaseDate
    FROM features_with_lag)

SELECT
    f.Customer_SK,
    f.ReferenceDate,
    f.DailyQty,
    f.SalesQtyLast30Days,
    COALESCE(f.SalesPrev30Days,0) AS SalesPrev30Days,

-- Permite analisar crescimento nos últimos 30 dias
-- Se = 0, não há crescimento
-- Se > 0, há crescimento
    CASE
        WHEN f.SalesPrev30Days IS NULL OR f.SalesPrev30Days = 0 THEN 0
        ELSE (f.SalesQtyLast30Days - f.SalesPrev30Days) / f.SalesPrev30Days
    END AS Growth30Days,
    f.SalesAmountLast30Days,

-- Ticket médio nos últimos 30 dias
    CASE
        WHEN f.SalesQtyLast30Days = 0 THEN 0
        ELSE f.SalesAmountLast30Days / f.SalesQtyLast30Days
    END AS AvgTicket30Days,

-- Quantidade de dias desde a última compra
    COALESCE(DATEDIFF(f.ReferenceDate, f.LastPurchaseDate),999) AS DaysSinceLastPurchase,

-- Se há mais de 60 dias desde a última compra,
    CASE
        WHEN DATEDIFF(f.ReferenceDate, f.LastPurchaseDate) >= 60 THEN 1
        ELSE 0
    END AS CustomerInactiveFlag,

-- Se há menos de 5 compras nos últimos 30 dias
    CASE
        WHEN f.SalesQtyLast30Days > 0 AND f.SalesQtyLast30Days < 5 THEN 1
        ELSE 0
    END AS LowFrequencyFlag,
    dc.CustomerType

FROM last_purchase_calc f
JOIN adventure_works_catalog.silver.dim_customer dc
    ON f.Customer_SK = dc.Customer_SK;

-- Chave primária
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_customer_daily
ALTER COLUMN Customer_SK SET NOT NULL;
ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_customer_daily
ALTER COLUMN ReferenceDate SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_sales_insights_customer_daily
ADD CONSTRAINT pk_sales_insights_customer
PRIMARY KEY (Customer_SK, ReferenceDate);
""")
