# Databricks notebook source
spark.sql("""
-- View para Modelo de Previsão de Demanda - V1
-- Agregando vendas por dia
-- Janelas: 7dias, 30dias, 90dias, 365dias
-- Permite ao modelo aprender padrões por período
    
CREATE OR REPLACE TABLE adventure_works_catalog.gold.feature_demand_product_daily_v1 AS
WITH daily_sales AS (
    -- Vendas reais agregadas por produto e dia
    SELECT
        d.Product_SK,
        dt.FullDate AS ReferenceDate,
        SUM(d.OrderQty) AS DailyQty
    FROM adventure_works_catalog.silver.fact_sales_order_detail d
    JOIN adventure_works_catalog.silver.fact_sales_order h
        ON d.OrderID = h.OrderID
    JOIN adventure_works_catalog.silver.dim_date dt
        ON h.OrderDate_SK = dt.DateKey
    GROUP BY
        d.Product_SK,
        dt.FullDate
),

product_date_range AS (
    -- Intervalo de datas em que cada produto teve movimentação
    SELECT
        Product_SK,
        MIN(ReferenceDate) AS MinDate,
        MAX(ReferenceDate) AS MaxDate
    FROM daily_sales
    GROUP BY Product_SK
),

daily_product_sales AS (
    -- Dias sem venda = ausência = venda zero
    SELECT
        p.Product_SK,
        d.FullDate AS ReferenceDate,
        COALESCE(s.DailyQty, 0) AS DailyQty
    FROM product_date_range p
    JOIN adventure_works_catalog.silver.dim_date d
        ON d.FullDate BETWEEN p.MinDate AND p.MaxDate
    LEFT JOIN daily_sales s
        ON p.Product_SK = s.Product_SK
        AND d.FullDate = s.ReferenceDate),

features_with_windows AS (
--Soma e média de unidades vendidas nas janelas
  SELECT
    Product_SK,
    ReferenceDate,
    DailyQty,

    -- Soma nas janelas
    -- Curto prazo
    SUM(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW)
      AS SalesLast7Days,

    -- Mensal
    SUM(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW)
      AS SalesLast30Days,

    -- Trimestral
    SUM(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 89 DAYS PRECEDING AND CURRENT ROW)
      AS SalesLast90Days,

    -- Anual
    SUM(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 364 DAYS PRECEDING AND CURRENT ROW)
      AS SalesLast365Days,

    -- Média nas janelas
    -- Curto prazo
    AVG(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW) 
      AS AvgSalesLast7Days,

    -- Mensal
    AVG(DailyQty) OVER (
      PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW) 
      AS AvgSalesLast30Days,

    -- Trimestral
    AVG(DailyQty) OVER (
        PARTITION BY Product_SK
        ORDER BY ReferenceDate
        RANGE BETWEEN INTERVAL 89 DAYS PRECEDING AND CURRENT ROW)
        AS AvgSalesLast90Days,

    -- Anual
    AVG(DailyQty) OVER (
        PARTITION BY Product_SK
        ORDER BY ReferenceDate
        RANGE BETWEEN INTERVAL 364 DAYS PRECEDING AND CURRENT ROW)
        AS AvgSalesLast365Days,

    -- Frequência de vendas (intermitência)
    SUM(
      CASE WHEN DailyQty > 0 THEN 1 ELSE 0 END)
      OVER
      (PARTITION BY Product_SK
      ORDER BY ReferenceDate
      RANGE BETWEEN INTERVAL 29 DAYS PRECEDING AND CURRENT ROW)
      / 30.0 AS Intermittency30Days
    FROM daily_product_sales),

features_with_recency AS (
    -- Última data em que houve venda
    SELECT *,
        MAX( CASE WHEN DailyQty > 0 THEN ReferenceDate
            END)
            OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS LastSaleDate FROM features_with_windows),

features_with_lags AS (
    SELECT *,
        -- Vendas do dia anterior
        LAG(DailyQty, 1) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate)
             AS Lag1,

        -- Vendas da semana anterior
        LAG(DailyQty, 7) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate)
            AS Lag7,

        -- Vendas do mês anterior 
        LAG(DailyQty, 30) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate)
            AS Lag30,

        -- Vendas de 3 meses atrás
        LAG(DailyQty, 90) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate)
            AS Lag90,

        -- Vendas de 1 ano atrás
        LAG(DailyQty, 365) OVER (
            PARTITION BY Product_SK
            ORDER BY ReferenceDate)
            AS Lag365
    FROM features_with_recency)

SELECT
    f.Product_SK,
    f.ReferenceDate,
    f.DailyQty,
    f.SalesLast7Days,
    f.SalesLast30Days,
    f.SalesLast90Days,
    f.SalesLast365Days,
    f.AvgSalesLast7Days,
    f.AvgSalesLast30Days,
    f.AvgSalesLast90Days,
    f.AvgSalesLast365Days,
    f.Intermittency30Days,

    -- Dias desde a última venda
    -- Produtos com demanda "intermitente", como 1 compra unica no mês
    COALESCE(DATEDIFF(f.ReferenceDate, f.LastSaleDate),999) 
    AS DaysSinceLastSale,

    -- Nos primeiros dias do produto, nao existe historico, substituição de Null por 0
    COALESCE(f.Lag1, 0) AS Lag1,
    COALESCE(f.Lag7, 0) AS Lag7,
    COALESCE(f.Lag30, 0) AS Lag30,
    COALESCE(f.Lag90, 0) AS Lag90,
    COALESCE(f.Lag365, 0) AS Lag365,

    -- Tendência semanal
    f.DailyQty - COALESCE(f.Lag7, 0) AS Trend7,

    -- Features de calendário
    d.DayNumberOfWeek,
    d.IsWeekend,
    d.MonthNumberOfYear,
    d.DayNumberOfMonth

FROM features_with_lags f
JOIN adventure_works_catalog.silver.dim_date d
    ON f.ReferenceDate = d.FullDate;


-- Chave primária
ALTER TABLE adventure_works_catalog.gold.feature_demand_product_daily_v1
ALTER COLUMN Product_SK SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_demand_product_daily_v1
ALTER COLUMN ReferenceDate SET NOT NULL;

ALTER TABLE adventure_works_catalog.gold.feature_demand_product_daily_v1
ADD CONSTRAINT pk_feature_demand
PRIMARY KEY (Product_SK, ReferenceDate);
""")
