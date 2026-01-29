# Databricks notebook source
# MAGIC %md
# MAGIC # Dimensional Model – Dimensions
# MAGIC
# MAGIC Notebook responsável pela criação das **dimensões analíticas** do modelo dimensional (Adventure Works)
# MAGIC
# MAGIC Inclui dimensões com **chaves substitutas** e documentação no Unity Catalog.

# COMMAND ----------

import sys
sys.path.append("..")

# COMMAND ----------

#Imports e setup

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import xxhash64
from datetime import datetime
from ADB.Module.SilverUtils import (
    deduplicate_by_rule,
    add_column_comments,
    write_silver,
)

# COMMAND ----------

# Leitura das tabelas Silver (clean)
df_customer        = spark.table("adventure_works_catalog.silver.clean_sales_customer")
df_sales_territory = spark.table("adventure_works_catalog.silver.clean_sales_territory")
df_sales           = spark.table("adventure_works_catalog.silver.clean_sales_order_header")
df_person          = spark.table("adventure_works_catalog.silver.clean_person")
df_store           = spark.table("adventure_works_catalog.silver.clean_sales_store")
df_inventory       = spark.table("adventure_works_catalog.silver.clean_production_inventory")
df_person_address  = spark.table("adventure_works_catalog.silver.clean_person_address")
df_vendor          = spark.table("adventure_works_catalog.silver.clean_purchasing_vendor")
df_product         = spark.table("adventure_works_catalog.silver.clean_production")
df_currency        = spark.table("adventure_works_catalog.silver.clean_sales_currency")
df_currency_rate   = spark.table("adventure_works_catalog.silver.clean_sales_currency_rate")

# COMMAND ----------

# =====================================================
# DIM_DATE - SILVER
# =====================================================
# Granularidade: 1 linha por dia do calendário

start_date = "2010-01-01"
end_date   = "2025-12-31"

df_date = (
    spark
    .range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date)),
                F.to_date(F.lit(end_date)),
                F.expr("interval 1 day")
            )
        ).alias("FullDate")
    )
)

df_dim_date = (
    df_date
    .select(
        F.date_format("FullDate", "yyyyMMdd").cast("int").alias("DateKey"),
        F.col("FullDate"),
        F.dayofweek("FullDate").alias("DayNumberOfWeek"),
        F.date_format("FullDate", "EEEE").alias("DayNameOfWeek"),
        F.dayofmonth("FullDate").alias("DayNumberOfMonth"),
        F.month("FullDate").alias("MonthNumberOfYear"),
        F.date_format("FullDate", "MMMM").alias("MonthName"),
        F.quarter("FullDate").alias("CalendarQuarter"),
        F.year("FullDate").alias("CalendarYear"),
        F.when(
            F.dayofweek("FullDate").isin([1, 7]),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("IsWeekend")
    )
)

# Garantia de Integridade
df_dim_date = (
    df_dim_date
    .filter(F.col("DateKey").isNotNull())
)

df_dim_date = deduplicate_by_rule(
    df_dim_date,
    partition_cols=["DateKey"],
    order_cols=[F.col("DateKey").asc()]
)

write_silver(df_dim_date, "dim_date")

# Descrição das colunas
dim_date_columns = {
    "DateKey": "Chave substituta da data no formato YYYYMMDD",
    "FullDate": "Data completa no formato calendário",
    "DayNumberOfWeek": "Número do dia da semana (1 = domingo, 7 = sábado)",
    "DayNameOfWeek": "Nome do dia da semana",
    "DayNumberOfMonth": "Número do dia no mês",
    "MonthNumberOfYear": "Número do mês no ano",
    "MonthName": "Nome do mês",
    "CalendarQuarter": "Trimestre do ano",
    "CalendarYear": "Ano calendário",
    "IsWeekend": "Indicador de final de semana (1 = sim, 0 = não)"
}

# Adicionando os comentários no Unity Catalog
add_column_comments(
    catalog="adventure_works_catalog",
    schema="silver",
    table="dim_date",
    columns_dict=dim_date_columns
)

