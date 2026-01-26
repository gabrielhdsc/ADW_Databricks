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
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, silver_selection
from ADB.Module.SilverUtils import (
    deduplicate_by_rule,
    add_column_comments,
    write_silver
)

# COMMAND ----------

current_run_id = get_run_id()

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
start_time = datetime.now()

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

full_table_name = "adventure_works_catalog.silver.dim_date"
table_name_short = "dim_date"

try:
    df_dim_date = deduplicate_by_rule(
        df_dim_date,
        partition_cols=["DateKey"],
        order_cols=[F.col("DateKey").asc()]
    )

    df_dim_date.cache()

    #Registros processados após as regras e joins (volume transformado)
    rows_processed = df_dim_date.count()

    silver_selection(
        spark=spark,
        run_id=current_run_id,
        df_input=df_dim_date,
        process_name="Criação_dim_table",
        table_name=table_name_short
    )

    write_silver(df_dim_date, "dim_date")

    #Le as linhas do estado final da tabela apos transformações
    rows_written = spark.table(full_table_name).count()

    #Registrar os metadados de auditoria
    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Criação_dim_table",
        layer="SILVER",
        table_saved = table_name_short,
        start_date = start_time,
        rows_readed_batch = rows_processed,
        rows_written_batch = rows_written
    )

    df_dim_date.unpersist()

except Exception as e:

    df_dim_date.unpersist()

    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Criar_dim_table",
        layer="SILVER",
        table_saved = table_name_short,
        start_date = start_time,
        status="FAIL",
        error_msg=str(e)
    )

    print(f"Erro em dim_date: {e}")


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

