# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Sales Order
# MAGIC Fato de pedidos de venda (nível de pedido / header) na camada Silver, integrada às dimensões analíticas para análise de valores consolidados do pedido, como subtotal, frete e impostos.

# COMMAND ----------

import sys
sys.path.append("..")

# COMMAND ----------

# Imports
from datetime import datetime
from pyspark.sql import functions as F
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, silver_selection
from ADB.Module.SilverUtils import (
    add_column_comments,
    deduplicate_by_rule
)

# COMMAND ----------

current_run_id = get_run_id()

# Leitura das tabelas Silver
df_clean_sales_order_header = spark.table("adventure_works_catalog.silver.clean_sales_order_header")
 
# Leitura das Dimensões
df_dim_customer = spark.table("adventure_works_catalog.silver.dim_customer")
df_dim_date = spark.table("adventure_works_catalog.silver.dim_date")
df_dim_currency = spark.table("adventure_works_catalog.silver.dim_currency")
df_dim_location = spark.table("adventure_works_catalog.silver.dim_location")
df_dim_territory = spark.table("adventure_works_catalog.silver.dim_territory")


# COMMAND ----------

start_time = datetime.now()

# Criando a tabela
spark.sql("""
CREATE TABLE IF NOT EXISTS adventure_works_catalog.silver.fact_sales_order (
    OrderID INT,
    ShipToAddressID INT,
 
    Customer_SK INT,
    OrderDate_SK INT,
    ShipDate_SK INT,
    Currency_SK INT,
    Location_SK INT,
    Territory_SK INT,
 
    SubTotal DECIMAL(19,4),
    ShippingCost DECIMAL(19,4),
    TaxAmt DECIMAL(19,4)
);
""")
 
# Preparação dos dados e chaves
df_fact_sales_order_base = (
    df_clean_sales_order_header.alias("sh")
 
    # joins
    .join(df_dim_customer.alias("c"),
          F.col("sh.CustomerID") == F.col("c.CustomerID"), "left")
 
    .join(df_dim_date.alias("od"),
          F.to_date(F.col("sh.OrderDate")) == F.col("od.FullDate"), "left")
 
    .join(df_dim_date.alias("sd"),
          F.to_date(F.col("sh.ShipDate")) == F.col("sd.FullDate"), "left")
 
    .join(df_dim_location.alias("l"),
          F.col("sh.ShipToAddressID") == F.col("l.AddressID"), "left")
 
    .join(df_dim_currency.alias("cur"),
          F.col("sh.CurrencyRateID") == F.col("cur.CurrencyRateID"), "left")
    
    .join(df_dim_territory.alias("t"),
        F.col("sh.TerritoryID") == F.col("t.TerritoryID"), "left")

 
    # Select Final
    .select(
        F.col("sh.SalesOrderID").alias("OrderID"),
        F.col("sh.ShipToAddressID"),
 
        F.coalesce(F.col("c.Customer_SK"), F.lit(-1)).alias("Customer_SK"),
        F.coalesce(F.col("od.DateKey"), F.lit(-1)).alias("OrderDate_SK"),
        F.coalesce(F.col("sd.DateKey"), F.lit(-1)).alias("ShipDate_SK"),
        F.coalesce(F.col("cur.Currency_SK"), F.lit(-1)).alias("Currency_SK"),
        F.coalesce(F.col("l.Location_SK"), F.lit(-1)).alias("Location_SK"),
        F.coalesce(F.col("t.Territory_SK"), F.lit(-1)).alias("Territory_SK"),
 
        F.col("sh.SubTotal"),
        F.col("sh.Freight").alias("ShippingCost"),
        F.col("sh.TaxAmt")
    )
)

full_table_name = "adventure_works_catalog.silver.fact_sales_order"
table_name_short = "fact_sales_order"


try:
    df_fact_sales_order_final = deduplicate_by_rule(
        df_fact_sales_order_base,
        partition_cols=["OrderID"],
        order_cols=[
            F.col("ShipDate_SK").desc(),
            F.col("OrderDate_SK").desc()
        ]
    )
    
    df_fact_sales_order_final.cache()

    #Registros processados após as regras (volume transformado)
    rows_processed = df_fact_sales_order_final.count()

    silver_selection(
        spark=spark,
        run_id=current_run_id,
        df_input=df_fact_sales_order_final,
        process_name="Criação_fact_table",
        table_name=table_name_short
    )

    # View Temporária
    df_fact_sales_order_final.createOrReplaceTempView("src_fact_sales_order")


    # Merge SCD Type 1
    spark.sql("""
    MERGE INTO adventure_works_catalog.silver.fact_sales_order tgt
    USING src_fact_sales_order src
    ON tgt.OrderID = src.OrderID
    
    WHEN MATCHED AND (
        NOT (tgt.Customer_SK      <=> src.Customer_SK)
        OR NOT (tgt.OrderDate_SK     <=> src.OrderDate_SK)
        OR NOT (tgt.ShipDate_SK      <=> src.ShipDate_SK)
        OR NOT (tgt.Currency_SK      <=> src.Currency_SK)
        OR NOT (tgt.Location_SK      <=> src.Location_SK)
        OR NOT (tgt.SubTotal         <=> src.SubTotal)
        OR NOT (tgt.ShippingCost     <=> src.ShippingCost)
        OR NOT (tgt.TaxAmt           <=> src.TaxAmt)
    )
    THEN UPDATE SET
        tgt.ShipToAddressID = src.ShipToAddressID,
        tgt.Customer_SK     = src.Customer_SK,
        tgt.OrderDate_SK    = src.OrderDate_SK,
        tgt.ShipDate_SK     = src.ShipDate_SK,
        tgt.Currency_SK     = src.Currency_SK,
        tgt.Location_SK     = src.Location_SK,
        tgt.SubTotal        = src.SubTotal,
        tgt.ShippingCost    = src.ShippingCost,
        tgt.TaxAmt          = src.TaxAmt
    
    WHEN NOT MATCHED THEN
    INSERT (
        OrderID,
        ShipToAddressID,
        Customer_SK,
        OrderDate_SK,
        ShipDate_SK,
        Currency_SK,
        Location_SK,
        SubTotal,
        ShippingCost,
        TaxAmt
    )
    VALUES (
        src.OrderID,
        src.ShipToAddressID,
        src.Customer_SK,
        src.OrderDate_SK,
        src.ShipDate_SK,
        src.Currency_SK,
        src.Location_SK,
        src.SubTotal,
        src.ShippingCost,
        src.TaxAmt
    );
    """)

    #Pegar métricas do merge do DESCRIBE_HISTORY
    merge_metrics = (
        spark.sql(f"DESCRIBE HISTORY {full_table_name}")
        .orderBy(F.col("version").desc())
        .limit(1)
        .select("operationMetrics")
        .collect()[0]["operationMetrics"]
    )

    rows_written = (
        int(merge_metrics.get("numTargetRowsInserted", 0))
        + int(merge_metrics.get("numTargetRowsUpdated", 0))
    )

    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Criação_fact_table",
        layer="SILVER",
        table_saved = table_name_short,
        start_date = start_time,
        rows_readed_batch = rows_processed,
        rows_written_batch = rows_written
    )

    df_fact_sales_order_final.unpersist()

except Exception as e:

    df_fact_sales_order_final.unpersist()

    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Criação_fact_table",
        layer="SILVER",
        table_saved = table_name_short,
        start_date = start_time,
        status = "FAIL",
        error_msg = str(e)
    )

    print(f"Erro em {table_name_short} {e}")
 
# Descrição das colunas
fact_sales_order_columns = {
    "OrderID": "Identificador do pedido de venda",
    "ShipToAddressID": "Identificador técnico do endereço de envio do pedido",
    "Customer_SK": "Chave substituta do cliente associado ao pedido",
    "OrderDate_SK": "Chave da data de criação do pedido de venda",
    "ShipDate_SK": "Chave da data de envio do pedido de venda",
    "Currency_SK": "Chave substituta da taxa de câmbio associada ao pedido",
    "Location_SK": "Chave substituta da localização de envio do pedido",
    "Territory_SK": "Chave substituta do território de vendas associado ao pedido",
    "SubTotal": "Subtotal do pedido (soma dos itens, sem impostos e frete)",
    "ShippingCost": "Custo de frete do pedido de venda",
    "TaxAmt": "Valor de imposto aplicado ao pedido de venda"
}
 
# Adicionando comentários no Unity Catalog
add_column_comments(
    catalog="adventure_works_catalog",
    schema="silver",
    table="fact_sales_order",
    columns_dict=fact_sales_order_columns
)
 
