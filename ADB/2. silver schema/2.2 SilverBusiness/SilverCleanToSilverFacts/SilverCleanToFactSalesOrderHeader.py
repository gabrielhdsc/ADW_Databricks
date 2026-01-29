# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Sales Order
# MAGIC Fato de pedidos de venda (nível de pedido / header) na camada Silver, integrada às dimensões analíticas para análise de valores consolidados do pedido, como subtotal, frete e impostos.

# COMMAND ----------

import sys
sys.path.append("..")

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from ADB.Module.SilverUtils import (
    add_column_comments,
    deduplicate_by_rule,
)

# COMMAND ----------

# Leitura das tabelas Silver
df_clean_sales_order_header = spark.table("adventure_works_catalog.silver.clean_sales_order_header")
 
# Leitura das Dimensões
df_dim_customer = spark.table("adventure_works_catalog.silver.dim_customer")
df_dim_date = spark.table("adventure_works_catalog.silver.dim_date")
df_dim_currency = spark.table("adventure_works_catalog.silver.dim_currency")
df_dim_location = spark.table("adventure_works_catalog.silver.dim_location")


# COMMAND ----------

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
 
    # Select Final
    .select(
        F.col("sh.SalesOrderID").alias("OrderID"),
        F.col("sh.ShipToAddressID"),
 
        F.coalesce(F.col("c.Customer_SK"), F.lit(-1)).alias("Customer_SK"),
        F.coalesce(F.col("od.DateKey"), F.lit(-1)).alias("OrderDate_SK"),
        F.coalesce(F.col("sd.DateKey"), F.lit(-1)).alias("ShipDate_SK"),
        F.coalesce(F.col("cur.Currency_SK"), F.lit(-1)).alias("Currency_SK"),
        F.coalesce(F.col("l.Location_SK"), F.lit(-1)).alias("Location_SK"),
 
        F.col("sh.SubTotal"),
        F.col("sh.Freight").alias("ShippingCost"),
        F.col("sh.TaxAmt")
    )
)
 
df_fact_sales_order_final = deduplicate_by_rule(
    df_fact_sales_order_base,
    partition_cols=["OrderID"],
    order_cols=[
        F.col("ShipDate_SK").desc(),
        F.col("OrderDate_SK").desc()
    ]
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
 
# Descrição das colunas
fact_sales_order_columns = {
    "OrderID": "Identificador do pedido de venda",
    "ShipToAddressID": "Identificador técnico do endereço de envio do pedido",
    "Customer_SK": "Chave substituta do cliente associado ao pedido",
    "OrderDate_SK": "Chave da data de criação do pedido de venda",
    "ShipDate_SK": "Chave da data de envio do pedido de venda",
    "Currency_SK": "Chave substituta da taxa de câmbio associada ao pedido",
    "Location_SK": "Chave substituta da localização de envio do pedido",
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
 
