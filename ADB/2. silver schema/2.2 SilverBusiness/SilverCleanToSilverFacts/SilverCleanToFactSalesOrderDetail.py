# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Sales Detail
# MAGIC
# MAGIC Fato de itens de pedidos de venda na camada Silver, integrada às dimensões analíticas, permitindo análises detalhadas por produto, quantidade e valores unitários.

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
df_clean_sales_order_detail = spark.table("adventure_works_catalog.silver.clean_sales_order_detail")

# Leitura das Dimensões
df_dim_product = spark.table("adventure_works_catalog.silver.dim_product")


# COMMAND ----------

# Criando a tabela
spark.sql("""
CREATE TABLE IF NOT EXISTS 
adventure_works_catalog.silver.fact_sales_order_detail (
    SalesID INT,
    OrderID INT,
    Product_SK INT,
    OrderQty INT,
    UnitPrice DECIMAL(19,4),
    UnitPriceDiscount DECIMAL(19,4),
    LineTotal DECIMAL(19,4)
)
""")

# Preparação dos dados e chaves
df_fact_sales_order_detail_base = (
    df_clean_sales_order_detail.alias("sd")

    .join(
        df_dim_product.alias("p"),
        F.col("sd.ProductID") == F.col("p.ProductID"),
        "left"
    )

    .select(
        F.col("sd.SalesOrderDetailID").alias("SalesID"),
        F.col("sd.SalesOrderID").alias("OrderID"),

        F.coalesce(F.col("p.Product_SK"), F.lit(-1)).alias("Product_SK"),

        F.col("sd.OrderQty"),
        F.col("sd.UnitPrice"),
        F.col("sd.UnitPriceDiscount"),
        F.col("sd.LineTotal")
    )
)

# View Temporária
df_fact_sales_order_detail_base.createOrReplaceTempView("src_fact_sales_order_detail")

# Merge SCD Type 1
spark.sql("""
MERGE INTO adventure_works_catalog.silver.fact_sales_order_detail tgt
USING src_fact_sales_order_detail src
ON tgt.SalesID = src.SalesID

WHEN MATCHED AND (
       NOT (tgt.Product_SK         <=> src.Product_SK)
    OR NOT (tgt.OrderQty           <=> src.OrderQty)
    OR NOT (tgt.UnitPrice          <=> src.UnitPrice)
    OR NOT (tgt.UnitPriceDiscount  <=> src.UnitPriceDiscount)
    OR NOT (tgt.LineTotal          <=> src.LineTotal)
)
THEN UPDATE SET
    tgt.OrderID           = src.OrderID,
    tgt.Product_SK        = src.Product_SK,
    tgt.OrderQty          = src.OrderQty,
    tgt.UnitPrice         = src.UnitPrice,
    tgt.UnitPriceDiscount = src.UnitPriceDiscount,
    tgt.LineTotal         = src.LineTotal

WHEN NOT MATCHED THEN
  INSERT (
    SalesID,
    OrderID,
    Product_SK,
    OrderQty,
    UnitPrice,
    UnitPriceDiscount,
    LineTotal
  )
  VALUES (
    src.SalesID,
    src.OrderID,
    src.Product_SK,
    src.OrderQty,
    src.UnitPrice,
    src.UnitPriceDiscount,
    src.LineTotal
  );
""")

# Descrição das colunas
fact_sales_order_detail_columns = {
    "SalesID": "Identificador do item do pedido de venda",
    "OrderID": "Identificador do pedido de venda",
    "Product_SK": "Chave substituta do produto vendido",
    "OrderQty": "Quantidade vendida do produto",
    "UnitPrice": "Preço unitário do produto no item do pedido",
    "UnitPriceDiscount": "Desconto aplicado sobre o preço unitário",
    "LineTotal": "Valor total do item do pedido"
}

# Adicionando comentários no Unity Catalog
add_column_comments(
    catalog="adventure_works_catalog",
    schema="silver",
    table="fact_sales_order_detail",
    columns_dict=fact_sales_order_detail_columns
)

