# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Purchases
# MAGIC
# MAGIC Fato de itens de pedidos de compra na camada Silver, integrada às dimensões analíticas, consolidando informações de produto, fornecedor, datas e métricas de compra.

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
df_purchase_header = spark.table("adventure_works_catalog.silver.clean_purchase_order")
df_purchase_detail = spark.table("adventure_works_catalog.silver.clean_purchase_order_line")

# Leitura das Dimensões
df_dim_product   = spark.table("adventure_works_catalog.silver.dim_product")
df_dim_supplier  = spark.table("adventure_works_catalog.silver.dim_supplier")
df_dim_date      = spark.table("adventure_works_catalog.silver.dim_date")
df_dim_location  = spark.table("adventure_works_catalog.silver.dim_location") 


# COMMAND ----------

# ============================================================
# FACT_PURCHASES — SILVER (DIMENSIONAL)
# ============================================================
start_time = datetime.now()

# Criando a tabela
spark.sql("""
CREATE TABLE IF NOT EXISTS adventure_works_catalog.silver.fact_purchases (
    PurchaseID INT,
    OrderID INT,

    Product_SK INT,
    Supplier_SK INT,
    OrderDate_SK INT,
    ShipDate_SK INT,

    OrderQty INT,
    ReceivedQuantity INT,
    RejectedQuantity INT,
    StockedQuantity INT,

    UnitPrice DECIMAL(19,4),
    LineTotal DECIMAL(19,4),

    SubTotal DECIMAL(19,4),
    TaxAmt DECIMAL(19,4),
    ShippingCost DECIMAL(19,4),
    TotalDue DECIMAL(19,4)
);
""")

# Join Header + Detail
df_purchase_base = (
    df_purchase_detail.alias("pd")
    .join(
        df_purchase_header.alias("ph"),
        on="PurchaseOrderID",
        how="inner"
    )
)

# Join com Dimensões
df_purchase_enriched = (
    df_purchase_base

    # Product
    .join(
        df_dim_product.alias("p"),
        F.col("pd.ProductID") == F.col("p.ProductID"),
        "left"
    )

    # Supplier
    .join(
        df_dim_supplier.alias("s"),
        F.col("ph.VendorID") == F.col("s.SupplierID"),
        "left"
    )

    # Order Date
    .join(
        df_dim_date.alias("od"),
        F.to_date(F.col("ph.OrderDate")) == F.col("od.FullDate"),
        "left"
    )

    # Ship Date
    .join(
        df_dim_date.alias("sdte"),
        F.to_date(F.col("ph.ShipDate")) == F.col("sdte.FullDate"),
        "left"
    )
)

# Seleção final
df_fact_purchases = (
    df_purchase_enriched
    .select(
        # Identificadores
        F.col("pd.PurchaseOrderDetailID").alias("PurchaseID"),
        F.col("pd.PurchaseOrderID").alias("OrderID"),

        # Chaves substitutas
        F.coalesce(F.col("p.Product_SK"),F.lit(-1)).alias("Product_SK"),
        F.coalesce(F.col("s.Supplier_SK"),F.lit(-1)).alias("Supplier_SK"),
        F.coalesce(F.col("od.DateKey"),F.lit(-1)).alias("OrderDate_SK"),
        F.coalesce(F.col("sdte.DateKey"),F.lit(-1)).alias("ShipDate_SK"),

        # Métricas
        F.col("pd.OrderQuantity").alias("OrderQty"),
        F.col("pd.ReceivedQuantity"),
        F.col("pd.RejectedQuantity"),
        F.col("pd.StockedQuantity"),
        F.col("pd.UnitPrice"),
        F.col("pd.LineTotal"),

        # Custos do pedido 
        F.col("ph.SubTotal"),
        F.col("ph.TaxAmount").alias("TaxAmt"),
        F.col("ph.Freight").alias("ShippingCost"),
        F.col("ph.TotalDue")
    )
)

full_table_name = "adventure_works_catalog.silver.fact_purchases"
table_name_short = "fact_purchases"

try:
    df_fact_purchases.cache()

    #Registros processados após as regras (volume transformado)
    rows_processed = df_fact_purchases.count()

    silver_selection(
        spark=spark,
        run_id=current_run_id,
        df_input=df_fact_purchases,
        process_name="Criação_fact_table",
        table_name=table_name_short
    )

    # Merge SCD type 1
    df_fact_purchases.createOrReplaceTempView("src_fact_purchase")

    spark.sql("""
        MERGE INTO adventure_works_catalog.silver.fact_purchases tgt
        USING src_fact_purchase src
        ON tgt.PurchaseID = src.PurchaseID

        WHEN MATCHED AND (
            NOT (tgt.OrderID <=> src.OrderID) OR
            NOT (tgt.Product_SK <=> src.Product_SK) OR
            NOT (tgt.Supplier_SK <=> src.Supplier_SK) OR
            NOT (tgt.OrderDate_SK <=> src.OrderDate_SK) OR
            NOT (tgt.ShipDate_SK <=> src.ShipDate_SK) OR
            NOT (tgt.OrderQty <=> src.OrderQty) OR
            NOT (tgt.ReceivedQuantity <=> src.ReceivedQuantity) OR
            NOT (tgt.RejectedQuantity <=> src.RejectedQuantity) OR
            NOT (tgt.StockedQuantity <=> src.StockedQuantity) OR
            NOT (tgt.UnitPrice <=> src.UnitPrice) OR
            NOT (tgt.LineTotal <=> src.LineTotal) OR
            NOT (tgt.SubTotal <=> src.SubTotal) OR
            NOT (tgt.TaxAmt <=> src.TaxAmt) OR
            NOT (tgt.ShippingCost <=> src.ShippingCost) OR
            NOT (tgt.TotalDue <=> src.TotalDue)
        )
        THEN UPDATE SET
            tgt.OrderID = src.OrderID,
            tgt.Product_SK = src.Product_SK,
            tgt.Supplier_SK = src.Supplier_SK,
            tgt.OrderDate_SK = src.OrderDate_SK,
            tgt.ShipDate_SK = src.ShipDate_SK,
            tgt.OrderQty = src.OrderQty,
            tgt.ReceivedQuantity = src.ReceivedQuantity,
            tgt.RejectedQuantity = src.RejectedQuantity,
            tgt.StockedQuantity = src.StockedQuantity,
            tgt.UnitPrice = src.UnitPrice,
            tgt.LineTotal = src.LineTotal,
            tgt.SubTotal = src.SubTotal,
            tgt.TaxAmt = src.TaxAmt,
            tgt.ShippingCost = src.ShippingCost,
            tgt.TotalDue = src.TotalDue

        WHEN NOT MATCHED THEN
            INSERT(
                PurchaseID,
                OrderID,
                Product_SK,
                Supplier_SK,
                OrderDate_SK,
                ShipDate_SK,
                OrderQty,
                ReceivedQuantity,
                RejectedQuantity,
                StockedQuantity,
                UnitPrice,
                LineTotal,
                SubTotal,
                TaxAmt,
                ShippingCost,
                TotalDue
            )
            VALUES(
                src.PurchaseID,
                src.OrderID,
                src.Product_SK,
                src.Supplier_SK,
                src.OrderDate_SK,
                src.ShipDate_SK,
                src.OrderQty,
                src.ReceivedQuantity,
                src.RejectedQuantity,
                src.StockedQuantity,
                src.UnitPrice,
                src.LineTotal,
                src.SubTotal,
                src.TaxAmt,
                src.ShippingCost,
                src.TotalDue
            )
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
    
    df_fact_purchases.unpersist()

except Exception as e:

    df_fact_purchases.unpersist()
    
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

    print(f"Erro em fact_purchases: {e}")

# Descrição das colunas
fact_purchases_columns = {
    "PurchaseID": "Identificador único do item da compra (nível de detalhe do pedido)",
    "OrderID": "Identificador do pedido de compra",
    "Product_SK": "Chave substituta da versão do produto (controle histórico SCD2)",
    "Supplier_SK": "Chave substituta do fornecedor",
    "OrderDate_SK": "Chave substituta da data do pedido de compra no formato YYYYMMDD",
    "ShipDate_SK": "Chave substituta da data em que o fornecedor enviou o pedido (YYYYMMDD)",
    "OrderQty": "Quantidade solicitada no pedido de compra",
    "ReceivedQuantity": "Quantidade efetivamente recebida do fornecedor",
    "RejectedQuantity": "Quantidade rejeitada no processo de recebimento",
    "StockedQuantity": "Quantidade efetivamente estocada após o recebimento",
    "UnitPrice": "Preço unitário do produto na compra",
    "LineTotal": "Valor total do item comprado (quantidade x preço unitário)",
    "SubTotal": "Subtotal do pedido de compra antes de impostos e frete",
    "TaxAmt": "Valor de imposto aplicado ao pedido de compra",
    "ShippingCost": "Custo de frete do pedido de compra",
    "TotalDue": "Valor total devido no pedido de compra (subtotal + impostos + frete)"
}

# Adicionando comentários no Unity Catalog
add_column_comments(
    catalog="adventure_works_catalog",
    schema="silver",
    table="fact_purchases",
    columns_dict=fact_purchases_columns
)

