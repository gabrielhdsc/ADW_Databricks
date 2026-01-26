# Databricks notebook source
import sys
sys.path.append("..")

# COMMAND ----------

# Imports 
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, silver_selection
from ADB.Module.SilverUtils import (
    deduplicate_by_rule,
    add_column_comments,
    write_silver
)

# COMMAND ----------

current_run_id = get_run_id()

#Leitura das tabelas
df_vendor                  = spark.table("adventure_works_catalog.bronze.purchasing_vendor")
df_product_vendor          = spark.table("adventure_works_catalog.bronze.purchasing_productvendor")
df_purchasing_order_header = spark.table("adventure_works_catalog.bronze.purchasing_purchaseorderheader")
df_purchasing_order_detail = spark.table("adventure_works_catalog.bronze.purchasing_purchaseorderdetail")
df_ship_method             = spark.table("adventure_works_catalog.bronze.purchasing_shipmethod")
df_unit_measure            = spark.table("adventure_works_catalog.bronze.production_unitmeasure")

# COMMAND ----------

# ====================================
# Purchasing - Clean Purchasing Vendor
# ====================================

df_clean_purchasing_vendor = (
    df_vendor.alias("v")
    .select(
        # Chave natural
        F.col("v.businessentityid").alias("VendorID"),

        # Identificação
        F.col("v.accountnumber").alias("AccountNumber"),
        F.col("v.name").alias("VendorName"),

        # Classificação
        F.col("v.creditrating").alias("CreditRating"),
        F.col("v.preferredvendorstatus").alias("PreferredVendorStatus"),
        F.col("v.activeflag").alias("ActiveFlag")
    )
)

# Garantia de Integridade
df_clean_purchasing_vendor = (
    df_clean_purchasing_vendor
    .filter(F.col("VendorID").isNotNull())
)

df_clean_purchasing_vendor = deduplicate_by_rule(
    df_clean_purchasing_vendor,
    partition_cols=["VendorID"],
    order_cols=[F.col("VendorID").asc()]
)

# COMMAND ----------

# ====================================
# Purchasing - Clean Purchasing Product Vendor
# ====================================

df_clean_purchasing_product_vendor = (
    df_product_vendor.alias("pv")

    # Join com UnitMeasure (lookup estrutural)
    .join(
        df_unit_measure.alias("u"),
        F.col("pv.unitmeasurecode") == F.col("u.unitmeasurecode"),
        "left"
    )

    .select(
        # Chaves
        F.col("pv.productid").alias("ProductID"),
        F.col("pv.businessentityid").alias("VendorID"),

        # Lead time e preços
        F.col("pv.averageleadtime").cast("int").alias("AverageLeadTime"),
        F.col("pv.standardprice").alias("StandardPrice"),
        F.col("pv.lastreceiptcost").alias("LastReceiptCost"),
        F.col("pv.lastreceiptdate").alias("LastReceiptDate"),

        # Quantidades
        F.col("pv.minorderqty").cast("int").alias("MinOrderQty"),
        F.col("pv.maxorderqty").cast("int").alias("MaxOrderQty"),
        F.col("pv.onorderqty").cast("int").alias("OnOrderQty"),

        # Unidade de medida
        F.col("pv.unitmeasurecode").alias("UnitMeasureCode"),
        F.col("u.name").alias("UnitMeasureName")
    )
)

# Garantia de Integridade
df_clean_purchasing_product_vendor = (
    df_clean_purchasing_product_vendor
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("VendorID").isNotNull()
    )
)

df_clean_purchasing_product_vendor = deduplicate_by_rule(
    df_clean_purchasing_product_vendor,
    partition_cols=["ProductID", "VendorID"],
    order_cols=[F.col("ProductID").asc()]
)

# COMMAND ----------

# ====================================
# Purchasing - Clean Purchase Order
# ====================================

df_clean_purchasing_order = (
    df_purchasing_order_header.alias("h")

    # Join com Vendor 
    .join(
        df_vendor.alias("v"),
        F.col("h.vendorid") == F.col("v.businessentityid"),
        "left"
    )

    # Join com ShipMethod 
    .join(
        df_ship_method.alias("s"),
        F.col("h.shipmethodid") == F.col("s.shipmethodid"),
        "left"
    )

    .select(
        # Chave do pedido
        F.col("h.purchaseorderid").alias("PurchaseOrderID"),

        # Fornecedor
        F.col("h.vendorid").alias("VendorID"),
        F.col("v.name").alias("VendorName"),

        # Status e datas
        F.col("h.status").alias("OrderStatus"),
        F.col("h.orderdate").alias("OrderDate"),
        F.col("h.shipdate").alias("ShipDate"),

        # Valores
        F.col("h.subtotal").alias("SubTotal"),
        F.col("h.taxamt").alias("TaxAmount"),
        F.col("h.freight").alias("Freight"),
        F.col("h.totaldue").alias("TotalDue"),

        # Método de envio
        F.col("s.name").alias("ShipMethodName"),
        F.col("s.shipbase").alias("ShipBase"),
        F.col("s.shiprate").alias("ShipRate")
    )
)

# Garantia de Integridade 
df_clean_purchasing_order = (
    df_clean_purchasing_order
    .filter(F.col("PurchaseOrderID").isNotNull())
)

df_clean_purchasing_order = deduplicate_by_rule(
    df_clean_purchasing_order,
    partition_cols=["PurchaseOrderID"],
    order_cols=[F.col("OrderDate").desc()]
)

# COMMAND ----------

# ====================================
# Purchasing - Clean Purchase Order Line
# ====================================

df_clean_purchasing_order_line = (
    df_purchasing_order_detail.alias("d")
    .select(
        # Chaves
        F.col("d.purchaseorderid").alias("PurchaseOrderID"),
        F.col("d.purchaseorderdetailid").alias("PurchaseOrderDetailID"),
        F.col("d.productid").alias("ProductID"),

        # Datas
        F.col("d.duedate").alias("DueDate"),

        # Quantidades
        F.col("d.orderqty").cast("int").alias("OrderQuantity"),
        F.col("d.receivedqty").cast("int").alias("ReceivedQuantity"),
        F.col("d.rejectedqty").cast("int").alias("RejectedQuantity"),
        F.col("d.stockedqty").cast("int").alias("StockedQuantity"),

        # Valores
        F.col("d.unitprice").alias("UnitPrice"),
        F.col("d.linetotal").alias("LineTotal")
    )
)

# Garantia de integridade 
df_clean_purchasing_order_line = (
    df_clean_purchasing_order_line
    .filter(
        F.col("PurchaseOrderID").isNotNull() &
        F.col("PurchaseOrderDetailID").isNotNull()
    )
)

df_clean_purchasing_order_line = deduplicate_by_rule(
    df_clean_purchasing_order_line,
    partition_cols=["PurchaseOrderID", "PurchaseOrderDetailID"],
    order_cols=[F.col("PurchaseOrderDetailID").asc()]
)

# COMMAND ----------

# Escrita
silver_tables = [
    (df_clean_purchasing_vendor, "clean_purchasing_vendor"),
    (df_clean_purchasing_product_vendor, "clean_purchasing_product_vendor"),
    (df_clean_purchasing_order, "clean_purchasing_order"),
    (df_clean_purchasing_order_line, "clean_purchasing_order_line"),
]

for df, table_name in silver_tables:
    start_time = datetime.now()
    full_table_name = f"adventure_works_catalog.silver.{table_name}"

    try:
        #Garante que as ações usem o mesmo dado processado usando menos memoria
        df.cache()

        #Contagem de registros processados após as regras e joins (volume transformado)
        rows_processed = df.count()

        #Verificação antes de salvar
        silver_selection(
            spark=spark,
            run_id=current_run_id,
            df_input=df,
            process_name="Transferência_Bronze_SilverClean",
            table_name=table_name
        )

        write_silver(df, table_name)

        #Le as linhas do estado final da tabela apos transformações
        rows_written = spark.table(full_table_name).count()

        #Registrar os metadados de auditoria
        audit_registration(
            spark=spark,
            run_id=current_run_id,
            process_name="Transferência_Bronze_SilverClean",
            layer="SILVER",
            table_saved = table_name,
            start_date = start_time,
            rows_readed_batch = rows_processed,
            rows_written_batch = rows_written
        )

        df.unpersist() #Libera a memória do cache

    
    except Exception as e:

        df.unpersist() #Libera a memória do cache

        audit_registration(
            spark=spark,
            run_id=current_run_id,
            process_name="Transferência_Bronze_SilverClean",
            layer="SILVER",
            table_saved = table_name,
            start_date = start_time,
            status="FAIL",
            error_msg=str(e)
        )

        print(f"Erro em {table_name}: {e}")
