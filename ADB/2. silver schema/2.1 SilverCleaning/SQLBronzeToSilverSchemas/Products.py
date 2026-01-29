# Databricks notebook source
import sys
sys.path.append("..")

# COMMAND ----------

# Imports 
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from ADB.Module.SilverUtils import (
    deduplicate_by_rule,
    add_column_comments,
    write_silver
)

# COMMAND ----------

#Leitura das tabelas
df_production                    = spark.table("adventure_works_catalog.bronze.production_product")
df_subcategory                   = spark.table("adventure_works_catalog.bronze.production_productsubcategory")
df_category                      = spark.table("adventure_works_catalog.bronze.production_productcategory")
df_model                         = spark.table("adventure_works_catalog.bronze.production_productmodel")
df_inventory                     = spark.table("adventure_works_catalog.bronze.production_productinventory")
df_workorder                     = spark.table("adventure_works_catalog.bronze.production_workorder")
df_scrap_reason                  = spark.table("adventure_works_catalog.bronze.production_scrapreason")
df_transaction_history           = spark.table("adventure_works_catalog.bronze.production_transactionhistory")
df_transaction_history_archive   = spark.table("adventure_works_catalog.bronze.production_transactionhistoryarchive")
df_bill_of_materials             = spark.table("adventure_works_catalog.bronze.production_billofmaterials")
df_production_cost_history       = spark.table("adventure_works_catalog.bronze.production_productcosthistory")
df_production_list_price_history = spark.table("adventure_works_catalog.bronze.production_productlistpricehistory")
df_location                      = spark.table("adventure_works_catalog.bronze.production_location")
df_unit_measure                  = spark.table("adventure_works_catalog.bronze.production_unitmeasure")

# COMMAND ----------

# ===================================
# Products — Clean Product
# ===================================

df_clean_production = (
    df_production.alias("p")
    .join(
        df_subcategory.alias("psc"),
        F.col("p.productsubcategoryid") == F.col("psc.productsubcategoryid"),
        "left"
    )
    .join(
        df_category.alias("pc"),
        F.col("psc.productcategoryid") == F.col("pc.productcategoryid"),
        "left"
    )
    .join(
        df_model.alias("pm"),
        F.col("p.productmodelid") == F.col("pm.productmodelid"),
        "left"
    )
    .join(
        df_unit_measure.alias("um_size"),
        F.col("p.sizeunitmeasurecode") == F.col("um_size.unitmeasurecode"),
        "left"
    )
    .join(
        df_unit_measure.alias("um_weight"),
        F.col("p.weightunitmeasurecode") == F.col("um_weight.unitmeasurecode"),
        "left"
    )
    .select(
        F.col("p.productid").alias("ProductID"),
        F.col("p.productnumber").alias("ProductNumber"),
        F.col("p.name").alias("ProductName"),
        F.col("p.color").alias("Color"),
        F.col("p.size").alias("Size"),
        F.col("p.weight").alias("Weight"),
        F.col("p.standardcost").alias("StandardCost"),
        F.col("p.listprice").alias("ListPrice"),
        F.col("pm.name").alias("ProductModelName"),
        F.col("psc.name").alias("ProductSubcategoryName"),
        F.col("pc.name").alias("ProductCategoryName"),
        F.col("um_size.name").alias("SizeUnitMeasure"),
        F.col("um_weight.name").alias("WeightUnitMeasure"),
        F.col("p.sellstartdate").alias("SellStartDate"),
        F.col("p.sellenddate").alias("SellEndDate"),
        F.col("p.makeflag").alias("MakeFlag"),
        F.col("p.finishedgoodsflag").alias("FinishedGoodsFlag")
    )
)

# Garantia de integridade 
df_clean_production = (
    df_clean_production
    .filter(F.col("ProductID").isNotNull())
)

df_clean_production = deduplicate_by_rule(
    df_clean_production,
    partition_cols=["ProductID"],
    order_cols=[F.col("ProductID").asc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product Inventory
# ====================================

df_clean_production_inventory = (
    df_inventory.alias("i")
    .join(
        df_location.alias("l"),
        F.col("i.locationid") == F.col("l.locationid"),
        "left"
    )
    .select(
        F.col("i.productid").alias("ProductID"),
        F.col("i.locationid").alias("LocationID"),
        F.col("i.quantity").cast("int").alias("Quantity"),
        F.col("l.name").alias("LocationName"),
        F.col("l.costrate").alias("LocationCostRate"),
        F.col("l.availability").alias("LocationAvailability")
    )
)
# Garantia de Integridade
df_clean_production_inventory = (
    df_clean_production_inventory
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("LocationID").isNotNull()
    )
)

df_clean_production_inventory = deduplicate_by_rule(
    df_clean_production_inventory,
    partition_cols=["ProductID", "LocationID"],
    order_cols=[F.col("ProductID").asc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product Transaction
# ====================================

df_transaction_current = (
    df_transaction_history.select(
        F.col("transactionid").alias("TransactionID"),
        F.col("productid").alias("ProductID"),
        F.col("referenceorderid").alias("ReferenceOrderID"),
        F.col("referenceorderlineid").alias("ReferenceOrderLineID"),
        F.col("transactiondate").alias("TransactionDate"),
        F.col("transactiontype").alias("TransactionType"),
        F.col("quantity").cast("int").alias("Quantity"),
        F.col("actualcost").alias("ActualCost")
    )
)

df_transaction_archive = (
    df_transaction_history_archive.select(
        F.col("transactionid").alias("TransactionID"),
        F.col("productid").alias("ProductID"),
        F.col("referenceorderid").alias("ReferenceOrderID"),
        F.col("referenceorderlineid").alias("ReferenceOrderLineID"),
        F.col("transactiondate").alias("TransactionDate"),
        F.col("transactiontype").alias("TransactionType"),
        F.col("quantity").cast("int").alias("Quantity"),
        F.col("actualcost").alias("ActualCost")
    )
)

# Garantia de integridade 
df_clean_production_transaction = (
    df_transaction_current
    .unionByName(df_transaction_archive)
    .filter(F.col("TransactionID").isNotNull())
)

df_clean_production_transaction = deduplicate_by_rule(
    df_clean_production_transaction,
    partition_cols=["TransactionID"],
    order_cols=[F.col("TransactionID").asc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product WorkOrder
# ====================================

df_clean_production_workorder = (
    df_workorder.alias("w")
    .join(
        df_scrap_reason.alias("s"),
        F.col("w.scrapreasonid") == F.col("s.scrapreasonid"),
        "left"
    )
    .select(
        F.col("w.workorderid").alias("WorkOrderID"),
        F.col("w.productid").alias("ProductID"),
        F.col("w.startdate").alias("StartDate"),
        F.col("w.enddate").alias("EndDate"),
        F.col("w.duedate").alias("DueDate"),
        F.col("w.orderqty").cast("int").alias("OrderQuantity"),
        F.col("w.scrappedqty").cast("int").alias("ScrappedQuantity"),
        F.col("s.name").alias("ScrapReason")
    )
)

# Garantia de Integridade
df_clean_production_workorder = (
    df_clean_production_workorder
    .filter(F.col("WorkOrderID").isNotNull())
)

df_clean_production_workorder = deduplicate_by_rule(
    df_clean_production_workorder,
    partition_cols=["WorkOrderID"],
    order_cols=[F.col("WorkOrderID").asc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product Bill of Materials
# ====================================

df_clean_production_bill_of_materials = (
    df_bill_of_materials.select(
        F.col("productassemblyid").alias("ProductAssemblyID"),
        F.col("componentid").alias("ComponentProductID"),
        F.col("perassemblyqty").alias("PerAssemblyQuantity"),
        F.col("startdate").alias("StartDate"),
        F.col("enddate").alias("EndDate")
    ) 
)

# Garantia de Integridade
df_clean_production_bill_of_materials = (
    df_clean_production_bill_of_materials
    .filter(
        F.col("ProductAssemblyID").isNotNull() &
        F.col("ComponentProductID").isNotNull() &
        F.col("StartDate").isNotNull()
    )
)

df_clean_production_bill_of_materials = deduplicate_by_rule(
    df_clean_production_bill_of_materials,
    partition_cols=["ProductAssemblyID", "ComponentProductID", "StartDate"],
    order_cols=[F.col("EndDate").desc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product Cost History
# ====================================

df_clean_production_cost_history = (
    df_production_cost_history.select(
        F.col("productid").alias("ProductID"),
        F.col("startdate").alias("StartDate"),
        F.col("enddate").alias("EndDate"),
        F.col("standardcost").alias("StandardCost")
    )
)

# Garantia de Integridade
df_clean_production_cost_history = (
    df_clean_production_cost_history
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("StartDate").isNotNull()
    )
)

df_clean_production_cost_history = deduplicate_by_rule(
    df_clean_production_cost_history,
    partition_cols=["ProductID", "StartDate"],
    order_cols=[F.col("StandardCost").desc()]
)

# COMMAND ----------

# ====================================
# Products — Clean Product List Price History
# ====================================

df_clean_production_list_price_history = (
    df_production_list_price_history.select(
        F.col("productid").alias("ProductID"),
        F.col("startdate").alias("StartDate"),
        F.col("enddate").alias("EndDate"),
        F.col("listprice").alias("ListPrice")
    )
)

# Garantia de Integridade
df_clean_production_list_price_history = (
    df_clean_production_list_price_history
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("StartDate").isNotNull()
    )
)

df_clean_production_list_price_history = deduplicate_by_rule(
    df_clean_production_list_price_history,
    partition_cols=["ProductID", "StartDate"],
    order_cols=[F.col("ListPrice").desc()]
)

# COMMAND ----------

# Escrita
silver_tables = [
    (df_clean_production, "clean_production"),
    (df_clean_production_inventory, "clean_production_inventory"),
    (df_clean_production_transaction, "clean_production_transaction"),
    (df_clean_production_workorder, "clean_production_workorder"),
    (df_clean_production_bill_of_materials, "clean_production_bill_of_materials"),
    (df_clean_production_cost_history, "clean_production_cost_history"),
    (df_clean_production_list_price_history, "clean_production_list_price_history"),
]
for df, table_name in silver_tables:
    write_silver(df, table_name)
