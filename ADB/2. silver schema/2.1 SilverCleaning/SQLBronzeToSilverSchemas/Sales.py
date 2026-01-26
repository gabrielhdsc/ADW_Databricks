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
# CSV
df_sales_order_header_csv     = spark.table("adventure_works_catalog.bronze.csv_salesorderheader_2010_2021")
df_sales_order_detail_csv     = spark.table("adventure_works_catalog.bronze.csv_salesorderdetail_2010_2021")
# SQL
df_sales_order_header_sql     = spark.table("adventure_works_catalog.bronze.sales_salesorderheader")
df_sales_order_detail_sql     = spark.table("adventure_works_catalog.bronze.sales_salesorderdetail")
df_customer                   = spark.table("adventure_works_catalog.bronze.sales_customer")
df_store                      = spark.table("adventure_works_catalog.bronze.sales_store")
df_sales_person               = spark.table("adventure_works_catalog.bronze.sales_salesperson")
df_sales_territory            = spark.table("adventure_works_catalog.bronze.sales_salesterritory")
df_currency                   = spark.table("adventure_works_catalog.bronze.sales_currency")
df_currency_rate              = spark.table("adventure_works_catalog.bronze.sales_currencyrate")
df_special_offer              = spark.table("adventure_works_catalog.bronze.sales_specialoffer")
df_special_offer_product      = spark.table("adventure_works_catalog.bronze.sales_specialofferproduct")
df_sales_person_quota_history = spark.table("adventure_works_catalog.bronze.sales_salespersonquotahistory")
df_sales_territory_history    = spark.table("adventure_works_catalog.bronze.sales_salesterritoryhistory")

# COMMAND ----------

# ====================================
# Sales — Clean Sales Order Header
# ====================================

df_sales_order_header_csv_clean = (
    df_sales_order_header_csv
    .select(
        F.expr("try_cast(_c0  as int)").alias("SalesOrderID"),
        F.expr("try_cast(_c1  as int)").alias("RevisionNumber"),
        F.expr("try_cast(_c2  as timestamp)").alias("OrderDate"),
        F.expr("try_cast(_c3  as timestamp)").alias("DueDate"),
        F.expr("try_cast(_c4  as timestamp)").alias("ShipDate"),
        F.expr("try_cast(_c5  as int)").alias("Status"),
        F.expr("try_cast(_c6  as boolean)").alias("OnlineOrderFlag"),
        F.col("_c7").cast("string").alias("SalesOrderNumber"),
        F.col("_c8").cast("string").alias("PurchaseOrderNumber"),
        F.col("_c9").cast("string").alias("AccountNumber"),
        F.expr("try_cast(_c10 as int)").alias("CustomerID"),
        F.expr("try_cast(_c11 as int)").alias("SalesPersonID"),
        F.expr("try_cast(_c12 as int)").alias("TerritoryID"),
        F.expr("try_cast(_c13 as int)").alias("BillToAddressID"),
        F.expr("try_cast(_c14 as int)").alias("ShipToAddressID"),
        F.expr("try_cast(_c15 as int)").alias("ShipMethodID"),
        F.expr("try_cast(_c16 as int)").alias("CreditCardID"),
        F.col("_c17").cast("string").alias("CreditCardApprovalCode"),
        F.expr("try_cast(_c18 as int)").alias("CurrencyRateID"),
        F.expr("try_cast(_c19 as decimal(19,4))").alias("SubTotal"),
        F.expr("try_cast(_c20 as decimal(19,4))").alias("TaxAmt"),
        F.expr("try_cast(_c21 as decimal(19,4))").alias("Freight"),
        F.expr("try_cast(_c22 as decimal(19,4))").alias("TotalDue"),
        F.col("_c23").cast("string").alias("Comment")
    )
)

df_sales_order_header_sql_clean = (
    df_sales_order_header_sql
    .select(
        "SalesOrderID","RevisionNumber","OrderDate","DueDate","ShipDate",
        "Status","OnlineOrderFlag","SalesOrderNumber","PurchaseOrderNumber",
        "AccountNumber","CustomerID","SalesPersonID","TerritoryID",
        "BillToAddressID","ShipToAddressID","ShipMethodID","CreditCardID",
        "CreditCardApprovalCode","CurrencyRateID",
        "SubTotal","TaxAmt","Freight","TotalDue","Comment"
    )
)

df_clean_sales_order_header = (
    df_sales_order_header_sql_clean
    .unionByName(df_sales_order_header_csv_clean)
)

# Garantia de Integridade
df_clean_sales_order_header = (
    df_clean_sales_order_header
    .filter(F.col("SalesOrderID").isNotNull())
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Order Detail
# ====================================

df_sales_order_detail_csv_clean = (
    df_sales_order_detail_csv
    .select(
        F.col("_c0").cast("int").alias("SalesOrderID"),
        F.col("_c1").cast("int").alias("SalesOrderDetailID"),
        F.col("_c2").cast("string").alias("CarrierTrackingNumber"),
        F.col("_c3").cast("int").alias("OrderQty"),
        F.col("_c4").cast("int").alias("ProductID"),
        F.col("_c5").cast("int").alias("SpecialOfferID"),
        F.col("_c6").cast("decimal(19,4)").alias("UnitPrice"),
        F.col("_c7").cast("decimal(19,4)").alias("UnitPriceDiscount"),
        F.col("_c8").cast("decimal(38,6)").alias("LineTotal")
    )
)

df_sales_order_detail_sql_clean = (
    df_sales_order_detail_sql
    .select(
        "SalesOrderID","SalesOrderDetailID","CarrierTrackingNumber",
        "OrderQty","ProductID","SpecialOfferID",
        "UnitPrice","UnitPriceDiscount","LineTotal"
    )
)

df_clean_sales_order_detail = (
    df_sales_order_detail_sql_clean
    .unionByName(df_sales_order_detail_csv_clean)
)

# Garantia de Integridade
df_clean_sales_order_detail = (
    df_clean_sales_order_detail
    .filter(F.col("SalesOrderDetailID").isNotNull())
)

df_clean_sales_order_detail = deduplicate_by_rule(
    df_clean_sales_order_detail,
    partition_cols=["SalesOrderDetailID"],
    order_cols=[F.col("SalesOrderDetailID").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Customer
# ====================================

df_clean_sales_customer = (
    df_customer
    .select(
        F.col("customerid").alias("CustomerID"),
        F.col("personid").alias("PersonID"),
        F.col("storeid").alias("StoreID"),
        F.col("territoryid").alias("TerritoryID")
    )
)

# Garantia de Integridade
df_clean_sales_customer = (
    df_clean_sales_customer
    .filter(F.col("CustomerID").isNotNull())
)

df_clean_sales_customer = deduplicate_by_rule(
    df_clean_sales_customer,
    partition_cols=["CustomerID"],
    order_cols=[F.col("CustomerID").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Store
# ====================================

df_clean_sales_store = (
    df_store
    .select(
        F.col("businessentityid").alias("StoreID"),
        F.col("name").alias("StoreName"),
        F.col("salespersonid").alias("SalesPersonID")
    )
)

# Garantia de Integridade
df_clean_sales_store = (
    df_clean_sales_store
    .filter(F.col("StoreID").isNotNull())
)

df_clean_sales_store = deduplicate_by_rule(
    df_clean_sales_store,
    partition_cols=["StoreID"],
    order_cols=[F.col("StoreID").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Territory
# ====================================

df_clean_sales_territory = (
    df_sales_territory
    .select(
        F.col("territoryid").alias("TerritoryID"),
        F.col("name").alias("TerritoryName"),
        F.col("countryregioncode").alias("CountryRegionCode"),
        F.col("group").alias("TerritoryGroup")
    )
)

# Garantia de Integridade
df_clean_sales_territory = (
    df_clean_sales_territory
    .filter(F.col("TerritoryID").isNotNull())
)

df_clean_sales_territory = deduplicate_by_rule(
    df_clean_sales_territory,
    partition_cols=["TerritoryID"],
    order_cols=[F.col("TerritoryID").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Currency
# ====================================

df_clean_sales_currency = (
    df_currency
    .select(
        F.col("currencycode").alias("CurrencyCode"),
        F.col("name").alias("CurrencyName")
    )
)

# Garantia de Integridade
df_clean_sales_currency = (
    df_clean_sales_currency
    .filter(F.col("CurrencyCode").isNotNull())
)

df_clean_sales_currency = deduplicate_by_rule(
    df_clean_sales_currency,
    partition_cols=["CurrencyCode"],
    order_cols=[F.col("CurrencyCode").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Currency Rate
# ====================================

df_clean_sales_currency_rate = (
    df_currency_rate
    .select(
        F.col("currencyrateid").alias("CurrencyRateID"),
        F.col("fromcurrencycode").alias("FromCurrencyCode"),
        F.col("tocurrencycode").alias("ToCurrencyCode"),
        F.col("currencyratedate").alias("CurrencyRateDate"),
        F.col("averagerate").alias("AverageRate"),
        F.col("endofdayrate").alias("EndOfDayRate")
    )
)

# Garantia de Integridade
df_clean_sales_currency_rate = (
    df_clean_sales_currency_rate
    .filter(F.col("CurrencyRateID").isNotNull())
)

df_clean_sales_currency_rate = deduplicate_by_rule(
    df_clean_sales_currency_rate,
    partition_cols=["CurrencyRateID"],
    order_cols=[F.col("CurrencyRateDate").desc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Special Offer
# ====================================

df_clean_sales_special_offer = (
    df_special_offer
    .select(
        "specialofferid",
        "description",
        "discountpct",
        "type",
        "category",
        "startdate",
        "enddate"
    )
)

# Garantia de Integridade
df_clean_sales_special_offer = (
    df_clean_sales_special_offer
    .filter(F.col("specialofferid").isNotNull())
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Special Offer Product
# ====================================

df_clean_sales_special_offer_product = (
    df_special_offer_product
    .select(
        "specialofferid",
        "productid"
    )
)

# Garantia de Integridade
df_clean_sales_special_offer_product = (
    df_clean_sales_special_offer_product
    .filter(
        F.col("specialofferid").isNotNull() &
        F.col("productid").isNotNull()
    )
)

df_clean_sales_special_offer_product = deduplicate_by_rule(
    df_clean_sales_special_offer_product,
    partition_cols=["specialofferid", "productid"],
    order_cols=[F.col("specialofferid").asc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean SalesPerson Quota History
# ====================================

df_clean_sales_person_quota_history = (
    df_sales_person_quota_history
    .select(
        "businessentityid",
        "quotadate",
        "salesquota"
    )
)

# Garantia de Integridade
df_clean_sales_person_quota_history = (
    df_clean_sales_person_quota_history
    .filter(
        F.col("businessentityid").isNotNull() &
        F.col("quotadate").isNotNull()
    )
)

df_clean_sales_person_quota_history = deduplicate_by_rule(
    df_clean_sales_person_quota_history,
    partition_cols=["businessentityid", "quotadate"],
    order_cols=[F.col("salesquota").desc()]
)

# COMMAND ----------

# ====================================
# Sales — Clean Sales Territory History
# ====================================

df_clean_sales_territory_history = (
    df_sales_territory_history
    .select(
        "businessentityid",
        "territoryid",
        "startdate",
        "enddate"
    )
)

# Garantia de Integridade
df_clean_sales_territory_history = (
    df_clean_sales_territory_history
    .filter(
        F.col("businessentityid").isNotNull() &
        F.col("territoryid").isNotNull() &
        F.col("startdate").isNotNull()
    )
)

df_clean_sales_territory_history = deduplicate_by_rule(
    df_clean_sales_territory_history,
    partition_cols=["businessentityid", "territoryid", "startdate"],
    order_cols=[F.col("enddate").desc()]
)

# COMMAND ----------

# Escrita
silver_tables = [
    (df_clean_sales_order_header, "clean_sales_order_header"),
    (df_clean_sales_order_detail, "clean_sales_order_detail"),
    (df_clean_sales_customer, "clean_sales_customer"),
    (df_clean_sales_store, "clean_sales_store"),
    (df_clean_sales_territory, "clean_sales_territory"),
    (df_clean_sales_currency, "clean_sales_currency"),
    (df_clean_sales_currency_rate, "clean_sales_currency_rate"),
    (df_clean_sales_special_offer, "clean_sales_special_offer"),
    (df_clean_sales_special_offer_product, "clean_sales_special_offer_product"),
    (df_clean_sales_person_quota_history, "clean_sales_person_quota_history"),
    (df_clean_sales_territory_history, "clean_sales_territory_history")
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

        df.unpersist()

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
