# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Clean Data
# MAGIC
# MAGIC A camada **Silver Clean** reúne dados **limpos, padronizados e confiáveis**, organizados de forma a **atender qualquer necessidade de análise futura**.
# MAGIC
# MAGIC Essa camada atua como intermediária entre a Bronze e a Gold, preservando a granularidade original e servindo como **fonte única de dados para consumo analítico e para a construção de modelos futuros**.
# MAGIC

# COMMAND ----------

# Imports e setup
from pyspark.sql import functions as F

# COMMAND ----------

# Leitura das tabelas Silver

# Human Resources
df_employee                    = spark.table("adventure_works_catalog.bronze.humanresources_employee")
df_employee_department_history = spark.table("adventure_works_catalog.bronze.humanresources_employeedepartmenthistory")
df_employee_pay_history        = spark.table("adventure_works_catalog.bronze.humanresources_employeepayhistory")
df_department                  = spark.table("adventure_works_catalog.bronze.humanresources_department")
df_shift                       = spark.table("adventure_works_catalog.bronze.humanresources_shift")


# Person
df_person                  = spark.table("adventure_works_catalog.bronze.person_person")
df_business_entity         = spark.table("adventure_works_catalog.bronze.person_businessentity")
df_business_entity_contact = spark.table("adventure_works_catalog.bronze.person_businessentitycontact")
df_contact_type            = spark.table("adventure_works_catalog.bronze.person_contacttype")
df_email_address           = spark.table("adventure_works_catalog.bronze.person_emailaddress")
df_person_phone            = spark.table("adventure_works_catalog.bronze.person_personphone")
df_phone_number_type       = spark.table("adventure_works_catalog.bronze.person_phonenumbertype")
df_business_entity_address = spark.table("adventure_works_catalog.bronze.person_businessentityaddress")
df_address                 = spark.table("adventure_works_catalog.bronze.person_address")
df_address_type            = spark.table("adventure_works_catalog.bronze.person_addresstype")
df_state_province          = spark.table("adventure_works_catalog.bronze.person_stateprovince")
df_country_region          = spark.table("adventure_works_catalog.bronze.person_countryregion")

#Purchasing
df_vendor                  = spark.table("adventure_works_catalog.bronze.purchasing_vendor")
df_product_vendor          = spark.table("adventure_works_catalog.bronze.purchasing_productvendor")
df_purchasing_order_header = spark.table("adventure_works_catalog.bronze.purchasing_purchaseorderheader")
df_purchasing_order_detail = spark.table("adventure_works_catalog.bronze.purchasing_purchaseorderdetail")
df_ship_method             = spark.table("adventure_works_catalog.bronze.purchasing_shipmethod")
df_unit_measure            = spark.table("adventure_works_catalog.bronze.production_unitmeasure")

# Products
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


# Sales
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



# Criação de coluna para comentários e documentação
def add_column_comments(catalog, schema, table, columns_dict):
    """
    Adiciona comentários nas colunas de uma tabela no Unity Catalog.
    """
    for column, comment in columns_dict.items():
        spark.sql(f"""
            COMMENT ON COLUMN {catalog}.{schema}.{table}.{column}
            IS '{comment}'
        """)

# COMMAND ----------

# Human Resources

# ====================================
# HumanResources — Clean HR Employee
# ====================================

df_clean_hr_employee = (
    df_employee.alias("e")
    .select(
        # Chave
        F.col("e.businessentityid").alias("EmployeeID"),

        # Atributos estáveis
        F.col("e.jobtitle").alias("JobTitle"),
        F.col("e.hiredate").alias("HireDate"),
        F.col("e.birthdate").alias("BirthDate"),
        F.col("e.gender").alias("Gender"),
        F.col("e.maritalstatus").alias("MaritalStatus"),
        F.col("e.vacationhours").alias("VacationHours"),
        F.col("e.sickleavehours").alias("SickLeaveHours")
    )
)

# Garantia de integridade e granularidade
df_clean_hr_employee = (
    df_clean_hr_employee
    .filter(F.col("EmployeeID").isNotNull())
    .dropDuplicates(["EmployeeID"])
)

# Conferência
df_clean_hr_employee.printSchema()
df_clean_hr_employee.display()

# Escrita na Silver
(
    df_clean_hr_employee.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_hr_employee")
)


# ==========================================
# HumanResources — Clean HR Employee Org History
# ==========================================

df_clean_hr_employee_org_history = (
    df_employee_department_history.alias("edh")

    # Employee
    .join(
        df_employee.alias("e"),
        F.col("edh.businessentityid") == F.col("e.businessentityid"),
        "left"
    )

    # Department
    .join(
        df_department.alias("d"),
        F.col("edh.departmentid") == F.col("d.departmentid"),
        "left"
    )

    # Shift
    .join(
        df_shift.alias("s"),
        F.col("edh.shiftid") == F.col("s.shiftid"),
        "left"
    )

    .select(
        # Chaves
        F.col("edh.businessentityid").alias("EmployeeID"),
        F.col("edh.departmentid").alias("DepartmentID"),
        F.col("edh.shiftid").alias("ShiftID"),

        # Período
        F.col("edh.startdate").alias("StartDate"),
        F.col("edh.enddate").alias("EndDate"),

        # Employee
        F.col("e.jobtitle").alias("JobTitle"),
        F.col("e.hiredate").alias("HireDate"),
        F.col("e.gender").alias("Gender"),

        # Department
        F.col("d.name").alias("DepartmentName"),
        F.col("d.groupname").alias("DepartmentGroup"),

        # Shift
        F.col("s.name").alias("ShiftName"),
        F.col("s.starttime").alias("ShiftStartTime"),
        F.col("s.endtime").alias("ShiftEndTime")
    )
)

# Garantia de integridade e granularidade
df_clean_hr_employee_org_history = (
    df_clean_hr_employee_org_history
    .filter(F.col("EmployeeID").isNotNull())
    .dropDuplicates(["EmployeeID", "DepartmentID", "StartDate"])
)

# Conferência
df_clean_hr_employee_org_history.printSchema()
df_clean_hr_employee_org_history.display()

# Escrita na Silver
(
    df_clean_hr_employee_org_history.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_hr_employee_org_history")
)

# ==========================================
# HumanResources — Clean HR Employee Pay History
# ==========================================

df_clean_hr_employee_pay_history = (
    df_employee_pay_history.alias("ph")

    # Employee
    .join(
        df_employee.alias("e"),
        F.col("ph.businessentityid") == F.col("e.businessentityid"),
        "left"
    )

    .select(
        # Chaves
        F.col("ph.businessentityid").alias("EmployeeID"),
        F.col("ph.ratechangedate").alias("RateChangeDate"),

        # Salário
        F.col("ph.rate").cast("decimal(18,2)").alias("PayRate"),
        F.col("ph.payfrequency").alias("PayFrequency"),

        # Contexto
        F.col("e.jobtitle").alias("JobTitle")
    )
)

# Garantia de integridade e granularidade
df_clean_hr_employee_pay_history = (
    df_clean_hr_employee_pay_history
    .filter(F.col("EmployeeID").isNotNull())
    .dropDuplicates(["EmployeeID", "RateChangeDate"])
)

# Conferência
df_clean_hr_employee_pay_history.printSchema()
df_clean_hr_employee_pay_history.display()

# Escrita na Silver
(
    df_clean_hr_employee_pay_history.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_hr_employee_pay_history"
    )
)


# COMMAND ----------

# Person

# ====================================
# Person - Clean Person
# ====================================

df_clean_person = (
    df_person.alias("p")

    # Join com BusinessEntity (garantia de entidade)
    .join(
        df_business_entity.alias("be"),
        F.col("p.businessentityid") == F.col("be.businessentityid"),
        "inner"
    )

    .select(
        # Chave natural
        F.col("p.businessentityid").alias("PersonID"),

        # Identificação
        F.col("p.persontype").alias("PersonType"),
        F.col("p.namestyle").alias("NameStyle"),
        F.col("p.title").alias("Title"),
        F.col("p.firstname").alias("FirstName"),
        F.col("p.middlename").alias("MiddleName"),
        F.col("p.lastname").alias("LastName"),
        F.col("p.suffix").alias("Suffix"),

        # Preferências
        F.col("p.emailpromotion").alias("EmailPromotion")
    )
)

# Garantia de integridade e granularidade
df_clean_person = (
    df_clean_person
    .filter(F.col("PersonID").isNotNull())
    .dropDuplicates(["PersonID"])
)

# Conferência
df_clean_person.printSchema()
df_clean_person.display()

# Escrita na Silver
(
    df_clean_person.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_person"
    )
)

# ====================================
# Person - Clean Person Contact
# ====================================

df_clean_person_contact = (
    df_business_entity_contact.alias("bec")

    # Join com ContactType (papel do contato)
    .join(
        df_contact_type.alias("ct"),
        F.col("bec.contacttypeid") == F.col("ct.contacttypeid"),
        "left"
    )

    # Join com Person (nome da pessoa)
    .join(
        df_person.alias("p"),
        F.col("bec.personid") == F.col("p.businessentityid"),
        "left"
    )

    .select(
        # Chaves
        F.col("bec.businessentityid").alias("PersonID"),
        F.col("bec.personid").alias("RelatedPersonID"),

        # Papel do contato
        F.col("ct.name").alias("ContactTypeName"),

        # Nome da pessoa relacionada
        F.col("p.firstname").alias("FirstName"),
        F.col("p.lastname").alias("LastName")
    )
)

# Garantia de integridade e granularidade
df_clean_person_contact = (
    df_clean_person_contact
    .filter(F.col("PersonID").isNotNull())
    .dropDuplicates(
        ["PersonID", "ContactTypeName", "RelatedPersonID"]
    )
)

# Conferência
df_clean_person_contact.printSchema()
df_clean_person_contact.display()

# Escrita na Silver
(
    df_clean_person_contact.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_person_contact"
    )
)


# ====================================
# Person - Clean Person Contact Method
# ====================================

df_email = (
    df_email_address.alias("e")
    .select(
        F.col("e.businessentityid").alias("PersonID"),
        F.lit("EMAIL").alias("ContactType"),
        F.col("e.emailaddress").alias("ContactValue"),
        F.lit(None).cast("string").alias("ContactSubtype")
    )
)

df_phone = (
    df_person_phone.alias("p")

    .join(
        df_phone_number_type.alias("t"),
        F.col("p.phonenumbertypeid") == F.col("t.phonenumbertypeid"),
        "left"
    )

    .select(
        F.col("p.businessentityid").alias("PersonID"),
        F.lit("PHONE").alias("ContactType"),
        F.col("p.phonenumber").alias("ContactValue"),
        F.col("t.name").alias("ContactSubtype")
    )
)

# Garantia de integridade e granularidade
df_clean_person_contact_method = (
    df_email
    .unionByName(df_phone)
    .filter(F.col("PersonID").isNotNull())
    .dropDuplicates(["PersonID", "ContactType", "ContactValue"])
)

# Conferência
df_clean_person_contact_method.printSchema()
df_clean_person_contact_method.display()

# Escrita na Silver
(
    df_clean_person_contact_method.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_person_contact_method"
    )
)

# ====================================
# Person - Clean Person Address
# ====================================

df_clean_person_address = (
    df_business_entity_address.alias("bea")

    # Join com Address
    .join(
        df_address.alias("a"),
        F.col("bea.addressid") == F.col("a.addressid"),
        "left"
    )

    # Join com AddressType
    .join(
        df_address_type.alias("at"),
        F.col("bea.addresstypeid") == F.col("at.addresstypeid"),
        "left"
    )

    # Join com StateProvince
    .join(
        df_state_province.alias("sp"),
        F.col("a.stateprovinceid") == F.col("sp.stateprovinceid"),
        "left"
    )

    # Join com CountryRegion
    .join(
        df_country_region.alias("cr"),
        F.col("sp.countryregioncode") == F.col("cr.countryregioncode"),
        "left"
    )

    .select(
        # Chaves
        F.col("bea.businessentityid").alias("PersonID"),
        F.col("at.name").alias("AddressTypeName"),

        # Endereço
        F.col("a.addressline1").alias("AddressLine1"),
        F.col("a.addressline2").alias("AddressLine2"),
        F.col("a.city").alias("City"),
        F.col("sp.name").alias("StateProvinceName"),
        F.col("cr.name").alias("CountryName"),
        F.col("a.postalcode").alias("PostalCode"),
        F.col("a.SpatialLocationString").alias("SpatialLocation")
    )
)

# Garantia de integridade e granularidade
df_clean_person_address = (
    df_clean_person_address
    .filter(F.col("PersonID").isNotNull())
    .dropDuplicates(
        ["PersonID", "AddressTypeName", "AddressLine1", "PostalCode"]
    )
)

# Conferência
df_clean_person_address.printSchema()
df_clean_person_address.display()

# Escrita na Silver
(
    df_clean_person_address.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_person_address"
    )
)

# COMMAND ----------

# Purchasing

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

# Garantia de integridade e granularidade
df_clean_purchasing_vendor = (
    df_clean_purchasing_vendor
    .filter(F.col("VendorID").isNotNull())
    .dropDuplicates(["VendorID"])
)

# Conferência
df_clean_purchasing_vendor.printSchema()
df_clean_purchasing_vendor.display()

# Escrita na Silver
(
    df_clean_purchasing_vendor.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_purchasing_vendor"
    )
)

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

# Garantia de integridade e granularidade
df_clean_purchasing_product_vendor = (
    df_clean_purchasing_product_vendor
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("VendorID").isNotNull()
    )
    .dropDuplicates(["ProductID", "VendorID"])
)

# Conferência
df_clean_purchasing_product_vendor.printSchema()
df_clean_purchasing_product_vendor.display()

# Escrita na Silver
(
    df_clean_purchasing_product_vendor.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_purchasing_product_vendor"
    )
)

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

# Garantia de integridade e granularidade
df_clean_purchasing_order = (
    df_clean_purchasing_order
    .filter(F.col("PurchaseOrderID").isNotNull())
    .dropDuplicates(["PurchaseOrderID"])
)

# Conferência
df_clean_purchasing_order.printSchema()
df_clean_purchasing_order.display()

# Escrita na Silver
(
    df_clean_purchasing_order.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_purchasing_order"
    )
)

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

# Garantia de integridade e granularidade
df_clean_purchasing_order_line = (
    df_clean_purchasing_order_line
    .filter(
        F.col("PurchaseOrderID").isNotNull() &
        F.col("PurchaseOrderDetailID").isNotNull()
    )
    .dropDuplicates(
        ["PurchaseOrderID", "PurchaseOrderDetailID"]
    )
)

# Conferência
df_clean_purchasing_order_line.printSchema()
df_clean_purchasing_order_line.display()

# Escrita na Silver
(
    df_clean_purchasing_order_line.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "adventure_works_catalog.silver.clean_purchasing_order_line"
    )
)

# COMMAND ----------

# Production

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

    # Garantia de integridade e granularidade
    .filter(F.col("ProductID").isNotNull())
    .dropDuplicates(["ProductID"])
)

# Conferência
df_clean_production.printSchema()
df_clean_production.display()

# Escrita na Silver
(
df_clean_production.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production")
)

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
    .filter(
        F.col("ProductID").isNotNull() &
        F.col("LocationID").isNotNull()
    )
    .dropDuplicates(["ProductID", "LocationID"])
)

(
    df_clean_production_inventory.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_production_inventory")
)


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

# Garantia de integridade e granularidade
df_clean_production_transaction = (
    df_transaction_current
    .unionByName(df_transaction_archive)
    .filter(F.col("TransactionID").isNotNull())
    .dropDuplicates(["TransactionID"])
)

# Conferência
df_transaction_current.printSchema()
df_transaction_current.display()

# Escrita na Silver
(
df_clean_production_transaction.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production_transaction")
)

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
    .filter(F.col("WorkOrderID").isNotNull())
    .dropDuplicates(["WorkOrderID"])
)

# Conferência
df_clean_production_workorder.display()
df_clean_production_workorder.printSchema()

# Escrita na Silver
(
df_clean_production_workorder.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production_workorder")
)

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
    .filter(
        F.col("ProductAssemblyID").isNotNull() &
        F.col("ComponentProductID").isNotNull()
    )
    .dropDuplicates(
        ["ProductAssemblyID", "ComponentProductID", "StartDate"]
    )
)

# Conferência
df_clean_production_bill_of_materials.display()
df_clean_production_bill_of_materials.printSchema()

# Escrita na Silver
(
df_clean_production_bill_of_materials.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production_bill_of_materials")
)

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
    .filter(F.col("ProductID").isNotNull())
    .dropDuplicates(["ProductID", "StartDate"])
)

# Conferência
df_clean_production_cost_history.display()
df_clean_production_cost_history.printSchema()

# Escrita na Silver
(
df_clean_production_cost_history.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production_cost_history")
)

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
    .filter(F.col("ProductID").isNotNull())
    .dropDuplicates(["ProductID", "StartDate"])
)

# Conferência
df_clean_production_list_price_history.display()
df_clean_production_list_price_history.printSchema()

# Escrita na Silver
(
df_clean_production_list_price_history.write
.format("delta")
.mode("overwrite") 
.saveAsTable("adventure_works_catalog.silver.clean_production_list_price_history")
)

# COMMAND ----------

# Sales

# ====================================
# Sales — Clean Sales Order Header
# ====================================

df_sales_order_header_csv_clean = (
    df_sales_order_header_csv.select(
        F.col("_c0").cast("int").alias("SalesOrderID"),
        F.col("_c2").cast("timestamp").alias("OrderDate"),
        F.col("_c3").cast("timestamp").alias("DueDate"),
        F.col("_c4").cast("timestamp").alias("ShipDate"),
        F.col("_c5").cast("int").alias("Status"),
        F.col("_c10").cast("int").alias("CustomerID"),
        F.col("_c12").cast("int").alias("TerritoryID"),
        F.col("_c18").cast("int").alias("CurrencyRateID"),
        F.col("_c21").cast("decimal(18,2)").alias("TaxAmt"),
        F.col("_c22").cast("decimal(18,2)").alias("Freight")
    )
)

df_sales_order_header_sql_clean = (
    df_sales_order_header_sql.select(
        "SalesOrderID",
        "OrderDate",
        "DueDate",
        "ShipDate",
        "Status",
        "CustomerID",
        "TerritoryID",
        "CurrencyRateID",
        "TaxAmt",
        "Freight"
    )
)

df_clean_sales_order_header = (
    df_sales_order_header_sql_clean
    .unionByName(df_sales_order_header_csv_clean)
    .filter(
        F.col("SalesOrderID").isNotNull() &
        F.col("CustomerID").isNotNull()
    )
    .dropDuplicates(["SalesOrderID"])
)


# Conferência
df_clean_sales_order_header.display()
df_clean_sales_order_header.printSchema()

# Escrita na Silver
(
    df_clean_sales_order_header.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_order_header")
)

# ====================================
# Sales — Clean Sales Order Detail
# ====================================

df_sales_order_detail_csv_clean = (
    df_sales_order_detail_csv.select(
        F.col("_c0").cast("int").alias("SalesOrderID"),
        F.col("_c1").cast("int").alias("SalesOrderDetailID"),
        F.col("_c3").cast("int").alias("OrderQty"),
        F.col("_c4").cast("int").alias("ProductID"),
        F.col("_c6").cast("decimal(18,2)").alias("UnitPrice"),
        F.col("_c7").cast("decimal(18,2)").alias("UnitPriceDiscount"),
        F.col("_c8").cast("decimal(18,2)").alias("LineTotal")
    )
)

df_sales_order_detail_sql_clean = (
    df_sales_order_detail_sql.select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderQty",
        "ProductID",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal"
    )
)

df_clean_sales_order_detail = (
    df_sales_order_detail_sql_clean
    .unionByName(df_sales_order_detail_csv_clean)
    .filter(
        F.col("SalesOrderDetailID").isNotNull() &
        F.col("SalesOrderID").isNotNull() &
        F.col("ProductID").isNotNull()
    )
    .dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])
)


# Conferência
df_clean_sales_order_detail.display()
df_clean_sales_order_detail.printSchema()

# Escrita na Silver
(
    df_clean_sales_order_detail.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_order_detail")
)

# ====================================
# Sales — Clean Sales Customer
# ====================================

df_clean_sales_customer = (
    df_customer.select(
        F.col("customerid").alias("CustomerID"),
        F.col("personid").alias("PersonID"),
        F.col("storeid").alias("StoreID"),
        F.col("territoryid").alias("TerritoryID")
    )
    .filter(F.col("CustomerID").isNotNull())
    .dropDuplicates(["CustomerID"])
)

# Conferência
df_clean_sales_customer.display()
df_clean_sales_customer.printSchema()

# Escrita na Silver
(
    df_clean_sales_customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_customer")
)

# ====================================
# Sales — Clean Sales Store
# ====================================

df_clean_sales_store = (
    df_store.select(
        F.col("businessentityid").alias("StoreID"),
        F.col("name").alias("StoreName"),
        F.col("salespersonid").alias("SalesPersonID")
    )
    .filter(F.col("StoreID").isNotNull())
    .dropDuplicates(["StoreID"])
)

# Conferência
df_clean_sales_store.display()
df_clean_sales_store.printSchema()

# Escrita na Silver
(
    df_clean_sales_store.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_store")
)

# ====================================
# Sales — Clean Sales Territory
# ====================================

df_clean_sales_territory = (
    df_sales_territory.select(
        F.col("territoryid").alias("TerritoryID"),
        F.col("name").alias("TerritoryName"),
        F.col("countryregioncode").alias("CountryRegionCode"),
        F.col("group").alias("TerritoryGroup")
    )
    .filter(F.col("TerritoryID").isNotNull())
    .dropDuplicates(["TerritoryID"])
)

# Conferência
df_clean_sales_territory.display()
df_clean_sales_territory.printSchema()

# Escrita na Silver
(
    df_clean_sales_territory.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_territory")
)

# ====================================
# Sales — Clean Sales Currency
# ====================================

df_clean_sales_currency = (
    df_currency.select(
        F.col("currencycode").alias("CurrencyCode"),
        F.col("name").alias("CurrencyName")
    )
    .filter(F.col("CurrencyCode").isNotNull())
    .dropDuplicates(["CurrencyCode"])
)

# Conferência
df_clean_sales_currency.display()
df_clean_sales_currency.printSchema()

# Escrita na Silver
(
    df_clean_sales_currency.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_currency")
)

# ====================================
# Sales — Clean Sales Currency Rate
# ====================================

df_clean_sales_currency_rate = (
    df_currency_rate.select(
        F.col("currencyrateid").alias("CurrencyRateID"),
        F.col("fromcurrencycode").alias("FromCurrencyCode"),
        F.col("tocurrencycode").alias("ToCurrencyCode"),
        F.col("currencyratedate").alias("CurrencyRateDate"),
        F.col("averagerate").alias("AverageRate"),
        F.col("endofdayrate").alias("EndOfDayRate")
    )
    .filter(F.col("CurrencyRateID").isNotNull())
    .dropDuplicates(["CurrencyRateID"])
)

# Conferência
df_clean_sales_currency_rate.display()
df_clean_sales_currency_rate.printSchema()

# Escrita na Silver
(
    df_clean_sales_currency_rate.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_currency_rate")
)


# ====================================
# Sales — Clean Sales Special Offer
# ====================================

df_clean_sales_special_offer = (
    df_special_offer.select(
        "specialofferid",
        "description",
        "discountpct",
        "type",
        "category",
        "startdate",
        "enddate"
    )
    .dropDuplicates(["specialofferid"])
)

# Conferência
df_clean_sales_special_offer.display()
df_clean_sales_special_offer.printSchema()

# Escrita na Silver
(
    df_clean_sales_special_offer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_special_offer")
)

# ====================================
# Sales — Clean Sales Special Offer Product
# ====================================

df_clean_sales_special_offer_product = (
    df_special_offer_product.select(
        "specialofferid",
        "productid"
    )
    .dropDuplicates(["specialofferid", "productid"])
)

# Conferência
df_clean_sales_special_offer_product.display()
df_clean_sales_special_offer_product.printSchema()

# Escrita na Silver
(
    df_clean_sales_special_offer_product.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_special_offer_product")
)

# ====================================
# Sales — Clean SalesPerson Quota History
# ====================================

df_clean_sales_person_quota_history = (
    df_sales_person_quota_history.select(
        "businessentityid",
        "quotadate",
        "salesquota"
    )
    .dropDuplicates(["businessentityid", "quotadate"])
)

# Conferência
df_clean_sales_person_quota_history.display()
df_clean_sales_person_quota_history.printSchema()

# Escrita na Silver
(
    df_clean_sales_person_quota_history.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_person_quota_history")
)

# ====================================
# Sales — Clean Sales Territory History
# ====================================

df_clean_sales_territory_history = (
    df_sales_territory_history.select(
        "businessentityid",
        "territoryid",
        "startdate",
        "enddate"
    )
    .dropDuplicates(["businessentityid", "territoryid", "startdate"])
)

# Conferência
df_clean_sales_territory_history.display()
df_clean_sales_territory_history.printSchema()

# Escrita na Silver
(
    df_clean_sales_territory_history.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("adventure_works_catalog.silver.clean_sales_territory_history")
)
