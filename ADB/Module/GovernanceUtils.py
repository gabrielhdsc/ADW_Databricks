from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import DataFrame
from datetime import datetime
import uuid 
import json


#Gera um ID unico de execução manual ou pega o ID oficial do job executado no Databricks
def get_run_id():    
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

        if context.runId().isDefined():
            job_run_id = context.runId().toJson()
            run_id_value = json.loads(job_run_id).get("runID")
            return f"Job_{run_id_value}"

    # caso o dbutils falhe vai para o registro manual
    except Exception:
        pass
    
    return f"Manual_{uuid.uuid4().hex[:8]}"



#Registrar autioría nas tabelas da bronze e silver
def audit_registration(spark, run_id, process_name, layer, table_saved, start_date, query_object = None, rows_readed_batch = None, rows_written_batch = None, status = "SUCESS", error_msg = None):
    
    #Define o enddate cna hora da execução da função
    end_date = datetime.now()

    # tratamento da msg de erro
    if error_msg:
        error_msg_clean = str(error_msg).replace("'", '"')[:500]
    
    else:
        error_msg_clean = ""
        
    rows_readed = 0
    rows_written = 0

    #Registro para streaming com query object
    if query_object and query_object.lastProgress:
        last_execution = query_object.lastProgress
        rows_readed = last_execution.get("numInputRows", 0)
        rows_written = last_execution.get("sink", {}).get("numOutputRows", 0)

    #Registro para batch
    else:
        if rows_readed_batch is not None:
            rows_readed = rows_readed_batch

        if rows_written_batch is not None:
            rows_written = rows_written_batch

    #Insere na tabela via SQL
    sql_insert = f"""
        INSERT INTO adventure_works_catalog.governance.data_lineage 
        VALUES (
            '{run_id}', '{process_name}', '{layer}', '{table_saved}', 
            '{start_date}', '{end_date}', {rows_readed}, {rows_written}, '{status}', '{error_msg_clean}'
        )
    """

    spark.sql(sql_insert)

    print(f"Log registrado para {table_saved} -> {status}. Linhas lidas: {rows_readed}, linhas escritas: {rows_written}.")





#Verifica a presença de nulos e dados duplicados em colunas específicas e registra o resultado.
def quality_verification(spark, run_id, df_input, process_name, layer, table_name, columns_list, check_type):

    total_rows = df_input.count()
    if total_rows == 0:
        return   #Se o df estiver vazio aborta aqui

    #Mantem no fomarmato de lista ainda que seja so uma coluna
    if isinstance(columns_list, str):
        columns_list = [columns_list]


    for column_name in columns_list:
        failed_rows = 0

        #Checagem de nulos, vazios e NaN na coluna indicada
        if check_type == "NULL":
            failed_rows = df_input.filter(
                F.col(column_name).isNull() |
                (F.trim(F.col(column_name).cast("string")) == "")
            ).count()

        #Checagem de duplicatas na coluna indicada
        elif check_type == "DUPLICATE":
            distinct_count = df_input.select(column_name).distinct().count()
            failed_rows = total_rows - distinct_count

        #Definição de status
        if failed_rows == 0:
            status = "SUCCESS"
        else:
            status = "FAIL"

        spark.sql(f"""
            INSERT INTO adventure_works_catalog.governance.data_quality
            VALUES (
                '{run_id}', '{process_name}', '{table_name}', '{column_name}', '{layer}', '{check_type}', {total_rows}, {failed_rows}, '{status}', current_timestamp()
            )
        """)

        if failed_rows > 0:
            print(f"Atenção: Verificar {table_name}, coluna {column_name}. {failed_rows} linhas com falha para o teste {check_type}.")

    print(f"Verificação de qualidade {check_type} concluída para {table_name}.")




#Aplica a verificação de qualidade com base nas tabelas e colunas listadas da bronze
def bronze_selection(spark, run_id, df_recent, process_name, table_name):

    #Pega as colunas definidas para checagem para a tabela sendo lida
    columns = columns_to_check_bronze()
    cols_to_check = columns.get(table_name.lower()) #garante que ache a chave mesmo se vier maiúsculo

    if not cols_to_check:
        print(f"Nenhuma regra configurada para a tabela '{table_name}'. Pulando qualidade.")
        return


    #VERIFICAÇÂO NULOS
    #Pega as colunas selecionadas para checagem de nulos
    cols_null = cols_to_check.get("check_null", [])

    # Adiciona para checagem colunas padrão em todas as tabelas, como colunas de metadados (se existirem no df)
    custom_cols = ["FileSource", "IngestionDate"]
    for p in custom_cols:
        if p in df_recent.columns and p not in cols_null:
            cols_null.append(p)

    # Filtra apenas colunas que existem no df
    valid_cols_null = []
    for c in cols_null:
        if c in df_recent.columns:
            valid_cols_null.append(c)

    if not valid_cols_null:
        print(f"Nenhuma coluna configurada para validar em {table_name}.")
        return

    #Aplica a verificação
    if valid_cols_null:
        quality_verification(
            spark=spark,
            run_id=run_id,
            df_input=df_recent,
            process_name=process_name,
            layer="BRONZE",
            table_name=table_name,
            columns_list=valid_cols_null,
            check_type="NULL"
        )

    else:
        #NÃO usa return, para permitir que o código siga para checar duplicatas
        print(f"Nenhuma coluna válida encontrada para checagem de NULL em {table_name}.")


    #VERIFICAÇÂO DUPLICATAS
    #Pega as colunas selecionadas para checagem de duplicatas
    cols_dup = cols_to_check.get("check_dup", [])

    # Filtra apenas colunas que existem no df
    valid_cols_dup = []
    for c in cols_dup:
        if c in df_recent.columns:
            valid_cols_dup.append(c)

    if not valid_cols_dup:
        print(f"Nenhuma coluna configurada para validar em {table_name}.")
        return

    if valid_cols_dup:
        quality_verification(
            spark=spark,
            run_id=run_id,
            df_input=df_recent,
            process_name=process_name,
            layer="BRONZE",
            table_name=table_name,
            columns_list=valid_cols_dup,
            check_type="DUPLICATE"
        )



#Aplica a verificação de qualidade com base nas tabelas e colunas listadas da prata
def silver_selection(spark, run_id, df_input, process_name, table_name):

    columns = columns_to_check_silver()
    cols_to_check = columns.get(table_name) # table_name deve vir limpo ex: 'clean_hr_employee'

    if not cols_to_check:
        print(f"Nenhuma regra configurada para a tabela '{table_name}'. Pulando qualidade.")
        return

    #VERIFICAÇÂO NULOS
    #Pega as colunas selecionadas para checagem de nulos
    cols_null = cols_to_check.get("check_null", [])

    # Filtra apenas colunas que existem no df
    valid_cols_null = []
    for c in cols_null:
        if c in df_input.columns:
            valid_cols_null.append(c)

    if not valid_cols_null:
        print(f"Nenhuma coluna configurada para validar em {table_name}.")
        return
    
    #Aplica a verificação
    if valid_cols_null:
        quality_verification(
            spark=spark, 
            run_id=run_id, 
            df_input=df_input,
            process_name=process_name,
            layer="SILVER",
            table_name=table_name,
            columns_list=valid_cols_null,
            check_type="NULL"
        )

    #VERIFICAÇÂO DUPLICATAS
    #Pega as colunas selecionadas para checagem de duplicatas
    cols_dup = cols_to_check.get("check_dup", [])

    # Filtra apenas colunas que existem no df
    valid_cols_dup = []
    for c in cols_dup:
        if c in df_input.columns:
            valid_cols_dup.append(c)

    if not valid_cols_dup:
        print(f"Nenhuma coluna configurada para validar em {table_name}.")
        return
    
    if valid_cols_dup:
        quality_verification(
            spark=spark,
            run_id=run_id,
            df_input=df_input,
            process_name=process_name,
            layer="SILVER",
            table_name=table_name,
            columns_list=valid_cols_dup,
            check_type="DUPLICATE")



#tabelas, colunas e tipo de checkagem que serão verificas na ingestão de todas as tabelas da bronze
def columns_to_check_bronze():
    return {
        #"nome_tabela": {#"check_null": ["coluna_obrigatoria_1", "coluna_obrigatoria_2"], "check_dup":  ["chave_primaria"]}

        "person_address":{"check_null": ["AddressID", "AddressLine1"], "check_dup": ["AddressID"]},
        "person_addresstype":{"check_null": ["AddressTypeID", "Name"], "check_dup": ["AddressTypeID"]},
        "person_businessentity":{"check_null": ["BusinessEntityID"], "check_dup": ["BusinessEntityID"]},
        "person_businessentityaddress":{"check_null": ["BusinessEntityID", "AddressID", "AddressTypeID"], "check_dup": ["BusinessEntityID"]},
        "person_businessentitycontact":{"check_null": ["BusinessEntityID", "PersonID", "ContactTypeID"], "check_dup": ["BusinessEntityID"]},
        "person_contacttype":{"check_null": ["ContactTypeID", "Name"], "check_dup": ["ContactTypeID"]},
        "person_countryregion":{"check_null": ["CountryRegionCode", "Name"], "check_dup": ["CountryRegionCode"]},
        "person_emailaddress":{"check_null": ["BusinessEntityID", "EmailAddressID", "EmailAddress"], "check_dup": ["EmailAddressID"]},
        "person_password":{"check_null": ["BusinessEntityID", "PasswordHash", "PasswordSalt"], "check_dup": ["BusinessEntityID"]},
        "person_person":{"check_null": ["BusinessEntityID", "PersonType"], "check_dup": ["BusinessEntityID"]},
        "person_personphone":{"check_null": ["BusinessEntityID", "PhoneNumber", "PhoneNumberTypeID"], "check_dup": ["BusinessEntityID", "PhoneNumber"]},
        "person_phonenumbertype":{"check_null": ["PhoneNumberTypeID", "Name"],"check_dup": ["PhoneNumberTypeID"]},
        "person_stateprovince":{"check_null": ["StateProvinceID", "StateProvinceCode", "CountryRegionCode"],"check_dup": ["StateProvinceID"]},

        "humanresources_department":{"check_null": ["DepartmentID", "Name"],    "check_dup":["DepartmentID"]},
        "humanresources_employee":{"check_null": ["BusinessEntityID", "NationalIDNumber", "JobTitle", ], "check_dup": ["BusinessEntityID"]},
        "humanresources_employeedepartmenthistory":{"check_null": ["BusinessEntityID", "DepartmentID", "ShiftID"], "check_dup": ["BusinessEntityID"]},
        "humanresources_employeepayhistory":{"check_null": ["BusinessEntityID", "RateChangeDate"],"check_dup": ["BusinessEntityID"]},
        "humanresources_jobcandidate":{"check_null": ["JobCandidateID"],"check_dup": ["JobCandidateID"]},
        "humanresources_shift":{"check_null": ["ShiftID", "Name", "StartTime", "EndTime"],"check_dup": ["ShiftID"]},

        "production_billofmaterials":{"check_null": ["BillOfMaterialsID", "ComponentID"],"check_dup": ["BillOfMaterialsID"]},
        "production_culture":{"check_null": ["Name",], "check_dup": []},
        "production_illustration":{"check_null": ["IllustrationID"],"check_dup": ["IllustrationID"]},
        "production_location":{"check_null": ["LocationID", "Name"], "check_dup": ["LocationID"]},
        "production_product":{"check_null": ["ProductID", "Name", "ProductNumber"],"check_dup": ["ProductID"]},
        "production_productcategory":{"check_null": ["ProductCategoryID", "Name"],"check_dup": ["ProductCategoryID"]},
        "production_productcosthistory":{"check_null": ["ProductID", "StartDate", "StandardCost"],"check_dup": ["ProductID"]},
        "production_productdescription":{"check_null": ["ProductDescriptionID"],"check_dup": ["ProductDescriptionID"]},
        "production_productinventory":{"check_null": ["ProductID", "LocationID", "Shelf", "Bin", "Quantity"],"check_dup": ["ProductID"]},
        "production_productlistpricehistory":{"check_null": ["ProductID", "StartDate", "ListPrice"],"check_dup": ["ProductID"]},
        "production_productmodel":{"check_null": ["ProductModelID", "Name"],"check_dup": ["ProductModelID"]},
        "production_productmodelillustration":{"check_null": ["ProductModelID", "IllustrationID"],"check_dup": ["ProductModelID"]},
        "production_productmodelproductdescriptionculture":{"check_null": ["ProductModelID", "ProductDescriptionID"],"check_dup": ["ProductDescriptionID"]},
        "production_productphoto":{"check_null": ["ProductPhotoID"],"check_dup": ["ProductPhotoID"]},
        "production_productproductphoto":{"check_null": ["ProductID", "ProductPhotoID"],"check_dup": ["ProductPhotoID"]},
        "production_productreview":{"check_null": ["ProductReviewID", "ProductID", "ReviewDate", "Rating"],"check_dup": ["ProductReviewID"]},
        "production_productsubcategory":{"check_null": ["ProductSubcategoryID", "ProductCategoryID", "Name"],"check_dup": ["ProductSubcategoryID"]},
        "production_scrapreason":{"check_null": ["ScrapReasonID", "Name"],"check_dup": ["ScrapReasonID"]},
        "production_transactionhistory":{"check_null": ["TransactionID", "ProductID", "ReferenceOrderID", "ReferenceOrderLineID", "TransactionType"],"check_dup": ["TransactionID"]},
        "production_transactionhistoryarchive":{"check_null": ["TransactionID", "ProductID", "ReferenceOrderID", "ReferenceOrderLineID", "TransactionDate"],"check_dup": ["TransactionID"]},
        "production_unitmeasure":{"check_null": ["UnitMeasureCode", "Name"],"check_dup": ["UnitMeasureCode"]},
        "production_workorder":{"check_null": ["WorkOrderID", "ProductID", "OrderQty", "StockedQty"],"check_dup": ["WorkOrderID"]},
        "production_workorderrouting":{"check_null": ["WorkOrderID", "ProductID", "LocationID", "ActualStartDate", "ActualCost"],"check_dup": ["WorkOrderID"]},

        "purchasing_productvendor":{"check_null": ["ProductID", "BusinessEntityID", "StandardPrice", "LastReceiptCost", "LastReceiptDate"],"check_dup": []},
        "purchasing_purchaseorderdetail":{"check_null": ["PurchaseOrderID", "PurchaseOrderDetailID", "OrderQty", "ProductID", "UnitPrice", "LineTotal"],"check_dup": ["PurchaseOrderDetailID"]},
        "purchasing_purchaseorderheader":{"check_null": ["PurchaseOrderID", "Status", "EmployeeID", "VendorID", "ShipMethodID", "TaxAmt", "Freight", "TotalDue"],"check_dup": ["PurchaseOrderID"]},
        "purchasing_shipmethod":{"check_null": ["ShipMethodID", "Name", "ShipBase", "ShipRate"],"check_dup": ["ShipMethodID"]},
        "purchasing_vendor":{"check_null": ["BusinessEntityID", "AccountNumber", "Name"],"check_dup": ["BusinessEntityID"]},

        "sales_countryregioncurrency":{"check_null": ["CountryRegionCode", "CurrencyCode"],"check_dup": ["CountryRegionCode"]},
        "sales_creditcard":{"check_null": ["CreditCardID", "CardType", "CardNumber"],"check_dup": ["CreditCardID"]},
        "sales_currency":{"check_null": ["CurrencyCode", "Name"],"check_dup": ["CurrencyCode"]},
        "sales_currencyrate":{"check_null": ["CurrencyRateID", "CurrencyRateDate", "FromCurrencyCode", "ToCurrencyCode", "AverageRate", "EndOfDayRate"],"check_dup": ["CurrencyRateID"]},
        "sales_customer":{"check_null": ["CustomerID", "StoreID", "TerritoryID", "AccountNumber"],"check_dup": ["CustomerID"]},
        "sales_personcreditcard":{"check_null": ["BusinessEntityID", "CreditCardID"],"check_dup": ["CreditCardID"]},
        "sales_salesorderdetail":{"check_null": ["SalesOrderID", "SalesOrderDetailID", "OrderQty", "ProductID", "SpecialOfferID", "UnitPrice", "UnitPriceDiscount", "LineTotal"],"check_dup": ["SalesOrderDetailID"]},
        "sales_salesorderheader":{"check_null": ["SalesOrderID", "OrderDate", "DueDate", "ShipDate", "SalesOrderNumber", "CustomerID", "TerritoryID", "BillToAddressID", "ShipToAddressID", "ShipMethodID", "CreditCardID", "SubTotal", "TaxAmt", "Freight", "TotalDue"],"check_dup": ["SalesOrderID"]},
        "sales_salesorderheadersalesreason":{"check_null": ["UnitMeasureCode", "SalesReasonID"]},
        "sales_salesperson":{"check_null": ["BusinessEntityID", "Bonus", "CommissionPct", "SalesYTD", "SalesLastYear"],"check_dup": ["BusinessEntityID"]},
        "sales_salespersonquotahistory":{"check_null": ["BusinessEntityID", "QuotaDate", "SalesQuota"]},
        "sales_salesreason":{"check_null": ["SalesReasonID", "Name", "ReasonType"],"check_dup": ["SalesReasonID"]},
        "sales_salestaxrate":{"check_null": ["SalesTaxRateID", "StateProvinceID", "TaxType", "TaxRate"],"check_dup": ["SalesTaxRateID"]},
        "sales_salesterritory":{"check_null": ["TerritoryID", "Name", "CountryRegionCode", "SalesYTD", "SalesLastYear", "CostYTD", "CostLastYear"],"check_dup": ["TerritoryID"]},
        "sales_salesterritoryhistory":{"check_null": ["BusinessEntityID", "TerritoryID", "StartDate"],"check_dup": ["BusinessEntityID"]},
        "sales_shoppingcartitem":{"check_null": ["ShoppingCartItemID", "ShoppingCartID", "Quantity", "ProductID", "DateCreated"],"check_dup": ["ShoppingCartItemID"]},
        "sales_specialoffer":{"check_null": ["SpecialOfferID", "Description", "DiscountPct", "Type", "Category", "StartDate"],"check_dup": ["SpecialOfferID"]},
        "sales_specialofferproduct":{"check_null": ["SpecialOfferID", "ProductID"],"check_dup": ["ProductID"]},
        "sales_store":{"check_null": ["BusinessEntityID", "Name", "SalesPersonID"],"check_dup": ["BusinessEntityID"]},

        "api_currencies":{"check_null": ["CurrencyDate", "data"],"check_dup": ["CurrencyDate"]},
        "api_geocoding":{"check_null": ["country", "name", "lat", "lon"],"check_dup": ["name"]},
        "api_weather_historical":{"check_null": ["latitude", "longitude", "daily"]},
        "api_weather_latest":{"check_null": ["latitude", "longitude", "daily"]},

        "csv_salesorderdetail_2010_2021":{"check_null": ["_c1"],"check_dup": ["_c1"]},
        "csv_salesorderheader_2010_2021":{"check_null": ["_c0"],"check_dup": ["_c0"]}
        }


#tabelas, colunas e tipo de checkagem que serão verificas na ingestão de todas as tabelas da prata
def columns_to_check_silver():
    return {
        #usar os nomes já renomeados no script do notebook (Alias)
        "clean_hr_employee":{"check_null": ["EmployeeID", "JobTitle", "HireDate"],"check_dup": ["EmployeeID"]},
        "clean_hr_employee_org_history":{"check_null": ["EmployeeID", "DepartmentID", "ShiftID", "StartDate"],"check_dup": ["EmployeeID"]},
        "clean_hr_employee_pay_history":{"check_null": ["EmployeeID", "RateChangeDate", "PayRate", "JobTitle"],"check_dup": ["EmployeeID"]},

        "clean_person":{"check_null": ["PersonID", "PersonType", "FirstName", "LastName"],"check_dup": ["PersonID"]},
        "clean_person_address":{"check_null": ["AddressID", "PersonID", "AddressLine1"],"check_dup": ["PersonID"]},
        "clean_person_contact":{"check_null": ["PersonID", "RelatedPersonID", "ContactTypeName"],"check_dup": ["PersonID"]},
        "clean_person_contact_method":{"check_null": ["PersonID", "ContactType", "ContactValue"]},

        "clean_production":{"check_null": ["ProductID", "ProductNumber", "ProductName"],"check_dup": ["ProductID"]},
        "clean_production_bill_of_materials":{"check_null": ["ProductAssemblyID", "ComponentProductID", "PerAssemblyQuantity"]},
        "clean_production_cost_history":{"check_null": ["ProductID", "StartDate", "StandardCost"]},
        "clean_production_inventory":{"check_null": ["ProductID", "LocationID", "LocationName", "LocationCostRate", "LocationAvailability"],"check_dup": ["LocationID"]},
        "clean_production_list_price_history":{"check_null": ["ProductID", "StartDate", "ListPrice"]},
        "clean_production_transaction":{"check_null": ["TransactionID", "ProductID", "ReferenceOrderID", "ReferenceOrderLineID", "TransactionDate", "ActualCost"],"check_dup": ["TransactionID"]},
        "clean_production_workorder":{"check_null": ["WorkOrderID", "ProductID", "StartDate", "OrderQuantity"],"check_dup": ["WorkOrderID"]},

        "clean_purchase_order":{"check_null": ["PurchaseOrderID", "VendorID", "OrderStatus", "OrderDate", "SubTotal", "TaxAmount", "Freight", "TotalDue", "ShipBase", "ShipRate"],"check_dup": ["PurchaseOrderID"]},
        "clean_purchase_order_line":{"check_null": ["PurchaseOrderID", "PurchaseOrderDetailID", "ProductID", "OrderQuantity", "UnitPrice", "LineTotal"],"check_dup": ["PurchaseOrderDetailID"]},

        "clean_purchasing_order":{"check_null": ["PurchaseOrderID", "VendorID", "OrderStatus", "OrderDate", "SubTotal", "TaxAmount", "Freight", "TotalDue", "ShipBase", "ShipRate"],"check_dup": ["PurchaseOrderID"]},
        "clean_purchasing_order_line":{"check_null": ["PurchaseOrderID", "PurchaseOrderDetailID", "ProductID", "OrderQuantity", "UnitPrice", "LineTotal"],"check_dup": ["PurchaseOrderDetailID"]},
        "clean_purchasing_product_vendor":{"check_null": ["ProductID", "VendorID", "StandardPrice", "LastReceiptCost", "UnitMeasureCode"],"check_dup": ["ProductID"]},
        "clean_purchasing_vendor":{"check_null": ["VendorID", "AccountNumber", "CreditRating"],"check_dup": ["VendorID"]},

        "clean_sales_currency":{"check_null": ["CurrencyCode", "CurrencyName"],"check_dup": ["CurrencyCode"]},
        "clean_sales_currency_rate":{"check_null": ["CurrencyRateID", "CurrencyRateDate", "AverageRate", "EndOfDayRate"],"check_dup": ["CurrencyRateID"]},
        "clean_sales_customer":{"check_null": ["CustomerID", "StoreID", "TerritoryID"],"check_dup": ["CustomerID"]},
        "clean_sales_order_detail":{"check_null": ["SalesOrderID", "SalesOrderDetailID", "ProductID", "SpecialOfferID", "UnitPrice", "UnitPriceDiscount", "LineTotal"],"check_dup": ["SalesOrderDetailID"]},
        "clean_sales_order_header":{"check_null": ["SalesOrderID", "OrderDate", "SalesOrderNumber", "PurchaseOrderNumber", "CustomerID", "SalesPersonID", "TerritoryID", "SubTotal", "TaxAmt", "Freight", "TotalDue"],"check_dup": ["SalesOrderID"]},
        "clean_sales_person_quota_history":{"check_null": ["businessentityid", "quotadate", "salesquota"]},
        "clean_sales_special_offer":{"check_null": ["specialofferid", "description", "discountpct", "category"],"check_dup": ["specialofferid"]},
        "clean_sales_special_offer_product":{"check_null": ["specialofferid", "productid"],"check_dup": ["productid"]},
        "clean_sales_store":{"check_null": ["StoreID", "SalesPersonID"],"check_dup": ["StoreID"]},
        "clean_sales_territory":{"check_null": ["TerritoryID", "CountryRegionCode"],"check_dup": ["TerritoryID"]},
        "clean_sales_territory_history":{"check_null": ["businessentityid", "territoryid", "startdate"]},

        "dim_currency": {"check_null": ["CurrencyRateID", "FromCurrencyCode", "ToCurrencyCode", "CurrencyRateDate", "EndOfDayRate"], "check_dup":  ["CurrencyRateID"]},

        "dim_customer": {"check_null": ["CustomerID", "StoreID"], "check_dup":  ["CustomerID"]},
        "dim_date": {"check_null": ["FullDate"], "check_dup":  ["FullDate"]},
        "dim_location": {"check_null": ["AddressID", "AddressLine1"], "check_dup":  ["AddressID"]},
        "dim_product": {"check_null": ["ProductID", "ProductName"], "check_dup":  ["ProductID"]},
        "dim_supplier": {"check_null": ["SupplierID", "CreditRating"], "check_dup":  ["SupplierID"]},
        "dim_territory": {"check_null": ["TerritoryID", "CountryRegionCode"], "check_dup":  ["TerritoryID"]},

        "fact_purchases": {"check_null": ["PurchaseID", "OrderID", "SubTotal","TaxAmt", "ShippingCost", "TotalDue"], "check_dup":  ["PurchaseID"]},
        "fact_sales_order": {"check_null": ["OrderID", "ShipToAddressID", "SubTotal"], "check_dup":  ["OrderID"]},
        "fact_sales_order_detail": {"check_null": ["SalesID", "OrderID", "LineTotal"], "check_dup":  ["SalesID"]}
    }