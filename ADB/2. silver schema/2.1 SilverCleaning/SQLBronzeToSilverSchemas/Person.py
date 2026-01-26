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

# COMMAND ----------

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

# Garantia de integridade 
df_clean_person = (
    df_clean_person
    .filter(F.col("PersonID").isNotNull())
)

df_clean_person = deduplicate_by_rule(
    df_clean_person,
    partition_cols=["PersonID"],
    order_cols=[F.col("PersonID").asc()]
)

# COMMAND ----------

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

# Garantia de Integridade
df_clean_person_contact = (
    df_clean_person_contact
    .filter(F.col("PersonID").isNotNull())
)

df_clean_person_contact = deduplicate_by_rule(
    df_clean_person_contact,
    partition_cols=["PersonID", "ContactTypeName", "RelatedPersonID"],
    order_cols=[F.col("PersonID").asc()]
)

# COMMAND ----------

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

# Garantia de integridade 
df_clean_person_contact_method = (
    df_email
    .unionByName(df_phone)
    .filter(F.col("PersonID").isNotNull())
)

df_clean_person_contact_method = deduplicate_by_rule(
    df_clean_person_contact_method,
    partition_cols=["PersonID", "ContactType", "ContactValue"],
    order_cols=[F.col("PersonID").asc()]
)

# COMMAND ----------

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
        # CHAVE NATURAL DO ENDEREÇO
        F.col("bea.AddressID").alias("AddressID"),


        # Relacionamento
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

# Garantia de integridade 
df_clean_person_address = (
    df_clean_person_address
    .filter(F.col("AddressID").isNotNull())
)

df_clean_person_address = deduplicate_by_rule(
    df_clean_person_address,
    partition_cols=["AddressID"],
    order_cols=[F.col("AddressID").asc()]
)

# COMMAND ----------

# Escrita
silver_tables = [
    (df_clean_person, "clean_person"),
    (df_clean_person_contact, "clean_person_contact"),
    (df_clean_person_contact_method, "clean_person_contact_method"),
    (df_clean_person_address, "clean_person_address"),
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
