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

# Leitura das tabelas
df_employee                    = spark.table("adventure_works_catalog.bronze.humanresources_employee")
df_employee_department_history = spark.table("adventure_works_catalog.bronze.humanresources_employeedepartmenthistory")
df_employee_pay_history        = spark.table("adventure_works_catalog.bronze.humanresources_employeepayhistory")
df_department                  = spark.table("adventure_works_catalog.bronze.humanresources_department")
df_shift                       = spark.table("adventure_works_catalog.bronze.humanresources_shift")

# COMMAND ----------

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

# Garantia de Integridade
df_clean_hr_employee = (
    df_clean_hr_employee
    .filter(F.col("EmployeeID").isNotNull())
)

df_clean_hr_employee = deduplicate_by_rule(
    df_clean_hr_employee,
    partition_cols=["EmployeeID"],
    order_cols=[F.col("HireDate").desc()]
)

# COMMAND ----------

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

# Garantia de integridade 
df_clean_hr_employee_org_history = (
    df_clean_hr_employee_org_history
    .filter(
        F.col("EmployeeID").isNotNull() &
        F.col("DepartmentID").isNotNull() &
        F.col("StartDate").isNotNull()
    )
)

df_clean_hr_employee_org_history = deduplicate_by_rule(
    df_clean_hr_employee_org_history,
    partition_cols=["EmployeeID", "DepartmentID", "StartDate"],
    order_cols=[F.col("EndDate").desc()]
)

# COMMAND ----------

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

# Garantia de Integridade
df_clean_hr_employee_pay_history = (
    df_clean_hr_employee_pay_history
    .filter(
        F.col("EmployeeID").isNotNull() &
        F.col("RateChangeDate").isNotNull()
    )
)

df_clean_hr_employee_pay_history = deduplicate_by_rule(
    df_clean_hr_employee_pay_history,
    partition_cols=["EmployeeID", "RateChangeDate"],
    order_cols=[F.col("PayRate").desc()]
)

# COMMAND ----------

# Escrita
silver_tables = [
    (df_clean_hr_employee, "clean_hr_employee"),
    (df_clean_hr_employee_org_history, "clean_hr_employee_org_history"),
    (df_clean_hr_employee_pay_history, "clean_hr_employee_pay_history"),
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

