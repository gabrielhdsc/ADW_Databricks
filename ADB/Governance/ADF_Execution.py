# Databricks notebook source
from datetime import datetime
from pyspark.sql import functions as F

#Captura os parâmetros enviados pelo ADF via Widgets
dbutils.widgets.text("adf_run_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("trigger_name", "")
dbutils.widgets.text("trigger_time", "")
dbutils.widgets.text("status", "")
dbutils.widgets.text("error_message", "")

#Atribui variáveis
adf_run_id    = dbutils.widgets.get("adf_run_id")
pipeline_name = dbutils.widgets.get("pipeline_name")
trigger_name  = dbutils.widgets.get("trigger_name")
trigger_time_str = dbutils.widgets.get("trigger_time")
status        = dbutils.widgets.get("status")
error_message = dbutils.widgets.get("error_message")

current_time = datetime.now()

# Tratamento de timestamp do ADF (que vem como string ISO)
try:
    #trata o Z do formato UTC do ADF (Ex:2026-02-11T14:00:00Z)
    trigger_time = datetime.strptime(trigger_time_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")

except:
    trigger_time = current_time # Fallback

# 3. Gravação na Tabela Delta
print(f"Registrando Log de Orquestração: {pipeline_name} | Status: {status}")

if status == "STARTED":

    spark.sql(f"""
        INSERT INTO adventure_works_catalog.governance.ADF_logs
        VALUES (
            '{adf_run_id}', 
            '{pipeline_name}', 
            '{trigger_name}', 
            '{trigger_time}', 
            current_timestamp(), 
            NULL,
            'STARTED', 
            NULL
        )
    """)

else:
    error_clean = error_message.replace("'", "''") if error_message else ""
    error_sql_value = f"'{error_clean}'" if error_clean else "NULL"

    # Atualiza Status, Mensagem de Erro e END_TIME
    spark.sql(f"""
        UPDATE adventure_works_catalog.governance.ADF_logs
        SET 
            Status = '{status}',
            EndTime = current_timestamp(),
            ErrorMessage = {error_sql_value}
        WHERE ADFRunID = '{adf_run_id}'
    """)
    

print("Log registrado com sucesso.")
