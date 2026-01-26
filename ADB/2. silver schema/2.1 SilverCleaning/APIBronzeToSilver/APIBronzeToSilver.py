# Databricks notebook source
from pyspark.sql.functions import current_timestamp, explode, col, from_json, round
from datetime import datetime
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, silver_selection

# COMMAND ----------

def CurrencyToSilver(checkpoint, tableName):
    source_table = "adventure_works_catalog.bronze.api_currencies"
    target_table = f"adventure_works_catalog.silver.{tableName}"
    checkpoint_location = f"/mnt/adventureworksproject/checkpoints/silver/{checkpoint}"

    current_run_id = get_run_id()
    start_time = datetime.now()

    try:
        df_bronze = spark.readStream.format("delta").table(source_table)

        json_schema = "MAP<STRING, DOUBLE>"

        df_silver = (
            df_bronze
            .withColumn("data_structured", from_json(col("data"), json_schema))
            .select(
                col("CurrencyDate").alias("Date"),
                explode(col("data_structured")).alias("CurrencyCode", "ExchangeRate"),
                col("IngestionDate").alias("BronzeIngestionDate")
            )
            .withColumn("ExchangeRate", round(col("ExchangeRate"), 2))
            .withColumn("SilverIngestionDate", current_timestamp())
        )

        query = (
            df_silver.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        #Espera a escrita terminar e capitura os números da ultima execução
        query.awaitTermination()
        
        #Verificação de qualidade após o dado já estar salvo (verifica só o que acabou de entrar)
        df_recent_data = spark.read.table(table).filter(col("SilverIngestionDate") >= lit(start_time)) #filtra pelo inicio da run
        
        silver_selection(
            spark=spark,
            run_id=current_run_id,
            df_recent=df_recent_data,
            process_name="Transferência_Bronze_SilverClean",
            table_name=tableName,
        )

        #Registrar os metadados de auditoria
        audit_registration(
            spark=spark,
            run_id=current_run_id,
            process_name="Transferência_Bronze_SilverClean",
            layer="SILVER",
            table_saved = tableName,
            start_date = start_time,
            query_object = query
        )

        print("Success: Currency Silver Load")

    except Exception as e:
        audit_registration(
            spark=spark,
            run_id=current_run_id,
            process_name="Transferência_Bronze_SilverClean",
            layer="SILVER",
            table_saved = tableName,
            start_date = start_time,
            status="FAIL",
            error_msg=str(e)
            )
        
        print(f"Error: {e}")


# COMMAND ----------

CurrencyToSilver("currency", "api_currency")
