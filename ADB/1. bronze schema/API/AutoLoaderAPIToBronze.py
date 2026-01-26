# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract, to_date, col, lit
from datetime import datetime
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, bronze_selection

# COMMAND ----------

def AutoLoaderCurrencyToBronze(checkpoint, tableName):
    source = "/mnt/landingzone/api/Currencies"   
    schemaLocation = f"/mnt/adventureworksproject/checkpoints/bronze/currency/schema{checkpoint}"
    checkpointLocation = f"/mnt/adventureworksproject/checkpoints/bronze/{checkpoint}"
    
    table = f"adventure_works_catalog.bronze.{tableName}"  

    current_run_id = get_run_id()        
    start_time = datetime.now() 

    try:
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "Currency_*.json")
            .option("cloudFiles.schemaLocation", schemaLocation)
            .load(source)
            .withColumn( 
                "CurrencyDate",
                to_date(
                     regexp_extract(
                     input_file_name(),
                     r"Currency_(\d{2}-\d{2}-\d{4})",
                     1
                     ),
                     "MM-dd-yyyy"
                     )
                )
            .withColumn("IngestionDate", current_timestamp())
            .withColumn("SourceFile", input_file_name())
        )

        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointLocation)
            .trigger(availableNow=True)
            .toTable(table)
        )

        #Espera a escrita terminar e capitura os números da ultima execução
        query.awaitTermination()
        
        #Verificação de qualidade após o dado já estar salvo (verifica só o que acabou de entrar)
        df_recent_data = spark.read.table(table).filter(col("IngestionDate") >= lit(start_time)) #filtra pelo inicio da run

        bronze_selection(
            spark=spark,
            run_id=current_run_id,
            df_recent=df_recent_data,
            process_name="Ingestão_Landing_Bronze",
            table_name=tableName,
        )    

        #Registrar os metadados de auditoria
        audit_registration(
            spark=spark,
            run_id = current_run_id,
            process_name="Ingestão_Landing_Bronze",
            layer="BRONZE",
            table_saved = tableName,
            start_date = start_time,
            query_object = query
        )

        print("Success: Currency Bronze Load")

    except Exception as e:
        audit_registration(
            spark=spark,
            run_id = current_run_id,
            process_name="Ingestão_Landing_Bronze",
            layer="BRONZE",
            table_saved = tableName,
            start_date = start_time,
            status="FAIL",
            error_msg=str(e)
            )
                    
        print(f"Error: {e}")

# COMMAND ----------

AutoLoaderCurrencyToBronze("currencyHistorical", "api_currencies")
