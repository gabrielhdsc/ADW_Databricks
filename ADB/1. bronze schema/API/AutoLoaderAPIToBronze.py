# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract, to_date

# COMMAND ----------

def AutoLoaderCurrencyToBronze(checkpoint, tableName):
    source = "/mnt/landingzone/api/Currencies"   
    schemaLocation = f"/mnt/adventureworksproject/checkpoints/bronze/currency/schema{checkpoint}"
    checkpointLocation = f"/mnt/adventureworksproject/checkpoints/bronze/{checkpoint}"
    
    table = f"adventure_works_catalog.bronze.{tableName}"   
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

        (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointLocation)
            .trigger(availableNow=True)
            .toTable(table)
        )

        print("Success: Currency Bronze Load")

    except Exception as e:
        print(f"Error: {e}")

# COMMAND ----------

AutoLoaderCurrencyToBronze("currencyHistorical", "api_currencies")
