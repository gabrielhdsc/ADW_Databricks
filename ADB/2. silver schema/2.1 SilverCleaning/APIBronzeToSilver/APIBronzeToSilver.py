# Databricks notebook source
from pyspark.sql.functions import current_timestamp, explode, col, from_json, round

# COMMAND ----------

def CurrencyToSilver(checkpoint, tableName):
    source_table = "adventure_works_catalog.bronze.api_currencies"
    target_table = f"adventure_works_catalog.silver.{tableName}"
    checkpoint_location = f"/mnt/adventureworksproject/checkpoints/silver/{checkpoint}"

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

        (
            df_silver.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        print("Success: Currency Silver Load")

    except Exception as e:
        print(f"Error: {e}")


# COMMAND ----------

CurrencyToSilver("currency", "api_currency")
