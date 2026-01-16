# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input
# Variáveis de configuração.
input_path = "/mnt/landingzone/api/Weather/CitiesCoordinates"
checkpoint_dir = "/mnt/adventureworksproject/checkpoints/bronze/WeatherAPI/GeocodingAPI"
target_table = "adventure_works_catalog.bronze.api_geocoding"

# COMMAND ----------


try:
    print("Iniciando a leitura do dir...")
    #1. Leitura
    df_batch = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true") #Pois o JSON retorna um objeto estruturado/array.
    .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/_schema")
    .load(input_path))
    
    #2. Inserção de colunas de Metadados ao final da table.
    print("Adicionando colunas de metadados...")
    df_batch = (
        df_batch
        .withColumn("IngestionDate", current_timestamp())
        .withColumn("FileSource", input_file_name())
    )
    
    #2.2 Escrita.
    print("Iniciando a escrita da table...")
    (
        df_batch.writeStream
        .format("delta") 
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(target_table) 
    )
    print(F"CONCLUÍDO!\nRead feito no dir: {input_path}\nTable escrita em: {target_table}")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
