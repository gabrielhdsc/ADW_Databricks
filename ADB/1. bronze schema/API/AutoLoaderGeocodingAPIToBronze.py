# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col
from datetime import datetime
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, bronze_selection

# Variáveis de configuração.
input_path = "/mnt/landingzone/api/Weather/CitiesCoordinates"
checkpoint_dir = "/mnt/adventureworksproject/checkpoints/bronze/WeatherAPI/GeocodingAPI"
target_table = "adventure_works_catalog.bronze.api_geocoding"

# COMMAND ----------


current_run_id = get_run_id()
start_time = datetime.now()
short_table_name = "api_geocoding"

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

    query = (
        df_batch.writeStream
        .format("delta") 
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(target_table) 
    )

    #Espera a escrita terminar e capitura os números da ultima execução
    query.awaitTermination()
    
    #Verificação de qualidade após o dado já estar salvo (verifica só o que acabou de entrar)
    df_recent_data = spark.read.table(target_table).filter(col("IngestionDate") >= lit(start_time)) #filtra pelo inicio da run

    bronze_selection(
        spark=spark,
        run_id=current_run_id,
        df_recent=df_recent_data,
        process_name="Ingestão_Landing_Bronze",
        table_name=short_table_name,
    )

    #Registrar os metadados de auditoria
    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Ingestão_Landing_Bronze",
        layer="BRONZE",
        table_saved = short_table_name,
        start_date = start_time,
        query_object = query
    )

    print(F"CONCLUÍDO!\nRead feito no dir: {input_path}\nTable escrita em: {target_table}")

except Exception as e:
    audit_registration(
        spark=spark,
        run_id=current_run_id,
        process_name="Ingestão_Landing_Bronze",
        layer="BRONZE",
        table_saved = short_table_name,
        start_date = start_time,
        status="FAIL",
        error_msg=str(e)
        )
        
    print(f"Ocorreu um erro: {e}")
