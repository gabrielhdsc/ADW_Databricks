# Databricks notebook source
#Bibliotecas para manipulação de Strings.
from pyspark.sql.functions import split, regexp_replace, element_at
#Bibliotecas para manipulação de DataFrames.
from pyspark.sql.functions import col, from_json, arrays_zip, url_decode, explode

#Variáveis de configuração.
df_bronze = spark.read.table("adventure_works_catalog.bronze.api_weather_historical")
json_schema = "time ARRAY<STRING>, temperature_2m_mean ARRAY<DOUBLE>, precipitation_sum ARRAY<DOUBLE>, rain_sum ARRAY<DOUBLE>, snowfall_sum ARRAY<DOUBLE>"

# COMMAND ----------

try:
    print("Iniciando a limpeza dos dados...")
    df_silver_clean = (
        df_bronze
        #1 A partir dos registros da coluna "FileSource" captura o nome da cidade, estado e páis.
        .withColumn("metadata", split(regexp_replace(element_at(split(col("FileSource"), "/"), -1), ".json", ""), "_"))
        
        #2 Converte a String 'daily' para um Structured Object usando o método "string DDL".
        .withColumn("daily_struct", from_json(col("daily"), json_schema))
        #2.1 Sincroniza as listas do Structured Object "daily".
        .withColumn("zipped_cols", arrays_zip(
            col("daily_struct.time"), 
            col("daily_struct.temperature_2m_mean"),
            col("daily_struct.precipitation_sum"),
            col("daily_struct.rain_sum"),
            col("daily_struct.snowfall_sum")
        ))
        #2.2 Transforma o array sincronizado em múltiplas linhas
        .withColumn("exploded_cols", explode(col("zipped_cols")))
        #3 Seleciona os campos finais renomeando as colunas
        .select(
            col("metadata")[0].alias("City"),
            col("metadata")[1].alias("State"),
            col("metadata")[2].alias("Country"),
            col("exploded_cols.time").alias("FullDate"),
            col("exploded_cols.temperature_2m_mean").alias("AvgTemp"),
            col("exploded_cols.precipitation_sum").alias("PrecipitationSum"),
            col("exploded_cols.rain_sum").alias("RainSum"),
            col("exploded_cols.snowfall_sum").alias("SnowfallSum"),
        ))
    print("Limpeza concluída. Iniciando a escrita do Dataframe atualizado...")
    #4 Escreve a tabela limpa para a camada Silver.
    (df_silver_clean.write
        .format("delta")
        .mode("append")
        .saveAsTable("adventure_works_catalog.silver.api_weather")
    )
    print("Escrita da Table na Siver. Operação finalizada.")
except Exception as e:
    print(f"Um erro ocorreu: {e}")
