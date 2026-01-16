# Databricks notebook source
# MAGIC %md
# MAGIC O código a seguir utiliza o método Auto Loader do Spark para ler os arquivos armazenados na Landing Zone do ADLS2 (Storage Account). 
# MAGIC
# MAGIC Nosso código lê cada arquivo da pasta "dadosplanilha" e os grava em tabelas únicas no Unity Catalog. Também salva os metadados dos arquivos lidos/escritos em checkpoints únicos para cada um dos arquivos. 
# MAGIC Para que isso ocorrá, no spark.readStream():
# MAGIC
# MAGIC - **.format("cloudFiles")**: Argumento que "chama" o Auto Loader na leitura dos arquivos. O recurso Auto Loader do Spark é capaz de ler dinamicamente novos arquivos que chegam ao diretório.
# MAGIC - **.option("cloudFiles.useIncrementalListing", "true")**: Força o Auto Loader a usar uma listagem de arquivos baseada na ordem alfabética/temporal para descobrir novos arquivos.
# MAGIC - **.option("pathGlobFilter", file.name)**: Argumento que define um filtro de busca, ao invés de ler a subpasta toda, o read.stream lerá apenas o arquivo que atende ao filtro dentro da pasta.
# MAGIC
# MAGIC - **.option("cloudFiles.schemaLocation")**: Como não definimos previamente um Schema para os CSV's a serem lidos, o Spark automaticamente os infere e os salva no diretório passado neste argumento.
# MAGIC - **.load**: Referenciamos a pasta que contém o arquivo a ser lido.
# MAGIC
# MAGIC Após fazer a leitura do arquivo, adicionamos duas colunas de metadados em cada um deles. Então começamos a escrita dos arquivos na camada Bronze do Databricks:
# MAGIC
# MAGIC - **.format("delta")**: Define que os dados serão salvos no formato Delta Lake.
# MAGIC - **.outputMode("append")**: Determina o comportamento da escrita. Garante que apenas os novos registros processados desde a última execução sejam adicionados ao final da tabela existente, sem apagar o que já estava lá.
# MAGIC - **.option("checkpointLocation", ...)**: É o "diário" do Spark. Ele salva os metadados em uma pasta (JSONs e logs) que registram exatamente quais arquivos já foram lidos.
# MAGIC - **.trigger(availableNow=True)**: O Gatilho de Execução. Ele processa todos os dados disponíveis na origem como um lote, mantendo as garantias de streaming, encerrando a query assim que terminar
# MAGIC - **.toTable(target_table)**: Destino Final. Registra os dados diretamente como uma tabela no Unity Catalog.
# MAGIC

# COMMAND ----------

#Bibliotecas utilizadas para inserir colunas de metadados nos arquivos lidos. 
from pyspark.sql.functions import current_timestamp, input_file_name, col
#Diretórios raiz para os arquivos na LandingZone e pastas de Checkpoints.
checkpoint_dir = f"/mnt/adventureworksproject/checkpoints/bronze/CSV/"
csv_root = "/mnt/landingzone/dadosplanilha/"

# COMMAND ----------

files = dbutils.fs.ls(csv_root)
for file in files:
    #Variáveis de configuração...
    target_table_name = "csv_"+file.name.replace(".csv","")
    target_table = f"adventure_works_catalog.bronze.{target_table_name}"
    specific_checkpoint_dir = f"{checkpoint_dir}{target_table_name}"
    try:
        print(f"Iniciando a leitura do arquivo {file.name}...")
        #2. Leitura do arquivo.
        df_batch = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("pathGlobFilter", file.name)
            .option("header", "False")
            .option("delimiter", ",")
            .option("cloudFiles.schemaLocation", f"{specific_checkpoint_dir}/_schema")
            .option("cloudFiles.useIncrementalListing", "true")
            .load(csv_root)
        )
        #2.1 Inserção de colunas de Metadados ao final da table.
        print("Adicionando colunas de metadados...")
        df_batch = (
            df_batch.
            withColumn("IngestionDate", current_timestamp()).
            withColumn("FileSource", input_file_name())
        )
        #3. Escrita do arquivo.
        print("Iniciando a escrita da table...")
        (
            df_batch.writeStream 
            .format("delta") 
            .outputMode("append")
            .option("checkpointLocation", specific_checkpoint_dir)
            .trigger(availableNow=True)
            .toTable(target_table) 
        )
        print(f"Read e Write concluídos para o arquivo: {file.path}")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
