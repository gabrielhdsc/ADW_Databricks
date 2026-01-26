# Databricks notebook source
# MAGIC %md
# MAGIC O código a seguir utiliza o método Auto Loader do Spark para ler os arquivos armazenados na Landing Zone do ADLS2 (Storage Account). Devido a estrutura do diretório da Ladinzing Zone foi implementado loops FOR para realizar a leitura dinâmica das pastas, subpastas e arquivos.
# MAGIC
# MAGIC Nosso código lê apenas o arquivo mais recente de cada subpasta e os grava em tabelas únicas no Unity Catalog. Também salva os metadados dos arquivos lidos/escritos em checkpoints únicos para cada um dos arquivos. 
# MAGIC Para que isso ocorrá:
# MAGIC
# MAGIC - **.format("cloudFiles")**: Argumento que "chama" o Auto Loader na leitura dos arquivos. O recurso Auto Loader do Spark é capaz de ler dinamicamente novos arquivos que chegam ao diretório.
# MAGIC - **.option("cloudFiles.useIncrementalListing", "true")**: Força o Auto Loader a usar uma listagem de arquivos baseada na ordem alfabética/temporal para descobrir novos arquivos. 
# MAGIC - **.option("pathGlobFilter", latest_file_name)**: Argumento que define um filtro de busca, ao invés de ler a subpasta toda, o read.stream lerá apenas o arquivo que atende ao filtro dentro da pasta.
# MAGIC - **.option("cloudFiles.schemaLocation")**: Como não definimos previamente um Schema para as tabelas a serem lidas, o Spark automaticamente os infere e os salva no diretório passado neste argumento.
# MAGIC - **.load**: Referenciamos a subpasta que contém o arquivo a ser lido.
# MAGIC
# MAGIC Após fazer a leitura do arquivo, adicionamos duas colunas de metadados em cada um deles. Então começamos a escrita dos arquivos na camada Bronze do Databricks:
# MAGIC
# MAGIC - **.format("delta")**: Define que os dados serão salvos no formato Delta Lake.
# MAGIC - **.outputMode("append")**: Determina o comportamento da escrita. Garante que apenas os novos registros processados desde a última execução sejam adicionados ao final da tabela existente, sem apagar o que já estava lá.
# MAGIC - **.option("checkpointLocation")**: É o "diário" do Spark. Ele salva os metadados em uma pasta (JSONs e logs) que registram exatamente quais arquivos já foram lidos.
# MAGIC - **.trigger(availableNow=True)**: O Gatilho de Execução. Ele processa todos os dados disponíveis na origem como um lote, mantendo as garantias de streaming, encerrando a query assim que terminar
# MAGIC - **.toTable(target_table)**: Destino Final. Registra os dados diretamente como uma tabela no Unity Catalog.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Bibliotecas utilizadas para inserir colunas de metadados nos arquivos lidos. 
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col
from datetime import datetime
from ADB.Module.GovernanceUtils import audit_registration, get_run_id, bronze_selection

#Diretórios raiz para os arquivos na LandingZone e pastas de Checkpoints.
checkpoint_dir = f"/mnt/adventureworksproject/checkpoints/bronze/SQL/"
sql_root = dbutils.fs.ls("/mnt/landingzone/azuresql/")

# COMMAND ----------

#Primeiro Nível: Lista as 5 pastas principais do SQL.
for folder in sql_root:

    #Segundo Nível: Lista as 5 subpastas dentro de cada pasta principal
    sub_folder = dbutils.fs.ls(folder.path)

    for sub in sub_folder:
        current_run_id = get_run_id()
        start_time = datetime.now()

        #Variáveis de configuração...
        target_table_name = folder.name.replace("/","_")+sub.name.replace("/","")
        target_table = f"adventure_works_catalog.bronze.{target_table_name}"
        specific_checkpoint_dir = f"{checkpoint_dir}{target_table_name}"

        #Terceiro Nível: Lista os arquivos dentro de cada subpasta.
        files = dbutils.fs.ls(sub.path)

        #Queremos apenas o arquivo com a data mais recente.
        latest_file = max(files, key=lambda f: f.modificationTime)
        latest_file_name = latest_file.name
        latest_file_path = latest_file.path

        #2. Leitura do arquivo.
        try:
            df_batch = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet") 
                .option("pathGlobFilter", latest_file_name)
                .option("cloudFiles.schemaLocation", f"{specific_checkpoint_dir}/_schema")
                .option("cloudFiles.useIncrementalListing", "true")
                .load(sub.path) 
            )
            print(f"Lendo do Mount: {latest_file_path}...")

            #2.1 Inserção de colunas de Metadados ao final da table.
            df_batch = (
                df_batch.
                withColumn("IngestionDate", current_timestamp()).
                withColumn("FileSource", input_file_name())
            )

            #3. Escrita do arquivo.
            query = (
                df_batch.writeStream
                .format("delta")
                .option("checkpointLocation", specific_checkpoint_dir)
                .option("mergeSchema", "true") 
                .outputMode("append")
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
                table_name=target_table_name,
            )

            #Registrar os metadados de auditoria
            audit_registration(
                spark=spark,
                run_id = current_run_id,
                process_name="Ingestão_Landing_Bronze",
                layer="BRONZE",
                table_saved = target_table_name,
                start_date = start_time,
                query_object = query
            )

            print(f"Gravando em: {target_table}...")
        
        except Exception as e:
            audit_registration(
                spark=spark,
                run_id = current_run_id,
                process_name="Ingestão_Landing_Bronze",
                layer="BRONZE",
                table_saved = target_table_name,
                start_date = start_time,
                status="FAIL",
                error_msg=str(e)
            )

            print(f"Erro: {e}")
            
