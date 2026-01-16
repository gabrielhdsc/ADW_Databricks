# Databricks notebook source
# MAGIC %md
# MAGIC OBSERVAÇÃO: 
# MAGIC     O Mount Point é persistente, ou seja, uma vez criado em um determinado Cluster ele permanece nele, mesmo após o Cluster ser desligado. Em outras palavras, fazemos apenas um Mount Point por Cluster.
# MAGIC
# MAGIC Secret Scope (método de segurança):
# MAGIC      O Secret Scope é uma "pasta" criptografada usada para armazenar informações sensíveis de forma organizada e acessível, evitando que elas sejam escritas diretamente nos códigos. O Secret Scope funciona em pares Escopo-Chave(s) e, em nosso caso, é gerenciado pelo próprio Databricks.
# MAGIC
# MAGIC ======================================
# MAGIC
# MAGIC Para verificar se já há um diretório montado com o mesmo diretório/nome que iremos montar.
# MAGIC
# MAGIC "if not any(mount.mountPoint == mount_point_directory for mount in dbutils.fs.mounts())
# MAGIC
# MAGIC - dbutils: Databrick Utilities, objeto nativo do Databricks utilizado para se comunicar com o Compute.
# MAGIC - any(): função nativa do Python que verifica se pelo menos um item de uma lista é True.
# MAGIC - dbutils.fs.mounts(): usa a ferramente "fs" (File System) junto com o método .mounts() para listar tudo que já está montado no ambiente do Databricks.
# MAGIC - mount.mountPoint == mount_point_directory: verifica se o diretório que queremos montar já existe.
# MAGIC

# COMMAND ----------

def MountPoint(container):
    """
    container = nome da pasta (blob) no ADLS2 onde a montagem será feita.
    """
    #1. Variáveis de configurações.
    #Escopo criado anteriormente via CLI para evitar o uso do Key Vault, assim podendo passar a AcessKey do Storage Account diretamente.
    secret_value = dbutils.secrets.get(scope="AdventureWorksSS", key="ADLS2")
    storage_account = "adlsg2desafiados2502dev"
    mount_point_directory = f"/mnt/{container}" #Nome que o diretório terá no FS do Databricks.

    ###################################################################################################################

    #2. Configuração de acesso.
    #Define o protocolo de segurança que o Databricks usará para se conectar com a Storage Account. 
    #Neste caso, utlizando o Windows Azure Storage Blob Secure (WABS)...

    source_protocol = f"wasbs://{container}@{storage_account}.blob.core.windows.net/"
    ###################################################################################################################

    #3. Execução da Montagem
    if not any(mount.mountPoint == mount_point_directory for mount in dbutils.fs.mounts()):
        try: #Tenta montar o diretório...
            dbutils.fs.mount(
                source = source_protocol,
                mount_point = mount_point_directory,
                extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": secret_value}
    )
            print(f"Sucesso! Container '{container}' montado em '{mount_point_directory}'")
        except Exception as e: #Em caso de erro, o armazena em "e".
            print(f"Erro ao montar: {e}")
    else:
        print(f"O diretório '{mount_point_directory}' já está montado!")

# COMMAND ----------

#Chamada da função de Mount Point.
MountPoint("landingzone")
MountPoint("adventureworksproject")

