from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Garantia de integridade
def deduplicate_by_rule(df, partition_cols, order_cols):
    """
    Deduplicação determinística baseada em regra de negócio.

    :param df: DataFrame de entrada
    :param partition_cols: lista de colunas que definem a granularidade
    :param order_cols: lista de colunas que definem prioridade
    """

    window = Window.partitionBy(*partition_cols).orderBy(*order_cols)

    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )



# Criação de coluna para comentários e documentação
def add_column_comments(catalog, schema, table, columns_dict):
    for column, comment in columns_dict.items():
        spark.sql(f"""
            ALTER TABLE `{catalog}`.`{schema}`.`{table}`
            ALTER COLUMN `{column}`
            COMMENT '{comment}'
        """)


# Escrita
def write_silver(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"adventure_works_catalog.silver.{table_name}")
    )