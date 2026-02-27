# Databricks notebook source
# Databricks notebook source
#Definição de Features e Target para o modelo de ML

from pyspark.sql import functions as f

#Carrega as tabelas
sales = spark.read.table("adventure_works_catalog.silver.fact_sales_order")
order_itens = spark.read.table("adventure_works_catalog.silver.fact_sales_order_detail")
products = spark.read.table("adventure_works_catalog.silver.dim_product")


#Transforma de string para data
sales = sales.withColumn(
    "OrderDate_SK",
    f.to_date(f.col("OrderDate_SK").cast("string"), "yyyyMMdd")
)

#Adiciona produtos a tabela sales
products_order = products.join(order_itens, "Product_SK", "inner") 
sales_products = sales.join(products_order, "OrderID", "inner")

#Encontra a data da primeira compra de cada cliente e junta com a fato sales
first_purchase = sales_products.groupBy("Customer_SK").agg(f.min("OrderDate_SK").alias("FirstPurchase"))
sales_first_date = sales_products.join(first_purchase, "Customer_SK")

#Deixa somente as compras das sales dos primeiros 90 dias para as features
sales_first_90days = sales_first_date.filter(f.datediff(f.col("OrderDate_SK"), f.col("FirstPurchase")) <= 90)


#Calculo de Features agregadas
df_features = sales_first_90days.groupBy("Customer_SK").agg(
    f.count("OrderID").alias("TotalOrdersFirst90Days"),
    f.sum("SubTotal").alias("SpendFirst90Days"),
    f.avg("SubTotal").alias("AvgTicketFirst90Days"),
    f.countDistinct("SubCategory").alias("CategoryDiversity"),
    f.countDistinct("Product_SK").alias("ProductDiversity"),
    f.when(f.count("OrderID") > 1,
           f.datediff(f.max("OrderDate_SK"), f.min("OrderDate_SK")) / (f.count("OrderID") - 1)
           ).otherwise(90).alias("DaysToRepurchase")
)


#Calcula o valor total gasto pelo cliente para o TARGET
df_historic_value = sales.groupby("Customer_SK").agg(f.sum("SubTotal").alias("TotalHistoricalValue"))


#Unificar com Customer (target) e Territory (JOINS)
df_join = df_features \
    .join(df_historic_value, "Customer_SK", "inner")


#Cria o Target com 3 classes usando Percentis baseado no total histórico
quantiles = df_join.stat.approxQuantile("TotalHistoricalValue", [0.33, 0.66], 0.0)
p33, p66 = quantiles[0], quantiles[1]

df_target = df_join.withColumn("ClientRank", 
    f.when(f.col("TotalHistoricalValue") >= p66, "GOLD")
     .when(f.col("TotalHistoricalValue") >= p33, "SILVER")
     .otherwise("BRONZE")
)


#Seleciona de colunas de interesse
df_final = df_target.select(
    "Customer_SK",
    "TotalOrdersFirst90Days",
    "SpendFirst90Days",
    "TotalHistoricalValue",
    "AvgTicketFirst90Days",
    "ClientRank",
    "CategoryDiversity",
    "ProductDiversity",
    "DaysToRepurchase"
)


display(df_final)


# COMMAND ----------

#Criar a tabela na Feature Store do databricks

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

table_name = "adventure_works_catalog.gold.feature_customer_first_behavior"

fe.create_table(
    name=table_name,
    primary_keys=["Customer_SK"],
    df=df_final, 
    schema=df_final.schema,
    description="Features de comportamento dos primeiros 90 dias dos clientes"
)

# COMMAND ----------

# DBTITLE 1,Install Feature Engineering
# MAGIC
# MAGIC %pip install databricks-feature-engineering
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#Compactação e vetorização

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

fe = FeatureEngineeringClient()

table_name = "adventure_works_catalog.gold.feature_customer_first_behavior"
df_final = fe.read_table(name=table_name)


#Transforma o ClientRank de String para um Número
client_indexer = StringIndexer(inputCol="ClientRank", outputCol="ClientRankIndexed")


#Colunas que servirão para ensinar o modelo
feature_cols = ["TotalOrdersFirst90Days", "SpendFirst90Days", "AvgTicketFirst90Days", "ProductDiversity", "CategoryDiversity", "DaysToRepurchase"]


#Compacta as colunas de features (Vetoriza)
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")


#Cria um pipeline de execução
pipeline = Pipeline(stages=[client_indexer, assembler])


df_model = pipeline.fit(df_final).transform(df_final)

display(df_model)

# COMMAND ----------

#Definição do modelo e treinamento
from pyspark.ml.classification import RandomForestClassifier

#Separar dados de treino e dados de teste do modelo (80%, 20%)
train_df, test_df = df_model.randomSplit([0.8, 0.2], seed=42)

#Definir o modelo
rf = RandomForestClassifier(labelCol="ClientRankIndexed", featuresCol="features",numTrees=100)

#Treina o modelo
rf_model = rf.fit(train_df)

#Testa o modelo com o resto dos dados
predictions = rf_model.transform(test_df)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="ClientRankIndexed", predictionCol="prediction")

#Calcula as métricas
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

print(f"Acurácia: {accuracy:.2%}")
print(f"Precision: {precision:.2%}")
print(f"Recall: {recall:.2%}")
print(f"F1: {f1:.2%}")

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix

# Converter as previsões para Pandas (apenas o necessário para o gráfico)
y_true = predictions.select("ClientRankIndexed").toPandas()
y_pred = predictions.select("prediction").toPandas()

# Criar a matriz
cm = confusion_matrix(y_true, y_pred)

# Plotar
plt.figure(figsize=(8,6))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
            xticklabels=['GOLD', 'SILVER', 'BRONZE'], 
            yticklabels=['GOLD', 'SILVER', 'BRONZE'])
plt.ylabel('Realidade')
plt.xlabel('Previsão do Modelo')
plt.title('Matriz de Confusão')
plt.show()

# COMMAND ----------

# Ver as features que mais influenciaram o resultado
importances = rf_model.featureImportances
features_list = ["TotalOrdersFirst90Days", "SpendFirst90Days", "AvgTicketFirst90Days", "ProductDiversity", "CategoryDiversity", "DaysToRepurchase"]

print("--- RANKING DE IMPORTÂNCIA DAS VARIÁVEIS ---")
for i, feature in enumerate(features_list):
    print(f"{feature:25} | {importances[i]:.2%}")

# COMMAND ----------

#Visualizando a primeira árvore da floresta para demostração
tree_model = rf_model.trees[0]
print(tree_model.toDebugString)

# COMMAND ----------

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import mlflow.sklearn
from mlflow.models import infer_signature
from databricks.feature_engineering import FeatureEngineeringClient

#Pega a tabela com features e trasnforma em para pandas
fe = FeatureEngineeringClient()
table_name = "adventure_works_catalog.gold.feature_customer_first_behavior"
df_pd = fe.read_table(name=table_name).toPandas()


#Define as colunas Feature (sem o Target e sem IDs)
feature_cols = ["TotalOrdersFirst90Days", "SpendFirst90Days", "AvgTicketFirst90Days", 
                "ProductDiversity", "CategoryDiversity", "DaysToRepurchase"]


#Transformação de Decimal para Float para evitar erro de JSON
for col in feature_cols:
    df_pd[col] = df_pd[col].astype(float)

X = df_pd[feature_cols].fillna(0)
y = df_pd["ClientRank"] #Target


#Treina o Random Forest do Scikit-Learn
sk_rf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
sk_rf.fit(X, y)


#Registra o SKlearn no MLflow (para o Serving)
model_name = "adventure_works_catalog.ml_models.customer_rank_sk_model"

with mlflow.start_run(run_name="Customer_Rank_Prediction_SKLearn"):
    input_example = X.head(5)
    signature = infer_signature(X, sk_rf.predict(X))

    #Scikit-Learn NÃO precisa de dfs_tmpdir ou Java
    mlflow.sklearn.log_model(
        sk_model=sk_rf,
        artifact_path="model",
        registered_model_name=model_name,
        signature=signature,
        input_example=input_example
    )
