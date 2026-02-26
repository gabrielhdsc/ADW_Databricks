# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd
import numpy as np
import pickle
import mlflow
import mlflow.spark
import mlflow.pyfunc
from mlflow.models import infer_signature

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.table("adventure_works_catalog.gold.vwsinteticdataml")

df = df.orderBy("YearMonth")

df_model = df.dropna(
    subset=[
        "label"
    ]
).select("YearMonth", "label")

df_pandas = df_model.toPandas()

df_pandas['YearMonth'] = pd.to_datetime(df_pandas['YearMonth'])
df_pandas.set_index('YearMonth', inplace=True)

ts_data = df_pandas['label'].astype(float)
ts_filtered = ts_data[~((ts_data.index.year == 2025) & (ts_data.index.month.isin([5, 6])))]
ts_filtered.index = pd.DatetimeIndex(ts_filtered.index)
ts_filtered = ts_filtered.asfreq('MS')

train_ts = ts_filtered[ts_filtered.index < "2025-01-01"]
test_ts = ts_filtered[ts_filtered.index >= "2025-01-01"]

# COMMAND ----------

def smape(y_true, y_pred):
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    
    return 100/len(y_true) * np.sum(
        2 * np.abs(y_pred - y_true) /
        (np.abs(y_true) + np.abs(y_pred) + 1e-8)
    )

# COMMAND ----------

class SarimaWithDate(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        import pickle
        
        model_path = context.artifacts["sarima_model"]

        with open(model_path, "rb") as f:
            self.model = pickle.load(f)

    def predict(self, context, model_input):

        import pandas as pd

        start = pd.to_datetime(model_input["start"].iloc[0])
        end = pd.to_datetime(model_input["end"].iloc[0])

        future_index = pd.date_range(start=start, end=end, freq="MS")
        steps = len(future_index)

        forecast = self.model.forecast(steps=steps)

        return pd.DataFrame({
            "date": future_index.strftime("%Y-%m-%d"),
            "prediction": forecast.round(2)
        })

# COMMAND ----------

mlflow.end_run()
with mlflow.start_run(run_name="sarima_monthly_sales_forecast"):
    model = SARIMAX(
        train_ts,
        order=(1, 1, 1),
        seasonal_order=(1, 0 , 1, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
        
    model_fit = model.fit(disp=False)
    predictions = model_fit.forecast(steps=len(test_ts))
    
    smape_value = smape(test_ts, predictions)

    mlflow.log_metric("smape", smape_value)
    mlflow.log_param("order", "(1, 1, 1)")

    model_path = "/tmp/sarima.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model_fit, f)

    input_example = pd.DataFrame({
        "start": ["2026-01-01"],
        "end": ["2026-06-01"]
    })

    wrapper = SarimaWithDate()
    wrapper.model = model_fit
    example_output = wrapper.predict(None, input_example)

    signature = infer_signature(input_example, example_output)

    mlflow.pyfunc.log_model(
        name="model",
        python_model=SarimaWithDate(),
        artifacts={"sarima_model": model_path},
        input_example=input_example,
        signature=signature,
        registered_model_name="adventure_works_catalog.ml_models.sarima_monthly_sales_model"
    )

# COMMAND ----------

final_model = SARIMAX(
    ts_filtered,
    order=(1, 1, 1),
    seasonal_order=(1, 0 , 1, 12),
    enforce_stationarity=False,
    enforce_invertibility=False
)

final_fit = final_model.fit(disp=False)