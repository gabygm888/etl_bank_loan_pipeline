import mlflow
import mlflow.sklearn
import dlt
from pyspark.sql import SparkSession

@dlt.table(
    name="rf_personal_loan_forecast",
    comment="Predicción de préstamo personal",
    table_properties={"pipelines.reset.allowed": "true"}
)
def rf_personal_loan_forecast():
    df_spark = spark.read.table("bank_loan.default.etl_bank_loan_gold")
    df = df_spark.toPandas()
    
    # Cargar modelo de MLflow
    model_uri = " runs:/cac1ba1b8f37458a8b6dd61b78058a26/personal_loan_model" 
    model = mlflow.sklearn.load_model(model_uri)
    
    # Separar features
    X = df.drop(columns=["id", "zip_code", "personal_loan"])
    
    # Predicción
    y_pred_prob = model.predict(X)
    y_pred = (y_pred_prob >= 0.5).astype(int)
    
    # Agregar columnas al final
    df["personal_loan_real"] = df["personal_loan"]
    df["personal_loan_pred"] = y_pred
    cols_order = [c for c in df.columns if c not in ["personal_loan_real", "personal_loan_pred"]] + ["personal_loan_real", "personal_loan_pred"]
    df = df[cols_order]
    
    return spark.createDataFrame(df)
