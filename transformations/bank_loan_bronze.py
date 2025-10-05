import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="etl_bank_loan_bronze",
    comment="Capa bronce datos bancarios",
    table_properties={"pipelines.reset.allowed": "true"}  # permite que se sobrescriba
)
def etl_bank_loan_bronze():
     # Leer la tabla original
    df_bronze = spark.sql("SELECT * FROM bank_loan.default.bank_personal_loan")

    # Limpiar nombres de columnas: minúsculas y espacios → _
    df_bronze = df_bronze.toDF(*[
    c.strip().lower().replace(" ", "_") for c in df_bronze.columns
    ])

    # Guardar como tabla Delta en la capa Bronze
    return df_bronze