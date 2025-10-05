import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

@dlt.table(
    name="etl_bank_loan_silver",
    comment="Capa plata datos bancarios",
    table_properties={"pipelines.reset.allowed": "true"}  # permite que se sobrescriba
)
def etl_bank_loan_silver():
    # -------------------------
    # 0. Leer la tabla Bronze
    # -------------------------
    df_silver = spark.read.table("bank_loan.default.etl_bank_loan_bronze")

    # -------------------------
    # 1. Convertir columnas ID y ZIP a string
    # -------------------------
    for c in ["id", "zip_code"]:
        df_silver = df_silver.withColumn(c, F.col(c).cast("string"))

    # -------------------------
    # 2. Convertir columnas numéricas bigint/int a double
    # -------------------------
    cols = [c for c, t in df_silver.dtypes if t in ("bigint", "int")]
    for c in cols:
        df_silver = df_silver.withColumn(c, F.col(c).cast("double"))

    # -------------------------
    # 3. Función UDF para convertir fracciones a float
    # -------------------------
    def frac_to_double(s):
        try:
            if s is None:
                return None
            if '/' in s:
                num, denom = s.split('/')
                return float(num) / float(denom)
            else:
                return float(s)
        except:
            return None

    frac_to_double_udf = F.udf(frac_to_double, DoubleType())
    df_silver = df_silver.withColumn("ccavg", frac_to_double_udf(F.col("ccavg")))

    # -------------------------
    # 4. Rellenar valores nulos con la mediana
    # -------------------------
    for col in ["age", "income", "ccavg"]:
        median_val = df_silver.approxQuantile(col, [0.5], 0.01)[0]
        df_silver = df_silver.fillna({col: median_val})

    # -------------------------
    # 5. Escalado manual (z-score) de columnas numéricas
    # -------------------------

    # Columnas numéricas continuas que sí queremos escalar
    numeric_cols = [c for c, t in df_silver.dtypes if t == "double"]
    string_cols = ["id", "zip_code"]

    # Variables que NO queremos escalar
    not_scaled_cols = [
        'education',
        'family',
        'personal_loan',
        'securities_account',	
        'cd_account',	
        'online',	
        'creditcard'
    ]

    # Filtrar solo las numéricas continuas
    numeric_cols_to_scale = [c for c in numeric_cols if c not in not_scaled_cols]


    scaled_cols = []

    for c in numeric_cols_to_scale:
        stats = df_silver.select(
            F.mean(F.col(c)).alias("mean"),
            F.stddev(F.col(c)).alias("std")
        ).collect()[0]

        mean_val = stats["mean"]
        std_val = stats["std"] if stats["std"] != 0 else 1.0

        scaled_col_name = f"{c}_scaled"
        df_silver = df_silver.withColumn(
            scaled_col_name,
            (F.col(c) - F.lit(mean_val)) / F.lit(std_val)
        )
        scaled_cols.append(scaled_col_name)

    # -------------------------
    #  6. Seleccionar solo columnas escaladas + columnas string
    # -------------------------
    df_silver_scaled = df_silver.select(string_cols + scaled_cols + not_scaled_cols)

    # -------------------------
    # 7. Quitar filas con valores nulos
    # -------------------------
    df_silver_scaled = df_silver_scaled.na.drop()

    # -------------------------
    # 8️. Return DataFrame
    # -------------------------
    return df_silver_scaled