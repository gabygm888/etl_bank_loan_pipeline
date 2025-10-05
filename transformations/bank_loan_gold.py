import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

@dlt.table(
    name="etl_bank_loan_gold",
    comment="Capa oro datos bancarios",
    table_properties={"pipelines.reset.allowed": "true"}  # permite que se sobrescriba
)
def etl_bank_loan_gold():

    # -------------------------------------------------------
    # Capa Gold: Features finales para análisis y ML
    # -------------------------------------------------------

    # Se define la tabla Gold con la tabla Silver escalada
    df_gold = spark.read.table("bank_loan.default.etl_bank_loan_silver")

    # -------------------------
    # 1. Crear flags y ratios usando solo columnas escaladas
    # -------------------------
    # Flag de alto ingreso (usando z-score)
    mean_income = df_gold.select(F.mean("income_scaled")).collect()[0][0]
    df_gold = df_gold.withColumn("high_income_flag", (F.col("income_scaled") > mean_income).cast("int"))

    # Ratio ingreso por miembro de la familia
    df_gold = df_gold.withColumn(
        "income_per_family_member",
        F.col("income_scaled") / F.col("family")
    )

    # Flag de clientes jóvenes
    df_gold = df_gold.withColumn("young_flag", (F.col("age_scaled") < 0).cast("int"))

    # -------------------------
    # 2️. Agregaciones por zip_code
    # -------------------------
    df_zip_agg = df_gold.groupBy("zip_code").agg(
        F.mean("ccavg_scaled").alias("avg_ccavg_by_zip"),
        F.mean("income_scaled").alias("avg_income_by_zip")
    )

    # Hacer join de nuevo con df_gold
    df_gold = df_gold.join(df_zip_agg, on="zip_code", how="left")

    # -------------------------
    # 3️. Seleccionar columnas finales
    # -------------------------
    # Mantener ID, zip, binarios, escaladas, y nuevas features
    not_scaled_cols = [
        'education',
        'family',
        'personal_loan',
        'securities_account',	
        'cd_account',	
        'online',	
        'creditcard'
    ]
    scaled_cols = [c for c in df_gold.columns if "_scaled" in c]
    new_features = ["high_income_flag", "income_per_family_member", "young_flag", "avg_ccavg_by_zip", "avg_income_by_zip"]

    final_cols = ["id", "zip_code"] + scaled_cols + not_scaled_cols + new_features
    df_gold_final = df_gold.select(final_cols)

    return df_gold_final