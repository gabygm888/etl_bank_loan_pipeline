# Proyecto ETL y Modelo de Predicción de Préstamos Personales

## Descripción general

Este proyecto implementa un pipeline de datos en **Databricks** para el procesamiento, enriquecimiento y modelado de información bancaria con el objetivo de **predecir la probabilidad de que un cliente acepte un préstamo personal** (`personal_loan`: 0 o 1).

El flujo completo sigue la arquitectura **Medallion (Bronze, Silver, Gold)** e integra un **modelo de Machine Learning** registrado en **MLflow**, con resultados visualizados en un **Dashboard de Databricks SQL**.

---

## Arquitectura del pipeline

### 1. Capa Bronze
**Objetivo:** almacenar los datos bancarios en su estado más crudo.  
**Fuente:** tabla base `bank_loan.default.bank_personal_loan`.

**Procesos realizados:**
- Limpieza básica de nombres de columnas (minúsculas, reemplazo de espacios).  
- Ingesta en formato **Delta Table**: `etl_bank_loan_bronze`.

**Propósito:** mantener un histórico completo y trazable de los datos originales.

---

### 2. Capa Silver
**Objetivo:** preparar los datos para su posterior análisis.

**Procesos realizados:**
- Normalización de tipos de datos.  
- Escalado de variables numéricas (z-score).  
- Eliminación o imputación de valores nulos.  
- Estandarización de variables categóricas y binarias.

**Salida:** `etl_bank_loan_silver`.

---

### 3. Capa Gold
**Objetivo:** enriquecer los datos y generar atributos derivados listos para modelos de machine learning.

**Procesos realizados:**
- Creación de variables agregadas por `zip_code` (por ejemplo: `avg_ccavg_by_zip`, `avg_income_by_zip`).  
- Cálculo de banderas (por ejemplo: `high_income_flag`, `young_flag`).  
- Integración de las variables escaladas y agregadas.

**Salida:** `etl_bank_loan_gold`.

---

## Modelo de Machine Learning

### Entrenamiento y registro
El modelo se entrena desde el notebook de exploración asociado al pipeline.  
Se utiliza un **Random Forest Classifier** para predecir la variable `personal_loan`.

**Entrenamiento y validación:**
- División de los datos en entrenamiento y prueba (`train_test_split`).  
- Cálculo de métricas (**Accuracy**, **Precision**, **Recall**, **F1-score**).  
- El modelo se registra en **MLflow** para su reutilización desde el pipeline sin necesidad de reentrenarlo en cada ejecución.

---

### Ejecución dentro del pipeline
- Desde la capa **Gold** se leen los datos y se cargan en memoria.  
- Se aplica el modelo almacenado en MLflow para generar la predicción binaria:

  - `personal_loan_real`: valor real.  
  - `personal_loan_pred`: valor predicho.

**Salida:** tabla `rf_personal_loan_forecast`.

---

## Visualización y monitoreo

Se construye un **Dashboard en Databricks SQL** para analizar el desempeño del modelo y los patrones en los datos.

**Visualizaciones principales:**
- Matriz de confusión: comparación entre `personal_loan_real` y `personal_loan_pred`.  
- Métricas del modelo: precisión, recall, f1-score, accuracy.  
- Distribución de predicciones por segmento: relación con `high_income_flag`, `young_flag`, `education`, etc.  
- Scatter plot de ingresos vs experiencia: análisis visual del comportamiento de las variables y su relación con la decisión del préstamo.

---

## Conclusiones

- El pipeline permite un flujo completo de datos, desde la ingesta cruda hasta la generación de insights en dashboards.  
- El modelo Random Forest muestra un desempeño estable, con buena precisión en la predicción de aceptación de préstamos.  
- La arquitectura Delta y MLflow garantizan trazabilidad, versionamiento y escalabilidad del proceso.
