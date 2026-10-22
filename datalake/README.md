# Data Lake · Riesgo transaccional

Estructura en capas. Los datos no se versionan en Git (ver `.gitignore`); solo este README.

| Capa | Ruta | Contenido | Formato |
|---|---|---|---|
| Raw | `datalake/raw/` | Eventos tal como llegan desde Kafka, sin transformar | Parquet |
| Processed | `datalake/processed/` | Eventos parseados, tipados y con columnas derivadas (banderas de riesgo) | Parquet |
| Curated | `datalake/curated/` | Agregaciones por ventana, KPIs y alertas listas para consumo | Parquet |
| Checkpoints | `datalake/checkpoints/` | Estado de Spark Structured Streaming | Interno de Spark |

Flujo: `raw -> processed -> curated`.
