# Checkpoint Semana 2 - Pipeline funcional y CI (`v0.2.0-streaming`)

## 1. Qué quedó funcionando esta semana
- **Producer de eventos de pago (`producer/producer.py`):** Generación y envío de eventos JSON al topic `rapidgo.payments.events` (3 particiones) en Kafka usando `order_id` como key para garantizar orden por pedido.
- **Procesamiento en tiempo real (`spark/streaming_job.py` y `spark/rules.py`):**
  - Lectura en streaming desde Kafka y parseo con esquema explícito (`ESQUEMA`).
  - 3 transformaciones en capa `processed`: filtrado de eventos inválidos (`T1`), aplanado de `location.city` (`T2`) y cálculo de banderas de riesgo `is_high_amount`, `is_multi_attempt` e `is_risky` (`T3`).
  - Agregaciones por ventana con `watermark` (`1 minute`) en capa `curated`: KPIs por pedido + alerta **A1** (`kpis_por_pedido`) y combinación de señales por usuario + alerta **A2** (`alertas_por_usuario`).
  - Persistencia en formato **Parquet** con checkpoints independientes por capa.
- **Pruebas automatizadas y CI (`tests/`, `spark/test_reglas.py`, `.github/workflows/ci.yml`):** 30 pruebas unitarias validando contrato JSON, productor y reglas de negocio A1/A2.

## 2. Valor o evidencia real observada en la ejecución
- **Eventos enviados por el Producer:** Tandas de prueba en vivo (ej. `Total enviados: 39` distribuidos entre las particiones `0`, `1` y `2` hasta los offsets `33`, `51` y `36`).
- **Eventos persistidos en el Data Lake (Parquet):** Al ejecutar `spark/streaming_job.py --ventana "1 minute"` con `--offsets earliest`, Spark procesó el acumulado del topic y escribió **123 registros en `datalake/raw/eventos`** y **123 registros limpios y enriquecidos en `datalake/processed/eventos`**, verificado con `pd.read_parquet()`.
- **Suite de pruebas (`pytest -v`):** `30 passed in 0.46s` en Python 3.12.5 (`9` tests de reglas en `spark/test_reglas.py`, `18` de contrato en `tests/test_contrato.py` y `3` del producer en `tests/test_producer.py`).

## 3. Decisión técnica tomada y por qué (ADR-002)
Se decidió ejecutar PySpark localmente en Windows configurando `HADOOP_HOME` con `winutils.exe` y `hadoop.dll` (Hadoop 3.3.x), fijando `spark.driver.host` y `spark.driver.bindAddress` a `127.0.0.1` y separando las rutas de checkpoint bajo el patrón `datalake/checkpoints/{capa}_{nombre}`. Esto evitó el bloqueo del `JavaSparkContext` con el adaptador de red de Docker en Windows y eliminó la colisión de checkpoints entre capas sin alterar la red del broker Kafka existente.

## 4. Problema encontrado y cómo se resolvió
- **Problema real:** Aunque Spark descargaba el conector `spark-sql-kafka-0-10_2.12:3.5.1`, el job se cerraba inmediatamente después de iniciar y el conteo en Parquet daba `raw: 0` y `processed: 0`. El log mostraba:
  `WARN StreamingQueryManager: Stopping existing streaming query [id=03b5643f-054b-4c85-94cd-cdb01616c2ba...], as a new run is being started.`
- **Causa y solución:** Las funciones de escritura de las capas `raw` y `processed` recibían el mismo nombre (`"eventos"`) y construían el checkpoint como `datalake/checkpoints/eventos`. Al iniciar el segundo stream, Spark detenía el primero y `spark.streams.awaitAnyTermination()` daba por finalizado todo el script. Se resolvió cambiando la ruta de checkpoint a `f"{lake}/checkpoints/{capa}_{nombre}"` y nombrando cada query con `.queryName(f"{capa}_{nombre}")`.

## 5. Enlaces a Issues / PR / Action relacionados
- **Rama y commit de Spark Streaming:** `feat/spark-streaming` (commit `53a289f`) — [Pull Request de Spark Streaming](https://github.com/felipe43891/rapidgo-equipo10-riesgo-transaccional/pulls)
- **Ejecución de GitHub Actions (CI RapidGo):** [Run #41121927874](https://github.com/felipe43891/rapidgo-equipo10-riesgo-transaccional/actions/runs/41121927874)
- **Documentación arquitectónica:** `docs/decisions/ADR-002.md`
