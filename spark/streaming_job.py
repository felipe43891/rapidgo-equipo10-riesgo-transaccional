"""Job de Spark Structured Streaming: Kafka -> Data Lake (Parquet con checkpoint).

Capas:
  raw        eventos tal como llegan de Kafka
  processed  eventos parseados, limpios y con banderas de riesgo
  curated    KPIs por ventana (por pedido) y alertas (por usuario)

Uso:
  python spark/streaming_job.py --ventana "1 minute"
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

from rules import MIN_SENALES, UMBRAL_INTENTOS, UMBRAL_MONTO

try:  # modulo del Data Lake (Issue #10); si no esta, se usa el local
    from lake import escribir as escribir_lake
except ImportError:  # pragma: no cover
    escribir_lake = None

PAQUETE_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

ESQUEMA = StructType([
    StructField("event_id", StringType()),
    StructField("team_id", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("device_id", StringType()),
    StructField("location", StructType([
        StructField("city", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])),
    StructField("attempt_number", IntegerType()),
    StructField("payment_status", StringType()),
])


def escribir(df, lake, capa, nombre, trigger):
    """Escribe un stream a Parquet con checkpoint (usa lake.py si existe)."""
    if escribir_lake is not None:
        return escribir_lake(df, lake, capa, nombre, trigger=trigger)
    return (df.writeStream.format("parquet").outputMode("append")
            .queryName(f"{capa}_{nombre}")
            .option("path", f"{lake}/{capa}/{nombre}")
            .option("checkpointLocation", f"{lake}/checkpoints/{capa}_{nombre}")
            .trigger(processingTime=trigger).start())


def construir_streams(kafka_df, ventana, watermark):
    """Devuelve (raw, processed, por_pedido, por_usuario)."""
    # RAW: tal como llega
    raw = kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
    )

    # Parseo del JSON con esquema explicito
    parsed = (
        raw.select(F.from_json("value", ESQUEMA).alias("e")).select("e.*")
        .withColumn("event_time",
                    F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX"))
    )

    # Tres transformaciones
    processed = (
        parsed
        # T1: limpieza de eventos invalidos
        .filter(F.col("order_id").isNotNull()
                & (F.col("amount") > 0)
                & (F.col("attempt_number") >= 1)
                & F.col("event_time").isNotNull())
        # T2: aplanar la ciudad
        .withColumn("city", F.col("location.city"))
        # T3: banderas de riesgo por evento
        .withColumn("is_high_amount", F.col("amount") > UMBRAL_MONTO)
        .withColumn("is_multi_attempt", F.col("attempt_number") >= UMBRAL_INTENTOS)
        .withColumn("is_risky", F.col("is_high_amount") | F.col("is_multi_attempt"))
    )

    con_watermark = processed.withWatermark("event_time", watermark)

    # Agregacion por ventana: KPIs por pedido y alerta A1
    por_pedido = (
        con_watermark
        .groupBy(F.window("event_time", ventana), "order_id")
        .agg(F.max("attempt_number").alias("max_intentos"),
             F.avg("amount").alias("monto_promedio"),
             F.count("*").alias("eventos"),
             F.sum(F.col("is_risky").cast("int")).alias("eventos_riesgo"))
        .withColumn("alerta_a1", F.col("max_intentos") >= UMBRAL_INTENTOS)
        .withColumn("ventana_inicio", F.col("window.start"))
        .drop("window")
    )

    # Alerta A2: combinacion de senales por usuario en la ventana
    por_usuario = (
        con_watermark
        .groupBy(F.window("event_time", ventana), "user_id")
        .agg(F.max("amount").alias("monto_max"),
             F.approx_count_distinct("device_id").alias("dispositivos"),
             F.approx_count_distinct("city").alias("ciudades"))
        .withColumn("senales",
                    (F.col("monto_max") > UMBRAL_MONTO).cast("int")
                    + (F.col("dispositivos") > 1).cast("int")
                    + (F.col("ciudades") > 1).cast("int"))
        .withColumn("alerta_a2", F.col("senales") >= MIN_SENALES)
        .withColumn("ventana_inicio", F.col("window.start"))
        .drop("window")
    )
    return raw, processed, por_pedido, por_usuario


def main():
    p = argparse.ArgumentParser(description="Streaming de riesgo transaccional")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="rapidgo.payments.events")
    p.add_argument("--lake", default="datalake")
    p.add_argument("--ventana", default="10 minutes")
    p.add_argument("--watermark", default="1 minute")
    p.add_argument("--trigger", default="10 seconds")
    p.add_argument("--offsets", default="earliest", choices=["earliest", "latest"])
    a = p.parse_args()

    spark = (SparkSession.builder.appName("rapidgo-riesgo-transaccional")
             .master("local[*]")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.ui.enabled", "false")
             .config("spark.jars.packages", PAQUETE_KAFKA)
             .config("spark.sql.shuffle.partitions", "3")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f">>> Spark iniciado. Conectando a Kafka en {a.bootstrap} (topic: {a.topic})...")

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", a.bootstrap)
                .option("subscribe", a.topic)
                .option("startingOffsets", a.offsets)
                .load())

    raw, processed, por_pedido, por_usuario = construir_streams(
        kafka_df, a.ventana, a.watermark)

    queries = [
        escribir(raw, a.lake, "raw", "eventos", a.trigger),
        escribir(processed, a.lake, "processed", "eventos", a.trigger),
        escribir(por_pedido, a.lake, "curated", "kpis_por_pedido", a.trigger),
        escribir(por_usuario, a.lake, "curated", "alertas_por_usuario", a.trigger),
    ]
    print(">>> 4 streams activos escribiendo en:", a.lake)

    try:
        while True:
            for q in queries:
                if q.exception() is not None:
                    raise q.exception()
            spark.streams.awaitAnyTermination(5)
            progreso = [f"{q.name}: {q.lastProgress['numInputRows']} filas"
                        for q in queries if q.lastProgress]
            if progreso:
                print("Estado micro-batch ->", " | ".join(progreso))
    except KeyboardInterrupt:
        print("\n>>> Deteniendo streams limpiamente...")
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()