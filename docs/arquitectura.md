# Arquitectura · Riesgo transaccional (fuente del diagrama)

El archivo `arquitectura.png` se genera a partir de este esquema. Si quieren rehacerlo en draw.io o Excalidraw, usen este mismo flujo.

```mermaid
flowchart LR
    A[Generador de eventos de pago<br/>producer Python] --> B[Kafka<br/>rapidgo.payments.events<br/>key=order_id, 3 particiones]
    B --> C[Spark Structured Streaming<br/>parseo, transformaciones,<br/>ventanas 10 min, alertas]
    C --> D[(Data Lake<br/>raw / processed / curated<br/>Parquet + checkpoint)]
    D --> E[Airflow<br/>calidad, agregación, reporte]
    E --> F[KPIs y alertas de riesgo]
```

| Componente | Responsabilidad en la variante |
|---|---|
| Producer | Simula pagos normales y anómalos (reintentos, montos altos, dispositivos y ciudades raros) |
| Kafka | Transporta eventos; la key `order_id` mantiene el orden por pedido |
| Spark | Calcula intentos por pedido, valor promedio, `is_risky` y alertas A1/A2 |
| Data Lake | Persiste cada capa en Parquet con checkpoint |
| Airflow | Valida calidad de raw y curated, agrega y genera el reporte |
