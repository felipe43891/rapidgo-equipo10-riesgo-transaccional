# Checkpoint Semana 1

**Equipo 10 · Variante 4: Riesgo transaccional · Release v0.1.0-foundation**

## Decisión más importante

Usar `order_id` como key del topic `rapidgo.payments.events` con 3 particiones, para garantizar el orden de los eventos por pedido y calcular correctamente los intentos de pago. Detalle en [ADR-001](../decisions/ADR-001-key-particiones-kafka.md).

## Riesgo principal

Integrar Kafka, Spark y el Data Lake en la Semana 2 puede tomar más tiempo del previsto, sobre todo la configuración de Docker. Un segundo riesgo es que los umbrales iniciales de las alertas generen demasiados falsos positivos o ninguna alerta. Mitigación: probar Kafka con el producer desde el inicio de la semana y generar eventos normales y anómalos para calibrar umbrales.

## Plan de la Semana 2

| Tarea | Responsable | Issue |
|---|---|---|
| Producer funcional | Julian | #7 |
| Kafka con key y particiones | Daniela | #8 |
| Spark Streaming con 3 transformaciones y ventana | Julian | #9 |
| Persistencia Parquet y checkpoint | Carlos | #10 |
| Pruebas y `ci.yml` | Carlos | #11 |
| ADR-002 y checkpoint semana 2 | Todos | (crear Issue si quieren) |

Cada integrante entrega al menos un PR sustancial y revisa el de otro compañero.

