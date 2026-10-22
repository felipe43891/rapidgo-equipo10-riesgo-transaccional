# ADR-001 - Key y particiones del topic de eventos de pago

**Estado:** Aceptada · **Fecha:** 2026-10-22 · **Equipo:** 10 (Variante 4, Riesgo transaccional)

## Contexto / problema

El KPI principal y la alerta A1 dependen de contar los intentos de pago por pedido. Si los eventos de un mismo pedido llegaran desordenados o repartidos entre particiones sin criterio, el cálculo de intentos sería inconsistente. Además, el entorno es local (Docker en un portátil), así que no conviene crear demasiadas particiones.

## Opciones consideradas

| Opción | Ventaja | Desventaja |
|---|---|---|
| Key = `order_id` | Todos los eventos de un pedido van a la misma partición, en orden | El análisis por usuario o dispositivo cruza particiones |
| Key = `user_id` | Facilita señales por usuario (dispositivo nuevo, ciudad) | Los intentos de un pedido siguen en una partición, pero se mezclan con otros pedidos del usuario |
| Key = `device_id` | Detecta patrones por dispositivo | Un pedido puede quedar repartido entre particiones |
| 1 partición | Orden total y simple | Sin paralelismo; no demuestra el modelo de Kafka |
| 3 particiones | Paralelismo visible con bajo consumo | Orden global no garantizado (solo por key) |
| 6 o más particiones | Mayor paralelismo | Sobredimensionado para un entorno local |

## Decisión tomada

- Topic: `rapidgo.payments.events`
- Key: `order_id`
- Particiones: 3
- Replication factor: 1 (solo entorno local)

## Evidencia utilizada

- El KPI 1 (intentos por pedido) y la alerta A1 se calculan por `order_id`.
- Evento de ejemplo en `samples/payment_event.json`, con `attempt_number` y `order_id`.
- Restricción de recursos del entorno local con Docker.

## Consecuencias / trade-offs

- El orden queda garantizado por pedido, no entre pedidos.
- Las señales por usuario o dispositivo (alerta A2) se resolverán con agregaciones en Spark, que reagrupa por esas columnas.
- Un `replication factor` de 1 no tolera fallos del broker; es aceptable solo en local.
- En la Semana 2 revisaremos esta decisión con offsets y distribución real por partición; si la distribución resulta muy desbalanceada, se documentará en ADR-002.
