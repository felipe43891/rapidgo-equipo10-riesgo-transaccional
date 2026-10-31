# Evidencia de Kafka: particiones y key

Verificado por: Daniela Vaca · Fecha: <fecha>

## Configuración
- Topic: rapidgo.payments.events · 3 particiones · replication factor 1 · key = order_id

## Salida real de describe
<pega aquí la salida de tu computador>

## Prueba de la key
- Eventos enviados: <N> (<P> pedidos)
- Ejemplo: <ORD-xxxxx intentos 1 a 4 -> partición X, offsets a-b>

## Reparto por partición
| Partición | Eventos | % |
|---|---|---|
| 0 | <n> | <%> |
| 1 | <n> | <%> |
| 2 | <n> | <%> |

## Observaciones
<¿El reparto está equilibrado? ¿Qué partición recibe más y por qué (pedidos con muchos reintentos)? ¿Qué significa para el ADR-002?>