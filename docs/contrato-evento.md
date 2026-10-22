# Contrato del evento de pago (v1)

Topic: `rapidgo.payments.events` · Key: `order_id` · Formato: JSON (UTF-8)

| Campo | Tipo | Obligatorio | Descripción / regla de validación |
|---|---|---|---|
| `event_id` | string (UUID) | Sí | Identificador único del evento |
| `order_id` | string | Sí | Pedido asociado. Es la key de Kafka |
| `user_id` | string | Sí | Cliente que paga |
| `timestamp` | string (ISO 8601 UTC) | Sí | Momento del evento, ej. `2026-10-22T15:40:12Z` |
| `amount` | number | Sí | Valor del pago, mayor que 0 |
| `currency` | string | Sí | Moneda, ej. `COP` |
| `payment_method` | string (enum) | Sí | `card`, `pse`, `cash`, `wallet` |
| `device_id` | string | Sí | Dispositivo desde el que se paga |
| `location` | object | Sí | `{ city: string, lat: number, lon: number }` |
| `attempt_number` | integer | Sí | Número de intento de pago, mayor o igual a 1 |
| `payment_status` | string (enum) | Sí | `approved`, `rejected`, `pending` |

Ejemplo: [`../samples/payment_event.json`](../samples/payment_event.json)

## Reglas de validación (se usarán en las pruebas de la Semana 2)

1. Todos los campos obligatorios presentes.
2. `amount` > 0 y `attempt_number` >= 1.
3. `payment_method` y `payment_status` dentro de los valores permitidos.
4. `timestamp` en formato ISO 8601.
5. `location.lat` entre -90 y 90; `location.lon` entre -180 y 180.
