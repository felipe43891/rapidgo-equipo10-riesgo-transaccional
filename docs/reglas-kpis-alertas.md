# KPIs y reglas de alerta · Riesgo transaccional

Los umbrales son **iniciales**. Se ajustarán con datos reales de la ejecución (Semana 2) y cada cambio se justificará en un ADR o checkpoint.

## KPIs

| # | KPI | Definición | Ventana |
|---|---|---|---|
| 1 | Intentos por pedido | Promedio y máximo de `attempt_number` agrupado por `order_id` | 10 minutos |
| 2 | Valor promedio | Promedio de `amount`, global y por `payment_method` | 10 minutos |
| 3 | Eventos de riesgo | Conteo y % de eventos con `is_risky = true` sobre el total | 10 minutos |

## Reglas de alerta

| # | Alerta | Condición | Umbral inicial | Justificación inicial |
|---|---|---|---|---|
| A1 | Múltiples intentos | Un mismo `order_id` acumula intentos de pago en la ventana | >= 3 intentos en 10 minutos | Más de dos reintentos seguidos suele indicar prueba de tarjetas o fallo recurrente |
| A2 | Señales anómalas combinadas | Coinciden 2 o más señales en un evento | Señales: `amount` > 500000 COP; `device_id` nuevo para el usuario; ciudad distinta a la habitual | Una señal aislada genera falsos positivos; la combinación es más fiable |

## Columna derivada `is_risky`

Se marca `true` cuando se cumple A1 o A2. Esta columna alimenta el KPI 3.
