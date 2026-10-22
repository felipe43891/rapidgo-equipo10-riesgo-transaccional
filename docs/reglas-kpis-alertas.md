# KPIs y reglas de alerta · Riesgo transaccional

Los umbrales son **iniciales**. Se ajustarán con datos reales de la ejecución (Semana 2) y cada cambio se justificará en un ADR o checkpoint.

## KPIs

|#|KPI|Definición|Ventana|
|-|-|-|-|
|1|Intentos por pedido|Promedio y máximo de `attempt\_number` agrupado por `order\_id`|10 minutos|
|2|Valor promedio|Promedio de `amount`, global y por `payment\_method`|10 minutos|
|3|Eventos de riesgo|Conteo y % de eventos con `is\_risky = true` sobre el total|10 minutos|

## Reglas de alerta

|#|Alerta|Condición|Umbral inicial|Justificación inicial|
|-|-|-|-|-|
|A1|Múltiples intentos|Un mismo `order\_id` acumula intentos de pago en la ventana|>= 3 intentos en 10 minutos|Más de dos reintentos seguidos suele indicar prueba de tarjetas o fallo recurrente|
|A2|Señales anómalas combinadas|Coinciden 2 o más señales en un evento|Señales: `amount` > 500000 COP; `device\_id` nuevo para el usuario; ciudad distinta a la habitual|Una señal aislada genera falsos positivos; la combinación es más fiable|

## Columna derivada `is\_risky`

Se marca `true` cuando se cumple A1 o A2. Esta columna alimenta el KPI 3.

## Cómo se calcula cada KPI

**1. Intentos por pedido**
Agrupar por `order_id` en una ventana de 10 minutos y calcular el promedio y el máximo de `attempt_number`.

**2. Valor promedio**
Promedio de `amount` en la ventana de 10 minutos, global y por `payment_method`.

**3. Eventos de riesgo**
Conteo de eventos con `is_risky = true` dividido por el total de eventos de la ventana, expresado en porcentaje. 

