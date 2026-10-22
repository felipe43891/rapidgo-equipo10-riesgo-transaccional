# RapidGo 360 · Equipo 10 · Variante 4: Riesgo transaccional

Proyecto final de la Línea de Profundización I - Big Data
Universidad de Cundinamarca · Ingeniería de Sistemas y Computación (Facatativá)
Docente: Ing. Oscar Jobany Gomez Ochoa

> Estado: **v0.1.0-foundation** (Semana 1: diseño y organización)

## Equipo

| Integrante | Usuario GitHub | Rol Semana 2 |
|---|---|---|
| Daniela Vaca | `<usuario-github>` | Por definir |
| Julian Jimenez | `<usuario-github>` | Por definir |
| Carlos Barake | `<usuario-github>` | Por definir |

## Problema de negocio

RapidGo es una plataforma de entregas. Durante los pagos y pedidos aparecen comportamientos atípicos: múltiples intentos de pago sobre un mismo pedido, montos inusuales, dispositivos o ubicaciones que no corresponden al usuario. Sin una plataforma de datos que los detecte en tiempo casi real, estos eventos se descubren tarde y generan pérdidas por fraude y mala experiencia.

**Valor de la solución:** un pipeline Big Data que captura los eventos de pago, calcula indicadores de riesgo y dispara alertas cuando se cumplen reglas de negocio, dejando los resultados persistidos en un Data Lake y orquestados con Airflow.

## Arquitectura

```
Generador de eventos (producer, Python)
        |
        v
KAFKA  topic: rapidgo.payments.events  (key = order_id, 3 particiones)
        |
        v
SPARK STRUCTURED STREAMING  (parseo, transformaciones, ventanas, alertas)
        |
        v
DATA LAKE  raw -> processed -> curated   (Parquet + checkpoint)
        |
        v
AIRFLOW  (calidad -> agregación -> reporte)
        |
        v
INDICADORES Y ALERTAS DE RIESGO
```

Diagrama: [`docs/arquitectura.png`](docs/arquitectura.png) (fuente editable en [`docs/arquitectura.md`](docs/arquitectura.md)).
Transversal: Docker + Git + GitHub + Kanban + GitHub Actions + Releases.

## Tecnologías y rol

| Tecnología | Rol |
|---|---|
| Python | Producer de eventos, pruebas y reglas |
| Apache Kafka | Transporte de eventos de pago |
| Spark Structured Streaming | Procesamiento en streaming, ventanas y alertas |
| Data Lake (Parquet) | Persistencia en capas raw / processed / curated |
| Apache Airflow | Orquestación, calidad y reportes |
| Docker Compose | Ejecución local de la arquitectura |
| GitHub (Issues, Projects, Actions, Releases) | Trazabilidad, CI y entregas |

## Evento, KPIs y alertas

- Contrato del evento: [`docs/contrato-evento.md`](docs/contrato-evento.md)
- Evento de ejemplo: [`samples/payment_event.json`](samples/payment_event.json)
- KPIs y reglas de alerta: [`docs/reglas-kpis-alertas.md`](docs/reglas-kpis-alertas.md)

**KPIs:** intentos por pedido · valor promedio · eventos de riesgo.
**Alertas:** múltiples intentos · combinación de señales anómalas.

## Decisiones

- [ADR-001: key y particiones de Kafka](docs/decisions/ADR-001-key-particiones-kafka.md)
- [Checkpoint Semana 1](docs/checkpoints/semana1.md)

## Estructura del repositorio

```
.github/workflows/   CI (desde Semana 2)
airflow/dags/        DAGs (Semana 3)
producer/            Generador de eventos
spark/               Jobs de Spark Structured Streaming
tests/               Pruebas de reglas y contrato
samples/             Eventos de ejemplo
datalake/            Capas raw / processed / curated (no se versionan datos)
docs/                Arquitectura, checkpoints, ADR, contrato
docker-compose.yml   Entorno local
```

## Cómo ejecutar (preliminar)

```bash
docker compose config --quiet   # valida el compose
docker compose up -d kafka      # levanta Kafka (Semana 1: solo Kafka)
```

Las instrucciones completas de instalación, ejecución, pruebas y Airflow se completarán en la Semana 4.

## Plan por releases

| Release | Semana | Contenido |
|---|---|---|
| v0.1.0-foundation | 1 | Arquitectura, backlog, contrato, KPIs, ADR-001 |
| v0.2.0-streaming | 2 | Kafka + Spark + Data Lake + tests + CI |
| v0.3.0-orchestration | 3 | Airflow, fallo controlado y recuperación |
| v1.0.0 | 4 | Quality gate, README final, video, release |

## Tabla de contribuciones

| Integrante | Responsabilidades | Issues / PR principales |
|---|---|---|
| Daniela Vaca | _completar_ | _completar_ |
| Julian Jimenez | _completar_ | _completar_ |
| Carlos Barake | _completar_ | _completar_ |

## Limitaciones conocidas y trabajo futuro

_Se completará en la Semana 4._

## Video final

_Enlace pendiente (Semana 4)._
