\# ADR-003: Estrategia de recuperación ante fallos y manejo de excepciones en Airflow



\* \*\*Estado:\*\* Aceptado

\* \*\*Fecha:\*\* 2026-11-06

\* \*\*Componentes afectados:\*\* `airflow/dags/rapidgo\_riesgo\_dag.py`, `airflow/dags/rapidgo\_tasks.py`

\* \*\*Issues relacionados:\*\* #13



\## Contexto y Problema

Durante la ejecución del pipeline de riesgo transaccional orquestado en Apache Airflow, es fundamental definir cómo debe reaccionar el sistema ante fallos operativos comunes, tales como la ausencia de archivos fuente en el Data Lake (`datalake/processed/eventos`) debido a fallas previas en la ingesta o errores de ruta. 



Por defecto, Airflow reintenta las tareas fallidas según los parámetros globales (`retries`), lo cual puede generar esperas innecesarias y consumo de recursos cuando el error raíz es de naturaleza determinística y estructural.



\## Opciones Evaluadas



1\. \*\*Opción 1: Reintentos estándar de Airflow (`retries=2`) sin manejo de excepciones\*\*

&#x20;  \* \*Ventajas:\* Configuración por defecto sencilla.

&#x20;  \* \*Desventajas:\* El pipeline consume tiempo valioso esperando los intervalos de reintento ante un archivo que físicamente no existe en el disco, retrasando la notificación del error al operador.



2\. \*\*Opción 2: Fallo rápido mediante `AirflowFailException` ante errores determinísticos\*\*

&#x20;  \* \*Ventajas:\* Interrumpe el flujo de inmediato al detectar un error crítico de fuente (`FileNotFoundError`), evitando reintentos inútiles y marcando la tarea como fallida de forma explícita y limpia.

&#x20;  \* \*Desventajas:\* Requiere interceptar la excepción en el DAG para tipificarla correctamente.



\## Decisión Tomada

Se selecciona la \*\*Opción 2\*\*. Se implementará un bloque de control en el DAG que capture la excepción `FileNotFoundError` proveniente de la validación de fuentes y lance una `AirflowFailException`. De esta forma, el pipeline adopta una política de \*\*fallo rápido (\*fail-fast\*)\*\* para errores de origen de datos no recuperables automáticamente.



\## Consecuencias

\* \*\*Positivas:\*\* Reducción drástica del tiempo de diagnóstico ante fallos de rutas o datos faltantes; prevención de bucles de reintentos en tareas que requieren intervención manual previa.

\* \*\*Negativas / Mitigaciones:\*\* Exige una monitorización activa en la interfaz de Airflow, la cual se mitiga con la estructuración correcta de las dependencias `upstream\_failed` en el grafo.

