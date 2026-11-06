"""DAG de orquestacion de RapidGo: riesgo transaccional.

validar_fuentes -> calidad_datos -> [agregar_kpis, evaluar_alertas] -> generar_reporte

- retries: cada tarea se reintenta 2 veces con espera de 30 s
- XCom: cada tarea devuelve un diccionario que la siguiente consulta
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import rapidgo_tasks as t

default_args = {
    "owner": "equipo-10",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def _validar_fuentes():
    return t.validar_fuentes()


def _calidad_datos(ti):
    fuentes = ti.xcom_pull(task_ids="validar_fuentes")          # XCom
    return t.calidad_datos(filas_esperadas=fuentes["filas"])


def _agregar_kpis():
    return t.agregar_kpis()


def _evaluar_alertas():
    return t.evaluar_alertas()


def _generar_reporte(ti):
    return t.generar_reporte(
        fuentes=ti.xcom_pull(task_ids="validar_fuentes"),
        calidad=ti.xcom_pull(task_ids="calidad_datos"),
        kpis=ti.xcom_pull(task_ids="agregar_kpis"),
        alertas=ti.xcom_pull(task_ids="evaluar_alertas"),
    )


with DAG(
    dag_id="rapidgo_riesgo_transaccional",
    description="Calidad, agregacion y reporte de riesgo transaccional",
    start_date=datetime(2026, 11, 1),
    schedule=None,          # ejecucion manual; cambiar a "@hourly" si se quiere programar
    catchup=False,
    default_args=default_args,
    tags=["rapidgo", "equipo-10", "riesgo"],
) as dag:
    validar_fuentes = PythonOperator(task_id="validar_fuentes", python_callable=_validar_fuentes)
    calidad_datos = PythonOperator(task_id="calidad_datos", python_callable=_calidad_datos)
    agregar_kpis = PythonOperator(task_id="agregar_kpis", python_callable=_agregar_kpis)
    evaluar_alertas = PythonOperator(task_id="evaluar_alertas", python_callable=_evaluar_alertas)
    generar_reporte = PythonOperator(task_id="generar_reporte", python_callable=_generar_reporte)

    validar_fuentes >> calidad_datos >> [agregar_kpis, evaluar_alertas] >> generar_reporte
