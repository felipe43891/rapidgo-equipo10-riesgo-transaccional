from airflow.exceptions import AirflowFailException
from rapidgo_tasks import validar_fuentes, calidad_datos, agregar_kpis, evaluar_alertas, generar_reporte

def _validar_fuentes(**context):
    try:
        # Llama a la función original de validación
        return validar_fuentes()
    except FileNotFoundError as e:
        # Falla de inmediato sin gastar reintentos (retries) en un error determinístico
        raise AirflowFailException(f"Fallo crítico de fuente no encontrada: {str(e)}")

# ... dentro de la definición de tus PythonOperator en el DAG ...
# task_validar = PythonOperator(
#     task_id='validar_fuentes',
#     python_callable=_validar_fuentes,
#     retries=2, # Aunque tenga reintentos configurados, AirflowFailException los anula y falla al instante
#     ...
# )
