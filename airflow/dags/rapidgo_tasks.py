"""Tareas del DAG de RapidGo (Riesgo transaccional).

Cada funcion es Python puro (pandas): se puede probar con pytest sin Airflow.
Las funciones devuelven diccionarios simples que Airflow guarda como XCom.

Variables de entorno opcionales:
  RAPIDGO_LAKE       carpeta del Data Lake (por defecto "datalake")
  RAPIDGO_SPARK_DIR  carpeta con rules.py (por defecto <repo>/spark)
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _dir_spark():
    env = os.environ.get("RAPIDGO_SPARK_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2] / "spark"


sys.path.insert(0, str(_dir_spark()))
import rules  # noqa: E402  (umbrales unicos: spark/rules.py)

CAMPOS_CLAVE = ["event_id", "order_id", "user_id", "amount",
                "attempt_number", "event_time"]


def _lake(lake=None):
    return Path(lake or os.environ.get("RAPIDGO_LAKE", "datalake"))


def _carpeta_processed(lake=None):
    return _lake(lake) / "processed" / "eventos"


def _archivos(carpeta):
    carpeta = Path(carpeta)
    return sorted(carpeta.rglob("*.parquet")) if carpeta.exists() else []


def _leer_processed(lake=None):
    carpeta = _carpeta_processed(lake)
    archivos = _archivos(carpeta)
    if not archivos:
        raise FileNotFoundError(f"No hay archivos Parquet en {carpeta}")
    df = pd.concat((pd.read_parquet(a) for a in archivos), ignore_index=True)
    if "city" not in df.columns and "location" in df.columns:
        df["city"] = df["location"].apply(
            lambda x: x.get("city") if isinstance(x, dict) else None)
    return df


# ---------------------------------------------------------------- tarea 1
def validar_fuentes(lake=None, minimo_filas=1):
    """Comprueba que la capa processed existe, tiene filas y las columnas clave."""
    df = _leer_processed(lake)
    if len(df) < minimo_filas:
        raise ValueError(
            f"Se esperaban al menos {minimo_filas} filas y hay {len(df)}")
    faltan = [c for c in CAMPOS_CLAVE if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas en processed: {faltan}")
    return {"carpeta": str(_carpeta_processed(lake)),
            "archivos": len(_archivos(_carpeta_processed(lake))),
            "filas": int(len(df))}


# ---------------------------------------------------------------- tarea 2
def calidad_datos(lake=None, filas_esperadas=None, max_nulos=0.0,
                  max_duplicados=0.0):
    """Tarea de calidad: nulos, duplicados y montos invalidos. Falla si hay problemas."""
    df = _leer_processed(lake)
    n = len(df)
    nulos = {c: float(df[c].isna().mean()) for c in CAMPOS_CLAVE}
    duplicados = float(df.duplicated(subset=["event_id"]).mean())
    montos_invalidos = int((df["amount"] <= 0).sum())

    problemas = []
    if filas_esperadas is not None and n < filas_esperadas:
        problemas.append(f"hay {n} filas y se esperaban {filas_esperadas} (dato del XCom)")
    for campo, pct in nulos.items():
        if pct > max_nulos:
            problemas.append(f"{campo} con {pct:.1%} de nulos")
    if duplicados > max_duplicados:
        problemas.append(f"event_id duplicado en {duplicados:.1%} de las filas")
    if montos_invalidos:
        problemas.append(f"{montos_invalidos} montos <= 0")
    if problemas:
        raise ValueError("Calidad insuficiente: " + "; ".join(problemas))

    return {"filas": int(n), "duplicados_pct": duplicados,
            "montos_invalidos": montos_invalidos, "nulos": nulos}


# ---------------------------------------------------------------- tarea 3
def agregar_kpis(lake=None):
    """Calcula los 3 KPIs y guarda el detalle por pedido en curated/reportes."""
    df = _leer_processed(lake)
    df["is_risky"] = ((df["amount"] > rules.UMBRAL_MONTO)
                      | (df["attempt_number"] >= rules.UMBRAL_INTENTOS))
    por_pedido = (df.groupby("order_id")
                  .agg(max_intentos=("attempt_number", "max"),
                       monto_promedio=("amount", "mean"),
                       eventos=("event_id", "count"),
                       eventos_riesgo=("is_risky", "sum"))
                  .reset_index())

    destino = _lake(lake) / "curated" / "reportes"
    destino.mkdir(parents=True, exist_ok=True)
    por_pedido.to_csv(destino / "kpis_por_pedido.csv", index=False)

    return {"pedidos": int(len(por_pedido)),
            "intentos_promedio": round(float(por_pedido["max_intentos"].mean()), 3),
            "valor_promedio": round(float(df["amount"].mean()), 2),
            "eventos_riesgo_pct": round(float(df["is_risky"].mean() * 100), 2)}


# ---------------------------------------------------------------- tarea 4
def evaluar_alertas(lake=None):
    """A1 (multiples intentos por pedido) y A2 (senales combinadas por usuario)."""
    df = _leer_processed(lake)

    max_por_pedido = df.groupby("order_id")["attempt_number"].max()
    a1 = [o for o, m in max_por_pedido.items() if rules.es_multi_intento(int(m))]

    por_usuario = (df.groupby("user_id")
                   .agg(monto_max=("amount", "max"),
                        dispositivos=("device_id", "nunique"),
                        ciudades=("city", "nunique")))
    a2 = []
    for usuario, fila in por_usuario.iterrows():
        senales = rules.contar_senales(float(fila["monto_max"]),
                                       int(fila["dispositivos"]),
                                       int(fila["ciudades"]))
        if rules.es_senal_combinada(senales):
            a2.append(usuario)

    return {"alertas_a1": len(a1), "alertas_a2": len(a2),
            "pedidos_a1": sorted(a1)[:20], "usuarios_a2": sorted(a2)[:20]}


# ---------------------------------------------------------------- tarea 5
def generar_reporte(fuentes, calidad, kpis, alertas, lake=None):
    """Salida/reporting: escribe un informe Markdown y un JSON en curated/reportes."""
    destino = _lake(lake) / "curated" / "reportes"
    destino.mkdir(parents=True, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    md = destino / f"reporte_{marca}.md"
    md.write_text(
        "# Reporte de riesgo transaccional\n\n"
        f"Generado: {marca} UTC\n\n"
        "## Fuente\n"
        f"- Filas en processed: {fuentes['filas']} ({fuentes['archivos']} archivos)\n\n"
        "## Calidad\n"
        f"- Duplicados: {calidad['duplicados_pct']:.2%}\n"
        f"- Montos invalidos: {calidad['montos_invalidos']}\n\n"
        "## KPIs\n"
        f"- Pedidos: {kpis['pedidos']}\n"
        f"- Intentos por pedido (promedio): {kpis['intentos_promedio']}\n"
        f"- Valor promedio: {kpis['valor_promedio']}\n"
        f"- Eventos de riesgo: {kpis['eventos_riesgo_pct']}%\n\n"
        "## Alertas\n"
        f"- A1 (multiples intentos): {alertas['alertas_a1']} pedidos\n"
        f"- A2 (senales combinadas): {alertas['alertas_a2']} usuarios\n",
        encoding="utf-8")
    (destino / f"reporte_{marca}.json").write_text(
        json.dumps({"fuentes": fuentes, "calidad": calidad, "kpis": kpis,
                    "alertas": alertas}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    return {"reporte": str(md)}
