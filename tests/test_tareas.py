import sys
from pathlib import Path

import pandas as pd
import pytest

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "airflow" / "dags"))

import rapidgo_tasks as t  # noqa: E402


def _evento(order, user, device, ciudad, monto, intento, eid):
    return {"event_id": eid, "order_id": order, "user_id": user,
            "device_id": device, "city": ciudad, "amount": float(monto),
            "attempt_number": intento,
            "event_time": pd.Timestamp("2026-11-05 10:00:00")}


def _crear_lake(tmp_path, filas):
    carpeta = tmp_path / "processed" / "eventos"
    carpeta.mkdir(parents=True)
    pd.DataFrame(filas).to_parquet(carpeta / "part-0.parquet")
    return str(tmp_path)


@pytest.fixture
def lake(tmp_path):
    filas = [
        _evento("ORD-1", "USR-1", "DEV-1", "Bogota", 50_000, 1, "e1"),
        _evento("ORD-2", "USR-2", "DEV-2", "Bogota", 800_000, 1, "e2"),
        _evento("ORD-2", "USR-2", "DEV-X", "Medellin", 800_000, 2, "e3"),
        _evento("ORD-2", "USR-2", "DEV-X", "Medellin", 800_000, 3, "e4"),
    ]
    return _crear_lake(tmp_path, filas)


def test_validar_fuentes_ok(lake):
    r = t.validar_fuentes(lake)
    assert r["filas"] == 4 and r["archivos"] == 1


def test_validar_fuentes_sin_datos(tmp_path):
    with pytest.raises(FileNotFoundError):
        t.validar_fuentes(str(tmp_path))


def test_validar_fuentes_pocas_filas(lake):
    with pytest.raises(ValueError):
        t.validar_fuentes(lake, minimo_filas=10)


def test_calidad_ok(lake):
    r = t.calidad_datos(lake, filas_esperadas=4)
    assert r["filas"] == 4 and r["montos_invalidos"] == 0


def test_calidad_falla_por_duplicados(tmp_path):
    filas = [_evento("ORD-1", "USR-1", "DEV-1", "Bogota", 1000, 1, "dup"),
             _evento("ORD-1", "USR-1", "DEV-1", "Bogota", 1000, 1, "dup")]
    with pytest.raises(ValueError, match="duplicado"):
        t.calidad_datos(_crear_lake(tmp_path, filas))


def test_calidad_falla_por_monto_invalido(tmp_path):
    filas = [_evento("ORD-1", "USR-1", "DEV-1", "Bogota", -5, 1, "e1")]
    with pytest.raises(ValueError, match="montos"):
        t.calidad_datos(_crear_lake(tmp_path, filas))


def test_calidad_falla_si_faltan_filas_segun_xcom(lake):
    with pytest.raises(ValueError, match="XCom"):
        t.calidad_datos(lake, filas_esperadas=99)


def test_kpis(lake):
    r = t.agregar_kpis(lake)
    assert r["pedidos"] == 2
    assert r["intentos_promedio"] == 2.0          # (1 + 3) / 2
    assert (Path(lake) / "curated" / "reportes" / "kpis_por_pedido.csv").exists()


def test_alertas(lake):
    r = t.evaluar_alertas(lake)
    assert r["alertas_a1"] == 1 and r["pedidos_a1"] == ["ORD-2"]
    assert r["alertas_a2"] == 1 and r["usuarios_a2"] == ["USR-2"]


def test_reporte(lake):
    f = t.validar_fuentes(lake)
    c = t.calidad_datos(lake)
    k = t.agregar_kpis(lake)
    a = t.evaluar_alertas(lake)
    r = t.generar_reporte(f, c, k, a, lake)
    assert Path(r["reporte"]).exists()
    assert "Reporte de riesgo transaccional" in Path(r["reporte"]).read_text(encoding="utf-8")
