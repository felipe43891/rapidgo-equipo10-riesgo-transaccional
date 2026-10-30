import copy
import glob
import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

RAIZ = Path(__file__).resolve().parent.parent
SCHEMA = json.loads(
    (RAIZ / "docs" / "contrato-evento.schema.json").read_text(encoding="utf-8")
)
VALIDADOR = Draft7Validator(SCHEMA)
SAMPLES = sorted(glob.glob(str(RAIZ / "samples" / "*.json")))


def _cargar(ruta):
    return json.loads(Path(ruta).read_text(encoding="utf-8"))


def _base():
    return _cargar(RAIZ / "samples" / "payment_event.json")


def test_hay_samples():
    assert SAMPLES, "No hay eventos de ejemplo en samples/"


@pytest.mark.parametrize("ruta", SAMPLES)
def test_sample_cumple_contrato(ruta):
    errores = [e.message for e in VALIDADOR.iter_errors(_cargar(ruta))]
    assert not errores, errores


@pytest.mark.parametrize(
    "campo", ["order_id", "amount", "attempt_number", "location", "payment_status"]
)
def test_falta_campo_obligatorio(campo):
    evento = copy.deepcopy(_base())
    del evento[campo]
    assert list(VALIDADOR.iter_errors(evento)), f"Debió fallar sin {campo}"


@pytest.mark.parametrize(
    "campo,valor",
    [
        ("amount", -5),
        ("amount", 0),
        ("attempt_number", 0),
        ("attempt_number", 1.5),
        ("payment_method", "bitcoin"),
        ("payment_status", "ok"),
        ("timestamp", "22/10/2026"),
    ],
)
def test_valor_invalido(campo, valor):
    evento = copy.deepcopy(_base())
    evento[campo] = valor
    assert list(VALIDADOR.iter_errors(evento)), f"Debió fallar con {campo}={valor!r}"


def test_ubicacion_fuera_de_rango():
    evento = copy.deepcopy(_base())
    evento["location"]["lat"] = 120
    assert list(VALIDADOR.iter_errors(evento))


def test_team_id_es_opcional():
    evento = copy.deepcopy(_base())
    evento.pop("team_id", None)
    assert not list(VALIDADOR.iter_errors(evento))