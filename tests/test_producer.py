import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "producer"))
from generador import generar_pedido, TEAM_ID

CAMPOS = ["event_id", "team_id", "order_id", "user_id", "timestamp", "amount", "currency",
          "payment_method", "device_id", "location", "attempt_number", "payment_status"]


def test_evento_normal_cumple_contrato():
    for e in generar_pedido(random.Random(1), anomalo=False):
        assert all(c in e for c in CAMPOS)
        assert e["team_id"] == TEAM_ID
        assert e["amount"] > 0 and e["attempt_number"] >= 1


def test_pedido_anomalo_tiene_senales_de_riesgo():
    eventos = generar_pedido(random.Random(2), anomalo=True)
    assert max(e["attempt_number"] for e in eventos) >= 3
    assert all(e["amount"] > 500000 for e in eventos)
    assert len({e["device_id"] for e in eventos}) > 1


def test_mismo_pedido_misma_key():
    eventos = generar_pedido(random.Random(3), anomalo=True)
    assert len({e["order_id"] for e in eventos}) == 1