import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))

from rules import (  # noqa: E402
    MIN_SENALES,
    UMBRAL_INTENTOS,
    UMBRAL_MONTO,
    contar_senales,
    es_multi_intento,
    es_senal_combinada,
)


def test_a1_dispara_en_el_umbral():
    assert es_multi_intento(UMBRAL_INTENTOS)


def test_a1_no_dispara_bajo_el_umbral():
    assert not es_multi_intento(UMBRAL_INTENTOS - 1)


def test_a1_dispara_sobre_el_umbral():
    assert es_multi_intento(UMBRAL_INTENTOS + 2)


def test_senales_ninguna():
    assert contar_senales(100_000, 1, 1) == 0


def test_senal_monto_en_el_limite_no_cuenta():
    assert contar_senales(UMBRAL_MONTO, 1, 1) == 0


def test_senal_monto_sobre_el_limite_cuenta():
    assert contar_senales(UMBRAL_MONTO + 1, 1, 1) == 1


def test_senales_todas():
    assert contar_senales(UMBRAL_MONTO + 1, 2, 2) == 3


def test_a2_requiere_dos_senales():
    assert es_senal_combinada(contar_senales(800_000, 2, 1))       # monto + dispositivo
    assert es_senal_combinada(contar_senales(100_000, 2, 2))       # dispositivo + ciudad
    assert not es_senal_combinada(contar_senales(800_000, 1, 1))   # solo monto
    assert not es_senal_combinada(contar_senales(100_000, 1, 1))   # ninguna


def test_a2_minimo_configurado():
    assert MIN_SENALES == 2
