"""Reglas de negocio de riesgo transaccional (puras, sin Spark).

Los umbrales son INICIALES: deben coincidir con docs/reglas-kpis-alertas.md
y se ajustan con datos reales de ejecucion (ver ADR-002).
"""

UMBRAL_INTENTOS = 3        # A1: intentos de pago por pedido dentro de la ventana
UMBRAL_MONTO = 500_000     # senal de monto alto (COP)
MIN_SENALES = 2            # A2: senales simultaneas para disparar la alerta


def es_multi_intento(max_intentos, umbral=UMBRAL_INTENTOS):
    """A1: True si un pedido acumula al menos `umbral` intentos en la ventana."""
    return max_intentos >= umbral


def contar_senales(monto_max, dispositivos_distintos, ciudades_distintas,
                   umbral_monto=UMBRAL_MONTO):
    """Cuenta cuantas senales anomalas se dan para un usuario en la ventana."""
    return (
        int(monto_max > umbral_monto)
        + int(dispositivos_distintos > 1)
        + int(ciudades_distintas > 1)
    )


def es_senal_combinada(senales, minimo=MIN_SENALES):
    """A2: True si coinciden al menos `minimo` senales anomalas."""
    return senales >= minimo
