import random
import uuid
from datetime import datetime, timezone

TEAM_ID = "equipo-10"
CIUDADES = {
    "Bogotá": (4.711, -74.072),
    "Medellín": (6.244, -75.581),
    "Cali": (3.451, -76.532),
}
METODOS = ["card", "pse", "cash", "wallet"]


def _usuario(n):
    """Cada usuario tiene un dispositivo y una ciudad habituales."""
    ciudad = list(CIUDADES)[n % len(CIUDADES)]
    return f"USR-{n}", f"DEV-{n}", ciudad


def _evento(order_id, user_id, device, ciudad, monto, metodo, intento, estado):
    lat, lon = CIUDADES[ciudad]
    return {
        "event_id": str(uuid.uuid4()),
        "team_id": TEAM_ID,
        "order_id": order_id,
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": monto,
        "currency": "COP",
        "payment_method": metodo,
        "device_id": device,
        "location": {"city": ciudad, "lat": lat, "lon": lon},
        "attempt_number": intento,
        "payment_status": estado,
    }


def generar_pedido(rng, anomalo=False):
    """Devuelve la lista de eventos (intentos de pago) de un pedido."""
    user_id, device, ciudad = _usuario(rng.randint(1000, 1099))
    order_id = f"ORD-{rng.randint(10000, 99999)}"
    metodo = rng.choice(METODOS)

    if not anomalo:
        monto = rng.randint(15000, 200000)
        n_intentos = 1 if rng.random() < 0.9 else 2
        return [
            _evento(order_id, user_id, device, ciudad, monto, metodo, i,
                    "approved" if i == n_intentos else "rejected")
            for i in range(1, n_intentos + 1)
        ]

    # Pedido anómalo: muchos intentos, monto alto, dispositivo y ciudad distintos
    monto = rng.randint(500001, 1200000)
    n_intentos = rng.randint(3, 5)
    device_raro = f"DEV-NEW-{rng.randint(1000, 9999)}"
    otras = [c for c in CIUDADES if c != ciudad]
    eventos = []
    for i in range(1, n_intentos + 1):
        eventos.append(_evento(
            order_id, user_id,
            device_raro if i > 1 else device,
            rng.choice(otras) if i > 1 else ciudad,
            monto, metodo, i, "rejected"))
    return eventos


def generar_lote(rng, n_pedidos, prop_anomalos=0.1):
    for _ in range(n_pedidos):
        yield generar_pedido(rng, anomalo=rng.random() < prop_anomalos)