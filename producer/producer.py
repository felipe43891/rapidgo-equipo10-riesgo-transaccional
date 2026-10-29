import argparse
import json
import random
import time

from kafka import KafkaProducer

from generador import generar_lote

TOPIC = "rapidgo.payments.events"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pedidos", type=int, default=100)
    p.add_argument("--pausa", type=float, default=0.2, help="segundos entre eventos")
    p.add_argument("--anomalos", type=float, default=0.1)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--semilla", type=int, default=None)
    a = p.parse_args()

    rng = random.Random(a.semilla)
    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    enviados = 0
    for pedido in generar_lote(rng, a.pedidos, a.anomalos):
        for evento in pedido:
            meta = producer.send(TOPIC, key=evento["order_id"], value=evento).get(timeout=10)
            enviados += 1
            print(f"{evento['order_id']} intento={evento['attempt_number']} "
                  f"-> partición={meta.partition} offset={meta.offset}")
            time.sleep(a.pausa)
    producer.flush()
    print(f"Total enviados: {enviados}")


if __name__ == "__main__":
    main()