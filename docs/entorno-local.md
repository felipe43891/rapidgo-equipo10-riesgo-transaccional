# Entorno local: Kafka con Docker

Verificado por: Daniela Vaca · Fecha: 2026-10-22

## Comandos ejecutados

```bash
docker compose config --quiet
docker compose up -d kafka
docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh --create --topic rapidgo.payments.events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic rapidgo.payments.events --bootstrap-server localhost:9092
docker compose down
```

## Salida real del describe

```text
<Equipo Pc@Daniela MINGW64 ~/Downloads/Clonacion/rapidgo-equipo10-riesgo-transaccional (docs/arquitectura-entorno)
$ MSYS_NO_PATHCONV=1 docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic rapidgo.payments.events --bootstrap-server localhost:9092
Topic: rapidgo.payments.events  TopicId: En7D8RwKQTi1g-a9jEJjxg PartitionCount: 3       ReplicationFactor: 1   Configs:
        Topic: rapidgo.payments.events  Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: rapidgo.payments.events  Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: rapidgo.payments.events  Partition: 2    Leader: 1       Replicas: 1     Isr: 1
>
```

## Observaciones

<Qué pasó: cuánto tardó en levantar, si hubo errores y cómo se resolvieron.>

## hubo un error en código y lo documentamos aquí, de manera en la que se pudiera subir el describe de los topic de rapidGo, después de eso no hubo problemas para ejecutar ni para mostrar las particiones correspondientes.