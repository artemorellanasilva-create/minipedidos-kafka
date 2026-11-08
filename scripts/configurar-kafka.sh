#!/bin/bash
set -e

TOPICO="pedidos-creados"
PARTICIONES=3

echo "Verificando el contenedor Kafka..."

if ! docker inspect -f '{{.State.Running}}' kafka 2>/dev/null | grep -q '^true$'; then
    echo "Iniciando Kafka..."
    docker compose up -d kafka
fi

echo "Esperando que Kafka esté disponible..."

for intento in $(seq 1 30); do
    if docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        break
    fi

    if [ "$intento" -eq 30 ]; then
        echo "ERROR: Kafka no respondió."
        exit 1
    fi

    sleep 2
done

echo "Verificando tópico $TOPICO..."

if docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --topic "$TOPICO" \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; then

    echo "El tópico ya existe."

else
    echo "Creando tópico..."

    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --create \
        --topic "$TOPICO" \
        --bootstrap-server localhost:9092 \
        --partitions "$PARTICIONES" \
        --replication-factor 1
fi

echo "Mostrando configuración del tópico..."

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --topic "$TOPICO" \
    --bootstrap-server localhost:9092

echo "Configuración finalizada correctamente."
