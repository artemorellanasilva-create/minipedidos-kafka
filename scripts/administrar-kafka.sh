
#!/usr/bin/env bash

# MiniPedidos V2 - Sesion 3
# Administracion de Apache Kafka

set -uo pipefail

CONTENEDOR="kafka"
BROKER="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

kafka_cli() {
    docker exec "$CONTENEDOR" \
        "$KAFKA_BIN/$1" "${@:2}"
}

verificar_broker() {
    if ! docker inspect -f '{{.State.Running}}' \
        "$CONTENEDOR" 2>/dev/null | grep -qx true; then
        echo "ERROR: El contenedor Kafka no esta activo."
        return 1
    fi

    if ! kafka_cli kafka-broker-api-versions.sh \
        --bootstrap-server "$BROKER" >/dev/null 2>&1; then
        echo "ERROR: El broker Kafka no responde."
        return 1
    fi
}

solicitar_topico() {
    read -r -p "Nombre del topico: " TOPICO

    if [[ ! "$TOPICO" =~ ^[a-zA-Z0-9._-]+$ ]] ||
       [[ "$TOPICO" == "." ]] ||
       [[ "$TOPICO" == ".." ]]; then
        echo "ERROR: Nombre de topico no valido."
        return 1
    fi
}

listar_topicos() {
    kafka_cli kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --list
}

consultar_topico() {
    solicitar_topico || return 1

    kafka_cli kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --describe \
        --topic "$TOPICO"
}

crear_topico() {
    solicitar_topico || return 1

    read -r -p "Numero de particiones: " PARTICIONES
    read -r -p "Factor de replicacion: " REPLICAS

    if [[ ! "$PARTICIONES" =~ ^[1-9][0-9]*$ ]] ||
       [[ ! "$REPLICAS" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: Debes ingresar enteros positivos."
        return 1
    fi

    kafka_cli kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --create \
        --topic "$TOPICO" \
        --partitions "$PARTICIONES" \
        --replication-factor "$REPLICAS"
}

consultar_configuracion() {
    solicitar_topico || return 1

    kafka_cli kafka-configs.sh \
        --bootstrap-server "$BROKER" \
        --entity-type topics \
        --entity-name "$TOPICO" \
        --describe
}

modificar_retencion() {
    solicitar_topico || return 1

    read -r -p "Retencion en dias (entero positivo): " DIAS

    if [[ ! "$DIAS" =~ ^[1-9][0-9]*$ ]] ||
       (( ${#DIAS} > 5 )); then
        echo "ERROR: Numero de dias no valido."
        return 1
    fi

    MS=$((10#$DIAS * 86400000))

    echo "Topico: $TOPICO"
    echo "Nueva retencion: $DIAS dias ($MS ms)"
    read -r -p "Confirmar modificacion (SI): " CONFIRMAR

    if [[ "$CONFIRMAR" != "SI" ]]; then
        echo "Operacion cancelada."
        return 0
    fi

    kafka_cli kafka-configs.sh \
        --bootstrap-server "$BROKER" \
        --entity-type topics \
        --entity-name "$TOPICO" \
        --alter \
        --add-config "retention.ms=$MS"
}

if ! verificar_broker; then
    exit 1
fi

while true; do
    echo ""
    echo "======================================"
    echo " MiniPedidos V2 - Administrar Kafka"
    echo "======================================"
    echo "1. Listar topicos"
    echo "2. Consultar particiones y replicas"
    echo "3. Crear topico"
    echo "4. Consultar configuraciones"
    echo "5. Modificar retencion"
    echo "0. Salir"
    echo "======================================"

    read -r -p "Selecciona una opcion: " OPCION || break

    case "$OPCION" in
        1) listar_topicos ;;
        2) consultar_topico ;;
        3) crear_topico ;;
        4) consultar_configuracion ;;
        5) modificar_retencion ;;
        0) echo "Saliendo..."; break ;;
        *) echo "Opcion no valida." ;;
    esac
done
