
#!/usr/bin/env bash

# ==========================================
# MiniPedidos V2 - Sesion 3
# Verificacion automatica de Apache Kafka
# ==========================================

set -euo pipefail

CONTENEDOR="kafka"
BROKER="localhost:9092"

TOPICOS=(
  "pedidos-creados"
  "pedidos-alertas"
)

PARTICIONES_ESPERADAS=3
REPLICAS_ESPERADAS=1
RETENCION_ESPERADA=604800000

errores=0

echo "=========================================="
echo "    MiniPedidos V2 - Verificacion Kafka"
echo "=========================================="

# 1. Verificar contenedor
echo ""
echo "[1] Verificando contenedor Kafka..."

if [ "$(docker inspect -f '{{.State.Running}}' "$CONTENEDOR" 2>/dev/null || true)" != "true" ]; then
    echo "ERROR: El contenedor Kafka no esta activo."
    echo "Ejecuta: docker compose up -d"
    exit 1
fi

echo "OK: Contenedor Kafka activo."

# 2. Verificar conectividad con el broker
echo ""
echo "[2] Verificando conexion con Kafka..."

if ! docker exec "$CONTENEDOR" \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BROKER" > /dev/null 2>&1; then

    echo "ERROR: Kafka no responde en $BROKER"
    exit 1
fi

echo "OK: Broker Kafka disponible."

# 3. Verificar los topicos
echo ""
echo "[3] Verificando topicos..."

for TOPICO in "${TOPICOS[@]}"; do

    echo ""
    echo "------------------------------------------"
    echo "Topico: $TOPICO"
    echo "------------------------------------------"

    DESCRIPCION=$(docker exec "$CONTENEDOR" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BROKER" \
        --describe \
        --topic "$TOPICO" 2>/dev/null) || {
            echo "ERROR: No se pudo consultar $TOPICO"
            errores=$((errores + 1))
            continue
        }

    echo "$DESCRIPCION"

    if ! echo "$DESCRIPCION" | grep -q "PartitionCount: $PARTICIONES_ESPERADAS"; then
        echo "ERROR: Numero de particiones incorrecto."
        errores=$((errores + 1))
    else
        echo "OK: 3 particiones."
    fi

    if ! echo "$DESCRIPCION" | grep -q "ReplicationFactor: $REPLICAS_ESPERADAS"; then
        echo "ERROR: Factor de replicacion incorrecto."
        errores=$((errores + 1))
    else
        echo "OK: Factor de replicacion 1."
    fi

    # Verificar lideres e ISR
    if echo "$DESCRIPCION" | grep "Partition:" | grep -Eq 'Leader: -1|Isr:($|[[:space:]]*$)'; then
        echo "ERROR: Existen particiones sin lider o ISR."
        errores=$((errores + 1))
    else
        echo "OK: No se detectaron particiones sin lider o ISR."
    fi

    # 4. Verificar configuraciones
    echo ""
    echo "Consultando configuraciones..."

    CONFIG=$(docker exec "$CONTENEDOR" \
        /opt/kafka/bin/kafka-configs.sh \
        --bootstrap-server "$BROKER" \
        --entity-type topics \
        --entity-name "$TOPICO" \
        --describe 2>/dev/null) || {
            echo "ERROR: No se pudo consultar la configuracion."
            errores=$((errores + 1))
            continue
        }

    if echo "$CONFIG" | grep -q "retention.ms=$RETENCION_ESPERADA"; then
        echo "OK: Retencion de 7 dias."
    else
        echo "ERROR: Retencion diferente a la esperada."
        errores=$((errores + 1))
    fi

    if echo "$CONFIG" | grep -q "cleanup.policy=delete"; then
        echo "OK: Cleanup policy delete."
    else
        echo "ERROR: Cleanup policy incorrecta."
        errores=$((errores + 1))
    fi

    if echo "$CONFIG" | grep -q "min.insync.replicas=1"; then
        echo "OK: Minimo de replicas sincronizadas = 1."
    else
        echo "ERROR: min.insync.replicas diferente."
        errores=$((errores + 1))
    fi

done

# 5. Resumen
echo ""
echo "=========================================="
echo "           RESULTADO FINAL"
echo "=========================================="

if [ "$errores" -eq 0 ]; then
    echo "OK: Todas las verificaciones finalizaron."
    echo "MiniPedidos V2 - Kafka configurado."
else
    echo "ATENCION: Se detectaron $errores errores."
    exit 1
fi
