
#!/bin/bash

set -e

URL="http://localhost:8080/api/pedidos"

echo "================================="
echo "  MiniPedidos V2 - Prueba Kafka"
echo "================================="

echo ""
echo "Enviando pedido 1: Notebook"
curl -fsS -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{"producto":"Notebook","cantidad":2,"precio":450000.0}'

echo ""
echo ""
echo "Enviando pedido 2: Mouse"
curl -fsS -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{"producto":"Mouse","cantidad":3,"precio":15000.0}'

echo ""
echo ""
echo "Enviando pedido 3: Teclado"
curl -fsS -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{"producto":"Teclado","cantidad":1,"precio":35000.0}'

echo ""
echo ""
echo "================================="
echo "  Solicitudes HTTP completadas"
echo "================================="
