#!/usr/bin/env bash
# Emisor deterministico. Publica un escenario fijo de ExecutionReports en Kafka.
#
# Deterministico quiere decir que el mismo escenario produce siempre la misma secuencia de ER
# en el mismo orden, asi que el estado final es predecible y asserteable. Los escenarios son
# archivos de texto en scenarios/, una linea por ER, en formato "key|payload".
#
# La key es siempre el numericOrderId: es lo que garantiza que todos los ER de una orden caigan
# en la misma particion y los consuma una sola instancia, en orden.
#
# Uso:
#   ./scripts/emit.sh 01-lifecycle          publica de una
#   ./scripts/emit.sh 01-lifecycle 0.5      publica con 0.5s entre ER, para poder matar una
#                                           instancia a mitad del stream
set -euo pipefail

ESCENARIO="${1:-}"
PAUSA="${2:-0}"
ARCHIVO="scenarios/${ESCENARIO}.txt"

if [[ -z "$ESCENARIO" || ! -f "$ARCHIVO" ]]; then
  echo "Escenarios disponibles:"
  ls scenarios/*.txt 2>/dev/null | sed 's|scenarios/||; s|\.txt||; s|^|  |'
  exit 1
fi

publicar() {
  docker exec -i maxcapital-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic execution-reports \
    --property parse.key=true \
    --property key.separator='|'
}

TOTAL=$(grep -c . "$ARCHIVO")
echo "emitiendo $ESCENARIO: $TOTAL execution reports"

if [[ "$PAUSA" == "0" ]]; then
  publicar < "$ARCHIVO"
else
  while IFS= read -r linea; do
    [[ -z "$linea" ]] && continue
    echo "$linea" | publicar
    sleep "$PAUSA"
  done < "$ARCHIVO"
fi

echo "listo"
