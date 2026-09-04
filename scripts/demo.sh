#!/usr/bin/env bash
# Corre un escenario de punta a punta y muestra la evidencia: base limpia, dos instancias,
# emision, y las consultas que prueban cada garantia.
set -u   # sin pipefail: grep -q cierra el pipe y docker logs muere con SIGPIPE
cd "$(dirname "$0")/.."

ESC="${1:-}"
if [[ ! -f "scenarios/${ESC}.txt" ]]; then
  echo "uso: ./scripts/demo.sh <escenario>"
  ls scenarios/*.txt | sed 's|scenarios/||; s|\.txt||; s|^|  |'
  exit 1
fi

pg()  { docker exec maxcapital-postgres psql -U orderstate -d orderstate "$@"; }
titulo() { printf '\n\033[1m%s\033[0m\n' "$*"; }
lag_total() {
  docker exec maxcapital-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group order-state-service --describe 2>/dev/null \
    | awk 'NF>5 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

titulo "1. Base limpia y dos instancias"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null
until docker logs maxcapital-app-1 2>&1 | grep -q "Started OrderState" \
   && docker logs maxcapital-app-2 2>&1 | grep -q "Started OrderState"; do sleep 3; done
docker compose ps --format '  {{.Name}}  {{.Status}}'
# Cual de las dos gana la carrera de Flyway es no deterministico: la que llega primero migra
# y la otra encuentra el esquema al dia. El lock de Flyway las coordina sin que hagamos nada.
echo "  migraciones:"
for i in 1 2; do
  docker logs "maxcapital-app-$i" 2>&1 \
    | grep -oE "Successfully applied [0-9]+ migrations|is up to date" \
    | head -1 | sed "s/^/    app-$i: /" || true
done

titulo "2. Emision del escenario $ESC"
if [[ "$ESC" == "04-rebalance" ]]; then
  ./scripts/emit.sh "$ESC" 0.4 & EMIT=$!
  sleep 5
  echo "  >>> matando app-1 a mitad del stream"
  docker kill maxcapital-app-1 >/dev/null
  wait $EMIT
else
  ./scripts/emit.sh "$ESC"
fi
for _ in $(seq 1 60); do [[ "$(lag_total)" == "0" ]] && break; sleep 2; done
echo "  lag pendiente: $(lag_total)"

titulo "3. Estado final de las ordenes"
pg -c "
select o.numeric_order_id as orden, o.status, o.applied_executions as aplicados,
       (select count(*) from execution_ledger l where l.numeric_order_id=o.numeric_order_id) as ledger,
       (select count(*) from execution_quarantine q where q.numeric_order_id=o.numeric_order_id) as cuarentena
from orders o order by 1;"

titulo "4. Evidencia especifica del escenario"
case "$ESC" in
  01-lifecycle)
    echo "  Los id se intercalan entre ordenes (avanzan en paralelo) y crecen dentro de cada una"
    echo "  (mantienen su secuencia). El orden lo da id, nunca transactionTime."
    pg -c "select numeric_order_id as orden, id as ledger_id, fix_id, status
           from execution_ledger order by numeric_order_id asc, id asc;" ;;
  02-duplicates)
    echo "  Se emitieron 6 ER y solo 3 se aplicaron. Los duplicados se detectan por identidad."
    echo "  El ultimo es un NEW repetido sobre una orden ya FILLED: si la deduplicacion no"
    echo "  corriera primero, congelaria una orden que se completo bien."
    for i in 1 2; do docker logs "maxcapital-app-$i" 2>&1 \
      | grep -oE "(applied|duplicate ignored) numericOrderId=[0-9]+ fixId=[A-Za-z0-9-]+" \
      | sed 's/^/    /' || true; done ;;
  03-rejections)
    echo "  Cada ER rechazado se preserva con su motivo y el estado contra el que se rechazo."
    pg -c "select numeric_order_id as orden, fix_id, incoming_status as llego,
                  order_status_at_rejection as estaba, reason as motivo,
                  nominal_amount as total, accumulative_nominal_amount as acum,
                  leaves_nominal_amount as resto
           from execution_quarantine order by 1;" ;;
  04-rebalance)
    echo "  app-1 murio a mitad del stream y app-2 tomo sus particiones."
    echo "    ER aplicados por app-1: $(docker logs maxcapital-app-1 2>&1 | grep -c 'applied numericOrderId' || true)"
    echo "    ER aplicados por app-2: $(docker logs maxcapital-app-2 2>&1 | grep -c 'applied numericOrderId' || true)"
    docker logs maxcapital-app-2 2>&1 | grep -oE "partitions assigned: \[.*\]" | tail -1 | sed 's/^/    app-2: /' || true ;;
esac

titulo "5. Verificacion"
DESALINEADAS=$(pg -tAc "
  select count(*) from orders o
  where o.applied_executions <> (select count(*) from execution_ledger l
                                 where l.numeric_order_id = o.numeric_order_id);")
DOBLES=$(pg -tAc "
  select count(*) from (select numeric_order_id, fix_id from execution_ledger
                        group by 1,2 having count(*) > 1) d;")
if [[ "$DESALINEADAS" == "0" && "$DOBLES" == "0" ]]; then
  echo "  OK  el contador iguala las entradas del ledger en todas las ordenes"
  echo "  OK  ningun (numericOrderId, fixId) aparece dos veces"
else
  echo "  FALLA  ordenes desalineadas=$DESALINEADAS  duplicados=$DOBLES"
  exit 1
fi

titulo "6. Consultar cualquier orden por HTTP"
PRIMERA=$(pg -tAc "select min(numeric_order_id) from orders;")
echo "  curl -s localhost:8081/orders/$PRIMERA"
echo "  (app-2 responde lo mismo en 8082: leen la misma base)"
