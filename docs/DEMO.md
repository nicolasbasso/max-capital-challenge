# Reproducir los escenarios

Cada escenario arranca de base limpia, levanta las dos instancias, emite, **muestra la evidencia**
y verifica. Termina en `OK` si el contador iguala las entradas del ledger y ningún ER se aplicó dos
veces.

```bash
docker compose build            # solo la primera vez

./scripts/demo.sh 01-lifecycle    # ordenes intercaladas, cada una completa
./scripts/demo.sh 02-duplicates   # reentregas: no-op, el contador no se mueve
./scripts/demo.sh 03-rejections   # rechazos: orden congelada y ER preservado
./scripts/demo.sh 04-rebalance    # mata app-1 a mitad del stream
```

| Escenario | Qué prueba | Evidencia que imprime |
|---|---|---|
| `01-lifecycle` | Órdenes distintas en paralelo, secuencia por orden | Ledger completo: los `id` se intercalan entre órdenes y crecen dentro de cada una |
| `02-duplicates` | Idempotencia, y que la dedup corre antes que la máquina de estados | Log de las dos instancias: `applied` vs `duplicate ignored` |
| `03-rejections` | Nada se descarta: el ER rechazado se preserva con su motivo | Tabla de cuarentena con motivo y montos |
| `04-rebalance` | Una caída no pierde ni duplica | Reparto de ER entre instancias y toma de particiones |

## Encadenar escenarios y bajar el stack

Cada escenario arranca con `docker compose down -v`, así que **entre uno y otro no hay que hacer
nada**: se corre el siguiente y listo. Lo mismo implica que el escenario nuevo se lleva puestos los
datos y los logs del anterior.

Al terminar:

```bash
docker compose down -v
```

## Logs

Al final de cada corrida el script deja los logs de los cuatro servicios en
`logs/<escenario>-<fecha>.log`, incluso si la verificación falla. Es la única copia que sobrevive al
escenario siguiente.

Para seguirlos en vivo, desde otra terminal, una vez que el paso 1 levantó las instancias. El `-p`
evita tener que pararse en la raíz del repo, que es el error fácil cuando la terminal nueva abre en
`~`:

```bash
docker compose -p max-capital-challenge logs -f app-1 app-2
```

Ese `-f` termina solo cuando el próximo escenario hace `down -v`.

Consultar una orden:

```bash
curl -s localhost:8081/orders/5101      # app-2 responde igual en 8082
```

Mirar la base a mano:

```bash
docker exec -it maxcapital-postgres psql -U orderstate -d orderstate
```
