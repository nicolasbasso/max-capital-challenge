# Order State Service - Max Capital Challenge

Servicio que consume `ExecutionReport` de forma asíncrona y mantiene el estado de cada orden
correcto bajo carga, reentregas y fallas, corriendo en dos instancias en paralelo.

El razonamiento detrás del diseño está en [`DECISIONS.md`](./DECISIONS.md).
La traducción del enunciado a compromisos verificables está en
[`docs/acceptance-matrix.md`](./docs/acceptance-matrix.md).

## Estado actual

> **Slice 1 - un ER recorre el sistema.** Un `NEW` publicado en Kafka se consume, se persiste como orden
> con su entrada de ledger, y se puede consultar por HTTP.
>
> Todavía **no** están: la máquina de estados y la terminalidad (D-005), las dos instancias en paralelo,
> las fallas inyectadas, el manejo de errores y cuarentena (D-006), y el settlement (D-007). Esas
> decisiones siguen abiertas y documentadas como preguntas en `DECISIONS.md`, no como conclusiones.

Este README se completa con los escenarios de demostración a medida que cada garantía se
implementa y se demuestra. No describe capacidades que el código todavía no tenga.

## Requisitos

- **Java 21.** El enunciado admite Java 21 o 25; se eligió 21 por ser el LTS con el soporte
  más maduro en Spring Boot, Testcontainers y drivers. Ninguna garantía del challenge
  necesita una feature exclusiva de 25.
- Docker y Docker Compose (para las etapas siguientes).
- Maven **no** hace falta: el repositorio incluye el wrapper (`./mvnw`).

El build **falla explícitamente** si se corre con un JDK distinto de 21. Es deliberado: la
restricción del enunciado es una condición del build, no una nota al pie.

### Fijar Java 21 sin alterar el resto de la máquina

En macOS con Homebrew:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
```

`openjdk@21` es keg-only: convive con otros JDK instalados sin ocupar el `java` del `PATH`.
La variable se exporta por shell o por proyecto; no hay que cambiar el `JAVA_HOME` global.

## Correr

```bash
./mvnw clean verify
```

Compila, corre los tests y empaqueta. Debe terminar en `BUILD SUCCESS`.

```bash
./mvnw spring-boot:run
```

Levanta el servicio en `http://localhost:8080`. Todavía no expone endpoints de negocio: hoy
sólo demuestra que el contexto de Spring levanta.

### Mutation testing

```bash
./mvnw test-compile org.pitest:pitest-maven:mutationCoverage
```

Genera el reporte en `target/pit-reports/`. No está atado a ninguna fase del ciclo de vida, así que no
encarece un `verify` normal.

La pregunta que responde no es *cuánto código tocan los tests*, sino **si los tests detectan un cambio de
comportamiento**. PIT altera el bytecode (invierte condiciones, cambia retornos, elimina llamadas) y verifica
que algún test falle. Un mutante que sobrevive es una línea que se puede romper sin que ninguna prueba se
entere.

Mientras no exista lógica de dominio no se generan mutantes. El umbral de mutation score se activa junto con
la máquina de estados de la orden.

### Levantar la infraestructura

```bash
docker compose up -d
```

Levanta PostgreSQL en el puerto `5433` y Kafka en el `19092`. Los puertos y los nombres de contenedor son
propios del proyecto para no colisionar con otros stacks.

```bash
docker compose down -v
```

Baja todo y borra el volumen de datos.

### Consultar una orden

```bash
curl -s http://localhost:8080/orders/13144742 | jq
```

Devuelve estado, cantidad de ejecuciones aplicadas y el ledger en orden de inserción. Una orden inexistente
devuelve `404` con el código `ORDER_NOT_FOUND`.

### Variables de entorno

| Variable | Default | Para qué |
|---|---|---|
| `SERVER_PORT` | `8080` | Puerto HTTP. |
| `APP_INSTANCE_ID` | `local` | Identifica la instancia en cada línea de log. Permite correlacionar qué orden procesó cuál de las dos instancias cuando haya que demostrarlo. |

## Ejercitar los escenarios a mano

Todo lo de abajo está verificado; los comandos se pueden copiar tal cual.

### Preparar

```bash
docker compose up -d
./mvnw clean package
java -jar target/order-state-service-0.0.1-SNAPSHOT.jar
```

### Publicar un execution report

La key del mensaje es el `numericOrderId`: es lo que hace que todos los ER de una orden caigan en
la misma partición.

```bash
echo '13144742:{"fixId":"FIX-0001","numericOrderId":13144742,"status":"NEW","ticker":"VSCPC"}' \
  | docker exec -i maxcapital-kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic execution-reports \
      --property parse.key=true --property key.separator=:
```

En el log del servicio:

```
INFO ExecutionReportConsumer - applied numericOrderId=13144742 fixId=FIX-0001 partition=2 offset=0
```

### Consultar la orden

```bash
curl -s http://localhost:8080/orders/13144742 | jq
```

```json
{
  "numericOrderId": 13144742,
  "status": "NEW",
  "appliedExecutions": 1,
  "ledger": [
    { "id": 1, "fixId": "FIX-0001", "status": "NEW", "recordedAt": "..." }
  ]
}
```

Una orden inexistente devuelve `404` con `ORDER_NOT_FOUND`.

### Duplicado

Publicar **exactamente el mismo mensaje** otra vez. El servicio lo detecta y no lo aplica:

```
INFO ExecutionReportConsumer - duplicate ignored numericOrderId=13144742 fixId=FIX-0001 partition=2 offset=1
```

El estado no cambia: `appliedExecutions` sigue en 1 y el ledger sigue con una entrada.

### Ver la evidencia en los tres lugares

Esta es la parte que importa: los tres números cuentan la misma historia desde sistemas distintos.

**Cuántos mensajes hay en el topic y hasta dónde llegó el consumidor:**

```bash
docker exec maxcapital-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-state-service
```

```
GROUP                TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-state-service  execution-reports  2          2               2               0
```

**Los mensajes siguen en el log, consumidos y todo:**

```bash
docker exec maxcapital-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic execution-reports \
  --from-beginning --timeout-ms 4000 --property print.key=true --property print.partition=true
```

**Y lo que realmente se aplicó:**

```bash
docker exec maxcapital-postgres psql -U orderstate -d orderstate \
  -c "SELECT numeric_order_id, status, applied_executions FROM orders;" \
  -c "SELECT id, numeric_order_id, fix_id, status FROM execution_ledger ORDER BY id;"
```

Dos mensajes consumidos, una sola aplicación. La barrera que lo impide es la restricción única:

```bash
docker exec maxcapital-postgres psql -U orderstate -d orderstate -c "\d execution_ledger"
```

```
"uq_execution_ledger_order_fix" UNIQUE CONSTRAINT, btree (numeric_order_id, fix_id)
```

## Verificación de Slice 0

| Qué se demuestra | Cómo | Resultado |
|---|---|---|
| El build limpio pasa con Java 21 | `./mvnw clean verify` | `BUILD SUCCESS`, 2 tests en verde |
| El build rechaza un JDK no permitido | `JAVA_HOME=<jdk-no-21> ./mvnw clean verify` | `BUILD FAILURE` con el mensaje del enforcer |
| El artefacto empaquetado arranca | `java -jar target/order-state-service-0.0.1-SNAPSHOT.jar` | `Started OrderStateServiceApplication` |

## Sobre el proceso

Las decisiones de diseño de este repositorio se trabajaron usando un modelo de lenguaje como *sparring*: para
cuestionar alternativas, forzar escenarios de falla concretos y contrastar afirmaciones contra la documentación
oficial de cada tecnología. Los commits llevan el trailer `Co-Authored-By` correspondiente.

El razonamiento y los trade-offs registrados en [`DECISIONS.md`](./DECISIONS.md) son propios y defendibles uno
por uno, incluyendo qué garantiza cada componente, qué no garantiza, y qué se resignó a conciencia.
