# DECISIONS

Este documento justifica el diseño. Cada decisión se cierra sólo cuando existe evidencia reproducible que la
respalde. Mientras una decisión esté abierta, la sección conserva la pregunta que debe responder y la
evidencia que la validará, en vez de una conclusión prematura.

**Regla de honestidad del documento:** no se afirma una garantía mayor que la demostrada. Si algo quedó fuera
de alcance, se declara acá en vez de simularlo.

**Alcance del documento:** registra las conclusiones, no la deliberación. Cada decisión se mantiene en el orden
de una página para que se pueda leer entera junto con el código.

## Estado de las decisiones

Una decisión atraviesa cuatro estados. La distinción importa: elegir no es lo mismo que haber demostrado.

| Estado | Significado |
|---|---|
| **Abierta** | La pregunta está formulada; no hay elección tomada. |
| **Elegida** | Hay una opción escogida y justificada, pero **la evidencia todavía no existe**. |
| **Implementada** | El mecanismo está en el código. |
| **Validada** | Existe una prueba reproducible que demuestra la garantía. |

| ID | Decisión | Estado | Evidencia |
|---|---|---|---|
| D-001 | Broker, particionado y secuencia por orden | **Elegida** | Pendiente: assert de offsets en T4.x / T5.3 |
| D-002 | Identidad individual del ER y clave de deduplicación | **Elegida** | Pendiente: reentrega y republicación en T3.2 |
| D-003 | Frontera transaccional, ACK/offset y recuperación | Abierta | — |
| D-004 | Modelo persistente de orden, ledger y concurrencia | Abierta | — |
| D-005 | Máquina de estados y terminalidad | Abierta | — |
| D-006 | Errores transitorios, permanentes y orden incompleta | Abierta | — |
| D-007 | Settlement, outbox y deduplicación downstream | Abierta | — |
| D-008 | Alcance declarado y trabajo fuera de alcance | Abierta | — |

---

## D-001 - Broker, particionado y secuencia por orden

**Estado:** elegida. La evidencia que la valida todavía no existe.

**Requisito.** Los ER de una misma orden se aplican en su secuencia de emisión; órdenes distintas avanzan en
paralelo; dos instancias consumen el mismo flujo, con failover.

**Decisión.** Kafka: topic particionado, `numericOrderId` como message key, ambas instancias en el mismo
consumer group. `enable.auto.commit=false`; el offset se commitea **después** del commit en PostgreSQL.

**Alternativa.** RabbitMQ con `x-modulus-hash` y Single Active Consumer **cubre las cuatro
garantías igual**: la elección no se apoya en capacidad. Super Streams se descartó sin evaluación profunda —
agrega protocolo Stream y manejo de offsets propio sin resolver nada que las otras dos no resuelvan.

**Por qué Kafka.**

1. Nos permite de manera consultiva saber el histórico de los mensajes y podemos reconstruir el ciclo de vida
   de una orden en un tiempo determinado, ya que existe la política de retención y es configurable, no queda
   para siempre. Mientras que Rabbit una vez recibido el ack borra el mensaje.
   Viniendo del lado bancario, que en procesos no solo de auditoría, sino también en algún proceso judicial
   nos han pedido poder armar el ciclo de vida de una transacción desde su login hasta que se otorga un
   préstamo por ejemplo, valoro mucho el hecho de reconstruir el ciclo de vida de la operación o transacción
   porque podemos saber exactamente en dónde estuvo la falla.
2. Nos permite consultar el offset para poder comprobar mediante testing que la reentrega se realizó, y en el
   caso correcto que no aplicamos el mismo ER reentregado (lo protege la idempotencia del dato durable en
   nuestra PostgreSQL) y es algo que debemos asegurar, en cambio Rabbit sólo nos agrega al mensaje un campo
   booleano de `redelivered` y en ese caso tenemos que confiar en un log en caso de estar viendo en runtime y
   nos hace más complejo comprobarlo.

**Se resigna frente a RabbitMQ.**

1. La DLQ conlleva una configuración manual.
2. El `docker-compose` es un poco más pesado, trivial.

**Evidencia que la validará.**

1. Dos instancias con órdenes intercaladas conservando el orden interno de cada una; y una caída entre el
   commit de PostgreSQL y el del offset, comparando el offset commiteado antes y después, con una sola entrada
   de ledger resultante.

**Supuestos.**

A raíz de las garantías en el enunciado: se va a correr un nodo con replicación 1, sé que eso pierde ante falla
del broker, lo dejo así porque el ejercicio acota a caídas del servicio y no amerita configuración de un
sistema de resiliencia ante caída del broker.

---

## D-002 - Identidad individual del ER y clave de deduplicación

**Estado:** elegida. La evidencia que la valida todavía no existe.

**Requisito.** Un ER duplicado o reentregado no debe corromper el estado. `numericOrderId` identifica la
**orden** y se repite en todos sus ER: muchos ER por orden es lo normal, no un duplicado.

**Qué protege.** La condición de *"exactamente una vez"* de la invariante de secuencia, en
[`docs/acceptance-matrix.md`](./docs/acceptance-matrix.md).

**Decisión.** Clave compuesta `(numericOrderId, fixId)`, con restricción de unicidad durable en PostgreSQL,
sin hashear.

**Por qué compuesta.** El enunciado no garantiza que `fixId` sea único globalmente. La clave compuesta es
correcta en los dos escenarios: si lo es, funciona; si sólo es único dentro de la orden, es la única que
funciona.

**Por qué `fixId` y no `secondaryTradeId`,** que el enunciado anota como identidad para dedup: todos los ER son
mensajes, pero no todos son ejecuciones — un `NEW` y un `CANCELLED` no operan nada. Como se exige una entrada
de ledger por ER **efectivamente aplicado** y no por ejecución, la clave tiene que identificar también a esos.

**Por qué no el offset de Kafka.** No cubre la republicación del emisor, que el enunciado distingue de la
reentrega: mismo hecho de negocio, dos offsets. Y metería un dato del transporte dentro del dominio, cuando el
requisito de idempotencia es anterior a la elección de broker. De ahí la regla: **la clave sale del payload,
nunca del transporte** — un campo del payload es idéntico en los dos casos.

**Identidad, no ordinalidad.** No hay número secuencial en el payload, y `status` no ordena parciales
repetidos. No hace falta: el orden lo garantiza D-001. La clave sólo responde *"¿ya lo apliqué?"*.

**Sin hash.** Una colisión sería una pérdida silenciosa, y la clave natural es legible al cruzar el ledger
contra el topic para demostrar la ventana de caída.

**Supuestos declarados.** `fixId` está presente y no vacío en todo ER, incluidos `NEW` y `CANCELLED`; y un
duplicado se republica idéntico, `fixId` incluido, que es como lo emite el generador determinístico del
ejercicio. No hay documentación del formato más allá del enunciado, que autoriza a resolver la duda y anotar
la suposición.

**Si el supuesto falla.** Un `fixId` ausente o vacío es un error permanente: se preserva en cuarentena y la
orden queda marcada como explícitamente incompleta. Ni descarte silencioso ni aplicación sin deduplicar.
El mecanismo se define en D-006. El supuesto es barato porque falla ruidoso: si faltara sistemáticamente,
todas las órdenes irían a cuarentena en su primer ER.

**Límite declarado.** Si un emisor real asignara un `fixId` nuevo en cada transmisión del mismo hecho, la clave
debería moverse a `secondaryTradeId`, con su probable ausencia en los ER que no ejecutan.

**Evidencia que la validará.** El mismo ER entregado dos veces —por reentrega tras una caída y por
republicación del emisor— produce una sola entrada de ledger y un solo incremento del contador.

---

## D-003 - Frontera transaccional, ACK/offset y recuperación

**Pregunta que debe responder:** ¿cuál es el resultado previsto ante una caída en cada ventana — antes de la
transacción, dentro de la transacción, después del commit y antes del ACK/commit de offset? ¿Por qué ninguna
de esas ventanas pierde ni duplica un ER?

**Sub-decisión heredada de D-001:** la granularidad del commit de offset. Desactivar el commit automático no
define si el offset se confirma por registro, por lote o manualmente. Spring Kafka usa `BATCH` por defecto, lo
que confirma el lote entero después de procesarlo: una caída a mitad del lote reentrega **todos** sus
registros, no sólo el que faltaba. La elección determina el tamaño exacto de la ventana de reentrega y por lo
tanto cuántas veces se ejercita la barrera de idempotencia.

**Evidencia que la validará:** una falla inyectada en cada ventana, con el resultado predicho antes de
ejecutarla y verificado después.

**Estado:** abierta.

---

## D-004 - Modelo persistente de orden, ledger y concurrencia

**Pregunta que debe responder:** ¿cómo se persiste la orden y su ledger de modo que el contador de
ejecuciones y la cantidad de entradas del ledger no puedan desalinearse, ni siquiera bajo intentos
concurrentes de dos instancias? ¿Qué motor y por qué?

**Evidencia que la validará:** intentos concurrentes sobre la misma orden no duplican entradas ni contador;
el ledger se devuelve en orden de inserción.

**Estado:** abierta.

---

## D-005 - Máquina de estados y terminalidad

**Pregunta que debe responder:** ¿cómo se computa el nuevo estado a partir del estado ya persistido más el ER
entrante, en vez de copiar el último mensaje? ¿Qué pasa con un ER que llega después de `FILLED` o
`CANCELLED`, y por qué la detección de duplicado ocurre antes del rechazo por terminalidad?

**Evidencia que la validará:** ciclo de vida completo con contador y ledger alineados; ER posterior a un
estado terminal no se aplica; reentrega del mismo ER terminal es un no-op y no un error.

**Estado:** abierta.

---

## D-006 - Errores transitorios, permanentes y orden incompleta

**Pregunta que debe responder:** ¿cómo se distinguen errores transitorios de permanentes, y cómo se evita a
la vez el descarte silencioso y el bloqueo indefinido del flujo? ¿Qué le pasa a una orden a la que le falta
un ER, y por qué una DLQ por sí sola no la repara?

**Evidencia que la validará:** un ER inválido de la orden A, un ER posterior de A y un ER de B producen
resultados diferenciados y observables; B sigue avanzando.

**Estado:** abierta.

**Alcance:** el enunciado no exige implementar el mecanismo completo, sólo que nada se descarte en silencio.
Lo que quede sin implementar se documenta acá.

---

## D-007 - Settlement, outbox y deduplicación downstream

**Pregunta que debe responder:** ¿cómo se garantiza que una orden `FILLED` produzca exactamente un efecto
lógico de settlement downstream, sin pérdida ni duplicados, aun con reentregas y dos instancias? ¿Por qué la
publicación física al broker no es exactly-once por sí sola, y dónde vive realmente la garantía?

**Evidencia que la validará:** `FILLED` crea un settlement; el duplicado de `FILLED` sigue dejando uno;
`CANCELLED` crea cero. Caída antes de publicar y caída después de publicar antes de registrar el resultado,
ambas verificadas.

**Estado:** abierta.

---

## D-008 - Alcance declarado y trabajo fuera de alcance

**Pregunta que debe responder:** ¿qué se dejó deliberadamente afuera, por qué, y cómo se resolvería en una
versión completa?

**Estado:** abierta. Se completa al final, desde lo efectivamente construido.
