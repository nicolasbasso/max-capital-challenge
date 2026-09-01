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
| D-003 | Frontera transaccional, ACK/offset y recuperación | **Elegida** | Pendiente: fallas inyectadas por ventana en T5.2-T5.4 |
| D-004 | Modelo persistente de orden, ledger y concurrencia | **Elegida** | Parcial: falta la de dos instancias reales (T4.4) |
| D-005 | Máquina de estados y terminalidad | **Elegida** | Pendiente: se construye en el slice 2 |
| D-006 | Errores transitorios, permanentes y orden incompleta | **Elegida** | Parcial: la cuarentena ya existe; falta la dead-letter y el backoff |
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

**Decisión.** Clave compuesta `(numericOrderId, fixId)`, con restricción de unicidad durable en PostgreSQL.

**Por qué.**

1. Elegí un ID compuesto porque no tenía un campo que sea único global dicho por el enunciado, por eso decidí
   tener el número de orden + el id del mensaje. Además, nos da visibilidad de lo procesado a simple vista en
   la base de datos.

   Analicé también el `secondaryTradeId` pero asumí que era un spanId, por ende iba a ser distinto por cada
   estado (`NEW`, `PARTIALLY_FILLED` y `FILLED`), además quizás en estados que no impacten al proceso no se
   envía por ejemplo: `NEW` o `CANCELLED`, y yo necesito marcar como único cada ER no importa si procesa
   porque hay que sumar uno al ledger.

   Por otro lado, también se me ocurrió el offset de Kafka, donde asumí que el `fixId` se mantendría del lado
   del cliente si llegara a duplicar el mensaje; en ese caso el offset sería distinto y duplicaría la entrada.
   Ahí fue donde entendí que un dato del "transporte" no me serviría, porque metería el transporte dentro del
   dominio.

2. Estoy buscando identidad en el mensaje, no un orden o cardinalidad: el orden me lo da el broker (Kafka en
   este caso).

**Qué asumí.**

1. `fixId` identificador del mensaje.
2. `fixId` siempre presente. Si no viene, va a la dead-letter; la orden no se marca, porque sin
   `numericOrderId` no hay orden que identificar (ver D-006).
3. Un duplicado se publica idéntico en su payload.
4. `secondaryTradeId` id por estado de procesamiento de la orden, un `spanId`; es decir, vacío en estados
   `NEW` y `CANCELLED`.

**Evidencia que la validará.** El mismo ER entregado dos veces —por reentrega tras una caída y por
republicación del emisor— produce una sola entrada de ledger y un solo incremento del contador.

---

## D-003 - Frontera transaccional, ACK/offset y recuperación

**Estado:** elegida. La evidencia que la valida todavía no existe.

**Requisito.** Una caída a mitad de procesamiento no puede perder ni aplicar dos veces un ER. El commit en
PostgreSQL y el commit del offset son dos sistemas distintos y no son atómicos entre sí: siempre queda una
ventana entre uno y otro.

**Decisión.**

1. La transacción en PostgreSQL va primero; el commit del offset después.
2. Dentro de la transacción: leer el estado persistido, insertar en el ledger, actualizar estado y contador.
3. El `INSERT` en el ledger lleva la restricción única `(numericOrderId, fixId)` de D-002. Esa restricción es
   la barrera.
4. Se intenta el `INSERT` y el duplicado se maneja fuera de la transacción.
5. `AckMode.MANUAL_IMMEDIATE`: un commit por registro, invocado explícitamente después de que la transacción
   cerró.

**Ventanas de falla.**

| Se cae | En PostgreSQL | En Kafka | Al recuperarse | Qué lo salva |
|---|---|---|---|---|
| Antes de abrir la transacción | nada | offset viejo | se procesa normal | nada, no hay qué deshacer |
| Con la transacción abierta | nada | offset viejo | se procesa normal | el rollback de PostgreSQL |
| Después del commit, antes del offset | el ER aplicado | offset viejo | se reentrega y se detecta duplicado | la restricción única |
| Después del commit del offset | el ER aplicado | offset nuevo | no se reentrega | nada, ya cerró |

**Por qué así.**

- Luego de validar los distintos escenarios entendí que si bien `AckMode.RECORD` me daría en el mismo momento
  el commit del offset que `AckMode.MANUAL_IMMEDIATE`, tengo la posibilidad de dejar explícito que después de
  un catch y logueo va el commit del offset, y no darlo por hecho por Spring. Por esa razón, decidí dejarlo
  explícito en el código antes que depender de Spring para los defaults.
- También evalué la posibilidad de una validación por base de datos antes de arrancar a procesar, pero
  entendí que si 2 instancias llegaran a tener el mismo evento en el mismo momento les daría que no existe, y
  sólo la constraint en la base de datos me daría la verdad, dejando al motor responsable de la validación de
  duplicidad.
- El offset, después e independiente del proceso, me permite aplicar las validaciones de constraint contra
  idempotencia de manera correcta. Nunca voy a dar por finalizado un mensaje si no hice el procesamiento más
  su persistencia en PostgreSQL. De otra forma —*offset primero*— tengo riesgo de perder mensajes. En esta
  alternativa está la posibilidad de duplicidad, que atajamos con la idempotencia del motor mediante la unique
  key.

**Qué asumí.**

- Por el nivel de carga de este challenge no vale la pena utilizar lotes para procesar los eventos.

**Evidencia que la validará.** Una falla inyectada en cada ventana, prediciendo el resultado antes de
ejecutarla. La crítica es la tercera: caída entre el commit de PostgreSQL y el del offset, con el ER
reentregado y el ledger todavía en una sola entrada.

---

## D-004 - Modelo persistente de orden, ledger y concurrencia

**Estado:** elegida.

**Pregunta que debe responder:** ¿cómo se persiste la orden y su ledger de modo que el contador de
ejecuciones y la cantidad de entradas del ledger no puedan desalinearse, ni siquiera bajo intentos
concurrentes de dos instancias? ¿Qué motor y por qué?

**Decisión**

- PostgreSQL.
- Dos tablas: `orders` (estado y contador) y `execution_ledger` (una entrada por ER aplicado).
- Clave autoincremental `BIGSERIAL` en el ledger.
- Restricción única `(numeric_order_id, fix_id)` — la misma de D-002.
- Los timestamps los escribe la base: defaults en las columnas y un trigger para `updated_at`.

**Por qué PostgreSQL**

- No hubo una elección en verdad, yo necesitaba un motor que pueda realizar lo siguiente:
  transacciones, una restricción única durable, y una clave autoincremental. La barrera de
  idempotencia no puede vivir en memoria porque tiene que verla la otra instancia.
  La elección nuevamente no fue por capacidad sino por practicidad a la hora de implementar,
  configuración sencilla y conocida.
- Gracias a las transacciones pude integrar el ledger a la operación, y con eso si falla algo se
  hace rollback y no queda desfasada. La unión de esta transacción es el candado del contador.

**Evidencia que la validará:** intentos concurrentes sobre la misma orden no duplican entradas ni
contador; el ledger se devuelve en orden de inserción.

---

## D-005 - Máquina de estados y terminalidad

**Estado:** elegida.

**Pregunta que debe responder:** ¿cómo se computa el nuevo estado a partir del estado ya persistido más el ER
entrante, en vez de copiar el último mensaje? ¿Qué pasa con un ER que llega después de `FILLED` o
`CANCELLED`, y por qué la detección de duplicado ocurre antes del rechazo por terminalidad?

**Qué decidí**

- Una orden **sólo se abre con un `NEW`**.
- Todo ER que no se puede aplicar: no se aplica, se preserva, y la orden pasa a `INCOMPLETE`. (ver tabla)
- `INCOMPLETE` es un valor de `OrderStatus`, que pasa a tener cinco.
- Una orden `INCOMPLETE` queda **congelada**: no se le aplica nada más.

| Desde ↓ / Llega → | `NEW` | `PARTIALLY_FILLED` | `FILLED` | `CANCELLED` |
|---|---|---|---|---|
| *(no existe)* | se abre | no se aplica | no se aplica | no se aplica |
| `NEW` | no se aplica | se aplica | se aplica | se aplica |
| `PARTIALLY_FILLED` | no se aplica | se aplica | se aplica | se aplica |
| `FILLED` | no se aplica | no se aplica | no se aplica | no se aplica |
| `CANCELLED` | no se aplica | no se aplica | no se aplica | no se aplica |
| `INCOMPLETE` | no se aplica | no se aplica | no se aplica | no se aplica |

Todo "no se aplica" es lo mismo: el ER se preserva y la orden queda `INCOMPLETE`.

**Por qué**

- El enunciado indicaba que una orden iniciaba con un `NEW`, si nos encontramos con un `NEW` posterior a
  algún estado no es aplicable.
- Agregamos el estado `INCOMPLETE` al enum de estados aunque no es un estado del proveedor porque ya no
  estamos en su scope y es nuestro dominio, si bien representamos sus estados de manera correlativa este
  estado nos da visibilidad al momento de querer recuperar una orden y saber si dejamos de confiar en ella.
  Surgió de la prueba donde falló el `NEW` y recibimos `PARTIALLY_FILLED` que cuando la consultábamos nos
  devolvía 404 not found y sí hubo una orden pero no se guardó.
- La detección del duplicado tiene que ocurrir primero en el escenario que llegue 2 veces el mismo ER de un
  estado terminal. Porque tomando la premisa de antes esto sería un estado `INCOMPLETE` si no, y no es así:
  es un ER que ya procesamos y no es pérdida si tampoco lo procesamos.

**Qué asumí**

- Si no puedo aplicar un ER, la orden deja de ser confiable y prefiero pasar a un estado `INCOMPLETE`.
- Aunque tenga una orden en un estado terminal (`FILLED` o `CANCELLED`) y reciba otro ER, esa orden pasa a
  estado `INCOMPLETE`, no puedo distinguir por qué pasó eso y la orden deja de ser confiable. No puedo ni
  ignorar el ER que vino luego ni percibir la orden como terminada.
- Al poner un estado `INCOMPLETE` pierdo el último estado del mercado.

**Evidencia que la validará:** ciclo de vida completo con contador y ledger alineados; ER posterior a un
estado terminal no se aplica; reentrega del mismo ER terminal es un no-op y no un error.

---

## D-006 - Errores transitorios, permanentes y orden incompleta

**Estado:** elegida. La cuarentena y el error handler ya existen; la dead-letter y el backoff con pausa, no.

**Pregunta que debe responder:** ¿cómo se distinguen errores transitorios de permanentes, y cómo se evita a
la vez el descarte silencioso y el bloqueo indefinido del flujo? ¿Qué le pasa a una orden a la que le falta
un ER, y por qué una DLQ por sí sola no la repara?

| Qué pasó | ¿Reintenta? | Dónde queda el ER | Consumidor | La orden |
|---|---|---|---|---|
| Rechazo de dominio: la transición no aplica | No | `execution_quarantine`, visible en el `GET` | Sigue | **Congelada** |
| Falla de contrato: falta un campo obligatorio | No | Dead-letter topic | Sigue | Sin tocar |
| Falla transitoria: base o broker caídos | **Sí**, backoff con el container pausado | En ningún lado: se aplica cuando vuelve | **Pausado** | Sin tocar |
| Transitoria que no cede en el tope de tiempo | — | — | **Frenado** | Sin tocar |

**Qué decidí**

1. Decidí que un error transitorio (por ejemplo: caída de DB) debería reintentarse y pausar el consumer hasta
   poder ser procesada, si llega al tiempo límite establecido se frena el sistema. Asumimos que una falla de
   este alcance ya saltan las alertas al equipo de monitoreo, ya sea el error base de datos al querer
   conectarse o el parámetro `lag` de Kafka que puede monitorearse. En ese momento el equipo de
   infraestructura o de desarrollo deberían resolver la incidencia para restablecerlo.

   No perdemos lo que no se procesó, al pausar la instancia no rebalancea ni queda zombie, entiendo por la
   documentación que queda haciendo polls que devuelven vacío para poder cumplir con lo anterior. Entiendo
   que es un problema del entorno no del procesamiento (código), una vez restablecido se debería intentar
   aplicar el ER correctamente o sino, tenemos las soluciones siguientes.

   Ahora bien, si el error es de contrato y más aún que no cumple lo asumido de que el mensaje llegue con un
   `fixId` o `numericOrderId`, este no lo podemos persistir en la base de datos de cuarentena porque no le
   podemos dar identidad, pero sí podemos crear un Dead-letter para no perderlo ni omitirlo. Esto nos ayudará
   a recrear el ciclo de vida de la orden junto con la base de datos de cuarentena si es que algo no es
   consistente con las aplicadas más cuarentena: este es nuestro tercer lugar donde almacenamos los ER que no
   pudimos aplicar. Tengo en claro que una orden puede quedar con un "hueco" por no poder asociarla, pero un
   hueco que podemos rellenar gracias al Dead-letter.

   Mientras que, si un ER no aplica y cumple con lo asumido, sí va a poder persistirse en la tabla de
   cuarentena. Este es un error que por más que reintentemos X veces no podrá aplicarse y no vale la pena un
   protocolo de reintentos.

   Y la razón de ser de cuarentena más estado `INCOMPLETE` es la evidencia de que una DLQ no sirve para estos
   casos donde tenemos identificada a la orden y su ER no fue aplicado.

   La idea de todo esto es siempre tener el ciclo de vida de una orden completo al momento de alguna falla,
   ya sea de dominio, de contrato o transitoria, cumpliendo la premisa de no perder ningún ER.

2. Una orden que está marcada como `INCOMPLETE` y siguen llegando ER se van a acumular en cuarentena para
   tener al alcance el ciclo de vida con tan sólo un GET, esto nos permite no frenar la partición a esa orden
   `INCOMPLETE` y poder continuar procesando otras órdenes.

3. Queda pendiente un protocolo de recuperación de las órdenes `INCOMPLETE` y los ER que quedan en la
   Dead-letter si lo ameritan.

**Evidencia que la validará:** un ER inválido de la orden A, un ER posterior de A y un ER de B producen
resultados diferenciados y observables; B sigue avanzando.

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
