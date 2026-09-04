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
| D-001 | Broker, particionado y secuencia por orden | **Elegida** | Parcial: dos instancias reparten las particiones 2/2 en docker; falta el assert de offsets |
| D-002 | Identidad individual del ER y clave de deduplicación | **Elegida** | Duplicado exacto en docker: no agrega entrada al ledger |
| D-003 | Frontera transaccional, ACK/offset y recuperación | **Elegida** | Parcial: SIGKILL con 36 ER en vuelo, 12/12 correctas; falta la matriz de 4 ventanas |
| D-004 | Modelo persistente de orden, ledger y concurrencia | **Elegida** | Verificada con dos instancias reales: contador = ledger, sin duplicados |
| D-005 | Máquina de estados y terminalidad | **Elegida** | Construida: 30 casos de transición, más el ciclo de vida en docker |
| D-006 | Errores transitorios, permanentes y orden incompleta | **Elegida** | Cuarentena, dead-letter y backoff, con pruebas en docker |
| D-007 | Settlement, outbox y deduplicación downstream | **Elegida** | Barrido, aviso de cambio y contrato del mensaje, con pruebas en docker |
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

**Qué protege.** La condición de *"exactamente una vez"* de la invariante de secuencia: el estado
persistido de una orden corresponde a la aplicación, exactamente una vez y en orden, de un prefijo
de la secuencia de ER que el mercado emitió para ella.

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
  La transacción hace que el ledger y el contador se muevan juntos. Que dos ER de la misma orden
  no se apliquen a la vez nos lo da un consumidor por partición.

**Evidencia que la validará:** el ledger no repite entradas para un mismo ER, el contador coincide con
la cantidad de filas de esa orden, y el ledger se devuelve en orden de inserción. Si dos ER de la misma
orden se aplicaran en paralelo el contador se desalinearía: lo probamos llamando al servicio
directamente y da contador 2 contra 3 filas de ledger. No pasa por el camino real porque D-001 lo
impide, y esa prueba es justamente la que muestra que D-001 no es una comodidad sino un requisito.

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

**Estado:** elegida.

**Pregunta que debe responder:** ¿cómo se distinguen errores transitorios de permanentes, y cómo se evita a
la vez el descarte silencioso y el bloqueo indefinido del flujo? ¿Qué le pasa a una orden a la que le falta
un ER, y por qué una DLQ por sí sola no la repara?

| Qué pasó | ¿Reintenta? | Dónde queda el ER | Consumidor | La orden |
|---|---|---|---|---|
| Rechazo de dominio: la transición no aplica | No | `execution_quarantine`, visible en el `GET` | Sigue | **Congelada** |
| Falla de contrato: falta un campo obligatorio | No | Dead-letter topic | Sigue | Sin tocar |
| Falla transitoria: base caída | **Sí**, backoff exponencial con el container pausado | En ningún lado: se aplica cuando vuelve | Pausado entre intentos, bloqueado dentro de cada uno | Sin tocar |
| Transitoria que no cede en N intentos | — | — | **Frenada la ingesta de esa instancia** | Sin tocar |

**Qué decidí**

1. Decidí que un error transitorio (por ejemplo: caída de DB) debería reintentarse y pausar el consumer hasta poder ser procesada, si llega al tiempo límite establecido* se frena la ingesta de ER. Asumimos que una falla de este alcance ya saltan las alertas al equipo de monitoreo, ya sea el error base de datos al querer conectarse o el parámetro `lag` de Kafka que puede monitorearse. En ese momento el equipo de infraestructura o de desarollo deberían resolver la incidencia para restablecerlo.
**Al resolver la incidencia se deberá reiniciar la instancia porque se frena la ingesta, para el challenge no se implementó pero podríamos contar con un health indicador para que kubernetes o docker reinicien**
No perdemos lo que no se procesó, al pausar la instancia no rebalancea ni queda zombie, entiendo por la documentación que queda haciendo polls que devuelven vacío para poder cumplir con lo anterior. Entiendo que es un problema del entorno no del procesamiento (código), una vez restablecido se debería intentar aplicar el ER correctamente o sino, tenemos las soluciones siguientes.
* Se cambió el uso del tiempo por la cantidad de reintentos: `maxElapsedTime` to `setMaxAttempts`

**luego de las pruebas, si bien la instancia se ponía en pausa pudimos ver que aún así se rebalanceaba porque cada reintento costaba 30s (default de Hikari), la pausa sirve entre reintentos pero no en el intento**
Log de referencia con max.poll.interval.ms=10s:
- HikariPool-1 - Connection is not available, request timed out after 30009ms
- consumer poll timeout has expired ... sending LeaveGroup
- Paused consumer resumed by Kafka due to rebalance; consumer paused again

**Otro hallazgo en las pruebas es que ExponentialBackOff no acumula tiempo de reloj, acumula la suma de espera mientras el sistema esta "dormido" no el que tarda en trabajar, no mira el reloj nunca**

Hallazgo de forma cronologica:
1. Decidí pausar el consumer y poner un tope de tiempo
2. En la primer prueba la pausa cubre el hueco entre intentos, y no el tiempo dentro de uno como parecía. Con la base caída y max.poll.interval.ms en default cada intento cuesta 30s parados en Hikari
3. Noté que el tope no medía tiempo de reloj, sino la suma total del tiempo "dormido" y no de trabajo. Tope de 5s → 158s reales.
4. Prueba final: 4 intentos, 123 segundos, contra los 300 de max.poll.interval.ms. Y con los defaults no hay expulsión:
**poll timeout expired = 0 en las dos instancias, frenó por decisión propia.**

**Por eso lo cambié a cantidad de intentos.**

Forzando el poll a 10s pude reproducir una expulsión. Y aun expulsado y rebalanceando, no se perdió ni se duplicó nada.
**A pesar de esto, no hubo problemas, el ER fue aplicado, no hubo duplicación y el `applied_executions` correspondía con las ER y el ledger**

**Para el challenge nos alcanza, pero podríamos implementar fail fast para la conexión de Hikari o un circuit breaker que corte sin esperarlo.**

Ahora bien, si el error es de contrato y más aún que no cumple lo asumido de que el mensaje llegue con un `fixId` o `numericOrderId` este no lo podemos persistir en la base de datos de cuarentena porque no le podemos dar identidad, pero sí podemos crear un Dead-letter para no perderlo ni omitirlo, esto nos ayudará a recrear el ciclo de vida de la orden junto con la base de datos de cuarentena si es que algo no es consistente con las aplicadas + cuarentena este es nuestro tercer lugar donde almacenamos los ER corruptos. Tengo en claro que una orden puede quedar con un "hueco" por no poder asociarla, pero un hueco que podemos rellenar gracias al Dead-letter.

`numericOrderId` viaja como entero literal. Un decimal, un string o un número en notación científica no
cumplen el contrato aunque representen el mismo entero, así que van a la Dead-letter como cualquier otra
violación. Lo declaro porque antes se aceptaban por coerción: `992023.9` se truncaba y se aplicaba sobre
la orden 992023, que es una orden sana de otro. Preferimos rechazar de más antes que tocar una orden que
no corresponde.

Mientras que, si un ER no aplica y cumple con lo asumido si va a poder persistirse en la tabla de cuarentena. Este es un error que por mas que reintentemos X veces no podrá aplicarse y no vale la pena un protocolo de reintentos.
Y la razón de ser de cuarentena + estado `INCOMPLETE` es la evidencia que una DLQ no sirve para estos casos donde tenemos identificada a la orden y su ER no es aplicado.
La idea de todo esto es siempre tener el ciclo de vida de una orden completo al momento de alguna falla, ya sea de dominio, de contrato o transitoria, cumpliendo la premisa de no perder ningún ER.

2. Una orden que está marcada como `INCOMPLETE` y siguen llegando ER se van a acumular en cuarentena para tener al alcance el ciclo de vida con tan sólo un GET, esto nos permite no frenar la partición a esa orden `INCOMPLETE` y poder continuar procesando otras ordenes.

3. Queda pendiente un protocolo de recuperación de las ordenes `INCOMPLETE` y los ER que quedan en la Dead-letter si lo ameritan.

**Evidencia que la validará:** un ER que rompe el contrato termina en el dead-letter topic, no se persiste, y
el ER publicado detrás de él se procesa igual: ninguna instancia frena. Un SIGKILL a una instancia con 36 ER
en vuelo deja las 12 órdenes en `FILLED` con exactamente 3 ejecuciones cada una. Una caída de base más
corta que el presupuesto de reintentos se recupera sola; una más larga frena la ingesta y hay que
reiniciar la instancia. En los dos casos no se pierde nada: el offset no se commiteó, así que al volver
el ER pendiente se aplica. En todos los escenarios `applied_executions` coincide con las filas del ledger
de esa orden y no hay `fixId` repetido dentro de una misma orden.


**Alcance:** el enunciado no exige implementar el mecanismo completo, sólo que nada se descarte en silencio.
Lo que quede sin implementar se documenta acá.

---

## D-007 - Settlement, outbox y deduplicación downstream

**Estado:** elegida e implementada.

**Pregunta que debe responder:** ¿cómo se garantiza que una orden `FILLED` produzca exactamente un efecto
lógico de settlement downstream, sin pérdida ni duplicados, aun con reentregas y dos instancias? ¿Por qué la
publicación física al broker no es exactly-once por sí sola, y dónde vive realmente la garantía?

| Qué pasó | Downstream recibe |
|---|---|
| Completa, y el barrido la encuentra `FILLED` | **settlement** |
| Completa, se publica, después llega un ER tardío | **settlement y aviso**, en ese orden |
| Completa, y un ER tardío la ensucia **antes** de que el barrido pase | **nada** |
| Completa, se cae el servicio antes de publicar, nada más llega | settlement, en el barrido siguiente |
| Termina `CANCELLED` | nada |
| Queda `INCOMPLETE` sin haber completado nunca | nada |

**Por qué**

1. Como primer instancia evalué que la tabla Order era un outbox perfecto para poder barrer y enviar el mensaje o que downstream trabaje sobre la tabla, pero me di cuenta que el estado podía cambiar y eso lo dejo sin efecto inmediato, no eran datos inmutables, podría sufrir cambios el `status`. A simple vista puede parecer que seguimos utilizando de outbox porque recorremos la misma tabla para dar el aviso, lo que cambió es que la recorremos y avisamos lo que en ese momento es el hecho, y volveríamos a barrer en caso de que haya cambiado, ahi nos cubrimos de la mutabilidad del campo `status`
2. Después de eso decidí enviar el evento en el momento exacto en que la orden se marcaba `FILLED` porque representaba el hecho, pero me di cuenta que podría haber perdidas y necesitaba el barrido, asi que finalmente me quedé sólo con el barrido.

**Qué decidí**

1. Si bien el enunciado decía explicitamente "cuando una orden se completa ( status = FILLED ), el servicio debe publicar un mensaje de liquidación ( settlement ) hacia un destino downstream (otra cola/topic)." no lo hacemos en el momento porque podemos sufrir la caída entre que terminamos de procesar la `FILLED` y publicamos, para poder prevenir eso deberíamos hacer el barrido de las `FILLED` + `settlement_published_at` == null de todas maneras. Para ahorrarnos eso y que el proceso de una orden quede independiente de la publicación, sólo implementamos el barrido.
2. El barrido va a hacer una lectura del momento y enviar lo que ve, publicamos un hecho real.
3. Como medida preventiva de que una orden haya cambiado su estado a `INCOMPLETE` luego de dar el aviso de que su estado fue FILLED vamos a incorporar un segundo barrido y un campo `marked_incomplete_notified_at`, donde vamos a chequear si el estado de la orden es `INCOMPLETE` y su campo `settlement_published_at` != null para informar que su estado cambió a `INCOMPLETE` al downstream y accione frente a eso.
4. En caso de caída entre que estuvo `FILLED` y luego se marcó como `INCOMPLETE` no se va a enviar nada al downstream, se evaluó envíar `FILLED` y luego `INCOMPLETE` al downstream, estariamos haciendo la reversa de algo que ya sabemos que no es así y como dije en el punto 2 publicamos el hecho que vemos en el momento del barrido.
5. El payload del mensaje al downstream sólo contendra el `numericOrderId` y un `type` que reflejará el hecho `FILLED` (`ORDER_SETTLED`) o `INCOMPLETE`(`ORDER_MARKED_INCOMPLETE`), puede obtener los datos necesarios desde el GET y si algo llegara a cambiar tenemos el aviso para que la política de cambio actue.
6. El mensaje al downstream va siempre al mismo tópico y misma key (`numericOrderId`), garantizando el orden de llegada por parte del broker.
7. Tópico distinto al del ciclo de vida de la orden.
8. Este nuevo tópico no se compacta porque el `numericOrderId` sería el mismo que en el aviso de `FILLED` y se ignora en el aviso de `INCOMPLETE` porque conserva el último valor por clave, nos paso en una prueba que al compactar por la key asumía que era el mismo mensaje y lo ignoraba, hablando a nivel logs almacenados y en el caso de querer reconstruir los hechos.
   Un consumidor hubiese recibido ambos eventos, la compactación no impacta los envíos.
   evidencia en logs:

   ```
   ANTES de compactar
     981001    {"type":"ORDER_SETTLED","numericOrderId":981001}
     981001    {"type":"ORDER_MARKED_INCOMPLETE","numericOrderId":981001}

   DESPUÉS de compactar
     981001    {"type":"ORDER_MARKED_INCOMPLETE","numericOrderId":981001}
   ```

9. Poner `FOR UPDATE SKIP LOCKED` así aunque haya N instancias haciendo el barrido hasta no terminar de procesarla no libera la fila y sólo una instancia procesaría ese envío al downstream
10. Intervalo de 1s, nos da más chance de envíar un aviso de `INCOMPLETE` al downstream, ahora bien si aumentamos este intervalo tendríamos más chances de haber sufrido un cambio en el status y directamente impacta en no informar `FILLED` gracias a que no veamos ese momento porque ya mutó a `INCOMPLETE`. Pero como no sabemos en cuanto tiempo nos va a llegar un ER tardío que marque el estado `INCOMPLETE`, prioricé la velocidad de enviar un evento de `FILLED` y asumí tener que enviar mas cambios de estado.
11. En las pruebas pudimos comprobar que cuando pasa el escenario 3 que no se termina enviando al downstream, cuando consultamos la orden pudimos ver su estado `INCOMPLETE` y el campo `order_status_at_rejection` nos marcaba que estuvo en `FILLED` antes, nos dió una visibilidad inmediata de por qué no llego al downstream por mas que veamos que hay un ER en `FILLED`

**Qué asumimos**

1. Deduplicado por parte del downstream con `numericOrderId` + `type` como identificador.
2. El downstream no verá status, sólo accionará ante el hecho que reciba a través del `type` en el mensaje

**Hallazgos post pruebas + correcciones**

1. El hilo de kafka y el hilo del barrido (independiente de kafka, un hilo generado por @Scheduler) podrían a llegar a escribir la misma fila y generar errores de concurrencia dejando campos en null:

   ```
   t=0.000  hilo del consumidor: llega el ER tardío de la orden X
                                 lee la fila → status=FILLED, marca=null

   t=0.001  hilo del barrido:    toma la orden X, publica el settlement,
                                 escribe la marca
   t=0.050  hilo del barrido:    COMMIT

   t=0.051  hilo del consumidor: hace su UPDATE → escribe TODAS las columnas,
                                 incluida marca=null, que es lo que leyó en t=0
   ```

   Lo corregimos sumando `@DynamicUpdate`, esto hace que cada update sólo escriba las columnas que sufren cambios y como cada hilo es dueño de cada columna ya no ocurría. El hilo del consumidor sólo escribe `status`, `applied_executions` y los tres montos, mientras que el del barrido las marcas de las publicaciones.

2. El barrido tomaba de a lotes, por ende, hasta no terminar de procesar no liberaba el lote y si fallaba una volvía atrás todas y se republicaban. Se modificó para poder hacer una transacción por fila y dar el commit a la base de datos para liberar al publicar esa fila, el lock dura lo que tarda una publicación y el bloqueo por lote deja de importar. La consecuencia fue 3 idas y vueltas a la base de datos por publicación. Manteniendo el consumo en el lote de 100 de todas maneras, pero liberamos la db por cada fila.

   Alternativas evaluadas y descartadas:
   - Si bajabamos el lote a 1, era aproximadamente una fila por segundo y con 500 ordenes ibamos a tardarnos 8 minutos, lo considere no viable.
   - Marcarla en la base de datos primero y luego publicar el settlement, pero consideré una caída entre el update en la db y la publicación, donde nos perderíamos de publicar el settlement.
   - Publicar el settlement y luego marcar en la base de datos, nunca frenaba la ingesta pero si dos intancias barrían cada 1s el duplicado pasaba a ser algo habitual, también lo descarte, debe ser una protección no algo habitual del downstream el deduplicador.

3. `publish-timeout` no es suficiente para fijar un timeout en las publicaciones porque cuando se hace un .send() lo bloquea y puede tardar mas:
   - Medido: con `publish-timeout` en 100ms, una publicación a un topic inexistente tardó **2146ms**.

   Para corregir eso sumamos `max.block.ms` que el peor escenario ahora es la suma de las dos propiedades, pero controlado.

4. `FOR UPDATE SKIP LOCKED` al final no era lo que buscabamos sino lo que Hibernate 6 nos dió para esto es `FOR NO KEY UPDATE SKIP LOCKED` cuando aplicamos @Lock(PESSIMISTIC_WRITE). Este no es lock mas fuerte, sino que lo que nos ofrece es bloquear la fila hasta terminar para que esa fila no sea levantada por otro barrido y nos evita la concurrencia, y permite que el consumidor escriba el ledger cuando tomamos la FK de la orden para sumar al ledger.

5. Hallazgo de último momento, los schedulers de los barridos se colgaban del creado para el backoff que pausaba las instancias y no respetaba los numeros de max.poll.interval.ms y además teníamos un pool para 3 schedulers (pausa, barrido FILLED y barrido INCOMPLETE), agregando un pool específico para los barridos tenemos uno sin posibilidad de encolarse para la pausa y otro pool de 2 hilos (configurado por property) para los barridos

   El pausado es del container, no de la instancia: el servicio sigue vivo y el `GET` sigue
   respondiendo con normalidad, lo único que se detiene es el consumo de ER.

   Medido con la base caída, antes y después de separar los pools:

   ```
   esperas entre reintentos   antes  60s / 90s / 90s      configuradas  0,5s / 1s / 2s
   peor caso hasta frenar     antes  271s                 validado al arrancar  123,5s
                              ahora  125s
   expulsiones por poll timeout   0 en las dos instancias  (max.poll.interval.ms = 300s)
   ```

   Y la prueba de que los pools quedaron separados es el hilo que aparece en el log:

   ```
   antes   [app-2] [er-backoff-1]  settlement published numericOrderId=660001
   ahora   [app-2] [settlement-1]  settlement published numericOrderId=660001
   ```

**Evidencia**

| Qué se afirma | Cómo se probó | Resultado |
|---|---|---|
| Las seis filas de la tabla | `SettlementTest`, ocho casos | Verdes. Sacando el filtro por `FILLED` del barrido, **3 en rojo**: settlean la cancelada, la suprimida y la que nunca completó. Sacando la exigencia de publicación previa en el aviso, **2 en rojo** |
| Los dos mensajes van keyeados por `numericOrderId` | Test que lee el topic y compara la key | Los dos con la key de la orden. Con una key constante, **rojo** |
| Los barridos corren solos | Test sobre las tareas agendadas | Los dos registrados. Sacando los `@Scheduled`, **rojo** |
| La carrera del hallazgo 1 | Test determinista de dos hilos con latches | Verde. Sin `@DynamicUpdate`, falla con *"Expecting actual not to be null"* |
| Un settlement por orden con dos instancias | `docker compose` con app-1 y app-2 barriendo cada segundo | Un solo `ORDER_SETTLED`, publicado por **una sola** instancia, la otra en cero |
| El orden entre settlement y aviso | Mismo escenario, con un ER tardío después | `ORDER_SETTLED` y después `ORDER_MARKED_INCOMPLETE` |
| La compactación pisa el settlement | Topic con `cleanup.policy=compact` y segmento forzado a rotar | Sobrevive sólo el aviso. Evidencia en el punto 8 |
| La suite completa | `./mvnw clean verify` en orden normal, inverso y aleatorio | 96 tests, sin dependencias de orden |

Dos cosas que **no** están cubiertas por tests y conviene decirlo: **el barrido automático de 1s** —los
tests lo desactivan y lo disparan a mano, así que lo que lo prueba es la corrida en docker— y **la fila 3
de la tabla**, el settlement suprimido, que está cubierta por test pero no forzada en la demo, porque
hacer llegar el ER tardío dentro del segundo del barrido a mano es una carrera.

**Alcance:** el intervalo del barrido es la latencia del settlement y también la ventana en la que un ER
tardío puede suprimirlo. El aviso no anula el settlement: informa que la orden cambió. No hay protocolo de
recuperación para una orden que quedó `INCOMPLETE` después de settlear.

---

## D-008 - Alcance declarado y trabajo fuera de alcance

**Pregunta que debe responder:** ¿qué se dejó deliberadamente afuera, por qué, y cómo se resolvería en una
versión completa?

**Estado:** abierta. Se completa al final, desde lo efectivamente construido.
