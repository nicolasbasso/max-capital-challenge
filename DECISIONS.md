# DECISIONS

Este documento justifica el diseño. Cada decisión se cierra sólo cuando existe evidencia reproducible que la
respalde. Mientras una decisión esté abierta, la sección conserva la pregunta que debe responder y la
evidencia que la validará, en vez de una conclusión prematura.

**Regla de honestidad del documento:** no se afirma una garantía mayor que la demostrada. Si algo quedó fuera
de alcance, se declara acá en vez de simularlo.

## Estado de las decisiones

| ID | Decisión | Estado | Evidencia |
|---|---|---|---|
| D-001 | Broker, particionado y secuencia por orden | **Aceptada** | Pendiente: assert de offsets en T4.x / T5.3 |
| D-002 | Identidad individual del ER y clave de deduplicación | Abierta | — |
| D-003 | Frontera transaccional, ACK/offset y recuperación | Abierta | — |
| D-004 | Modelo persistente de orden, ledger y concurrencia | Abierta | — |
| D-005 | Máquina de estados y terminalidad | Abierta | — |
| D-006 | Errores transitorios, permanentes y orden incompleta | Abierta | — |
| D-007 | Settlement, outbox y deduplicación downstream | Abierta | — |
| D-008 | Alcance declarado y trabajo fuera de alcance | Abierta | — |

---

## D-001 - Broker, particionado y secuencia por orden

**Estado:** aceptada.

**Requisito.** Los ER de una misma orden deben aplicarse en su secuencia de emisión; órdenes distintas deben
avanzar en paralelo; dos instancias consumen el mismo flujo y debe existir failover.

**Decisión.** Apache Kafka: un topic particionado, `numericOrderId` como message key, ambas instancias en el
mismo consumer group.

**Alternativa considerada.** RabbitMQ con exchange `x-modulus-hash` (built-in, no requiere plugin) y Single
Active Consumer por cola. **Cubre las cuatro garantías exactamente igual que Kafka.** La elección no se apoya
en capacidad. RabbitMQ Super Streams se descartó sin evaluación profunda: introduce el protocolo Stream y
manejo de offsets propio sin resolver nada que las otras dos alternativas no resuelvan.

**Mecanismo.**

- El productor usa `numericOrderId` como key; el particionador por defecto la hashea, de modo que todos los ER
  de una misma orden caen en la misma partición.
- Kafka preserva el orden dentro de una partición y asigna cada partición a un solo consumidor del grupo, con
  failover automático.
- `enable.auto.commit=false`. El offset se commitea **después** del commit de la transacción en PostgreSQL.

**Qué cubre el broker.** El mensaje no se pierde y se reentrega; el orden por orden se preserva; una sola
instancia consume una orden a la vez; un consumidor con generación caducada tras un rebalance queda *fenced* y
su commit de offset es rechazado.

**Qué NO cubre el broker.** Kafka no tiene visibilidad sobre PostgreSQL. Un consumidor zombie puede haber
commiteado en la base antes de ser expulsado, y ese registro queda escrito. La aplicación única de un ER y la
alineación entre contador y ledger no las provee el broker: son barreras durables en la base (D-002, D-003,
D-004).

**Por qué Kafka y no RabbitMQ.**

1. *(Primaria)* La posición del consumidor es un offset consultable, independiente de los mensajes. La
   reentrega tras una caída se demuestra comparando el offset commiteado antes y después, y es asserteable
   desde un test. En RabbitMQ la evidencia es el flag `redelivered` dentro del consumidor: alcanza para
   probarlo, pero no es un valor inspeccionable ni automatizable del mismo modo.
2. *(Secundaria)* Consumir no borra: el log permite reconstruir la historia de una orden. RabbitMQ elimina el
   mensaje al ackearlo.

**Lo que se resigna frente a RabbitMQ.** Son costos diferenciales, y son acotados:

- **Dead-letter:** RabbitMQ la expone como configuración de la cola (`x-dead-letter-exchange`); en Kafka hay
  que construirla.
- **Topología de despliegue:** el `docker-compose` de Kafka es más pesado (variables de KRaft) que el de
  RabbitMQ.

**Límites de la solución, independientes del broker elegido.** No son resignaciones frente a RabbitMQ: se
tendrían con cualquiera de las dos alternativas bajo el requisito de orden por clave.

- **Mensaje venenoso:** en Kafka la unidad de orden y la unidad de avance son la misma (la partición), porque
  el offset es un cursor único. Un ER permanentemente inválido frena a las órdenes co-particionadas mientras
  dura el reintento. RabbitMQ permitiría desacoplarlas gracias al ack por mensaje, pero sólo agregando
  concurrencia con serialización propia por clave —es decir, reconstruyendo a mano lo que la partición da— o
  con reintento diferido, que devuelve el ER más tarde y rompe la secuencia de la propia orden. Bajo la
  restricción de orden por clave, ninguna de las dos alternativas lo evita en la práctica. Se declara el radio
  de daño: la partición, no la orden. La mitigación se define en D-006, donde el registro en la dead-letter
  debe quedar durable **antes** de commitear el offset, para que el desbloqueo no se convierta en una pérdida
  silenciosa.
- **Paralelismo con techo:** limitado por la cantidad de particiones (o de colas en RabbitMQ), que se mantiene
  fija durante el ejercicio; cambiarla remapea keys en ambos casos.
- **Retención:** Kafka no borra al consumir, pero sí borra por política de retención. El histórico dura lo que
  se configure; no es un archivo permanente salvo que se lo defina como tal.

**Evidencia que la validará.** Dos instancias reales consumiendo órdenes intercaladas, conservando el orden
interno de cada una; y una caída entre el commit de PostgreSQL y el commit del offset, comparando el offset
commiteado antes y después, con una sola entrada de ledger resultante.

---

## D-002 - Identidad individual del ER y clave de deduplicación

**Pregunta que debe responder:** ¿qué campo o combinación identifica un ER individual, dado que
`numericOrderId` identifica la orden y se repite en todos sus ER? ¿Qué ocurre si ese campo viene ausente,
vacío o repetido entre órdenes distintas?

**Evidencia que la validará:** el mismo ER entregado dos veces produce una sola aplicación efectiva: una sola
entrada de ledger y un contador que no se incrementa dos veces.

**Estado:** abierta.

---

## D-003 - Frontera transaccional, ACK/offset y recuperación

**Pregunta que debe responder:** ¿cuál es el resultado previsto ante una caída en cada ventana — antes de la
transacción, dentro de la transacción, después del commit y antes del ACK/commit de offset? ¿Por qué ninguna
de esas ventanas pierde ni duplica un ER?

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
