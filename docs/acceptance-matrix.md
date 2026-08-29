# Matriz de aceptación

Traducción del enunciado a compromisos verificables. La columna **Requisito** transcribe lo que pide el
enunciado; las tres columnas siguientes son el trabajo de diseño.

- **Invariante:** la propiedad que debe ser verdadera siempre, escrita sin nombrar ninguna tecnología.
  Si para enunciarla hace falta decir "Kafka" o "PostgreSQL", todavía no es una invariante: es una solución.
- **Escenario de aceptación:** la situación concreta que la pone a prueba, definida *antes* de implementar.
- **Evidencia:** el test o comando reproducible que la demuestra.

Una fila sin las tres columnas completas es una garantía que todavía no sabemos verificar.

### La invariante ancla: prefijo

R3 sostiene a varias de las demás, así que conviene enunciarla aparte.

El mercado emite, para cada orden, una secuencia de ER: `ER1 → ER2 → ... → ERn`. En cualquier instante el
servicio aplicó sólo una parte de esa secuencia, porque los ER siguen llegando. La pregunta es qué partes son
estados legales.

- `{ER1, ER2}` es legal.
- `{ER1, ER3}` no lo es: falta uno en el medio.
- `{ER2, ER3}` tampoco: no arranca por el `NEW`.

Lo que tienen en común los estados legales es que son **prefijos** de la secuencia emitida: sin huecos y desde
el principio. De ahí sale la invariante, y su valor está en que **no menciona ninguna tecnología** y puede
verificarse congelando el sistema en cualquier momento, sin conocer el historial de cómo se llegó ahí.

Una sola propiedad cubre tres fallas distintas:

| Falla | Qué condición del prefijo rompe |
|---|---|
| Se perdió un ER intermedio | "sin huecos" |
| Se aplicó un ER antes de tiempo | "desde el principio" |
| Se aplicó dos veces el mismo ER | "exactamente una vez" |

| # | Requisito (del enunciado) | Invariante | Escenario de aceptación | Evidencia |
|---|---|---|---|---|
| R1 | Ingesta asíncrona de ER a través de un message broker. | | | |
| R2 | Dos instancias del servicio corriendo en paralelo vía `docker compose`, ambas consumiendo. | | | |
| R3 | Los ER de una misma orden se aplican en la secuencia en que fueron emitidos (no se puede aplicar un `FILLED` antes del `NEW` de esa orden). | **En todo momento, el estado persistido de una orden corresponde a la aplicación, exactamente una vez y en orden, de un prefijo de la secuencia de ER que el mercado emitió para esa orden.** | *(propuesto)* Se emiten los ER de dos órdenes intercalados entre sí, incluyendo una reentrega de un ER intermedio de la primera. Al terminar, el estado y el ledger de cada orden corresponden a un prefijo de su propia secuencia. | *(propuesto)* Test que reconstruye la secuencia aplicada desde el ledger y verifica que sea un prefijo de la secuencia emitida, para ambas órdenes. |
| R4 | Entre órdenes distintas no importa el orden relativo: pueden procesarse en paralelo. | | | |
| R5 | Un ER duplicado o reentregado no corrompe el estado. La identidad del duplicado es la del ER individual, no la de la orden. | | | |
| R6 | Entidad orden con estado mutable, persistente y consultable. | | | |
| R7 | Ledger de ejecuciones: una entrada por ER **efectivamente aplicado**, con clave autoincremental que refleja el orden de inserción. Un duplicado detectado no agrega entrada. | | | |
| R8 | El estado nuevo se **computa** a partir del estado ya persistido más el ER entrante. No es sobreescribir con el último ER. | | | |
| R9 | El `status` actual se evalúa contra el `status` ya guardado: un ER no puede aplicarse sobre una orden ya terminal (`FILLED` / `CANCELLED`). | | | |
| R10 | La cantidad de ejecuciones aplicadas se incrementa sobre el valor anterior, y refleja exactamente la secuencia de ER que se aplicó. | | | |
| R11 | Endpoint HTTP para consultar una orden por `numericOrderId`, devolviendo `status`, cantidad de ejecuciones aplicadas y el ledger en orden de inserción. | | | |
| R12 | Si una instancia se cae a mitad de procesamiento: no se pierden ni se aplican dos veces los ER; al reiniciar, retoma correctamente. | | | |
| R13 | Un ER que falla al procesarse no puede provocar pérdida silenciosa (una orden a la que le falta un ER sin que nadie se entere) ni bloqueo indefinido del flujo de esa orden. | | | |
| R14 | Se distinguen errores transitorios (reintentables) de permanentes (mensaje inválido / envenenado), y está contemplado qué pasa con una orden que queda incompleta. | | | |
| R15 | Al completarse una orden (`status = FILLED`) se publica un mensaje de `settlement` a un destino downstream, de modo que downstream lo reciba **exactamente una vez por orden**, aun con reentregas o con las dos instancias viendo el completado. | | | |
| R16 | Las órdenes que terminan en `CANCELLED` **no** emiten settlement. | | | |
| R17 | `transactionTime` es reloj de pared del origen: no es confiable como criterio de secuencia. | | | |
| R18 | Los campos de cantidad son acumulados (snapshots), no deltas. | | | |

## Requisitos de entrega (no funcionales, pero evaluados)

| # | Requisito | Estado |
|---|---|---|
| E1 | `docker compose up` levanta todo: 2 instancias + broker + persistencia. | |
| E2 | Forma de emitir un stream de ER de prueba, con órdenes intercaladas y algún ER duplicado. | |
| E3 | Nota en el README de cómo ejercitar los escenarios clave: órdenes intercaladas, duplicados, caída de una instancia. | |
| E4 | `DECISIONS.md` cubriendo broker, secuencia con 2 consumidores, idempotencia y clave de dedup, motor de persistencia, exactitud del estado, política de errores, unicidad del settlement y trade-offs. | |
| E5 | Tests focalizados sobre lo crítico, no cobertura amplia de código trivial. | |
| E6 | Java 21 o 25 + Spring Boot. | |
| E7 | Historial de commits conservado, repositorio git público. | |

## Fuera de alcance por indicación explícita del enunciado

- Auth, UI real, features extra, hardening de producción.
- El consumidor del `settlement`: alcanza con publicarlo.
- Implementar el mecanismo de errores completo (basta con que nada se descarte en silencio).
