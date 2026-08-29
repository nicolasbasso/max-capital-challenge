# Order State Service - Max Capital Challenge

Servicio que consume `ExecutionReport` de forma asíncrona y mantiene el estado de cada orden
correcto bajo carga, reentregas y fallas, corriendo en dos instancias en paralelo.

El razonamiento detrás del diseño está en [`DECISIONS.md`](./DECISIONS.md).
La traducción del enunciado a compromisos verificables está en
[`docs/acceptance-matrix.md`](./docs/acceptance-matrix.md).

## Estado actual

> **Slice 0 - andamiaje.** Todavía no hay ingesta, persistencia ni endpoint de consulta.
> Lo que existe hoy es un servicio que arranca y un build verificable. Las decisiones de
> arquitectura (broker, persistencia, idempotencia, settlement) están **abiertas** y
> documentadas como preguntas en `DECISIONS.md`, no como conclusiones.

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

### Variables de entorno

| Variable | Default | Para qué |
|---|---|---|
| `SERVER_PORT` | `8080` | Puerto HTTP. |
| `APP_INSTANCE_ID` | `local` | Identifica la instancia en cada línea de log. Permite correlacionar qué orden procesó cuál de las dos instancias cuando haya que demostrarlo. |

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
