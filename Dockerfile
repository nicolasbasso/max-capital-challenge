# Etapa de build: la imagen trae su propio JDK 21, asi que el evaluador no instala nada.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
# Los tests se corren con ./mvnw verify fuera de la imagen: necesitan Docker para
# Testcontainers y no pueden correr dentro del build.
RUN mvn -B -q clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
