# syntax=docker/dockerfile:1.7
# Imagen de prisma-nominations-api: build con Maven (JDK 21) y runtime JRE 21 Alpine con el jar extraído por capas.
#   docker build -t prisma-nominations-api ./app      (o: docker compose up -d --build desde la raíz)
# Los tests no se corren acá (requieren Docker/Testcontainers): se corren con `mvn verify` en CI.

# ---------- build ----------
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace

# Dependencias primero: esta capa solo se invalida si cambia el pom. El cache mount conserva ~/.m2 entre builds.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package \
    && cp target/nominations-api-*.jar target/app.jar

# Extrae el jar en capas (dependencies / spring-boot-loader / snapshot-dependencies / application): un cambio de
# código solo reescribe la capa "application" (~cientos de KB) y no las dependencias.
RUN java -Djarmode=tools -jar target/app.jar extract --layers --destination /workspace/extracted

# ---------- runtime ----------
FROM eclipse-temurin:21.0.8_9-jre-alpine
WORKDIR /app

RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app -H -s /sbin/nologin app

# De la capa que menos cambia a la que más cambia.
COPY --from=build --chown=app:app /workspace/extracted/dependencies/ ./
COPY --from=build --chown=app:app /workspace/extracted/spring-boot-loader/ ./
COPY --from=build --chown=app:app /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build --chown=app:app /workspace/extracted/application/ ./

USER app:app

# El heap se dimensiona por el límite de memoria del contenedor (cgroup), no por la RAM del host.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

# /actuator/health es público (sin token devuelve solo el estado). wget viene en BusyBox (Alpine).
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=5 \
  CMD wget -q -O /dev/null http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
