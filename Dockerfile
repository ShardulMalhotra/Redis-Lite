# --- Build stage ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

# --- Runtime stage ---
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/redis-lite.jar app.jar

# AOF file persists here — mount a volume to this path to keep data across container restarts
VOLUME /app/data
WORKDIR /app/data

EXPOSE 6380
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
