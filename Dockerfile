# ---- Build stage: produce the shaded jar ----
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Resolve dependencies first so they stay cached when only sources change.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests \
    && cp target/everestmq-*-jar-with-dependencies.jar /build/everestmq.jar

# ---- Runtime stage: broker, producer and consumer all run from this image ----
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /build/everestmq.jar /app/everestmq.jar

# Message logs and client offsets live here; mounted as a volume in compose.
RUN mkdir -p /data
ENV EVERESTMQ_DATA_DIR=/data

EXPOSE 9876

# The broker is the default; producer and consumer override the command.
CMD ["java", "-cp", "/app/everestmq.jar", "com.everestmq.broker.server.EverestBrokerServer"]
