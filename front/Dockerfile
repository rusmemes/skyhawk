# ---------- Stage 1: build ----------
FROM openjdk:25-ea-jdk-slim-bookworm AS builder

WORKDIR /build
COPY .. .

RUN chmod +x mvnw
RUN ./mvnw -pl front -am clean package -DskipTests


# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/front/target/*-jar-with-dependencies.jar app.jar

EXPOSE 8080
HEALTHCHECK --interval=3s --timeout=1s --retries=1 CMD bash -c 'echo > /dev/tcp/127.0.0.1/8080' || exit 1

RUN useradd -m appuser
USER appuser

ENTRYPOINT ["/bin/bash"]
