# ---------- Stage 1: build ----------
FROM eclipse-temurin:25-jdk-jammy AS builder

WORKDIR /build
COPY .. .

RUN chmod +x mvnw
RUN ./mvnw -pl front -am clean package -DskipTests


# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:25-jre-jammy

WORKDIR /app

RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin appuser

COPY --from=builder \
    /build/front/target/*-jar-with-dependencies.jar \
    /app/app.jar

RUN chown appuser:appuser /app/app.jar

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=3s --timeout=1s --retries=1 CMD bash -c 'echo > /dev/tcp/127.0.0.1/8080' || exit 1

ENTRYPOINT ["/bin/bash"]
