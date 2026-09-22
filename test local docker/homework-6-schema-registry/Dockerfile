# ============================================================
# HW6 Monitoring — сборка и запуск
# ============================================================
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

# 1. Копируем УПРОЩЁННЫЙ parent POM (без отсутствующих модулей)
COPY pom-docker.xml /build/pom.xml

# 2. Копируем POM'ы модулей
COPY common/pom.xml /build/common/pom.xml
COPY homework-6-schema-registry/pom.xml /build/homework-6-schema-registry/pom.xml

# 3. Скачиваем зависимости (кэшируется)
RUN mvn -q -DskipTests -pl common,homework-6-schema-registry dependency:go-offline

# 4. Копируем исходники
COPY common/src /build/common/src
COPY homework-6-schema-registry/src /build/homework-6-schema-registry/src

# 5. Собираем
RUN mvn -q -DskipTests -pl common,homework-6-schema-registry install

# ============================================================
# Runtime
# ============================================================
FROM eclipse-temurin:21-jre
WORKDIR /app

# Копируем собранный JAR
COPY --from=build /build/homework-6-schema-registry/target/hw6-schema-registry.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]