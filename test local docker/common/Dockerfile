# ============================================================
# Common library — сборка
# ============================================================
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

# Копируем parent POM и common POM
COPY pom.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml

# Копируем исходники common
COPY common/src /build/common/src

# Собираем common
RUN mvn -q -DskipTests -pl common install

FROM eclipse-temurin:21-jre
WORKDIR /app
# Библиотека не запускается, только собирается
CMD ["echo", "common library built"]
