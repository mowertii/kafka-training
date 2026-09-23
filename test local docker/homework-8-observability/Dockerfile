FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom-docker-hw8.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY homework-8-observability/pom.xml /build/homework-8-observability/pom.xml

RUN mvn -q -DskipTests -pl common,homework-8-observability dependency:go-offline

COPY common/src /build/common/src
COPY homework-8-observability/src /build/homework-8-observability/src

RUN mvn -q -DskipTests -pl common,homework-8-observability install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/homework-8-observability/target/hw8-observability.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
