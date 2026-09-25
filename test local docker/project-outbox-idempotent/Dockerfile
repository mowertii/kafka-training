FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom-docker-project.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY project-outbox-idempotent/pom.xml /build/project-outbox-idempotent/pom.xml

RUN mvn -q -DskipTests -pl common,project-outbox-idempotent dependency:go-offline

COPY common/src /build/common/src
COPY project-outbox-idempotent/src /build/project-outbox-idempotent/src

RUN mvn -q -DskipTests -pl common,project-outbox-idempotent install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/project-outbox-idempotent/target/project-outbox-idempotent.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
