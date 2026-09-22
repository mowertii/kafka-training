FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

# 👇 Используем ОТДЕЛЬНЫЙ pom для HW7 (только common + hw7)
COPY pom-docker-hw7.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY homework-7-rebalancing/pom.xml /build/homework-7-rebalancing/pom.xml

RUN mvn -q -DskipTests -pl common,homework-7-rebalancing dependency:go-offline

COPY common/src /build/common/src
COPY homework-7-rebalancing/src /build/homework-7-rebalancing/src

RUN mvn -q -DskipTests -pl common,homework-7-rebalancing install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/homework-7-rebalancing/target/hw7-rebalancing.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
