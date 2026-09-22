FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY homework-8-streams/pom.xml /build/homework-8-streams/pom.xml
RUN mvn -q -DskipTests -pl common,homework-8-streams dependency:go-offline
COPY common/src /build/common/src
COPY homework-8-streams/src /build/homework-8-streams/src
RUN mvn -q -DskipTests -pl common,homework-8-streams install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/homework-8-streams/target/hw8-streams.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]