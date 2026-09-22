FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY homework-7-connect/pom.xml /build/homework-7-connect/pom.xml
RUN mvn -q -DskipTests -pl common,homework-7-connect dependency:go-offline
COPY common/src /build/common/src
COPY homework-7-connect/src /build/homework-7-connect/src
RUN mvn -q -DskipTests -pl common,homework-7-connect install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/homework-7-connect/target/hw7-connect.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]