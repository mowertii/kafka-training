FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml /build/pom.xml
COPY common/pom.xml /build/common/pom.xml
COPY homework-9-k8s/pom.xml /build/homework-9-k8s/pom.xml
RUN mvn -q -DskipTests -pl common,homework-9-k8s dependency:go-offline
COPY common/src /build/common/src
COPY homework-9-k8s/src /build/homework-9-k8s/src
RUN mvn -q -DskipTests -pl common,homework-9-k8s install

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/homework-9-k8s/target/hw9-k8s.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]