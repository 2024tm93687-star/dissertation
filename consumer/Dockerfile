FROM eclipse-temurin:21-jre

WORKDIR /app
COPY target/telecom-consumer-0.1.0-SNAPSHOT.jar /app/telecom-consumer.jar

ENTRYPOINT ["java", "-jar", "/app/telecom-consumer.jar"]
