FROM openjdk:11-jdk-slim

WORKDIR /app
COPY build/libs/indexer-*.jar /app/indexer.jar
ARG PROPERTIES=prise.properties
COPY ${PROPERTIES} /app/prise.properties

CMD ["java", "-jar", "/app/indexer.jar", "-config", "/app/prise.properties"]

EXPOSE 9108