FROM bellsoft/liberica-openjdk-alpine:11

COPY build/libs/indexer-0.0.1.jar /app/
ARG PROPERTIES
COPY ${PROPERTIES} /app/prise.properties

CMD ["java", "-Xmx200m", "-jar", "/app/indexer-0.0.1.jar", "-config", "/app/prise.properties"]

EXPOSE 9108