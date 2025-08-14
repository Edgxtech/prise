FROM bellsoft/liberica-openjdk-alpine:17

COPY build/libs/webserver-*.jar /app/webserver.jar
CMD ["java", "-Xmx200m", "-jar", "/app/webserver.jar"]

EXPOSE 8092