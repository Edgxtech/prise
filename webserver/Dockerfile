FROM maven:alpine as build
ENV HOME=/usr/app
RUN mkdir -p $HOME
WORKDIR $HOME
ADD . $HOME
RUN --mount=type=cache,target=/root/.m2 mvn -f $HOME/pom.xml clean package -DskipTests

FROM openjdk:11
COPY --from=build /usr/app/target/webserver-0.1.jar /app/runner.jar
EXPOSE 8092
ENTRYPOINT ["java","-Dspring.profiles.active=docker-default", "-jar","/app/runner.jar"]