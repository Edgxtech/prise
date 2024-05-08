<div style="text-align: center";>

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Edgxtech/prise/blob/master/LICENSE)
</div>

# Prise - Cardano Native Token Price Webserver
Companion Web Application for the Prise indexer. Spring application providing api endpoints for latest and historical prices

## Build

    cd webserver
    mvn clean install -DskipTests

## Run

    mvn spring-boot:run

## Configs
####    application.properties - app configuration
This is the standard spring application.properties file.
Override properties per environment, e.g. application-default.properties & application-production.properties
And set the SPRING_PROFILE_ACTIVE environment variable accordingly; e.g. export SPRING_PROFILES_ACTIVE=production

    server.contextPath=
    server.port=
    cors.app.address=
    # SQL Database
    spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
    spring.datasource.url=jdbc:mysql://localhost:3306/prise
    spring.datasource.username=username
    spring.datasource.password=password

     *see application.properties for more detailed descriptions

## Java Version

    Tested with Java 20

## Mysql Installation

* See parent (Indexer) Sql Database installation instructions 

## Support
This project is made possible by Delegators to the [AUSST](https://ausstaker.com.au) Cardano Stakepool and
supporters of [Edgx](https://edgx.tech) R&D