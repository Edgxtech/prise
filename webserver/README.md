<div style="text-align: center";>

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Edgxtech/prise/blob/master/LICENSE)
</div>

# Prise - Cardano Native Token Price Webserver
Companion Web Application for the Prise indexer. Spring application providing API endpoints for latest and historical prices

## Quickstart

You can run 
```bash
docker compose up app -d
```

## Build

    cd webserver
    ./gradlew clean build -x test

## Run

    ./gradlew bootRun

Or

    java -jar build/libs/webserver-<VERSION>.jar

## Configs
####    application.properties - app configuration
This is the standard spring application.properties file.
Override properties per environment, e.g. application-default.properties & application-production.properties
And set the SPRING_PROFILE_ACTIVE environment variable accordingly; e.g. export SPRING_PROFILES_ACTIVE=production

    server.port=<yours>
    cors.app.address=<yours>
    # SQL Database
    spring.datasource.username=<yours>
    spring.datasource.password=<yours>

     *see application.properties for more detailed descriptions

## Java Version

    Tested with Java 21

## Support
This project is made possible by Delegators to the [AUSST](https://ausstaker.com.au) Cardano Stakepool and
supporters of [Edgx](https://edgx.tech) R&D