#!/bin/bash
mvn clean install -DskipTests
mvn exec:exec -Dconfig=prise.properties