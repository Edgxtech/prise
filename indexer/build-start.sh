#!/bin/bash

./gradlew clean build -x test
java -jar build/libs/indexer-0.0.1.jar -config prise.properties
