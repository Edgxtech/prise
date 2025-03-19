#!/bin/bash

docker build -t prise/indexer:latest --build-arg PROPERTIES=prise.docker.properties .
