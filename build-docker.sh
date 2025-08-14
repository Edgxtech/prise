#!/bin/bash

source ./build.sh

cd webserver
docker build -t edgxtech/prise-app:latest .

cd ../indexer
docker build -t edgxtech/prise-indexer:latest .