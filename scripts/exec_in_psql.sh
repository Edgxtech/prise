#!/bin/bash

docker exec -it prise-postgres-1 psql -U prise -h localhost -p 5433 -d prise
