#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
  if [ $1 -eq 0 ]; then
    echo -e "${GREEN}✓ $2${NC}"
  else
    echo -e "${RED}✗ $2${NC}"
    exit 1
  fi
}

echo "=== Docker Compose Stats Snapshot ==="
docker compose stats --no-stream
echo ""

echo "=== Health Checks ==="

# Postgres: Check if accepting connections
echo "Checking Postgres..."
docker exec prise-postgres-1 pg_isready -U "${DATABASE_USERNAME:-postgres}" -d "${DATABASE_NAME:-postgres}" -p 5433 -h localhost
print_status $? "Postgres is ready"

# Indexer: Check if /metrics endpoint returns 200
INDEXER_CHECK_URL=http://localhost:9108/metrics
echo "Checking Indexer ($INDEXER_CHECK_URL) ..."
curl -s -o /dev/null -w "%{http_code}" $INDEXER_CHECK_URL | grep -q 200
if [ $? -eq 0 ]; then
  print_status 0 "Indexer is responding (HTTP 200)"
else
  print_status 1 "Indexer health check failed"
fi

# App: Check if /tokens/symbols endpoint returns 200
APP_CHECK_URL=http://localhost:8092/tokens/symbols
echo "Checking Prise App ($APP_CHECK_URL) ..."
curl -s -o /dev/null -w "%{http_code}" $APP_CHECK_URL | grep -q 200
if [ $? -eq 0 ]; then
  print_status 0 "Prise App is responding (HTTP 200)"
else
  print_status 1 "Prise App health check failed"
fi

# Redis: Check if container is running before health check
echo "Checking Redis..."
if docker inspect prise-redis-1 >/dev/null 2>&1 && [ "$(docker inspect --format='{{.State.Running}}' prise-redis-1)" = "true" ]; then
  docker exec prise-redis-1 redis-cli -p 6379 ping | grep -q PONG
  if [ $? -eq 0 ]; then
    print_status 0 "Redis is ready"
  else
    print_status 1 "Redis health check failed"
  fi
else
  echo -e "${GREEN}Redis is not running (optional service, skipped)${NC}"
fi

echo -e "${GREEN}All checks passed successfully!${NC}"