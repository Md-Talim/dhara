#!/usr/bin/env sh

set -eu

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
DB_SERVICE="${DB_SERVICE:-test_db}"
DATABASE_URL="${DATABASE_URL:-postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable}"

echo "Starting test database container: ${DB_SERVICE}"
docker compose -f "${COMPOSE_FILE}" up -d "${DB_SERVICE}"

echo "Waiting for database to be ready..."
ATTEMPTS=30
SLEEP_SECS=1
i=1

while [ "$i" -le "$ATTEMPTS" ]; do
    if docker compose -f "${COMPOSE_FILE}" exec -T "${DB_SERVICE}" pg_isready -U dhara -d dhara_test >/dev/null 2>&1; then
        echo "Database is ready."
        break
    fi

    if [ "$i" -eq "$ATTEMPTS" ]; then
        echo "Database did not become ready in time."
        exit 1
    fi

    i=$((i + 1))
    sleep "$SLEEP_SECS"
done

echo "Running tests with DATABASE_URL=${DATABASE_URL}"
DATABASE_URL="${DATABASE_URL}" go test ./... -v

docker compose -f "${COMPOSE_FILE}" stop "${DB_SERVICE}"
docker compose -f "${COMPOSE_FILE}" rm -f "${DB_SERVICE}"
