#!/usr/bin/env sh

set -eu

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
DB_SERVICE="${DB_SERVICE:-test_db}"
# Only DHARA_TEST_DATABASE_URL is honored for tests (never DATABASE_URL: this
# project may run alongside other services that use that variable).
DHARA_TEST_DATABASE_URL="${DHARA_TEST_DATABASE_URL:-postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable}"
export DHARA_TEST_DATABASE_URL

# -----------------------------------------------------------------------------
# Container-engine detection: docker compose, docker-compose, or podman-compose.
# Tests only need the test_db service, so any of the three works.
# -----------------------------------------------------------------------------
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
elif command -v podman-compose >/dev/null 2>&1; then
    COMPOSE="podman-compose"
else
    echo "error: no container engine found (need docker compose, docker-compose, or podman-compose)." >&2
    exit 1
fi

echo "Using compose engine: ${COMPOSE}"

echo "Starting test database container: ${DB_SERVICE}"
# shellcheck disable=SC2086
$COMPOSE -f "${COMPOSE_FILE}" up -d "${DB_SERVICE}"

echo "Waiting for database to be ready..."
ATTEMPTS=30
SLEEP_SECS=1
i=1

while [ "$i" -le "$ATTEMPTS" ]; do
    # shellcheck disable=SC2086
    if $COMPOSE -f "${COMPOSE_FILE}" exec -T "${DB_SERVICE}" pg_isready -U dhara -d dhara_test >/dev/null 2>&1; then
        echo "Database is ready."
        break
    fi

    if [ "$i" -eq "$ATTEMPTS" ]; then
        echo "Database did not become ready in time."
        # shellcheck disable=SC2086
        $COMPOSE -f "${COMPOSE_FILE}" stop "${DB_SERVICE}" || true
        # shellcheck disable=SC2086
        $COMPOSE -f "${COMPOSE_FILE}" rm -f "${DB_SERVICE}" || true
        exit 1
    fi

    i=$((i + 1))
    sleep "$SLEEP_SECS"
done

echo "Running tests with DHARA_TEST_DATABASE_URL=${DHARA_TEST_DATABASE_URL}"
# -timeout guards against a wedged test hanging CI forever; -shuffle makes
# ordering bugs (tests depending on rows left behind) reproducible.
go test ./... -p 1 -v -race -shuffle=on -timeout=600s

echo "Stopping test database container."
# shellcheck disable=SC2086
$COMPOSE -f "${COMPOSE_FILE}" stop "${DB_SERVICE}"
# shellcheck disable=SC2086
$COMPOSE -f "${COMPOSE_FILE}" down "${DB_SERVICE}"
