# Local development defaults: override via environment if you use another database.
DHARA_DATABASE_URL ?= postgres://dhara:dhara@localhost:5432/dhara?sslmode=disable
export DHARA_DATABASE_URL

# Compose engine detection: docker compose > docker-compose > podman-compose.
# Override explicitly with COMPOSE="podman-compose" if detection misfires.
COMPOSE ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || (command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo podman-compose))

.PHONY: help dev dev-server dev-worker dev-watch db-up db-down wait-db migrate migrate-down test build playground playground-build playground-up playground-down up down

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

dev: db-up migrate ## Start Postgres, run migrations, then run server + worker together (Ctrl-C stops both)
	@echo "Starting API server on http://localhost:8080 and worker..."
	@trap 'kill 0' INT TERM EXIT; \
		go run ./cmd/server & \
		go run ./cmd/worker & \
		wait

dev-server: db-up migrate ## Run only the API server
	go run ./cmd/server

dev-worker: db-up migrate ## Run only the worker
	go run ./cmd/worker

db-up: ## Start the local Postgres container
	$(COMPOSE) up -d db
	@$(MAKE) wait-db

db-down: ## Stop and remove the local Postgres container (data volume is kept)
	$(COMPOSE) down db

wait-db: ## Wait until Postgres accepts connections
	@echo "Waiting for database to be ready..."
	@until $(COMPOSE) exec -T db pg_isready -U dhara -d dhara >/dev/null 2>&1; do sleep 1; done
	@echo "Database is ready."

migrate: ## Apply pending migrations (db must be running)
	go run ./cmd/migrate

migrate-down: ## Roll back the last migration (db must be running)
	go run ./cmd/migrate down

test: ## Run the full test suite (spins up a test database container)
	chmod +x ./run-tests.sh
	./run-tests.sh

build: ## Build all binaries into ./bin
	go build -o bin/dhara-server ./cmd/server
	go build -o bin/dhara-worker ./cmd/worker
	go build -o bin/dhara-migrate ./cmd/migrate
	go build -o bin/dhara-playground ./cmd/playground

playground: db-up ## Run the interactive playground against the local database
	go run ./cmd/playground

playground-build: ## Build the playground binary into ./bin
	go build -o bin/dhara-playground ./cmd/playground

playground-up: ## Build and start the playground + its own Postgres in containers
	$(COMPOSE) -f docker-compose.playground.yml up -d --build

playground-down: ## Stop the playground compose stack (data volume is kept)
	$(COMPOSE) -f docker-compose.playground.yml down

up: ## Build and start the full compose stack (zero manual steps)
	$(COMPOSE) up -d --build

down: ## Stop the compose stack
	$(COMPOSE) down
