.PHONY: run test migrate

run:
	go run ./cmd/server

test:
	chmod +x ./run-tests.sh
	./run-tests.sh

db-up:
	docker compose up db

test-db-up:
	docker compose up test_db

migrate:
	go run ./cmd/migrate
