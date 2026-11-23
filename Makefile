.PHONY: up down logs topics health producer detector ledger test test-e2e

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

topics:
	docker compose run --rm kafka-init

health:
	docker compose ps
	docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
	docker compose exec redis redis-cli ping
	docker compose exec postgres pg_isready -U "$${POSTGRES_USER:-strim}" -d "$${POSTGRES_DB:-strim}"

producer:
	go run ./cmd/producer

detector:
	go run ./cmd/detector

ledger:
	go run ./cmd/ledger

test:
	go test -v ./...

test-e2e:
	STRIM_E2E=1 go test -v -timeout 5m ./test/e2e/...
