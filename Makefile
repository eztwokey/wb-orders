.PHONY: up down logs integration-up migrate-up migrate-down fmt fmt-check vet test test-race test-integration lint build tidy clean

GO ?= go
DATABASE_URL ?= postgres://wb_orders:wb_orders@localhost:5432/wb_orders?sslmode=disable

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

integration-up:
	docker compose up -d postgres kafka
	docker compose run --rm migrate

migrate-up:
	docker run --rm --network host -v "$(CURDIR)/migrations:/migrations:ro" migrate/migrate:v4.18.2 -path=/migrations -database="$(DATABASE_URL)" up

migrate-down:
	docker run --rm --network host -v "$(CURDIR)/migrations:/migrations:ro" migrate/migrate:v4.18.2 -path=/migrations -database="$(DATABASE_URL)" down 1

fmt:
	$(GO) fmt ./...

fmt-check:
	test -z "$$(find . -type f -name '*.go' -exec gofmt -l {} +)"

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	TEST_DATABASE_URL="$(DATABASE_URL)" TEST_KAFKA_BROKERS="$${TEST_KAFKA_BROKERS:-localhost:29092}" $(GO) test -tags=integration ./internal/order ./internal/consumer ./internal/outbox ./internal/platform/kafka

lint:
	golangci-lint run ./...

build:
	mkdir -p bin
	$(GO) build -o bin/order-service ./cmd/order-service
	$(GO) build -o bin/outbox-publisher ./cmd/outbox-publisher
	$(GO) build -o bin/order-worker ./cmd/order-worker

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin coverage.out
