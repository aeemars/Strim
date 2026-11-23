# Strim

Strim is a real-time financial transaction pipeline for anomaly detection and double-entry ledger processing.

The project uses Go, Kafka, Redis, and PostgreSQL. Monetary values are stored as integer minor units, with Nigerian naira (`NGN`) as the default currency.

## Run locally

Requirements: Docker Engine with the Compose plugin and Go.

```bash
cp .env.example .env
docker compose up -d
make health
```

This starts Kafka, Redis, and PostgreSQL. Kafka topics are created automatically, and the initial PostgreSQL schema is applied on first startup.

Stop the services:

```bash
docker compose down
```

Remove the local database volume as well:

```bash
docker compose down -v
```

## Generate transactions

Start Kafka and Redis, then run the producer:

```bash
docker compose up -d kafka redis kafka-init
make producer
```

Useful producer options:

```bash
go run ./cmd/producer --rate=10 --burst=12 --poison=0.001
go run ./cmd/producer --scenario=velocity --burst=12
go run ./cmd/producer --scenario=high-value
go run ./cmd/producer --scenario=impossible-travel
```

## Run the detector

With Kafka and Redis running:

```bash
make detector
```

The detector reads `transactions.raw`, evaluates NGN/kobo rules, and publishes decisions to `transactions.flagged` or `transactions.cleared`.

## Run the ledger

With Kafka and PostgreSQL running:

```bash
make ledger
```

The ledger service consumes cleared transactions from `transactions.cleared`, enforces strict idempotency via `processed_transactions`, acquires deterministic row-level locks on accounts to prevent deadlocks, mutates balances atomically, and writes double-entry records (`DEBIT` and `CREDIT`) to `ledger_entries`.


