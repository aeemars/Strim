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


