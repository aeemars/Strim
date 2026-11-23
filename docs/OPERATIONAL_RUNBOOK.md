# Strim: Operational Runbook & Production Guide

This runbook documents operational procedures, incident response playbooks, security baselines, and replay mechanisms for the **Strim Real-Time Financial Transaction & Anomaly Detection Pipeline**.

---

## 1. Topic Topologies & Retention Policies

| Topic | Partitions | Retention Period | Compaction / Cleanup | Description |
| :--- | :--- | :--- | :--- | :--- |
| `transactions.raw` | 3+ | 7 days | `delete` | Ingested raw transactions keyed by `AccountID`. |
| `transactions.flagged` | 3+ | 30 days | `delete` | Decisions containing detected anomalies & security violations. |
| `transactions.cleared` | 3+ | 7 days | `delete` | Validated clean transactions ready for ledger settlement. |
| `transactions.dlq` | 3+ | 90 days | `delete` | Malformed or poison-pill payloads diverted for audit. |

### Topic Inspection Commands
```bash
# List all topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Check consumer group lag for detector
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group fraud-detector-service

# Check consumer group lag for ledger
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group ledger-service
```

---

## 2. Dead-Letter Queue (DLQ) Management & Replay

### Inspecting DLQ Messages
Each DLQ message is enveloped with metadata (`original_topic`, `original_partition`, `original_offset`, `error_message`, `failed_at`):

```bash
# Read the latest DLQ envelopes
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic transactions.dlq \
  --from-beginning \
  --max-messages 10
```

### DLQ Replay Procedure
When a bug or schema discrepancy causes poison-pill diversions:
1. Identify the root cause from `error_message` in the DLQ envelope.
2. Deploy the bug fix to `internal/detector` or `internal/ledger`.
3. Use the DLQ replayer utility or read from `transactions.dlq`, extract `original_payload`, and republish to `transactions.raw` (or `transactions.cleared`).
4. Because consumers enforce idempotency via `processed_transactions`, replayed events will settle safely without double debits.

---

## 3. Incident Response Playbooks

### Playbook A: PostgreSQL Outage / Connection Pool Exhaustion
* **Symptom:** Ledger service logs `begin db transaction` or `connection refused` errors; consumer restarts or retries with backoff.
* **Impact:** Ingestion does not stop; Kafka buffers messages in `transactions.cleared`. Offsets are **not** committed until the database recovers.
* **Resolution:**
  1. Check PostgreSQL health: `docker compose exec postgres pg_isready -U strim -d strim`.
  2. Inspect connection count: `SELECT count(*) FROM pg_stat_activity;`.
  3. Verify PostgreSQL volume storage: `df -h /var/lib/postgresql/data`.
  4. Once database connectivity is restored, the `ledger-service` consumer automatically drains the backlog without data loss.

### Playbook B: Redis Outage / Eviction
* **Symptom:** Detector service logs `connect to Redis` or velocity evaluation failures.
* **Impact:** Sliding-window velocity checks and geo-travel checks cannot evaluate.
* **Resolution:**
  1. Check Redis status: `docker compose exec redis redis-cli ping`.
  2. Verify memory usage: `docker compose exec redis redis-cli info memory`.
  3. Ensure `maxmemory-policy` is configured to `noeviction` or `volatile-lru` for financial state keys.
  4. Redis state keys are configured with TTLs (`60s` for velocity, `12h` for geo-location) and naturally rehydrate as traffic flows.

### Playbook C: Sudden Consumer Lag Spike
* **Symptom:** Consumer lag increases on `transactions.raw` or `transactions.cleared`.
* **Resolution:**
  1. Inspect partition distribution: Ensure partition count >= number of consumer replicas.
  2. Scale consumer instances horizontally:
     ```bash
     # Run additional detector or ledger instances in the same group ID
     go run ./cmd/detector &
     go run ./cmd/detector &
     ```
  3. Kafka automatically triggers a rebalance and assigns distinct partitions to the new instances.

---

## 4. Security Baseline & Operational Hardening

1. **Credential Management:**
   * Never store raw credentials in Git. Use `.env` or external secret managers (e.g. HashiCorp Vault, AWS Secrets Manager).
   * All database connection strings are masked in logs via `maskURL` (`[REDACTED]`).
2. **PII & Log Sanitization:**
   * The `telemetry.RedactingHandler` automatically redacts attributes matching `password`, `secret`, `token`, `key`, `authorization`, and `database_url`.
3. **Database Principle of Least Privilege:**
   * Use dedicated database users for services:
     * `strim_app`: `SELECT`, `INSERT`, `UPDATE` on `accounts`, `processed_transactions`, `ledger_entries`.
     * Migration runner / admin: `CREATE`, `ALTER`, `DROP`.
4. **Network Boundaries:**
   * Keep Kafka port `29092` internal to Docker / private VPC. Expose port `9092` only to authorized internal workers.
