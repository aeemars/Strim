package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/config"
	"github.com/aeemars/Strim/internal/detector"
	"github.com/aeemars/Strim/internal/domain"
	strimkafka "github.com/aeemars/Strim/internal/kafka"
	"github.com/aeemars/Strim/internal/ledger"
)

func TestEndToEndPipeline(t *testing.T) {
	if os.Getenv("STRIM_E2E") != "1" {
		t.Skip("set STRIM_E2E=1 to run end-to-end integration tests")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokers := splitBrokers(cfg.KafkaBrokers)

	// 1. Database & Redis Connections
	pgPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	if err := pgPool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	// 2. Snapshot Initial Ledger State
	var initialBalanceSum int64
	err = pgPool.QueryRow(ctx, "SELECT COALESCE(SUM(balance_minor), 0) FROM accounts").Scan(&initialBalanceSum)
	if err != nil {
		t.Fatalf("query initial balance sum: %v", err)
	}

	var initialLedgerCount int64
	err = pgPool.QueryRow(ctx, "SELECT COUNT(*) FROM ledger_entries").Scan(&initialLedgerCount)
	if err != nil {
		t.Fatalf("query initial ledger count: %v", err)
	}

	// 3. Start Detector and Ledger Pipeline in Background
	rawTopic := fmt.Sprintf("e2e-raw-%s", uuid.NewString()[:8])
	clearedTopic := fmt.Sprintf("e2e-cleared-%s", uuid.NewString()[:8])
	flaggedTopic := fmt.Sprintf("e2e-flagged-%s", uuid.NewString()[:8])
	dlqTopic := fmt.Sprintf("e2e-dlq-%s", uuid.NewString()[:8])
	_ = dlqTopic

	detectorEngine := &detector.Engine{
		Rules: &detector.RuleEngine{
			Velocity: detector.NewVelocityTracker(redisClient),
			Geo:      redisClient,
		},
		Publisher: &segmentio.Writer{
			Addr:         segmentio.TCP(brokers...),
			Balancer:     &segmentio.Hash{},
			RequiredAcks: segmentio.RequireAll,
			MaxAttempts:  5,
			Compression:  segmentio.Snappy,
		},
		ClearedTopic: clearedTopic,
		FlaggedTopic: flaggedTopic,
	}

	// Detector consumer
	detectorConsumer, err := strimkafka.NewConsumer(
		brokers,
		fmt.Sprintf("e2e-det-group-%s", uuid.NewString()[:8]),
		rawTopic,
		detectorEngine.Process,
	)
	if err != nil {
		t.Fatalf("create detector consumer: %v", err)
	}
	defer detectorConsumer.Close()

	// Ledger consumer
	ledgerProcessor := ledger.NewProcessor(pgPool)
	ledgerConsumer, err := strimkafka.NewConsumer(
		brokers,
		fmt.Sprintf("e2e-ledger-group-%s", uuid.NewString()[:8]),
		clearedTopic,
		ledgerProcessor.Process,
	)
	if err != nil {
		t.Fatalf("create ledger consumer: %v", err)
	}
	defer ledgerConsumer.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = detectorConsumer.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = ledgerConsumer.Run(ctx)
	}()

	// 4. Publish Test Transactions
	rawProducer, err := strimkafka.NewProducer(brokers, rawTopic)
	if err != nil {
		t.Fatalf("create raw producer: %v", err)
	}
	defer rawProducer.Close()

	const normalCount = 50
	const highValueCount = 5
	const velocityCount = 10
	const poisonPillCount = 3

	// A. Normal clean transactions (between account-001 and account-002)
	for i := 0; i < normalCount; i++ {
		tx := domain.Transaction{
			ID:                   uuid.NewString(),
			AccountID:            "account-001",
			DestinationAccountID: "account-002",
			AmountMinor:          1000,
			Currency:             "NGN",
			Timestamp:            time.Now().UTC(),
			LocationCountry:      "NG",
			DeviceID:             "dev-001",
			IdempotencyKey:       uuid.NewString(),
			Type:                 domain.TransactionTypeTransfer,
		}
		if err := rawProducer.Publish(ctx, tx); err != nil {
			t.Fatalf("publish normal tx: %v", err)
		}
	}

	// B. High-value transactions (> 1,000,000 minor units -> flagged)
	for i := 0; i < highValueCount; i++ {
		tx := domain.Transaction{
			ID:                   uuid.NewString(),
			AccountID:            "account-001",
			DestinationAccountID: "account-002",
			AmountMinor:          2_500_000,
			Currency:             "NGN",
			Timestamp:            time.Now().UTC(),
			LocationCountry:      "NG",
			DeviceID:             "dev-001",
			IdempotencyKey:       uuid.NewString(),
			Type:                 domain.TransactionTypeTransfer,
		}
		if err := rawProducer.Publish(ctx, tx); err != nil {
			t.Fatalf("publish high-value tx: %v", err)
		}
	}

	// C. Velocity burst on account-003 (> 5 within 60s)
	for i := 0; i < velocityCount; i++ {
		tx := domain.Transaction{
			ID:                   uuid.NewString(),
			AccountID:            "account-003",
			DestinationAccountID: "account-004",
			AmountMinor:          100,
			Currency:             "NGN",
			Timestamp:            time.Now().UTC(),
			LocationCountry:      "NG",
			DeviceID:             "dev-003",
			IdempotencyKey:       uuid.NewString(),
			Type:                 domain.TransactionTypeTransfer,
		}
		if err := rawProducer.Publish(ctx, tx); err != nil {
			t.Fatalf("publish velocity tx: %v", err)
		}
	}

	// D. Poison pills (malformed payloads)
	for i := 0; i < poisonPillCount; i++ {
		if err := rawProducer.PublishRaw(ctx, []byte("poison"), []byte(`{malformed-json}`)); err != nil {
			t.Fatalf("publish poison pill: %v", err)
		}
	}

	// 5. Await Processing Completion
	time.Sleep(5 * time.Second)
	cancel() // signal consumers to exit gracefully
	wg.Wait()

	// 6. Verify Mathematical Value Conservation in PostgreSQL
	var finalBalanceSum int64
	err = pgPool.QueryRow(context.Background(), "SELECT COALESCE(SUM(balance_minor), 0) FROM accounts").Scan(&finalBalanceSum)
	if err != nil {
		t.Fatalf("query final balance sum: %v", err)
	}

	if finalBalanceSum != initialBalanceSum {
		t.Errorf("VIOLATION: Total monetary supply changed! Initial: %d, Final: %d, Diff: %d",
			initialBalanceSum, finalBalanceSum, finalBalanceSum-initialBalanceSum)
	}

	// 7. Verify No Negative Balances
	var negativeCount int64
	err = pgPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM accounts WHERE balance_minor < 0").Scan(&negativeCount)
	if err != nil {
		t.Fatalf("query negative balances: %v", err)
	}
	if negativeCount > 0 {
		t.Errorf("VIOLATION: Found %d accounts with negative balances", negativeCount)
	}

	// 8. Verify Double-Entry Balance (Total Debits == Total Credits)
	var imbalance int64
	err = pgPool.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(CASE WHEN entry_type = 'DEBIT' THEN amount_minor ELSE -amount_minor END), 0)
		FROM ledger_entries
	`).Scan(&imbalance)
	if err != nil {
		t.Fatalf("query ledger balance: %v", err)
	}
	if imbalance != 0 {
		t.Errorf("VIOLATION: Double-entry ledger is unbalanced by %d minor units", imbalance)
	}

	// 9. Verify Zero Orphan Ledger Entries
	var orphanCount int64
	err = pgPool.QueryRow(context.Background(), `
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id NOT IN (SELECT transaction_id FROM processed_transactions)
	`).Scan(&orphanCount)
	if err != nil {
		t.Fatalf("query orphan entries: %v", err)
	}
	if orphanCount > 0 {
		t.Errorf("VIOLATION: Found %d orphan ledger entries", orphanCount)
	}

	t.Logf("E2E Verification Complete: Value Conservation verified (%d minor units), Ledger Balanced (0 delta)", finalBalanceSum)
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
