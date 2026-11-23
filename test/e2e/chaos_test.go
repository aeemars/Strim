package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/config"
	"github.com/aeemars/Strim/internal/domain"
	strimkafka "github.com/aeemars/Strim/internal/kafka"
	"github.com/aeemars/Strim/internal/ledger"
)

func TestChaosConsumerKillAndResume(t *testing.T) {
	if os.Getenv("STRIM_E2E") != "1" {
		t.Skip("set STRIM_E2E=1 to run chaos recovery integration tests")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx := context.Background()
	brokers := splitBrokers(cfg.KafkaBrokers)

	pgPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	topic := fmt.Sprintf("chaos-txs-%s", uuid.NewString()[:8])
	groupID := fmt.Sprintf("chaos-group-%s", uuid.NewString()[:8])

	// 1. Publish 30 transactions to topic
	producer, err := strimkafka.NewProducer(brokers, topic)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	const totalTxs = 30
	txList := make([]domain.Transaction, totalTxs)
	for i := 0; i < totalTxs; i++ {
		txList[i] = domain.Transaction{
			ID:                   uuid.NewString(),
			AccountID:            "account-005",
			DestinationAccountID: "account-006",
			AmountMinor:          500,
			Currency:             "NGN",
			Timestamp:            time.Now().UTC(),
			LocationCountry:      "NG",
			DeviceID:             "dev-chaos",
			IdempotencyKey:       uuid.NewString(),
			Type:                 domain.TransactionTypeTransfer,
		}
		payload, _ := json.Marshal(map[string]any{
			"transaction": txList[i],
		})
		if err := producer.PublishRaw(ctx, []byte(txList[i].AccountID), payload); err != nil {
			t.Fatalf("publish tx %d: %v", i, err)
		}
	}

	// 2. Start Consumer 1 and Cancel Mid-Flight
	processor := ledger.NewProcessor(pgPool)
	ctx1, cancel1 := context.WithCancel(ctx)

	consumer1, err := strimkafka.NewConsumer(brokers, groupID, topic, processor.Process)
	if err != nil {
		t.Fatalf("create consumer 1: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer1.Run(ctx1)
	}()

	// Simulate crash: cancel context after 500ms
	time.Sleep(500 * time.Millisecond)
	cancel1()
	consumer1.Close()
	wg.Wait()

	// 3. Start Consumer 2 on the SAME Group ID to resume uncommitted offsets
	ctx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()

	consumer2, err := strimkafka.NewConsumer(brokers, groupID, topic, processor.Process)
	if err != nil {
		t.Fatalf("create consumer 2: %v", err)
	}
	defer consumer2.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer2.Run(ctx2)
	}()

	// Allow consumer 2 time to process remaining messages
	time.Sleep(5 * time.Second)
	cancel2()
	wg.Wait()

	// 4. Verification: All 30 transactions must be processed exactly once
	var processedCount int
	err = pgPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM processed_transactions
		WHERE idempotency_key = ANY($1)
	`, extractIdempotencyKeys(txList)).Scan(&processedCount)
	if err != nil {
		t.Fatalf("query processed count: %v", err)
	}

	if processedCount != totalTxs {
		t.Errorf("expected exactly %d processed transactions, got %d", totalTxs, processedCount)
	}

	// 5. Replay Invariance Test: Re-feed all 30 transactions
	for i, tx := range txList {
		payload, _ := json.Marshal(map[string]any{"transaction": tx})
		msg := segmentio.Message{Value: payload}
		if err := processor.Process(ctx, msg); err != nil {
			t.Errorf("tx %d: replay failed: %v", i, err)
		}
	}

	// Re-verify processed count unchanged
	var recheckCount int
	_ = pgPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM processed_transactions
		WHERE idempotency_key = ANY($1)
	`, extractIdempotencyKeys(txList)).Scan(&recheckCount)

	if recheckCount != totalTxs {
		t.Errorf("replay caused duplicate rows: expected %d, got %d", totalTxs, recheckCount)
	}

	t.Logf("Chaos & Replay Test Passed: %d/%d transactions settled with zero duplicates across consumer kill/restart", processedCount, totalTxs)
}

func extractIdempotencyKeys(txs []domain.Transaction) []string {
	keys := make([]string, len(txs))
	for i, tx := range txs {
		keys[i] = tx.IdempotencyKey
	}
	return keys
}
