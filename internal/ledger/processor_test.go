package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
	"github.com/aeemars/Strim/internal/ledger"
)

func TestParseClearedTransaction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tx := domain.Transaction{
		ID:                   uuid.NewString(),
		AccountID:            "account-001",
		DestinationAccountID: "account-002",
		AmountMinor:          50000,
		Currency:             domain.DefaultCurrency,
		Timestamp:            now,
		LocationCountry:      "NG",
		DeviceID:             "device-001",
		IdempotencyKey:       uuid.NewString(),
		Type:                 domain.TransactionTypeTransfer,
	}

	t.Run("from routed decision envelope", func(t *testing.T) {
		envelope := map[string]any{
			"decision_id":  uuid.NewString(),
			"rule_version": "detector.v1",
			"processed_at": now,
			"transaction":  tx,
		}
		data, err := json.Marshal(envelope)
		if err != nil {
			t.Fatalf("marshal envelope: %v", err)
		}

		parsed, err := ledger.ParseClearedTransaction(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if parsed.ID != tx.ID {
			t.Errorf("expected tx ID %s, got %s", tx.ID, parsed.ID)
		}
		if parsed.AmountMinor != tx.AmountMinor {
			t.Errorf("expected amount %d, got %d", tx.AmountMinor, parsed.AmountMinor)
		}
	})

	t.Run("from direct transaction JSON", func(t *testing.T) {
		data, err := json.Marshal(tx)
		if err != nil {
			t.Fatalf("marshal transaction: %v", err)
		}

		parsed, err := ledger.ParseClearedTransaction(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if parsed.ID != tx.ID {
			t.Errorf("expected tx ID %s, got %s", tx.ID, parsed.ID)
		}
	})

	t.Run("malformed payload", func(t *testing.T) {
		_, err := ledger.ParseClearedTransaction([]byte(`{"invalid": true}`))
		if err == nil {
			t.Fatal("expected error on malformed payload, got nil")
		}
	})
}

func TestProcessorRejectsInvalidDomainTransaction(t *testing.T) {
	t.Parallel()

	proc := ledger.NewProcessor(nil)
	msg := segmentio.Message{
		Value: []byte(`{"id": "not-a-uuid"}`),
	}
	err := proc.Process(context.Background(), msg)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestLivePostgresSettlement(t *testing.T) {
	if os.Getenv("STRIM_LIVE_POSTGRES") != "1" {
		t.Skip("set STRIM_LIVE_POSTGRES=1 to run against live PostgreSQL")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://strim:strim-local-only@localhost:5432/strim?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	// Create two isolated test accounts
	senderID := uuid.New()
	recipientID := uuid.New()
	senderNum := "test-sender-" + senderID.String()[:8]
	recipientNum := "test-recipient-" + recipientID.String()[:8]

	_, err = pool.Exec(ctx, `
		INSERT INTO accounts (id, account_number, balance_minor, currency)
		VALUES ($1, $2, 100000, 'NGN'), ($3, $4, 50000, 'NGN')
	`, senderID, senderNum, recipientID, recipientNum)
	if err != nil {
		t.Fatalf("seed test accounts: %v", err)
	}

	proc := ledger.NewProcessor(pool)

	tx := domain.Transaction{
		ID:                   uuid.NewString(),
		AccountID:            senderNum,
		DestinationAccountID: recipientNum,
		AmountMinor:          25000,
		Currency:             "NGN",
		Timestamp:            time.Now().UTC(),
		LocationCountry:      "NG",
		DeviceID:             "dev-001",
		IdempotencyKey:       uuid.NewString(),
		Type:                 domain.TransactionTypeTransfer,
	}

	payload, err := json.Marshal(map[string]any{
		"decision_id":  uuid.NewString(),
		"rule_version": "detector.v1",
		"transaction":  tx,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	msg := segmentio.Message{Value: payload}

	// 1. Process transfer
	if err := proc.Process(ctx, msg); err != nil {
		t.Fatalf("expected successful settlement, got: %v", err)
	}

	// Verify balances
	var senderBal, recipientBal int64
	if err := pool.QueryRow(ctx, "SELECT balance_minor FROM accounts WHERE id = $1", senderID).Scan(&senderBal); err != nil {
		t.Fatalf("query sender balance: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT balance_minor FROM accounts WHERE id = $1", recipientID).Scan(&recipientBal); err != nil {
		t.Fatalf("query recipient balance: %v", err)
	}
	if senderBal != 75000 {
		t.Errorf("expected sender balance 75000, got %d", senderBal)
	}
	if recipientBal != 75000 {
		t.Errorf("expected recipient balance 75000, got %d", recipientBal)
	}

	// 2. Redeliver same message (Idempotency test)
	if err := proc.Process(ctx, msg); err != nil {
		t.Fatalf("expected idempotent success on redelivery, got error: %v", err)
	}

	// Verify balances unchanged after duplicate delivery
	if err := pool.QueryRow(ctx, "SELECT balance_minor FROM accounts WHERE id = $1", senderID).Scan(&senderBal); err != nil {
		t.Fatalf("query sender balance: %v", err)
	}
	if senderBal != 75000 {
		t.Errorf("balance was mutated on duplicate delivery: expected 75000, got %d", senderBal)
	}

	// 3. Test Insufficient Funds
	txOverdraw := tx
	txOverdraw.ID = uuid.NewString()
	txOverdraw.IdempotencyKey = uuid.NewString()
	txOverdraw.AmountMinor = 1_000_000 // exceeds 75,000

	overdrawPayload, _ := json.Marshal(map[string]any{"transaction": txOverdraw})
	err = proc.Process(ctx, segmentio.Message{Value: overdrawPayload})
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Errorf("expected ErrInsufficientFunds, got: %v", err)
	}
}
