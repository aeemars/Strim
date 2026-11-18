package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type recordingPublisher struct {
	messages []segmentio.Message
	err      error
	closed   bool
}

func (r *recordingPublisher) WriteMessages(_ context.Context, messages ...segmentio.Message) error {
	r.messages = append(r.messages, messages...)
	return r.err
}

func (r *recordingPublisher) Close() error {
	r.closed = true
	return nil
}

func validTransaction() domain.Transaction {
	now := time.Now().UTC()
	return domain.Transaction{
		ID:                   uuid.NewString(),
		AccountID:            "account-001",
		DestinationAccountID: "account-002",
		AmountMinor:          12500,
		Currency:             domain.DefaultCurrency,
		Timestamp:            now,
		LocationCountry:      "NG",
		DeviceID:             "device-001",
		IdempotencyKey:       uuid.NewString(),
		Type:                 domain.TransactionTypeTransfer,
	}
}

func TestProducerPublishUsesAccountHashKey(t *testing.T) {
	publisher := &recordingPublisher{}
	producer := &Producer{Topic: "transactions.raw", Publisher: publisher}
	transaction := validTransaction()

	if err := producer.Publish(context.Background(), transaction); err != nil {
		t.Fatal(err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(publisher.messages))
	}
	message := publisher.messages[0]
	if string(message.Key) != transaction.AccountID {
		t.Fatalf("got key %q, want %q", message.Key, transaction.AccountID)
	}
	var decoded domain.Transaction
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AccountID != transaction.AccountID {
		t.Fatalf("got account %q, want %q", decoded.AccountID, transaction.AccountID)
	}
}

func TestProducerPublishFailureAndClose(t *testing.T) {
	publisher := &recordingPublisher{err: errors.New("broker unavailable")}
	producer := &Producer{Topic: "transactions.raw", Publisher: publisher}
	if err := producer.Publish(context.Background(), validTransaction()); err == nil {
		t.Fatal("expected publish failure")
	}
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
	if !publisher.closed {
		t.Fatal("expected publisher to be closed")
	}
}

func TestProducerRejectsInvalidTransaction(t *testing.T) {
	publisher := &recordingPublisher{}
	producer := &Producer{Topic: "transactions.raw", Publisher: publisher}
	transaction := validTransaction()
	transaction.AmountMinor = 0
	if err := producer.Publish(context.Background(), transaction); err == nil {
		t.Fatal("expected validation failure")
	}
	if len(publisher.messages) != 0 {
		t.Fatal("invalid transaction must not be published")
	}
}
