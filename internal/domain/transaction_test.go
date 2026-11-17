package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validTransaction(now time.Time) Transaction {
	return Transaction{
		ID:                   uuid.NewString(),
		AccountID:            "account-001",
		DestinationAccountID: "account-002",
		AmountMinor:          125000,
		Currency:             DefaultCurrency,
		Timestamp:            now.UTC(),
		LocationCountry:      "NG",
		DeviceID:             "device-001",
		IdempotencyKey:       "idem-001",
		Type:                 TransactionTypeTransfer,
	}
}

func TestTransactionRoundTripAndValidation(t *testing.T) {
	now := time.Date(2026, 11, 17, 18, 0, 0, 0, time.UTC)
	want := validTransaction(now)
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Transaction
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip mismatch: got %#v, want %#v", got, want)
	}
}

func TestTransactionRejectsUnknownAndDuplicateFields(t *testing.T) {
	now := validTransaction(time.Now().UTC())
	data, err := json.Marshal(now)
	if err != nil {
		t.Fatal(err)
	}
	unknown := strings.TrimSuffix(string(data), "}") + `,"unexpected":true}`
	var transaction Transaction
	if err := json.Unmarshal([]byte(unknown), &transaction); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
	duplicate := strings.TrimSuffix(string(data), "}") + `,"id":"` + now.ID + `"}`
	if err := json.Unmarshal([]byte(duplicate), &transaction); err == nil {
		t.Fatal("expected duplicate field to be rejected")
	}
	if err := json.Unmarshal([]byte(`{"id":`), &transaction); err == nil {
		t.Fatal("expected malformed JSON to be rejected")
	}
}

func TestTransactionValidationBoundaries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name string
		edit func(*Transaction)
	}{
		{"zero amount", func(t *Transaction) { t.AmountMinor = 0 }},
		{"same accounts", func(t *Transaction) { t.DestinationAccountID = t.AccountID }},
		{"future timestamp", func(t *Transaction) { t.Timestamp = now.Add(6 * time.Minute) }},
		{"non UTC timestamp", func(t *Transaction) { t.Timestamp = now.In(time.FixedZone("WAT", 3600)) }},
		{"whitespace account", func(t *Transaction) { t.AccountID = " account-001" }},
		{"unsupported currency", func(t *Transaction) { t.Currency = "JPY" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transaction := validTransaction(now)
			test.edit(&transaction)
			if err := transaction.ValidateAt(now); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
