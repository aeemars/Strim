package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	TransactionTypeTransfer   = "TRANSFER"
	TransactionTypePayment    = "PAYMENT"
	TransactionTypeWithdrawal = "WITHDRAWAL"

	DefaultCurrency = "NGN"
)

var supportedCurrencies = map[string]struct{}{
	"EUR": {}, "GBP": {}, "NGN": {}, "USD": {},
}

type Transaction struct {
	ID                   string    `json:"id"`
	AccountID            string    `json:"account_id"`
	DestinationAccountID string    `json:"destination_account_id,omitempty"`
	AmountMinor          int64     `json:"amount_minor"`
	Currency             string    `json:"currency"`
	Timestamp            time.Time `json:"timestamp"`
	LocationCountry      string    `json:"location_country"`
	DeviceID             string    `json:"device_id"`
	IdempotencyKey       string    `json:"idempotency_key"`
	Type                 string    `json:"type"`
}

func (t Transaction) Validate() error {
	return t.ValidateAt(time.Now().UTC())
}

func (t Transaction) ValidateAt(now time.Time) error {
	if _, err := uuid.Parse(t.ID); err != nil {
		return fmt.Errorf("id must be a UUID: %w", err)
	}
	if err := validateIdentifier("account_id", t.AccountID); err != nil {
		return err
	}
	if t.Type == TransactionTypeTransfer {
		if err := validateIdentifier("destination_account_id", t.DestinationAccountID); err != nil {
			return err
		}
		if t.AccountID == t.DestinationAccountID {
			return errors.New("account_id and destination_account_id must differ")
		}
	} else if t.DestinationAccountID != "" {
		if err := validateIdentifier("destination_account_id", t.DestinationAccountID); err != nil {
			return err
		}
	}
	if t.AmountMinor <= 0 {
		return errors.New("amount_minor must be positive")
	}
	if _, ok := supportedCurrencies[t.Currency]; !ok {
		return fmt.Errorf("unsupported currency %q", t.Currency)
	}
	if t.Timestamp.Location() != time.UTC {
		return errors.New("timestamp must use UTC")
	}
	if t.Timestamp.Before(now.Add(-5*time.Minute)) || t.Timestamp.After(now.Add(5*time.Minute)) {
		return errors.New("timestamp is outside the allowed five-minute clock-skew window")
	}
	if len(t.LocationCountry) != 2 || t.LocationCountry != strings.ToUpper(t.LocationCountry) {
		return errors.New("location_country must be a two-letter uppercase country code")
	}
	if err := validateIdentifier("device_id", t.DeviceID); err != nil {
		return err
	}
	if err := validateIdentifier("idempotency_key", t.IdempotencyKey); err != nil {
		return err
	}
	switch t.Type {
	case TransactionTypeTransfer, TransactionTypePayment, TransactionTypeWithdrawal:
	default:
		return fmt.Errorf("unsupported transaction type %q", t.Type)
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if value == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be non-empty and have no surrounding whitespace", name)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s exceeds 128 characters", name)
	}
	return nil
}

func (t Transaction) MarshalJSON() ([]byte, error) {
	type transactionAlias Transaction
	return json.Marshal(transactionAlias(t))
}

func (t *Transaction) UnmarshalJSON(data []byte) error {
	if err := rejectDuplicateAndUnknownFields(data, []string{
		"id", "account_id", "destination_account_id", "amount_minor", "currency",
		"timestamp", "location_country", "device_id", "idempotency_key", "type",
	}); err != nil {
		return err
	}
	type transactionAlias Transaction
	var value transactionAlias
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode transaction: %w", err)
	}
	*t = Transaction(value)
	return nil
}

func rejectDuplicateAndUnknownFields(data []byte, allowed []string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	allowedFields := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedFields[field] = struct{}{}
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode object: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return errors.New("transaction must be a JSON object")
	}
	seen := make(map[string]struct{}, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode field name: %w", err)
		}
		field, ok := token.(string)
		if !ok {
			return errors.New("transaction field name must be a string")
		}
		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("duplicate field %q", field)
		}
		seen[field] = struct{}{}
		if _, ok := allowedFields[field]; !ok {
			return fmt.Errorf("unknown field %q", field)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("decode field %q: %w", field, err)
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("close object: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return errors.New("transaction object is not properly closed")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}
