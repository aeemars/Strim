package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAnomalyAlertValidation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	alert := AnomalyAlert{
		TransactionID: uuid.NewString(),
		AccountID:     "account-001",
		FlaggedAt:     now,
		Reason:        AnomalyReasonHighValue,
		Severity:      SeverityHigh,
		SnapshotData:  map[string]any{"amount_minor": float64(3000000)},
	}
	if err := alert.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	alert.Severity = "LOW"
	if err := alert.ValidateAt(now); err == nil {
		t.Fatal("expected unsupported severity to fail")
	}
}

func TestAnomalyAlertJSONRoundTrip(t *testing.T) {
	alert := AnomalyAlert{
		TransactionID: uuid.NewString(),
		AccountID:     "account-001",
		FlaggedAt:     time.Now().UTC().Truncate(time.Second),
		Reason:        AnomalyReasonVelocity,
		Severity:      SeverityMedium,
		SnapshotData:  map[string]any{"count": float64(6)},
	}
	data, err := json.Marshal(alert)
	if err != nil {
		t.Fatal(err)
	}
	var decoded AnomalyAlert
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TransactionID != alert.TransactionID || decoded.Reason != alert.Reason ||
		decoded.Severity != alert.Severity {
		t.Fatalf("round trip mismatch: got %#v, want %#v", decoded, alert)
	}
}
