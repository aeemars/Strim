package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	AnomalyReasonVelocity         = "VELOCITY_LIMIT_EXCEEDED"
	AnomalyReasonHighValue        = "HIGH_VALUE_THRESHOLD"
	AnomalyReasonImpossibleTravel = "IMPOSSIBLE_TRAVEL"

	SeverityMedium   = "MEDIUM"
	SeverityHigh     = "HIGH"
	SeverityCritical = "CRITICAL"
)

type AnomalyAlert struct {
	TransactionID string         `json:"transaction_id"`
	AccountID     string         `json:"account_id"`
	FlaggedAt     time.Time      `json:"flagged_at"`
	Reason        string         `json:"reason"`
	Severity      string         `json:"severity"`
	SnapshotData  map[string]any `json:"snapshot_data,omitempty"`
}

func (a AnomalyAlert) ValidateAt(now time.Time) error {
	if _, err := uuid.Parse(a.TransactionID); err != nil {
		return fmt.Errorf("transaction_id must be a UUID: %w", err)
	}
	if err := validateIdentifier("account_id", a.AccountID); err != nil {
		return err
	}
	if a.FlaggedAt.Location() != time.UTC {
		return errors.New("flagged_at must use UTC")
	}
	if a.FlaggedAt.After(now.Add(5 * time.Minute)) {
		return errors.New("flagged_at cannot be more than five minutes in the future")
	}
	switch a.Reason {
	case AnomalyReasonVelocity, AnomalyReasonHighValue, AnomalyReasonImpossibleTravel:
	default:
		return fmt.Errorf("unsupported anomaly reason %q", a.Reason)
	}
	switch a.Severity {
	case SeverityMedium, SeverityHigh, SeverityCritical:
	default:
		return fmt.Errorf("unsupported severity %q", a.Severity)
	}
	return nil
}

func (a AnomalyAlert) Validate() error {
	return a.ValidateAt(time.Now().UTC())
}
