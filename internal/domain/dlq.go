package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type DLQEnvelope struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalKey       string    `json:"original_key,omitempty"`
	OriginalPayload   []byte    `json:"original_payload"`
	EventID           string    `json:"event_id,omitempty"`
	ErrorMessage      string    `json:"error_message"`
	FailedAt          time.Time `json:"failed_at"`
}

func (d DLQEnvelope) Validate() error {
	if strings.TrimSpace(d.OriginalTopic) == "" {
		return errors.New("original_topic must be non-empty")
	}
	if d.OriginalPartition < 0 {
		return errors.New("original_partition cannot be negative")
	}
	if d.OriginalOffset < 0 {
		return errors.New("original_offset cannot be negative")
	}
	if len(d.OriginalPayload) == 0 {
		return errors.New("original_payload must be non-empty")
	}
	if strings.TrimSpace(d.ErrorMessage) == "" {
		return errors.New("error_message must be non-empty")
	}
	if d.FailedAt.Location() != time.UTC {
		return fmt.Errorf("failed_at must use UTC")
	}
	return nil
}
