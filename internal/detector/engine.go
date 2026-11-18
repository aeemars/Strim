package detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type MessagePublisher interface {
	WriteMessages(context.Context, ...segmentio.Message) error
	Close() error
}

type Engine struct {
	Rules        *RuleEngine
	Publisher    MessagePublisher
	ClearedTopic string
	FlaggedTopic string
}

func (e *Engine) Process(ctx context.Context, message segmentio.Message) error {
	var transaction domain.Transaction
	if err := json.Unmarshal(message.Value, &transaction); err != nil {
		return fmt.Errorf("decode transaction: %w", err)
	}
	if err := transaction.Validate(); err != nil {
		return fmt.Errorf("validate transaction: %w", err)
	}

	result, reasons, severity, err := e.Rules.Evaluate(ctx, transaction)
	if err != nil {
		return err
	}
	decision := RoutedDecision{
		DecisionID:    uuid.NewString(),
		RuleVersion:   RuleVersion,
		ProcessedAt:   time.Now().UTC(),
		Transaction:   transaction,
		AlertReasons:  reasons,
		AlertSeverity: severity,
		SnapshotData:  result,
	}
	payload, err := decision.JSON()
	if err != nil {
		return fmt.Errorf("marshal detector decision: %w", err)
	}
	topic := e.ClearedTopic
	if len(reasons) > 0 {
		topic = e.FlaggedTopic
	}
	err = e.Publisher.WriteMessages(ctx, segmentio.Message{
		Topic: topic,
		Key:   []byte(transaction.AccountID),
		Value: payload,
		Headers: []segmentio.Header{
			{Key: "schema", Value: []byte("decision.v1")},
			{Key: "decision_id", Value: []byte(decision.DecisionID)},
		},
		Time: time.Now().UTC(),
	})
	if err != nil {
		slog.Error("publish detector decision", "topic", topic, "transaction_id", transaction.ID, "error", err)
		return fmt.Errorf("publish detector decision to %s: %w", topic, err)
	}
	return nil
}

func (e *Engine) Validate() error {
	if e.Rules == nil {
		return errors.New("detector rules are nil")
	}
	if e.Publisher == nil {
		return errors.New("detector publisher is nil")
	}
	if e.ClearedTopic == "" || e.FlaggedTopic == "" {
		return errors.New("detector output topics must be configured")
	}
	return nil
}

func (e *Engine) Close() error {
	if e.Publisher == nil {
		return nil
	}
	return e.Publisher.Close()
}
