package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type DLQDispatcher struct {
	Topic     string
	Publisher Publisher
	Attempts  int
	Backoff   time.Duration
}

func NewDLQDispatcher(brokers []string, topic string) (*DLQDispatcher, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("DLQ topic must not be empty")
	}
	writer := &segmentio.Writer{
		Addr:         segmentio.TCP(brokers...),
		Topic:        topic,
		Balancer:     &segmentio.Hash{},
		RequiredAcks: segmentio.RequireAll,
		MaxAttempts:  5,
		Compression:  segmentio.Snappy,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	return &DLQDispatcher{
		Topic:     topic,
		Publisher: writer,
		Attempts:  3,
		Backoff:   100 * time.Millisecond,
	}, nil
}

func (d *DLQDispatcher) Dispatch(ctx context.Context, message segmentio.Message, processErr error) error {
	errorText := boundedError(processErr)
	envelope := domain.DLQEnvelope{
		OriginalTopic:     message.Topic,
		OriginalPartition: message.Partition,
		OriginalOffset:    message.Offset,
		OriginalKey:       string(message.Key),
		OriginalPayload:   message.Value,
		ErrorMessage:      errorText,
		FailedAt:          time.Now().UTC(),
	}
	if message.Headers != nil {
		for _, header := range message.Headers {
			if header.Key == "event_id" {
				envelope.EventID = string(header.Value)
				break
			}
		}
	}
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("validate DLQ envelope: %w", err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal DLQ envelope: %w", err)
	}

	attempts := d.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	backoff := d.Backoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}
	var publishErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		publishErr = d.Publisher.WriteMessages(ctx, segmentio.Message{
			Key:   message.Key,
			Value: payload,
			Headers: []segmentio.Header{
				{Key: "schema", Value: []byte("dlq.v1")},
			},
			Time: time.Now().UTC(),
		})
		if publishErr == nil {
			return nil
		}
		if attempt < attempts {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff *= 2
		}
	}
	return fmt.Errorf("publish DLQ envelope after %d attempts: %w", attempts, publishErr)
}

func (d *DLQDispatcher) Close() error {
	if d.Publisher == nil {
		return nil
	}
	return d.Publisher.Close()
}

func boundedError(err error) string {
	if err == nil {
		return "unclassified processing failure"
	}
	const maxLength = 1024
	text := strings.TrimSpace(err.Error())
	if len(text) > maxLength {
		text = text[:maxLength]
	}
	return text
}
