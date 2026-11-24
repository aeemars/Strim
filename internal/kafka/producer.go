package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type Publisher interface {
	WriteMessages(context.Context, ...segmentio.Message) error
	Close() error
}

type Producer struct {
	Topic     string
	Publisher Publisher
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("Kafka topic must not be empty")
	}

	writer := &segmentio.Writer{
		Addr:            segmentio.TCP(brokers...),
		Topic:           topic,
		Balancer:        &segmentio.Hash{},
		RequiredAcks:    segmentio.RequireAll,
		MaxAttempts:     5,
		WriteBackoffMin: 100 * time.Millisecond,
		WriteBackoffMax: 5 * time.Second,
		BatchSize:       100,
		BatchBytes:             1 << 20,
		Compression:            segmentio.Snappy,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		AllowAutoTopicCreation: true,
	}
	return &Producer{Topic: topic, Publisher: writer}, nil
}

func (p *Producer) Publish(ctx context.Context, transaction domain.Transaction) error {
	if err := transaction.Validate(); err != nil {
		return fmt.Errorf("validate transaction: %w", err)
	}
	payload, err := json.Marshal(transaction)
	if err != nil {
		return fmt.Errorf("marshal transaction: %w", err)
	}
	return p.PublishRaw(ctx, []byte(transaction.AccountID), payload)
}

func (p *Producer) PublishRaw(ctx context.Context, key, payload []byte) error {
	message := segmentio.Message{
		Key:   key,
		Value: payload,
		Headers: []segmentio.Header{
			{Key: "schema", Value: []byte("transaction.v1")},
		},
		Time: time.Now().UTC(),
	}
	if err := p.Publisher.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish to %s: %w", p.Topic, err)
	}
	return nil
}

func (p *Producer) Close() error {
	if p.Publisher == nil {
		return nil
	}
	return p.Publisher.Close()
}
