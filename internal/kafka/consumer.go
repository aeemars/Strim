package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

type Reader interface {
	FetchMessage(context.Context) (segmentio.Message, error)
	CommitMessages(context.Context, ...segmentio.Message) error
	Close() error
}

type Processor func(context.Context, segmentio.Message) error

type Consumer struct {
	Reader    Reader
	Processor Processor
	DLQ       *DLQDispatcher
	Timeout   time.Duration
}

func NewConsumer(brokers []string, groupID, topic string, processor Processor) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if strings.TrimSpace(groupID) == "" {
		return nil, errors.New("consumer group ID must not be empty")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("consumer topic must not be empty")
	}
	if processor == nil {
		return nil, errors.New("message processor must not be nil")
	}

	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     250 * time.Millisecond,
		StartOffset: segmentio.FirstOffset,
	})
	dlq, err := NewDLQDispatcher(brokers, "transactions.dlq")
	if err != nil {
		return nil, err
	}
	return &Consumer{
		Reader:    reader,
		Processor: processor,
		DLQ:       dlq,
		Timeout:   10 * time.Second,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.Reader == nil {
		return errors.New("Kafka reader is nil")
	}
	for {
		message, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("fetch Kafka message: %w", err)
		}

		timeout := c.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		processCtx, cancel := context.WithTimeout(ctx, timeout)
		processErr := c.Processor(processCtx, message)
		cancel()

		if processErr == nil {
			if err := c.Reader.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit offset topic=%s partition=%d offset=%d: %w",
					message.Topic, message.Partition, message.Offset, err)
			}
			continue
		}

		slog.Error("message processing failed",
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"error", processErr)

		if c.DLQ == nil {
			return fmt.Errorf("dispatch failed message: DLQ dispatcher is nil")
		}
		if err := c.DLQ.Dispatch(ctx, message, processErr); err != nil {
			return fmt.Errorf("dispatch message to DLQ: %w", err)
		}
		if err := c.Reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit DLQ-diverted offset topic=%s partition=%d offset=%d: %w",
				message.Topic, message.Partition, message.Offset, err)
		}
	}
}

func (c *Consumer) Close() error {
	var readerErr, dlqErr error
	if c.Reader != nil {
		readerErr = c.Reader.Close()
	}
	if c.DLQ != nil {
		dlqErr = c.DLQ.Close()
	}
	if readerErr != nil {
		return readerErr
	}
	return dlqErr
}
