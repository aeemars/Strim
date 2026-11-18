package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type fakeReader struct {
	messages       []segmentio.Message
	fetchErr       error
	commitErr      error
	committed      []segmentio.Message
	closed         bool
	cancelOnCommit context.CancelFunc
}

func (r *fakeReader) FetchMessage(_ context.Context) (segmentio.Message, error) {
	if r.fetchErr != nil {
		return segmentio.Message{}, r.fetchErr
	}
	if len(r.messages) == 0 {
		return segmentio.Message{}, errors.New("no test messages")
	}
	message := r.messages[0]
	r.messages = r.messages[1:]
	return message, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...segmentio.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, messages...)
	if r.cancelOnCommit != nil {
		r.cancelOnCommit()
	}
	return nil
}

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

func TestConsumerCommitsSuccessfulProcessing(t *testing.T) {
	reader := &fakeReader{messages: []segmentio.Message{{
		Topic: "transactions.raw", Partition: 1, Offset: 42, Value: []byte(`{"valid":true}`),
	}}}
	consumer := &Consumer{
		Reader: reader,
		Processor: func(context.Context, segmentio.Message) error {
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader.cancelOnCommit = cancel
	err := consumer.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want context canceled", err)
	}
	if len(reader.committed) != 1 {
		t.Fatalf("got %d committed offsets, want 1", len(reader.committed))
	}
}

func TestConsumerRoutesFailureToDLQBeforeCommit(t *testing.T) {
	reader := &fakeReader{messages: []segmentio.Message{{
		Topic: "transactions.raw", Partition: 0, Offset: 7,
		Value: []byte(`{"broken":true}`), Key: []byte("account-001"),
	}}}
	publisher := &recordingPublisher{}
	dispatcher := &DLQDispatcher{
		Topic: "transactions.dlq", Publisher: publisher,
		Attempts: 1,
	}
	consumer := &Consumer{
		Reader: reader,
		DLQ:    dispatcher,
		Processor: func(context.Context, segmentio.Message) error {
			return errors.New("invalid transaction")
		},
		Timeout: time.Second,
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	reader.cancelOnCommit = runCancel
	err := consumer.Run(runCtx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("got %d DLQ messages, want 1", len(publisher.messages))
	}
	var envelope domain.DLQEnvelope
	if err := json.Unmarshal(publisher.messages[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.OriginalOffset != 7 {
		t.Fatalf("got offset %d, want 7", envelope.OriginalOffset)
	}
	if len(reader.committed) != 1 {
		t.Fatalf("got %d committed offsets, want 1", len(reader.committed))
	}
}

func TestConsumerDoesNotCommitWhenDLQFails(t *testing.T) {
	reader := &fakeReader{messages: []segmentio.Message{{
		Topic: "transactions.raw", Partition: 0, Offset: 9, Value: []byte(`{bad}`),
	}}}
	dispatcher := &DLQDispatcher{
		Topic:     "transactions.dlq",
		Publisher: &recordingPublisher{err: errors.New("DLQ unavailable")},
		Attempts:  1,
	}
	consumer := &Consumer{
		Reader: reader,
		DLQ:    dispatcher,
		Processor: func(context.Context, segmentio.Message) error {
			return errors.New("malformed payload")
		},
	}
	err := consumer.Run(context.Background())
	if err == nil {
		t.Fatal("expected DLQ failure")
	}
	if len(reader.committed) != 0 {
		t.Fatal("source offset must not be committed when DLQ publishing fails")
	}
}

func TestConsumerReturnsCommitFailure(t *testing.T) {
	reader := &fakeReader{
		messages: []segmentio.Message{{
			Topic: "transactions.raw", Partition: 2, Offset: 12,
		}},
		commitErr: errors.New("commit unavailable"),
	}
	consumer := &Consumer{
		Reader: reader,
		Processor: func(context.Context, segmentio.Message) error {
			return nil
		},
	}
	err := consumer.Run(context.Background())
	if err == nil {
		t.Fatal("expected offset commit failure")
	}
	if len(reader.committed) != 0 {
		t.Fatal("failed commit must not be recorded as successful")
	}
}

func TestDLQDispatcherLiveKafka(t *testing.T) {
	if os.Getenv("STRIM_LIVE_KAFKA") != "1" {
		t.Skip("set STRIM_LIVE_KAFKA=1 to run against Kafka")
	}
	dispatcher, err := NewDLQDispatcher([]string{"localhost:9092"}, "transactions.dlq")
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	message := segmentio.Message{
		Topic:     "transactions.raw",
		Partition: 0,
		Offset:    123456,
		Key:       []byte("account-001"),
		Value:     []byte(`{"invalid":true}`),
		Headers: []segmentio.Header{
			{Key: "event_id", Value: []byte("phase4-live-test")},
		},
	}
	if err := dispatcher.Dispatch(ctx, message, errors.New("invalid transaction")); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedError(t *testing.T) {
	text := boundedError(errors.New("x" + string(make([]byte, 2048))))
	if len(text) > 1024 {
		t.Fatalf("bounded error is %d bytes", len(text))
	}
}
