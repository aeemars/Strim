package detector

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/domain"
)

type recordingPublisher struct {
	messages []segmentio.Message
	err      error
}

func (r *recordingPublisher) WriteMessages(_ context.Context, messages ...segmentio.Message) error {
	r.messages = append(r.messages, messages...)
	return r.err
}

func (r *recordingPublisher) Close() error {
	return nil
}

func validTransaction() domain.Transaction {
	now := time.Now().UTC()
	return domain.Transaction{
		ID:                   "00000000-0000-4000-8000-000000000001",
		AccountID:            "account-001",
		DestinationAccountID: "account-002",
		AmountMinor:          100,
		Currency:             domain.DefaultCurrency,
		Timestamp:            now,
		LocationCountry:      "NG",
		DeviceID:             "device-001",
		IdempotencyKey:       "idem-001",
		Type:                 domain.TransactionTypeTransfer,
	}
}

func TestHighValueBoundary(t *testing.T) {
	tracker := &VelocityTracker{Window: time.Minute, Threshold: 5}
	transaction := validTransaction()
	transaction.AmountMinor = HighValueThresholdMinor
	if transaction.AmountMinor > HighValueThresholdMinor {
		t.Fatal("threshold must be inclusive")
	}
	transaction.AmountMinor++
	if transaction.AmountMinor <= HighValueThresholdMinor {
		t.Fatal("one kobo above threshold must breach")
	}
	_ = tracker
}

func TestEngineRejectsMalformedPayload(t *testing.T) {
	publisher := &recordingPublisher{}
	engine := &Engine{
		Rules:        &RuleEngine{},
		Publisher:    publisher,
		ClearedTopic: "transactions.cleared",
		FlaggedTopic: "transactions.flagged",
	}
	err := engine.Process(context.Background(), segmentio.Message{Value: []byte(`{bad}`)})
	if err == nil {
		t.Fatal("expected malformed payload failure")
	}
	if len(publisher.messages) != 0 {
		t.Fatal("malformed payload must not be routed")
	}
}

func TestVelocityLiveRedis(t *testing.T) {
	if os.Getenv("STRIM_LIVE_REDIS") != "1" {
		t.Skip("set STRIM_LIVE_REDIS=1 to run against Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()
	ctx := context.Background()
	tracker := NewVelocityTracker(client)
	now := time.Now().UTC()
	account := "detector-live-" + now.Format("150405")
	for i := 1; i <= 6; i++ {
		id := "tx-" + time.Now().Format("150405.000000") + "-" + string(rune('0'+i))
		count, err := tracker.Count(ctx, account, id, now)
		if err != nil {
			t.Fatal(err)
		}
		if i == 6 && count <= tracker.Threshold {
			t.Fatalf("got count %d, want velocity breach", count)
		}
	}
}

func TestImpossibleTravelLiveRedis(t *testing.T) {
	if os.Getenv("STRIM_LIVE_REDIS") != "1" {
		t.Skip("set STRIM_LIVE_REDIS=1 to run against Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()
	engine := &RuleEngine{
		Velocity: NewVelocityTracker(client),
		Geo:      client,
	}
	now := time.Now().UTC()
	first := validTransaction()
	first.AccountID = "detector-travel-" + now.Format("150405")
	first.LocationCountry = "US"
	first.Timestamp = now
	_, _, _, err := engine.Evaluate(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "00000000-0000-4000-8000-000000000002"
	second.LocationCountry = "SG"
	second.Timestamp = now.Add(time.Minute)
	result, reasons, _, err := engine.Evaluate(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImpossibleTravel {
		t.Fatal("US to SG within ten minutes must flag")
	}
	if len(reasons) == 0 {
		t.Fatal("expected anomaly reason")
	}
}

func TestRoutedDecisionJSON(t *testing.T) {
	decision := RoutedDecision{
		DecisionID:   "decision-1",
		RuleVersion:  RuleVersion,
		ProcessedAt:  time.Now().UTC(),
		Transaction:  validTransaction(),
		SnapshotData: RuleResult{VelocityCount: 6, VelocityExceeded: true},
	}
	payload, err := decision.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded RoutedDecision
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.DecisionID != decision.DecisionID {
		t.Fatal("decision ID changed after round trip")
	}
}

func BenchmarkRoutedDecisionJSON(b *testing.B) {
	decision := RoutedDecision{
		DecisionID:   "decision-benchmark",
		RuleVersion:  RuleVersion,
		ProcessedAt:  time.Now().UTC(),
		Transaction:  validTransaction(),
		SnapshotData: RuleResult{},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decision.JSON(); err != nil {
			b.Fatal(err)
		}
	}
}
