package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDLQEnvelopeValidation(t *testing.T) {
	envelope := DLQEnvelope{
		OriginalTopic:     "transactions.raw",
		OriginalPartition: 1,
		OriginalOffset:    42,
		OriginalPayload:   []byte(`{"broken":true}`),
		ErrorMessage:      "missing amount_minor",
		FailedAt:          time.Now().UTC(),
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	envelope.OriginalOffset = -1
	if err := envelope.Validate(); err == nil {
		t.Fatal("expected negative offset to fail")
	}
}

func TestDLQEnvelopeJSONRoundTrip(t *testing.T) {
	envelope := DLQEnvelope{
		OriginalTopic:     "transactions.raw",
		OriginalPartition: 0,
		OriginalOffset:    7,
		OriginalPayload:   []byte(`{"invalid":true}`),
		ErrorMessage:      "invalid payload",
		FailedAt:          time.Now().UTC().Truncate(time.Second),
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var decoded DLQEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.OriginalPayload) != string(envelope.OriginalPayload) ||
		decoded.OriginalOffset != envelope.OriginalOffset {
		t.Fatalf("round trip mismatch: got %#v, want %#v", decoded, envelope)
	}
}
