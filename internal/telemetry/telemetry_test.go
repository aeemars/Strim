package telemetry_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aeemars/Strim/internal/telemetry"
)

func TestRedactingLogger(t *testing.T) {
	var buf bytes.Buffer
	innerHandler := slog.NewJSONHandler(&buf, nil)
	handler := &telemetry.RedactingHandler{Handler: innerHandler}
	logger := slog.New(handler)

	ctx := telemetry.WithContext(context.Background(), telemetry.LogFields{
		TraceID:       "trace-12345",
		TransactionID: "tx-67890",
		Partition:     2,
		Offset:        42,
	})

	logger.InfoContext(ctx, "processing transaction",
		slog.String("account_id", "acc-001"),
		slog.String("password", "super-secret-pwd"),
		slog.String("api_key", "key-999"),
		slog.String("database_url", "postgres://user:pass@host/db"),
	)

	output := buf.String()
	if strings.Contains(output, "super-secret-pwd") {
		t.Errorf("expected password to be redacted, got: %s", output)
	}
	if strings.Contains(output, "key-999") {
		t.Errorf("expected api_key to be redacted, got: %s", output)
	}
	if strings.Contains(output, "user:pass") {
		t.Errorf("expected database_url to be redacted, got: %s", output)
	}
	if !strings.Contains(output, "[REDACTED]") {
		t.Errorf("expected [REDACTED] in output, got: %s", output)
	}
	if !strings.Contains(output, "trace-12345") {
		t.Errorf("expected trace_id in output, got: %s", output)
	}
}

func TestMetricsRegistry(t *testing.T) {
	reg := telemetry.NewMetricsRegistry()

	reg.IncProcessed()
	reg.IncProcessed()
	reg.IncFlagged()
	reg.IncCleared()
	reg.IncDLQ()
	reg.IncDuplicates()
	reg.ObserveLatency(10 * time.Millisecond)
	reg.ObserveLatency(30 * time.Millisecond)

	snap := reg.Snapshot()
	if snap.Processed != 2 {
		t.Errorf("expected 2 processed, got %d", snap.Processed)
	}
	if snap.Flagged != 1 {
		t.Errorf("expected 1 flagged, got %d", snap.Flagged)
	}
	if snap.Cleared != 1 {
		t.Errorf("expected 1 cleared, got %d", snap.Cleared)
	}
	if snap.DLQ != 1 {
		t.Errorf("expected 1 DLQ, got %d", snap.DLQ)
	}
	if snap.Duplicates != 1 {
		t.Errorf("expected 1 duplicate, got %d", snap.Duplicates)
	}
	if snap.AvgLatency != 20*time.Millisecond {
		t.Errorf("expected 20ms avg latency, got %v", snap.AvgLatency)
	}
	if snap.MaxLatency != 30*time.Millisecond {
		t.Errorf("expected 30ms max latency, got %v", snap.MaxLatency)
	}

	prom := reg.FormatPrometheus()
	if !strings.Contains(prom, "strim_transactions_processed_total 2") {
		t.Errorf("missing processed count in prometheus output: %s", prom)
	}
}
