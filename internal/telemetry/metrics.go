package telemetry

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MetricsRegistry struct {
	TotalProcessed  atomic.Uint64
	TotalFlagged    atomic.Uint64
	TotalCleared    atomic.Uint64
	TotalDLQ        atomic.Uint64
	TotalDuplicates atomic.Uint64
	TotalErrors     atomic.Uint64

	mu           sync.Mutex
	latencyCount uint64
	latencySum   time.Duration
	latencyMax   time.Duration
}

var DefaultMetrics = NewMetricsRegistry()

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{}
}

func (m *MetricsRegistry) IncProcessed() {
	m.TotalProcessed.Add(1)
}

func (m *MetricsRegistry) IncFlagged() {
	m.TotalFlagged.Add(1)
}

func (m *MetricsRegistry) IncCleared() {
	m.TotalCleared.Add(1)
}

func (m *MetricsRegistry) IncDLQ() {
	m.TotalDLQ.Add(1)
}

func (m *MetricsRegistry) IncDuplicates() {
	m.TotalDuplicates.Add(1)
}

func (m *MetricsRegistry) IncErrors() {
	m.TotalErrors.Add(1)
}

func (m *MetricsRegistry) ObserveLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencyCount++
	m.latencySum += d
	if d > m.latencyMax {
		m.latencyMax = d
	}
}

type Snapshot struct {
	Processed  uint64        `json:"processed"`
	Flagged    uint64        `json:"flagged"`
	Cleared    uint64        `json:"cleared"`
	DLQ        uint64        `json:"dlq"`
	Duplicates uint64        `json:"duplicates"`
	Errors     uint64        `json:"errors"`
	AvgLatency time.Duration `json:"avg_latency"`
	MaxLatency time.Duration `json:"max_latency"`
}

func (m *MetricsRegistry) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	var avg time.Duration
	if m.latencyCount > 0 {
		avg = m.latencySum / time.Duration(m.latencyCount)
	}

	return Snapshot{
		Processed:  m.TotalProcessed.Load(),
		Flagged:    m.TotalFlagged.Load(),
		Cleared:    m.TotalCleared.Load(),
		DLQ:        m.TotalDLQ.Load(),
		Duplicates: m.TotalDuplicates.Load(),
		Errors:     m.TotalErrors.Load(),
		AvgLatency: avg,
		MaxLatency: m.latencyMax,
	}
}

// FormatPrometheus generates Prometheus text exposition format.
func (m *MetricsRegistry) FormatPrometheus() string {
	snap := m.Snapshot()
	var b strings.Builder
	b.WriteString("# HELP strim_transactions_processed_total Total transactions processed\n")
	b.WriteString("# TYPE strim_transactions_processed_total counter\n")
	b.WriteString(fmt.Sprintf("strim_transactions_processed_total %d\n", snap.Processed))

	b.WriteString("# HELP strim_transactions_flagged_total Total transactions flagged for anomaly\n")
	b.WriteString("# TYPE strim_transactions_flagged_total counter\n")
	b.WriteString(fmt.Sprintf("strim_transactions_flagged_total %d\n", snap.Flagged))

	b.WriteString("# HELP strim_transactions_cleared_total Total clean transactions forwarded to ledger\n")
	b.WriteString("# TYPE strim_transactions_cleared_total counter\n")
	b.WriteString(fmt.Sprintf("strim_transactions_cleared_total %d\n", snap.Cleared))

	b.WriteString("# HELP strim_transactions_dlq_total Total failed messages diverted to dead-letter queue\n")
	b.WriteString("# TYPE strim_transactions_dlq_total counter\n")
	b.WriteString(fmt.Sprintf("strim_transactions_dlq_total %d\n", snap.DLQ))

	b.WriteString("# HELP strim_transactions_duplicates_total Total duplicate redeliveries safely skipped\n")
	b.WriteString("# TYPE strim_transactions_duplicates_total counter\n")
	b.WriteString(fmt.Sprintf("strim_transactions_duplicates_total %d\n", snap.Duplicates))

	b.WriteString("# HELP strim_processing_latency_seconds Average processing latency\n")
	b.WriteString("# TYPE strim_processing_latency_seconds gauge\n")
	b.WriteString(fmt.Sprintf("strim_processing_latency_seconds %.6f\n", snap.AvgLatency.Seconds()))

	return b.String()
}
