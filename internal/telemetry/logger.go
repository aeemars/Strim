package telemetry

import (
	"context"
	"log/slog"
	"strings"
)

type contextKey struct{}

type LogFields struct {
	TraceID       string
	AccountID     string
	TransactionID string
	Partition     int
	Offset        int64
}

// WithContext adds tracing and transaction correlation identifiers to a context.
func WithContext(ctx context.Context, fields LogFields) context.Context {
	return context.WithValue(ctx, contextKey{}, fields)
}

// FieldsFromContext retrieves tracing fields from a context.
func FieldsFromContext(ctx context.Context) (LogFields, bool) {
	fields, ok := ctx.Value(contextKey{}).(LogFields)
	return fields, ok
}

// ContextAttrs returns slog attributes corresponding to correlation fields in the context.
func ContextAttrs(ctx context.Context) []any {
	fields, ok := FieldsFromContext(ctx)
	if !ok {
		return nil
	}
	attrs := make([]any, 0, 5)
	if fields.TraceID != "" {
		attrs = append(attrs, "trace_id", fields.TraceID)
	}
	if fields.AccountID != "" {
		attrs = append(attrs, "account_id", fields.AccountID)
	}
	if fields.TransactionID != "" {
		attrs = append(attrs, "transaction_id", fields.TransactionID)
	}
	if fields.Partition >= 0 {
		attrs = append(attrs, "partition", fields.Partition)
	}
	if fields.Offset >= 0 {
		attrs = append(attrs, "offset", fields.Offset)
	}
	return attrs
}

// RedactingHandler wraps an slog.Handler to sanitize sensitive keys before writing.
type RedactingHandler struct {
	Handler slog.Handler
}

var sensitiveKeys = map[string]struct{}{
	"password":      {},
	"secret":        {},
	"token":         {},
	"authorization": {},
	"api_key":       {},
	"database_url":  {},
	"postgres_pwd":  {},
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.Handler.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, record slog.Record) error {
	var newAttrs []slog.Attr
	record.Attrs(func(attr slog.Attr) bool {
		newAttrs = append(newAttrs, sanitizeAttr(attr))
		return true
	})

	// Inject correlation fields from context if present
	if fields, ok := FieldsFromContext(ctx); ok {
		if fields.TraceID != "" {
			newAttrs = append(newAttrs, slog.String("trace_id", fields.TraceID))
		}
		if fields.TransactionID != "" {
			newAttrs = append(newAttrs, slog.String("transaction_id", fields.TransactionID))
		}
	}

	sanitizedRecord := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	sanitizedRecord.AddAttrs(newAttrs...)
	return h.Handler.Handle(ctx, sanitizedRecord)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	sanitized := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		sanitized[i] = sanitizeAttr(a)
	}
	return &RedactingHandler{Handler: h.Handler.WithAttrs(sanitized)}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{Handler: h.Handler.WithGroup(name)}
}

func sanitizeAttr(attr slog.Attr) slog.Attr {
	keyLower := strings.ToLower(attr.Key)
	if _, sensitive := sensitiveKeys[keyLower]; sensitive {
		return slog.String(attr.Key, "[REDACTED]")
	}
	// Check substring for credentials
	if strings.Contains(keyLower, "password") || strings.Contains(keyLower, "secret") {
		return slog.String(attr.Key, "[REDACTED]")
	}
	return attr
}
