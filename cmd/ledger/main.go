package main

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aeemars/Strim/internal/config"
	strimkafka "github.com/aeemars/Strim/internal/kafka"
	"github.com/aeemars/Strim/internal/ledger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		slog.Error("parse database url", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = 1 * time.Hour
	poolConfig.MaxConnIdleTime = 15 * time.Minute

	dbPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		slog.Error("connect to database pool", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		slog.Error("ping postgres", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to PostgreSQL", "database_url", maskURL(cfg.DatabaseURL))

	processor := ledger.NewProcessor(dbPool)
	brokers := splitBrokers(cfg.KafkaBrokers)

	consumer, err := strimkafka.NewConsumer(
		brokers,
		"ledger-service",
		"transactions.cleared",
		processor.Process,
	)
	if err != nil {
		slog.Error("create ledger consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	slog.Info("Strim ledger service started",
		"kafka_brokers", cfg.KafkaBrokers,
		"topic", "transactions.cleared",
		"group_id", "ledger-service")

	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("ledger service stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("Strim ledger service stopped")
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func maskURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "[redacted-database-url]"
	}
	return parsed.Redacted()
}
