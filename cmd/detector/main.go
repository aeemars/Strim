package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/aeemars/Strim/internal/config"
	"github.com/aeemars/Strim/internal/detector"
	strimkafka "github.com/aeemars/Strim/internal/kafka"
	"github.com/aeemars/Strim/internal/telemetry"
)

func main() {
	handler := &telemetry.RedactingHandler{
		Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	}
	slog.SetDefault(slog.New(handler))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	brokers := splitBrokers(cfg.KafkaBrokers)
	redisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer redisClient.Close()
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		slog.Error("connect to Redis", "error", err)
		os.Exit(1)
	}

	engine := &detector.Engine{
		Rules: &detector.RuleEngine{
			Velocity: detector.NewVelocityTracker(redisClient),
			Geo:      redisClient,
		},
		Publisher: &segmentio.Writer{
			Addr:         segmentio.TCP(brokers...),
			Balancer:     &segmentio.Hash{},
			RequiredAcks: segmentio.RequireAll,
			MaxAttempts:  5,
			BatchSize:    100,
			Compression:  segmentio.Snappy,
			WriteTimeout: 10 * time.Second,
		},
		ClearedTopic: "transactions.cleared",
		FlaggedTopic: "transactions.flagged",
	}
	if err := engine.Validate(); err != nil {
		slog.Error("validate detector", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	consumer, err := strimkafka.NewConsumer(
		brokers,
		"fraud-detector-service",
		"transactions.raw",
		engine.Process,
	)
	if err != nil {
		slog.Error("create detector consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("Strim detector started",
		"kafka_brokers", cfg.KafkaBrokers,
		"redis_addr", cfg.RedisAddr,
		"group_id", "fraud-detector-service")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("detector stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("Strim detector stopped")
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
