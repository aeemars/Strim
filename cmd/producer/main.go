package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/aeemars/Strim/internal/config"
	"github.com/aeemars/Strim/internal/domain"
	strimkafka "github.com/aeemars/Strim/internal/kafka"
)

func main() {
	rate := flag.Float64("rate", 10, "normal transactions per second")
	burst := flag.Int("burst", 12, "number of transactions in a velocity burst")
	poison := flag.Float64("poison", 0.001, "poison-pill probability from 0 to 1")
	scenario := flag.String("scenario", "", "optional scenario: velocity, high-value, impossible-travel")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	if *rate <= 0 || *poison < 0 || *poison > 1 || *burst <= 0 {
		slog.Error("invalid producer arguments")
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	brokers := splitBrokers(cfg.KafkaBrokers)
	producer, err := strimkafka.NewProducer(brokers, "transactions.raw")
	if err != nil {
		slog.Error("create producer", "error", err)
		os.Exit(1)
	}
	defer closeProducer(producer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	random := rand.New(rand.NewSource(*seed))

	slog.Info("Strim producer started",
		"kafka_brokers", cfg.KafkaBrokers,
		"topic", "transactions.raw",
		"rate", *rate,
		"scenario", *scenario)

	if err := runScenario(ctx, producer, random, *scenario, *burst); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("producer scenario failed", "error", err)
		os.Exit(1)
	}
	if strings.TrimSpace(*scenario) != "" {
		return
	}
	runNormalTraffic(ctx, producer, random, *rate, *poison, *burst)
}

func runNormalTraffic(ctx context.Context, producer *strimkafka.Producer, random *rand.Rand, rate, poison float64, burst int) {
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if random.Float64() < poison {
				if err := producer.PublishRaw(ctx, []byte("poison-pill"), []byte(`{"not":"a_transaction"}`)); err != nil {
					slog.Error("publish poison pill", "error", err)
					return
				}
				continue
			}
			transaction := normalTransaction(random)
			if err := producer.Publish(ctx, transaction); err != nil {
				slog.Error("publish transaction", "error", err, "transaction_id", transaction.ID)
				return
			}
		}
	}
}

func runScenario(ctx context.Context, producer *strimkafka.Producer, random *rand.Rand, name string, burst int) error {
	switch name {
	case "":
		return nil
	case "velocity":
		accountID := fmt.Sprintf("account-%03d", random.Intn(100)+1)
		for i := 0; i < burst; i++ {
			transaction := normalTransaction(random)
			transaction.AccountID = accountID
			transaction.Timestamp = time.Now().UTC()
			if err := producer.Publish(ctx, transaction); err != nil {
				return err
			}
		}
	case "high-value":
		transaction := normalTransaction(random)
		transaction.AmountMinor = 2_500_001
		return publishIfContextActive(ctx, producer, transaction)
	case "impossible-travel":
		accountID := fmt.Sprintf("account-%03d", random.Intn(100)+1)
		for _, country := range []string{"US", "SG"} {
			transaction := normalTransaction(random)
			transaction.AccountID = accountID
			transaction.LocationCountry = country
			transaction.Timestamp = time.Now().UTC()
			if err := producer.Publish(ctx, transaction); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown scenario %q", name)
	}
	return ctx.Err()
}

func publishIfContextActive(ctx context.Context, producer *strimkafka.Producer, transaction domain.Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return producer.Publish(ctx, transaction)
}

func normalTransaction(random *rand.Rand) domain.Transaction {
	now := time.Now().UTC()
	sender := random.Intn(100) + 1
	destination := random.Intn(100) + 1
	for destination == sender {
		destination = random.Intn(100) + 1
	}
	return domain.Transaction{
		ID:                   uuid.NewString(),
		AccountID:            fmt.Sprintf("account-%03d", sender),
		DestinationAccountID: fmt.Sprintf("account-%03d", destination),
		AmountMinor:          int64(random.Intn(500_000) + 100),
		Currency:             domain.DefaultCurrency,
		Timestamp:            now,
		LocationCountry:      "NG",
		DeviceID:             fmt.Sprintf("device-%03d", random.Intn(100)+1),
		IdempotencyKey:       uuid.NewString(),
		Type:                 domain.TransactionTypeTransfer,
	}
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

func closeProducer(producer *strimkafka.Producer) {
	done := make(chan error, 1)
	go func() {
		done <- producer.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			slog.Error("close producer", "error", err)
		}
	case <-time.After(5 * time.Second):
		slog.Error("producer close timed out")
	}
}
