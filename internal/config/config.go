package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers string
	RedisAddr    string
	DatabaseURL  string
}

func Load() (Config, error) {
	config := Config{
		KafkaBrokers: envOrDefault("KAFKA_BROKERS", "localhost:9092"),
		RedisAddr:    envOrDefault("REDIS_ADDR", "localhost:6379"),
		DatabaseURL:  strings.TrimSpace(os.Getenv("DATABASE_URL")),
	}
	if strings.TrimSpace(config.KafkaBrokers) == "" {
		return Config{}, errors.New("KAFKA_BROKERS must not be empty")
	}
	if strings.TrimSpace(config.RedisAddr) == "" {
		return Config{}, errors.New("REDIS_ADDR must not be empty")
	}
	return config, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
