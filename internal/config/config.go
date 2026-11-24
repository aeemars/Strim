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
	loadDotEnv()
	config := Config{
		KafkaBrokers: envOrDefault("KAFKA_BROKERS", "localhost:9092"),
		RedisAddr:    envOrDefault("REDIS_ADDR", "localhost:6379"),
		DatabaseURL:  envOrDefault("DATABASE_URL", "postgres://strim:strim-local-only@localhost:5432/strim?sslmode=disable"),
	}
	if strings.TrimSpace(config.KafkaBrokers) == "" {
		return Config{}, errors.New("KAFKA_BROKERS must not be empty")
	}
	if strings.TrimSpace(config.RedisAddr) == "" {
		return Config{}, errors.New("REDIS_ADDR must not be empty")
	}
	return config, nil
}

func loadDotEnv() {
	paths := []string{".env", "../.env", "../../.env"}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				val := strings.TrimSpace(parts[1])
				if os.Getenv(key) == "" {
					_ = os.Setenv(key, val)
				}
			}
		}
		break
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
