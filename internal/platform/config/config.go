package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Common struct {
	ServiceName  string
	DatabaseURL  string
	KafkaBrokers []string
	LogLevel     string
	HealthAddr   string
}

func LoadCommon(serviceName, defaultHealthAddr string) (Common, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return Common{}, fmt.Errorf("DATABASE_URL is required")
	}

	brokers := splitNonEmpty(env("KAFKA_BROKERS", "localhost:29092"))
	if len(brokers) == 0 {
		return Common{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}

	return Common{
		ServiceName:  serviceName,
		DatabaseURL:  databaseURL,
		KafkaBrokers: brokers,
		LogLevel:     env("LOG_LEVEL", "info"),
		HealthAddr:   env("HEALTH_ADDR", defaultHealthAddr),
	}, nil
}

func String(name, fallback string) string { return env(name, fallback) }

func Int(name string, fallback int) (int, error) {
	raw := env(name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func Duration(name string, fallback time.Duration) (time.Duration, error) {
	raw := env(name, fallback.String())
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive duration: %w", name, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}
