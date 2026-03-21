package main

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	Port         string
	KafkaBrokers []string
}

func LoadConfig() (Config, error) {
	port := getEnv("PORT", "8082")

	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}

	brokers := splitAndTrim(rawBrokers)
	if len(brokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is empty after parsing")
	}

	return Config{
		Port:         port,
		KafkaBrokers: brokers,
	}, nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
