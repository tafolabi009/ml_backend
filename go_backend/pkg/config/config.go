package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the API Gateway
type Config struct {
	Environment string
	Port        int
	DatabaseURL string
	RedisURL    string
	JWTSecret   string
	AWSRegion   string
	S3Bucket    string

	// Job Orchestrator address (replaces direct gRPC connections)
	OrchestratorAddr string

	// Legacy gRPC service addresses (deprecated, use orchestrator)
	ValidationServiceAddr string
	CollapseServiceAddr   string
	DataServiceAddr       string
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
	port, err := strconv.Atoi(getEnv("PORT", "8080"))
	if err != nil {
		return nil, fmt.Errorf("invalid PORT: %w", err)
	}

	cfg := &Config{
		Environment:           getEnv("ENVIRONMENT", "development"),
		Port:                  port,
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/synthos?sslmode=disable"),
		RedisURL:              getEnv("REDIS_URL", "localhost:6379"),
		JWTSecret:             getEnv("JWT_SECRET", "change-this-in-production"),
		AWSRegion:             getEnv("AWS_REGION", "us-east-1"),
		S3Bucket:              getEnv("S3_BUCKET", "synthos-datasets"),
		OrchestratorAddr:      getEnv("ORCHESTRATOR_ADDR", "localhost:8080"),
		ValidationServiceAddr: getEnv("VALIDATION_SERVICE_ADDR", "localhost:50051"),
		CollapseServiceAddr:   getEnv("COLLAPSE_SERVICE_ADDR", "localhost:50052"),
		DataServiceAddr:       getEnv("DATA_SERVICE_ADDR", "localhost:50054"),
	}

	// Validate required fields in production
	if cfg.Environment == "production" {
		if cfg.JWTSecret == "change-this-in-production" {
			return nil, fmt.Errorf("JWT_SECRET must be set in production")
		}
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
