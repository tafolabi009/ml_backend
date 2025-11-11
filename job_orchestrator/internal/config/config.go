package config

import (
	"fmt"
	"os"
)

type Config struct {
	Environment           string
	Port                  string
	DatabaseURL           string
	RedisURL              string
	TLSCertFile           string
	TLSKeyFile            string
	TLSCAFile             string
	
	// Service addresses
	ValidationEngineAddr  string
	CollapseEngineAddr    string
	DataServiceAddr       string
	
	// Feature flags
	EnableMetrics         bool
	EnableTracing         bool
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		Environment:          getEnv("ENVIRONMENT", "development"),
		Port:                 getEnv("PORT", "50052"),
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/synthos?sslmode=disable"),
		RedisURL:             getEnv("REDIS_URL", "localhost:6379"),
		TLSCertFile:          getEnv("TLS_CERT_FILE", "/etc/synthos/certs/server.crt"),
		TLSKeyFile:           getEnv("TLS_KEY_FILE", "/etc/synthos/certs/server.key"),
		TLSCAFile:            getEnv("TLS_CA_FILE", "/etc/synthos/certs/ca.crt"),
		ValidationEngineAddr: getEnv("VALIDATION_ENGINE_ADDR", "localhost:50051"),
		CollapseEngineAddr:   getEnv("COLLAPSE_ENGINE_ADDR", "localhost:50053"),
		DataServiceAddr:      getEnv("DATA_SERVICE_ADDR", "localhost:50054"),
		EnableMetrics:        getEnv("ENABLE_METRICS", "true") == "true",
		EnableTracing:        getEnv("ENABLE_TRACING", "false") == "true",
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.ValidationEngineAddr == "" {
		return fmt.Errorf("VALIDATION_ENGINE_ADDR is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
