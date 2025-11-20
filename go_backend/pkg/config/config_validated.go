package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Server
	Port int
	Env  string

	// Database (REQUIRED)
	DatabaseURL string

	// Redis (REQUIRED)
	RedisURL      string
	RedisPassword string

	// Storage (REQUIRED for GCS)
	GCSBucket         string
	GCSCredentials    string
	StorageProvider   string // "gcs", "s3", "local"

	// JWT (REQUIRED)
	JWTSecret            string
	JWTAccessExpireHours int
	JWTRefreshExpireDays int

	// Orchestrator
	OrchestratorAddr string

	// ML Services
	ValidationServiceAddr string
	CollapseServiceAddr   string
	DataServiceAddr       string

	// Features
	EnableMetrics bool
	EnableTracing bool
	JaegerEndpoint string

	// Rate Limiting
	RateLimitRequests int
	RateLimitWindow   int
}

func Load() *Config {
	config := &Config{
		// Server
		Port: getEnvAsInt("PORT", 8000),
		Env:  getEnv("ENV", "development"),

		// Database (REQUIRED)
		DatabaseURL: getRequiredEnv("DATABASE_URL"),

		// Redis (REQUIRED)
		RedisURL:      getRequiredEnv("REDIS_URL"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		// Storage (REQUIRED)
		GCSBucket:       getRequiredEnv("GCS_BUCKET"),
		GCSCredentials:  getRequiredEnv("GCS_CREDENTIALS"),
		StorageProvider: getEnv("STORAGE_PROVIDER", "gcs"),

		// JWT (REQUIRED)
		JWTSecret:            getRequiredEnv("JWT_SECRET"),
		JWTAccessExpireHours: getEnvAsInt("JWT_ACCESS_EXPIRE_HOURS", 1),
		JWTRefreshExpireDays: getEnvAsInt("JWT_REFRESH_EXPIRE_DAYS", 30),

		// Orchestrator
		OrchestratorAddr: getEnv("ORCHESTRATOR_ADDR", "http://job-orchestrator:8080"),

		// ML Services
		ValidationServiceAddr: getEnv("VALIDATION_SERVICE_ADDR", "localhost:50051"),
		CollapseServiceAddr:   getEnv("COLLAPSE_SERVICE_ADDR", "localhost:50052"),
		DataServiceAddr:       getEnv("DATA_SERVICE_ADDR", "localhost:50054"),

		// Features
		EnableMetrics:  getEnvAsBool("ENABLE_METRICS", true),
		EnableTracing:  getEnvAsBool("ENABLE_TRACING", false),
		JaegerEndpoint: getEnv("JAEGER_ENDPOINT", ""),

		// Rate Limiting
		RateLimitRequests: getEnvAsInt("RATE_LIMIT_REQUESTS", 100),
		RateLimitWindow:   getEnvAsInt("RATE_LIMIT_WINDOW", 60),
	}

	// Validate configuration
	config.Validate()

	log.Printf("✅ Configuration loaded successfully (env=%s)", config.Env)
	return config
}

// Validate ensures all required configuration is present
func (c *Config) Validate() {
	errors := []string{}

	// Critical checks
	if c.DatabaseURL == "" {
		errors = append(errors, "DATABASE_URL is required")
	}
	if c.RedisURL == "" {
		errors = append(errors, "REDIS_URL is required")
	}
	if c.JWTSecret == "" {
		errors = append(errors, "JWT_SECRET is required")
	}
	if c.GCSBucket == "" && c.StorageProvider == "gcs" {
		errors = append(errors, "GCS_BUCKET is required when using GCS storage")
	}
	if c.GCSCredentials == "" && c.StorageProvider == "gcs" {
		errors = append(errors, "GCS_CREDENTIALS is required when using GCS storage")
	}

	// Warning checks (non-fatal)
	if c.JWTSecret == "your-secret-key" || c.JWTSecret == "change-me" {
		log.Println("⚠️  WARNING: Using default JWT secret - CHANGE IN PRODUCTION!")
	}
	if c.Env == "production" && !strings.HasPrefix(c.DatabaseURL, "postgres://") {
		log.Println("⚠️  WARNING: Database URL should use postgres:// in production")
	}

	if len(errors) > 0 {
		log.Fatal("❌ Configuration validation failed:\n  - " + strings.Join(errors, "\n  - "))
	}
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getRequiredEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("❌ CRITICAL: Required environment variable %s is not set", key)
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Printf("⚠️  WARNING: Invalid integer for %s, using default %d", key, defaultValue)
		return defaultValue
	}
	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		log.Printf("⚠️  WARNING: Invalid boolean for %s, using default %v", key, defaultValue)
		return defaultValue
	}
	return value
}

// IsDevelopment returns true if running in development mode
func (c *Config) IsDevelopment() bool {
	return c.Env == "development" || c.Env == "dev"
}

// IsProduction returns true if running in production mode
func (c *Config) IsProduction() bool {
	return c.Env == "production" || c.Env == "prod"
}
