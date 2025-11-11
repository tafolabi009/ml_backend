package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

// Init initializes the database connection pool
func Init(databaseURL string) error {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Connection pool settings
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = time.Minute

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	db = pool
	log.Println("✅ Database connection established")
	return nil
}

// GetDB returns the database connection pool
func GetDB() *pgxpool.Pool {
	return db
}

// Close closes the database connection pool
func Close() {
	if db != nil {
		db.Close()
		log.Println("Database connection closed")
	}
}

// Health checks database health
func Health() error {
	if db == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}
