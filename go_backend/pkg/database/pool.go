package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
)

// PoolConfig represents database pool configuration
type PoolConfig struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// DefaultPoolConfig returns sensible defaults for production
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConns:          25, // Max connections in pool
		MinConns:          5,  // Min idle connections
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    10 * time.Second,
	}
}

// NewPool creates a new PostgreSQL connection pool with optimized settings
func NewPool(ctx context.Context, dsn string, poolConfig *PoolConfig) (*pgxpool.Pool, error) {
	log := logger.Get().With("component", "database")

	if poolConfig == nil {
		poolConfig = DefaultPoolConfig()
	}

	// Parse DSN and configure pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// Apply pool configuration
	config.MaxConns = poolConfig.MaxConns
	config.MinConns = poolConfig.MinConns
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime
	config.HealthCheckPeriod = poolConfig.HealthCheckPeriod
	config.ConnConfig.ConnectTimeout = poolConfig.ConnectTimeout

	// Create pool
	log.Info("Creating database connection pool",
		"max_conns", config.MaxConns,
		"min_conns", config.MinConns,
		"max_lifetime", config.MaxConnLifetime,
	)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Log pool stats
	stats := pool.Stat()
	log.Info("Database pool created successfully",
		"total_conns", stats.TotalConns(),
		"idle_conns", stats.IdleConns(),
		"acquired_conns", stats.AcquiredConns(),
	)

	return pool, nil
}

// PoolStats represents current pool statistics
type PoolStats struct {
	TotalConns    int32 `json:"total_conns"`
	IdleConns     int32 `json:"idle_conns"`
	AcquiredConns int32 `json:"acquired_conns"`
	MaxConns      int32 `json:"max_conns"`
}

// GetPoolStats returns current pool statistics
func GetPoolStats(pool *pgxpool.Pool) PoolStats {
	stats := pool.Stat()
	config := pool.Config()
	return PoolStats{
		TotalConns:    stats.TotalConns(),
		IdleConns:     stats.IdleConns(),
		AcquiredConns: stats.AcquiredConns(),
		MaxConns:      config.MaxConns,
	}
}

// MonitorPool logs pool statistics periodically
func MonitorPool(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	log := logger.Get().With("component", "pool-monitor")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Pool monitor stopped")
			return
		case <-ticker.C:
			stats := GetPoolStats(pool)
			log.Debug("Pool statistics",
				"total", stats.TotalConns,
				"idle", stats.IdleConns,
				"acquired", stats.AcquiredConns,
				"max", stats.MaxConns,
			)
			// Warn if pool is exhausted
			if stats.AcquiredConns >= stats.MaxConns {
				log.Warn("Connection pool exhausted",
					"acquired", stats.AcquiredConns,
					"max", stats.MaxConns,
				)
			}
		}
	}
}
