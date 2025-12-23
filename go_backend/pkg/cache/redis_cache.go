package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache provides Redis-based caching with aggressive strategies
type Cache struct {
	client *redis.Client
}

// NewCache creates a new cache instance
func NewCache(redisURL, password string) (*Cache, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL: %w", err)
	}
	if password != "" {
		opt.Password = password
	}
	client := redis.NewClient(opt)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Cache{client: client}, nil
}

// Set stores a value with TTL
func (c *Cache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

// Get retrieves a value
func (c *Cache) Get(ctx context.Context, key string, dest interface{}) error {
	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return fmt.Errorf("cache miss")
	}
	if err != nil {
		return fmt.Errorf("failed to get cache: %w", err)
	}
	return json.Unmarshal([]byte(data), dest)
}

// Delete removes a key
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

// Exists checks if key exists
func (c *Cache) Exists(ctx context.Context, key string) (bool, error) {
	count, err := c.client.Exists(ctx, key).Result()
	return count > 0, err
}

// Invalidate invalidates cache by pattern
func (c *Cache) Invalidate(ctx context.Context, pattern string) error {
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	keys := []string{}
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return c.client.Del(ctx, keys...).Err()
	}
	return nil
}

// SetNX sets only if not exists
func (c *Cache) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("failed to marshal value: %w", err)
	}
	return c.client.SetNX(ctx, key, data, ttl).Result()
}

// Increment increments a counter
func (c *Cache) Increment(ctx context.Context, key string) (int64, error) {
	return c.client.Incr(ctx, key).Result()
}

// IncrementBy increments by amount
func (c *Cache) IncrementBy(ctx context.Context, key string, amount int64) (int64, error) {
	return c.client.IncrBy(ctx, key, amount).Result()
}

// Expire sets TTL on existing key
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.client.Expire(ctx, key, ttl).Err()
}

// Close closes the Redis connection
func (c *Cache) Close() error {
	return c.client.Close()
}

// Predefined cache strategies
const (
	// Validation results - cache for 1 hour (expensive to compute)
	ValidationKeyPrefix = "validation:"
	ValidationCacheTTL  = 1 * time.Hour

	// Dataset metadata - cache for 5 minutes
	DatasetKeyPrefix = "dataset:"
	DatasetCacheTTL  = 5 * time.Minute

	// User sessions - cache for 15 minutes
	SessionKeyPrefix = "session:"
	SessionCacheTTL  = 15 * time.Minute

	// API responses - cache for 1 minute
	APIKeyPrefix = "api:"
	APICacheTTL  = 1 * time.Minute

	// Analytics - cache for 10 minutes
	AnalyticsKeyPrefix = "analytics:"
	AnalyticsCacheTTL  = 10 * time.Minute
)

// CacheKey generates a cache key
func CacheKey(prefix, id string) string {
	return fmt.Sprintf("%s%s", prefix, id)
}

// CacheValidation caches validation result
func (c *Cache) CacheValidation(ctx context.Context, validationID string, result interface{}) error {
	key := CacheKey(ValidationKeyPrefix, validationID)
	return c.Set(ctx, key, result, ValidationCacheTTL)
}

// GetValidation retrieves cached validation
func (c *Cache) GetValidation(ctx context.Context, validationID string, dest interface{}) error {
	key := CacheKey(ValidationKeyPrefix, validationID)
	return c.Get(ctx, key, dest)
}

// CacheDataset caches dataset metadata
func (c *Cache) CacheDataset(ctx context.Context, datasetID string, dataset interface{}) error {
	key := CacheKey(DatasetKeyPrefix, datasetID)
	return c.Set(ctx, key, dataset, DatasetCacheTTL)
}

// GetDataset retrieves cached dataset
func (c *Cache) GetDataset(ctx context.Context, datasetID string, dest interface{}) error {
	key := CacheKey(DatasetKeyPrefix, datasetID)
	return c.Get(ctx, key, dest)
}

// InvalidateUser invalidates all user-related caches
func (c *Cache) InvalidateUser(ctx context.Context, userID string) error {
	patterns := []string{
		fmt.Sprintf("%s%s:*", DatasetKeyPrefix, userID),
		fmt.Sprintf("%s%s:*", ValidationKeyPrefix, userID),
		fmt.Sprintf("%s%s:*", AnalyticsKeyPrefix, userID),
	}
	for _, pattern := range patterns {
		if err := c.Invalidate(ctx, pattern); err != nil {
			return err
		}
	}
	return nil
}
