package middleware

import (
	"fmt"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	apperrors "github.com/tafolabi009/backend/go_backend/pkg/errors"
)

type rateLimiter struct {
	requests map[string][]time.Time
	mu       sync.RWMutex
	limit    int
	window   time.Duration
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		requests: make(map[string][]time.Time),
		limit:    limit,
		window:   window,
	}

	// Cleanup old entries every minute
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rl.cleanup()
		}
	}()

	return rl
}

func (rl *rateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for key, timestamps := range rl.requests {
		validTimestamps := []time.Time{}
		for _, ts := range timestamps {
			if now.Sub(ts) < rl.window {
				validTimestamps = append(validTimestamps, ts)
			}
		}
		if len(validTimestamps) == 0 {
			delete(rl.requests, key)
		} else {
			rl.requests[key] = validTimestamps
		}
	}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	timestamps := rl.requests[key]

	// Remove old timestamps
	validTimestamps := []time.Time{}
	for _, ts := range timestamps {
		if now.Sub(ts) < rl.window {
			validTimestamps = append(validTimestamps, ts)
		}
	}

	if len(validTimestamps) >= rl.limit {
		return false
	}

	validTimestamps = append(validTimestamps, now)
	rl.requests[key] = validTimestamps
	return true
}

var globalLimiter = newRateLimiter(100, time.Minute)

// RateLimit middleware enforces rate limiting
func RateLimit() fiber.Handler {
	return func(c *fiber.Ctx) error {
		userID := c.Locals("user_id")
		if userID == nil {
			userID = c.IP()
		}

		key := fmt.Sprintf("%v", userID)

		if !globalLimiter.allow(key) {
			return apperrors.HandleError(c, apperrors.RateLimitExceeded("Too many requests, please try again later"))
		}

		return c.Next()
	}
}

// RateLimitWithConfig creates a rate limiter with custom config
func RateLimitWithConfig(limit int, window time.Duration) fiber.Handler {
	limiter := newRateLimiter(limit, window)

	return func(c *fiber.Ctx) error {
		userID := c.Locals("user_id")
		if userID == nil {
			userID = c.IP()
		}

		key := fmt.Sprintf("%v", userID)

		if !limiter.allow(key) {
			return apperrors.HandleError(c, apperrors.RateLimitExceeded("Too many requests, please try again later"))
		}

		return c.Next()
	}
}
