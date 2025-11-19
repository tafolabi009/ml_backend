package circuitbreaker

import (
	"context"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

// CircuitBreaker wraps gobreaker with logging
type CircuitBreaker struct {
	cb     *gobreaker.CircuitBreaker
	logger *zap.Logger
	name   string
}

// Config for circuit breaker
type Config struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts gobreaker.Counts) bool
	OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
}

// DefaultConfig returns default circuit breaker configuration
func DefaultConfig(name string) Config {
	return Config{
		Name:        name,
		MaxRequests: 3,
		Interval:    time.Second * 10,
		Timeout:     time.Second * 60,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config Config, logger *zap.Logger) *CircuitBreaker {
	settings := gobreaker.Settings{
		Name:        config.Name,
		MaxRequests: config.MaxRequests,
		Interval:    config.Interval,
		Timeout:     config.Timeout,
		ReadyToTrip: config.ReadyToTrip,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("Circuit breaker state changed",
				zap.String("circuit", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
			if config.OnStateChange != nil {
				config.OnStateChange(name, from, to)
			}
		},
	}

	return &CircuitBreaker{
		cb:     gobreaker.NewCircuitBreaker(settings),
		logger: logger,
		name:   config.Name,
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	result, err := cb.cb.Execute(func() (interface{}, error) {
		return fn()
	})

	if err != nil {
		if err == gobreaker.ErrOpenState {
			cb.logger.Warn("Circuit breaker is open",
				zap.String("circuit", cb.name),
			)
			return nil, fmt.Errorf("service unavailable: circuit breaker open")
		}
		if err == gobreaker.ErrTooManyRequests {
			cb.logger.Warn("Circuit breaker: too many requests",
				zap.String("circuit", cb.name),
			)
			return nil, fmt.Errorf("service unavailable: too many requests")
		}
	}

	return result, err
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() gobreaker.State {
	return cb.cb.State()
}

// Counts returns the current counts
func (cb *CircuitBreaker) Counts() gobreaker.Counts {
	return cb.cb.Counts()
}
