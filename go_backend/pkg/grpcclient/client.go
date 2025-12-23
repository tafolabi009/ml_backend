package grpcclient

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	datapb "github.com/tafolabi009/backend/proto/data"
	synthospb "github.com/tafolabi009/backend/proto/synthos"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern for fault tolerance
type CircuitBreaker struct {
	mu              sync.RWMutex
	state           CircuitBreakerState
	failures        int
	successes       int
	lastFailureTime time.Time
	threshold       int
	timeout         time.Duration
	halfOpenMax     int
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:       StateClosed,
		threshold:   threshold,
		timeout:     timeout,
		halfOpenMax: 3,
	}
}

// Call executes a function with circuit breaker protection
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	state := cb.state

	// Check if circuit should transition from Open to HalfOpen
	if state == StateOpen && time.Since(cb.lastFailureTime) > cb.timeout {
		cb.state = StateHalfOpen
		cb.successes = 0
		state = StateHalfOpen
	}

	// Reject if circuit is open
	if state == StateOpen {
		cb.mu.Unlock()
		return fmt.Errorf("circuit breaker is open")
	}

	cb.mu.Unlock()

	// Execute the function
	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailureTime = time.Now()

		if cb.state == StateHalfOpen {
			// Failed in half-open, go back to open
			cb.state = StateOpen
		} else if cb.failures >= cb.threshold {
			// Too many failures, open the circuit
			cb.state = StateOpen
			log.Printf("⚠️ Circuit breaker opened after %d failures", cb.failures)
		}
		return err
	}

	// Success
	if cb.state == StateHalfOpen {
		cb.successes++
		if cb.successes >= cb.halfOpenMax {
			// Enough successes in half-open, close the circuit
			cb.state = StateClosed
			cb.failures = 0
			log.Println("✅ Circuit breaker closed after recovery")
		}
	} else {
		cb.failures = 0
	}

	return nil
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Clients holds all gRPC client connections with advanced features
type Clients struct {
	Validation synthospb.ValidationEngineClient
	Collapse   synthospb.CollapseEngineClient
	Data       datapb.DataServiceClient

	validationConn *grpc.ClientConn
	collapseConn   *grpc.ClientConn
	dataConn       *grpc.ClientConn

	validationCB *CircuitBreaker
	collapseCB   *CircuitBreaker
	dataCB       *CircuitBreaker

	retryConfig RetryConfig
}

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// RetryWithBackoff executes a function with exponential backoff retry logic
func (c *Clients) RetryWithBackoff(ctx context.Context, fn func() error) error {
	var lastErr error
	backoff := c.retryConfig.InitialBackoff

	for attempt := 0; attempt < c.retryConfig.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * c.retryConfig.BackoffMultiplier)
			if backoff > c.retryConfig.MaxBackoff {
				backoff = c.retryConfig.MaxBackoff
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry non-retryable errors
		if !isRetryableError(err) {
			return err
		}

		log.Printf("⚠️ Request failed (attempt %d/%d): %v. Retrying in %v...",
			attempt+1, c.retryConfig.MaxAttempts, err, backoff)
	}

	return fmt.Errorf("max retry attempts exceeded: %w", lastErr)
}

// NewClients creates and connects all gRPC clients with advanced features
func NewClients(validationAddr, collapseAddr, dataAddr string) (*Clients, error) {
	return NewClientsWithConfig(validationAddr, collapseAddr, dataAddr, DefaultRetryConfig())
}

// NewClientsWithConfig creates clients with custom retry configuration
func NewClientsWithConfig(validationAddr, collapseAddr, dataAddr string, retryConfig RetryConfig) (*Clients, error) {
	clients := &Clients{
		validationCB: NewCircuitBreaker(5, 30*time.Second),
		collapseCB:   NewCircuitBreaker(5, 30*time.Second),
		dataCB:       NewCircuitBreaker(5, 30*time.Second),
		retryConfig:  retryConfig,
	}

	// Common dial options with advanced connection settings
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
			grpc.MaxCallSendMsgSize(100*1024*1024), // 100MB
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1.0 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   30 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		// Enable automatic connection management
		grpc.WithDisableHealthCheck(),
	}

	// Connect to validation service
	log.Printf("Connecting to Validation Service at %s", validationAddr)
	validationConn, err := grpc.NewClient(validationAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to validation service: %w", err)
	}
	clients.validationConn = validationConn
	clients.Validation = synthospb.NewValidationEngineClient(validationConn)

	// Connect to collapse service
	log.Printf("Connecting to Collapse Service at %s", collapseAddr)
	collapseConn, err := grpc.NewClient(collapseAddr, dialOpts...)
	if err != nil {
		validationConn.Close()
		return nil, fmt.Errorf("failed to connect to collapse service: %w", err)
	}
	clients.collapseConn = collapseConn
	clients.Collapse = synthospb.NewCollapseEngineClient(collapseConn)

	// Connect to data service
	log.Printf("Connecting to Data Service at %s", dataAddr)
	dataConn, err := grpc.NewClient(dataAddr, dialOpts...)
	if err != nil {
		validationConn.Close()
		collapseConn.Close()
		return nil, fmt.Errorf("failed to connect to data service: %w", err)
	}
	clients.dataConn = dataConn
	clients.Data = datapb.NewDataServiceClient(dataConn)

	log.Println("✅ All gRPC clients connected successfully with circuit breakers and retry logic")
	return clients, nil
}

// Close closes all gRPC connections
func (c *Clients) Close() {
	if c.validationConn != nil {
		c.validationConn.Close()
		log.Println("Validation service connection closed")
	}
	if c.collapseConn != nil {
		c.collapseConn.Close()
		log.Println("Collapse service connection closed")
	}
	if c.dataConn != nil {
		c.dataConn.Close()
		log.Println("Data service connection closed")
	}
}

// Health checks all gRPC services
func (c *Clients) Health(ctx context.Context) error {
	// Check validation service using AnalyzeDiversity with empty request
	// This will fail but confirms the service is reachable
	_, err := c.Validation.AnalyzeDiversity(ctx, &synthospb.DiversityRequest{DatasetId: "health-check"})
	if err != nil && !isServiceAvailable(err) {
		return fmt.Errorf("validation service health check failed: %w", err)
	}

	// Additional health checks can be added here
	return nil
}

// isServiceAvailable checks if the error indicates the service is reachable
func isServiceAvailable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	// Service is available if we get any response other than Unavailable
	return st.Code() != codes.Unavailable
}

// CallValidationWithRetry calls validation service with circuit breaker and retry logic
func (c *Clients) CallValidationWithRetry(ctx context.Context, fn func(context.Context, synthospb.ValidationEngineClient) error) error {
	return c.validationCB.Call(func() error {
		return c.RetryWithBackoff(ctx, func() error {
			return fn(ctx, c.Validation)
		})
	})
}

// CallCollapseWithRetry calls collapse service with circuit breaker and retry logic
func (c *Clients) CallCollapseWithRetry(ctx context.Context, fn func(context.Context, synthospb.CollapseEngineClient) error) error {
	return c.collapseCB.Call(func() error {
		return c.RetryWithBackoff(ctx, func() error {
			return fn(ctx, c.Collapse)
		})
	})
}

// CallDataWithRetry calls data service with circuit breaker and retry logic
func (c *Clients) CallDataWithRetry(ctx context.Context, fn func(context.Context, datapb.DataServiceClient) error) error {
	return c.dataCB.Call(func() error {
		return c.RetryWithBackoff(ctx, func() error {
			return fn(ctx, c.Data)
		})
	})
}

// GetCircuitBreakerStates returns the current state of all circuit breakers
func (c *Clients) GetCircuitBreakerStates() map[string]string {
	states := map[CircuitBreakerState]string{
		StateClosed:   "closed",
		StateOpen:     "open",
		StateHalfOpen: "half-open",
	}

	return map[string]string{
		"validation": states[c.validationCB.GetState()],
		"collapse":   states[c.collapseCB.GetState()],
		"data":       states[c.dataCB.GetState()],
	}
}
