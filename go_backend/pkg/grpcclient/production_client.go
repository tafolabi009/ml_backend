package grpcclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	collapsepb "github.com/tafolabi009/backend/proto/collapse"
	validationpb "github.com/tafolabi009/backend/proto/validation"
)

// ProductionClientConfig holds configuration for production gRPC clients
type ProductionClientConfig struct {
	ValidationAddr string
	CollapseAddr   string
	DataAddr       string
	
	// Retry configuration
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	
	// Circuit breaker configuration
	CBFailureThreshold int
	CBRecoveryTimeout  time.Duration
	
	// Timeouts
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
	
	// TLS
	TLSEnabled bool
	TLSCertFile string
}

// DefaultProductionConfig returns default production configuration
func DefaultProductionConfig() ProductionClientConfig {
	return ProductionClientConfig{
		ValidationAddr:     "localhost:50051",
		CollapseAddr:       "localhost:50052",
		DataAddr:           "localhost:50054",
		MaxRetries:         5,
		InitialBackoff:     100 * time.Millisecond,
		MaxBackoff:         10 * time.Second,
		BackoffMultiplier:  2.0,
		CBFailureThreshold: 5,
		CBRecoveryTimeout:  30 * time.Second,
		ConnectTimeout:     10 * time.Second,
		RequestTimeout:     60 * time.Second,
		TLSEnabled:         false,
	}
}

// ProductionClients holds all production gRPC clients
type ProductionClients struct {
	Validation validationpb.ValidationEngineClient
	Collapse   collapsepb.CollapseServiceClient
	
	validationConn *grpc.ClientConn
	collapseConn   *grpc.ClientConn
	
	validationCB *CircuitBreaker
	collapseCB   *CircuitBreaker
	
	config ProductionClientConfig
}

// NewProductionClients creates production-ready gRPC clients with all features
func NewProductionClients(ctx context.Context, cfg ProductionClientConfig) (*ProductionClients, error) {
	// Create connection options
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
		grpc.WithChainUnaryInterceptor(
			loggingInterceptor(),
			retryInterceptor(cfg),
			timeoutInterceptor(cfg.RequestTimeout),
		),
	}
	
	if cfg.TLSEnabled {
		if cfg.TLSCertFile == "" {
			return nil, fmt.Errorf("tls is enabled but no TLSCertFile was provided")
		}

		transportCredentials, err := credentials.NewClientTLSFromFile(cfg.TLSCertFile, "")
		if err != nil {
			return nil, fmt.Errorf("failed to load tls credentials: %w", err)
		}

		opts = append(opts, grpc.WithTransportCredentials(transportCredentials))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	
	// Connect to validation service
	log.Printf("Connecting to validation service at %s...", cfg.ValidationAddr)
	validationConn, err := grpc.NewClient(cfg.ValidationAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to validation service: %w", err)
	}
	
	// Connect to collapse service
	log.Printf("Connecting to collapse service at %s...", cfg.CollapseAddr)
	collapseConn, err := grpc.NewClient(cfg.CollapseAddr, opts...)
	if err != nil {
		validationConn.Close()
		return nil, fmt.Errorf("failed to connect to collapse service: %w", err)
	}
	
	clients := &ProductionClients{
		Validation:     validationpb.NewValidationEngineClient(validationConn),
		Collapse:       collapsepb.NewCollapseServiceClient(collapseConn),
		validationConn: validationConn,
		collapseConn:   collapseConn,
		validationCB:   NewCircuitBreaker(cfg.CBFailureThreshold, cfg.CBRecoveryTimeout),
		collapseCB:     NewCircuitBreaker(cfg.CBFailureThreshold, cfg.CBRecoveryTimeout),
		config:         cfg,
	}
	
	log.Println("✅ Production gRPC clients initialized successfully")
	return clients, nil
}

// Close closes all gRPC connections
func (c *ProductionClients) Close() error {
	var errs []error
	
	if c.validationConn != nil {
		if err := c.validationConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close validation connection: %w", err))
		}
	}
	
	if c.collapseConn != nil {
		if err := c.collapseConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close collapse connection: %w", err))
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	
	log.Println("✅ All gRPC connections closed")
	return nil
}

// CallValidation calls validation service with circuit breaker
func (c *ProductionClients) CallValidation(ctx context.Context, fn func(validationpb.ValidationEngineClient) error) error {
	return c.validationCB.Call(func() error {
		return fn(c.Validation)
	})
}

// CallCollapse calls collapse service with circuit breaker
func (c *ProductionClients) CallCollapse(ctx context.Context, fn func(collapsepb.CollapseServiceClient) error) error {
	return c.collapseCB.Call(func() error {
		return fn(c.Collapse)
	})
}

// GetValidationHealth checks validation service health
func (c *ProductionClients) GetValidationHealth() string {
	state := c.validationCB.GetState()
	switch state {
	case StateClosed:
		return "healthy"
	case StateHalfOpen:
		return "recovering"
	case StateOpen:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// GetCollapseHealth checks collapse service health
func (c *ProductionClients) GetCollapseHealth() string {
	state := c.collapseCB.GetState()
	switch state {
	case StateClosed:
		return "healthy"
	case StateHalfOpen:
		return "recovering"
	case StateOpen:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// loggingInterceptor logs all gRPC calls
func loggingInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()
		
		// Extract trace ID from context
		traceID := "unknown"
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			if vals := md.Get("x-trace-id"); len(vals) > 0 {
				traceID = vals[0]
			}
		}
		
		err := invoker(ctx, method, req, reply, cc, opts...)
		
		duration := time.Since(start)
		
		if err != nil {
			st, _ := status.FromError(err)
			log.Printf("gRPC [%s] %s - %s (%v) - trace: %s", 
				st.Code().String(), method, err.Error(), duration, traceID)
		} else {
			log.Printf("gRPC [OK] %s (%v) - trace: %s", method, duration, traceID)
		}
		
		return err
	}
}

// retryInterceptor implements retry logic with exponential backoff
func retryInterceptor(cfg ProductionClientConfig) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var lastErr error
		backoff := cfg.InitialBackoff
		
		for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}
			
			lastErr = err
			st, ok := status.FromError(err)
			if !ok {
				return err
			}
			
			// Only retry on retriable errors
			if !isRetriable(st.Code()) {
				return err
			}
			
			if attempt < cfg.MaxRetries {
				log.Printf("gRPC retry %d/%d for %s: %v", attempt+1, cfg.MaxRetries, method, err)
				
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				
				// Exponential backoff
				backoff = time.Duration(float64(backoff) * cfg.BackoffMultiplier)
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
			}
		}
		
		return fmt.Errorf("max retries exceeded: %w", lastErr)
	}
}

// timeoutInterceptor adds timeout to all calls
func timeoutInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// isRetriable determines if an error is retriable
func isRetriable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
