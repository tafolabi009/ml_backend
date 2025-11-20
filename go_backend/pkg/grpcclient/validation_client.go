package grpcclient

import (
	"context"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tafolabi009/backend/go_backend/pkg/circuitbreaker"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
	pb "github.com/tafolabi009/backend/proto/validation"
)

// ValidationClient wraps gRPC validation service with circuit breaker
type ValidationClient struct {
	conn    *grpc.ClientConn
	client  pb.ValidationServiceClient
	breaker *circuitbreaker.CircuitBreaker
	log     *logger.Logger
}

// NewValidationClient creates a new validation client with circuit breaker
func NewValidationClient(addr string) (*ValidationClient, error) {
	// Connect with proper options
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to validation service: %w", err)
	}

	client := pb.NewValidationServiceClient(conn)

	log := logger.Get().With("service", "validation-client")
	breaker := circuitbreaker.NewCircuitBreaker(
		circuitbreaker.DefaultConfig("validation-service"),
		log.Logger,
	)

	log.Info("Connected to validation service", "address", addr)

	return &ValidationClient{
		conn:    conn,
		client:  client,
		breaker: breaker,
		log:     log,
	}, nil
}

// TrainCascade initiates cascade training with circuit breaker protection
func (v *ValidationClient) TrainCascade(ctx context.Context, jobID, datasetPath string, config *pb.CascadeConfig) (*pb.TrainCascadeResponse, error) {
	traceID := ctx.Value("trace_id")
	if traceID != nil {
		v.log = v.log.With("trace_id", traceID)
	}

	v.log.Info("Initiating cascade training", "job_id", jobID, "dataset", datasetPath)

	// Add trace ID to metadata
	md := metadata.New(map[string]string{
		"x-trace-id": fmt.Sprintf("%v", traceID),
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Execute with circuit breaker
	result, err := v.breaker.Execute(ctx, func() (interface{}, error) {
		req := &pb.TrainCascadeRequest{
			JobId:       jobID,
			DatasetPath: datasetPath,
			Config:      config,
		}

		resp, err := v.client.TrainCascade(ctx, req)
		if err != nil {
			v.log.Error("Cascade training failed", "error", err, "job_id", jobID)
			return nil, err
		}

		return resp, nil
	})

	if err != nil {
		return nil, fmt.Errorf("cascade training failed: %w", err)
	}

	response := result.(*pb.TrainCascadeResponse)
	v.log.Info("Cascade training completed",
		"job_id", jobID,
		"status", response.Status,
		"models", len(response.Results),
	)

	return response, nil
}

// AnalyzeDiversity performs diversity analysis with circuit breaker
func (v *ValidationClient) AnalyzeDiversity(ctx context.Context, jobID, datasetPath string, config *pb.DiversityConfig) (*pb.AnalyzeDiversityResponse, error) {
	traceID := ctx.Value("trace_id")
	if traceID != nil {
		v.log = v.log.With("trace_id", traceID)
	}

	v.log.Info("Starting diversity analysis", "job_id", jobID, "dataset", datasetPath)

	// Add trace ID to metadata
	md := metadata.New(map[string]string{
		"x-trace-id": fmt.Sprintf("%v", traceID),
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	result, err := v.breaker.Execute(ctx, func() (interface{}, error) {
		req := &pb.AnalyzeDiversityRequest{
			JobId:       jobID,
			DatasetPath: datasetPath,
			Config:      config,
		}

		resp, err := v.client.AnalyzeDiversity(ctx, req)
		if err != nil {
			v.log.Error("Diversity analysis failed", "error", err, "job_id", jobID)
			return nil, err
		}

		return resp, nil
	})

	if err != nil {
		return nil, fmt.Errorf("diversity analysis failed: %w", err)
	}

	response := result.(*pb.AnalyzeDiversityResponse)
	v.log.Info("Diversity analysis completed",
		"job_id", jobID,
		"score", response.Score.OverallScore,
	)

	return response, nil
}

// Close closes the client connection
func (v *ValidationClient) Close() error {
	v.log.Info("Closing validation client")
	return v.conn.Close()
}

// Health checks if the service is healthy
func (v *ValidationClient) Health(ctx context.Context, jobID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Simple connection check
	if v.conn == nil {
		return fmt.Errorf("client not connected")
	}

	// Check circuit breaker state
	if v.breaker.State() == gobreaker.StateOpen {
		return fmt.Errorf("validation service circuit breaker open")
	}

	return nil
}
