package grpcclient

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tafolabi009/backend/go_backend/pkg/circuitbreaker"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
	pb "github.com/tafolabi009/backend/proto/synthos"
)

// ValidationClient wraps gRPC validation service with circuit breaker
type ValidationClient struct {
	conn    *grpc.ClientConn
	client  pb.ValidationEngineClient
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

	client := pb.NewValidationEngineClient(conn)

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

// CascadeResult holds the final result of cascade training
type CascadeResult struct {
	Status          string
	ModelsCompleted int32
	BestAccuracy    float64
	BestModel       string
}

// TrainCascade initiates cascade training with circuit breaker protection
// TrainCascade is now a streaming RPC
func (v *ValidationClient) TrainCascade(ctx context.Context, jobID, datasetPath string, config *pb.CascadeConfig) (*CascadeResult, error) {
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
		req := &pb.CascadeRequest{
			JobId:       jobID,
			SampleS3Path: datasetPath,
			Config:      config,
		}

		// TrainCascade returns a stream
		stream, err := v.client.TrainCascade(ctx, req)
		if err != nil {
			v.log.Error("Cascade training failed to start", "error", err, "job_id", jobID)
			return nil, err
		}

		// Collect streaming results
		var lastProgress *pb.CascadeProgress
		var bestAccuracy float64
		var bestModel string

		for {
			progress, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				v.log.Error("Cascade training stream error", "error", err, "job_id", jobID)
				return nil, err
			}

			lastProgress = progress

			// Track best model
			if progress.Result != nil && progress.Result.ValidationAccuracy > bestAccuracy {
				bestAccuracy = progress.Result.ValidationAccuracy
				bestModel = progress.Result.ModelName
			}

			// Check for errors
			if progress.Status == "failed" && progress.Error != nil {
				return nil, fmt.Errorf("cascade training failed: %s", progress.Error.Message)
			}

			v.log.Debug("Cascade progress",
				"tier", progress.CurrentTier,
				"variant", progress.CurrentVariant,
				"completed", progress.ModelsCompleted,
				"total", progress.ModelsTotal,
				"progress", progress.ProgressPercent)
		}

		return &CascadeResult{
			Status:          "completed",
			ModelsCompleted: lastProgress.ModelsCompleted,
			BestAccuracy:    bestAccuracy,
			BestModel:       bestModel,
		}, nil
	})

	if err != nil {
		return nil, fmt.Errorf("cascade training failed: %w", err)
	}

	response := result.(*CascadeResult)
	v.log.Info("Cascade training completed",
		"job_id", jobID,
		"status", response.Status,
		"models_completed", response.ModelsCompleted,
	)

	return response, nil
}

// AnalyzeDiversity performs diversity analysis with circuit breaker
func (v *ValidationClient) AnalyzeDiversity(ctx context.Context, jobID, datasetPath string) (*pb.DiversityResponse, error) {
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
		req := &pb.DiversityRequest{
			JobId:     jobID,
			DatasetId: jobID,
			S3Path:    datasetPath,
			Format:    pb.DataFormat_CSV,
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

	response := result.(*pb.DiversityResponse)
	v.log.Info("Diversity analysis completed",
		"job_id", jobID,
		"status", response.Status,
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
