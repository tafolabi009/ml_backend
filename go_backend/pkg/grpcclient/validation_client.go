package grpcclient

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tafolabi009/backend/go_backend/pkg/circuitbreaker"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
	pb "github.com/tafolabi009/backend/proto/validation"
)

// ValidationClient wraps the ML backend's ValidationEngine gRPC service with a
// circuit breaker. It speaks the canonical `validation.ValidationEngine` contract
// (matching the Python ML server) and exposes simplified, transport-agnostic
// result structs so handlers don't depend on generated protobuf types.
type ValidationClient struct {
	conn    *grpc.ClientConn
	client  pb.ValidationEngineClient
	breaker *circuitbreaker.CircuitBreaker
	log     *logger.Logger
}

// DiversityResult is a simplified view of DiversityResponse.
type DiversityResult struct {
	OverallScore      float64 // 0..1 composite (higher = more diverse / lower risk)
	Entropy           float64
	GiniCoefficient   float64
	OutlierPercentage float64
	ClusterCount      int32
	SampleS3Path      string
}

// TierResult is a simplified per-tier cascade outcome.
type TierResult struct {
	Tier               int32
	ValidationAccuracy float64 // derived from validation_loss (1 - loss, clamped)
	CollapseDetected   bool
}

// CascadeResult is a simplified view of the streamed cascade training.
type CascadeResult struct {
	Results          []TierResult
	CollapseDetected bool
}

// NewValidationClient creates a new ValidationEngine client with a circuit breaker.
func NewValidationClient(addr string) (*ValidationClient, error) {
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

	log := logger.Get().With("service", "validation-client")
	breaker := circuitbreaker.NewCircuitBreaker(
		circuitbreaker.DefaultConfig("validation-service"),
		log.Logger,
	)
	log.Info("Connected to validation service", "address", addr)

	return &ValidationClient{
		conn:    conn,
		client:  pb.NewValidationEngineClient(conn),
		breaker: breaker,
		log:     log,
	}, nil
}

// inferDataFormat maps a file path/extension to the proto DataFormat enum.
func inferDataFormat(path string) pb.DataFormat {
	p := strings.ToLower(path)
	switch {
	case strings.HasSuffix(p, ".csv"):
		return pb.DataFormat_CSV
	case strings.HasSuffix(p, ".jsonl") || strings.HasSuffix(p, ".ndjson"):
		return pb.DataFormat_JSONL
	case strings.HasSuffix(p, ".json"):
		return pb.DataFormat_JSON
	case strings.HasSuffix(p, ".parquet"):
		return pb.DataFormat_PARQUET
	case strings.HasSuffix(p, ".h5") || strings.HasSuffix(p, ".hdf5"):
		return pb.DataFormat_HDF5
	case strings.HasSuffix(p, ".arrow"):
		return pb.DataFormat_ARROW
	case strings.HasSuffix(p, ".feather"):
		return pb.DataFormat_FEATHER
	case strings.HasSuffix(p, ".xlsx") || strings.HasSuffix(p, ".xls"):
		return pb.DataFormat_EXCEL
	case strings.HasSuffix(p, ".tsv"):
		return pb.DataFormat_TSV
	default:
		return pb.DataFormat_UNKNOWN_FORMAT
	}
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// diversityScore derives a 0..1 score from the ML diversity metrics:
// higher entropy + lower gini concentration + fewer outliers => more diverse.
func diversityScore(m *pb.DiversityMetrics, samplingConfidence int32) float64 {
	if m == nil {
		return clamp01(float64(samplingConfidence) / 100.0)
	}
	entropyNorm := clamp01(m.GetEntropy() / 5.0)
	giniInv := clamp01(1 - m.GetGiniCoefficient())
	outlierPenalty := clamp01(1 - m.GetOutlierPercentage()/100.0)
	return clamp01(0.4*entropyNorm + 0.3*giniInv + 0.3*outlierPenalty)
}

func (v *ValidationClient) reqLogger(ctx context.Context) (*logger.Logger, context.Context) {
	traceID := ctx.Value("trace_id")
	log := v.log
	if traceID != nil {
		log = log.With("trace_id", traceID)
	}
	md := metadata.New(map[string]string{"x-trace-id": fmt.Sprintf("%v", traceID)})
	return log, metadata.NewOutgoingContext(ctx, md)
}

// AnalyzeDiversity runs Phase 2 diversity analysis on the ML backend.
func (v *ValidationClient) AnalyzeDiversity(ctx context.Context, datasetID, s3Path string) (*DiversityResult, error) {
	log, ctx := v.reqLogger(ctx)
	log.Info("Starting diversity analysis", "dataset_id", datasetID, "dataset", s3Path)

	result, err := v.breaker.Execute(ctx, func() (interface{}, error) {
		resp, err := v.client.AnalyzeDiversity(ctx, &pb.DiversityRequest{
			DatasetId: datasetID,
			S3Path:    s3Path,
			Format:    inferDataFormat(s3Path),
		})
		if err != nil {
			log.Error("Diversity analysis failed", "error", err, "dataset_id", datasetID)
			return nil, err
		}
		if resp.GetError() != nil && resp.GetError().GetMessage() != "" {
			return nil, fmt.Errorf("ml error: %s", resp.GetError().GetMessage())
		}
		return resp, nil
	})
	if err != nil {
		return nil, fmt.Errorf("diversity analysis failed: %w", err)
	}

	resp := result.(*pb.DiversityResponse)
	out := &DiversityResult{
		OverallScore: diversityScore(resp.GetMetrics(), resp.GetSamplingConfidence()),
		SampleS3Path: resp.GetSampleS3Path(),
	}
	if m := resp.GetMetrics(); m != nil {
		out.Entropy = m.GetEntropy()
		out.GiniCoefficient = m.GetGiniCoefficient()
		out.OutlierPercentage = m.GetOutlierPercentage()
		out.ClusterCount = m.GetClusterCount()
	}
	log.Info("Diversity analysis completed", "dataset_id", datasetID, "score", out.OverallScore)
	return out, nil
}

// TrainCascade runs Phase 4 cascade training. The ML backend streams
// CascadeProgress; we consume the stream and return the per-tier model results.
func (v *ValidationClient) TrainCascade(ctx context.Context, datasetID, validationID, sampleS3Path string) (*CascadeResult, error) {
	log, ctx := v.reqLogger(ctx)
	log.Info("Initiating cascade training", "validation_id", validationID, "sample", sampleS3Path)

	result, err := v.breaker.Execute(ctx, func() (interface{}, error) {
		stream, err := v.client.TrainCascade(ctx, &pb.CascadeRequest{
			DatasetId:    datasetID,
			ValidationId: validationID,
			SampleS3Path: sampleS3Path,
		})
		if err != nil {
			log.Error("Cascade training failed to start", "error", err, "validation_id", validationID)
			return nil, err
		}

		out := &CascadeResult{}
		for {
			progress, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				// Return what we have so far on stream error.
				log.Error("Cascade stream error", "error", err, "validation_id", validationID)
				return out, err
			}
			if e := progress.GetError(); e != nil && e.GetMessage() != "" {
				return out, fmt.Errorf("ml error: %s", e.GetMessage())
			}
			if r := progress.GetResult(); r != nil {
				acc := clamp01(1 - r.GetValidationLoss())
				out.Results = append(out.Results, TierResult{
					Tier:               r.GetTier(),
					ValidationAccuracy: acc,
					CollapseDetected:   r.GetCollapseDetected(),
				})
				if r.GetCollapseDetected() {
					out.CollapseDetected = true
				}
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("cascade training failed: %w", err)
	}

	resp := result.(*CascadeResult)
	log.Info("Cascade training completed", "validation_id", validationID, "tiers", len(resp.Results))
	return resp, nil
}

// Close closes the client connection.
func (v *ValidationClient) Close() error {
	v.log.Info("Closing validation client")
	return v.conn.Close()
}

// Health checks if the service is reachable / breaker is closed.
func (v *ValidationClient) Health(ctx context.Context, jobID string) error {
	if v.conn == nil {
		return fmt.Errorf("client not connected")
	}
	if v.breaker.State() == gobreaker.StateOpen {
		return fmt.Errorf("validation service circuit breaker open")
	}
	return nil
}
