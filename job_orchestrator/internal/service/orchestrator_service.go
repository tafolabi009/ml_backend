package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
	datapb "github.com/tafolabi009/backend/proto/data"
	synthospb "github.com/tafolabi009/backend/proto/synthos"
)

// OrchestratorService manages the entire orchestration system
type OrchestratorService struct {
	queue           *JobQueue
	resourceManager *ResourceManager
	pipelineManager *PipelineManager

	// gRPC clients - using unified synthos proto
	validationClient synthospb.ValidationEngineClient
	collapseClient   synthospb.CollapseEngineClient
	dataClient       datapb.DataServiceClient

	validationConn *grpc.ClientConn
	collapseConn   *grpc.ClientConn
	dataConn       *grpc.ClientConn

	mu sync.RWMutex
}

// NewOrchestratorService creates a new orchestrator service
func NewOrchestratorService(workers int, validationAddr, collapseAddr, dataAddr string) (*OrchestratorService, error) {
	// Initialize gRPC connections
	validationConn, err := grpc.NewClient(
		validationAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to validation service: %w", err)
	}

	collapseConn, err := grpc.NewClient(
		collapseAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		validationConn.Close()
		return nil, fmt.Errorf("failed to connect to collapse service: %w", err)
	}

	dataConn, err := grpc.NewClient(
		dataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		validationConn.Close()
		collapseConn.Close()
		return nil, fmt.Errorf("failed to connect to data service: %w", err)
	}

	service := &OrchestratorService{
		queue:            NewJobQueue(workers),
		resourceManager:  NewResourceManager(workers),
		pipelineManager:  NewPipelineManager(),
		validationClient: synthospb.NewValidationEngineClient(validationConn),
		collapseClient:   synthospb.NewCollapseEngineClient(collapseConn),
		dataClient:       datapb.NewDataServiceClient(dataConn),
		validationConn:   validationConn,
		collapseConn:     collapseConn,
		dataConn:         dataConn,
	}

	// Register job executors
	service.registerExecutors()

	log.Println("Orchestrator service initialized successfully")
	return service, nil
}

// registerExecutors registers job execution handlers
func (s *OrchestratorService) registerExecutors() {
	// Validation job executor
	s.queue.RegisterExecutor("validation", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeValidationJob(ctx, job)
	})

	// Collapse detection executor
	s.queue.RegisterExecutor("collapse_detection", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeCollapseJob(ctx, job)
	})

	// Data processing executor
	s.queue.RegisterExecutor("data_processing", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeDataProcessingJob(ctx, job)
	})

	// Diversity analysis executor
	s.queue.RegisterExecutor("diversity_analysis", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeDiversityAnalysisJob(ctx, job)
	})

	// Collapse localization executor
	s.queue.RegisterExecutor("collapse_localization", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeCollapseLocalizationJob(ctx, job)
	})

	// Recommendations generation executor
	s.queue.RegisterExecutor("recommendations", func(ctx context.Context, job *Job) (map[string]string, error) {
		return s.executeRecommendationsJob(ctx, job)
	})
}

// CreateJob creates a new job
func (s *OrchestratorService) CreateJob(ctx context.Context, userID, jobType string, priority int32, payload map[string]string) (*Job, int, error) {
	// Check resource availability
	if !s.resourceManager.CanAcceptJob() {
		return nil, 0, fmt.Errorf("system at capacity, please try again later")
	}

	job, err := s.queue.CreateJob(userID, jobType, priority, payload)
	if err != nil {
		return nil, 0, err
	}

	stats := s.queue.GetQueueStats()
	queuePosition := stats["queued"]

	log.Printf("Created job %s (type: %s, priority: %d, queue position: %d)", job.ID, jobType, priority, queuePosition)
	return job, queuePosition, nil
}

// GetJob retrieves a job by ID
func (s *OrchestratorService) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return s.queue.GetJob(jobID)
}

// CancelJob cancels a job
func (s *OrchestratorService) CancelJob(ctx context.Context, jobID, reason string) error {
	return s.queue.CancelJob(jobID)
}

// ListJobs lists jobs with filtering
func (s *OrchestratorService) ListJobs(ctx context.Context, userID string, status JobStatus, page, pageSize int) ([]*Job, int) {
	return s.queue.ListJobs(userID, status, page, pageSize)
}

// CreateValidationPipeline creates a validation-only pipeline
func (s *OrchestratorService) CreateValidationPipeline(ctx context.Context, req CreateValidationPipelineRequest) (*Pipeline, error) {
	pipelineID := "pipeline_" + uuid.New().String()[:8]

	stages := []PipelineStage{
		{Name: "diversity_analysis", Status: StatusQueued, EstimatedTime: 14400},
		{Name: "cascade_training", Status: StatusQueued, EstimatedTime: 108000},
		{Name: "report_generation", Status: StatusQueued, EstimatedTime: 7200},
	}

	pipeline := &Pipeline{
		ID:                  pipelineID,
		UserID:              req.UserID,
		DatasetID:           req.DatasetID,
		DatasetPath:         req.DatasetPath,
		Status:              StatusQueued,
		Stages:              stages,
		JobIDs:              []string{},
		Results:             make(map[string]any),
		CreatedAt:           time.Now(),
		EstimatedCompletion: time.Now().Add(time.Hour * 42), // ~42 hours for validation
	}

	// Register pipeline
	s.pipelineManager.AddPipeline(pipeline)

	// Start pipeline execution asynchronously
	go s.executeValidationPipeline(context.Background(), pipeline, req)

	return pipeline, nil
}

// CreateFullPipeline creates a full pipeline with validation and collapse detection
func (s *OrchestratorService) CreateFullPipeline(ctx context.Context, req CreateFullPipelineRequest) (*Pipeline, error) {
	pipelineID := "pipeline_" + uuid.New().String()[:8]

	stages := []PipelineStage{}

	if req.EnableValidation {
		stages = append(stages,
			PipelineStage{Name: "diversity_analysis", Status: StatusQueued, EstimatedTime: 14400},
			PipelineStage{Name: "cascade_training", Status: StatusQueued, EstimatedTime: 108000},
		)
	}

	if req.EnableCollapse {
		stages = append(stages,
			PipelineStage{Name: "collapse_detection", Status: StatusQueued, EstimatedTime: 21600},
			PipelineStage{Name: "collapse_localization", Status: StatusQueued, EstimatedTime: 10800},
		)
	}

	if req.EnableRecommendations {
		stages = append(stages,
			PipelineStage{Name: "recommendations", Status: StatusQueued, EstimatedTime: 7200},
		)
	}

	stages = append(stages, PipelineStage{Name: "report_generation", Status: StatusQueued, EstimatedTime: 7200})

	pipeline := &Pipeline{
		ID:                  pipelineID,
		UserID:              req.UserID,
		DatasetID:           req.DatasetID,
		DatasetPath:         req.DatasetPath,
		Status:              StatusQueued,
		Stages:              stages,
		JobIDs:              []string{},
		Results:             make(map[string]any),
		CreatedAt:           time.Now(),
		EstimatedCompletion: time.Now().Add(time.Hour * 48),
	}

	// Register pipeline
	s.pipelineManager.AddPipeline(pipeline)

	// Start pipeline execution asynchronously
	go s.executeFullPipeline(context.Background(), pipeline, req)

	return pipeline, nil
}

// GetPipeline retrieves pipeline status
func (s *OrchestratorService) GetPipeline(ctx context.Context, pipelineID string) (*Pipeline, error) {
	return s.pipelineManager.GetPipeline(pipelineID)
}

// GetResourceStatus returns current resource status
func (s *OrchestratorService) GetResourceStatus(ctx context.Context) map[string]any {
	stats := s.queue.GetQueueStats()
	resourceStats := s.resourceManager.GetStats()

	return map[string]any{
		"total_workers":    resourceStats["total_workers"],
		"active_workers":   resourceStats["active_workers"],
		"idle_workers":     resourceStats["idle_workers"],
		"queued_jobs":      stats["queued"],
		"running_jobs":     stats["running"],
		"cpu_usage":        resourceStats["cpu_usage"],
		"memory_usage":     resourceStats["memory_usage"],
		"gpu_available":    resourceStats["gpu_available"],
		"gpu_in_use":       resourceStats["gpu_in_use"],
		"total_jobs_today": stats["total"],
	}
}

// GetQueueStats returns queue statistics
func (s *OrchestratorService) GetQueueStats() map[string]int {
	return s.queue.GetQueueStats()
}

// Stop gracefully stops the orchestrator
func (s *OrchestratorService) Stop() {
	log.Println("Stopping orchestrator service...")
	s.queue.Stop()

	// Close gRPC connections
	if s.validationConn != nil {
		s.validationConn.Close()
	}
	if s.collapseConn != nil {
		s.collapseConn.Close()
	}
	if s.dataConn != nil {
		s.dataConn.Close()
	}

	log.Println("Orchestrator service stopped")
}

// Job execution methods
func (s *OrchestratorService) executeValidationJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing validation job %s", job.ID)

	// Allocate resources (1GB memory, 0 GPUs for non-GPU mode)
	allocated := s.resourceManager.AllocateResources(job.ID, 1, 1024, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build cascade request using new unified proto
	req := &synthospb.CascadeRequest{
		JobId:       job.ID,
		DatasetId:   job.Payload["dataset_id"],
		SampleS3Path: job.Payload["dataset_path"],
		Config: &synthospb.CascadeConfig{
			NumEpochs:    5,
			BatchSize:    32,
			LearningRate: 0.001,
			UseMultiGpu:  false,
			NumGpus:      1,
			Tiers: []*synthospb.ModelTier{
				{TierNumber: 1, TierName: "light", ModelSize: 1000000},
				{TierNumber: 2, TierName: "medium", ModelSize: 10000000},
				{TierNumber: 3, TierName: "heavy", ModelSize: 100000000},
			},
		},
	}

	// Call validation service - TrainCascade is now a streaming RPC
	stream, err := s.validationClient.TrainCascade(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("validation service error: %w", err)
	}

	// Collect streaming results
	var lastProgress *synthospb.CascadeProgress
	var totalModelsCompleted int32
	var bestAccuracy float64
	var bestModel string

	for {
		progress, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("stream error: %w", err)
		}

		lastProgress = progress
		totalModelsCompleted = progress.ModelsCompleted

		// Track best model
		if progress.Result != nil && progress.Result.ValidationAccuracy > bestAccuracy {
			bestAccuracy = progress.Result.ValidationAccuracy
			bestModel = progress.Result.ModelName
		}

		// Check for errors
		if progress.Status == "failed" && progress.Error != nil {
			return nil, fmt.Errorf("validation failed: %s", progress.Error.Message)
		}

		log.Printf("Cascade progress: tier %d, variant %d, %d/%d models (%.1f%%)",
			progress.CurrentTier, progress.CurrentVariant,
			progress.ModelsCompleted, progress.ModelsTotal, progress.ProgressPercent)
	}

	// Build result
	result := map[string]string{
		"status":           "completed",
		"job_id":           job.ID,
		"models_completed": fmt.Sprintf("%d", totalModelsCompleted),
		"best_accuracy":    fmt.Sprintf("%.4f", bestAccuracy),
		"best_model":       bestModel,
	}

	if lastProgress != nil {
		result["final_progress"] = fmt.Sprintf("%.1f%%", lastProgress.ProgressPercent)
	}

	log.Printf("Validation job %s completed successfully", job.ID)
	return result, nil
}

func (s *OrchestratorService) executeCollapseJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing collapse detection job %s", job.ID)

	// Allocate resources (2GB memory, 0 GPUs for non-GPU mode)
	allocated := s.resourceManager.AllocateResources(job.ID, 1, 2048, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build collapse detection request using new unified proto
	req := &synthospb.CollapseRequest{
		JobId:       job.ID,
		DatasetId:   job.Payload["dataset_id"],
		DatasetPath: job.Payload["dataset_path"],
		Config: &synthospb.CollapseConfig{
			ChunkSize: 10000,
			UseGpu:    true,
			NumGpus:   1,
			DimensionThresholds: map[string]float32{
				"distribution_fidelity":    0.7,
				"correlation_preservation": 0.7,
				"diversity_retention":      0.7,
			},
		},
	}

	// Call collapse service
	resp, err := s.collapseClient.DetectCollapse(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("collapse service error: %w", err)
	}

	if resp.Error != nil && resp.Error.Message != "" {
		return nil, fmt.Errorf("collapse detection failed: %s", resp.Error.Message)
	}

	// Build result
	result := map[string]string{
		"job_id":            resp.JobId,
		"overall_score":     fmt.Sprintf("%.2f", resp.OverallScore),
		"collapse_detected": fmt.Sprintf("%t", resp.CollapseDetected),
		"severity":          resp.Severity,
		"collapse_type":     resp.CollapseType,
	}

	log.Printf("Collapse detection job %s completed successfully", job.ID)
	return result, nil
}

func (s *OrchestratorService) executeDataProcessingJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing data processing job %s", job.ID)

	// Allocate resources (1GB memory for data processing)
	allocated := s.resourceManager.AllocateResources(job.ID, 1, 1024, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build data processing request - commented out until proto is updated
	// req := &datapb.ProcessDatasetRequest{
	// 	DatasetId: job.Payload["dataset_id"],
	// 	Options: &datapb.ProcessingOptions{
	// 		Format:          job.Payload["data_format"],
	// 		ChunkSize:       10000,
	// 		ValidateOnly:    false,
	// 		EnableProfiling: true,
	// 	},
	// }

	// Call data service
	// resp, err := s.dataClient.ProcessDataset(ctx, req)
	// if err != nil {
	// 	return nil, fmt.Errorf("data service error: %w", err)
	// }

	// if !resp.Success {
	// 	return nil, fmt.Errorf("data processing failed: %s", resp.Message)
	// }

	// Build result (placeholder)
	result := map[string]string{
		"status":  "completed",
		"message": "Data processing placeholder",
	}

	log.Printf("Data processing job %s completed successfully", job.ID)
	return result, nil
}

func (s *OrchestratorService) executeDiversityAnalysisJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing diversity analysis job %s", job.ID)

	allocated := s.resourceManager.AllocateResources(job.ID, 1, 1024, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build diversity request using new unified proto
	req := &synthospb.DiversityRequest{
		JobId:     job.ID,
		DatasetId: job.Payload["dataset_id"],
		S3Path:    job.Payload["dataset_path"],
		Format:    synthospb.DataFormat_CSV, // Default to CSV
	}

	resp, err := s.validationClient.AnalyzeDiversity(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("diversity analysis error: %w", err)
	}

	if resp.Status == "failed" {
		if resp.Error != nil {
			return nil, fmt.Errorf("diversity analysis failed: %s", resp.Error.Message)
		}
		return nil, fmt.Errorf("diversity analysis failed")
	}

	result := map[string]string{
		"status":        resp.Status,
		"overall_score": fmt.Sprintf("%.2f", resp.Metrics.OverallScore),
		"spread_score":  fmt.Sprintf("%.2f", resp.Metrics.SpreadScore),
		"balance_score": fmt.Sprintf("%.2f", resp.Metrics.BalanceScore),
	}

	return result, nil
}

func (s *OrchestratorService) executeCollapseLocalizationJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing collapse localization job %s", job.ID)

	allocated := s.resourceManager.AllocateResources(job.ID, 1, 1536, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build localization request using new unified proto
	req := &synthospb.LocalizationRequest{
		JobId:       job.ID,
		DatasetId:   job.Payload["dataset_id"],
		DatasetPath: job.Payload["dataset_path"],
		Config: &synthospb.LocalizationConfig{
			ChunkSize:   10000,
			TopKRegions: 10,
			UseGpu:      true,
		},
	}

	resp, err := s.collapseClient.LocalizeProblems(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("localization error: %w", err)
	}

	if resp.Error != nil && resp.Error.Message != "" {
		return nil, fmt.Errorf("localization failed: %s", resp.Error.Message)
	}

	result := map[string]string{
		"status":        "completed",
		"regions_found": fmt.Sprintf("%d", len(resp.Regions)),
	}

	return result, nil
}

func (s *OrchestratorService) executeRecommendationsJob(ctx context.Context, job *Job) (map[string]string, error) {
	log.Printf("Executing recommendations job %s", job.ID)

	allocated := s.resourceManager.AllocateResources(job.ID, 1, 512, 0)
	if !allocated {
		return nil, fmt.Errorf("failed to allocate resources")
	}
	defer s.resourceManager.ReleaseResources(job.ID)

	// Build recommendations request using new unified proto
	req := &synthospb.RecommendationRequest{
		JobId:       job.ID,
		DatasetId:   job.Payload["dataset_id"],
		DatasetPath: job.Payload["dataset_path"],
		Config: &synthospb.RecommendationConfig{
			MaxRecommendations:        5,
			IncludeImpactEstimates:    true,
			IncludeImplementationCode: true,
		},
	}

	resp, err := s.collapseClient.GenerateRecommendations(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("recommendations error: %w", err)
	}

	if resp.Error != nil && resp.Error.Message != "" {
		return nil, fmt.Errorf("recommendations failed: %s", resp.Error.Message)
	}

	result := map[string]string{
		"status":                "completed",
		"recommendations_count": fmt.Sprintf("%d", len(resp.Recommendations)),
	}

	return result, nil
}
