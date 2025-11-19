package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Pipeline represents an orchestrated workflow
type Pipeline struct {
	ID                  string
	UserID              string
	DatasetID           string
	DatasetPath         string
	Status              JobStatus
	CurrentStage        string
	Progress            float32
	Stages              []PipelineStage
	JobIDs              []string
	Results             map[string]any
	ErrorMessage        string
	CreatedAt           time.Time
	CompletedAt         *time.Time
	EstimatedCompletion time.Time
}

// PipelineStage represents a stage in the pipeline
type PipelineStage struct {
	Name          string
	Status        JobStatus
	Progress      float32
	JobID         string
	StartedAt     *time.Time
	CompletedAt   *time.Time
	EstimatedTime int32
	Result        map[string]any
}

// PipelineManager manages pipeline lifecycles
type PipelineManager struct {
	mu        sync.RWMutex
	pipelines map[string]*Pipeline
}

// NewPipelineManager creates a new pipeline manager
func NewPipelineManager() *PipelineManager {
	return &PipelineManager{
		pipelines: make(map[string]*Pipeline),
	}
}

// AddPipeline registers a new pipeline
func (pm *PipelineManager) AddPipeline(pipeline *Pipeline) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pipelines[pipeline.ID] = pipeline
}

// GetPipeline retrieves a pipeline by ID
func (pm *PipelineManager) GetPipeline(pipelineID string) (*Pipeline, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pipeline, exists := pm.pipelines[pipelineID]
	if !exists {
		return nil, fmt.Errorf("pipeline not found: %s", pipelineID)
	}

	return pipeline, nil
}

// UpdatePipelineStatus updates pipeline status
func (pm *PipelineManager) UpdatePipelineStatus(pipelineID string, status JobStatus, currentStage string, progress float32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pipeline, exists := pm.pipelines[pipelineID]
	if !exists {
		return fmt.Errorf("pipeline not found: %s", pipelineID)
	}

	pipeline.Status = status
	pipeline.CurrentStage = currentStage
	pipeline.Progress = progress

	if status == StatusCompleted || status == StatusFailed {
		now := time.Now()
		pipeline.CompletedAt = &now
	}

	return nil
}

// UpdateStageStatus updates a specific stage status
func (pm *PipelineManager) UpdateStageStatus(pipelineID, stageName string, status JobStatus, progress float32, result map[string]any) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pipeline, exists := pm.pipelines[pipelineID]
	if !exists {
		return fmt.Errorf("pipeline not found: %s", pipelineID)
	}

	for i := range pipeline.Stages {
		if pipeline.Stages[i].Name == stageName {
			pipeline.Stages[i].Status = status
			pipeline.Stages[i].Progress = progress

			if result != nil {
				pipeline.Stages[i].Result = result
				// Merge into pipeline results
				if pipeline.Results == nil {
					pipeline.Results = make(map[string]any)
				}
				pipeline.Results[stageName] = result
			}

			now := time.Now()
			if status == StatusRunning && pipeline.Stages[i].StartedAt == nil {
				pipeline.Stages[i].StartedAt = &now
			}

			if status == StatusCompleted || status == StatusFailed {
				pipeline.Stages[i].CompletedAt = &now
			}

			return nil
		}
	}

	return fmt.Errorf("stage not found: %s", stageName)
}

// Pipeline execution methods
func (s *OrchestratorService) executeValidationPipeline(ctx context.Context, pipeline *Pipeline, req CreateValidationPipelineRequest) {
	log.Printf("Starting validation pipeline %s", pipeline.ID)

	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusRunning, "diversity_analysis", 0)

	// Stage 1: Diversity Analysis
	if err := s.executePipelineStage(ctx, pipeline, "diversity_analysis", req.DatasetPath, req.DataFormat, req.Metadata); err != nil {
		s.handlePipelineError(pipeline, "diversity_analysis", err)
		return
	}

	// Stage 2: Cascade Training
	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusRunning, "cascade_training", 33)
	if err := s.executePipelineStage(ctx, pipeline, "cascade_training", req.DatasetPath, req.DataFormat, req.Metadata); err != nil {
		s.handlePipelineError(pipeline, "cascade_training", err)
		return
	}

	// Stage 3: Report Generation
	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusRunning, "report_generation", 80)
	if err := s.executePipelineStage(ctx, pipeline, "report_generation", req.DatasetPath, req.DataFormat, req.Metadata); err != nil {
		s.handlePipelineError(pipeline, "report_generation", err)
		return
	}

	// Complete pipeline
	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusCompleted, "completed", 100)
	log.Printf("Validation pipeline %s completed successfully", pipeline.ID)
}

func (s *OrchestratorService) executeFullPipeline(ctx context.Context, pipeline *Pipeline, req CreateFullPipelineRequest) {
	log.Printf("Starting full pipeline %s", pipeline.ID)

	totalStages := len(pipeline.Stages)
	currentStageNum := 0

	for _, stage := range pipeline.Stages {
		currentStageNum++
		progress := float32(currentStageNum-1) / float32(totalStages) * 100

		s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusRunning, stage.Name, progress)

		if err := s.executePipelineStage(ctx, pipeline, stage.Name, req.DatasetPath, req.DataFormat, req.Metadata); err != nil {
			s.handlePipelineError(pipeline, stage.Name, err)
			return
		}
	}

	// Complete pipeline
	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusCompleted, "completed", 100)
	log.Printf("Full pipeline %s completed successfully", pipeline.ID)
}

func (s *OrchestratorService) executePipelineStage(ctx context.Context, pipeline *Pipeline, stageName, datasetPath, dataFormat string, metadata map[string]string) error {
	log.Printf("Executing stage %s for pipeline %s", stageName, pipeline.ID)

	s.pipelineManager.UpdateStageStatus(pipeline.ID, stageName, StatusRunning, 0, nil)

	// Map stage name to job type
	jobType := stageName
	if stageName == "cascade_training" {
		jobType = "validation"
	}

	// Create payload
	payload := map[string]string{
		"dataset_path": datasetPath,
		"data_format":  dataFormat,
		"dataset_id":   pipeline.DatasetID,
		"pipeline_id":  pipeline.ID,
	}
	for k, v := range metadata {
		payload[k] = v
	}

	// Create and execute job
	job, _, err := s.CreateJob(ctx, pipeline.UserID, jobType, 5, payload)
	if err != nil {
		return fmt.Errorf("failed to create job for stage %s: %w", stageName, err)
	}

	// Store job ID
	s.mu.Lock()
	pipeline.JobIDs = append(pipeline.JobIDs, job.ID)
	s.mu.Unlock()

	// Poll job status until completion
	for {
		time.Sleep(5 * time.Second)

		currentJob, err := s.GetJob(ctx, job.ID)
		if err != nil {
			return fmt.Errorf("failed to get job status: %w", err)
		}

		// Update stage progress
		s.pipelineManager.UpdateStageStatus(pipeline.ID, stageName, currentJob.Status, currentJob.Progress, nil)

		if currentJob.Status == StatusCompleted {
			// Convert result to map[string]any
			result := make(map[string]any)
			for k, v := range currentJob.Result {
				result[k] = v
			}
			s.pipelineManager.UpdateStageStatus(pipeline.ID, stageName, StatusCompleted, 100, result)
			log.Printf("Stage %s completed for pipeline %s", stageName, pipeline.ID)
			return nil
		}

		if currentJob.Status == StatusFailed {
			return fmt.Errorf("job failed: %s", currentJob.ErrorMessage)
		}

		if currentJob.Status == StatusCancelled {
			return fmt.Errorf("job was cancelled")
		}
	}
}

func (s *OrchestratorService) handlePipelineError(pipeline *Pipeline, stageName string, err error) {
	log.Printf("Pipeline %s failed at stage %s: %v", pipeline.ID, stageName, err)

	s.mu.Lock()
	pipeline.Status = StatusFailed
	pipeline.ErrorMessage = fmt.Sprintf("Failed at stage %s: %v", stageName, err)
	now := time.Now()
	pipeline.CompletedAt = &now
	s.mu.Unlock()

	s.pipelineManager.UpdateStageStatus(pipeline.ID, stageName, StatusFailed, 0, nil)
	s.pipelineManager.UpdatePipelineStatus(pipeline.ID, StatusFailed, stageName, 0)
}
