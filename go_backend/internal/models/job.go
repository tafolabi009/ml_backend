package models

import (
	"time"
)

// Job represents an orchestrator job in the database
type Job struct {
	ID           string                 `json:"id" db:"id"`
	UserID       string                 `json:"user_id" db:"user_id"`
	Type         string                 `json:"type" db:"type"` // validation, collapse, pipeline
	Status       string                 `json:"status" db:"status"` // pending, running, completed, failed, cancelled
	Priority     int                    `json:"priority" db:"priority"`
	DatasetPath  string                 `json:"dataset_path" db:"dataset_path"`
	Config       map[string]interface{} `json:"config" db:"config"` // JSONB
	Result       map[string]interface{} `json:"result,omitempty" db:"result"` // JSONB
	ErrorMessage string                 `json:"error_message,omitempty" db:"error_message"`
	GPURequested int                    `json:"gpu_requested" db:"gpu_requested"`
	GPUAllocated int                    `json:"gpu_allocated" db:"gpu_allocated"`
	RetryCount   int                    `json:"retry_count" db:"retry_count"`
	MaxRetries   int                    `json:"max_retries" db:"max_retries"`
	Progress     float32                `json:"progress" db:"progress"`
	CreatedAt    time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at" db:"updated_at"`
	StartedAt    *time.Time             `json:"started_at,omitempty" db:"started_at"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty" db:"completed_at"`
}

// CreateJobRequest is the request to create a new job
type CreateJobRequest struct {
	Type        string                 `json:"type" binding:"required"`
	DatasetPath string                 `json:"dataset_path" binding:"required"`
	Priority    int                    `json:"priority"`
	Config      map[string]interface{} `json:"config"`
}

// CreateJobResponse contains the created job info
type CreateJobResponse struct {
	JobID               string    `json:"job_id"`
	Type                string    `json:"type"`
	Status              string    `json:"status"`
	EstimatedCompletion time.Time `json:"estimated_completion"`
}

// JobStatusResponse contains job status details
type JobStatusResponse struct {
	JobID        string                 `json:"job_id"`
	Type         string                 `json:"type"`
	Status       string                 `json:"status"`
	Progress     float32                `json:"progress"`
	Result       map[string]interface{} `json:"result,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
}

// JobListResponse is the paginated list of jobs
type JobListResponse struct {
	Jobs       []Job      `json:"jobs"`
	Pagination Pagination `json:"pagination"`
}
