package orchestrator

import (
	"context"
	"time"
)

// Client wraps HTTPClient with domain-specific methods
type Client struct {
	http *HTTPClient
}

// NewClient creates a new orchestrator client
func NewClient(baseURL string) (*Client, error) {
	return &Client{
		http: NewHTTPClient(baseURL, 30*time.Second),
	}, nil
}

// Close closes the client
func (c *Client) Close() error {
	// No resources to clean up for HTTP client
	return nil
}

// ResourceStatus represents the orchestrator resource status
type ResourceStatus struct {
	TotalWorkers    int `json:"total_workers"`
	ActiveWorkers   int `json:"active_workers"`
	QueuedJobs      int `json:"queued_jobs"`
	RunningJobs     int `json:"running_jobs"`
	AvailableMemory int `json:"available_memory"`
	AvailableGPUs   int `json:"available_gpus"`
}

// GetResourceStatus retrieves the current resource status
func (c *Client) GetResourceStatus(ctx context.Context) (*ResourceStatus, error) {
	var status ResourceStatus
	err := c.http.Get(ctx, "/api/v1/status", &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

// CreateJobRequest represents a job creation request
type CreateJobRequest struct {
	UserID   string            `json:"user_id"`
	JobType  string            `json:"job_type"`
	Priority int               `json:"priority"`
	Payload  map[string]string `json:"payload"`
}

// CreateJobResponse represents a job creation response
type CreateJobResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// CreateJob creates a new job
func (c *Client) CreateJob(ctx context.Context, req *CreateJobRequest) (*CreateJobResponse, error) {
	var resp CreateJobResponse
	err := c.http.Post(ctx, "/api/v1/jobs", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// JobStatus represents a job status response
type JobStatus struct {
	JobID     string            `json:"job_id"`
	Status    string            `json:"status"`
	Progress  float64           `json:"progress"`
	Result    map[string]string `json:"result,omitempty"`
	Error     string            `json:"error,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// GetJobStatus retrieves the status of a job
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (*JobStatus, error) {
	var status JobStatus
	err := c.http.Get(ctx, "/api/v1/jobs/"+jobID, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

// GetJob is an alias for GetJobStatus
func (c *Client) GetJob(ctx context.Context, jobID string) (*JobStatus, error) {
	return c.GetJobStatus(ctx, jobID)
}

// CancelJobResponse represents a cancel job response
type CancelJobResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// CancelJob cancels a running or queued job
func (c *Client) CancelJob(ctx context.Context, jobID string) (*CancelJobResponse, error) {
	var resp CancelJobResponse
	err := c.http.Delete(ctx, "/api/v1/jobs/"+jobID, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListJobsResponse represents a list jobs response
type ListJobsResponse struct {
	Jobs       []JobStatus `json:"jobs"`
	TotalCount int         `json:"total_count"`
}

// ListJobs lists all jobs for a user
func (c *Client) ListJobs(ctx context.Context, userID string) (*ListJobsResponse, error) {
	var resp ListJobsResponse
	err := c.http.Get(ctx, "/api/v1/jobs?user_id="+userID, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
