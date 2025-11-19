package service

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// generateJobID generates a unique job ID
func generateJobID() string {
	return uuid.New().String()
}

// JobStatus represents job states
type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusRunning    JobStatus = "running"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusCancelled  JobStatus = "cancelled"
)

// Job represents a job in the system
type Job struct {
	ID               string
	UserID           string
	JobType          string
	Status           JobStatus
	Priority         int32
	Payload          map[string]string
	Result           map[string]string
	ErrorMessage     string
	Progress         float32
	CreatedAt        time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	RetryCount       int
	MaxRetries       int
}

// JobQueue manages job scheduling and execution
type JobQueue struct {
	mu           sync.RWMutex
	jobs         map[string]*Job
	queue        []*Job
	workers      int
	stopChan     chan struct{}
	workerChan   chan *Job
	jobCompleted chan string
	jobCallbacks map[string]JobExecutor // Map of job type to executor
}

// JobExecutor is a function that executes a job
type JobExecutor func(ctx context.Context, job *Job) (map[string]string, error)

// NewJobQueue creates a new job queue with specified number of workers
func NewJobQueue(workers int) *JobQueue {
	jq := &JobQueue{
		jobs:         make(map[string]*Job),
		queue:        make([]*Job, 0),
		workers:      workers,
		stopChan:     make(chan struct{}),
		workerChan:   make(chan *Job, workers*2), // Buffer for workers
		jobCompleted: make(chan string, 100),
		jobCallbacks: make(map[string]JobExecutor),
	}

	// Start worker pool
	for i := 0; i < workers; i++ {
		go jq.worker(i)
	}

	// Start queue processor
	go jq.processQueue()

	log.Printf("Job queue initialized with %d workers", workers)
	return jq
}

// RegisterExecutor registers a job executor for a specific job type
func (jq *JobQueue) RegisterExecutor(jobType string, executor JobExecutor) {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	jq.jobCallbacks[jobType] = executor
	log.Printf("Registered executor for job type: %s", jobType)
}

// CreateJob creates a new job and adds it to the queue
func (jq *JobQueue) CreateJob(userID, jobType string, priority int32, payload map[string]string) (*Job, error) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	job := &Job{
		ID:         generateJobID(),
		UserID:     userID,
		JobType:    jobType,
		Priority:   priority,
		Status:     StatusQueued,
		Payload:    payload,
		Result:     make(map[string]string),
		CreatedAt:  time.Now(),
		RetryCount: 0,
		MaxRetries: 3,
	}

	jq.jobs[job.ID] = job
	jq.queue = append(jq.queue, job)

	// Sort queue by priority (higher priority first)
	sort.Slice(jq.queue, func(i, j int) bool {
		return jq.queue[i].Priority > jq.queue[j].Priority
	})

	log.Printf("Job created: id=%s, type=%s, priority=%d", job.ID, jobType, priority)
	return job, nil
}

// GetJob retrieves a job by ID
func (jq *JobQueue) GetJob(jobID string) (*Job, error) {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	job, exists := jq.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return job, nil
}

// ListJobs returns jobs for a user with optional status filter
func (jq *JobQueue) ListJobs(userID string, statusFilter JobStatus, page, pageSize int) ([]*Job, int) {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	filtered := make([]*Job, 0)
	for _, job := range jq.jobs {
		if job.UserID == userID {
			if statusFilter == "" || job.Status == statusFilter {
				filtered = append(filtered, job)
			}
		}
	}

	totalCount := len(filtered)
	
	// Pagination
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > len(filtered) {
		return []*Job{}, totalCount
	}
	if end > len(filtered) {
		end = len(filtered)
	}

	return filtered[start:end], totalCount
}

// CancelJob cancels a job
func (jq *JobQueue) CancelJob(jobID string) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	job, exists := jq.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status == StatusCompleted || job.Status == StatusFailed {
		return fmt.Errorf("cannot cancel job in status: %s", job.Status)
	}

	job.Status = StatusCancelled
	now := time.Now()
	job.CompletedAt = &now

	// Remove from queue if queued
	jq.removeFromQueue(jobID)

	log.Printf("Cancelled job %s", jobID)
	return nil
}

// UpdateJobStatus updates job status and result
func (jq *JobQueue) UpdateJobStatus(jobID string, status JobStatus, result map[string]string, errorMsg string, progress float32) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	job, exists := jq.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	job.Status = status
	job.Progress = progress
	if result != nil {
		job.Result = result
	}
	if errorMsg != "" {
		job.ErrorMessage = errorMsg
	}

	if status == StatusRunning && job.StartedAt == nil {
		now := time.Now()
		job.StartedAt = &now
	}

	if status == StatusCompleted || status == StatusFailed {
		now := time.Now()
		job.CompletedAt = &now
	}

	return nil
}

// sortQueue sorts queue by priority (higher first)
func (jq *JobQueue) sortQueue() {
	for i := 0; i < len(jq.queue); i++ {
		for j := i + 1; j < len(jq.queue); j++ {
			if jq.queue[j].Priority > jq.queue[i].Priority {
				jq.queue[i], jq.queue[j] = jq.queue[j], jq.queue[i]
			}
		}
	}
}

// removeFromQueue removes a job from the queue
func (jq *JobQueue) removeFromQueue(jobID string) {
	for i, job := range jq.queue {
		if job.ID == jobID {
			jq.queue = append(jq.queue[:i], jq.queue[i+1:]...)
			break
		}
	}
}

// processQueue continuously processes the job queue
func (jq *JobQueue) processQueue() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-jq.stopChan:
			return
		case <-ticker.C:
			jq.mu.Lock()
			if len(jq.queue) > 0 {
				// Get highest priority job
				job := jq.queue[0]
				jq.queue = jq.queue[1:]
				
				// Send to worker
				select {
				case jq.workerChan <- job:
					job.Status = StatusRunning
					log.Printf("Dispatched job %s to worker", job.ID)
				default:
					// Worker channel full, put back in queue
					jq.queue = append([]*Job{job}, jq.queue...)
				}
			}
			jq.mu.Unlock()
		}
	}
}

// worker processes jobs from the worker channel
func (jq *JobQueue) worker(id int) {
	log.Printf("Worker %d started", id)
	
	for {
		select {
		case <-jq.stopChan:
			log.Printf("Worker %d stopped", id)
			return
		case job := <-jq.workerChan:
			log.Printf("Worker %d processing job %s", id, job.ID)
			jq.executeJob(job)
		}
	}
}

// executeJob executes a job using the registered executor
func (jq *JobQueue) executeJob(job *Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Get executor
	jq.mu.RLock()
	executor, exists := jq.jobCallbacks[job.JobType]
	jq.mu.RUnlock()

	if !exists {
		log.Printf("No executor found for job type: %s", job.JobType)
		jq.UpdateJobStatus(job.ID, StatusFailed, nil, "No executor found for job type", 0)
		return
	}

	// Update to running
	jq.UpdateJobStatus(job.ID, StatusRunning, nil, "", 0)

	// Execute job
	result, err := executor(ctx, job)
	
	if err != nil {
		log.Printf("Job %s failed: %v", job.ID, err)
		
		// Retry logic
		if job.RetryCount < job.MaxRetries {
			jq.mu.Lock()
			job.RetryCount++
			job.Status = StatusQueued
			jq.queue = append(jq.queue, job)
			jq.sortQueue()
			jq.mu.Unlock()
			log.Printf("Job %s queued for retry (%d/%d)", job.ID, job.RetryCount, job.MaxRetries)
		} else {
			jq.UpdateJobStatus(job.ID, StatusFailed, nil, err.Error(), 0)
		}
	} else {
		log.Printf("Job %s completed successfully", job.ID)
		jq.UpdateJobStatus(job.ID, StatusCompleted, result, "", 100)
	}
}

// Stop gracefully stops the job queue
func (jq *JobQueue) Stop() {
	close(jq.stopChan)
	log.Println("Job queue stopped")
}

// GetQueueStats returns queue statistics
func (jq *JobQueue) GetQueueStats() map[string]int {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	stats := map[string]int{
		"total":     len(jq.jobs),
		"queued":    0,
		"running":   0,
		"completed": 0,
		"failed":    0,
		"cancelled": 0,
	}

	for _, job := range jq.jobs {
		switch job.Status {
		case StatusQueued:
			stats["queued"]++
		case StatusRunning:
			stats["running"]++
		case StatusCompleted:
			stats["completed"]++
		case StatusFailed:
			stats["failed"]++
		case StatusCancelled:
			stats["cancelled"]++
		}
	}

	return stats
}
