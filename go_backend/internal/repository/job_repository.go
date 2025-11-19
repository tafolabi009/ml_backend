package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
)

// JobRepository handles job persistence
type JobRepository struct {
	db  *pgxpool.Pool
	log *logger.Logger
}

// NewJobRepository creates a new job repository
func NewJobRepository(db *pgxpool.Pool) *JobRepository {
	return &JobRepository{
		db:  db,
		log: logger.Get().With("component", "job-repository"),
	}
}

// CreateJob creates a new job in the database
func (r *JobRepository) CreateJob(ctx context.Context, job *models.Job) error {
	traceID := ctx.Value("trace_id")
	log := r.log.With("trace_id", traceID, "job_id", job.ID)

	config, err := json.Marshal(job.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	query := `
		INSERT INTO jobs (
			id, user_id, type, status, priority, dataset_path,
			config, gpu_requested, gpu_allocated, max_retries,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
	`

	_, err = r.db.Exec(ctx, query,
		job.ID, job.UserID, job.Type, job.Status, job.Priority,
		job.DatasetPath, config, job.GPURequested, job.GPUAllocated,
		job.MaxRetries, job.CreatedAt, job.UpdatedAt,
	)

	if err != nil {
		log.Error("Failed to create job", zap.Error(err))
		return fmt.Errorf("failed to create job: %w", err)
	}

	log.Info("Job created successfully")
	return nil
}

// GetJob retrieves a job by ID
func (r *JobRepository) GetJob(ctx context.Context, jobID string) (*models.Job, error) {
	log := r.log.With("job_id", jobID)

	query := `
		SELECT 
			id, user_id, type, status, priority, dataset_path, 
			config, result, error_message, gpu_requested, gpu_allocated,
			retry_count, max_retries, progress, created_at, updated_at,
			started_at, completed_at
		FROM jobs
		WHERE id = $1
	`

	var job models.Job
	var configJSON []byte
	var resultJSON sql.NullString
	var startedAt, completedAt sql.NullTime

	err := r.db.QueryRow(ctx, query, jobID).Scan(
		&job.ID, &job.UserID, &job.Type, &job.Status, &job.Priority,
		&job.DatasetPath, &configJSON, &resultJSON, &job.ErrorMessage,
		&job.GPURequested, &job.GPUAllocated, &job.RetryCount,
		&job.MaxRetries, &job.Progress, &job.CreatedAt, &job.UpdatedAt,
		&startedAt, &completedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", jobID)
		}
		log.Error("Failed to get job", zap.Error(err))
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Unmarshal config
	if err := json.Unmarshal(configJSON, &job.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Unmarshal result if present
	if resultJSON.Valid {
		if err := json.Unmarshal([]byte(resultJSON.String), &job.Result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal result: %w", err)
		}
	}

	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}

	return &job, nil
}

// UpdateJobStatus updates job status and progress
func (r *JobRepository) UpdateJobStatus(ctx context.Context, jobID string, status string, progress float32, errorMsg string) error {
	log := r.log.With("job_id", jobID, "status", status)

	query := `
		UPDATE jobs
		SET status = $1, progress = $2, error_message = $3, updated_at = $4
		WHERE id = $5
	`

	_, err := r.db.Exec(ctx, query, status, progress, errorMsg, time.Now(), jobID)
	if err != nil {
		log.Error("Failed to update job status", zap.Error(err))
		return fmt.Errorf("failed to update job status: %w", err)
	}

	log.Info("Job status updated", zap.Float32("progress", progress))
	return nil
}

// UpdateJobResult updates job result
func (r *JobRepository) UpdateJobResult(ctx context.Context, jobID string, result map[string]interface{}) error {
	log := r.log.With("job_id", jobID)

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	query := `
		UPDATE jobs
		SET result = $1, status = $2, completed_at = $3, updated_at = $4
		WHERE id = $5
	`

	_, err = r.db.Exec(ctx, query, resultJSON, "completed", time.Now(), time.Now(), jobID)
	if err != nil {
		log.Error("Failed to update job result", zap.Error(err))
		return fmt.Errorf("failed to update job result: %w", err)
	}

	log.Info("Job result updated successfully")
	return nil
}

// ListJobs lists jobs with pagination
func (r *JobRepository) ListJobs(ctx context.Context, userID string, limit, offset int) ([]*models.Job, error) {
	query := `
		SELECT 
			id, user_id, type, status, priority, dataset_path,
			config, result, error_message, gpu_requested, gpu_allocated,
			retry_count, max_retries, progress, created_at, updated_at,
			started_at, completed_at
		FROM jobs
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Query(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		var configJSON []byte
		var resultJSON sql.NullString
		var startedAt, completedAt sql.NullTime

		err := rows.Scan(
			&job.ID, &job.UserID, &job.Type, &job.Status, &job.Priority,
			&job.DatasetPath, &configJSON, &resultJSON, &job.ErrorMessage,
			&job.GPURequested, &job.GPUAllocated, &job.RetryCount,
			&job.MaxRetries, &job.Progress, &job.CreatedAt, &job.UpdatedAt,
			&startedAt, &completedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}

		if err := json.Unmarshal(configJSON, &job.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		if resultJSON.Valid {
			if err := json.Unmarshal([]byte(resultJSON.String), &job.Result); err != nil {
				return nil, fmt.Errorf("failed to unmarshal result: %w", err)
			}
		}

		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}

		jobs = append(jobs, &job)
	}

	return jobs, nil
}

// DeleteJob deletes a job (soft delete by updating status)
func (r *JobRepository) DeleteJob(ctx context.Context, jobID string) error {
	query := `
		UPDATE jobs
		SET status = 'deleted', updated_at = $1
		WHERE id = $2
	`

	_, err := r.db.Exec(ctx, query, time.Now(), jobID)
	if err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}

	return nil
}
