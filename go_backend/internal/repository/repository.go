package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// Create creates a new user
func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	query := `
		INSERT INTO users (id, email, password_hash, full_name, company_id, company_name, subscription_tier)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at, updated_at
	`

	err := r.db.QueryRow(ctx, query,
		user.ID,
		user.Email,
		user.PasswordHash,
		user.FullName,
		user.CompanyID,
		user.CompanyName,
		user.SubscriptionTier,
	).Scan(&user.CreatedAt, &user.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

// GetByEmail retrieves a user by email
func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	query := `
		SELECT id, email, password_hash, full_name, company_id, company_name, 
		       subscription_tier, created_at, updated_at
		FROM users
		WHERE email = $1
	`

	user := &models.User{}
	err := r.db.QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.FullName,
		&user.CompanyID,
		&user.CompanyName,
		&user.SubscriptionTier,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

// GetByID retrieves a user by ID
func (r *UserRepository) GetByID(ctx context.Context, userID string) (*models.User, error) {
	query := `
		SELECT id, email, password_hash, full_name, company_id, company_name,
		       subscription_tier, created_at, updated_at
		FROM users
		WHERE id = $1
	`

	user := &models.User{}
	err := r.db.QueryRow(ctx, query, userID).Scan(
		&user.ID,
		&user.Email,
		&user.PasswordHash,
		&user.FullName,
		&user.CompanyID,
		&user.CompanyName,
		&user.SubscriptionTier,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

// Update updates user information
func (r *UserRepository) Update(ctx context.Context, user *models.User) error {
	query := `
		UPDATE users
		SET full_name = $2, company_name = $3, subscription_tier = $4, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING updated_at
	`

	err := r.db.QueryRow(ctx, query,
		user.ID,
		user.FullName,
		user.CompanyName,
		user.SubscriptionTier,
	).Scan(&user.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	return nil
}

type DatasetRepository struct {
	db *pgxpool.Pool
}

func NewDatasetRepository(db *pgxpool.Pool) *DatasetRepository {
	return &DatasetRepository{db: db}
}

// Create creates a new dataset
func (r *DatasetRepository) Create(ctx context.Context, dataset *models.Dataset) error {
	query := `
		INSERT INTO datasets (id, user_id, filename, file_size, file_type, status, s3_path)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at
	`

	err := r.db.QueryRow(ctx, query,
		dataset.ID,
		dataset.UserID,
		dataset.Filename,
		dataset.FileSize,
		dataset.FileType,
		dataset.Status,
		dataset.S3Path,
	).Scan(&dataset.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	return nil
}

// GetByID retrieves a dataset by ID
func (r *DatasetRepository) GetByID(ctx context.Context, datasetID string) (*models.Dataset, error) {
	query := `
		SELECT id, user_id, filename, file_size, file_type, status, s3_path,
		       row_count, column_count, created_at, updated_at
		FROM datasets
		WHERE id = $1
	`

	dataset := &models.Dataset{}
	err := r.db.QueryRow(ctx, query, datasetID).Scan(
		&dataset.ID,
		&dataset.UserID,
		&dataset.Filename,
		&dataset.FileSize,
		&dataset.FileType,
		&dataset.Status,
		&dataset.S3Path,
		&dataset.RowCount,
		&dataset.ColumnCount,
		&dataset.CreatedAt,
		&dataset.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get dataset: %w", err)
	}

	return dataset, nil
}

// List retrieves datasets for a user with pagination
func (r *DatasetRepository) List(ctx context.Context, userID string, page, pageSize int) ([]models.Dataset, int, error) {
	// Get total count
	var totalCount int
	countQuery := `SELECT COUNT(*) FROM datasets WHERE user_id = $1`
	err := r.db.QueryRow(ctx, countQuery, userID).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count datasets: %w", err)
	}

	// Get datasets
	offset := (page - 1) * pageSize
	query := `
		SELECT id, user_id, filename, file_size, file_type, status, s3_path,
		       row_count, column_count, created_at, updated_at
		FROM datasets
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Query(ctx, query, userID, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list datasets: %w", err)
	}
	defer rows.Close()

	datasets := []models.Dataset{}
	for rows.Next() {
		var dataset models.Dataset
		err := rows.Scan(
			&dataset.ID,
			&dataset.UserID,
			&dataset.Filename,
			&dataset.FileSize,
			&dataset.FileType,
			&dataset.Status,
			&dataset.S3Path,
			&dataset.RowCount,
			&dataset.ColumnCount,
			&dataset.CreatedAt,
			&dataset.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan dataset: %w", err)
		}
		datasets = append(datasets, dataset)
	}

	return datasets, totalCount, nil
}

// Update updates dataset status and metadata
func (r *DatasetRepository) Update(ctx context.Context, dataset *models.Dataset) error {
	query := `
		UPDATE datasets
		SET status = $2, row_count = $3, column_count = $4, updated_at = NOW()
		WHERE id = $1
	`

	_, err := r.db.Exec(ctx, query,
		dataset.ID,
		dataset.Status,
		dataset.RowCount,
		dataset.ColumnCount,
	)

	if err != nil {
		return fmt.Errorf("failed to update dataset: %w", err)
	}

	return nil
}

// Delete deletes a dataset
func (r *DatasetRepository) Delete(ctx context.Context, datasetID string) error {
	query := `DELETE FROM datasets WHERE id = $1`

	_, err := r.db.Exec(ctx, query, datasetID)
	if err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

type ValidationRepository struct {
	db *pgxpool.Pool
}

func NewValidationRepository(db *pgxpool.Pool) *ValidationRepository {
	return &ValidationRepository{db: db}
}

// Create creates a new validation
func (r *ValidationRepository) Create(ctx context.Context, validation *models.Validation) error {
	query := `
		INSERT INTO validations (id, dataset_id, user_id, status, estimated_completion)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at
	`

	err := r.db.QueryRow(ctx, query,
		validation.ID,
		validation.DatasetID,
		validation.UserID,
		validation.Status,
		validation.EstimatedCompletion,
	).Scan(&validation.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create validation: %w", err)
	}

	return nil
}

// GetByID retrieves a validation by ID
func (r *ValidationRepository) GetByID(ctx context.Context, validationID string) (*models.Validation, error) {
	query := `
		SELECT id, dataset_id, user_id, status, risk_score, risk_level, recommendation,
		       warranty_eligible, created_at, started_at, completed_at, estimated_completion, error_message
		FROM validations
		WHERE id = $1
	`

	validation := &models.Validation{}
	err := r.db.QueryRow(ctx, query, validationID).Scan(
		&validation.ID,
		&validation.DatasetID,
		&validation.UserID,
		&validation.Status,
		&validation.RiskScore,
		&validation.RiskLevel,
		&validation.Recommendation,
		&validation.WarrantyEligible,
		&validation.CreatedAt,
		&validation.StartedAt,
		&validation.CompletedAt,
		&validation.EstimatedCompletion,
		&validation.ErrorMessage,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get validation: %w", err)
	}

	return validation, nil
}

// List retrieves validations for a user with pagination
func (r *ValidationRepository) List(ctx context.Context, userID string, page, pageSize int) ([]models.Validation, int, error) {
	// Get total count
	var totalCount int
	countQuery := `SELECT COUNT(*) FROM validations WHERE user_id = $1`
	err := r.db.QueryRow(ctx, countQuery, userID).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count validations: %w", err)
	}

	// Get validations
	offset := (page - 1) * pageSize
	query := `
		SELECT id, dataset_id, user_id, status, risk_score, risk_level, recommendation,
		       warranty_eligible, created_at, started_at, completed_at, estimated_completion
		FROM validations
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Query(ctx, query, userID, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list validations: %w", err)
	}
	defer rows.Close()

	validations := []models.Validation{}
	for rows.Next() {
		var validation models.Validation
		err := rows.Scan(
			&validation.ID,
			&validation.DatasetID,
			&validation.UserID,
			&validation.Status,
			&validation.RiskScore,
			&validation.RiskLevel,
			&validation.Recommendation,
			&validation.WarrantyEligible,
			&validation.CreatedAt,
			&validation.StartedAt,
			&validation.CompletedAt,
			&validation.EstimatedCompletion,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan validation: %w", err)
		}
		validations = append(validations, validation)
	}

	return validations, totalCount, nil
}

// Update updates validation status and results
func (r *ValidationRepository) Update(ctx context.Context, validation *models.Validation) error {
	query := `
		UPDATE validations
		SET status = $2, risk_score = $3, risk_level = $4, recommendation = $5,
		    warranty_eligible = $6, started_at = $7, completed_at = $8, error_message = $9
		WHERE id = $1
	`

	now := time.Now()
	if validation.Status == "running" && validation.StartedAt == nil {
		validation.StartedAt = &now
	}
	if validation.Status == "completed" && validation.CompletedAt == nil {
		validation.CompletedAt = &now
	}

	_, err := r.db.Exec(ctx, query,
		validation.ID,
		validation.Status,
		validation.RiskScore,
		validation.RiskLevel,
		validation.Recommendation,
		validation.WarrantyEligible,
		validation.StartedAt,
		validation.CompletedAt,
		validation.ErrorMessage,
	)

	if err != nil {
		return fmt.Errorf("failed to update validation: %w", err)
	}

	return nil
}
