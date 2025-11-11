package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Warranty represents a warranty contract
type Warranty struct {
	ID             string     `json:"warranty_id"`
	ValidationID   string     `json:"validation_id"`
	UserID         string     `json:"user_id"`
	Status         string     `json:"status"`
	WarrantyType   string     `json:"warranty_type"`
	CoverageAmount float64    `json:"coverage_amount"`
	StartDate      *time.Time `json:"start_date"`
	EndDate        *time.Time `json:"end_date"`
	Terms          string     `json:"terms"`
	CreatedAt      time.Time  `json:"created_at"`
	ApprovedAt     *time.Time `json:"approved_at"`
	RejectedAt     *time.Time `json:"rejected_at"`
	RejectionReason string    `json:"rejection_reason,omitempty"`
}

// WarrantyClaim represents a warranty claim
type WarrantyClaim struct {
	ID           string     `json:"claim_id"`
	WarrantyID   string     `json:"warranty_id"`
	UserID       string     `json:"user_id"`
	ClaimType    string     `json:"claim_type"`
	ClaimAmount  float64    `json:"claim_amount"`
	Description  string     `json:"description"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	ReviewedAt   *time.Time `json:"reviewed_at"`
	ResolvedAt   *time.Time `json:"resolved_at"`
	Resolution   string     `json:"resolution,omitempty"`
}

type WarrantyRepository struct {
	db *pgxpool.Pool
}

func NewWarrantyRepository(db *pgxpool.Pool) *WarrantyRepository {
	return &WarrantyRepository{db: db}
}

// Create creates a new warranty
func (r *WarrantyRepository) Create(ctx context.Context, warranty *Warranty) error {
	query := `
		INSERT INTO warranties (id, validation_id, user_id, status, warranty_type, coverage_amount, terms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at
	`

	err := r.db.QueryRow(ctx, query,
		warranty.ID,
		warranty.ValidationID,
		warranty.UserID,
		warranty.Status,
		warranty.WarrantyType,
		warranty.CoverageAmount,
		warranty.Terms,
	).Scan(&warranty.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create warranty: %w", err)
	}

	return nil
}

// GetByID retrieves a warranty by ID
func (r *WarrantyRepository) GetByID(ctx context.Context, warrantyID string) (*Warranty, error) {
	query := `
		SELECT id, validation_id, user_id, status, warranty_type, coverage_amount,
		       start_date, end_date, terms, created_at, approved_at, rejected_at, rejection_reason
		FROM warranties
		WHERE id = $1
	`

	warranty := &Warranty{}
	err := r.db.QueryRow(ctx, query, warrantyID).Scan(
		&warranty.ID,
		&warranty.ValidationID,
		&warranty.UserID,
		&warranty.Status,
		&warranty.WarrantyType,
		&warranty.CoverageAmount,
		&warranty.StartDate,
		&warranty.EndDate,
		&warranty.Terms,
		&warranty.CreatedAt,
		&warranty.ApprovedAt,
		&warranty.RejectedAt,
		&warranty.RejectionReason,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get warranty: %w", err)
	}

	return warranty, nil
}

// List retrieves warranties for a user
func (r *WarrantyRepository) List(ctx context.Context, userID string, page, pageSize int) ([]Warranty, int, error) {
	// Get total count
	var totalCount int
	countQuery := `SELECT COUNT(*) FROM warranties WHERE user_id = $1`
	err := r.db.QueryRow(ctx, countQuery, userID).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count warranties: %w", err)
	}

	// Get warranties
	offset := (page - 1) * pageSize
	query := `
		SELECT id, validation_id, user_id, status, warranty_type, coverage_amount,
		       start_date, end_date, terms, created_at, approved_at
		FROM warranties
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Query(ctx, query, userID, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list warranties: %w", err)
	}
	defer rows.Close()

	warranties := []Warranty{}
	for rows.Next() {
		var warranty Warranty
		err := rows.Scan(
			&warranty.ID,
			&warranty.ValidationID,
			&warranty.UserID,
			&warranty.Status,
			&warranty.WarrantyType,
			&warranty.CoverageAmount,
			&warranty.StartDate,
			&warranty.EndDate,
			&warranty.Terms,
			&warranty.CreatedAt,
			&warranty.ApprovedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan warranty: %w", err)
		}
		warranties = append(warranties, warranty)
	}

	return warranties, totalCount, nil
}

// Approve approves a warranty
func (r *WarrantyRepository) Approve(ctx context.Context, warrantyID string, startDate, endDate time.Time) error {
	query := `
		UPDATE warranties
		SET status = 'active', start_date = $2, end_date = $3, approved_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`

	_, err := r.db.Exec(ctx, query, warrantyID, startDate, endDate)
	if err != nil {
		return fmt.Errorf("failed to approve warranty: %w", err)
	}

	return nil
}

// Reject rejects a warranty
func (r *WarrantyRepository) Reject(ctx context.Context, warrantyID, reason string) error {
	query := `
		UPDATE warranties
		SET status = 'rejected', rejected_at = CURRENT_TIMESTAMP, rejection_reason = $2
		WHERE id = $1
	`

	_, err := r.db.Exec(ctx, query, warrantyID, reason)
	if err != nil {
		return fmt.Errorf("failed to reject warranty: %w", err)
	}

	return nil
}

// CreateClaim creates a new warranty claim
func (r *WarrantyRepository) CreateClaim(ctx context.Context, claim *WarrantyClaim) error {
	query := `
		INSERT INTO warranty_claims (id, warranty_id, user_id, claim_type, claim_amount, description, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at
	`

	err := r.db.QueryRow(ctx, query,
		claim.ID,
		claim.WarrantyID,
		claim.UserID,
		claim.ClaimType,
		claim.ClaimAmount,
		claim.Description,
		claim.Status,
	).Scan(&claim.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to create claim: %w", err)
	}

	return nil
}

// GetClaimByID retrieves a claim by ID
func (r *WarrantyRepository) GetClaimByID(ctx context.Context, claimID string) (*WarrantyClaim, error) {
	query := `
		SELECT id, warranty_id, user_id, claim_type, claim_amount, description,
		       status, created_at, reviewed_at, resolved_at, resolution
		FROM warranty_claims
		WHERE id = $1
	`

	claim := &WarrantyClaim{}
	err := r.db.QueryRow(ctx, query, claimID).Scan(
		&claim.ID,
		&claim.WarrantyID,
		&claim.UserID,
		&claim.ClaimType,
		&claim.ClaimAmount,
		&claim.Description,
		&claim.Status,
		&claim.CreatedAt,
		&claim.ReviewedAt,
		&claim.ResolvedAt,
		&claim.Resolution,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get claim: %w", err)
	}

	return claim, nil
}
