package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

// WarrantyRepository handles warranty data operations
type WarrantyRepository struct {
	db *pgxpool.Pool
}

// NewWarrantyRepository creates a new warranty repository
func NewWarrantyRepository(db *pgxpool.Pool) *WarrantyRepository {
	return &WarrantyRepository{db: db}
}

// CreateWarrantyRequest creates a new warranty request
func (r *WarrantyRepository) CreateWarrantyRequest(ctx context.Context, req *models.WarrantyRequest) (*models.Warranty, error) {
	// Calculate premium based on validation results and dataset size
	premium := r.calculatePremium(req)

	warranty := &models.Warranty{
		ID:           fmt.Sprintf("war_%d", time.Now().UnixNano()),
		UserID:       req.UserID,
		ValidationID: req.ValidationID,
		DatasetID:    req.DatasetID,
		Status:       "pending_payment",
		Coverage: models.WarrantyCoverage{
			MaxCoverage:        req.RequestedCoverage,
			DeductiblePercent:  req.DeductiblePercent,
			CoverageType:       req.CoverageType,
			IncludedDimensions: req.Dimensions,
		},
		Premium:      premium,
		TermMonths:   req.TermMonths,
		StartDate:    nil, // Set after payment
		ExpiryDate:   nil,
		ClaimsMade:   0,
		ClaimsPaid:   0,
		TotalPayouts: 0,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Insert into database
	query := `
		INSERT INTO warranties (
			id, user_id, validation_id, dataset_id, status,
			coverage, premium, term_months,
			claims_made, claims_paid, total_payouts,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id`

	coverageJSON, err := json.Marshal(warranty.Coverage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal coverage: %w", err)
	}

	err = r.db.QueryRow(ctx, query,
		warranty.ID, warranty.UserID, warranty.ValidationID, warranty.DatasetID,
		warranty.Status, coverageJSON, warranty.Premium, warranty.TermMonths,
		warranty.ClaimsMade, warranty.ClaimsPaid, warranty.TotalPayouts,
		warranty.CreatedAt, warranty.UpdatedAt,
	).Scan(&warranty.ID)

	if err != nil {
		return nil, fmt.Errorf("failed to create warranty: %w", err)
	}

	return warranty, nil
}

// calculatePremium calculates warranty premium based on risk factors
func (r *WarrantyRepository) calculatePremium(req *models.WarrantyRequest) float64 {
	// Base premium calculation
	basePremium := req.RequestedCoverage * 0.02 // 2% of coverage amount

	// Risk multipliers based on validation scores
	riskMultiplier := 1.0

	// Adjust based on collapse score (higher score = lower risk)
	if req.CollapseScore < 70 {
		riskMultiplier *= 1.5 // 50% premium increase for risky data
	} else if req.CollapseScore > 85 {
		riskMultiplier *= 0.8 // 20% discount for high-quality data
	}

	// Adjust based on diversity score
	if req.DiversityScore < 60 {
		riskMultiplier *= 1.3
	}

	// Adjust based on dataset size (larger = more expensive)
	sizeMultiplier := 1.0
	if req.DatasetSize > 1000000 {
		sizeMultiplier = 1.2
	} else if req.DatasetSize > 100000 {
		sizeMultiplier = 1.1
	}

	// Adjust based on term length (longer = discount)
	termDiscount := 1.0
	if req.TermMonths >= 12 {
		termDiscount = 0.9
	} else if req.TermMonths >= 6 {
		termDiscount = 0.95
	}

	// Adjust based on deductible (higher deductible = lower premium)
	deductibleDiscount := 1.0 - (req.DeductiblePercent / 200) // Max 50% discount at 100% deductible

	premium := basePremium * riskMultiplier * sizeMultiplier * termDiscount * deductibleDiscount

	// Minimum premium
	if premium < 100 {
		premium = 100
	}

	return premium
}

// ActivateWarranty activates a warranty after payment
func (r *WarrantyRepository) ActivateWarranty(ctx context.Context, warrantyID string) error {
	now := time.Now()
	// Get warranty to determine term
	var termMonths int
	err := r.db.QueryRow(ctx, "SELECT term_months FROM warranties WHERE id = $1", warrantyID).Scan(&termMonths)
	if err != nil {
		return fmt.Errorf("failed to get warranty: %w", err)
	}

	expiryDate := now.AddDate(0, termMonths, 0)

	query := `
		UPDATE warranties 
		SET status = 'active', 
		    start_date = $1, 
		    expiry_date = $2,
		    updated_at = $3
		WHERE id = $4 AND status = 'pending_payment'`

	result, err := r.db.Exec(ctx, query, now, expiryDate, now, warrantyID)
	if err != nil {
		return fmt.Errorf("failed to activate warranty: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("warranty not found or already activated")
	}

	return nil
}

// GetWarranty retrieves a warranty by ID
func (r *WarrantyRepository) GetWarranty(ctx context.Context, warrantyID string) (*models.Warranty, error) {
	query := `
		SELECT id, user_id, validation_id, dataset_id, status, coverage,
		       premium, term_months, start_date, expiry_date,
		       claims_made, claims_paid, total_payouts,
		       created_at, updated_at
		FROM warranties
		WHERE id = $1`

	var warranty models.Warranty
	var coverageJSON []byte
	var startDate, expiryDate sql.NullTime

	err := r.db.QueryRow(ctx, query, warrantyID).Scan(
		&warranty.ID, &warranty.UserID, &warranty.ValidationID, &warranty.DatasetID,
		&warranty.Status, &coverageJSON, &warranty.Premium, &warranty.TermMonths,
		&startDate, &expiryDate,
		&warranty.ClaimsMade, &warranty.ClaimsPaid, &warranty.TotalPayouts,
		&warranty.CreatedAt, &warranty.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("warranty not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get warranty: %w", err)
	}

	if err := json.Unmarshal(coverageJSON, &warranty.Coverage); err != nil {
		return nil, fmt.Errorf("failed to unmarshal coverage: %w", err)
	}

	if startDate.Valid {
		warranty.StartDate = &startDate.Time
	}
	if expiryDate.Valid {
		warranty.ExpiryDate = &expiryDate.Time
	}

	return &warranty, nil
}

// ListWarranties lists warranties for a user
func (r *WarrantyRepository) ListWarranties(ctx context.Context, userID string, limit, offset int) ([]*models.Warranty, int, error) {
	// Get total count
	var total int
	err := r.db.QueryRow(ctx, "SELECT COUNT(*) FROM warranties WHERE user_id = $1", userID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count warranties: %w", err)
	}

	// Get warranties
	query := `
		SELECT id, user_id, validation_id, dataset_id, status, coverage,
		       premium, term_months, start_date, expiry_date,
		       claims_made, claims_paid, total_payouts,
		       created_at, updated_at
		FROM warranties
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`

	rows, err := r.db.Query(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query warranties: %w", err)
	}
	defer rows.Close()

	warranties := []*models.Warranty{}
	for rows.Next() {
		var warranty models.Warranty
		var coverageJSON []byte
		var startDate, expiryDate sql.NullTime

		err := rows.Scan(
			&warranty.ID, &warranty.UserID, &warranty.ValidationID, &warranty.DatasetID,
			&warranty.Status, &coverageJSON, &warranty.Premium, &warranty.TermMonths,
			&startDate, &expiryDate,
			&warranty.ClaimsMade, &warranty.ClaimsPaid, &warranty.TotalPayouts,
			&warranty.CreatedAt, &warranty.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan warranty: %w", err)
		}

		if err := json.Unmarshal(coverageJSON, &warranty.Coverage); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal coverage: %w", err)
		}

		if startDate.Valid {
			warranty.StartDate = &startDate.Time
		}
		if expiryDate.Valid {
			warranty.ExpiryDate = &expiryDate.Time
		}

		warranties = append(warranties, &warranty)
	}

	return warranties, total, nil
}

// CreateClaim creates a new warranty claim
func (r *WarrantyRepository) CreateClaim(ctx context.Context, claim *models.WarrantyClaim) error {
	// Verify warranty is active and not expired
	warranty, err := r.GetWarranty(ctx, claim.WarrantyID)
	if err != nil {
		return err
	}

	if warranty.Status != "active" {
		return fmt.Errorf("warranty is not active")
	}

	if warranty.ExpiryDate != nil && warranty.ExpiryDate.Before(time.Now()) {
		return fmt.Errorf("warranty has expired")
	}

	// Check if claim amount is within coverage
	if claim.ClaimAmount > warranty.Coverage.MaxCoverage {
		return fmt.Errorf("claim amount exceeds maximum coverage")
	}

	// Calculate payout (after deductible)
	deductible := claim.ClaimAmount * (warranty.Coverage.DeductiblePercent / 100)
	payout := claim.ClaimAmount - deductible

	claim.ID = fmt.Sprintf("clm_%d", time.Now().UnixNano())
	claim.Status = "under_review"
	claim.ApprovedAmount = 0 // Set during review
	claim.CreatedAt = time.Now()
	claim.UpdatedAt = time.Now()

	query := `
		INSERT INTO warranty_claims (
			id, warranty_id, user_id, claim_type, claim_amount,
			description, evidence, status, approved_amount,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	evidenceJSON, err := json.Marshal(claim.Evidence)
	if err != nil {
		return fmt.Errorf("failed to marshal evidence: %w", err)
	}

	_, err = r.db.Exec(ctx, query,
		claim.ID, claim.WarrantyID, claim.UserID, claim.ClaimType,
		claim.ClaimAmount, claim.Description, evidenceJSON,
		claim.Status, claim.ApprovedAmount, claim.CreatedAt, claim.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create claim: %w", err)
	}

	// Update warranty claims count
	_, err = r.db.Exec(ctx,
		"UPDATE warranties SET claims_made = claims_made + 1, updated_at = $1 WHERE id = $2",
		time.Now(), claim.WarrantyID,
	)

	return err
}

// ApproveClaim approves and processes a claim
func (r *WarrantyRepository) ApproveClaim(ctx context.Context, claimID string, approvedAmount float64) error {
	now := time.Now()

	// Get claim and warranty
	var warrantyID string
	err := r.db.QueryRow(ctx, "SELECT warranty_id FROM warranty_claims WHERE id = $1", claimID).Scan(&warrantyID)
	if err != nil {
		return fmt.Errorf("failed to get claim: %w", err)
	}

	// Update claim
	query := `
		UPDATE warranty_claims 
		SET status = 'approved',
		    approved_amount = $1,
		    processed_at = $2,
		    updated_at = $2
		WHERE id = $3 AND status = 'under_review'`

	result, err := r.db.Exec(ctx, query, approvedAmount, now, claimID)
	if err != nil {
		return fmt.Errorf("failed to approve claim: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("claim not found or already processed")
	}

	// Update warranty
	_, err = r.db.Exec(ctx,
		"UPDATE warranties SET claims_paid = claims_paid + 1, total_payouts = total_payouts + $1, updated_at = $2 WHERE id = $3",
		approvedAmount, now, warrantyID,
	)

	return err
}
