package handlers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// RequestWarrantyFiber handles warranty requests with auto-approval logic based on risk score.
func RequestWarrantyFiber(c *fiber.Ctx) error {
	validationID := c.Params("validation_id")
	userID := c.Locals("user_id").(string)

	var req struct {
		WarrantyType   string  `json:"warranty_type"`
		CoverageType   string  `json:"coverage_type"`
		CoverageAmount float64 `json:"coverage_amount"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	// Accept both warranty_type and coverage_type field names
	if req.WarrantyType == "" {
		req.WarrantyType = req.CoverageType
	}

	ctx := context.Background()

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Validation not found",
			},
		})
	}

	// Check ownership BEFORE eligibility so a user cannot probe other users'
	// validations for existence/eligibility via distinguishable responses.
	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
	}

	if validation.WarrantyEligible == nil || !*validation.WarrantyEligible {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_ELIGIBLE",
				"message": "This validation is not eligible for warranty",
			},
		})
	}

	coverageAmount := req.CoverageAmount
	if coverageAmount == 0 {
		switch req.WarrantyType {
		case "standard":
			coverageAmount = 100000
		case "premium":
			coverageAmount = 500000
		case "enterprise":
			coverageAmount = 1000000
		default:
			coverageAmount = 100000
		}
	}

	// Determine warranty status based on risk score
	warrantyStatus := "pending_review"
	riskScore := 50 // default
	if validation.RiskScore != nil {
		riskScore = *validation.RiskScore
	}

	if riskScore < 30 {
		// Low risk: auto-approve
		warrantyStatus = "active"
	} else if riskScore > 60 {
		// High risk: auto-reject
		warrantyStatus = "rejected"
	}
	// 30-60: stays "pending_review" for manual approval

	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	warranty := &repository.Warranty{
		ID:             "war_" + uuid.New().String()[:8],
		ValidationID:   validationID,
		UserID:         userID,
		Status:         warrantyStatus,
		WarrantyType:   req.WarrantyType,
		CoverageAmount: coverageAmount,
		Terms:          generateWarrantyTerms(req.WarrantyType),
	}

	// Set dates for auto-approved warranties
	if warrantyStatus == "active" {
		now := time.Now()
		endDate := now.Add(90 * 24 * time.Hour)
		warranty.StartDate = &now
		warranty.EndDate = &endDate
	}

	if err := warrantyRepo.Create(ctx, warranty); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to create warranty",
			},
		})
	}

	// Update dates in DB for auto-approved/rejected
	if warrantyStatus == "active" {
		database.GetDB().Exec(ctx,
			`UPDATE warranties SET start_date = $2, end_date = $3, approved_at = NOW() WHERE id = $1`,
			warranty.ID, warranty.StartDate, warranty.EndDate)
	} else if warrantyStatus == "rejected" {
		database.GetDB().Exec(ctx,
			`UPDATE warranties SET rejected_at = NOW(), rejection_reason = $2 WHERE id = $1`,
			warranty.ID, fmt.Sprintf("Auto-rejected: risk score %d exceeds threshold of 60", riskScore))
	}

	response := fiber.Map{
		"warranty_id":     warranty.ID,
		"validation_id":   validationID,
		"status":          warrantyStatus,
		"warranty_type":   warranty.WarrantyType,
		"coverage_amount": warranty.CoverageAmount,
		"risk_score":      riskScore,
		"created_at":      warranty.CreatedAt.Format(time.RFC3339),
	}

	if warrantyStatus == "active" {
		response["start_date"] = warranty.StartDate.Format(time.RFC3339)
		response["end_date"] = warranty.EndDate.Format(time.RFC3339)
		response["message"] = "Warranty auto-approved based on low risk score"
	} else if warrantyStatus == "rejected" {
		response["message"] = fmt.Sprintf("Warranty auto-rejected: risk score %d exceeds threshold", riskScore)
	} else {
		response["estimated_approval"] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
		response["message"] = "Warranty is pending manual review"
	}

	return c.Status(fiber.StatusCreated).JSON(response)
}

// ApproveWarrantyFiber allows admin to approve a pending warranty.
// PATCH /api/v1/admin/warranties/:id/approve
func ApproveWarrantyFiber(c *fiber.Ctx) error {
	warrantyID := c.Params("id")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
	}

	if warranty.Status != "pending" && warranty.Status != "pending_review" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":           "INVALID_STATUS",
				"message":        "Only pending warranties can be approved",
				"current_status": warranty.Status,
			},
		})
	}

	startDate := time.Now()
	endDate := startDate.Add(90 * 24 * time.Hour)

	if err := warrantyRepo.Approve(ctx, warrantyID, startDate, endDate); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to approve warranty",
			},
		})
	}

	adminID := c.Locals("user_id").(string)
	log.Printf("Warranty %s approved by admin %s", warrantyID, adminID)

	insertNotification(ctx, warranty.UserID, "warranty_approved",
		"Warranty approved",
		fmt.Sprintf("Your quality warranty %s is now active until %s.", warrantyID, endDate.Format("2006-01-02")))

	return c.JSON(fiber.Map{
		"warranty_id": warrantyID,
		"status":      "active",
		"start_date":  startDate.Format(time.RFC3339),
		"end_date":    endDate.Format(time.RFC3339),
		"approved_by": adminID,
		"message":     "Warranty approved and activated",
	})
}

// RejectWarrantyFiber allows admin to reject a pending warranty.
// PATCH /api/v1/admin/warranties/:id/reject
func RejectWarrantyFiber(c *fiber.Ctx) error {
	warrantyID := c.Params("id")

	var req struct {
		Reason string `json:"reason"`
	}
	if err := c.BodyParser(&req); err != nil {
		req.Reason = "Rejected by administrator"
	}
	if req.Reason == "" {
		req.Reason = "Rejected by administrator"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
	}

	if warranty.Status != "pending" && warranty.Status != "pending_review" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":           "INVALID_STATUS",
				"message":        "Only pending warranties can be rejected",
				"current_status": warranty.Status,
			},
		})
	}

	if err := warrantyRepo.Reject(ctx, warrantyID, req.Reason); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to reject warranty",
			},
		})
	}

	adminID := c.Locals("user_id").(string)
	log.Printf("Warranty %s rejected by admin %s: %s", warrantyID, adminID, req.Reason)

	insertNotification(ctx, warranty.UserID, "warranty_rejected",
		"Warranty request rejected",
		fmt.Sprintf("Your warranty request %s was rejected: %s", warrantyID, req.Reason))

	return c.JSON(fiber.Map{
		"warranty_id": warrantyID,
		"status":      "rejected",
		"reason":      req.Reason,
		"rejected_by": adminID,
		"message":     "Warranty rejected",
	})
}

// ProcessWarrantyClaimFiber allows support/admin to process a warranty claim.
// PATCH /api/v1/support/warranties/:id/claim/:claim_id/process
func ProcessWarrantyClaimFiber(c *fiber.Ctx) error {
	warrantyID := c.Params("id")
	claimID := c.Params("claim_id")

	var req struct {
		Status     string `json:"status"`     // approved or rejected
		Resolution string `json:"resolution"` // resolution details
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	if req.Status != "approved" && req.Status != "rejected" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_STATUS",
				"message": "Status must be 'approved' or 'rejected'",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())

	// Verify warranty exists
	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
	}

	// Verify claim exists
	claim, err := warrantyRepo.GetClaimByID(ctx, claimID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Claim not found",
			},
		})
	}

	if claim.WarrantyID != warrantyID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "MISMATCH",
				"message": "Claim does not belong to this warranty",
			},
		})
	}

	if claim.Status != "submitted" && claim.Status != "under_review" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":           "INVALID_STATUS",
				"message":        "Claim is not in a processable state",
				"current_status": claim.Status,
			},
		})
	}

	// Update claim status
	now := time.Now()
	_, err = database.GetDB().Exec(ctx,
		`UPDATE warranty_claims
		 SET status = $2, resolution = $3, reviewed_at = $4, resolved_at = $4, updated_at = $4
		 WHERE id = $1`,
		claimID, req.Status, req.Resolution, now)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to process claim",
			},
		})
	}

	processedBy := c.Locals("user_id").(string)
	log.Printf("Claim %s for warranty %s processed by %s: status=%s", claimID, warrantyID, processedBy, req.Status)

	insertNotification(ctx, warranty.UserID, "warranty_claim_"+req.Status,
		fmt.Sprintf("Warranty claim %s", req.Status),
		fmt.Sprintf("Your claim %s on warranty %s was %s. %s", claimID, warrantyID, req.Status, req.Resolution))

	return c.JSON(fiber.Map{
		"claim_id":     claimID,
		"warranty_id":  warrantyID,
		"status":       req.Status,
		"resolution":   req.Resolution,
		"processed_by": processedBy,
		"processed_at": now.Format(time.RFC3339),
		"message":      fmt.Sprintf("Claim %s", req.Status),
	})
}

// ListAllWarrantiesFiber lists all warranties across users for admin.
// GET /api/v1/admin/warranties
func ListAllWarrantiesFiber(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	statusFilter := c.Query("status")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := database.GetDB()

	// Count total
	var totalCount int
	countQuery := `SELECT COUNT(*) FROM warranties`
	countArgs := []interface{}{}
	if statusFilter != "" {
		countQuery += ` WHERE status = $1`
		countArgs = append(countArgs, statusFilter)
	}
	err := db.QueryRow(ctx, countQuery, countArgs...).Scan(&totalCount)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to count warranties",
			},
		})
	}

	// Fetch warranties
	offset := (page - 1) * pageSize
	query := `SELECT id, validation_id, user_id, status, warranty_type, coverage_amount,
	                 start_date, end_date, terms, created_at, approved_at, rejected_at, COALESCE(rejection_reason, '')
	          FROM warranties`
	args := []interface{}{}
	argIdx := 1
	if statusFilter != "" {
		query += fmt.Sprintf(` WHERE status = $%d`, argIdx)
		args = append(args, statusFilter)
		argIdx++
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, argIdx, argIdx+1)
	args = append(args, pageSize, offset)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to list warranties",
			},
		})
	}
	defer rows.Close()

	warranties := []repository.Warranty{}
	for rows.Next() {
		var w repository.Warranty
		err := rows.Scan(
			&w.ID, &w.ValidationID, &w.UserID, &w.Status, &w.WarrantyType, &w.CoverageAmount,
			&w.StartDate, &w.EndDate, &w.Terms, &w.CreatedAt, &w.ApprovedAt, &w.RejectedAt, &w.RejectionReason,
		)
		if err != nil {
			log.Printf("Failed to scan warranty: %v", err)
			continue
		}
		warranties = append(warranties, w)
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	return c.JSON(fiber.Map{
		"warranties": warranties,
		"pagination": fiber.Map{
			"page":        page,
			"page_size":   pageSize,
			"total_count": totalCount,
			"total_pages": totalPages,
		},
	})
}

// ListWarrantiesFiber returns all warranties for a user - Fiber version
func ListWarrantiesFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())

	warranties, totalCount, err := warrantyRepo.List(ctx, userID, page, pageSize)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to list warranties",
			},
		})
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	return c.JSON(fiber.Map{
		"warranties": warranties,
		"pagination": fiber.Map{
			"page":        page,
			"page_size":   pageSize,
			"total_count": totalCount,
			"total_pages": totalPages,
		},
	})
}

// GetWarrantyFiber returns details for a specific warranty - Fiber version
func GetWarrantyFiber(c *fiber.Ctx) error {
	warrantyID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())

	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
	}

	if warranty.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this warranty",
			},
		})
	}

	return c.JSON(warranty)
}

// FileWarrantyClaimFiber handles warranty claim filing - Fiber version
func FileWarrantyClaimFiber(c *fiber.Ctx) error {
	warrantyID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		ClaimType   string  `json:"claim_type" validate:"required"`
		ClaimAmount float64 `json:"claim_amount" validate:"required"`
		Description string  `json:"description" validate:"required"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())

	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
	}

	if warranty.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this warranty",
			},
		})
	}

	if warranty.Status != "active" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":           "INVALID_STATUS",
				"message":        "Warranty must be active to file a claim",
				"current_status": warranty.Status,
			},
		})
	}

	if req.ClaimAmount > warranty.CoverageAmount {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":            "AMOUNT_EXCEEDED",
				"message":         "Claim amount exceeds warranty coverage",
				"coverage_amount": warranty.CoverageAmount,
			},
		})
	}

	claim := &repository.WarrantyClaim{
		ID:          "clm_" + uuid.New().String()[:8],
		WarrantyID:  warrantyID,
		UserID:      userID,
		ClaimType:   req.ClaimType,
		ClaimAmount: req.ClaimAmount,
		Description: req.Description,
		Status:      "submitted",
	}

	if err := warrantyRepo.CreateClaim(ctx, claim); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to file claim",
			},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"claim_id":         claim.ID,
		"warranty_id":      warrantyID,
		"status":           claim.Status,
		"claim_type":       claim.ClaimType,
		"claim_amount":     claim.ClaimAmount,
		"created_at":       claim.CreatedAt.Format(time.RFC3339),
		"estimated_review": time.Now().Add(48 * time.Hour).Format(time.RFC3339),
	})
}

// generateWarrantyTerms generates warranty terms based on type
func generateWarrantyTerms(warrantyType string) string {
	baseTerms := `Synthos AI Warranty Terms & Conditions

1. Coverage: This warranty covers model performance degradation, data quality issues, and model failures as specified in the warranty agreement.

2. Duration: Warranty is valid for 12 months from the date of validation completion.

3. Claims: Claims must be filed within 30 days of discovering the issue with supporting evidence.

4. Exclusions: Does not cover issues arising from data modifications, infrastructure changes, or third-party integrations.

5. Payout: Approved claims will be paid within 30 business days of claim approval.
`

	switch warrantyType {
	case "standard":
		return baseTerms + "\n6. Coverage Amount: Up to $100,000\n7. Review Time: 5-7 business days"
	case "premium":
		return baseTerms + "\n6. Coverage Amount: Up to $500,000\n7. Review Time: 2-3 business days\n8. Priority Support: Included"
	case "enterprise":
		return baseTerms + "\n6. Coverage Amount: Up to $1,000,000\n7. Review Time: 24 hours\n8. Priority Support: Included\n9. Dedicated Account Manager: Included"
	default:
		return baseTerms
	}
}
