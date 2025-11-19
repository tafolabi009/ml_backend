package handlers

import (
	"context"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// RequestWarrantyFiber handles warranty requests - Fiber version
func RequestWarrantyFiber(c *fiber.Ctx) error {
	validationID := c.Params("validation_id")
	userID := c.Locals("user_id").(string)

	var req struct {
		WarrantyType   string  `json:"warranty_type" validate:"required"`
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

	if validation.WarrantyEligible == nil || !*validation.WarrantyEligible {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_ELIGIBLE",
				"message": "This validation is not eligible for warranty",
			},
		})
	}

	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
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

	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	warranty := &repository.Warranty{
		ID:             "war_" + uuid.New().String()[:8],
		ValidationID:   validationID,
		UserID:         userID,
		Status:         "pending",
		WarrantyType:   req.WarrantyType,
		CoverageAmount: coverageAmount,
		Terms:          generateWarrantyTerms(req.WarrantyType),
	}

	if err := warrantyRepo.Create(ctx, warranty); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to create warranty",
			},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"warranty_id":        warranty.ID,
		"validation_id":      validationID,
		"status":             warranty.Status,
		"warranty_type":      warranty.WarrantyType,
		"coverage_amount":    warranty.CoverageAmount,
		"created_at":         warranty.CreatedAt.Format(time.RFC3339),
		"estimated_approval": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
	})
}

// ListWarrantiesFiber returns all warranties for a user - Fiber version
func ListWarrantiesFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))

	ctx := context.Background()
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

	ctx := context.Background()
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

	ctx := context.Background()
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
