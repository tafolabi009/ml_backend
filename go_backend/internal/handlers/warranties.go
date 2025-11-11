package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// RequestWarranty handles warranty requests
func RequestWarranty(c *gin.Context) {
	validationID := c.Param("validation_id")
	userID := c.GetString("user_id")

	var req struct {
		WarrantyType string  `json:"warranty_type" binding:"required"` // standard, premium, enterprise
		CoverageAmount float64 `json:"coverage_amount"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
		return
	}

	ctx := context.Background()
	
	// Verify validation exists and is eligible
	validationRepo := repository.NewValidationRepository(database.GetDB())
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"code":    "NOT_FOUND",
				"message": "Validation not found",
			},
		})
		return
	}

	// Check eligibility
	if validation.WarrantyEligible == nil || !*validation.WarrantyEligible {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "NOT_ELIGIBLE",
				"message": "This validation is not eligible for warranty",
			},
		})
		return
	}

	// Verify ownership
	if validation.UserID != userID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
		return
	}

	// Calculate coverage amount based on type
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

	// Create warranty
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to create warranty",
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"warranty_id":    warranty.ID,
		"validation_id":  validationID,
		"status":         warranty.Status,
		"warranty_type":  warranty.WarrantyType,
		"coverage_amount": warranty.CoverageAmount,
		"created_at":     warranty.CreatedAt.Format(time.RFC3339),
		"estimated_approval": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
	})
}

// ListWarranties returns all warranties for a user
func ListWarranties(c *gin.Context) {
	userID := c.GetString("user_id")

	// Parse query parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

	ctx := context.Background()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	
	warranties, totalCount, err := warrantyRepo.List(ctx, userID, page, pageSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to list warranties",
			},
		})
		return
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	c.JSON(http.StatusOK, gin.H{
		"warranties": warranties,
		"pagination": gin.H{
			"page":        page,
			"page_size":   pageSize,
			"total_count": totalCount,
			"total_pages": totalPages,
		},
	})
}

// GetWarranty returns details for a specific warranty
func GetWarranty(c *gin.Context) {
	warrantyID := c.Param("id")
	userID := c.GetString("user_id")

	ctx := context.Background()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	
	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
		return
	}

	// Verify ownership
	if warranty.UserID != userID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this warranty",
			},
		})
		return
	}

	c.JSON(http.StatusOK, warranty)
}

// FileWarrantyClaim handles warranty claim filing
func FileWarrantyClaim(c *gin.Context) {
	warrantyID := c.Param("id")
	userID := c.GetString("user_id")

	var req struct {
		ClaimType   string  `json:"claim_type" binding:"required"`   // performance_degradation, data_quality, model_failure
		ClaimAmount float64 `json:"claim_amount" binding:"required"`
		Description string  `json:"description" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
		return
	}

	ctx := context.Background()
	warrantyRepo := repository.NewWarrantyRepository(database.GetDB())
	
	// Verify warranty exists and is active
	warranty, err := warrantyRepo.GetByID(ctx, warrantyID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"code":    "NOT_FOUND",
				"message": "Warranty not found",
			},
		})
		return
	}

	// Verify ownership
	if warranty.UserID != userID {
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this warranty",
			},
		})
		return
	}

	// Check warranty status
	if warranty.Status != "active" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "INVALID_STATUS",
				"message": "Warranty must be active to file a claim",
				"current_status": warranty.Status,
			},
		})
		return
	}

	// Check claim amount doesn't exceed coverage
	if req.ClaimAmount > warranty.CoverageAmount {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":    "AMOUNT_EXCEEDED",
				"message": "Claim amount exceeds warranty coverage",
				"coverage_amount": warranty.CoverageAmount,
			},
		})
		return
	}

	// Create claim
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "Failed to file claim",
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"claim_id":     claim.ID,
		"warranty_id":  warrantyID,
		"status":       claim.Status,
		"claim_type":   claim.ClaimType,
		"claim_amount": claim.ClaimAmount,
		"created_at":   claim.CreatedAt.Format(time.RFC3339),
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
