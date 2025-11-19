package handlers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/pdfgen"
)

// CreateValidationFiber creates a new validation job - Fiber version
func CreateValidationFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	var req models.CreateValidationRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	// Generate validation ID
	validationID := "val_" + uuid.New().String()[:8]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify dataset exists and belongs to user
	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	dataset, err := datasetRepo.GetByID(ctx, req.DatasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATASET_NOT_FOUND",
				"message": "Dataset not found",
			},
		})
	}

	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this dataset",
			},
		})
	}

	// Check if dataset is ready for validation
	if dataset.Status != "processed" && dataset.Status != "ready" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATASET_NOT_READY",
				"message": fmt.Sprintf("Dataset must be processed before validation (current status: %s)", dataset.Status),
			},
		})
	}

	// Calculate estimated completion based on priority
	estimatedCompletion := time.Now().Add(24 * time.Hour)
	if req.Options.Priority == "express" {
		estimatedCompletion = time.Now().Add(12 * time.Hour)
	} else if req.Options.Priority == "standard" {
		estimatedCompletion = time.Now().Add(48 * time.Hour)
	}

	// Create validation job in database
	validation := models.Validation{
		ID:                  validationID,
		DatasetID:           req.DatasetID,
		UserID:              userID,
		Status:              "queued",
		EstimatedCompletion: estimatedCompletion,
		CreatedAt:           time.Now().UTC(),
	}

	validationRepo := repository.NewValidationRepository(database.GetDB())
	if err := validationRepo.Create(ctx, &validation); err != nil {
		log.Printf("Failed to create validation: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to create validation job",
			},
		})
	}

	response := models.CreateValidationResponse{
		ValidationID:        validationID,
		DatasetID:           req.DatasetID,
		Status:              "queued",
		EstimatedCompletion: estimatedCompletion,
		EstimatedCost:       35000,
		Stages: []models.ValidationStage{
			{Stage: "diversity_analysis", Status: "pending", EstimatedDuration: 14400},
			{Stage: "cascade_training", Status: "pending", EstimatedDuration: 108000},
			{Stage: "collapse_detection", Status: "pending", EstimatedDuration: 21600},
			{Stage: "report_generation", Status: "pending", EstimatedDuration: 7200},
		},
	}

	return c.Status(fiber.StatusCreated).JSON(response)
}

// ListValidationsFiber returns paginated list of validations - Fiber version
func ListValidationsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}

	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	status := c.Query("status")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validations, totalCount, err := validationRepo.List(ctx, userID, page, pageSize)
	if err != nil {
		log.Printf("Failed to list validations: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to retrieve validations",
			},
		})
	}

	if status != "" {
		filtered := []models.Validation{}
		for _, val := range validations {
			if val.Status == status {
				filtered = append(filtered, val)
			}
		}
		validations = filtered
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	response := models.ValidationListResponse{
		Validations: validations,
		Pagination: models.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalCount: totalCount,
			TotalPages: totalPages,
		},
	}

	return c.JSON(response)
}

// GetValidationFiber returns validation details and results - Fiber version
func GetValidationFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
	}

	response := fiber.Map{
		"validation_id": validation.ID,
		"dataset_id":    validation.DatasetID,
		"status":        validation.Status,
		"created_at":    validation.CreatedAt.Format(time.RFC3339),
	}

	if validation.StartedAt != nil {
		response["started_at"] = validation.StartedAt.Format(time.RFC3339)
	}

	if validation.CompletedAt != nil {
		response["completed_at"] = validation.CompletedAt.Format(time.RFC3339)
	}

	if validation.Status == "completed" && validation.RiskScore != nil {
		response["results"] = models.ValidationResults{
			RiskScore: *validation.RiskScore,
			RiskLevel: *validation.RiskLevel,
			PredictedPerformance: models.PredictedPerformance{
				Accuracy:           0.87,
				ConfidenceInterval: []float64{0.84, 0.90},
				ConfidenceLevel:    0.95,
			},
			CollapseProbability: 0.05,
			Dimensions: map[string]int{
				"distribution_fidelity":    92,
				"correlation_preservation": 88,
				"diversity_retention":      85,
				"rare_pattern_handling":    78,
				"temporal_stability":       91,
				"semantic_coherence":       89,
			},
			Recommendation:   *validation.Recommendation,
			WarrantyEligible: *validation.WarrantyEligible,
		}
		response["report_url"] = fmt.Sprintf("/api/v1/validations/%s/report", validationID)
		response["certificate_url"] = fmt.Sprintf("/api/v1/validations/%s/certificate", validationID)
	}

	return c.JSON(response)
}

// GetValidationReportFiber generates and returns validation report PDF - Fiber version
func GetValidationReportFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

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

	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
	}

	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_COMPLETED",
				"message": "Report can only be generated for completed validations",
			},
		})
	}

	mockResults := &models.ValidationResults{
		RiskScore: *validation.RiskScore,
		RiskLevel: *validation.RiskLevel,
		PredictedPerformance: models.PredictedPerformance{
			Accuracy:           0.87,
			ConfidenceInterval: []float64{0.84, 0.90},
			ConfidenceLevel:    0.95,
		},
		CollapseProbability: 0.05,
		Dimensions: map[string]int{
			"distribution_fidelity":    92,
			"correlation_preservation": 88,
			"diversity_retention":      85,
			"rare_pattern_handling":    78,
			"temporal_stability":       91,
			"semantic_coherence":       89,
		},
		Recommendation:   *validation.Recommendation,
		WarrantyEligible: *validation.WarrantyEligible,
	}

	pdfBytes, err := pdfgen.GenerateValidationReport(validation, mockResults)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "PDF_GENERATION_ERROR",
				"message": "Failed to generate PDF report",
			},
		})
	}

	c.Set("Content-Type", "application/pdf")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=validation_report_%s.pdf", validationID))
	return c.Send(pdfBytes)
}

// GetValidationCertificateFiber returns validation certificate PDF - Fiber version
func GetValidationCertificateFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

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
				"message": "Certificate can only be generated for warranty-eligible validations",
			},
		})
	}

	warrantyID := "war_" + validationID[4:]

	pdfBytes, err := pdfgen.GenerateWarrantyCertificate(validation, warrantyID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "PDF_GENERATION_ERROR",
				"message": "Failed to generate PDF certificate",
			},
		})
	}

	c.Set("Content-Type", "application/pdf")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=warranty_certificate_%s.pdf", validationID))
	return c.Send(pdfBytes)
}

// GetCollapseDetailsFiber returns detailed collapse analysis - Fiber version
func GetCollapseDetailsFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
	}

	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_COMPLETED",
				"message": "Collapse details are only available for completed validations",
			},
		})
	}

	collapseDetected := validation.RiskScore != nil && *validation.RiskScore > 50

	details := models.CollapseDetails{
		ValidationID:     validationID,
		CollapseDetected: collapseDetected,
		CollapseType:     "Type B: Correlation Collapse",
		Severity:         "medium",
		AffectedDimensions: []models.AffectedDimension{
			{Dimension: "correlation_preservation", Score: 62, Threshold: 70, Impact: "high"},
			{Dimension: "rare_pattern_handling", Score: 58, Threshold: 70, Impact: "medium"},
		},
		ProblematicRegions: []models.ProblematicRegion{
			{
				RegionID:        "reg_001",
				RowRange:        []int64{1200000, 1500000},
				Issue:           "duplicate_entities",
				ImpactScore:     35,
				AffectedColumns: []string{"user_id", "email"},
			},
		},
		RootCauses: []models.RootCause{
			{Cause: "Duplicate entities detected", Percentage: 60, Description: "~15% of rows are duplicates (300K rows)"},
			{Cause: "Excessive outliers", Percentage: 30, Description: "Outlier density >10% in numerical columns"},
		},
	}

	return c.JSON(details)
}

// GetRecommendationsFiber returns actionable fix recommendations - Fiber version
func GetRecommendationsFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this validation",
			},
		})
	}

	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_COMPLETED",
				"message": "Recommendations are only available for completed validations",
			},
		})
	}

	currentRiskScore := 62
	if validation.RiskScore != nil {
		currentRiskScore = *validation.RiskScore
	}

	recommendations := models.Recommendations{
		ValidationID: validationID,
		Recommendations: []models.Recommendation{
			{
				Priority:    1,
				Category:    "data_removal",
				Title:       "Remove duplicate entities",
				Description: "Remove rows 1.2M-1.5M (duplicate user accounts)",
				Impact: models.RecommendationImpact{
					CurrentRiskScore:  currentRiskScore,
					ExpectedRiskScore: currentRiskScore - 24,
					Improvement:       24,
				},
				Implementation: models.Implementation{
					Method:        "deduplication",
					AffectedRows:  300000,
					EstimatedTime: "2 hours",
				},
			},
		},
		CombinedImpact: models.CombinedImpact{
			CurrentRiskScore:  currentRiskScore,
			ExpectedRiskScore: currentRiskScore - 47,
			TotalImprovement:  47,
			EstimatedTime:     "3.5 hours",
		},
	}

	return c.JSON(recommendations)
}
