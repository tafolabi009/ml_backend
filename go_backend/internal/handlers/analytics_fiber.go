package handlers

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// GetUsageAnalyticsFiber returns customer usage statistics - Fiber version
func GetUsageAnalyticsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userRepo := repository.NewUserRepository(database.GetDB())
	user, err := userRepo.GetByID(ctx, userID)
	if err != nil {
		log.Printf("Failed to fetch user: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to retrieve user information",
			},
		})
	}

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	datasets, totalDatasets, err := datasetRepo.List(ctx, userID, 1, 1000)
	if err != nil {
		log.Printf("Failed to fetch datasets: %v", err)
		totalDatasets = 0
	}

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validations, totalValidations, err := validationRepo.List(ctx, userID, 1, 1000)
	if err != nil {
		log.Printf("Failed to fetch validations: %v", err)
		totalValidations = 0
	}

	completedValidations := 0
	totalRiskScore := 0
	totalRowsValidated := int64(0)
	warrantyCount := 0

	for _, val := range validations {
		if val.Status == "completed" {
			completedValidations++
			if val.RiskScore != nil {
				totalRiskScore += *val.RiskScore
			}
			if val.WarrantyEligible != nil && *val.WarrantyEligible {
				warrantyCount++
			}
		}
	}

	for _, ds := range datasets {
		if ds.RowCount != nil {
			totalRowsValidated += *ds.RowCount
		}
	}

	averageRiskScore := 0
	if completedValidations > 0 {
		averageRiskScore = totalRiskScore / completedValidations
	}

	validationsLimit := 10
	if user.SubscriptionTier == "professional" {
		validationsLimit = 20
	} else if user.SubscriptionTier == "enterprise" {
		validationsLimit = 100
	}

	now := time.Now()
	period := now.Format("2006-01")

	analytics := fiber.Map{
		"period":                period,
		"datasets_uploaded":     totalDatasets,
		"validations_completed": completedValidations,
		"total_rows_validated":  totalRowsValidated,
		"average_risk_score":    averageRiskScore,
		"warranty_contracts":    warrantyCount,
		"subscription_tier":     user.SubscriptionTier,
		"usage_limits": fiber.Map{
			"validations_limit":     validationsLimit,
			"validations_used":      totalValidations,
			"validations_remaining": validationsLimit - totalValidations,
		},
	}

	return c.JSON(analytics)
}

// GetValidationHistoryFiber returns historical validation results - Fiber version
func GetValidationHistoryFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	startDateStr := c.Query("start_date")
	endDateStr := c.Query("end_date")
	riskLevel := c.Query("risk_level")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validations, _, err := validationRepo.List(ctx, userID, 1, 1000)
	if err != nil {
		log.Printf("Failed to fetch validations: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to retrieve validation history",
			},
		})
	}

	var startDate, endDate time.Time
	if startDateStr != "" {
		startDate, _ = time.Parse(time.RFC3339, startDateStr)
	}
	if endDateStr != "" {
		endDate, _ = time.Parse(time.RFC3339, endDateStr)
	}

	validationList := []fiber.Map{}
	totalRiskScore := 0
	validationCount := 0
	totalComputeSaved := int64(0)

	for _, val := range validations {
		if !startDate.IsZero() && val.CreatedAt.Before(startDate) {
			continue
		}
		if !endDate.IsZero() && val.CreatedAt.After(endDate) {
			continue
		}

		if riskLevel != "" && val.RiskLevel != nil && *val.RiskLevel != riskLevel {
			continue
		}

		if val.Status != "completed" {
			continue
		}

		datasetRepo := repository.NewDatasetRepository(database.GetDB())
		dataset, err := datasetRepo.GetByID(ctx, val.DatasetID)
		datasetName := "Unknown"
		if err == nil {
			datasetName = dataset.Filename
		}

		validationInfo := fiber.Map{
			"validation_id": val.ID,
			"dataset_name":  datasetName,
		}

		if val.RiskScore != nil {
			validationInfo["risk_score"] = *val.RiskScore
			totalRiskScore += *val.RiskScore
			validationCount++
		}

		if val.CompletedAt != nil {
			validationInfo["completed_at"] = val.CompletedAt.Format(time.RFC3339)
		}

		validationList = append(validationList, validationInfo)

		if dataset != nil && dataset.RowCount != nil {
			totalComputeSaved += *dataset.RowCount / 1000
		}
	}

	averageRiskScore := 0
	if validationCount > 0 {
		averageRiskScore = totalRiskScore / validationCount
	}

	averageRiskScoreChange := -5

	history := fiber.Map{
		"validations": validationList,
		"trends": fiber.Map{
			"average_risk_score":        averageRiskScore,
			"average_risk_score_change": averageRiskScoreChange,
			"total_compute_saved":       totalComputeSaved,
		},
	}

	return c.JSON(history)
}
