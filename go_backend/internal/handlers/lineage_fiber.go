package handlers

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Version lineage: a dataset's quality trajectory across validations, and an
// EU-AI-Act-flavoured datasheet assembling everything we know about a
// completed validation into a compliance-ready document.

// GetDatasetHistoryFiber returns the validation history for one dataset.
// GET /datasets/:id/history
func GetDatasetHistoryFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	datasetRepo := repository.NewDatasetRepository(db)
	dataset, err := datasetRepo.GetByID(ctx, datasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Dataset not found"},
		})
	}
	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this dataset"},
		})
	}

	rows, err := db.Query(ctx,
		`SELECT id, status, risk_score, risk_level, created_at, completed_at
		 FROM validations
		 WHERE dataset_id = $1 AND user_id = $2
		 ORDER BY created_at ASC LIMIT 200`,
		datasetID, userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to load history"},
		})
	}
	defer rows.Close()

	history := []fiber.Map{}
	var completedScores []int
	for rows.Next() {
		var id, status string
		var riskScore *int
		var riskLevel *string
		var createdAt time.Time
		var completedAt *time.Time
		if err := rows.Scan(&id, &status, &riskScore, &riskLevel, &createdAt, &completedAt); err != nil {
			continue
		}
		entry := fiber.Map{
			"validation_id": id,
			"status":        status,
			"risk_score":    riskScore,
			"risk_level":    riskLevel,
			"created_at":    createdAt,
			"completed_at":  completedAt,
		}
		history = append(history, entry)
		if status == "completed" && riskScore != nil {
			completedScores = append(completedScores, *riskScore)
		}
	}

	trend := fiber.Map{"direction": "insufficient_data", "delta": nil}
	if n := len(completedScores); n >= 2 {
		delta := completedScores[n-1] - completedScores[0]
		direction := "stable"
		if delta <= -3 {
			direction = "improving" // risk going down
		} else if delta >= 3 {
			direction = "degrading"
		}
		trend = fiber.Map{
			"direction":         direction,
			"delta":             delta,
			"first_risk_score":  completedScores[0],
			"latest_risk_score": completedScores[n-1],
		}
	}

	return c.JSON(fiber.Map{
		"dataset_id":   datasetID,
		"dataset_name": dataset.Filename,
		"history":      history,
		"trend":        trend,
	})
}

// GetValidationDatasheetFiber assembles a model-card-style datasheet for a
// completed validation. GET /validations/:id/datasheet
func GetValidationDatasheetFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	validationRepo := repository.NewValidationRepository(db)
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Validation not found"},
		})
	}
	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this validation"},
		})
	}
	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_COMPLETED", "message": "Datasheets are only available for completed validations"},
		})
	}

	datasetSection := fiber.Map{"dataset_id": validation.DatasetID}
	datasetRepo := repository.NewDatasetRepository(db)
	if ds, derr := datasetRepo.GetByID(ctx, validation.DatasetID); derr == nil {
		datasetSection = fiber.Map{
			"dataset_id":  ds.ID,
			"name":        ds.Filename,
			"file_type":   ds.FileType,
			"size_bytes":  ds.FileSize,
			"row_count":   ds.RowCount,
			"uploaded_at": ds.UploadedAt,
		}
	}

	resultsSection := fiber.Map{}
	if stored, serr := loadStoredResults(db, validationID); serr == nil && stored != nil {
		resultsSection = fiber.Map{
			"risk_score":           stored.RiskScore,
			"risk_level":           stored.RiskLevel,
			"collapse_probability": stored.CollapseProbability,
			"dimensions":           stored.Dimensions,
			"warranty_eligible":    stored.WarrantyEligible,
			"recommendation":       stored.Recommendation,
		}
	} else {
		rs := 0
		if validation.RiskScore != nil {
			rs = *validation.RiskScore
		}
		rl := "unknown"
		if validation.RiskLevel != nil {
			rl = *validation.RiskLevel
		}
		resultsSection = fiber.Map{"risk_score": rs, "risk_level": rl}
	}

	// Privacy sidecar (computed by the ML tier during diversity analysis).
	var privacySection interface{} = fiber.Map{
		"status": "not_computed",
		"note":   "Privacy analysis runs during diversity analysis; re-validate to generate it.",
	}
	if p, ok := fetchPrivacyReport(ctx, validation.DatasetID); ok {
		privacySection = p
	}

	// Recorded downstream outcome, if the customer reported one.
	var outcomeSection interface{}
	{
		var outcome string
		var predictedRisk *int
		var observedAt time.Time
		oerr := db.QueryRow(ctx,
			`SELECT outcome, predicted_risk, observed_at FROM validation_outcomes WHERE validation_id = $1`,
			validationID).Scan(&outcome, &predictedRisk, &observedAt)
		if oerr == nil {
			outcomeSection = fiber.Map{
				"outcome":        outcome,
				"predicted_risk": predictedRisk,
				"observed_at":    observedAt,
			}
		}
	}

	datasheet := fiber.Map{
		"datasheet_version": "1.0",
		"generated_at":      time.Now().UTC(),
		"issuer":            "Synthos",
		"validation": fiber.Map{
			"validation_id": validationID,
			"type":          validation.ValidationType,
			"created_at":    validation.CreatedAt,
			"completed_at":  validation.CompletedAt,
			"method": fiber.Map{
				"pipeline": []string{"diversity_analysis", "cascade_training", "collapse_detection", "report_generation"},
				"engine":   "Synthos Validation Engine",
			},
		},
		"dataset":            datasetSection,
		"results":            resultsSection,
		"privacy":            privacySection,
		"downstream_outcome": outcomeSection,
		"intended_use": "Assessment of dataset fitness for training machine-learning models, " +
			"including model-collapse risk for synthetic or mixed provenance data.",
		"limitations": []string{
			"Scores reflect the dataset sample analyzed at validation time; later modifications are not covered (use monitors for continuous coverage).",
			"Collapse risk is a statistical prediction, not a guarantee of downstream behaviour.",
			"Privacy analysis is heuristic screening, not a formal differential-privacy audit.",
		},
		"compliance_mapping": fiber.Map{
			"eu_ai_act_art10": fiber.Map{
				"data_governance":       "Dataset provenance, size and format recorded in the 'dataset' section",
				"examination_of_biases": "Distribution and diversity dimensions in the 'results.dimensions' section",
				"data_quality_criteria": "Risk score, risk level and per-dimension quality scores in 'results'",
			},
			"note": "This datasheet is supporting documentation, not a conformity assessment.",
		},
		"verification": fiber.Map{
			"signed_certificate": "/api/v1/validations/" + validationID + "/certificate.json",
			"public_key":         "/api/v1/certificates/public-key",
		},
	}

	return c.JSON(datasheet)
}
