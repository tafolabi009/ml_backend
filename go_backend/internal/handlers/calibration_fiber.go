package handlers

import (
	"context"
	"log"
	"math"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Calibration loop: customers report what actually happened downstream
// ("we trained on it — did it collapse?") and we compare that against the
// risk we predicted. This is the evidence that makes the warranty insurable:
// "when we said 20% collapse risk, it happened 19% of the time."

var validOutcomes = map[string]bool{
	"healthy":   true, // downstream model trained fine
	"degraded":  true, // noticeable quality loss
	"collapsed": true, // downstream model collapsed
}

// RecordValidationOutcomeFiber upserts the observed downstream outcome.
// POST /validations/:id/outcome {"outcome":"collapsed","actual_metric":0.41,"notes":"..."}
func RecordValidationOutcomeFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		Outcome      string   `json:"outcome"`
		ActualMetric *float64 `json:"actual_metric"`
		Notes        string   `json:"notes"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}
	req.Outcome = strings.ToLower(strings.TrimSpace(req.Outcome))
	if !validOutcomes[req.Outcome] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_OUTCOME", "message": "outcome must be one of: healthy, degraded, collapsed"},
		})
	}
	if len(req.Notes) > 2000 {
		req.Notes = req.Notes[:2000]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validationRepo := repository.NewValidationRepository(database.GetDB())
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
			"error": fiber.Map{"code": "NOT_COMPLETED", "message": "Outcomes can only be recorded for completed validations"},
		})
	}

	// Snapshot the risk we predicted at recording time.
	var predictedRisk *int
	if validation.RiskScore != nil {
		predictedRisk = validation.RiskScore
	}

	_, err = database.GetDB().Exec(ctx,
		`INSERT INTO validation_outcomes (id, validation_id, user_id, predicted_risk, outcome, actual_metric, notes, observed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		 ON CONFLICT (validation_id) DO UPDATE
		 SET outcome = $5, actual_metric = $6, notes = $7, observed_at = NOW(), updated_at = NOW()`,
		"vo_"+uuid.New().String()[:12], validationID, userID, predictedRisk, req.Outcome, req.ActualMetric, req.Notes)
	if err != nil {
		log.Printf("Failed to record outcome for %s: %v", validationID, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to record outcome"},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"validation_id":  validationID,
		"outcome":        req.Outcome,
		"predicted_risk": predictedRisk,
		"actual_metric":  req.ActualMetric,
		"message":        "Outcome recorded. Thank you — this directly improves prediction calibration.",
	})
}

// GetValidationOutcomeFiber returns the recorded outcome for a validation.
func GetValidationOutcomeFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var outcome, notes string
	var predictedRisk *int
	var actualMetric *float64
	var observedAt time.Time
	err := database.GetDB().QueryRow(ctx,
		`SELECT outcome, COALESCE(notes, ''), predicted_risk, actual_metric, observed_at
		 FROM validation_outcomes WHERE validation_id = $1 AND user_id = $2`,
		validationID, userID).Scan(&outcome, &notes, &predictedRisk, &actualMetric, &observedAt)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "No outcome recorded for this validation"},
		})
	}

	return c.JSON(fiber.Map{
		"validation_id":  validationID,
		"outcome":        outcome,
		"predicted_risk": predictedRisk,
		"actual_metric":  actualMetric,
		"notes":          notes,
		"observed_at":    observedAt,
	})
}

// calibrationSummary computes reliability bins and a Brier score.
// userFilter == "" means global (admin view).
func calibrationSummary(ctx context.Context, userFilter string) (fiber.Map, error) {
	db := database.GetDB()

	query := `SELECT predicted_risk, outcome FROM validation_outcomes WHERE predicted_risk IS NOT NULL`
	args := []interface{}{}
	if userFilter != "" {
		query += ` AND user_id = $1`
		args = append(args, userFilter)
	}

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type bin struct {
		n         int
		predSum   float64
		collapsed int
		degraded  int
	}
	bins := make([]bin, 10)
	var brierSum float64
	total, totalCollapsed := 0, 0

	for rows.Next() {
		var predicted int
		var outcome string
		if err := rows.Scan(&predicted, &outcome); err != nil {
			continue
		}
		if predicted < 0 {
			predicted = 0
		}
		if predicted > 100 {
			predicted = 100
		}
		b := predicted / 10
		if b > 9 {
			b = 9
		}
		p := float64(predicted) / 100.0
		y := 0.0
		if outcome == "collapsed" {
			y = 1.0
			bins[b].collapsed++
			totalCollapsed++
		} else if outcome == "degraded" {
			bins[b].degraded++
		}
		bins[b].n++
		bins[b].predSum += p
		brierSum += (p - y) * (p - y)
		total++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	outBins := []fiber.Map{}
	for i, b := range bins {
		entry := fiber.Map{
			"range":          fiber.Map{"min": i * 10, "max": i*10 + 10},
			"count":          b.n,
			"degraded_count": b.degraded,
		}
		if b.n > 0 {
			entry["predicted_rate"] = math.Round(b.predSum/float64(b.n)*1000) / 1000
			entry["observed_rate"] = math.Round(float64(b.collapsed)/float64(b.n)*1000) / 1000
		} else {
			entry["predicted_rate"] = nil
			entry["observed_rate"] = nil
		}
		outBins = append(outBins, entry)
	}

	result := fiber.Map{
		"bins":            outBins,
		"sample_count":    total,
		"collapsed_count": totalCollapsed,
	}
	if total > 0 {
		result["brier_score"] = math.Round(brierSum/float64(total)*10000) / 10000
		result["base_rate"] = math.Round(float64(totalCollapsed)/float64(total)*1000) / 1000
	}
	if total < 30 {
		result["note"] = "Fewer than 30 recorded outcomes — calibration estimates are not yet statistically meaningful. Keep recording outcomes via POST /validations/{id}/outcome."
	}
	return result, nil
}

// GetCalibrationFiber returns the caller's reliability data.
// GET /analytics/calibration
func GetCalibrationFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	summary, err := calibrationSummary(ctx, userID)
	if err != nil {
		log.Printf("calibration summary failed: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to compute calibration"},
		})
	}
	return c.JSON(summary)
}

// GetCalibrationAdminFiber returns platform-wide reliability data (admin).
// GET /admin/calibration
func GetCalibrationAdminFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	summary, err := calibrationSummary(ctx, "")
	if err != nil {
		log.Printf("admin calibration summary failed: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to compute calibration"},
		})
	}
	return c.JSON(summary)
}
