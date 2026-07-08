package handlers

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Continuous drift monitoring: a monitor re-validates a dataset on a schedule
// and alerts (webhook + notification) when the risk score crosses the
// threshold. Turns one-shot validation into an ongoing quality subscription.

var monitorValidationTypes = map[string]bool{
	"comprehensive": true,
	"distribution":  true,
	"correlation":   true,
	"temporal":      true,
	"full":          true,
}

// CreateMonitorFiber registers a drift monitor for a dataset.
// POST /monitors {"dataset_id":"ds_x","name":"...","interval_hours":24,"max_risk_score":50,"validation_type":"comprehensive"}
func CreateMonitorFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	var req struct {
		DatasetID      string `json:"dataset_id"`
		Name           string `json:"name"`
		IntervalHours  int    `json:"interval_hours"`
		MaxRiskScore   int    `json:"max_risk_score"`
		ValidationType string `json:"validation_type"`
	}
	if err := c.BodyParser(&req); err != nil || req.DatasetID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "dataset_id is required"},
		})
	}
	if req.IntervalHours <= 0 {
		req.IntervalHours = 24
	}
	if req.IntervalHours > 720 { // 30 days
		req.IntervalHours = 720
	}
	if req.MaxRiskScore <= 0 {
		req.MaxRiskScore = 50
	}
	if req.MaxRiskScore > 100 {
		req.MaxRiskScore = 100
	}
	req.ValidationType = strings.ToLower(strings.TrimSpace(req.ValidationType))
	if req.ValidationType == "" {
		req.ValidationType = "comprehensive"
	}
	if !monitorValidationTypes[req.ValidationType] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_VALIDATION_TYPE", "message": "Invalid validation type"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	dataset, err := datasetRepo.GetByID(ctx, req.DatasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATASET_NOT_FOUND", "message": "Dataset not found"},
		})
	}
	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this dataset"},
		})
	}
	if dataset.Status != "processed" && dataset.Status != "ready" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATASET_NOT_READY", "message": "Dataset must be processed before monitoring"},
		})
	}

	if req.Name == "" {
		req.Name = fmt.Sprintf("Monitor: %s", dataset.Filename)
	}
	if len(req.Name) > 255 {
		req.Name = req.Name[:255]
	}

	// Cap active monitors per user to protect the ML tier.
	var activeCount int
	_ = database.GetDB().QueryRow(ctx,
		`SELECT COUNT(*) FROM dataset_monitors WHERE user_id = $1 AND is_active = true`, userID).Scan(&activeCount)
	if activeCount >= 20 {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error": fiber.Map{"code": "MONITOR_LIMIT", "message": "Active monitor limit reached (20). Pause or delete one first."},
		})
	}

	monitorID := "mon_" + uuid.New().String()[:8]
	_, err = database.GetDB().Exec(ctx,
		`INSERT INTO dataset_monitors (id, user_id, dataset_id, name, interval_hours, max_risk_score, validation_type, next_run_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
		monitorID, userID, req.DatasetID, req.Name, req.IntervalHours, req.MaxRiskScore, req.ValidationType)
	if err != nil {
		log.Printf("Failed to create monitor: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to create monitor"},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"monitor_id":      monitorID,
		"dataset_id":      req.DatasetID,
		"name":            req.Name,
		"interval_hours":  req.IntervalHours,
		"max_risk_score":  req.MaxRiskScore,
		"validation_type": req.ValidationType,
		"is_active":       true,
		"message":         "Monitor created. First run starts within a minute; each run consumes validation credits.",
	})
}

// ListMonitorsFiber lists the caller's monitors.
func ListMonitorsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := database.GetDB().Query(ctx,
		`SELECT m.id, m.dataset_id, m.name, m.interval_hours, m.max_risk_score, m.validation_type,
		        m.is_active, COALESCE(m.paused_reason, ''), m.last_run_at, m.next_run_at,
		        m.last_validation_id, m.last_risk_score, m.consecutive_alerts, m.created_at,
		        COALESCE(d.filename, '')
		 FROM dataset_monitors m
		 LEFT JOIN datasets d ON d.id = m.dataset_id
		 WHERE m.user_id = $1
		 ORDER BY m.created_at DESC LIMIT 100`, userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to list monitors"},
		})
	}
	defer rows.Close()

	monitors := []fiber.Map{}
	for rows.Next() {
		var id, datasetID, name, vtype, pausedReason, datasetName string
		var intervalHours, maxRisk, consecutiveAlerts int
		var isActive bool
		var lastRunAt, nextRunAt, createdAt *time.Time
		var lastValidationID *string
		var lastRiskScore *int
		if err := rows.Scan(&id, &datasetID, &name, &intervalHours, &maxRisk, &vtype,
			&isActive, &pausedReason, &lastRunAt, &nextRunAt,
			&lastValidationID, &lastRiskScore, &consecutiveAlerts, &createdAt, &datasetName); err != nil {
			log.Printf("monitor scan failed: %v", err)
			continue
		}
		monitors = append(monitors, fiber.Map{
			"monitor_id":         id,
			"dataset_id":         datasetID,
			"dataset_name":       datasetName,
			"name":               name,
			"interval_hours":     intervalHours,
			"max_risk_score":     maxRisk,
			"validation_type":    vtype,
			"is_active":          isActive,
			"paused_reason":      pausedReason,
			"last_run_at":        lastRunAt,
			"next_run_at":        nextRunAt,
			"last_validation_id": lastValidationID,
			"last_risk_score":    lastRiskScore,
			"consecutive_alerts": consecutiveAlerts,
			"created_at":         createdAt,
		})
	}
	return c.JSON(fiber.Map{"monitors": monitors})
}

// GetMonitorFiber returns a monitor with its recent runs.
func GetMonitorFiber(c *fiber.Ctx) error {
	monitorID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	var datasetID, name, vtype, pausedReason string
	var intervalHours, maxRisk, consecutiveAlerts int
	var isActive bool
	var lastRunAt, nextRunAt *time.Time
	var lastValidationID *string
	var lastRiskScore *int
	var createdAt time.Time
	err := db.QueryRow(ctx,
		`SELECT dataset_id, name, interval_hours, max_risk_score, validation_type, is_active,
		        COALESCE(paused_reason, ''), last_run_at, next_run_at, last_validation_id,
		        last_risk_score, consecutive_alerts, created_at
		 FROM dataset_monitors WHERE id = $1 AND user_id = $2`,
		monitorID, userID).Scan(&datasetID, &name, &intervalHours, &maxRisk, &vtype, &isActive,
		&pausedReason, &lastRunAt, &nextRunAt, &lastValidationID, &lastRiskScore, &consecutiveAlerts, &createdAt)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Monitor not found"},
		})
	}

	runs := []fiber.Map{}
	rows, err := db.Query(ctx,
		`SELECT id, validation_id, status, risk_score, alerted, created_at, evaluated_at
		 FROM monitor_runs WHERE monitor_id = $1 ORDER BY created_at DESC LIMIT 30`, monitorID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var runID, validationID, status string
			var riskScore *int
			var alerted bool
			var runCreated time.Time
			var evaluatedAt *time.Time
			if err := rows.Scan(&runID, &validationID, &status, &riskScore, &alerted, &runCreated, &evaluatedAt); err != nil {
				continue
			}
			runs = append(runs, fiber.Map{
				"run_id":        runID,
				"validation_id": validationID,
				"status":        status,
				"risk_score":    riskScore,
				"alerted":       alerted,
				"created_at":    runCreated,
				"evaluated_at":  evaluatedAt,
			})
		}
	}

	return c.JSON(fiber.Map{
		"monitor_id":         monitorID,
		"dataset_id":         datasetID,
		"name":               name,
		"interval_hours":     intervalHours,
		"max_risk_score":     maxRisk,
		"validation_type":    vtype,
		"is_active":          isActive,
		"paused_reason":      pausedReason,
		"last_run_at":        lastRunAt,
		"next_run_at":        nextRunAt,
		"last_validation_id": lastValidationID,
		"last_risk_score":    lastRiskScore,
		"consecutive_alerts": consecutiveAlerts,
		"created_at":         createdAt,
		"runs":               runs,
	})
}

// UpdateMonitorFiber updates monitor settings, including pause/resume.
// PATCH /monitors/:id
func UpdateMonitorFiber(c *fiber.Ctx) error {
	monitorID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		Name           *string `json:"name"`
		IntervalHours  *int    `json:"interval_hours"`
		MaxRiskScore   *int    `json:"max_risk_score"`
		ValidationType *string `json:"validation_type"`
		IsActive       *bool   `json:"is_active"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	// Ownership check.
	var exists bool
	if err := db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM dataset_monitors WHERE id = $1 AND user_id = $2)`,
		monitorID, userID).Scan(&exists); err != nil || !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Monitor not found"},
		})
	}

	if req.Name != nil && *req.Name != "" {
		name := *req.Name
		if len(name) > 255 {
			name = name[:255]
		}
		_, _ = db.Exec(ctx, `UPDATE dataset_monitors SET name = $2, updated_at = NOW() WHERE id = $1`, monitorID, name)
	}
	if req.IntervalHours != nil && *req.IntervalHours >= 1 && *req.IntervalHours <= 720 {
		_, _ = db.Exec(ctx, `UPDATE dataset_monitors SET interval_hours = $2, updated_at = NOW() WHERE id = $1`, monitorID, *req.IntervalHours)
	}
	if req.MaxRiskScore != nil && *req.MaxRiskScore >= 1 && *req.MaxRiskScore <= 100 {
		_, _ = db.Exec(ctx, `UPDATE dataset_monitors SET max_risk_score = $2, updated_at = NOW() WHERE id = $1`, monitorID, *req.MaxRiskScore)
	}
	if req.ValidationType != nil && monitorValidationTypes[strings.ToLower(*req.ValidationType)] {
		_, _ = db.Exec(ctx, `UPDATE dataset_monitors SET validation_type = $2, updated_at = NOW() WHERE id = $1`, monitorID, strings.ToLower(*req.ValidationType))
	}
	if req.IsActive != nil {
		if *req.IsActive {
			// Resume: clear pause reason and schedule the next run now.
			_, _ = db.Exec(ctx,
				`UPDATE dataset_monitors SET is_active = true, paused_reason = NULL, next_run_at = NOW(), updated_at = NOW() WHERE id = $1`,
				monitorID)
		} else {
			_, _ = db.Exec(ctx,
				`UPDATE dataset_monitors SET is_active = false, paused_reason = 'paused_by_user', updated_at = NOW() WHERE id = $1`,
				monitorID)
		}
	}

	return GetMonitorFiber(c)
}

// DeleteMonitorFiber deletes a monitor and its run history.
func DeleteMonitorFiber(c *fiber.Ctx) error {
	monitorID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := database.GetDB().Exec(ctx,
		`DELETE FROM dataset_monitors WHERE id = $1 AND user_id = $2`, monitorID, userID)
	if err != nil || tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Monitor not found"},
		})
	}
	return c.JSON(fiber.Map{"deleted": true, "monitor_id": monitorID})
}
